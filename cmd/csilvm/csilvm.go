package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/mesosphere/csilvm/pkg/csilvm"
	"github.com/mesosphere/csilvm/pkg/lvm"

	datadogstatsd "github.com/DataDog/datadog-go/statsd"
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/mesosphere/csilvm/pkg/ddstatsd"
	"github.com/uber-go/tally"
	tallystatsd "github.com/uber-go/tally/statsd"
)

const (
	defaultDefaultFs         = "xfs"
	defaultDefaultVolumeSize = 10 << 30
	defaultRequestLimit      = 10
)

type stringsFlag []string

func (f *stringsFlag) String() string {
	return fmt.Sprint(*f)
}

func (f *stringsFlag) Set(tag string) error {
	*f = append(*f, tag)
	return nil
}

func main() {
	rand.Seed(time.Now().UnixNano())

	// Configure flags
	requestLimitF := flag.Int("request-limit", defaultRequestLimit, "Limits backlog of pending requests.")
	vgnameF := flag.String("volume-group", "", "The name of the volume group to manage")
	pvnamesF := flag.String("devices", "", "A comma-seperated list of devices in the volume group")
	defaultFsF := flag.String("default-fs", defaultDefaultFs, "The default filesystem to format new volumes with")
	defaultVolumeSizeF := flag.Uint64("default-volume-size", defaultDefaultVolumeSize, "The default volume size in bytes")
	socketFileF := flag.String("unix-addr", "", "The path to the listening unix socket file")
	socketFileEnvF := flag.String("unix-addr-env", "", "An optional environment variable from which to read the unix-addr")
	removeF := flag.Bool("remove-volume-group", false, "If set, the volume group will be removed when ProbeNode is called.")
	var tagsF stringsFlag
	flag.Var(&tagsF, "tag", "Value to tag the volume group with (can be given multiple times)")
	var probeModulesF stringsFlag
	flag.Var(&probeModulesF, "probe-module", "Probe checks that the kernel module is loaded")
	nodeIDF := flag.String("node-id", "", "The node ID reported via the CSI Node gRPC service")
	// Metrics-related flags
	statsdUDPHostEnvVarF := flag.String("statsd-udp-host-env-var", "", "The name of the environment variable containing the host where a statsd service is listening for stats over UDP")
	statsdUDPPortEnvVarF := flag.String("statsd-udp-port-env-var", "", "The name of the environment variable containing the port where a statsd service is listening for stats over UDP")
	statsdFormatF := flag.String("statsd-format", "datadog", "The statsd format to use (one of: classic, datadog)")
	statsdMaxUDPSizeF := flag.Int("statsd-max-udp-size", 1432, "The size to buffer before transmitting a statsd UDP packet")
	flag.Parse()
	// Setup logging
	logprefix := fmt.Sprintf("[%s]", *vgnameF)
	logflags := log.LstdFlags | log.Lshortfile
	logger := log.New(os.Stderr, logprefix, logflags)
	csilvm.SetLogger(logger)
	lvm.SetLogger(logger)
	// Determine listen address.
	if *socketFileF != "" && *socketFileEnvF != "" {
		log.Fatalf("[%s] cannot specify -unix-addr and -unix-addr-env", *vgnameF)
	}
	sock := *socketFileF
	if *socketFileEnvF != "" {
		sock = os.Getenv(*socketFileEnvF)
	}
	if strings.HasPrefix(sock, "unix://") {
		sock = sock[len("unix://"):]
	}
	// Unlink the domain socket in case it is left lying around from a
	// previous run. err return is not really interesting because it is
	// normal for this to fail if the process is starting for the first time.
	log.Printf("[%s] Unlinking %s", *vgnameF, sock)
	syscall.Unlink(sock)
	// Setup socket listener
	lis, err := net.Listen("unix", sock)
	if err != nil {
		log.Fatalf("[%s] Failed to listen: %v", *vgnameF, err)
	}
	// Setup server
	if *requestLimitF < 1 {
		log.Fatalf("request-limit requires a positive, integer value instead of %d", *requestLimitF)
	}
	// TODO(jdef) at some point we should require the node-id flag since it's
	// a required part of the CSI spec.
	const defaultMaxStringLen = 128
	if len(*nodeIDF) > defaultMaxStringLen {
		log.Fatalf("node-id cannot be longer than %d bytes: %q", defaultMaxStringLen, *nodeIDF)
	}
	scope := tally.NoopScope
	if *statsdUDPHostEnvVarF != "" && *statsdUDPPortEnvVarF != "" {
		statsdHost := os.Getenv(*statsdUDPHostEnvVarF)
		statsdPort := os.Getenv(*statsdUDPPortEnvVarF)
		statsdServerAddr := fmt.Sprintf("%s:%s", statsdHost, statsdPort)
		// Set no statsd prefix, tags are already prefixed using 'csilvm'.
		const (
			statsdPrefix     = ""
			maxFlushInterval = time.Second
		)
		var reporter tally.StatsReporter
		switch *statsdFormatF {
		case "datadog":
			// The datadog statsd client does not support setting a
			// custom flush interval. It defaults to 100ms:
			// https://github.com/DataDog/datadog-go/blob/40bafcb5f6c1d49df36deaf4ab019e44961d5e36/statsd/statsd.go#L150
			client, err := datadogstatsd.NewBuffered(
				statsdServerAddr,
				*statsdMaxUDPSizeF,
			)
			if err != nil {
				log.Fatal(err)
			}
			client.Namespace = statsdPrefix
			reporter = ddstatsd.NewReporter(client, ddstatsd.Options{
				SampleRate: 1.0,
			})
		case "classic":
			client, err := statsd.NewBufferedClient(
				statsdServerAddr,
				statsdPrefix,
				maxFlushInterval,
				*statsdMaxUDPSizeF,
			)
			if err != nil {
				log.Fatal(err)
			}
			reporter = tallystatsd.NewReporter(client, tallystatsd.Options{
				SampleRate: 1.0,
			})
		default:
			log.Fatalf("unknown -statsd-format value: %q", *statsdFormatF)
		}
		var closer io.Closer
		scope, closer = tally.NewRootScope(tally.ScopeOptions{
			Prefix:   "csilvm",
			Tags:     map[string]string{},
			Reporter: reporter,
		}, time.Second)
		defer closer.Close()
	}
	var grpcOpts []grpc.ServerOption
	grpcOpts = append(grpcOpts,
		grpc.UnaryInterceptor(
			csilvm.ChainUnaryServer(
				csilvm.RequestLimitInterceptor(*requestLimitF),
				csilvm.SerializingInterceptor(),
				csilvm.LoggingInterceptor(),
				csilvm.MetricsInterceptor(scope),
			),
		),
	)
	grpcServer := grpc.NewServer(grpcOpts...)
	opts := []csilvm.ServerOpt{
		csilvm.NodeID(*nodeIDF),
	}
	opts = append(opts,
		csilvm.DefaultVolumeSize(*defaultVolumeSizeF),
		csilvm.ProbeModules(probeModulesF),
		csilvm.Metrics(scope),
	)
	if *removeF {
		opts = append(opts, csilvm.RemoveVolumeGroup())
	}
	for _, tag := range tagsF {
		opts = append(opts, csilvm.Tag(tag))
	}
	s := csilvm.NewServer(*vgnameF, strings.Split(*pvnamesF, ","), *defaultFsF, opts...)
	if err := s.Setup(); err != nil {
		log.Fatalf("[%s] error initializing csilvm plugin: err=%v", *vgnameF, err)
	}
	defer s.ReportUptime()
	csi.RegisterIdentityServer(grpcServer, csilvm.IdentityServerValidator(s))
	csi.RegisterControllerServer(grpcServer, csilvm.ControllerServerValidator(s, s.RemovingVolumeGroup(), s.SupportedFilesystems()))
	csi.RegisterNodeServer(grpcServer, csilvm.NodeServerValidator(s, s.RemovingVolumeGroup(), s.SupportedFilesystems()))
	grpcServer.Serve(lis)
}
