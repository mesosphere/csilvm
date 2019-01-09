package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"google.golang.org/grpc"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/mesosphere/csilvm/pkg/csilvm"
	"github.com/mesosphere/csilvm/pkg/lvm"
)

const (
	defaultDefaultFs         = "xfs"
	defaultDefaultVolumeSize = 10 << 30
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
	// Configure flags
	vgnameF := flag.String("volume-group", "", "The name of the volume group to manage")
	pvnamesF := flag.String("devices", "", "A comma-seperated list of devices in the volume group")
	defaultFsF := flag.String("default-fs", defaultDefaultFs, "The default filesystem to format new volumes with")
	defaultVolumeSizeF := flag.Uint64("default-volume-size", defaultDefaultVolumeSize, "The default volume size in bytes")
	socketFileF := flag.String("unix-addr", "", "The path to the listening unix socket file")
	socketFileEnvF := flag.String("unix-addr-env", "", "An optional environment variable from which to read the unix-addr")
	removeF := flag.Bool("remove-volume-group", false, "If set, the volume group will be removed when ProbeNode is called.")
	var tagsF stringsFlag
	flag.Var(&tagsF, "tag", "Value to tag the volume group with (can be given multiple times)")
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
	// Setup socket listener
	lis, err := net.Listen("unix", sock)
	if err != nil {
		log.Fatalf("[%s] Failed to listen: %v", *vgnameF, err)
	}
	// Setup server
	var grpcOpts []grpc.ServerOption
	grpcOpts = append(grpcOpts,
		grpc.UnaryInterceptor(
			csilvm.ChainUnaryServer(
				csilvm.SerializingInterceptor(),
				csilvm.LoggingInterceptor(),
			),
		),
	)
	grpcServer := grpc.NewServer(grpcOpts...)
	var opts []csilvm.ServerOpt
	opts = append(opts, csilvm.DefaultVolumeSize(*defaultVolumeSizeF))
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
	csi.RegisterIdentityServer(grpcServer, csilvm.IdentityServerValidator(s))
	csi.RegisterControllerServer(grpcServer, csilvm.ControllerServerValidator(s, s.RemovingVolumeGroup(), s.SupportedFilesystems()))
	csi.RegisterNodeServer(grpcServer, csilvm.NodeServerValidator(s, s.RemovingVolumeGroup(), s.SupportedFilesystems()))
	grpcServer.Serve(lis)
}
