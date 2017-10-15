package main

import (
	"flag"
	"log"
	"net"
	"strings"

	"google.golang.org/grpc"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/mesosphere/csilvm/lvs"
)

var (
	vgnameF            = flag.String("volume-group", "", "The name of the volume group to manage")
	pvnamesF           = flag.String("devices", "", "A comma-seperated list of devices in the volume group")
	defaultFsF         = flag.String("default-fs", "xfs", "The default filesystem to format new volumes with")
	defaultVolumeSizeF = flag.Uint64("default-volume-size", 10<<30, "The default volume size in bytes")
	socketFileF        = flag.String("unix-addr", "", "The path to the listening unix socket file")
)

func main() {
	flag.Parse()
	lis, err := net.Listen("unix", *socketFileF)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	var opts []lvs.ServerOpt
	opts = append(opts, lvs.DefaultVolumeSize(*defaultVolumeSizeF))
	s := lvs.NewServer(*vgnameF, strings.Split(*pvnamesF, ","), *defaultFsF, opts...)
	csi.RegisterIdentityServer(grpcServer, s)
	csi.RegisterControllerServer(grpcServer, s)
	grpcServer.Serve(lis)
}
