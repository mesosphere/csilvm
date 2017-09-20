package main

import (
	"flag"
	"log"
	"net"

	"google.golang.org/grpc"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/mesosphere/csilvm/lvs"
)

var (
	socketFile = flag.String("unix-addr", "", "The path to the listening unix socket file")
)

func main() {
	flag.Parse()
	lis, err := net.Listen("unix", *socketFile)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	s := lvs.NewServer()
	csi.RegisterIdentityServer(grpcServer, s)
	csi.RegisterControllerServer(grpcServer, s)
	grpcServer.Serve(lis)
}
