package main

import (
	"context"
	"flag"
	"log"
	"net"

	"google.golang.org/grpc"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

var (
	socketFile = flag.String("socket_file", "", "The path to the listening unix socket file")
)

type server struct {
}

func (s *server) GetSupportedVersions(
	ctx context.Context,
	request *csi.GetSupportedVersionsRequest) (*csi.GetSupportedVersionsResponse, error) {
	return &csi.GetSupportedVersionsResponse{}, nil
}

func (s *server) GetPluginInfo(
	ctx context.Context,
	request *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	return &csi.GetPluginInfoResponse{}, nil
}

func main() {
	flag.Parse()
	lis, err := net.Listen("unix", *socketFile)
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	s := new(server)
	csi.RegisterIdentityServer(grpcServer, s)
	grpcServer.Serve(lis)
}
