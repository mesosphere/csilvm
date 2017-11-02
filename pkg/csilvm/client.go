package csilvm

import (
	"github.com/container-storage-interface/spec/lib/go/csi"
	grpc "google.golang.org/grpc"
)

type Client struct {
	csi.IdentityClient
	csi.ControllerClient
	csi.NodeClient
}

func NewClient(conn *grpc.ClientConn) *Client {
	return &Client{
		csi.NewIdentityClient(conn),
		csi.NewControllerClient(conn),
		csi.NewNodeClient(conn),
	}
}
