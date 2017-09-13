package lvs

import (
	"context"
	"flag"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	"github.com/mesosphere/csilvm"
)

var (
	socketFile = flag.String("socket_file", "", "The path to the listening unix socket file")
)

func TestGetSupportedVersions(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	resp, err := client.GetSupportedVersions(context.Background(), &csi.GetSupportedVersionsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result == nil {
		t.Fatalf("Error: %+v", resp.GetError())
	}
	if len(result.SupportedVersions) != 1 {
		t.Fatalf("Expected only one supported version, got %d", len(result.SupportedVersions))
	}
	got := *result.SupportedVersions[0]
	exp := csi.Version{0, 1, 0}
	if got != exp {
		t.Fatalf("Expected version %#v but got %#v", exp, got)
	}
}

func startTest() (client csi.IdentityClient, cleanupFn func()) {
	var cleanup csilvm.CleanupSteps
	defer func() {
		if x := recover(); x != nil {
			cleanup.Unwind()
			panic(x)
		}
	}()
	lis, err := net.Listen("unix", "@/lvs-test-"+uuid.New().String())
	if err != nil {
		panic(err)
	}
	cleanup.Add(lis.Close)
	var opts []grpc.ServerOption
	// Start a grpc server listening on the socket.
	grpcServer := grpc.NewServer(opts...)
	s := NewServer()
	csi.RegisterIdentityServer(grpcServer, s)
	go grpcServer.Serve(lis)

	// Start a grpc client connected to the server.
	unixDialer := func(addr string, timeout time.Duration) (net.Conn, error) {
		return net.DialTimeout("unix", addr, timeout)
	}
	clientOpts := []grpc.DialOption{
		grpc.WithDialer(unixDialer),
		grpc.WithInsecure(),
	}
	conn, err := grpc.Dial(lis.Addr().String(), clientOpts...)
	if err != nil {
		panic(err)
	}
	cleanup.Add(conn.Close)
	client = csi.NewIdentityClient(conn)
	return client, cleanup.Unwind
}
