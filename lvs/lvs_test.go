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
	req := &csi.GetSupportedVersionsRequest{}
	resp, err := client.GetSupportedVersions(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result == nil {
		t.Fatalf("Error: %+v", resp.GetError())
	}
	if len(result.GetSupportedVersions()) != 1 {
		t.Fatalf("Expected only one supported version, got %d", len(result.SupportedVersions))
	}
	got := *result.GetSupportedVersions()[0]
	exp := csi.Version{0, 1, 0}
	if got != exp {
		t.Fatalf("Expected version %#v but got %#v", exp, got)
	}
}

func TestGetPluginInfoGoodVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := &csi.GetPluginInfoRequest{Version: &csi.Version{0, 1, 0}}
	resp, err := client.GetPluginInfo(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result == nil {
		t.Fatalf("Error: %+v", resp.GetError())
	}
	if result.GetName() != PluginName {
		t.Fatal("Expected plugin name %s but got %s", PluginName, result.GetName())
	}
	if result.GetVendorVersion() != PluginVersion {
		t.Fatal("Expected plugin version %s but got %s", PluginVersion, result.GetVendorVersion())
	}
	if result.GetManifest() != nil {
		t.Fatal("Expected a nil manifest but got %s", result.GetManifest())
	}
}

func TestGetPluginInfoUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := &csi.GetPluginInfoRequest{Version: &csi.Version{0, 2, 0}}
	resp, err := client.GetPluginInfo(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != true {
		t.Fatal("Expected CallerMustNotRetry to be true")
	}
	if error.GetErrorDescription() != "" {
		t.Fatalf("Expected ErrorDescription to be '' but was '%s'", error.GetErrorDescription())
	}
}

func TestGetPluginInfoUnspecifiedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := &csi.GetPluginInfoRequest{}
	resp, err := client.GetPluginInfo(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetGeneralError()
	expcode := csi.Error_GeneralError_MISSING_REQUIRED_FIELD
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	if error.GetCallerMustNotRetry() != false {
		t.Fatal("Expected CallerMustNotRetry to be false")
	}
	if error.GetErrorDescription() != "" {
		t.Fatalf("Expected ErrorDescription to be '' but was '%s'", error.GetErrorDescription())
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
