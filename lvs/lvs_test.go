package lvs

import (
	"context"
	"flag"
	"net"
	"reflect"
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

// IdentityService RPCs

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

func TestGetPluginInfo(t *testing.T) {
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

// ControllerService RPCs

func TestCreateVolume(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	const requiredBytes = 100 << 20
	const limitBytes = 1000 << 20
	volumeCapabilities := []*csi.VolumeCapability{
		{
			&csi.VolumeCapability_Block{
				&csi.VolumeCapability_BlockVolume{},
			},
			&csi.VolumeCapability_AccessMode{
				csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	req := &csi.CreateVolumeRequest{
		&csi.Version{0, 1, 0},
		"test-volume",
		&csi.CapacityRange{requiredBytes, limitBytes},
		volumeCapabilities,
		nil,
		nil,
	}
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	// Method is still stubbed...
	if result != nil {
		t.Fatalf("method is still stubbed")
	}
}

func TestDeleteVolume(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	volumeHandle := &csi.VolumeHandle{
		"test-volume",
		nil,
	}
	req := &csi.DeleteVolumeRequest{
		&csi.Version{0, 1, 0},
		volumeHandle,
		nil,
	}
	resp, err := client.DeleteVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	// Method is still stubbed...
	if result != nil {
		t.Fatalf("method is still stubbed")
	}
}

func TestControllerPublishVolumeNotSupported(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := &csi.ControllerPublishVolumeRequest{}
	resp, err := client.ControllerPublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetControllerPublishVolumeError()
	expcode := csi.Error_ControllerPublishVolumeError_CALL_NOT_IMPLEMENTED
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	expdesc := "The ControllerPublishVolume RPC is not supported."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
	if error.GetNodeIds() != nil {
		t.Fatalf("Expected NodeIds to be nil but was '%s'", error.GetNodeIds())
	}
}

func TestControllerUnpublishVolumeNotSupported(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := &csi.ControllerUnpublishVolumeRequest{}
	resp, err := client.ControllerUnpublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result != nil {
		t.Fatalf("Expected Result to be nil but was: %+v", resp.GetResult())
	}
	error := resp.GetError().GetControllerUnpublishVolumeError()
	expcode := csi.Error_ControllerUnpublishVolumeError_CALL_NOT_IMPLEMENTED
	if error.GetErrorCode() != expcode {
		t.Fatalf("Expected error code %d but got %d", expcode, error.GetErrorCode())
	}
	expdesc := "The ControllerUnpublishVolume RPC is not supported."
	if error.GetErrorDescription() != expdesc {
		t.Fatalf("Expected ErrorDescription to be '%s' but was '%s'", expdesc, error.GetErrorDescription())
	}
}

func TestValidateVolumeCapabilities(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	const capacityBytes = 100 << 20
	volumeHandle := &csi.VolumeHandle{
		"test-volume",
		nil,
	}
	volumeInfo := &csi.VolumeInfo{
		100 << 20,
		volumeHandle,
	}
	volumeCapabilities := []*csi.VolumeCapability{
		{
			&csi.VolumeCapability_Block{
				&csi.VolumeCapability_BlockVolume{},
			},
			&csi.VolumeCapability_AccessMode{
				csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	req := &csi.ValidateVolumeCapabilitiesRequest{
		&csi.Version{0, 1, 0},
		volumeInfo,
		volumeCapabilities,
	}
	resp, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	// Method is still stubbed...
	if result != nil {
		t.Fatalf("method is still stubbed")
	}
}

func TestListVolumes(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := &csi.ListVolumesRequest{
		&csi.Version{0, 1, 0},
		0,
		"",
	}
	resp, err := client.ListVolumes(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	// Method is still stubbed...
	if result != nil {
		t.Fatalf("method is still stubbed")
	}
}

func TestGetCapacity(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	volumeCapabilities := []*csi.VolumeCapability{
		{
			&csi.VolumeCapability_Block{
				&csi.VolumeCapability_BlockVolume{},
			},
			&csi.VolumeCapability_AccessMode{
				csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	req := &csi.GetCapacityRequest{
		&csi.Version{0, 1, 0},
		volumeCapabilities,
		nil,
	}
	resp, err := client.GetCapacity(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	// Method is still stubbed...
	if result != nil {
		t.Fatalf("method is still stubbed")
	}
}

func TestControllerGetCapabilities(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := &csi.ControllerGetCapabilitiesRequest{Version: &csi.Version{0, 1, 0}}
	resp, err := client.ControllerGetCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result == nil {
		t.Fatalf("Error: %+v", resp.GetError())
	}
	expected := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
		csi.ControllerServiceCapability_RPC_GET_CAPACITY,
	}
	got := []csi.ControllerServiceCapability_RPC_Type{}
	for _, capability := range result.GetCapabilities() {
		got = append(got, capability.GetRpc().Type)
	}
	if !reflect.DeepEqual(expected, got) {
		t.Fatalf("Expected capabilities %+v but got %+v", expected, got)
	}
}

// NodeService RPCs

func TestNodePublishVolume(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	volumeHandle := &csi.VolumeHandle{
		"test-volume",
		nil,
	}
	const targetPath = "/run/dcos/lvs/mnt/test-volume"
	volumeCapability := &csi.VolumeCapability{
		&csi.VolumeCapability_Block{
			&csi.VolumeCapability_BlockVolume{},
		},
		&csi.VolumeCapability_AccessMode{
			csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
		},
	}
	const readonly = false
	req := &csi.NodePublishVolumeRequest{
		&csi.Version{0, 1, 0},
		volumeHandle,
		nil,
		targetPath,
		volumeCapability,
		readonly,
		nil,
	}
	resp, err := client.NodePublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	// Method is still stubbed...
	if result != nil {
		t.Fatalf("method is still stubbed")
	}
}

func TestNodeUnpublishVolume(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	volumeHandle := &csi.VolumeHandle{
		"test-volume",
		nil,
	}
	const targetPath = "/run/dcos/lvs/mnt/test-volume"
	req := &csi.NodeUnpublishVolumeRequest{
		&csi.Version{0, 1, 0},
		volumeHandle,
		targetPath,
		nil,
	}
	resp, err := client.NodeUnpublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	// Method is still stubbed...
	if result != nil {
		t.Fatalf("method is still stubbed")
	}
}

func TestGetNodeID(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := &csi.GetNodeIDRequest{
		&csi.Version{0, 1, 0},
	}
	resp, err := client.GetNodeID(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	// Method is still stubbed...
	if result != nil {
		t.Fatalf("method is still stubbed")
	}
}

func TestProbeNode(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := &csi.ProbeNodeRequest{
		&csi.Version{0, 1, 0},
	}
	resp, err := client.ProbeNode(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	// Method is still stubbed...
	if result != nil {
		t.Fatalf("method is still stubbed")
	}
}

func TestNodeGetCapabilities(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := &csi.NodeGetCapabilitiesRequest{
		&csi.Version{0, 1, 0},
	}
	resp, err := client.NodeGetCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	// Method is still stubbed...
	if result != nil {
		t.Fatalf("method is still stubbed")
	}
}

func startTest() (client *Client, cleanupFn func()) {
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
	csi.RegisterControllerServer(grpcServer, s)
	csi.RegisterNodeServer(grpcServer, s)
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
	return NewClient(conn), cleanup.Unwind
}
