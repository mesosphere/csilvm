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
	"github.com/mesosphere/csilvm/lvm"
)

// The size of the physical volumes we create in our tests.
const pvsize = 100 << 20 // 100MiB

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

func testGetPluginInfoRequest() *csi.GetPluginInfoRequest {
	req := &csi.GetPluginInfoRequest{Version: &csi.Version{0, 1, 0}}
	return req
}

func TestGetPluginInfo(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetPluginInfoRequest()
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

func testCreateVolumeRequest() *csi.CreateVolumeRequest {
	const requiredBytes = 80 << 20
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
	return req
}

func TestCreateVolume(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.GetError(); err != nil {
		t.Fatalf("error: %#v", err)
	}
	result := resp.GetResult()
	info := result.VolumeInfo
	if info.GetCapacityBytes() != req.GetCapacityRange().GetRequiredBytes() {
		t.Fatalf("Expected required_bytes (%v) to match volume size (%v).", req.GetCapacityRange().GetRequiredBytes(), info.GetCapacityBytes())
	}
	if info.GetHandle().GetId() != req.GetName() {
		t.Fatalf("Expected volume ID (%v) to match name (%v).", info.GetHandle().GetId(), req.GetName())
	}
}

func TestCreateVolumeAlreadyExists(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	// Use only half the usual size so there is enough space for a
	// second volume to be created.
	req.CapacityRange.RequiredBytes /= 2
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.GetResult()
	if result == nil || resp.GetError() != nil {
		t.Fatalf("unexpected error")
	}
	// Check that trying to create the volume again fails with
	// VOLUME_ALREADY_EXISTS.
	resp, err = client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetResult() != nil {
		t.Fatal(err)
	}
	grpcErr := resp.GetError()
	errorCode := grpcErr.GetCreateVolumeError().GetErrorCode()
	errorDesc := grpcErr.GetCreateVolumeError().GetErrorDescription()
	expCode := csi.Error_CreateVolumeError_VOLUME_ALREADY_EXISTS
	if errorCode != expCode {
		t.Fatalf("Expected error code %v but got %v", expCode, errorCode)
	}
	expDesc := "A logical volume with that name already exists."
	if errorDesc != expDesc {
		t.Fatalf("Expected error description '%v' but got '%v'", expDesc, errorDesc)
	}
}

func TestCreateVolumeUnsupportedCapacityRange(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	// Use only half the usual size so there is enough space for a
	// second volume to be created.
	req.CapacityRange.RequiredBytes = pvsize * 2
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	grpcErr := resp.GetError()
	errorCode := grpcErr.GetCreateVolumeError().GetErrorCode()
	errorDesc := grpcErr.GetCreateVolumeError().GetErrorDescription()
	expCode := csi.Error_CreateVolumeError_UNSUPPORTED_CAPACITY_RANGE
	if errorCode != expCode {
		t.Fatalf("Expected error code %v but got %v.", expCode, errorCode)
	}
	expDesc := "Not enough free space."
	if errorDesc != expDesc {
		t.Fatalf("Expected error description %v but got %v.", expDesc, errorDesc)
	}
}

func TestCreateVolumeInvalidVolumeName(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	// Use only half the usual size so there is enough space for a
	// second volume to be created.
	req.Name = "invalid name : /"
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	grpcErr := resp.GetError()
	errorCode := grpcErr.GetCreateVolumeError().GetErrorCode()
	errorDesc := grpcErr.GetCreateVolumeError().GetErrorDescription()
	exp := csi.Error_CreateVolumeError_INVALID_VOLUME_NAME
	if errorCode != exp {
		t.Fatalf("Expected error code %v but got %v.", exp, errorCode)
	}
	expDesc := "lvm: validateLogicalVolumeName: Name contains invalid character, valid set includes: [a-zA-Z0-9.-_+]. (-1)"
	if errorDesc != expDesc {
		t.Fatalf("Expected error description %v but got %v.", expDesc, errorDesc)
	}
}

func testDeleteVolumeRequest() *csi.DeleteVolumeRequest {
	volumeHandle := &csi.VolumeHandle{
		"test-volume",
		nil,
	}
	req := &csi.DeleteVolumeRequest{
		&csi.Version{0, 1, 0},
		volumeHandle,
		nil,
	}
	return req
}

func TestDeleteVolume(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testDeleteVolumeRequest()
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

func testValidateVolumeCapabilitiesRequest() *csi.ValidateVolumeCapabilitiesRequest {
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
	return req
}

func TestValidateVolumeCapabilities(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest()
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

func testListVolumesRequest() *csi.ListVolumesRequest {
	req := &csi.ListVolumesRequest{
		&csi.Version{0, 1, 0},
		0,
		"",
	}
	return req
}

func TestListVolumes(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testListVolumesRequest()
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

func testGetCapacityRequest() *csi.GetCapacityRequest {
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
	return req
}

func TestGetCapacity(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetCapacityRequest()
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

func testNodePublishVolumeRequest() *csi.NodePublishVolumeRequest {
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
	return req
}

func TestNodePublishVolume(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest()
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

func testNodeUnpublishVolumeRequest() *csi.NodeUnpublishVolumeRequest {
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
	return req
}

func TestNodeUnpublishVolume(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodeUnpublishVolumeRequest()
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

func testGetNodeIDRequest() *csi.GetNodeIDRequest {
	req := &csi.GetNodeIDRequest{
		&csi.Version{0, 1, 0},
	}
	return req
}

func TestGetNodeID(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetNodeIDRequest()
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

func testProbeNodeRequest() *csi.ProbeNodeRequest {
	req := &csi.ProbeNodeRequest{
		&csi.Version{0, 1, 0},
	}
	return req
}

func TestProbeNode(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testProbeNodeRequest()
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

func testNodeGetCapabilitiesRequest() *csi.NodeGetCapabilitiesRequest {
	req := &csi.NodeGetCapabilitiesRequest{
		&csi.Version{0, 1, 0},
	}
	return req
}

func TestNodeGetCapabilities(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodeGetCapabilitiesRequest()
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

	// Create a volume group for the server to manage.
	handle, err := lvm.NewLibHandle()
	if err != nil {
		panic(err)
	}
	cleanup.Add(handle.Close)

	loop, err := lvm.CreateLoopDevice(pvsize)
	if err != nil {
		panic(err)
	}
	cleanup.Add(loop.Close)

	// Create a physical volume using the loop device.
	var pvs []*lvm.PhysicalVolume
	pv, err := handle.CreatePhysicalVolume(loop.Path())
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() error { return pv.Remove() })
	pvs = append(pvs, pv)

	// Create a volume group containing the physical volume.
	vgname := "test-vg-" + uuid.New().String()
	vg, err := handle.CreateVolumeGroup(vgname, pvs)
	if err != nil {
		panic(err)
	}
	cleanup.Add(vg.Remove)

	// Clean up any remaining logical volumes.
	cleanup.Add(func() error {
		lvnames, err := vg.ListLogicalVolumeNames()
		if err != nil {
			panic(err)
		}
		for _, lvname := range lvnames {
			lv, err := vg.LookupLogicalVolume(lvname)
			if err != nil {
				panic(err)
			}
			if err := lv.Remove(); err != nil {
				panic(err)
			}
		}
		return nil
	})

	var opts []grpc.ServerOption
	// Start a grpc server listening on the socket.
	grpcServer := grpc.NewServer(opts...)
	s := NewServer(vg)
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
