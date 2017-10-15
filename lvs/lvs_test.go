package lvs

import (
	"context"
	"flag"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
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

func init() {
	// Refresh the LVM metadata held by the lvmetad process to
	// clear any metadata left over from a previous run.
	if err := lvm.PVScan(""); err != nil {
		panic(err)
	}
}

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
		{
			&csi.VolumeCapability_Mount{
				&csi.VolumeCapability_MountVolume{
					"xfs",
					nil,
				},
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

func TestCreateVolume_BlockVolume(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.GetError(); err != nil {
		t.Fatalf("Error: %+v", err)
	}
	result := resp.GetResult()
	info := result.VolumeInfo
	if info.GetCapacityBytes() != req.GetCapacityRange().GetRequiredBytes() {
		t.Fatalf("Expected required_bytes (%v) to match volume size (%v).", req.GetCapacityRange().GetRequiredBytes(), info.GetCapacityBytes())
	}
	if !strings.HasSuffix(info.GetHandle().GetId(), req.GetName()) {
		t.Fatalf("Expected volume ID (%v) to name as a suffix (%v).", info.GetHandle().GetId(), req.GetName())
	}
}

func TestCreateVolume_MountVolume(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.GetError(); err != nil {
		t.Fatalf("Error: %+v", err)
	}
	result := resp.GetResult()
	info := result.VolumeInfo
	if info.GetCapacityBytes() != req.GetCapacityRange().GetRequiredBytes() {
		t.Fatalf("Expected required_bytes (%v) to match volume size (%v).", req.GetCapacityRange().GetRequiredBytes(), info.GetCapacityBytes())
	}
	if !strings.HasSuffix(info.GetHandle().GetId(), req.GetName()) {
		t.Fatalf("Expected volume ID (%v) to name as a suffix (%v).", info.GetHandle().GetId(), req.GetName())
	}
}

func TestCreateVolumeDefaultSize(t *testing.T) {
	const defaultVolumeSize = uint64(20 << 20)
	client, cleanup := startTest(DefaultVolumeSize(defaultVolumeSize))
	defer cleanup()
	req := testCreateVolumeRequest()
	// Specify no CapacityRange so the volume gets the default
	// size.
	req.CapacityRange = nil
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.GetError(); err != nil {
		t.Fatalf("Error: %+v", err)
	}
	result := resp.GetResult()
	info := result.VolumeInfo
	if info.GetCapacityBytes() != defaultVolumeSize {
		t.Fatalf("Expected defaultVolumeSize (%v) to match volume size (%v).", defaultVolumeSize, info.GetCapacityBytes())
	}
	if !strings.HasSuffix(info.GetHandle().GetId(), req.GetName()) {
		t.Fatalf("Expected volume ID (%v) to name as a suffix (%v).", info.GetHandle().GetId(), req.GetName())
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

func testDeleteVolumeRequest(volumeId string) *csi.DeleteVolumeRequest {
	volumeHandle := &csi.VolumeHandle{
		volumeId,
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
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	if createResp.GetError() != nil {
		t.Fatal("CreateVolume failed.")
	}
	result := createResp.GetResult()
	info := result.VolumeInfo
	id := info.GetHandle().GetId()
	req := testDeleteVolumeRequest(id)
	resp, err := client.DeleteVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetResult() == nil || resp.GetError() != nil {
		t.Fatalf("DeleteVolume failed: %+v", resp.GetError())
	}
}

func TestDeleteVolumeUnknownVolume(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testDeleteVolumeRequest("missing-volume")
	resp, err := client.DeleteVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	grpcErr := resp.GetError()
	errorCode := grpcErr.GetDeleteVolumeError().GetErrorCode()
	errorDesc := grpcErr.GetDeleteVolumeError().GetErrorDescription()
	expCode := csi.Error_DeleteVolumeError_VOLUME_DOES_NOT_EXIST
	if errorCode != expCode {
		t.Fatalf("Expected error code %v but got %v", expCode, errorCode)
	}
	expDesc := lvm.ErrLogicalVolumeNotFound.Error()
	if errorDesc != expDesc {
		t.Fatalf("Expected error description '%v' but got '%v'", expDesc, errorDesc)
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

func testValidateVolumeCapabilitiesRequest(volumeHandle *csi.VolumeHandle, filesystem string, mountOpts []string) *csi.ValidateVolumeCapabilitiesRequest {
	const capacityBytes = 100 << 20
	volumeInfo := &csi.VolumeInfo{
		capacityBytes,
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
		{
			&csi.VolumeCapability_Mount{
				&csi.VolumeCapability_MountVolume{
					filesystem,
					mountOpts,
				},
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

func TestValidateVolumeCapabilities_BlockVolume(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	if err := createResp.GetError(); err != nil {
		t.Fatalf("error: %+v", err)
	}
	createResult := createResp.GetResult()
	volumeHandle := createResult.VolumeInfo.GetHandle()
	req := testValidateVolumeCapabilitiesRequest(volumeHandle, "xfs", nil)
	resp, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	if err := createResp.GetError(); err != nil {
		t.Fatalf("error: %+v", err)
	}
	result := resp.GetResult()
	if !result.GetSupported() {
		t.Fatal("Expected requested volume capabilities to be supported.")
	}
}

func TestValidateVolumeCapabilities_MountVolume(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	if err := createResp.GetError(); err != nil {
		t.Fatalf("error: %+v", err)
	}
	createResult := createResp.GetResult()
	volumeHandle := createResult.VolumeInfo.GetHandle()
	// Publish the volume with fstype 'xfs' then unmount it.
	tmpdirPath, err := ioutil.TempDir("", "lvs_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeHandle.GetId())
	if err := os.Mkdir(targetPath, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPath)
	publishReq := testNodePublishVolumeRequest(volumeHandle, targetPath, "xfs", nil)
	publishResp, err := client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	if err := publishResp.GetError(); err != nil {
		t.Fatalf("error: %+v", err)
	}
	publishResult := publishResp.GetResult()
	if publishResult == nil {
		t.Fatal("Expected Result to not be nil.")
	}
	// Unpublish the volume now that it has been formatted.
	unpublishReq := testNodeUnpublishVolumeRequest(volumeHandle, publishReq.TargetPath)
	resp, err := client.NodeUnpublishVolume(context.Background(), unpublishReq)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.GetError(); err != nil {
		t.Fatalf("Error: %+v", err)
	}
	validateReq := testValidateVolumeCapabilitiesRequest(volumeHandle, "xfs", nil)
	validateResp, err := client.ValidateVolumeCapabilities(context.Background(), validateReq)
	if err != nil {
		t.Fatal(err)
	}
	validateResult := validateResp.GetResult()
	if !validateResult.GetSupported() {
		t.Fatal("Expected requested volume capabilities to be supported.")
	}
}

func TestValidateVolumeCapabilities_MountVolume_MismatchedFsTypes(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	if err := createResp.GetError(); err != nil {
		t.Fatalf("error: %+v", err)
	}
	createResult := createResp.GetResult()
	volumeHandle := createResult.VolumeInfo.GetHandle()
	// Publish the volume with fstype 'xfs' then unmount it.
	tmpdirPath, err := ioutil.TempDir("", "lvs_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeHandle.GetId())
	if err := os.Mkdir(targetPath, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPath)
	publishReq := testNodePublishVolumeRequest(volumeHandle, targetPath, "xfs", nil)
	publishResp, err := client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	if err := publishResp.GetError(); err != nil {
		t.Fatalf("error: %+v", err)
	}
	publishResult := publishResp.GetResult()
	if publishResult == nil {
		t.Fatal("Expected Result to not be nil.")
	}
	// Unpublish the volume now that it has been formatted.
	unpublishReq := testNodeUnpublishVolumeRequest(volumeHandle, publishReq.TargetPath)
	resp, err := client.NodeUnpublishVolume(context.Background(), unpublishReq)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.GetError(); err != nil {
		t.Fatalf("Error: %+v", err)
	}
	validateReq := testValidateVolumeCapabilitiesRequest(volumeHandle, "ext4", nil)
	validateResp, err := client.ValidateVolumeCapabilities(context.Background(), validateReq)
	if err != nil {
		t.Fatal(err)
	}
	validateResult := validateResp.GetResult()
	if validateResult.GetSupported() {
		t.Fatal("Expected requested volume capabilities to not be supported.")
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

func testNodePublishVolumeRequest(volumeHandle *csi.VolumeHandle, targetPath string, filesystem string, mountOpts []string) *csi.NodePublishVolumeRequest {
	var volumeCapability *csi.VolumeCapability
	if filesystem == "block" {
		volumeCapability = &csi.VolumeCapability{
			&csi.VolumeCapability_Block{
				&csi.VolumeCapability_BlockVolume{},
			},
			&csi.VolumeCapability_AccessMode{
				csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		}
	} else {
		volumeCapability = &csi.VolumeCapability{
			&csi.VolumeCapability_Mount{
				&csi.VolumeCapability_MountVolume{
					filesystem,
					mountOpts,
				},
			},
			&csi.VolumeCapability_AccessMode{
				csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		}
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

func testNodeUnpublishVolumeRequest(volumeHandle *csi.VolumeHandle, targetPath string) *csi.NodeUnpublishVolumeRequest {
	req := &csi.NodeUnpublishVolumeRequest{
		&csi.Version{0, 1, 0},
		volumeHandle,
		targetPath,
		nil,
	}
	return req
}

func TestNodePublishVolumeNodeUnpublishVolume_BlockVolume(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	if err := createResp.GetError(); err != nil {
		t.Fatalf("error: %+v", err)
	}
	createResult := createResp.GetResult()
	volumeHandle := createResult.VolumeInfo.GetHandle()
	// Prepare a temporary mount directory.
	tmpdirPath, err := ioutil.TempDir("", "lvs_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeHandle.GetId())
	// As we're publishing as a BlockVolume we need to bind mount
	// the device over a file, not a directory.
	if file, err := os.Create(targetPath); err != nil {
		t.Fatal(err)
	} else {
		// Immediately close the file, we're just creating it
		// as a mount target.
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	defer os.Remove(targetPath)
	// Publish the volume to /the/tmp/dir/volume-id
	publishReq := testNodePublishVolumeRequest(volumeHandle, targetPath, "block", nil)
	publishResp, err := client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	if err := publishResp.GetError(); err != nil {
		t.Fatalf("error: %+v", err)
	}
	publishResult := publishResp.GetResult()
	if publishResult == nil {
		t.Fatal("Expected Result to not be nil.")
	}
	// Unpublish the volume when the test ends.
	defer func() {
		req := testNodeUnpublishVolumeRequest(volumeHandle, publishReq.TargetPath)
		resp, err := client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if err := resp.GetError(); err != nil {
			t.Fatalf("Error: %+v", err)
		}
	}()
	// Ensure that the device was mounted.
	buf, err := ioutil.ReadFile("/proc/mounts")
	if err != nil {
		t.Fatal(err)
	}
	had := false
	for _, line := range strings.Split(string(buf), "\n") {
		if strings.Fields(line)[1] == publishReq.TargetPath {
			had = true
			break
		}
	}
	if !had {
		t.Fatalf("Expected volume to be mounted at %v.", publishReq.TargetPath)
	}
}

func TestNodePublishVolumeNodeUnpublishVolume_MountVolume(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	if err := createResp.GetError(); err != nil {
		t.Fatalf("error: %+v", err)
	}
	createResult := createResp.GetResult()
	volumeHandle := createResult.VolumeInfo.GetHandle()
	// Prepare a temporary mount directory.
	tmpdirPath, err := ioutil.TempDir("", "lvs_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeHandle.GetId())
	if err := os.Mkdir(targetPath, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPath)
	// Publish the volume to /the/tmp/dir/volume-id
	publishReq := testNodePublishVolumeRequest(volumeHandle, targetPath, "xfs", nil)
	publishResp, err := client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	if err := publishResp.GetError(); err != nil {
		t.Fatalf("error: %+v", err)
	}
	publishResult := publishResp.GetResult()
	if publishResult == nil {
		t.Fatal("Expected Result to not be nil.")
	}
	// Unpublish the volume when the test ends unless the test
	// called unpublish already.
	alreadyUnpublished := false
	defer func() {
		if alreadyUnpublished {
			return
		}
		req := testNodeUnpublishVolumeRequest(volumeHandle, publishReq.TargetPath)
		resp, err := client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if err := resp.GetError(); err != nil {
			t.Fatalf("Error: %+v", err)
		}
	}()
	// Ensure that the device was mounted.
	if !targetPathIsMountPoint(publishReq.TargetPath) {
		t.Fatalf("Expected volume to be mounted at %v.", publishReq.TargetPath)
	}
	// Create a file on the mounted volume.
	file, err := os.Create(filepath.Join(targetPath, "test"))
	if err != nil {
		t.Fatal(err)
	}
	file.Close()
	// Check that the file exists where it is expected.
	matches, err := filepath.Glob(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("Expected to see only file %v but got %v.", file.Name(), matches)
	}
	if matches[0] != file.Name() {
		t.Fatalf("Expected to see file %v but got %v.", file.Name(), matches[0])
	}
	// Unpublish to check that the file is now missing.
	req := testNodeUnpublishVolumeRequest(volumeHandle, publishReq.TargetPath)
	resp, err := client.NodeUnpublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.GetError(); err != nil {
		t.Fatalf("Error: %+v", err)
	}
	alreadyUnpublished = true
	// Check that the targetPath is now no longer a mountpoint.
	if targetPathIsMountPoint(publishReq.TargetPath) {
		t.Fatalf("Expected target path %v not to be a mountpoint.", publishReq.TargetPath)
	}
	// Check that the file is now missing.
	matches, err = filepath.Glob(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("Expected to see no files but got %v.", matches)
	}
	// Publish again to make sure the file comes back
	publishResp, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	if err := publishResp.GetError(); err != nil {
		t.Fatalf("error: %+v", err)
	}
	publishResult = publishResp.GetResult()
	if publishResult == nil {
		t.Fatal("Expected Result to not be nil.")
	}
	alreadyUnpublished = false
	// Check that the file exists where it is expected.
	matches, err = filepath.Glob(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("Expected to see only file %v but got %v.", file.Name(), matches)
	}
	if matches[0] != file.Name() {
		t.Fatalf("Expected to see file %v but got %v.", file.Name(), matches[0])
	}
}

func TestNodePublishVolumeNodeUnpublishVolume_MountVolume_UnspecifiedFS(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	if err := createResp.GetError(); err != nil {
		t.Fatalf("error: %+v", err)
	}
	createResult := createResp.GetResult()
	volumeHandle := createResult.VolumeInfo.GetHandle()
	// Prepare a temporary mount directory.
	tmpdirPath, err := ioutil.TempDir("", "lvs_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeHandle.GetId())
	if err := os.Mkdir(targetPath, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPath)
	// Publish the volume to /the/tmp/dir/volume-id
	publishReq := testNodePublishVolumeRequest(volumeHandle, targetPath, "", nil)
	publishResp, err := client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	if err := publishResp.GetError(); err != nil {
		t.Fatalf("error: %+v", err)
	}
	publishResult := publishResp.GetResult()
	if publishResult == nil {
		t.Fatal("Expected Result to not be nil.")
	}
	// Unpublish the volume when the test ends unless the test
	// called unpublish already.
	alreadyUnpublished := false
	defer func() {
		if alreadyUnpublished {
			return
		}
		req := testNodeUnpublishVolumeRequest(volumeHandle, publishReq.TargetPath)
		resp, err := client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if err := resp.GetError(); err != nil {
			t.Fatalf("Error: %+v", err)
		}
	}()
	// Ensure that the device was mounted.
	if !targetPathIsMountPoint(publishReq.TargetPath) {
		t.Fatalf("Expected volume to be mounted at %v.", publishReq.TargetPath)
	}
	// Create a file on the mounted volume.
	file, err := os.Create(filepath.Join(targetPath, "test"))
	if err != nil {
		t.Fatal(err)
	}
	file.Close()
	// Check that the file exists where it is expected.
	matches, err := filepath.Glob(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("Expected to see only file %v but got %v.", file.Name(), matches)
	}
	if matches[0] != file.Name() {
		t.Fatalf("Expected to see file %v but got %v.", file.Name(), matches[0])
	}
	// Unpublish to check that the file is now missing.
	req := testNodeUnpublishVolumeRequest(volumeHandle, publishReq.TargetPath)
	resp, err := client.NodeUnpublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.GetError(); err != nil {
		t.Fatalf("Error: %+v", err)
	}
	alreadyUnpublished = true
	// Check that the targetPath is now no longer a mountpoint.
	if targetPathIsMountPoint(publishReq.TargetPath) {
		t.Fatalf("Expected target path %v not to be a mountpoint.", publishReq.TargetPath)
	}
	// Check that the file is now missing.
	matches, err = filepath.Glob(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("Expected to see no files but got %v.", matches)
	}
	// Publish again to make sure the file comes back
	publishResp, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	if err := publishResp.GetError(); err != nil {
		t.Fatalf("error: %+v", err)
	}
	publishResult = publishResp.GetResult()
	if publishResult == nil {
		t.Fatal("Expected Result to not be nil.")
	}
	alreadyUnpublished = false
	// Check that the file exists where it is expected.
	matches, err = filepath.Glob(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("Expected to see only file %v but got %v.", file.Name(), matches)
	}
	if matches[0] != file.Name() {
		t.Fatalf("Expected to see file %v but got %v.", file.Name(), matches[0])
	}
}

func TestNodePublishVolumeNodeUnpublishVolume_MountVolume_ReadOnly(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	if err := createResp.GetError(); err != nil {
		t.Fatalf("error: %+v", err)
	}
	createResult := createResp.GetResult()
	volumeHandle := createResult.VolumeInfo.GetHandle()
	// Prepare a temporary mount directory.
	tmpdirPath, err := ioutil.TempDir("", "lvs_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeHandle.GetId())
	if err := os.Mkdir(targetPath, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPath)
	// Publish the volume.
	publishReq := testNodePublishVolumeRequest(volumeHandle, targetPath, "xfs", nil)
	publishResp, err := client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	if err := publishResp.GetError(); err != nil {
		t.Fatalf("error: %+v", err)
	}
	publishResult := publishResp.GetResult()
	if publishResult == nil {
		t.Fatal("Expected Result to not be nil.")
	}
	// Unpublish the volume when the test ends unless the test
	// called unpublish already.
	alreadyUnpublished := false
	defer func() {
		if alreadyUnpublished {
			return
		}
		req := testNodeUnpublishVolumeRequest(volumeHandle, publishReq.TargetPath)
		resp, err := client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if err := resp.GetError(); err != nil {
			t.Fatalf("Error: %+v", err)
		}
	}()
	// Ensure that the device was mounted.
	if !targetPathIsMountPoint(publishReq.TargetPath) {
		t.Fatalf("Expected volume to be mounted at %v.", publishReq.TargetPath)
	}
	// Create a file on the mounted volume.
	file, err := os.Create(filepath.Join(targetPath, "test"))
	if err != nil {
		t.Fatal(err)
	}
	file.Close()
	// Check that the file exists where it is expected.
	matches, err := filepath.Glob(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("Expected to see only file %v but got %v.", file.Name(), matches)
	}
	if matches[0] != file.Name() {
		t.Fatalf("Expected to see file %v but got %v.", file.Name(), matches[0])
	}
	// Unpublish to check that the file is now missing.
	req := testNodeUnpublishVolumeRequest(volumeHandle, publishReq.TargetPath)
	resp, err := client.NodeUnpublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.GetError(); err != nil {
		t.Fatalf("Error: %+v", err)
	}
	alreadyUnpublished = true
	// Check that the targetPath is now no longer a mountpoint.
	if targetPathIsMountPoint(publishReq.TargetPath) {
		t.Fatalf("Expected target path %v not to be a mountpoint.", publishReq.TargetPath)
	}
	// Check that the file is now missing.
	matches, err = filepath.Glob(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 0 {
		t.Fatalf("Expected to see no files but got %v.", matches)
	}
	// Publish again to make sure the file comes back
	publishResp, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	if err := publishResp.GetError(); err != nil {
		t.Fatalf("error: %+v", err)
	}
	publishResult = publishResp.GetResult()
	if publishResult == nil {
		t.Fatal("Expected Result to not be nil.")
	}
	alreadyUnpublished = false
	// Check that the file exists where it is expected.
	matches, err = filepath.Glob(file.Name())
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("Expected to see only file %v but got %v.", file.Name(), matches)
	}
	if matches[0] != file.Name() {
		t.Fatalf("Expected to see file %v but got %v.", file.Name(), matches[0])
	}
	// Unpublish to volume so that we can republish it as readonly.
	req = testNodeUnpublishVolumeRequest(volumeHandle, publishReq.TargetPath)
	resp, err = client.NodeUnpublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err := resp.GetError(); err != nil {
		t.Fatalf("Error: %+v", err)
	}
	alreadyUnpublished = true
	// Publish the volume again, as SINGLE_NODE_READER_ONLY (ie., readonly).
	targetPathRO := filepath.Join(tmpdirPath, volumeHandle.GetId()+"-ro")
	if err := os.Mkdir(targetPathRO, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPathRO)
	publishReqRO := testNodePublishVolumeRequest(volumeHandle, targetPathRO, "xfs", nil)
	publishReqRO.VolumeCapability.AccessMode.Mode = csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY
	publishRespRO, err := client.NodePublishVolume(context.Background(), publishReqRO)
	if err != nil {
		t.Fatal(err)
	}
	if err := publishRespRO.GetError(); err != nil {
		os.Exit(0)
		t.Fatalf("error: %+v", err)
	}
	publishResultRO := publishRespRO.GetResult()
	if publishResultRO == nil {
		t.Fatal("Expected Result to not be nil.")
	}
	defer func() {
		req := testNodeUnpublishVolumeRequest(volumeHandle, publishReqRO.TargetPath)
		resp, err := client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		if err := resp.GetError(); err != nil {
			t.Fatalf("Error: %+v", err)
		}
	}()
	// Check that the file exists at the new, readonly targetPath.
	roFilepath := filepath.Join(publishReqRO.TargetPath, filepath.Base(file.Name()))
	matches, err = filepath.Glob(roFilepath)
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("Expected to see only file %v but got %v.", file.Name(), matches)
	}
	if matches[0] != roFilepath {
		t.Fatalf("Expected to see file %v but got %v.", roFilepath, matches[0])
	}
	// Check that we cannot create a new file at this location.
	_, err = os.Create(roFilepath + ".2")
	if err.(*os.PathError).Err != syscall.EROFS {
		t.Fatal("Expected file creation to fail due to read-only filesystem.")
	}
}

func targetPathIsMountPoint(path string) bool {
	buf, err := ioutil.ReadFile("/proc/mounts")
	if err != nil {
		panic(err)
	}
	for _, line := range strings.Split(string(buf), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if strings.Fields(line)[1] == path {
			return true
		}
	}
	return false
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
	if result == nil {
		t.Fatalf("Expected result to be present.")
	}
	if result.GetNodeId() != nil {
		t.Fatalf("Expected node_id to be nil.")
	}
}

func testProbeNodeRequest() *csi.ProbeNodeRequest {
	req := &csi.ProbeNodeRequest{
		&csi.Version{0, 1, 0},
	}
	return req
}

func TestProbeNode_NewVolumeGroup_NewPhysicalVolumes(t *testing.T) {
	loop1, err := lvm.CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop1.Close()
	loop2, err := lvm.CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop2.Close()
	pvnames := []string{loop1.Path(), loop2.Path()}
	vgname := "test-vg-" + uuid.New().String()
	client, cleanup := prepareProbeNodeTest(vgname, pvnames)
	defer cleanup()
	probeResp, err := client.ProbeNode(context.Background(), testProbeNodeRequest())
	if err != nil {
		t.Fatal(err)
	}
	if err := probeResp.GetError(); err != nil {
		t.Fatal(err)
	}
}

func TestProbeNode_NewVolumeGroup_NonExistantPhysicalVolume(t *testing.T) {
	pvnames := []string{"/dev/does/not/exist"}
	vgname := "test-vg-" + uuid.New().String()
	client, cleanup := prepareProbeNodeTest(vgname, pvnames)
	defer cleanup()
	probeResp, err := client.ProbeNode(context.Background(), testProbeNodeRequest())
	if err != nil {
		t.Fatal(err)
	}
	grpcErr := probeResp.GetError()
	errorCode := grpcErr.GetProbeNodeError().GetErrorCode()
	errorDesc := grpcErr.GetProbeNodeError().GetErrorDescription()
	expCode := csi.Error_ProbeNodeError_BAD_PLUGIN_CONFIG
	if errorCode != expCode {
		t.Fatalf("Expected error code %v but got %v", expCode, errorCode)
	}
	expDesc := "lvm: CreatePhysicalVolume: Device /dev/does/not/exist not found (or ignored by filtering). (-1)"
	if errorDesc != expDesc {
		t.Fatalf("Expected error description '%v' but got '%v'", expDesc, errorDesc)
	}
}

func TestProbeNode_NewVolumeGroup_BusyPhysicalVolume(t *testing.T) {
	loop1, err := lvm.CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop1.Close()
	loop2, err := lvm.CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop2.Close()
	pvnames := []string{loop1.Path(), loop2.Path()}
	vgname := "test-vg-" + uuid.New().String()
	// Format and mount loop1 so it appears busy.
	if err := formatDevice(loop1.Path(), "xfs"); err != nil {
		t.Fatal(err)
	}
	targetPath, err := ioutil.TempDir("", "lvs_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPath)
	if err := syscall.Mount(loop1.Path(), targetPath, "xfs", 0, ""); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := syscall.Unmount(targetPath, 0); err != nil {
			t.Fatal(err)
		}
	}()
	client, cleanup := prepareProbeNodeTest(vgname, pvnames)
	defer cleanup()
	probeResp, err := client.ProbeNode(context.Background(), testProbeNodeRequest())
	if err != nil {
		t.Fatal(err)
	}
	grpcErr := probeResp.GetError()
	errorCode := grpcErr.GetProbeNodeError().GetErrorCode()
	errorDesc := grpcErr.GetProbeNodeError().GetErrorDescription()
	expCode := csi.Error_ProbeNodeError_BAD_PLUGIN_CONFIG
	if errorCode != expCode {
		t.Fatalf("Expected error code %v but got %v", expCode, errorCode)
	}
	expDesc := "lvm: CreatePhysicalVolume: Can't open /dev/loop20 exclusively.  Mounted filesystem? (-1)"
	if errorDesc != expDesc {
		t.Fatalf("Expected error description '%v' but got '%v'", expDesc, errorDesc)
	}
}

func TestProbeNode_ExistingVolumeGroup(t *testing.T) {
	loop1, err := lvm.CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop1.Close()
	loop2, err := lvm.CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop2.Close()
	pv1, err := lvm.CreatePhysicalVolume(loop1.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer pv1.Remove()
	pv2, err := lvm.CreatePhysicalVolume(loop2.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer pv2.Remove()
	pvs := []*lvm.PhysicalVolume{pv1, pv2}
	vgname := "test-vg-" + uuid.New().String()
	vg, err := lvm.CreateVolumeGroup(vgname, pvs)
	if err != nil {
		panic(err)
	}
	defer vg.Remove()
	pvnames := []string{loop1.Path(), loop2.Path()}
	client, cleanup := prepareProbeNodeTest(vgname, pvnames)
	defer cleanup()
	probeResp, err := client.ProbeNode(context.Background(), testProbeNodeRequest())
	if err != nil {
		t.Fatal(err)
	}
	result := probeResp.GetResult()
	if result == nil {
		t.Fatalf("Expected result to be present.")
	}
}

func TestProbeNode_ExistingVolumeGroup_MissingPhysicalVolume(t *testing.T) {
	loop1, err := lvm.CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop1.Close()
	loop2, err := lvm.CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop2.Close()
	pv1, err := lvm.CreatePhysicalVolume(loop1.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer pv1.Remove()
	pv2, err := lvm.CreatePhysicalVolume(loop2.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer pv2.Remove()
	pvs := []*lvm.PhysicalVolume{pv1, pv2}
	vgname := "test-vg-" + uuid.New().String()
	vg, err := lvm.CreateVolumeGroup(vgname, pvs)
	if err != nil {
		panic(err)
	}
	defer vg.Remove()
	pvnames := []string{loop1.Path(), loop2.Path(), "/dev/missing-device"}
	client, cleanup := prepareProbeNodeTest(vgname, pvnames)
	defer cleanup()
	probeResp, err := client.ProbeNode(context.Background(), testProbeNodeRequest())
	if err != nil {
		t.Fatal(err)
	}
	grpcErr := probeResp.GetError()
	errorCode := grpcErr.GetProbeNodeError().GetErrorCode()
	errorDesc := grpcErr.GetProbeNodeError().GetErrorDescription()
	expCode := csi.Error_ProbeNodeError_BAD_PLUGIN_CONFIG
	if errorCode != expCode {
		t.Fatalf("Expected error code %v but got %v", expCode, errorCode)
	}
	expDesc := "Volume group contains unexpected volumes [] and is missing volumes [/dev/missing-device]"
	if errorDesc != expDesc {
		t.Fatalf("Expected error description '%v' but got '%v'", expDesc, errorDesc)
	}
}

func TestProbeNode_ExistingVolumeGroup_UnexpectedExtraPhysicalVolume(t *testing.T) {
	loop1, err := lvm.CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop1.Close()
	loop2, err := lvm.CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop2.Close()
	pv1, err := lvm.CreatePhysicalVolume(loop1.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer pv1.Remove()
	pv2, err := lvm.CreatePhysicalVolume(loop2.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer pv2.Remove()
	pvs := []*lvm.PhysicalVolume{pv1, pv2}
	vgname := "test-vg-" + uuid.New().String()
	vg, err := lvm.CreateVolumeGroup(vgname, pvs)
	if err != nil {
		panic(err)
	}
	defer vg.Remove()
	pvnames := []string{loop1.Path()}
	client, cleanup := prepareProbeNodeTest(vgname, pvnames)
	defer cleanup()
	probeResp, err := client.ProbeNode(context.Background(), testProbeNodeRequest())
	if err != nil {
		t.Fatal(err)
	}
	grpcErr := probeResp.GetError()
	errorCode := grpcErr.GetProbeNodeError().GetErrorCode()
	errorDesc := grpcErr.GetProbeNodeError().GetErrorDescription()
	expCode := csi.Error_ProbeNodeError_BAD_PLUGIN_CONFIG
	if errorCode != expCode {
		t.Fatalf("Expected error code %v but got %v", expCode, errorCode)
	}
	expDesc := fmt.Sprintf("Volume group contains unexpected volumes %v and is missing volumes []", []string{loop2.Path()})
	if errorDesc != expDesc {
		t.Fatalf("Expected error description '%v' but got '%v'", expDesc, errorDesc)
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
	if result == nil {
		t.Fatalf("Expected result to be present.")
	}
}

func prepareProbeNodeTest(vgname string, pvnames []string) (client *Client, cleanupFn func()) {
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
	s := NewServer(vgname, pvnames, "xfs")
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
	client = NewClient(conn)
	return client, cleanup.Unwind
}

func startTest(serverOpts ...ServerOpt) (client *Client, cleanupFn func()) {
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
	loop, err := lvm.CreateLoopDevice(pvsize)
	if err != nil {
		panic(err)
	}
	cleanup.Add(loop.Close)
	// Create a physical volume using the loop device.
	var pvnames []string
	var pvs []*lvm.PhysicalVolume
	pv, err := lvm.CreatePhysicalVolume(loop.Path())
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() error { return pv.Remove() })
	pvnames = append(pvnames, loop.Path())
	pvs = append(pvs, pv)
	// Create a volume group containing the physical volume.
	vgname := "test-vg-" + uuid.New().String()
	vg, err := lvm.CreateVolumeGroup(vgname, pvs)
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
	s := NewServer(vgname, pvnames, "xfs", serverOpts...)
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
	client = NewClient(conn)
	// Initialize the Server by calling ProbeNode.
	probeResp, err := client.ProbeNode(context.Background(), testProbeNodeRequest())
	if err != nil {
		panic(err)
	}
	if err := probeResp.GetError(); err != nil {
		panic(err)
	}
	return client, cleanup.Unwind
}
