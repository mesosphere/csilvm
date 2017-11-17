package csilvm

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	stdlog "log"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/google/uuid"
	"github.com/mesosphere/csilvm/pkg/cleanup"
	"github.com/mesosphere/csilvm/pkg/lvm"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The size of the physical volumes we create in our tests.
const pvsize = 100 << 20 // 100MiB

var (
	socketFile = flag.String("socket_file", "", "The path to the listening unix socket file")
)

func init() {
	// Set test logging
	stdlog.SetFlags(stdlog.LstdFlags | stdlog.Lshortfile)
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
	if len(resp.GetSupportedVersions()) != 1 {
		t.Fatalf("Expected only one supported version, got %d", len(resp.GetSupportedVersions()))
	}
	got := *resp.GetSupportedVersions()[0]
	exp := csi.Version{0, 1, 0}
	if got != exp {
		t.Fatalf("Expected version %#v but got %#v", exp, got)
	}
}

func TestGetSupportedVersionsRemoveVolumeGroup(t *testing.T) {
	client, cleanup := startTest(RemoveVolumeGroup())
	defer cleanup()
	req := &csi.GetSupportedVersionsRequest{}
	resp, err := client.GetSupportedVersions(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetSupportedVersions()) != 1 {
		t.Fatalf("Expected only one supported version, got %d", len(resp.GetSupportedVersions()))
	}
	got := *resp.GetSupportedVersions()[0]
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
	if resp.GetName() != PluginName {
		t.Fatal("Expected plugin name %s but got %s", PluginName, resp.GetName())
	}
	if resp.GetVendorVersion() != PluginVersion {
		t.Fatal("Expected plugin version %s but got %s", PluginVersion, resp.GetVendorVersion())
	}
	if resp.GetManifest() != nil {
		t.Fatal("Expected a nil manifest but got %s", resp.GetManifest())
	}
}

func TestGetPluginInfoRemoveVolumeGroup(t *testing.T) {
	client, cleanup := startTest(RemoveVolumeGroup())
	defer cleanup()
	req := testGetPluginInfoRequest()
	resp, err := client.GetPluginInfo(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetName() != PluginName {
		t.Fatal("Expected plugin name %s but got %s", PluginName, resp.GetName())
	}
	if resp.GetVendorVersion() != PluginVersion {
		t.Fatal("Expected plugin version %s but got %s", PluginVersion, resp.GetVendorVersion())
	}
	if resp.GetManifest() != nil {
		t.Fatal("Expected a nil manifest but got %s", resp.GetManifest())
	}
}

// ControllerService RPCs

func testControllerProbeRequest() *csi.ControllerProbeRequest {
	req := &csi.ControllerProbeRequest{
		&csi.Version{0, 1, 0},
	}
	return req
}

func TestControllerProbe(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testControllerProbeRequest()
	_, err := client.ControllerProbe(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
}

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

type repeater struct {
	src byte
}

func (r repeater) Read(buf []byte) (int, error) {
	n := copy(buf, bytes.Repeat([]byte{r.src}, len(buf)))
	return n, nil
}

func TestCreateVolume(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	info := resp.GetVolumeInfo()
	if info.GetCapacityBytes() != req.GetCapacityRange().GetRequiredBytes() {
		t.Fatalf("Expected required_bytes (%v) to match volume size (%v).", req.GetCapacityRange().GetRequiredBytes(), info.GetCapacityBytes())
	}
	if !strings.HasSuffix(info.GetId(), req.GetName()) {
		t.Fatalf("Expected volume ID (%v) to name as a suffix (%v).", info.GetId(), req.GetName())
	}
}

func TestCreateVolume_WithTag(t *testing.T) {
	expected := []string{"some-tag"}
	client, cleanup := startTest(Tag(expected[0]))
	defer cleanup()
	req := testCreateVolumeRequest()
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	info := resp.GetVolumeInfo()
	if info.GetCapacityBytes() != req.GetCapacityRange().GetRequiredBytes() {
		t.Fatalf("Expected required_bytes (%v) to match volume size (%v).", req.GetCapacityRange().GetRequiredBytes(), info.GetCapacityBytes())
	}
	if !strings.HasSuffix(info.GetId(), req.GetName()) {
		t.Fatalf("Expected volume ID (%v) to name as a suffix (%v).", info.GetId(), req.GetName())
	}
	vgnames, err := lvm.ListVolumeGroupNames()
	if err != nil {
		panic(err)
	}
	found := false
	for _, vgname := range vgnames {
		vg, err := lvm.LookupVolumeGroup(vgname)
		if err != nil {
			panic(err)
		}
		lv, err := vg.LookupLogicalVolume(info.GetId())
		if err == lvm.ErrLogicalVolumeNotFound {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		found = true
		tags, err := lv.Tags()
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(tags, expected) {
			t.Fatalf("Expected tags not found %v != %v", expected, tags)
		}
	}
	if !found {
		t.Fatal("Could not find created volume.")
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
	info := resp.GetVolumeInfo()
	if info.GetCapacityBytes() != defaultVolumeSize {
		t.Fatalf("Expected defaultVolumeSize (%v) to match volume size (%v).", defaultVolumeSize, info.GetCapacityBytes())
	}
	if !strings.HasSuffix(info.GetId(), req.GetName()) {
		t.Fatalf("Expected volume ID (%v) to name as a suffix (%v).", info.GetId(), req.GetName())
	}
}

func TestCreateVolume_Idempotent(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	// Use only half the usual size so there is enough space for a
	// second volume to be created.
	req.CapacityRange.RequiredBytes /= 2
	resp1, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// Check that trying to create the volume again fails with
	// ErrVolumeAlreadyExists.
	resp2, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	volInfo := resp2.GetVolumeInfo()
	if got := volInfo.GetCapacityBytes(); got != req.CapacityRange.RequiredBytes {
		t.Fatalf("Unexpected capacity_bytes %v != %v", got, req.CapacityRange.RequiredBytes)
	}
	if got := volInfo.GetId(); got != resp1.GetVolumeInfo().GetId() {
		t.Fatalf("Unexpected id %v != %v", got, resp1.GetVolumeInfo().GetId())
	}
	if got := volInfo.GetAttributes(); !reflect.DeepEqual(got, resp1.GetVolumeInfo().GetAttributes()) {
		t.Fatalf("Unexpected attributes %v != %v", got, resp1.GetVolumeInfo().GetAttributes())
	}
}

func TestCreateVolumeUnsupportedCapacityRange(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	// Use only half the usual size so there is enough space for a
	// second volume to be created.
	req.CapacityRange.RequiredBytes = pvsize * 2
	_, err := client.CreateVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrInsufficientCapacity) {
		t.Fatal(err)
	}
}

func TestCreateVolumeInvalidVolumeName(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	// Use only half the usual size so there is enough space for a
	// second volume to be created.

	req.Name = "invalid name : /"
	_, err := client.CreateVolume(context.Background(), req)
	expdesc := "The volume name is invalid: err=lvm: validateLogicalVolumeName: Name contains invalid character, valid set includes: [a-zA-Z0-9.-_+]. (-1)"
	experr := status.Error(codes.InvalidArgument, expdesc)
	if !grpcErrorEqual(err, experr) {
		t.Fatal(err)
	}
}

func testDeleteVolumeRequest(volumeId string) *csi.DeleteVolumeRequest {
	req := &csi.DeleteVolumeRequest{
		&csi.Version{0, 1, 0},
		volumeId,
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
	volumeId := createResp.GetVolumeInfo().GetId()
	req := testDeleteVolumeRequest(volumeId)
	_, err = client.DeleteVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeleteVolume_Idempotent(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolumeInfo().GetId()
	req := testDeleteVolumeRequest(volumeId)
	_, err = client.DeleteVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.DeleteVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrVolumeNotFound) {
		t.Fatal(err)
	}
}

func TestDeleteVolumeUnknownVolume(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testDeleteVolumeRequest("missing-volume")
	_, err := client.DeleteVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrVolumeNotFound) {
		t.Fatal(err)
	}
}

func TestDeleteVolumeErasesData(t *testing.T) {
	loop1, err := lvm.CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop1.Close()
	pvnames := []string{loop1.Path()}
	vgname := "test-vg-" + uuid.New().String()
	client, cleanup := prepareNodeProbeTest(vgname, pvnames)
	defer cleanup()
	_, err = client.NodeProbe(context.Background(), testNodeProbeRequest())
	if err != nil {
		t.Fatal(err)
	}
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolumeInfo().GetId()
	capacityBytes := createResp.GetVolumeInfo().GetCapacityBytes()
	// Prepare a temporary mount directory.
	tmpdirPath, err := ioutil.TempDir("", "csilvm_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeId)
	if err := os.Mkdir(targetPath, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPath)
	// Publish the volume to /the/tmp/dir/volume-id
	publishReq := testNodePublishVolumeRequest(volumeId, targetPath, "xfs", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	// Unpublish the volume when the test ends unless the test
	// called unpublish already.
	alreadyUnpublished := false
	defer func() {
		if alreadyUnpublished {
			return
		}
		req := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
		_, err := client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
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
	// Fill the file with 1's.
	ones := repeater{1}
	wrote, err := io.CopyN(file, ones, int64(capacityBytes))
	if err.(*os.PathError).Err != syscall.ENOSPC {
		t.Fatalf("Expected ENOSPC but got %v", err)
	}
	file.Close()
	// Check that we wrote at least half the volume's capacity full of ones.
	// We can't check for equality due to filesystem metadata, etc.
	if uint64(wrote) < capacityBytes/2 {
		t.Fatalf("Failed to write even half of the volume: %v of %v", wrote, capacityBytes)
	}
	// Unpublish the volume.
	req := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
	_, err = client.NodeUnpublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	alreadyUnpublished = true
	deleteReq := testDeleteVolumeRequest(volumeId)
	_, err = client.DeleteVolume(context.Background(), deleteReq)
	if err != nil {
		t.Fatal(err)
	}
	// Create a new volume and check that it contains only zeros.
	createReq.Name += "-2"
	createResp, err = client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId = createResp.GetVolumeInfo().GetId()
	capacityBytes = createResp.GetVolumeInfo().GetCapacityBytes()
	targetPath = filepath.Join(tmpdirPath, volumeId)
	if file, err := os.Create(targetPath); err != nil {
		t.Fatal(err)
	} else {
		// Immediately close the file, we're just creating it
		// as a mount target.
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}
	defer os.RemoveAll(targetPath)
	publishReq = testNodePublishVolumeRequest(volumeId, targetPath, "block", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		req := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
		_, err := client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
	}()
	// Check that the device is filled with zeros.
	n, ok := containsOnly(targetPath, 0)
	if !ok {
		t.Fatal("Expected device to consists of zeros only.")
	}
	if uint64(n) != capacityBytes {
		t.Fatalf("Bad read, expected device to have size %d but read only %d bytes", capacityBytes, n)
	}
}

func containsOnly(devicePath string, i byte) (uint64, bool) {
	file, err := os.Open(devicePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	br := bufio.NewReader(file)
	idx := uint64(0)
	for {
		b, err := br.ReadByte()
		if err != nil {
			if err == io.EOF {
				break
			}
			panic(err)
		}
		if b != 0 {
			return idx, false
		}
		idx++
	}
	return idx, true
}

func TestControllerPublishVolumeNotSupported(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := &csi.ControllerPublishVolumeRequest{}
	_, err := client.ControllerPublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrCallNotImplemented) {
		t.Fatal(err)
	}
}

func TestControllerUnpublishVolumeNotSupported(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := &csi.ControllerUnpublishVolumeRequest{}
	_, err := client.ControllerUnpublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrCallNotImplemented) {
		t.Fatal(err)
	}
}

func testValidateVolumeCapabilitiesRequest(volumeId string, filesystem string, mountOpts []string) *csi.ValidateVolumeCapabilitiesRequest {
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
		volumeId,
		volumeCapabilities,
		nil,
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
	volumeId := createResp.GetVolumeInfo().GetId()
	req := testValidateVolumeCapabilitiesRequest(volumeId, "xfs", nil)
	resp, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
	if !resp.GetSupported() {
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
	volumeId := createResp.GetVolumeInfo().GetId()
	// Publish the volume with fstype 'xfs' then unmount it.
	tmpdirPath, err := ioutil.TempDir("", "csilvm_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeId)
	if err := os.Mkdir(targetPath, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPath)
	publishReq := testNodePublishVolumeRequest(volumeId, targetPath, "xfs", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	// Unpublish the volume now that it has been formatted.
	unpublishReq := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
	_, err = client.NodeUnpublishVolume(context.Background(), unpublishReq)
	if err != nil {
		t.Fatal(err)
	}
	validateReq := testValidateVolumeCapabilitiesRequest(volumeId, "xfs", nil)
	validateResp, err := client.ValidateVolumeCapabilities(context.Background(), validateReq)
	if err != nil {
		t.Fatal(err)
	}
	if !validateResp.GetSupported() {
		t.Fatal("Expected requested volume capabilities to be supported.")
	}
}

func TestValidateVolumeCapabilities_MissingVolume(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	// Create the volume that we'll be publishing.
	validateReq := testValidateVolumeCapabilitiesRequest("foo", "xfs", nil)
	_, err := client.ValidateVolumeCapabilities(context.Background(), validateReq)
	if !grpcErrorEqual(err, ErrVolumeNotFound) {
		t.Fatal(err)
	}
}

func TestValidateVolumeCapabilities_MountVolume_MismatchedFsTypes(t *testing.T) {
	client, cleanup := startTest(SupportedFilesystem("ext4"))
	defer cleanup()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolumeInfo().GetId()
	// Publish the volume with fstype 'xfs' then unmount it.
	tmpdirPath, err := ioutil.TempDir("", "csilvm_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeId)
	if err := os.Mkdir(targetPath, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPath)
	publishReq := testNodePublishVolumeRequest(volumeId, targetPath, "xfs", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	// Unpublish the volume now that it has been formatted.
	unpublishReq := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
	_, err = client.NodeUnpublishVolume(context.Background(), unpublishReq)
	if err != nil {
		t.Fatal(err)
	}
	validateReq := testValidateVolumeCapabilitiesRequest(volumeId, "ext4", nil)
	_, err = client.ValidateVolumeCapabilities(context.Background(), validateReq)
	if !grpcErrorEqual(err, ErrMismatchedFilesystemType) {
		t.Fatal(err)
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

func TestListVolumes_NoVolumes(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testListVolumesRequest()
	resp, err := client.ListVolumes(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.GetEntries()) != 0 {
		t.Fatal("Expected no entries.")
	}
}

func TestListVolumes_TwoVolumes(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	var infos []*csi.VolumeInfo
	// Add the first volume.
	req := testCreateVolumeRequest()
	req.Name = "test-volume-1"
	req.CapacityRange.RequiredBytes /= 2
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	infos = append(infos, resp.GetVolumeInfo())
	// Add the second volume.
	req = testCreateVolumeRequest()
	req.Name = "test-volume-2"
	req.CapacityRange.RequiredBytes /= 2
	resp, err = client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	infos = append(infos, resp.GetVolumeInfo())
	// Check that ListVolumes returns the two volumes.
	listReq := testListVolumesRequest()
	listResp, err := client.ListVolumes(context.Background(), listReq)
	if err != nil {
		t.Fatal(err)
	}
	entries := listResp.GetEntries()
	if len(entries) != len(infos) {
		t.Fatalf("ListVolumes returned %v entries, expected %d.", len(entries), len(infos))
	}
	for _, entry := range entries {
		had := false
		for _, info := range infos {
			if reflect.DeepEqual(info, entry.GetVolumeInfo()) {
				had = true
				break
			}
		}
		if !had {
			t.Fatalf("Cannot find volume info %+v in %+v.", entry.GetVolumeInfo(), infos)
		}
	}
}

func testGetCapacityRequest(fstype string) *csi.GetCapacityRequest {
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
					fstype,
					nil,
				},
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

func TestGetCapacity_NoVolumes(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetCapacityRequest("xfs")
	resp, err := client.GetCapacity(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// Two extents are reserved for metadata.
	const extentSize = uint64(2 << 20)
	const metadataExtents = 2
	exp := pvsize - extentSize*metadataExtents
	if got := resp.GetAvailableCapacity(); got != exp {
		t.Fatalf("Expected %d bytes free but got %v.", exp, got)
	}
}

func TestGetCapacity_OneVolume(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	createReq := testCreateVolumeRequest()
	createReq.Name = "test-volume-1"
	createReq.CapacityRange.RequiredBytes /= 2
	_, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	req := testGetCapacityRequest("xfs")
	resp, err := client.GetCapacity(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// Two extents are reserved for metadata.
	const extentSize = uint64(2 << 20)
	const metadataExtents = 2
	exp := pvsize - extentSize*metadataExtents - createReq.CapacityRange.RequiredBytes
	if got := resp.GetAvailableCapacity(); got != exp {
		t.Fatalf("Expected %d bytes free but got %v.", exp, got)
	}
}

func TestGetCapacity_RemoveVolumeGroup(t *testing.T) {
	client, cleanup := startTest(RemoveVolumeGroup())
	defer cleanup()
	req := testGetCapacityRequest("xfs")
	resp, err := client.GetCapacity(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.GetAvailableCapacity(); got != 0 {
		t.Fatalf("Expected 0 bytes free but got %v.", got)
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
	expected := []csi.ControllerServiceCapability_RPC_Type{
		csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
		csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
		csi.ControllerServiceCapability_RPC_GET_CAPACITY,
	}
	got := []csi.ControllerServiceCapability_RPC_Type{}
	for _, capability := range resp.GetCapabilities() {
		got = append(got, capability.GetRpc().GetType())
	}
	if !reflect.DeepEqual(expected, got) {
		t.Fatalf("Expected capabilities %+v but got %+v", expected, got)
	}
}

// NodeService RPCs

func testNodePublishVolumeRequest(volumeId string, targetPath string, filesystem string, mountOpts []string) *csi.NodePublishVolumeRequest {
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
		volumeId,
		nil,
		targetPath,
		volumeCapability,
		readonly,
		nil,
		nil,
	}
	return req
}

func testNodeUnpublishVolumeRequest(volumeId string, targetPath string) *csi.NodeUnpublishVolumeRequest {
	req := &csi.NodeUnpublishVolumeRequest{
		&csi.Version{0, 1, 0},
		volumeId,
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
	volumeId := createResp.GetVolumeInfo().GetId()
	// Prepare a temporary mount directory.
	tmpdirPath, err := ioutil.TempDir("", "csilvm_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeId)
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
	publishReq := testNodePublishVolumeRequest(volumeId, targetPath, "block", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	// Unpublish the volume when the test ends.
	defer func() {
		req := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
		_, err := client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
	}()
	mp, err := getMountAt(publishReq.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	if mp == nil {
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
	volumeId := createResp.GetVolumeInfo().GetId()
	// Prepare a temporary mount directory.
	tmpdirPath, err := ioutil.TempDir("", "csilvm_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeId)
	if err := os.Mkdir(targetPath, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPath)
	// Publish the volume to /the/tmp/dir/volume-id
	publishReq := testNodePublishVolumeRequest(volumeId, targetPath, "xfs", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	// Unpublish the volume when the test ends unless the test
	// called unpublish already.
	alreadyUnpublished := false
	defer func() {
		if alreadyUnpublished {
			return
		}
		req := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
		_, err := client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
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
	req := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
	_, err = client.NodeUnpublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
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
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
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
	volumeId := createResp.GetVolumeInfo().GetId()
	// Prepare a temporary mount directory.
	tmpdirPath, err := ioutil.TempDir("", "csilvm_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeId)
	if err := os.Mkdir(targetPath, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPath)
	// Publish the volume to /the/tmp/dir/volume-id
	publishReq := testNodePublishVolumeRequest(volumeId, targetPath, "", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	// Unpublish the volume when the test ends unless the test
	// called unpublish already.
	alreadyUnpublished := false
	defer func() {
		if alreadyUnpublished {
			return
		}
		req := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
		_, err := client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
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
	req := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
	_, err = client.NodeUnpublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
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
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
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
	volumeId := createResp.GetVolumeInfo().GetId()
	// Prepare a temporary mount directory.
	tmpdirPath, err := ioutil.TempDir("", "csilvm_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeId)
	if err := os.Mkdir(targetPath, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPath)
	// Publish the volume.
	publishReq := testNodePublishVolumeRequest(volumeId, targetPath, "xfs", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	// Unpublish the volume when the test ends unless the test
	// called unpublish already.
	alreadyUnpublished := false
	defer func() {
		if alreadyUnpublished {
			return
		}
		req := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
		_, err := client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
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
	req := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
	_, err = client.NodeUnpublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
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
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
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
	req = testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
	_, err = client.NodeUnpublishVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	alreadyUnpublished = true
	// Publish the volume again, as SINGLE_NODE_READER_ONLY (ie., readonly).
	targetPathRO := filepath.Join(tmpdirPath, volumeId+"-ro")
	if err := os.Mkdir(targetPathRO, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPathRO)
	publishReqRO := testNodePublishVolumeRequest(volumeId, targetPathRO, "xfs", nil)
	publishReqRO.VolumeCapability.AccessMode.Mode = csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY
	_, err = client.NodePublishVolume(context.Background(), publishReqRO)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		req := testNodeUnpublishVolumeRequest(volumeId, publishReqRO.TargetPath)
		_, err := client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
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

func TestNodePublishVolume_BlockVolume_Idempotent(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolumeInfo().GetId()
	// Prepare a temporary mount directory.
	tmpdirPath, err := ioutil.TempDir("", "csilvm_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeId)
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
	publishReq := testNodePublishVolumeRequest(volumeId, targetPath, "block", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	// Unpublish the volume when the test ends.
	defer func() {
		req := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
		_, err := client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
	}()
	// Check that calling NodePublishVolume with the same
	// parameters succeeds and doesn't mount anything new at
	// targetPath.
	mountsBefore, err := getMountsAt(publishReq.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	publishReq = testNodePublishVolumeRequest(volumeId, targetPath, "block", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	mountsAfter, err := getMountsAt(publishReq.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(mountsBefore, mountsAfter) {
		t.Fatal("Expected idempotent publish to not mount anything new at targetPath.")
	}
}

func TestNodePublishVolume_BlockVolume_TargetPathOccupied(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	// Create the volume that we'll be publishing.
	createReq1 := testCreateVolumeRequest()
	createReq1.CapacityRange.RequiredBytes /= 2
	createResp1, err := client.CreateVolume(context.Background(), createReq1)
	if err != nil {
		t.Fatal(err)
	}
	volumeId1 := createResp1.GetVolumeInfo().GetId()
	// Create a second volume.
	createReq2 := testCreateVolumeRequest()
	createReq2.Name += "-2"
	createReq2.CapacityRange.RequiredBytes /= 2
	createResp2, err := client.CreateVolume(context.Background(), createReq2)
	if err != nil {
		t.Fatal(err)
	}
	volumeId2 := createResp2.GetVolumeInfo().GetId()
	// Prepare a temporary mount directory.
	tmpdirPath, err := ioutil.TempDir("", "csilvm_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeId1)
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
	publishReq1 := testNodePublishVolumeRequest(volumeId1, targetPath, "block", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq1)
	if err != nil {
		t.Fatal(err)
	}
	// Unpublish the volume when the test ends.
	defer func() {
		req := testNodeUnpublishVolumeRequest(volumeId1, publishReq1.TargetPath)
		_, err := client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
	}()
	// Check that mounting the second volume at the same target path will fail.
	publishReq2 := testNodePublishVolumeRequest(volumeId2, targetPath, "block", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq2)
	if !grpcErrorEqual(err, ErrTargetPathNotEmpty) {
		t.Fatal(err)
	}
}

func TestNodePublishVolume_MountVolume_Idempotent(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolumeInfo().GetId()
	// Prepare a temporary mount directory.
	tmpdirPath, err := ioutil.TempDir("", "csilvm_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeId)
	if err := os.Mkdir(targetPath, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPath)
	// Publish the volume to /the/tmp/dir/volume-id
	publishReq := testNodePublishVolumeRequest(volumeId, targetPath, "xfs", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	// Unpublish the volume when the test ends unless the test
	// called unpublish already.
	alreadyUnpublished := false
	defer func() {
		if alreadyUnpublished {
			return
		}
		req := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
		_, err := client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
	}()
	// Ensure that the device was mounted.
	if !targetPathIsMountPoint(publishReq.TargetPath) {
		t.Fatalf("Expected volume to be mounted at %v.", publishReq.TargetPath)
	}
	// Check that calling NodePublishVolume with the same
	// parameters succeeds and doesn't mount anything new at
	// targetPath.
	mountsBefore, err := getMountsAt(publishReq.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	publishReq = testNodePublishVolumeRequest(volumeId, targetPath, "xfs", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	mountsAfter, err := getMountsAt(publishReq.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(mountsBefore, mountsAfter) {
		t.Fatal("Expected idempotent publish to not mount anything new at targetPath.")
	}
}

func TestNodePublishVolume_MountVolume_TargetPathOccupied(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	// Create the volume that we'll be publishing.
	createReq1 := testCreateVolumeRequest()
	createReq1.CapacityRange.RequiredBytes /= 2
	createResp1, err := client.CreateVolume(context.Background(), createReq1)
	if err != nil {
		t.Fatal(err)
	}
	volumeId1 := createResp1.GetVolumeInfo().GetId()
	// Create a second volume that we'll try to publish to the same targetPath.
	createReq2 := testCreateVolumeRequest()
	createReq2.Name += "-2"
	createReq2.CapacityRange.RequiredBytes /= 2
	createResp2, err := client.CreateVolume(context.Background(), createReq2)
	if err != nil {
		t.Fatal(err)
	}
	volumeId2 := createResp2.GetVolumeInfo().GetId()
	// Prepare a temporary mount directory.
	tmpdirPath, err := ioutil.TempDir("", "csilvm_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeId1)
	if err := os.Mkdir(targetPath, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPath)
	// Publish the volume to /the/tmp/dir/volume-id
	publishReq1 := testNodePublishVolumeRequest(volumeId1, targetPath, "xfs", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		req := testNodeUnpublishVolumeRequest(volumeId1, publishReq1.TargetPath)
		_, err := client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
	}()
	// Ensure that the device was mounted.
	if !targetPathIsMountPoint(publishReq1.TargetPath) {
		t.Fatalf("Expected volume to be mounted at %v.", publishReq1.TargetPath)
	}
	publishReq2 := testNodePublishVolumeRequest(volumeId2, targetPath, "xfs", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq2)
	if err == nil {
		req := testNodeUnpublishVolumeRequest(volumeId2, publishReq2.TargetPath)
		_, err := client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		t.Fatal("Expected operation to fail")
	}
}

func TestNodeUnpublishVolume_BlockVolume_Idempotent(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolumeInfo().GetId()
	// Prepare a temporary mount directory.
	tmpdirPath, err := ioutil.TempDir("", "csilvm_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeId)
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
	publishReq := testNodePublishVolumeRequest(volumeId, targetPath, "block", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	// Unpublish the volume when the test ends.
	alreadyUnpublished := false
	defer func() {
		if alreadyUnpublished {
			return
		}
		req := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
		_, err := client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
	}()
	// Unpublish the volume.
	unpublishReq := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
	_, err = client.NodeUnpublishVolume(context.Background(), unpublishReq)
	if err != nil {
		t.Fatal(err)
	}
	alreadyUnpublished = true
	// Check that calling NodeUnpublishVolume with the same
	// parameters succeeds and doesn't modify the mounts at
	// targetPath.
	mountsBefore, err := getMountsAt(publishReq.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	// Unpublish the volume again to check that it is idempotent.
	unpublishReq = testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
	_, err = client.NodeUnpublishVolume(context.Background(), unpublishReq)
	if err != nil {
		t.Fatal(err)
	}
	mountsAfter, err := getMountsAt(publishReq.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(mountsBefore, mountsAfter) {
		t.Fatal("Expected idempotent unpublish to not modify mountpoints at targetPath.")
	}
}

func TestNodeUnpublishVolume_MountVolume_Idempotent(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolumeInfo().GetId()
	// Prepare a temporary mount directory.
	tmpdirPath, err := ioutil.TempDir("", "csilvm_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpdirPath)
	targetPath := filepath.Join(tmpdirPath, volumeId)
	if err := os.Mkdir(targetPath, 0755); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPath)
	// Publish the volume to /the/tmp/dir/volume-id
	publishReq := testNodePublishVolumeRequest(volumeId, targetPath, "xfs", nil)
	_, err = client.NodePublishVolume(context.Background(), publishReq)
	if err != nil {
		t.Fatal(err)
	}
	// Unpublish the volume when the test ends unless the test
	// called unpublish already.
	alreadyUnpublished := false
	defer func() {
		if alreadyUnpublished {
			return
		}
		req := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
		_, err := client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
	}()
	// Ensure that the device was mounted.
	if !targetPathIsMountPoint(publishReq.TargetPath) {
		t.Fatalf("Expected volume to be mounted at %v.", publishReq.TargetPath)
	}
	// Unpublish the volume.
	unpublishReq := testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
	_, err = client.NodeUnpublishVolume(context.Background(), unpublishReq)
	if err != nil {
		t.Fatal(err)
	}
	alreadyUnpublished = true
	// Unpublish the volume again to check that it is idempotent.
	mountsBefore, err := getMountsAt(publishReq.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	unpublishReq = testNodeUnpublishVolumeRequest(volumeId, publishReq.TargetPath)
	_, err = client.NodeUnpublishVolume(context.Background(), unpublishReq)
	if err != nil {
		t.Fatal(err)
	}
	mountsAfter, err := getMountsAt(publishReq.TargetPath)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(mountsBefore, mountsAfter) {
		t.Fatal("Expected idempotent unpublish to not modify mountpoints at targetPath.")
	}
}

func targetPathIsMountPoint(path string) bool {
	mp, err := getMountAt(path)
	if err != nil {
		panic(err)
	}
	if mp == nil {
		return false
	}
	return true
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
	if resp.GetNodeId() != "" {
		t.Fatalf("Expected node_id to be ''.")
	}
}

func testNodeProbeRequest() *csi.NodeProbeRequest {
	req := &csi.NodeProbeRequest{
		&csi.Version{0, 1, 0},
	}
	return req
}

func TestNodeProbe_NewVolumeGroup_NewPhysicalVolumes(t *testing.T) {
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
	client, cleanup := prepareNodeProbeTest(vgname, pvnames)
	defer cleanup()
	_, err = client.NodeProbe(context.Background(), testNodeProbeRequest())
	if err != nil {
		t.Fatal(err)
	}
}

func TestNodeProbe_NewVolumeGroup_NewPhysicalVolumes_WithTag(t *testing.T) {
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
	tag := "blue"
	expected := []string{tag}
	client, cleanup := prepareNodeProbeTest(vgname, pvnames, Tag(tag))
	defer cleanup()
	_, err = client.NodeProbe(context.Background(), testNodeProbeRequest())
	if err != nil {
		t.Fatal(err)
	}
	vg, err := lvm.LookupVolumeGroup(vgname)
	if err != nil {
		t.Fatal(err)
	}
	tags, err := vg.Tags()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(tags, expected) {
		t.Fatalf("Expected tags not found %v != %v", expected, tags)
	}
}

func TestNodeProbe_NewVolumeGroup_NewPhysicalVolumes_WithMalformedTag(t *testing.T) {
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
	vg, err := lvm.CreateVolumeGroup(vgname, pvs, nil)
	if err != nil {
		panic(err)
	}
	defer vg.Remove()
	pvnames := []string{loop1.Path(), loop2.Path()}
	tag := "-some-malformed-tag"
	client, cleanup := prepareNodeProbeTest(vgname, pvnames, Tag(tag))
	defer cleanup()
	_, err = client.NodeProbe(context.Background(), testNodeProbeRequest())
	experr := status.Errorf(
		codes.FailedPrecondition,
		"Invalid tag '%v': err=%v",
		"lvm: tag must consist of only [A-Za-z0-9_+.-] and cannot start with a '-'")
	if !grpcErrorEqual(err, experr) {
		t.Fatal(err)
	}
}

func TestNodeProbe_NewVolumeGroup_NonExistantPhysicalVolume(t *testing.T) {
	pvnames := []string{"/dev/does/not/exist"}
	vgname := "test-vg-" + uuid.New().String()
	client, cleanup := prepareNodeProbeTest(vgname, pvnames)
	defer cleanup()
	_, err := client.NodeProbe(context.Background(), testNodeProbeRequest())
	experr := status.Error(
		codes.FailedPrecondition,
		"Could not stat device /dev/does/not/exist: err=stat /dev/does/not/exist: no such file or directory")
	if !grpcErrorEqual(err, experr) {
		t.Fatal(err)
	}
}

func TestNodeProbe_NewVolumeGroup_BusyPhysicalVolume(t *testing.T) {
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
	targetPath, err := ioutil.TempDir("", "csilvm_tests")
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
	client, cleanup := prepareNodeProbeTest(vgname, pvnames)
	defer cleanup()
	_, err = client.NodeProbe(context.Background(), testNodeProbeRequest())
	experr := status.Errorf(
		codes.FailedPrecondition,
		"Cannot create LVM2 physical volume %s: err=lvm: CreatePhysicalVolume: Can't open %s exclusively.  Mounted filesystem? (-1)",
		loop1.Path(),
		loop1.Path())
	if !grpcErrorEqual(err, experr) {
		t.Fatal(err)
	}
}

func TestNodeProbe_NewVolumeGroup_FormattedPhysicalVolume(t *testing.T) {
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
	if err := exec.Command("mkfs", "-t", "xfs", loop2.Path()).Run(); err != nil {
		t.Fatal(err)
	}
	pvnames := []string{loop1.Path(), loop2.Path()}
	vgname := "test-vg-" + uuid.New().String()
	client, cleanup := prepareNodeProbeTest(vgname, pvnames)
	defer cleanup()
	_, err = client.NodeProbe(context.Background(), testNodeProbeRequest())
	if err != nil {
		t.Fatal(err)
	}
}

func readPartitionTable(devicePath string) []byte {
	file, err := os.Open(devicePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()
	buf := make([]byte, 512)
	if _, err := io.ReadFull(file, buf); err != nil {
		panic(err)
	}
	return buf
}

func TestZeroPartitionTable(t *testing.T) {
	loop1, err := lvm.CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop1.Close()
	if err := exec.Command("mkfs", "-t", "xfs", loop1.Path()).Run(); err != nil {
		t.Fatal(err)
	}
	zerosector := bytes.Repeat([]byte{0}, 512)
	before := readPartitionTable(loop1.Path())
	if reflect.DeepEqual(before, zerosector) {
		t.Fatal("Expected non-zero partition table.")
	}
	if err := zeroPartitionTable(loop1.Path()); err != nil {
		t.Fatal(err)
	}
	after := readPartitionTable(loop1.Path())
	if !reflect.DeepEqual(after, zerosector) {
		t.Fatal("Expected zeroed partition table.")
	}
}

func TestNodeProbe_NewVolumeGroup_NewPhysicalVolumes_RemoveVolumeGroup(t *testing.T) {
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
	client, cleanup := prepareNodeProbeTest(vgname, pvnames, RemoveVolumeGroup())
	defer cleanup()
	_, err = client.NodeProbe(context.Background(), testNodeProbeRequest())
	if err != nil {
		t.Fatal(err)
	}
}

func TestNodeProbe_ExistingVolumeGroup(t *testing.T) {
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
	vg, err := lvm.CreateVolumeGroup(vgname, pvs, nil)
	if err != nil {
		panic(err)
	}
	defer vg.Remove()
	pvnames := []string{loop1.Path(), loop2.Path()}
	client, cleanup := prepareNodeProbeTest(vgname, pvnames)
	defer cleanup()
	_, err = client.NodeProbe(context.Background(), testNodeProbeRequest())
	if err != nil {
		t.Fatal(err)
	}
}

func TestNodeProbe_ExistingVolumeGroup_MissingPhysicalVolume(t *testing.T) {
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
	vg, err := lvm.CreateVolumeGroup(vgname, pvs, nil)
	if err != nil {
		panic(err)
	}
	defer vg.Remove()
	pvnames := []string{loop1.Path(), loop2.Path(), "/dev/missing-device"}
	client, cleanup := prepareNodeProbeTest(vgname, pvnames)
	defer cleanup()
	_, err = client.NodeProbe(context.Background(), testNodeProbeRequest())
	experr := status.Error(
		codes.FailedPrecondition,
		"Volume group contains unexpected volumes [] and is missing volumes [/dev/missing-device]")
	if !grpcErrorEqual(err, experr) {
		t.Fatal(err)
	}
}

func TestNodeProbe_ExistingVolumeGroup_UnexpectedExtraPhysicalVolume(t *testing.T) {
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
	vg, err := lvm.CreateVolumeGroup(vgname, pvs, nil)
	if err != nil {
		panic(err)
	}
	defer vg.Remove()
	pvnames := []string{loop1.Path()}
	client, cleanup := prepareNodeProbeTest(vgname, pvnames)
	defer cleanup()
	_, err = client.NodeProbe(context.Background(), testNodeProbeRequest())
	experr := status.Errorf(
		codes.FailedPrecondition,
		"Volume group contains unexpected volumes %v and is missing volumes []",
		[]string{loop2.Path()})
	if !grpcErrorEqual(err, experr) {
		t.Fatal(err)
	}
}

func TestNodeProbe_ExistingVolumeGroup_RemoveVolumeGroup(t *testing.T) {
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
	vg, err := lvm.CreateVolumeGroup(vgname, pvs, nil)
	if err != nil {
		panic(err)
	}
	defer vg.Remove()
	pvnames := []string{loop1.Path(), loop2.Path()}
	client, cleanup := prepareNodeProbeTest(vgname, pvnames, RemoveVolumeGroup())
	defer cleanup()
	vgnamesBefore, err := lvm.ListVolumeGroupNames()
	if err != nil {
		t.Fatal(err)
	}
	var vgnamesExpect []string
	for _, name := range vgnamesBefore {
		if name == vgname {
			continue
		}
		vgnamesExpect = append(vgnamesExpect, name)
	}
	_, err = client.NodeProbe(context.Background(), testNodeProbeRequest())
	if err != nil {
		t.Fatal(err)
	}
	vgnamesAfter, err := lvm.ListVolumeGroupNames()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(vgnamesExpect, vgnamesAfter) {
		t.Fatalf("Expected volume groups %v after NodeProbe but got %v", vgnamesExpect, vgnamesAfter)
	}
}

func TestNodeProbe_ExistingVolumeGroup_UnexpectedExtraPhysicalVolume_RemoveVolumeGroup(t *testing.T) {
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
	vg, err := lvm.CreateVolumeGroup(vgname, pvs, nil)
	if err != nil {
		panic(err)
	}
	defer vg.Remove()
	pvnames := []string{loop1.Path()}
	client, cleanup := prepareNodeProbeTest(vgname, pvnames, RemoveVolumeGroup())
	defer cleanup()
	_, err = client.NodeProbe(context.Background(), testNodeProbeRequest())
	experr := status.Errorf(
		codes.FailedPrecondition,
		"Volume group contains unexpected volumes %v and is missing volumes []",
		[]string{loop2.Path()})
	if !grpcErrorEqual(err, experr) {
		t.Fatal(err)
	}
}

func TestProbeNode_ExistingVolumeGroup_WithTag(t *testing.T) {
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
	tags := []string{"blue", "foo"}
	vg, err := lvm.CreateVolumeGroup(vgname, pvs, tags)
	if err != nil {
		panic(err)
	}
	defer vg.Remove()
	pvnames := []string{loop1.Path(), loop2.Path()}
	client, cleanup := prepareNodeProbeTest(vgname, pvnames, Tag(tags[0]), Tag(tags[1]))
	defer cleanup()
	_, err = client.NodeProbe(context.Background(), testNodeProbeRequest())
	if err != nil {
		t.Fatal(err)
	}
}

func TestProbeNode_ExistingVolumeGroup_UnexpectedTag(t *testing.T) {
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
	tag := "blue"
	vg, err := lvm.CreateVolumeGroup(vgname, pvs, []string{tag})
	if err != nil {
		panic(err)
	}
	defer vg.Remove()
	pvnames := []string{loop1.Path(), loop2.Path()}
	client, cleanup := prepareNodeProbeTest(vgname, pvnames)
	defer cleanup()
	_, err = client.NodeProbe(context.Background(), testNodeProbeRequest())
	experr := status.Errorf(
		codes.FailedPrecondition,
		"Volume group tags did not match expected: err=csilvm: Configured tags don't match existing tags: [] != [%s]",
		tag)
	if !grpcErrorEqual(err, experr) {
		t.Fatal(err)
	}
}

func TestProbeNode_ExistingVolumeGroup_MissingTag(t *testing.T) {
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
	vg, err := lvm.CreateVolumeGroup(vgname, pvs, []string{"some-other-tag"})
	if err != nil {
		panic(err)
	}
	defer vg.Remove()
	pvnames := []string{loop1.Path(), loop2.Path()}
	tag := "blue"
	client, cleanup := prepareNodeProbeTest(vgname, pvnames, Tag(tag))
	defer cleanup()
	_, err = client.NodeProbe(context.Background(), testNodeProbeRequest())
	experr := status.Error(
		codes.FailedPrecondition,
		"Volume group tags did not match expected: err=csilvm: Configured tags don't match existing tags: [blue] != [some-other-tag]")
	if !grpcErrorEqual(err, experr) {
		t.Fatal(err)
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
	_, err := client.NodeGetCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
}

func prepareNodeProbeTest(vgname string, pvnames []string, serverOpts ...ServerOpt) (client *Client, cleanupFn func()) {
	var cleanup cleanup.Steps
	defer func() {
		if x := recover(); x != nil {
			cleanup.Unwind()
			panic(x)
		}
	}()
	lis, err := net.Listen("unix", "@/csilvm-test-"+uuid.New().String())
	if err != nil {
		panic(err)
	}
	cleanup.Add(lis.Close)
	cleanup.Add(func() error {
		for _, pvname := range pvnames {
			pv, err := lvm.LookupPhysicalVolume(pvname)
			if err != nil {
				if err == lvm.ErrPhysicalVolumeNotFound {
					continue
				}
				panic(err)
			}
			if err := pv.Remove(); err != nil {
				panic(err)
			}
		}
		return nil
	})
	cleanup.Add(func() error {
		vg, err := lvm.LookupVolumeGroup(vgname)
		if err == lvm.ErrVolumeGroupNotFound {
			// Already removed this volume group in the test.
			return nil
		}
		if err != nil {
			panic(err)
		}
		return vg.Remove()
	})
	cleanup.Add(func() error {
		vg, err := lvm.LookupVolumeGroup(vgname)
		if err == lvm.ErrVolumeGroupNotFound {
			// Already removed this volume group in the test.
			return nil
		}
		if err != nil {
			panic(err)
		}
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
	// setup logging
	logprefix := fmt.Sprintf("[%s]", vgname)
	logflags := stdlog.LstdFlags | stdlog.Lshortfile
	SetLogger(stdlog.New(os.Stderr, logprefix, logflags))
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
	return client, cleanup.Unwind
}

func startTest(serverOpts ...ServerOpt) (client *Client, cleanupFn func()) {
	var cleanup cleanup.Steps
	defer func() {
		if x := recover(); x != nil {
			cleanup.Unwind()
			panic(x)
		}
	}()
	// Create a volume group for the server to manage.
	loop, err := lvm.CreateLoopDevice(pvsize)
	if err != nil {
		panic(err)
	}
	cleanup.Add(loop.Close)
	// Create a volume group containing the physical volume.
	vgname := "test-vg-" + uuid.New().String()
	client, cleanup2 := prepareNodeProbeTest(vgname, []string{loop.Path()}, serverOpts...)
	// Initialize the Server by calling ProbeNode.
	_, err = client.NodeProbe(context.Background(), testNodeProbeRequest())
	if err != nil {
		panic(err)
	}
	cleanup.Add(func() error { cleanup2(); return nil })
	return client, cleanup.Unwind
}
