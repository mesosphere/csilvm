// +build !unit

package csilvm

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
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
	"sort"
	"strings"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
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

func testGetPluginInfoRequest() *csi.GetPluginInfoRequest {
	req := &csi.GetPluginInfoRequest{}
	return req
}

const pluginName = "io.mesosphere.csi.lvm"

func TestGetPluginInfo(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	req := testGetPluginInfoRequest()
	resp, err := client.GetPluginInfo(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// This shouldn't really change, ever, unless someone is doing a specialized build. In which case
	// this test isn't very useful anyway.
	if resp.GetName() != pluginName {
		t.Fatalf("Expected plugin name %s but got %s", pluginName, resp.GetName())
	}
	// Plugin version and manifest metadata are volatile (build-time dependent).
}

func TestGetPluginInfoRemoveVolumeGroup(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname}, RemoveVolumeGroup())
	defer clean()
	req := testGetPluginInfoRequest()
	resp, err := client.GetPluginInfo(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// This shouldn't really change, ever, unless someone is doing a specialized build. In which case
	// this test isn't very useful anyway.
	if resp.GetName() != pluginName {
		t.Fatalf("Expected plugin name %s but got %s", pluginName, resp.GetName())
	}
	// Plugin version and manifest metadata are volatile (build-time dependent).
}

func testGetPluginCapabilitiesRequest() *csi.GetPluginCapabilitiesRequest {
	req := &csi.GetPluginCapabilitiesRequest{}
	return req
}

func TestGetPluginCapabilities(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	req := testGetPluginCapabilitiesRequest()
	resp, err := client.GetPluginCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if x := resp.GetCapabilities(); len(x) != 1 {
		t.Fatalf("Expected 1 capability, but got %v", x)
	}
	if x := resp.GetCapabilities()[0].GetService().Type; x != csi.PluginCapability_Service_CONTROLLER_SERVICE {
		t.Fatalf("Expected plugin to have capability CONTROLLER_SERVICE but had %v", x)
	}
}

func TestGetPluginCapabilitiesRemoveVolumeGroup(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname}, RemoveVolumeGroup())
	defer clean()
	req := testGetPluginCapabilitiesRequest()
	resp, err := client.GetPluginCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if x := resp.GetCapabilities(); len(x) != 1 {
		t.Fatalf("Expected 1 capability, but got %v", x)
	}
	if x := resp.GetCapabilities()[0].GetService().Type; x != csi.PluginCapability_Service_CONTROLLER_SERVICE {
		t.Fatalf("Expected plugin to have capability CONTROLLER_SERVICE but had %v", x)
	}
}

// ControllerService RPCs

func testCreateVolumeRequest() *csi.CreateVolumeRequest {
	const requiredBytes = 80 << 20
	const limitBytes = 1000 << 20
	volumeCapabilities := []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Block{
				&csi.VolumeCapability_BlockVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
		{
			AccessType: &csi.VolumeCapability_Mount{
				&csi.VolumeCapability_MountVolume{
					FsType:     "xfs",
					MountFlags: nil,
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	req := &csi.CreateVolumeRequest{
		Name:               "test-volume",
		CapacityRange:      &csi.CapacityRange{RequiredBytes: requiredBytes, LimitBytes: limitBytes},
		VolumeCapabilities: volumeCapabilities,
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
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	req := testCreateVolumeRequest()
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	info := resp.GetVolume()
	if info.GetCapacityBytes() != req.GetCapacityRange().GetRequiredBytes() {
		t.Fatalf("Expected required_bytes (%v) to match volume size (%v).", req.GetCapacityRange().GetRequiredBytes(), info.GetCapacityBytes())
	}
	checkAttributesIncludeVolumeTag(t, info, req.GetName())
}

func checkAttributesIncludeVolumeTag(t *testing.T, info *csi.Volume, name string) {
	attr := info.GetAttributes()
	tags := tagsFromAttributes(t, attr)
	var found bool
	for _, tag := range tags {
		found = (tag == (tagVolumeNamePlainPrefix + "test-volume"))
		if found {
			break
		}
	}
	if !found {
		t.Fatalf("Expected volume ID (%v) to have a tag with the suffix (%v).", info.GetId(), name)
	}
}

func TestCreateVolume_WithTag(t *testing.T) {
	expected := []string{"some-tag", tagVolumeNamePlainPrefix + "test-volume"}
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname}, Tag(expected[0]))
	defer clean()
	req := testCreateVolumeRequest()
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	info := resp.GetVolume()
	if info.GetCapacityBytes() != req.GetCapacityRange().GetRequiredBytes() {
		t.Fatalf("Expected required_bytes (%v) to match volume size (%v).", req.GetCapacityRange().GetRequiredBytes(), info.GetCapacityBytes())
	}
	checkAttributesIncludeVolumeTag(t, info, req.GetName())
	vgnames, err := lvm.ListVolumeGroupNames()
	if err != nil {
		panic(err)
	}
	sort.Strings(expected)
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
		sort.Strings(tags)
		if !reflect.DeepEqual(tags, expected) {
			t.Fatalf("Expected tags not found %v != %v", expected, tags)
		}
	}
	if !found {
		t.Fatal("Could not find created volume.")
	}
}

func TestCreateVolumeDefaultSize(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	const defaultVolumeSize = uint64(20 << 20)
	client, clean := startTest(vgname, []string{pvname}, DefaultVolumeSize(defaultVolumeSize))
	defer clean()
	req := testCreateVolumeRequest()
	// Specify no CapacityRange so the volume gets the default
	// size.
	req.CapacityRange = nil
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	info := resp.GetVolume()
	if uint64(info.GetCapacityBytes()) != defaultVolumeSize {
		t.Fatalf("Expected defaultVolumeSize (%v) to match volume size (%v).", defaultVolumeSize, info.GetCapacityBytes())
	}
	checkAttributesIncludeVolumeTag(t, info, req.GetName())
}

func TestCreateVolume_Idempotent(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	req := testCreateVolumeRequest()
	// Use only half the usual size so there is enough space for a
	// second volume to be created.
	req.CapacityRange.RequiredBytes /= 2
	resp1, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// Check that trying to create the exact same volume again succeeds.
	resp2, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	volInfo := resp2.GetVolume()
	if got := volInfo.GetCapacityBytes(); got != req.CapacityRange.RequiredBytes {
		t.Fatalf("Unexpected capacity_bytes %v != %v", got, req.CapacityRange.RequiredBytes)
	}
	if got := volInfo.GetId(); got != resp1.GetVolume().GetId() {
		t.Fatalf("Unexpected id %v != %v", got, resp1.GetVolume().GetId())
	}
	if got := volInfo.GetAttributes(); !reflect.DeepEqual(got, resp1.GetVolume().GetAttributes()) {
		t.Fatalf("Unexpected attributes %v != %v", got, resp1.GetVolume().GetAttributes())
	}
}

func TestCreateVolume_AlreadyExists_CapacityRange(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	req := testCreateVolumeRequest()
	// Use only half the usual size so there is enough space for a
	// second volume to be created.
	req.CapacityRange.RequiredBytes /= 2
	_, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// Check that trying to create a volume with the same name but
	// incompatible capacity_range fails.
	req.CapacityRange.RequiredBytes += 1
	_, err = client.CreateVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrVolumeAlreadyExists) {
		t.Fatal(err)
	}
}

func TestCreateVolume_AlreadyExists_VolumeCapabilities(t *testing.T) {
	// Prepare a test server with a known volume group name.
	var clean cleanup.Steps
	defer func() {
		if x := recover(); x != nil {
			clean.Unwind()
			panic(x)
		}
	}()
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, cleanup := startTest(vgname, []string{pvname}, SupportedFilesystem("ext4"))
	defer cleanup()
	// Create a test volume.
	req := testCreateVolumeRequest()
	// Use only half the usual size so there is enough space for a
	// second volume to be created.
	req.CapacityRange.RequiredBytes /= 2
	resp1, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	// Format the newly created volume with xfs.
	vg, err := lvm.LookupVolumeGroup(vgname)
	if err != nil {
		t.Fatal(err)
	}
	lv, err := vg.LookupLogicalVolume(resp1.GetVolume().GetId())
	if err != nil {
		t.Fatal(err)
	}
	lvpath, err := lv.Path()
	if err != nil {
		t.Fatal(err)
	}
	if err := formatDevice(lvpath, "xfs"); err != nil {
		t.Fatal(err)
	}
	// Wait for filesystem creation to be reflected in udev.
	_, err = exec.Command("udevadm", "settle").CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}

	// Try and create the same volume, with 'ext4' specified as fs_type in
	// a mount volume_capability.
	req.VolumeCapabilities[1].GetMount().FsType = "ext4"
	_, err = client.CreateVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrVolumeAlreadyExists) {
		t.Fatal("expected 'already exists' error instead of ", err)
	}
}

func TestCreateVolume_Idempotent_UnspecifiedExistingFsType(t *testing.T) {
	// Prepare a test server with a known volume group name.
	var clean cleanup.Steps
	defer func() {
		if x := recover(); x != nil {
			clean.Unwind()
			panic(x)
		}
	}()
	vgname := testvgname()
	pvname, pvclean := testpv()
	clean.Add(pvclean)
	client, clean2 := startTest(vgname, []string{pvname}, SupportedFilesystem("ext4"))
	clean.Add(func() error { clean2(); return nil })
	defer clean.Unwind()

	// Create a test volume.
	req := testCreateVolumeRequest()
	// Use only half the usual size so there is enough space for a
	// second volume to be created.
	req.CapacityRange.RequiredBytes /= 2
	resp1, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	// Format the newly created volume with xfs.
	vg, err := lvm.LookupVolumeGroup(vgname)
	if err != nil {
		t.Fatal(err)
	}
	lv, err := vg.LookupLogicalVolume(resp1.GetVolume().GetId())
	if err != nil {
		t.Fatal(err)
	}
	lvpath, err := lv.Path()
	if err != nil {
		t.Fatal(err)
	}
	if err := formatDevice(lvpath, "xfs"); err != nil {
		t.Fatal(err)
	}
	// Wait for filesystem creation to be reflected in udev.
	_, err = exec.Command("udevadm", "settle").CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}

	// Try and create the same volume, with no specified fs_type in a mount
	// volume_capability.
	req.VolumeCapabilities[1].GetMount().FsType = ""
	resp2, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	volInfo := resp2.GetVolume()
	if got := volInfo.GetCapacityBytes(); got != req.CapacityRange.RequiredBytes {
		t.Fatalf("Unexpected capacity_bytes %v != %v", got, req.CapacityRange.RequiredBytes)
	}
	if got := volInfo.GetId(); got != resp1.GetVolume().GetId() {
		t.Fatalf("Unexpected id %v != %v", got, resp1.GetVolume().GetId())
	}
	if got := volInfo.GetAttributes(); !reflect.DeepEqual(got, resp1.GetVolume().GetAttributes()) {
		t.Fatalf("Unexpected attributes %v != %v", got, resp1.GetVolume().GetAttributes())
	}
}

func TestCreateVolumeCapacityRangeNotSatisfied(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	req := testCreateVolumeRequest()
	// Use only half the usual size so there is enough space for a
	// second volume to be created.
	req.CapacityRange.RequiredBytes = pvsize * 2
	_, err := client.CreateVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrInsufficientCapacity) {
		t.Fatal(err)
	}
}

/* TODO(jdef) re-enable this test once we add length validation

func TestCreateVolumeInvalidVolumeName(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	req := testCreateVolumeRequest()
	// Use only half the usual size so there is enough space for a
	// second volume to be created.
	req.Name = "invalid name : /"
	_, err := client.CreateVolume(context.Background(), req)
	expdesc := "The volume name is invalid: err=lvm: Name contains invalid character, valid set includes: [A-Za-z0-9_+.-]"
	experr := status.Error(codes.InvalidArgument, expdesc)
	if !grpcErrorEqual(err, experr) {
		t.Fatal("expected 'invalid argument' error instead of ", err)
	}
}
*/

func TestCreateVolume_VolumeLayout_Linear(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	req := testCreateVolumeRequest()
	req.Parameters = map[string]string{
		"type": "linear",
	}
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	info := resp.GetVolume()
	if info.GetCapacityBytes() != req.GetCapacityRange().GetRequiredBytes() {
		t.Fatalf("Expected required_bytes (%v) to match volume size (%v).", req.GetCapacityRange().GetRequiredBytes(), info.GetCapacityBytes())
	}
	checkAttributesIncludeVolumeTag(t, info, req.GetName())
}

func TestCreateVolume_VolumeLayout_RAID1(t *testing.T) {
	vgname := testvgname()
	pvname1, pvclean1 := testpv()
	defer pvclean1()
	pvname2, pvclean2 := testpv()
	defer pvclean2()
	client, clean := startTest(vgname, []string{pvname1, pvname2})
	defer clean()
	req := testCreateVolumeRequest()
	req.Parameters = map[string]string{
		"type": "raid1",
	}
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	info := resp.GetVolume()
	if info.GetCapacityBytes() != req.GetCapacityRange().GetRequiredBytes() {
		t.Fatalf("Expected required_bytes (%v) to match volume size (%v).", req.GetCapacityRange().GetRequiredBytes(), info.GetCapacityBytes())
	}
	checkAttributesIncludeVolumeTag(t, info, req.GetName())
}

func TestCreateVolume_VolumeLayout_RAID1_Mirror2(t *testing.T) {
	vgname := testvgname()
	pvname1, pvclean1 := testpv()
	defer pvclean1()
	pvname2, pvclean2 := testpv()
	defer pvclean2()
	pvname3, pvclean3 := testpv()
	defer pvclean3()
	pvname4, pvclean4 := testpv()
	defer pvclean4()
	client, clean := startTest(vgname, []string{pvname1, pvname2, pvname3, pvname4})
	defer clean()
	req := testCreateVolumeRequest()
	req.Parameters = map[string]string{
		"type":    "raid1",
		"mirrors": "2",
	}
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	info := resp.GetVolume()
	if info.GetCapacityBytes() != req.GetCapacityRange().GetRequiredBytes() {
		t.Fatalf("Expected required_bytes (%v) to match volume size (%v).", req.GetCapacityRange().GetRequiredBytes(), info.GetCapacityBytes())
	}
	checkAttributesIncludeVolumeTag(t, info, req.GetName())
}

func TestCreateVolume_VolumeLayout_TooFewDisks(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	req := testCreateVolumeRequest()
	req.Parameters = map[string]string{
		"type": "raid1",
	}
	_, err := client.CreateVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrInsufficientCapacity) {
		// We expect ErrInsufficientCapacity as CreateVolume checks
		// whether there is sufficient capacity to create the volume.
		t.Fatal(err)
	}
}

func testDeleteVolumeRequest(volumeId string) *csi.DeleteVolumeRequest {
	req := &csi.DeleteVolumeRequest{
		VolumeId: volumeId,
	}
	return req
}

func TestDeleteVolume(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolume().GetId()
	req := testDeleteVolumeRequest(volumeId)
	_, err = client.DeleteVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeleteVolume_Idempotent(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolume().GetId()
	req := testDeleteVolumeRequest(volumeId)
	_, err = client.DeleteVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.DeleteVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeleteVolumeUnknownVolume(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	req := testDeleteVolumeRequest("missing-volume")
	_, err := client.DeleteVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
}

func TestDeleteVolumeAfterDeviceDisappears(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolume().GetId()
	// Remove the device node.
	vg, err := lvm.LookupVolumeGroup(vgname)
	if err != nil {
		panic(err)
	}
	lv, err := vg.LookupLogicalVolume(volumeId)
	if err != nil {
		t.Fatal(err)
	}
	path, err := lv.Path()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	// Delete the volume, even though the device node has already been
	// removed, and expect it to succeed.
	expErr := status.Errorf(codes.Internal,
		"The device path does not exist, cannot zero volume contents. To bypass the zeroing of the volume contents, ensure the file exists, or create it by hand, and reissue the DeleteVolume operation. path=%s",
		path)
	deleteReq := testDeleteVolumeRequest(volumeId)
	_, err = client.DeleteVolume(context.Background(), deleteReq)
	if !grpcErrorEqual(err, expErr) {
		t.Fatalf("expected %v got %v", expErr, err)
	}
}

func TestDeleteVolumeErasesData(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolume().GetId()
	capacityBytes := createResp.GetVolume().GetCapacityBytes()
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
		_, err = client.NodeUnpublishVolume(context.Background(), req)
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
	if int64(wrote) < capacityBytes/2 {
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
	volumeId = createResp.GetVolume().GetId()
	capacityBytes = createResp.GetVolume().GetCapacityBytes()
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
		_, err = client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
	}()
	// Check that the device is filled with zeros.
	n, ok := containsOnly(targetPath, 0)
	if !ok {
		t.Fatal("Expected device to consists of zeros only.")
	}
	if int64(n) != capacityBytes {
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
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	req := &csi.ControllerPublishVolumeRequest{}
	_, err := client.ControllerPublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrCallNotImplemented) {
		t.Fatal(err)
	}
}

func TestControllerUnpublishVolumeNotSupported(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	req := &csi.ControllerUnpublishVolumeRequest{}
	_, err := client.ControllerUnpublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrCallNotImplemented) {
		t.Fatal(err)
	}
}

func testValidateVolumeCapabilitiesRequest(volumeId string, filesystem string, mountOpts []string) *csi.ValidateVolumeCapabilitiesRequest {
	volumeCapabilities := []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Block{
				&csi.VolumeCapability_BlockVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
		{
			AccessType: &csi.VolumeCapability_Mount{
				&csi.VolumeCapability_MountVolume{
					FsType:     filesystem,
					MountFlags: mountOpts,
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	req := &csi.ValidateVolumeCapabilitiesRequest{
		VolumeId:           volumeId,
		VolumeCapabilities: volumeCapabilities,
	}
	return req
}

func TestValidateVolumeCapabilities_BlockVolume(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolume().GetId()
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
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolume().GetId()
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
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	// Create the volume that we'll be publishing.
	validateReq := testValidateVolumeCapabilitiesRequest("foo", "xfs", nil)
	_, err := client.ValidateVolumeCapabilities(context.Background(), validateReq)
	if !grpcErrorEqual(err, ErrVolumeNotFound) {
		t.Fatal(err)
	}
}

func TestValidateVolumeCapabilities_MountVolume_MismatchedFsTypes(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname}, SupportedFilesystem("ext4"))
	defer clean()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolume().GetId()
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
		MaxEntries:    0,
		StartingToken: "",
	}
	return req
}

func TestListVolumes_NoVolumes(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
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
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	tag := "vg_asset_123"
	client, clean := startTest(vgname, []string{pvname}, Tag(tag))
	defer clean()
	var infos []*csi.Volume
	// Add the first volume.
	req := testCreateVolumeRequest()
	req.Name = "test-volume-1"
	req.CapacityRange.RequiredBytes /= 2
	resp, err := client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	infos = append(infos, resp.GetVolume())
	// Add the second volume.
	req = testCreateVolumeRequest()
	req.Name = "test-volume-2"
	req.CapacityRange.RequiredBytes /= 2
	resp, err = client.CreateVolume(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	infos = append(infos, resp.GetVolume())
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
	nameTags := []string{"VN.test-volume-1", "VN.test-volume-2"}
	sort.Slice(entries, func(i, j int) bool { return entries[i].GetVolume().GetId() < entries[j].GetVolume().GetId() })
	for i, entry := range entries {
		had := false
		for _, info := range infos {
			if reflect.DeepEqual(info, entry.GetVolume()) {
				had = true
				break
			}
		}
		if !had {
			t.Fatalf("Cannot find volume info %+v in %+v.", entry.GetVolume(), infos)
		}

		// This validates that create and list both properly return the tags attribute.
		attr := entry.GetVolume().GetAttributes()
		tags := tagsFromAttributes(t, attr)
		expected := []string{tag, nameTags[i]}
		sort.Strings(expected)
		sort.Strings(tags)
		if !reflect.DeepEqual(tags, expected) {
			t.Fatalf("unexpected tags: %v", tags)
		}
	}
}

func tagsFromAttributes(t *testing.T, attr map[string]string) []string {
	etags, ok := attr[attrTags]
	if !ok {
		t.Fatalf("volume attributes missing tags")
	}
	buf, err := base64.RawURLEncoding.DecodeString(etags)
	if err != nil {
		t.Fatal("failed to decode tags:", err)
	}
	var itags []interface{}
	err = json.Unmarshal(buf, &itags)
	if err != nil {
		t.Fatal("failed to unmarshal tags:", err)
	}
	tags := make([]string, len(itags))
	for i := range itags {
		tags[i] = itags[i].(string)
	}
	return tags
}

func testGetCapacityRequest(fstype string) *csi.GetCapacityRequest {
	volumeCapabilities := []*csi.VolumeCapability{
		{
			AccessType: &csi.VolumeCapability_Block{
				&csi.VolumeCapability_BlockVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
		{
			AccessType: &csi.VolumeCapability_Mount{
				&csi.VolumeCapability_MountVolume{
					FsType: fstype,
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		},
	}
	req := &csi.GetCapacityRequest{
		VolumeCapabilities: volumeCapabilities,
	}
	return req
}

type testGetCapacity struct {
	numberOfPVs  uint64
	params       map[string]string
	createVolume bool
	err          error
}

func (tc testGetCapacity) test(t *testing.T) {
	vgname := testvgname()
	var pvnames []string
	for ii := uint64(0); ii < tc.numberOfPVs; ii++ {
		pvname, pvclean := testpv()
		defer pvclean()
		pvnames = append(pvnames, pvname)
	}
	client, clean := startTest(vgname, pvnames)
	defer clean()
	volumesize := int64(0)
	if tc.createVolume {
		volumesize = 40 << 20
		createReq := testCreateVolumeRequest()
		createReq.Name = "test-volume-1"
		createReq.CapacityRange.RequiredBytes = volumesize
		_, err := client.CreateVolume(context.Background(), createReq)
		if err != nil {
			t.Fatal(err)
		}
	}
	req := testGetCapacityRequest("xfs")
	req.Parameters = map[string]string{
		"type":    "raid1",
		"mirrors": "2",
	}
	resp, err := client.GetCapacity(context.Background(), req)
	if tc.err != nil {
		if !grpcErrorEqual(err, tc.err) {
			t.Fatal(err)
		}
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	layout, err := takeVolumeLayoutFromParameters(dupParams(req.GetParameters()))
	if err != nil {
		t.Fatal(err)
	}
	if layout.MinNumberOfDevices() > tc.numberOfPVs {
		if resp.GetAvailableCapacity() != 0 {
			t.Fatalf("Expected 0 bytes free.")
		}
		return
	}
	const extentsize = uint64(4 << 20)
	// One extent per PV is reserved for metadata.
	metadataExtents := tc.numberOfPVs
	// Calculate the linear available capacity.
	totalExtents := tc.numberOfPVs * pvsize / extentsize
	// Subtract the per-PV metadata extents from the total expected number of extents.
	totalAvailableExtents := totalExtents - metadataExtents
	// Reduce the capacity by the required_bytes of the created volume (if any).
	linearAvailableExtents := totalAvailableExtents - uint64(volumesize)/extentsize
	// Reduce the capacity by one more extent per data copy as each needs
	// to store metadata in a single extent. Then divide the remaining extents
	// by the number of data copies.
	expextents := (linearAvailableExtents - 3) / 3
	// The remaining bytes we get by multiplying by the extentsize again.
	expbytes := expextents * extentsize
	if got := resp.GetAvailableCapacity(); uint64(got) != expbytes {
		t.Fatalf("Expected %d bytes free but got %v.", expbytes, got)
	}
}

func TestGetCapacity_NoVolumes(t *testing.T) {
	testGetCapacity{
		numberOfPVs:  1,
		params:       nil,
		createVolume: false,
	}.test(t)
}

func TestGetCapacity_NoVolumes_OneDisk_VolumeLayout_Linear(t *testing.T) {
	testGetCapacity{
		numberOfPVs:  1,
		params:       map[string]string{"type": "linear"},
		createVolume: false,
	}.test(t)
}

func TestGetCapacity_NoVolumes_OneDisk_VolumeLayout_RAID1(t *testing.T) {
	testGetCapacity{
		numberOfPVs:  1,
		params:       map[string]string{"type": "raid1"},
		createVolume: false,
	}.test(t)
}

func TestGetCapacity_NoVolumes_TwoDisks_VolumeLayout_RAID1(t *testing.T) {
	testGetCapacity{
		numberOfPVs:  2,
		params:       map[string]string{"type": "raid1"},
		createVolume: false,
	}.test(t)
}

func TestGetCapacity_NoVolumes_FourDisks_VolumeLayout_RAID1_Mirrors2(t *testing.T) {
	testGetCapacity{
		numberOfPVs:  4,
		params:       map[string]string{"type": "raid1", "mirrors": "2"},
		createVolume: false,
	}.test(t)
}

func TestGetCapacity_OneVolume(t *testing.T) {
	testGetCapacity{
		numberOfPVs:  1,
		params:       nil,
		createVolume: true,
	}.test(t)
}

func TestGetCapacity_OneVolume_VolumeLayout_Linear(t *testing.T) {
	testGetCapacity{
		numberOfPVs:  1,
		params:       map[string]string{"type": "linear"},
		createVolume: true,
	}.test(t)
}

func TestGetCapacity_OneVolume_TwoDisks_VolumeLayout_RAID1(t *testing.T) {
	testGetCapacity{
		numberOfPVs:  2,
		params:       map[string]string{"type": "linear"},
		createVolume: true,
	}.test(t)
}

func TestGetCapacity_OneVolume_FourDisks_VolumeLayout_Mirror2(t *testing.T) {
	testGetCapacity{
		numberOfPVs:  2,
		params:       map[string]string{"type": "raid1", "mirrors": "2"},
		createVolume: true,
	}.test(t)
}

func TestGetCapacity_RemoveVolumeGroup(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname}, RemoveVolumeGroup())
	defer clean()
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
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	req := &csi.ControllerGetCapabilitiesRequest{}
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

func TestControllerGetCapabilitiesRemoveVolumeGroup(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname}, RemoveVolumeGroup())
	defer clean()
	req := &csi.ControllerGetCapabilitiesRequest{}
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
			AccessType: &csi.VolumeCapability_Block{
				&csi.VolumeCapability_BlockVolume{},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		}
	} else {
		volumeCapability = &csi.VolumeCapability{
			AccessType: &csi.VolumeCapability_Mount{
				&csi.VolumeCapability_MountVolume{
					FsType:     filesystem,
					MountFlags: mountOpts,
				},
			},
			AccessMode: &csi.VolumeCapability_AccessMode{
				Mode: csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER,
			},
		}
	}
	const readonly = false
	req := &csi.NodePublishVolumeRequest{
		VolumeId:         volumeId,
		TargetPath:       targetPath,
		VolumeCapability: volumeCapability,
		Readonly:         readonly,
	}
	return req
}

func testNodeUnpublishVolumeRequest(volumeId string, targetPath string) *csi.NodeUnpublishVolumeRequest {
	req := &csi.NodeUnpublishVolumeRequest{
		VolumeId:   volumeId,
		TargetPath: targetPath,
	}
	return req
}

func TestNodePublishVolumeNodeUnpublishVolume_BlockVolume(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolume().GetId()
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
		_, err = client.NodeUnpublishVolume(context.Background(), req)
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
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolume().GetId()
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
		_, err = client.NodeUnpublishVolume(context.Background(), req)
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
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolume().GetId()
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
		_, err = client.NodeUnpublishVolume(context.Background(), req)
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
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolume().GetId()
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
		_, err = client.NodeUnpublishVolume(context.Background(), req)
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
		_, err = client.NodeUnpublishVolume(context.Background(), req)
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
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolume().GetId()
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
		_, err = client.NodeUnpublishVolume(context.Background(), req)
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
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	// Create the volume that we'll be publishing.
	createReq1 := testCreateVolumeRequest()
	createReq1.CapacityRange.RequiredBytes /= 2
	createResp1, err := client.CreateVolume(context.Background(), createReq1)
	if err != nil {
		t.Fatal(err)
	}
	volumeId1 := createResp1.GetVolume().GetId()
	// Create a second volume.
	createReq2 := testCreateVolumeRequest()
	createReq2.Name += "-2"
	createReq2.CapacityRange.RequiredBytes /= 2
	createResp2, err := client.CreateVolume(context.Background(), createReq2)
	if err != nil {
		t.Fatal(err)
	}
	volumeId2 := createResp2.GetVolume().GetId()
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
		_, err = client.NodeUnpublishVolume(context.Background(), req)
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
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolume().GetId()
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
		_, err = client.NodeUnpublishVolume(context.Background(), req)
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
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	// Create the volume that we'll be publishing.
	createReq1 := testCreateVolumeRequest()
	createReq1.CapacityRange.RequiredBytes /= 2
	createResp1, err := client.CreateVolume(context.Background(), createReq1)
	if err != nil {
		t.Fatal(err)
	}
	volumeId1 := createResp1.GetVolume().GetId()
	// Create a second volume that we'll try to publish to the same targetPath.
	createReq2 := testCreateVolumeRequest()
	createReq2.Name += "-2"
	createReq2.CapacityRange.RequiredBytes /= 2
	createResp2, err := client.CreateVolume(context.Background(), createReq2)
	if err != nil {
		t.Fatal(err)
	}
	volumeId2 := createResp2.GetVolume().GetId()
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
		_, err = client.NodeUnpublishVolume(context.Background(), req)
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
		_, err = client.NodeUnpublishVolume(context.Background(), req)
		if err != nil {
			t.Fatal(err)
		}
		t.Fatal("Expected operation to fail")
	}
}

func TestNodeUnpublishVolume_BlockVolume_Idempotent(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolume().GetId()
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
		_, err = client.NodeUnpublishVolume(context.Background(), req)
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
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	// Create the volume that we'll be publishing.
	createReq := testCreateVolumeRequest()
	createResp, err := client.CreateVolume(context.Background(), createReq)
	if err != nil {
		t.Fatal(err)
	}
	volumeId := createResp.GetVolume().GetId()
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
		_, err = client.NodeUnpublishVolume(context.Background(), req)
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

func testNodeGetIdRequest() *csi.NodeGetIdRequest {
	req := &csi.NodeGetIdRequest{}
	return req
}

func TestNodeGetIdNotSupported(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	req := &csi.NodeGetIdRequest{}
	_, err := client.NodeGetId(context.Background(), req)
	if !grpcErrorEqual(err, ErrCallNotImplemented) {
		t.Fatal(err)
	}
}

func TestNodeGetId(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname}, NodeID("foo"))
	defer clean()
	req := &csi.NodeGetIdRequest{}
	resp, err := client.NodeGetId(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetNodeId() != "foo" {
		t.Fatal("unexpected response", resp)
	}
}

func testProbeRequest() *csi.ProbeRequest {
	req := &csi.ProbeRequest{}
	return req
}

func TestProbe(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	req := testProbeRequest()
	_, err := client.Probe(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
}

func TestProbe_MissingRequiredModule(t *testing.T) {
	// Same as TestProbe() except that it attempts to verify that a ficticious
	// module is loaded.
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname}, ProbeModules([]string{
		"no_such_module",
	}))
	defer clean()
	req := testProbeRequest()
	_, err := client.Probe(context.Background(), req)
	if err == nil {
		t.Fatal("expected probe failure due to missing module")
	}
	t.Log(err)
}

func TestSetup_NewVolumeGroup_NewPhysicalVolumes(t *testing.T) {
	vgname := testvgname()
	pv1name, pv1clean := testpv()
	defer pv1clean()
	pv2name, pv2clean := testpv()
	defer pv2clean()
	pvnames := []string{pv1name, pv2name}
	_, server, clean := prepareSetupTest(vgname, pvnames)
	defer clean()
	if err := server.Setup(); err != nil {
		t.Fatal(err)
	}
}

func TestSetup_NewVolumeGroup_NewPhysicalVolumes_WithTag(t *testing.T) {
	vgname := testvgname()
	pv1name, pv1clean := testpv()
	defer pv1clean()
	pv2name, pv2clean := testpv()
	defer pv2clean()
	pvnames := []string{pv1name, pv2name}
	tag := "blue"
	_, server, clean := prepareSetupTest(vgname, pvnames, Tag(tag))
	defer clean()
	if err := server.Setup(); err != nil {
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
	expected := []string{tag}
	if !reflect.DeepEqual(tags, expected) {
		t.Fatalf("Expected tags not found %v != %v", expected, tags)
	}
}

func TestSetup_NewVolumeGroup_NewPhysicalVolumes_WithMalformedTag(t *testing.T) {
	vgname := testvgname()
	pv1name, pv1clean := testpv()
	defer pv1clean()
	pv2name, pv2clean := testpv()
	defer pv2clean()
	pvnames := []string{pv1name, pv2name}
	tag := "-some-malformed-tag"
	_, server, clean := prepareSetupTest(vgname, pvnames, Tag(tag))
	defer clean()
	experr := fmt.Sprintf("Invalid tag '%v': err=%v",
		tag,
		"lvm: Tag must consist of only [A-Za-z0-9_+.-] and cannot start with a '-'")
	err := server.Setup()
	if err.Error() != experr {
		t.Fatal(err)
	}
}

func TestSetup_NewVolumeGroup_NonExistantPhysicalVolume(t *testing.T) {
	vgname := testvgname()
	pvnames := []string{"/dev/does/not/exist"}
	_, server, clean := prepareSetupTest(vgname, pvnames)
	defer clean()
	experr := "Could not stat device /dev/does/not/exist: err=stat /dev/does/not/exist: no such file or directory"
	err := server.Setup()
	if err.Error() != experr {
		t.Fatal(err)
	}
}

func TestSetup_NewVolumeGroup_BusyPhysicalVolume(t *testing.T) {
	vgname := testvgname()
	pv1name, pv1clean := testpv()
	defer pv1clean()
	pv2name, pv2clean := testpv()
	defer pv2clean()
	pvnames := []string{pv1name, pv2name}
	// Format and mount loop1 so it appears busy.
	if err := formatDevice(pv1name, "xfs"); err != nil {
		t.Fatal(err)
	}
	targetPath, err := ioutil.TempDir("", "csilvm_tests")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(targetPath)
	if merr := syscall.Mount(pv1name, targetPath, "xfs", 0, ""); merr != nil {
		t.Fatal(merr)
	}
	defer func() {
		if merr := syscall.Unmount(targetPath, 0); merr != nil {
			t.Fatal(merr)
		}
	}()
	_, server, clean := prepareSetupTest(vgname, pvnames)
	defer clean()
	experr := fmt.Sprintf(
		"Cannot create LVM2 physical volume %s: err=lvm: CreatePhysicalVolume: Can't open %s exclusively.  Mounted filesystem?",
		pv1name,
		pv1name,
	)
	err = server.Setup()
	// TODO(gpaul): Contains instead of '==' as LVM2.02.180-183 has a bug
	// where the error is printed twice.
	// See https://jira.mesosphere.com/browse/DCOS_OSS-4650
	if !strings.Contains(err.Error(), experr) {
		t.Fatal(err)
	}
}

func TestSetup_NewVolumeGroup_FormattedPhysicalVolume(t *testing.T) {
	vgname := testvgname()
	pv1name, pv1clean := testpv()
	defer pv1clean()
	pv2name, pv2clean := testpv()
	defer pv2clean()
	if err := exec.Command("mkfs", "-t", "xfs", pv2name).Run(); err != nil {
		t.Fatal(err)
	}
	pvnames := []string{pv1name, pv2name}
	_, server, clean := prepareSetupTest(vgname, pvnames)
	defer clean()
	if err := server.Setup(); err != nil {
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

func TestSetup_NewVolumeGroup_NewPhysicalVolumes_RemoveVolumeGroup(t *testing.T) {
	vgname := testvgname()
	pv1name, pv1clean := testpv()
	defer pv1clean()
	pv2name, pv2clean := testpv()
	defer pv2clean()
	pvnames := []string{pv1name, pv2name}
	_, server, clean := prepareSetupTest(vgname, pvnames, RemoveVolumeGroup())
	defer clean()
	if err := server.Setup(); err != nil {
		t.Fatal(err)
	}
	vgs, err := lvm.ListVolumeGroupNames()
	if err != nil {
		t.Fatal(err)
	}
	for _, vg := range vgs {
		if vg == vgname {
			t.Fatal("Unexpected volume group")
		}
	}
}

func TestSetup_ExistingVolumeGroup(t *testing.T) {
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
	_, server, clean := prepareSetupTest(vgname, pvnames)
	defer clean()
	if err := server.Setup(); err != nil {
		t.Fatal(err)
	}
}

func TestSetup_ExistingVolumeGroup_MissingPhysicalVolume(t *testing.T) {
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
	_, server, clean := prepareSetupTest(vgname, pvnames)
	defer clean()
	experr := "Volume group contains unexpected volumes [] and is missing volumes [/dev/missing-device]"
	err = server.Setup()
	if err.Error() != experr {
		t.Fatal(err)
	}
}

func TestSetup_ExistingVolumeGroup_UnexpectedExtraPhysicalVolume(t *testing.T) {
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
	_, server, clean := prepareSetupTest(vgname, pvnames)
	defer clean()
	experr := fmt.Sprintf(
		"Volume group contains unexpected volumes %v and is missing volumes []",
		[]string{loop2.Path()})
	err = server.Setup()
	if err.Error() != experr {
		t.Fatal(err)
	}
}

func TestSetup_ExistingVolumeGroup_RemoveVolumeGroup(t *testing.T) {
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
	_, server, clean := prepareSetupTest(vgname, pvnames, RemoveVolumeGroup())
	defer clean()
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
	if err := server.Setup(); err != nil {
		t.Fatal(err)
	}
	vgnamesAfter, err := lvm.ListVolumeGroupNames()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(vgnamesExpect, vgnamesAfter) {
		t.Fatalf("Expected volume groups %v after Setup but got %v", vgnamesExpect, vgnamesAfter)
	}
}

func TestSetup_ExistingVolumeGroup_UnexpectedExtraPhysicalVolume_RemoveVolumeGroup(t *testing.T) {
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
	_, server, clean := prepareSetupTest(vgname, pvnames, RemoveVolumeGroup())
	defer clean()
	experr := fmt.Sprintf(
		"Volume group contains unexpected volumes %v and is missing volumes []",
		[]string{loop2.Path()})
	err = server.Setup()
	if err.Error() != experr {
		t.Fatal(err)
	}
}

func TestSetup_ExistingVolumeGroup_WithTag(t *testing.T) {
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
	_, server, clean := prepareSetupTest(vgname, pvnames, Tag(tags[0]), Tag(tags[1]))
	defer clean()
	err = server.Setup()
	if err != nil {
		t.Fatal(err)
	}
}

func TestSetup_ExistingVolumeGroup_UnexpectedTag(t *testing.T) {
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
	_, server, clean := prepareSetupTest(vgname, pvnames)
	defer clean()
	experr := fmt.Sprintf(
		"Volume group tags did not match expected: err=csilvm: Configured tags don't match existing tags: [] != [%s]",
		tag)
	err = server.Setup()
	if err.Error() != experr {
		t.Fatal(err)
	}
}

func TestSetup_ExistingVolumeGroup_MissingTag(t *testing.T) {
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
	_, server, clean := prepareSetupTest(vgname, pvnames, Tag(tag))
	defer clean()
	experr := fmt.Sprintf(
		"Volume group tags did not match expected: err=csilvm: Configured tags don't match existing tags: [blue] != [some-other-tag]")
	err = server.Setup()
	if err.Error() != experr {
		t.Fatal(err)
	}
}

func testNodeGetCapabilitiesRequest() *csi.NodeGetCapabilitiesRequest {
	req := &csi.NodeGetCapabilitiesRequest{}
	return req
}

func TestNodeGetCapabilities(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname})
	defer clean()
	req := testNodeGetCapabilitiesRequest()
	_, err := client.NodeGetCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
}

func TestNodeGetCapabilitiesRemoveVolumeGroup(t *testing.T) {
	vgname := testvgname()
	pvname, pvclean := testpv()
	defer pvclean()
	client, clean := startTest(vgname, []string{pvname}, RemoveVolumeGroup())
	defer clean()
	req := testNodeGetCapabilitiesRequest()
	_, err := client.NodeGetCapabilities(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
}

func prepareSetupTest(vgname string, pvnames []string, serverOpts ...ServerOpt) (client *Client, server *Server, cleanupFn func()) {
	var clean cleanup.Steps
	defer func() {
		if x := recover(); x != nil {
			clean.Unwind()
			panic(x)
		}
	}()
	lis, err := net.Listen("unix", "@/csilvm-test-"+uuid.New().String())
	if err != nil {
		panic(err)
	}
	clean.Add(lis.Close)
	clean.Add(func() error {
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
	clean.Add(func() error {
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
	clean.Add(func() error {
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

	s := NewServer(vgname, pvnames, "xfs", serverOpts...)
	var opts []grpc.ServerOption
	opts = append(opts,
		grpc.UnaryInterceptor(
			ChainUnaryServer(
				LoggingInterceptor(),
				MetricsInterceptor(s.metrics),
			),
		),
	)
	// setup logging
	logprefix := fmt.Sprintf("[%s]", vgname)
	logflags := stdlog.LstdFlags | stdlog.Lshortfile
	SetLogger(stdlog.New(os.Stderr, logprefix, logflags))
	// Start a grpc server listening on the socket.
	grpcServer := grpc.NewServer(opts...)
	csi.RegisterIdentityServer(grpcServer, IdentityServerValidator(s))
	csi.RegisterControllerServer(grpcServer, ControllerServerValidator(s, s.RemovingVolumeGroup(), s.SupportedFilesystems()))
	csi.RegisterNodeServer(grpcServer, NodeServerValidator(s, s.RemovingVolumeGroup(), s.SupportedFilesystems()))
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
	clean.Add(conn.Close)
	client = NewClient(conn)
	return client, s, clean.Unwind
}

func testvgname() string {
	return "test-vg-" + uuid.New().String()
}

func testpv() (pvname string, cleanup func() error) {
	loop, err := lvm.CreateLoopDevice(pvsize)
	if err != nil {
		panic(err)
	}
	return loop.Path(), loop.Close
}

func startTest(vgname string, pvnames []string, serverOpts ...ServerOpt) (client *Client, cleanupFn func()) {
	var clean cleanup.Steps
	defer func() {
		if x := recover(); x != nil {
			clean.Unwind()
			panic(x)
		}
	}()

	serverOpts = append(serverOpts, ProbeModules([]string{
		"dm_raid",
		"raid1",
	}))

	// Create a volume group for the server to manage.
	// Create a volume group containing the physical volume.
	if vgname == "" {
		vgname = testvgname()
	}
	if len(pvnames) == 0 {
		pvname, pvclean := testpv()
		pvnames = append(pvnames, pvname)
		clean.Add(pvclean)
	}
	client, server, cleanup2 := prepareSetupTest(vgname, pvnames, serverOpts...)
	clean.Add(func() error { cleanup2(); return nil })
	// Perform the actual volume group create/remove.
	if err := server.Setup(); err != nil {
		panic(err)
	}
	clean.Add(func() error { server.ReportUptime()(); return nil })
	return client, clean.Unwind
}
