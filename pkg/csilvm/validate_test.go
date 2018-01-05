package csilvm

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/status"
)

// IdentityService RPCs

func TestGetPluginInfoMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetPluginInfoRequest()
	req.Version = nil
	_, err := client.GetPluginInfo(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingVersion) {
		t.Fatal(err)
	}
}

func TestGetPluginInfoUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetPluginInfoRequest()
	req.Version = &csi.Version{0, 2, 0}
	_, err := client.GetPluginInfo(context.Background(), req)
	if !grpcErrorEqual(err, ErrUnsupportedVersion) {
		t.Fatal(err)
	}
}

// ControllerService RPCs

func TestControllerProbeMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testControllerProbeRequest()
	req.Version = nil
	_, err := client.ControllerProbe(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingVersion) {
		t.Fatal(err)
	}
}

func TestControllerProbeUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testControllerProbeRequest()
	req.Version = &csi.Version{0, 2, 0}
	_, err := client.ControllerProbe(context.Background(), req)
	if !grpcErrorEqual(err, ErrUnsupportedVersion) {
		t.Fatal(err)
	}
}

func TestCreateVolumeRemoveVolumeGroup(t *testing.T) {
	client, cleanup := startTest(RemoveVolumeGroup())
	defer cleanup()
	req := testCreateVolumeRequest()
	_, err := client.CreateVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrRemovingMode) {
		t.Fatal(err)
	}
}

func TestCreateVolumeMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	req.Version = nil
	_, err := client.CreateVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingVersion) {
		t.Fatal(err)
	}
}

func TestCreateVolumeUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	req.Version = &csi.Version{0, 2, 0}
	_, err := client.CreateVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrUnsupportedVersion) {
		t.Fatal(err)
	}
}

func TestCreateVolumeMissingName(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	req.Name = ""
	_, err := client.CreateVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingName) {
		t.Fatal(err)
	}
}

func TestCreateVolumeMissingVolumeCapabilities(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	req.VolumeCapabilities = nil
	_, err := client.CreateVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingVolumeCapabilities) {
		t.Fatal(err)
	}
}

func TestCreateVolumeMissingVolumeCapabilitiesAccessType(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	req.VolumeCapabilities[0].AccessType = nil
	_, err := client.CreateVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingAccessType) {
		t.Fatal(err)
	}
}

func TestCreateVolumeMissingVolumeCapabilitiesAccessMode(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	req.VolumeCapabilities[0].AccessMode = nil
	_, err := client.CreateVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingAccessMode) {
		t.Fatal(err)
	}
}

func TestCreateVolumeVolumeCapabilitiesAccessModeUNKNOWN(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	req.VolumeCapabilities[0].AccessMode.Mode = csi.VolumeCapability_AccessMode_UNKNOWN
	_, err := client.CreateVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingAccessModeMode) {
		t.Fatal(err)
	}
}

func TestCreateVolumeVolumeCapabilitiesAccessModeUnsupported(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	req.VolumeCapabilities[0].AccessMode.Mode = csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY
	_, err := client.CreateVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrUnsupportedAccessMode) {
		t.Fatal(err)
	}
}

func TestCreateVolumeVolumeCapabilitiesAccessModeInvalid(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	req.VolumeCapabilities[0].AccessMode.Mode = 1000
	_, err := client.CreateVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrInvalidAccessMode) {
		t.Fatal(err)
	}
}

func TestCreateVolumeVolumeCapabilitiesReadonlyBlock(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	req.VolumeCapabilities[0].AccessMode.Mode = csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY
	_, err := client.CreateVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrBlockVolNoRO) {
		t.Fatal(err)
	}
}

func TestCreateVolumeVolumeCapabilitiesCapacityRangeRequiredLessThanLimit(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	req.CapacityRange.RequiredBytes = 1000
	req.CapacityRange.LimitBytes = req.CapacityRange.RequiredBytes - 1
	_, err := client.CreateVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrCapacityRangeInvalidSize) {
		t.Fatal(err)
	}
}

func TestCreateVolumeVolumeCapabilitiesCapacityRangeUnspecified(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testCreateVolumeRequest()
	req.CapacityRange.RequiredBytes = 0
	req.CapacityRange.LimitBytes = 0
	_, err := client.CreateVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrCapacityRangeUnspecified) {
		t.Fatal(err)
	}
}

func TestDeleteVolumeRemoveVolumeGroup(t *testing.T) {
	client, cleanup := startTest(RemoveVolumeGroup())
	defer cleanup()
	req := testDeleteVolumeRequest("test-volume")
	_, err := client.DeleteVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrRemovingMode) {
		t.Fatal(err)
	}
}

func TestDeleteVolumeMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testDeleteVolumeRequest("test-volume")
	req.Version = nil
	_, err := client.DeleteVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingVersion) {
		t.Fatal(err)
	}
}

func TestDeleteVolumeUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testDeleteVolumeRequest("test-volume")
	req.Version = &csi.Version{0, 2, 0}
	_, err := client.DeleteVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrUnsupportedVersion) {
		t.Fatal(err)
	}
}

func TestDeleteVolumeMissingVolumeId(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testDeleteVolumeRequest("test-volume")
	req.VolumeId = ""
	_, err := client.DeleteVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingVolumeId) {
		t.Fatal(err)
	}
}

func TestValidateVolumeCapabilitiesRemoveVolumeGroup(t *testing.T) {
	client, cleanup := startTest(RemoveVolumeGroup())
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest("fake_volume_id", "", nil)
	_, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if !grpcErrorEqual(err, ErrRemovingMode) {
		t.Fatal(err)
	}
}

func TestValidateVolumeCapabilitiesMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest("fake_volume_id", "", nil)
	req.Version = nil
	_, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingVersion) {
		t.Fatal(err)
	}
}

func TestValidateVolumeCapabilitiesUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest("fake_volume_id", "", nil)
	req.Version = &csi.Version{0, 2, 0}
	_, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if !grpcErrorEqual(err, ErrUnsupportedVersion) {
		t.Fatal(err)
	}
}

func TestValidateVolumeCapabilitiesMissingVolumeId(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest("fake_volume_id", "", nil)
	req.VolumeId = ""
	_, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingVolumeId) {
		t.Fatal(err)
	}
}

func TestValidateVolumeCapabilitiesMissingVolumeCapabilities(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest("fake_volume_id", "", nil)
	req.VolumeCapabilities = nil
	_, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingVolumeCapabilities) {
		t.Fatal(err)
	}
}

func TestValidateVolumeCapabilitiesMissingVolumeCapabilitiesAccessType(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest("fake_volume_id", "", nil)
	req.VolumeCapabilities[0].AccessType = nil
	_, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingAccessType) {
		t.Fatal(err)
	}
}

func TestValidateVolumeCapabilitiesNodeUnpublishVolume_MountVolume_BadFilesystem(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest("fake_volume_id", "ext4", nil)
	_, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if !grpcErrorEqual(err, ErrUnsupportedFilesystem) {
		t.Fatal(err)
	}
}

func TestValidateVolumeCapabilitiesMissingVolumeCapabilitiesAccessMode(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest("fake_volume_id", "", nil)
	req.VolumeCapabilities[0].AccessMode = nil
	_, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingAccessMode) {
		t.Fatal(err)
	}
}

func TestValidateVolumeCapabilitiesVolumeCapabilitiesAccessModeUNKNOWN(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testValidateVolumeCapabilitiesRequest("fake_volume_id", "", nil)
	req.VolumeCapabilities[0].AccessMode.Mode = csi.VolumeCapability_AccessMode_UNKNOWN
	_, err := client.ValidateVolumeCapabilities(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingAccessModeMode) {
		t.Fatal(err)
	}
}

func TestListVolumesRemoveVolumeGroup(t *testing.T) {
	client, cleanup := startTest(RemoveVolumeGroup())
	defer cleanup()
	req := testListVolumesRequest()
	_, err := client.ListVolumes(context.Background(), req)
	if !grpcErrorEqual(err, ErrRemovingMode) {
		t.Fatal(err)
	}
}

func TestListVolumesMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testListVolumesRequest()
	req.Version = nil
	_, err := client.ListVolumes(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingVersion) {
		t.Fatal(err)
	}
}

func TestListVolumesUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testListVolumesRequest()
	req.Version = &csi.Version{0, 2, 0}
	_, err := client.ListVolumes(context.Background(), req)
	if !grpcErrorEqual(err, ErrUnsupportedVersion) {
		t.Fatal(err)
	}
}

func TestGetCapacityMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetCapacityRequest("xfs")
	req.Version = nil
	_, err := client.GetCapacity(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingVersion) {
		t.Fatal(err)
	}
}

func TestGetCapacityUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetCapacityRequest("xfs")
	req.Version = &csi.Version{0, 2, 0}
	_, err := client.GetCapacity(context.Background(), req)
	if !grpcErrorEqual(err, ErrUnsupportedVersion) {
		t.Fatal(err)
	}
}

func TestGetCapacityMissingVolumeCapabilitiesAccessType(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetCapacityRequest("xfs")
	req.VolumeCapabilities[0].AccessType = nil
	_, err := client.GetCapacity(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingAccessType) {
		t.Fatal(err)
	}
}

func TestGetCapacity_BadFilesystem(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetCapacityRequest("ext4")
	resp, err := client.GetCapacity(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetAvailableCapacity() != 0 {
		t.Fatalf("Expected available_capacity=0 for unsupported filesystem type")
	}
}

func TestGetCapacityMissingVolumeCapabilitiesAccessMode(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetCapacityRequest("xfs")
	req.VolumeCapabilities[0].AccessMode = nil
	_, err := client.GetCapacity(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingAccessMode) {
		t.Fatal(err)
	}
}

func TestGetCapacityVolumeCapabilitiesAccessModeUNKNOWN(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testGetCapacityRequest("xfs")
	req.VolumeCapabilities[0].AccessMode.Mode = csi.VolumeCapability_AccessMode_UNKNOWN
	_, err := client.GetCapacity(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingAccessModeMode) {
		t.Fatal(err)
	}
}

func TestControllerGetCapabilitiesInfoMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := &csi.ControllerGetCapabilitiesRequest{}
	_, err := client.ControllerGetCapabilities(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingVersion) {
		t.Fatal(err)
	}
}

func TestControllerGetCapabilitiesInfoUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := &csi.ControllerGetCapabilitiesRequest{Version: &csi.Version{0, 2, 0}}
	_, err := client.ControllerGetCapabilities(context.Background(), req)
	if !grpcErrorEqual(err, ErrUnsupportedVersion) {
		t.Fatal(err)
	}
}

// NodeService RPCs

var fakeMountDir = "/run/dcos/csilvm/mnt"

func TestNodePublishVolumeRemoveVolumeGroup(t *testing.T) {
	client, cleanup := startTest(RemoveVolumeGroup())
	defer cleanup()
	req := testNodePublishVolumeRequest("fake_volume_id", fakeMountDir, "", nil)
	_, err := client.NodePublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrRemovingMode) {
		t.Fatal(err)
	}
}

func TestNodePublishVolumeMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest("fake_volume_id", fakeMountDir, "", nil)
	req.Version = nil
	_, err := client.NodePublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingVersion) {
		t.Fatal(err)
	}
}

func TestNodePublishVolumeUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest("fake_volume_id", fakeMountDir, "", nil)
	req.Version = &csi.Version{0, 2, 0}
	_, err := client.NodePublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrUnsupportedVersion) {
		t.Fatal(err)
	}
}

func TestNodePublishVolumeMissingVolumeId(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest("fake_volume_id", fakeMountDir, "", nil)
	req.VolumeId = ""
	_, err := client.NodePublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingVolumeId) {
		t.Fatal(err)
	}
}

func TestNodePublishVolumePresentPublishVolumeInfo(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest("fake_volume_id", fakeMountDir, "", nil)
	req.PublishVolumeInfo = map[string]string{"foo": "bar"}
	_, err := client.NodePublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrSpecifiedPublishVolumeInfo) {
		t.Fatal(err)
	}
}

func TestNodePublishVolumeMissingTargetPath(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest("fake_volume_id", fakeMountDir, "", nil)
	req.TargetPath = ""
	_, err := client.NodePublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingTargetPath) {
		t.Fatal(err)
	}
}

func TestNodePublishVolumeMissingVolumeCapability(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest("fake_volume_id", fakeMountDir, "", nil)
	req.VolumeCapability = nil
	_, err := client.NodePublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingVolumeCapability) {
		t.Fatal(err)
	}
}

func TestNodePublishVolumeMissingVolumeCapabilityAccessType(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest("fake_volume_id", fakeMountDir, "", nil)
	req.VolumeCapability.AccessType = nil
	_, err := client.NodePublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingAccessType) {
		t.Fatal(err)
	}
}

func TestNodePublishVolumeMissingVolumeCapabilityAccessMode(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest("fake_volume_id", fakeMountDir, "", nil)
	req.VolumeCapability.AccessMode = nil
	_, err := client.NodePublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingAccessMode) {
		t.Fatal(err)
	}
}

func TestNodePublishVolumeVolumeCapabilityAccessModeUNKNOWN(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest("fake_volume_id", fakeMountDir, "", nil)
	req.VolumeCapability.AccessMode.Mode = csi.VolumeCapability_AccessMode_UNKNOWN
	_, err := client.NodePublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingAccessModeMode) {
		t.Fatal(err)
	}
}

func TestNodePublishVolumeNodeUnpublishVolume_MountVolume_BadFilesystem(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodePublishVolumeRequest("fake_volume_id", fakeMountDir, "ext4", nil)
	_, err := client.NodePublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrUnsupportedFilesystem) {
		t.Fatal(err)
	}
}

var fakeTargetPath = filepath.Join(fakeMountDir, "fake_volume_id")

func TestNodeUnpublishVolumeRemoveVolumeGroup(t *testing.T) {
	client, cleanup := startTest(RemoveVolumeGroup())
	defer cleanup()
	req := testNodeUnpublishVolumeRequest("fake_volume_id", fakeTargetPath)
	_, err := client.NodeUnpublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrRemovingMode) {
		t.Fatal(err)
	}
}

func TestNodeUnpublishVolumeMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodeUnpublishVolumeRequest("fake_volume_id", fakeTargetPath)
	req.Version = nil
	_, err := client.NodeUnpublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingVersion) {
		t.Fatal(err)
	}
}

func TestNodeUnpublishVolumeUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodeUnpublishVolumeRequest("fake_volume_id", fakeTargetPath)
	req.Version = &csi.Version{0, 2, 0}
	_, err := client.NodeUnpublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrUnsupportedVersion) {
		t.Fatal(err)
	}
}

func TestNodeUnpublishVolumeMissingVolumeId(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodeUnpublishVolumeRequest("fake_volume_id", fakeTargetPath)
	req.VolumeId = ""
	_, err := client.NodeUnpublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingVolumeId) {
		t.Fatal(err)
	}
}

func TestNodeUnpublishVolumeMissingTargetPath(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodeUnpublishVolumeRequest("fake_volume_id", fakeTargetPath)
	req.TargetPath = ""
	_, err := client.NodeUnpublishVolume(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingTargetPath) {
		t.Fatal(err)
	}
}

func TestNodeProbeMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodeProbeRequest()
	req.Version = nil
	_, err := client.NodeProbe(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingVersion) {
		t.Fatal(err)
	}
}

func TestNodeProbeUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodeProbeRequest()
	req.Version = &csi.Version{0, 2, 0}
	_, err := client.NodeProbe(context.Background(), req)
	if !grpcErrorEqual(err, ErrUnsupportedVersion) {
		t.Fatal(err)
	}
}

func TestNodeGetCapabilitiesRequestMissingVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodeGetCapabilitiesRequest()
	req.Version = nil
	_, err := client.NodeGetCapabilities(context.Background(), req)
	if !grpcErrorEqual(err, ErrMissingVersion) {
		t.Fatal(err)
	}
}

func TestNodeGetCapabilitiesRequestUnsupportedVersion(t *testing.T) {
	client, cleanup := startTest()
	defer cleanup()
	req := testNodeGetCapabilitiesRequest()
	req.Version = &csi.Version{0, 2, 0}
	_, err := client.NodeGetCapabilities(context.Background(), req)
	if !grpcErrorEqual(err, ErrUnsupportedVersion) {
		t.Fatal(err)
	}
}

func grpcErrorEqual(gotErr, expErr error) bool {
	got, ok := status.FromError(gotErr)
	if !ok {
		return false
	}
	exp, ok := status.FromError(expErr)
	if !ok {
		return false
	}
	return got.Code() == exp.Code() && got.Message() == exp.Message()
}
