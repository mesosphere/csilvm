package csilvm

import (
	"context"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrRemovingMode = status.Error(
	codes.FailedPrecondition,
	"This service is running in 'remove volume group' mode.")

func validateRemoving(removingVolumeGroup bool) error {
	if removingVolumeGroup {
		return ErrRemovingMode
	}
	return nil
}

// IdentityService RPCs

type identityServerValidator struct {
	inner csi.IdentityServer
}

func IdentityServerValidator(inner csi.IdentityServer) csi.IdentityServer {
	return &identityServerValidator{inner}
}

func (v *identityServerValidator) GetPluginInfo(
	ctx context.Context,
	request *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	if err := validateGetPluginInfoRequest(request); err != nil {
		return nil, err
	}
	return v.inner.GetPluginInfo(ctx, request)
}

func validateGetPluginInfoRequest(request *csi.GetPluginInfoRequest) error {
	return nil
}

func (v *identityServerValidator) GetPluginCapabilities(
	ctx context.Context,
	request *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	if err := validateGetPluginCapabilitiesRequest(request); err != nil {
		return nil, err
	}
	return v.inner.GetPluginCapabilities(ctx, request)
}

func validateGetPluginCapabilitiesRequest(request *csi.GetPluginCapabilitiesRequest) error {
	return nil
}

func (v *identityServerValidator) Probe(
	ctx context.Context,
	request *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	if err := validateProbeRequest(request); err != nil {
		return nil, err
	}
	return v.inner.Probe(ctx, request)
}

func validateProbeRequest(request *csi.ProbeRequest) error {
	return nil
}

// ControllerService RPCs

type controllerServerValidator struct {
	inner                csi.ControllerServer
	removingVolumeGroup  bool
	supportedFilesystems map[string]string
}

func ControllerServerValidator(inner csi.ControllerServer, removingVolumeGroup bool, supportedFilesystems map[string]string) csi.ControllerServer {
	return &controllerServerValidator{inner, removingVolumeGroup, supportedFilesystems}
}

func (v *controllerServerValidator) CreateVolume(
	ctx context.Context,
	request *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if err := validateCreateVolumeRequest(request, v.removingVolumeGroup, v.supportedFilesystems); err != nil {
		return nil, err
	}
	return v.inner.CreateVolume(ctx, request)
}

func validateCreateVolumeRequest(request *csi.CreateVolumeRequest, removingVolumeGroup bool, supportedFilesystems map[string]string) error {
	if err := validateRemoving(removingVolumeGroup); err != nil {
		return err
	}
	name := request.GetName()
	if name == "" {
		return ErrMissingName
	}
	if capacityRange := request.GetCapacityRange(); capacityRange != nil {
		if err := validateCapacityRange(capacityRange); err != nil {
			return err
		}
	}
	if err := validateVolumeCapabilities(request.GetVolumeCapabilities(), supportedFilesystems); err != nil {
		return err
	}
	return nil
}

func (v *controllerServerValidator) DeleteVolume(
	ctx context.Context,
	request *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if err := validateDeleteVolumeRequest(request, v.removingVolumeGroup); err != nil {
		return nil, err
	}
	return v.inner.DeleteVolume(ctx, request)
}

var ErrMissingVolumeId = status.Error(codes.InvalidArgument, "The volume_id field must be specified.")

func validateDeleteVolumeRequest(request *csi.DeleteVolumeRequest, removingVolumeGroup bool) error {
	if err := validateRemoving(removingVolumeGroup); err != nil {
		return err
	}
	volumeId := request.GetVolumeId()
	if volumeId == "" {
		return ErrMissingVolumeId
	}
	return nil
}

var ErrMissingName = status.Error(codes.InvalidArgument, "The name field must be specified.")
var ErrUnsupportedFilesystem = status.Error(codes.FailedPrecondition, "The requested filesystem type is unknown.")

var ErrCapacityRangeUnspecified = status.Error(
	codes.InvalidArgument,
	"One of required_bytes or limit_bytes must "+
		"be specified if capacity_range is specified.")

var ErrCapacityRangeInvalidSize = status.Error(
	codes.InvalidArgument,
	"The required_bytes cannot exceed the limit_bytes.")

func validateCapacityRange(capacityRange *csi.CapacityRange) error {
	if capacityRange.GetRequiredBytes() == 0 && capacityRange.GetLimitBytes() == 0 {
		return ErrCapacityRangeUnspecified
	}

	// limit_bytes of 0 is equivalent to not setting a limit, so this is
	// allowed. We already know required_bytes is non-zero because of the above
	// check
	if capacityRange.GetLimitBytes() == 0 {
		return nil
	}
	if capacityRange.GetRequiredBytes() > capacityRange.GetLimitBytes() {
		// return ErrCapacityRangeInvalidSize
		return status.Errorf(
			codes.InvalidArgument,
			"required_bytes: %d cannot exceed the limit_bytes: %d",
			capacityRange.GetRequiredBytes(),
			capacityRange.GetLimitBytes(),
		)
	}
	return nil
}

func (v *controllerServerValidator) ControllerPublishVolume(
	ctx context.Context,
	request *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	return v.inner.ControllerPublishVolume(ctx, request)
}

func (v *controllerServerValidator) ControllerUnpublishVolume(
	ctx context.Context,
	request *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return v.inner.ControllerUnpublishVolume(ctx, request)
}

func (v *controllerServerValidator) ValidateVolumeCapabilities(
	ctx context.Context,
	request *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if err := validateValidateVolumeCapabilitiesRequest(request, v.removingVolumeGroup, v.supportedFilesystems); err != nil {
		return nil, err
	}
	return v.inner.ValidateVolumeCapabilities(ctx, request)
}

func validateValidateVolumeCapabilitiesRequest(request *csi.ValidateVolumeCapabilitiesRequest, removingVolumeGroup bool, supportedFilesystems map[string]string) error {
	if err := validateRemoving(removingVolumeGroup); err != nil {
		return err
	}
	volumeId := request.GetVolumeId()
	if volumeId == "" {
		return ErrMissingVolumeId
	}
	if err := validateVolumeCapabilities(request.GetVolumeCapabilities(), supportedFilesystems); err != nil {
		return err
	}
	return nil
}

var ErrMissingVolumeCapabilities = status.Error(codes.InvalidArgument, "The volume_capabilities field must be specified.")

func validateVolumeCapabilities(volumeCapabilities []*csi.VolumeCapability, supportedFilesystems map[string]string) error {
	if len(volumeCapabilities) == 0 {
		return ErrMissingVolumeCapabilities
	}
	for _, volumeCapability := range volumeCapabilities {
		const treatUnsupportedFsAsError = false
		if err := validateVolumeCapability(volumeCapability, supportedFilesystems, treatUnsupportedFsAsError, false); err != nil {
			return err
		}
	}
	return nil
}

var ErrMissingAccessType = status.Error(
	codes.InvalidArgument,
	"The volume_capability.access_type field must be specified.")
var ErrMissingAccessMode = status.Error(
	codes.InvalidArgument,
	"The volume_capability.access_mode field must be specified.")
var ErrMissingAccessModeMode = status.Error(
	codes.InvalidArgument,
	"The volume_capability.access_mode.mode field must be specified.")
var ErrInvalidAccessMode = status.Error(
	codes.InvalidArgument,
	"The volume_capability.access_mode.mode is invalid.")
var ErrUnsupportedAccessMode = status.Error(
	codes.InvalidArgument,
	"The volume_capability.access_mode.mode is unsupported.")
var ErrBlockVolNoRO = status.Error(
	codes.InvalidArgument,
	"Cannot publish block volume as readonly.")

func validateVolumeCapability(volumeCapability *csi.VolumeCapability, supportedFilesystems map[string]string, unsupportedFsOK, readonly bool) error {
	accessType := volumeCapability.GetAccessType()
	if accessType == nil {
		return ErrMissingAccessType
	}
	if mnt := volumeCapability.GetMount(); mnt != nil {
		// This is a MOUNT_VOLUME request.
		fstype := mnt.GetFsType()
		// If unsupportedFsOK is true, we don't treat an unsupported
		// filesystem as an error.
		if _, ok := supportedFilesystems[fstype]; !ok && !unsupportedFsOK {
			return ErrUnsupportedFilesystem
		}
	}
	if block := volumeCapability.GetBlock(); block != nil {
		readonly = readonly || volumeCapability.GetAccessMode().GetMode() == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY
		if readonly {
			// A block device cannot be bind mounted readonly.
			return ErrBlockVolNoRO
		}
	}
	accessMode := volumeCapability.GetAccessMode()
	if accessMode == nil {
		return ErrMissingAccessMode
	} else {
		mode := accessMode.GetMode()
		switch mode {
		case csi.VolumeCapability_AccessMode_UNKNOWN:
			return ErrMissingAccessModeMode
		case csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY,
			csi.VolumeCapability_AccessMode_SINGLE_NODE_WRITER:
			// Single node modes are satisfiable with this plugin.
		case csi.VolumeCapability_AccessMode_MULTI_NODE_READER_ONLY,
			csi.VolumeCapability_AccessMode_MULTI_NODE_SINGLE_WRITER,
			csi.VolumeCapability_AccessMode_MULTI_NODE_MULTI_WRITER:
			// Multinode modes are not satisfiable with this plugin.
			return ErrUnsupportedAccessMode
		default:
			return ErrInvalidAccessMode
		}
	}
	return nil
}

func (v *controllerServerValidator) ListVolumes(
	ctx context.Context,
	request *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	if err := validateListVolumesRequest(request); err != nil {
		return nil, err
	}
	return v.inner.ListVolumes(ctx, request)
}

func validateListVolumesRequest(request *csi.ListVolumesRequest) error {
	return nil
}

func (v *controllerServerValidator) GetCapacity(
	ctx context.Context,
	request *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	if err := validateGetCapacityRequest(request, v.supportedFilesystems); err != nil {
		return nil, err
	}
	return v.inner.GetCapacity(ctx, request)
}

func validateGetCapacityRequest(request *csi.GetCapacityRequest, supportedFilesystems map[string]string) error {
	// If they are provided, the individual volume capabilities must be validated.
	for _, volumeCapability := range request.GetVolumeCapabilities() {
		// We don't treat "unsupported fs type" as an error for
		// GetCapacity. We'll just return 0 capacity.
		const ignoreUnsupportedFs = true
		if err := validateVolumeCapability(volumeCapability, supportedFilesystems, ignoreUnsupportedFs, false); err != nil {
			return err
		}
	}
	return nil
}

func (v *controllerServerValidator) ControllerGetCapabilities(
	ctx context.Context,
	request *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	if err := validateControllerGetCapabilitiesRequest(request); err != nil {
		return nil, err
	}
	return v.inner.ControllerGetCapabilities(ctx, request)
}

func validateControllerGetCapabilitiesRequest(request *csi.ControllerGetCapabilitiesRequest) error {
	return nil
}

func (v *controllerServerValidator) CreateSnapshot(
	ctx context.Context,
	request *csi.CreateSnapshotRequest) (*csi.CreateSnapshotResponse, error) {
	return v.inner.CreateSnapshot(ctx, request)
}

func (v *controllerServerValidator) DeleteSnapshot(
	ctx context.Context,
	request *csi.DeleteSnapshotRequest) (*csi.DeleteSnapshotResponse, error) {
	return v.inner.DeleteSnapshot(ctx, request)
}

func (v *controllerServerValidator) ListSnapshots(
	ctx context.Context,
	request *csi.ListSnapshotsRequest) (*csi.ListSnapshotsResponse, error) {
	return v.inner.ListSnapshots(ctx, request)
}

// NodeService RPCs

type nodeServerValidator struct {
	inner                csi.NodeServer
	removingVolumeGroup  bool
	supportedFilesystems map[string]string
}

func NodeServerValidator(inner csi.NodeServer, removingVolumeGroup bool, supportedFilesystems map[string]string) csi.NodeServer {
	return &nodeServerValidator{inner, removingVolumeGroup, supportedFilesystems}
}

func (v *nodeServerValidator) NodePublishVolume(
	ctx context.Context,
	request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if err := validateNodePublishVolumeRequest(request, v.removingVolumeGroup, v.supportedFilesystems); err != nil {
		return nil, err
	}
	return v.inner.NodePublishVolume(ctx, request)
}

var ErrMissingTargetPath = status.Error(codes.InvalidArgument, "The target_path field must be specified.")
var ErrMissingVolumeCapability = status.Error(codes.InvalidArgument, "The volume_capability field must be specified.")
var ErrSpecifiedPublishInfo = status.Error(codes.InvalidArgument, "The publish_volume_info field must not be specified.")

func validateNodePublishVolumeRequest(request *csi.NodePublishVolumeRequest, removingVolumeGroup bool, supportedFilesystems map[string]string) error {
	if err := validateRemoving(removingVolumeGroup); err != nil {
		return err
	}
	volumeId := request.GetVolumeId()
	if volumeId == "" {
		return ErrMissingVolumeId
	}
	publishInfo := request.GetPublishInfo()
	if publishInfo != nil {
		return ErrSpecifiedPublishInfo
	}
	targetPath := request.GetTargetPath()
	if targetPath == "" {
		return ErrMissingTargetPath
	}
	volumeCapability := request.GetVolumeCapability()
	if volumeCapability == nil {
		return ErrMissingVolumeCapability
	} else {
		const treatUnsupportedFsAsError = false
		readonly := request.GetReadonly()
		if err := validateVolumeCapability(volumeCapability, supportedFilesystems, treatUnsupportedFsAsError, readonly); err != nil {
			return err
		}
	}
	return nil
}

func (v *nodeServerValidator) NodeUnpublishVolume(
	ctx context.Context,
	request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if err := validateNodeUnpublishVolumeRequest(request, v.removingVolumeGroup); err != nil {
		return nil, err
	}
	return v.inner.NodeUnpublishVolume(ctx, request)
}

func validateNodeUnpublishVolumeRequest(request *csi.NodeUnpublishVolumeRequest, removingVolumeGroup bool) error {
	if err := validateRemoving(removingVolumeGroup); err != nil {
		return err
	}
	volumeId := request.GetVolumeId()
	if volumeId == "" {
		return ErrMissingVolumeId
	}
	targetPath := request.GetTargetPath()
	if targetPath == "" {
		return ErrMissingTargetPath
	}
	return nil
}

func (v *nodeServerValidator) NodeGetCapabilities(
	ctx context.Context,
	request *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	if err := validateNodeGetCapabilitiesRequest(request); err != nil {
		return nil, err
	}
	return v.inner.NodeGetCapabilities(ctx, request)
}

func validateNodeGetCapabilitiesRequest(request *csi.NodeGetCapabilitiesRequest) error {
	return nil
}

func (v *nodeServerValidator) NodeGetId(
	ctx context.Context,
	request *csi.NodeGetIdRequest) (*csi.NodeGetIdResponse, error) {
	return v.inner.NodeGetId(ctx, request)
}

func (v *nodeServerValidator) NodeGetInfo(
	ctx context.Context,
	request *csi.NodeGetInfoRequest) (*csi.NodeGetInfoResponse, error) {
	return v.inner.NodeGetInfo(ctx, request)
}

func (v *nodeServerValidator) NodeStageVolume(
	ctx context.Context,
	request *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	return v.inner.NodeStageVolume(ctx, request)
}

func (v *nodeServerValidator) NodeUnstageVolume(
	ctx context.Context,
	request *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	return v.inner.NodeUnstageVolume(ctx, request)
}
