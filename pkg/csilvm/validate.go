package csilvm

import (
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrRemovingMode = status.Error(
	codes.FailedPrecondition,
	"This service is running in 'remove volume group' mode.")

func (s *Server) validateRemoving() error {
	if s.removingVolumeGroup {
		return ErrRemovingMode
	}
	return nil
}

var ErrMissingVersion = status.Error(codes.InvalidArgument, "The version field must be specified.")
var ErrUnsupportedVersion = status.Error(codes.InvalidArgument, "The requested version is not supported.")

func (s *Server) validateVersion(version *csi.Version) error {
	if version == nil {
		return ErrMissingVersion
	}
	supportedVersion := false
	for _, v := range s.supportedVersions() {
		if *v == *version {
			supportedVersion = true
			break
		}
	}
	if !supportedVersion {
		return ErrUnsupportedVersion
	}
	return nil
}

var ErrMissingVolumeCapabilities = status.Error(codes.InvalidArgument, "The volume_capabilities field must be specified.")

func (s *Server) validateVolumeCapabilities(volumeCapabilities []*csi.VolumeCapability) error {
	if len(volumeCapabilities) == 0 {
		return ErrMissingVolumeCapabilities
	}
	for _, volumeCapability := range volumeCapabilities {
		const treatUnsupportedFsAsError = false
		if err := s.validateVolumeCapability(volumeCapability, treatUnsupportedFsAsError, false); err != nil {
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

func (s *Server) validateVolumeCapability(volumeCapability *csi.VolumeCapability, unsupportedFsOK, readonly bool) error {
	accessType := volumeCapability.GetAccessType()
	if accessType == nil {
		return ErrMissingAccessType
	}
	if mnt := volumeCapability.GetMount(); mnt != nil {
		// This is a MOUNT_VOLUME request.
		fstype := mnt.GetFsType()
		// If unsupportedFsOK is true, we don't treat an unsupported
		// filesystem as an error.
		if _, ok := s.supportedFilesystems[fstype]; !ok && !unsupportedFsOK {
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

// IdentityService RPCs

func (s *Server) validateGetPluginInfoRequest(request *csi.GetPluginInfoRequest) error {
	if err := s.validateVersion(request.GetVersion()); err != nil {
		return err
	}
	return nil
}

// ControllerService RPCs

var ErrMissingName = status.Error(codes.InvalidArgument, "The name field must be specified.")
var ErrUnsupportedFilesystem = status.Error(codes.FailedPrecondition, "The requested filesystem type is unknown.")

func (s *Server) validateCreateVolumeRequest(request *csi.CreateVolumeRequest) error {
	if err := s.validateRemoving(); err != nil {
		return err
	}
	if err := s.validateVersion(request.GetVersion()); err != nil {
		return err
	}
	name := request.GetName()
	if name == "" {
		return ErrMissingName
	}
	if capacityRange := request.GetCapacityRange(); capacityRange != nil {
		if err := s.validateCapacityRange(capacityRange); err != nil {
			return err
		}
	}
	if err := s.validateVolumeCapabilities(request.GetVolumeCapabilities()); err != nil {
		return err
	}
	return nil
}

var ErrCapacityRangeUnspecified = status.Error(
	codes.InvalidArgument,
	"One of required_bytes or limit_bytes must"+
		"be specified if capacity_range is specified.")

var ErrCapacityRangeInvalidSize = status.Error(
	codes.InvalidArgument,
	"The required_bytes cannot exceed the limit_bytes.")

func (s *Server) validateCapacityRange(capacityRange *csi.CapacityRange) error {
	if capacityRange.GetRequiredBytes() == 0 && capacityRange.GetLimitBytes() == 0 {
		return ErrCapacityRangeUnspecified
	}
	if capacityRange.GetRequiredBytes() > capacityRange.GetLimitBytes() {
		return ErrCapacityRangeInvalidSize
	}
	return nil
}

var ErrMissingVolumeId = status.Error(codes.InvalidArgument, "The volume_id field must be specified.")

func (s *Server) validateDeleteVolumeRequest(request *csi.DeleteVolumeRequest) error {
	if err := s.validateRemoving(); err != nil {
		return err
	}
	if err := s.validateVersion(request.GetVersion()); err != nil {
		return err
	}
	volumeId := request.GetVolumeId()
	if volumeId == "" {
		return ErrMissingVolumeId
	}
	return nil
}

func (s *Server) validateValidateVolumeCapabilitiesRequest(request *csi.ValidateVolumeCapabilitiesRequest) error {
	if err := s.validateRemoving(); err != nil {
		return err
	}
	if err := s.validateVersion(request.GetVersion()); err != nil {
		return err
	}
	volumeId := request.GetVolumeId()
	if volumeId == "" {
		return ErrMissingVolumeId
	}
	if err := s.validateVolumeCapabilities(request.GetVolumeCapabilities()); err != nil {
		return err
	}
	return nil
}

func (s *Server) validateListVolumesRequest(request *csi.ListVolumesRequest) error {
	if err := s.validateRemoving(); err != nil {
		return err
	}
	if err := s.validateVersion(request.GetVersion()); err != nil {
		return err
	}
	return nil
}

func (s *Server) validateGetCapacityRequest(request *csi.GetCapacityRequest) error {
	if err := s.validateVersion(request.GetVersion()); err != nil {
		return err
	}
	// If they are provided, the individual volume capabilities must be validated.
	for _, volumeCapability := range request.GetVolumeCapabilities() {
		// We don't treat "unsupported fs type" as an error for
		// GetCapacity. We'll just return 0 capacity.
		const ignoreUnsupportedFs = true
		if err := s.validateVolumeCapability(volumeCapability, ignoreUnsupportedFs, false); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) validateControllerGetCapabilitiesRequest(request *csi.ControllerGetCapabilitiesRequest) error {
	if err := s.validateVersion(request.GetVersion()); err != nil {
		return err
	}
	return nil
}

// NodeService RPCs

var ErrMissingTargetPath = status.Error(codes.InvalidArgument, "The target_path field must be specified.")
var ErrMissingVolumeCapability = status.Error(codes.InvalidArgument, "The volume_capability field must be specified.")
var ErrSpecifiedPublishVolumeInfo = status.Error(codes.InvalidArgument, "The publish_volume_info field must not be specified.")

func (s *Server) validateNodePublishVolumeRequest(request *csi.NodePublishVolumeRequest) error {
	if err := s.validateRemoving(); err != nil {
		return err
	}
	if err := s.validateVersion(request.GetVersion()); err != nil {
		return err
	}
	volumeId := request.GetVolumeId()
	if volumeId == "" {
		return ErrMissingVolumeId
	}
	publishVolumeInfo := request.GetPublishVolumeInfo()
	if publishVolumeInfo != nil {
		return ErrSpecifiedPublishVolumeInfo
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
		if err := s.validateVolumeCapability(volumeCapability, treatUnsupportedFsAsError, readonly); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) validateNodeUnpublishVolumeRequest(request *csi.NodeUnpublishVolumeRequest) error {
	if err := s.validateRemoving(); err != nil {
		return err
	}
	if err := s.validateVersion(request.GetVersion()); err != nil {
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

func (s *Server) validateGetNodeIDRequest(request *csi.GetNodeIDRequest) error {
	if err := s.validateVersion(request.GetVersion()); err != nil {
		return err
	}
	return nil
}

func (s *Server) validateNodeProbeRequest(request *csi.NodeProbeRequest) error {
	if err := s.validateVersion(request.GetVersion()); err != nil {
		return err
	}
	return nil
}

func (s *Server) validateControllerProbeRequest(request *csi.ControllerProbeRequest) error {
	if err := s.validateVersion(request.GetVersion()); err != nil {
		return err
	}
	return nil
}

func (s *Server) validateNodeGetCapabilitiesRequest(request *csi.NodeGetCapabilitiesRequest) error {
	if err := s.validateVersion(request.GetVersion()); err != nil {
		return err
	}
	return nil
}
