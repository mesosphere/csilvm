package csilvm

import (
	"github.com/container-storage-interface/spec/lib/go/csi"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	callerMustNotRetry = true
	callerMayRetry     = false
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
		const unsupportedFsIsError = false
		if err := s.validateVolumeCapability(volumeCapability, unsupportedFsIsError); err != nil {
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

func (s *Server) validateVolumeCapability(volumeCapability *csi.VolumeCapability, unsupportedFsOK bool) *csi.Error {
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
	accessMode := volumeCapability.GetAccessMode()
	if accessMode == nil {
		return ErrMissingAccessMode
	} else {
		mode := accessMode.GetMode()
		if mode == csi.VolumeCapability_AccessMode_UNKNOWN {
			return ErrMissingAccessModeMode
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
	if err := s.validateVolumeCapabilities(request.GetVolumeCapabilities()); err != nil {
		return err
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
	if volumeId == nil {
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
	if volumeId == nil {
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
		if err := s.validateVolumeCapability(volumeCapability, ignoreUnsupportedFs); err != nil {
			return err
		}
	}
	return nil
}

func (s *Server) validateControllerGetCapabilitiesRequest(request *csi.ControllerGetCapabilitiesRequest) error {
	if err := s.validateRemoving(); err != nil {
		return err
	}
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
	if volumeId == nil {
		return ErrMissingVolumeId
	}
	publishVolumeInfo := request.GetPublishVolumeInfo()
	if publishVolumeInfo != "" {
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
		const unsupportedFsIsError = false
		if err := s.validateVolumeCapability(volumeCapability, unsupportedFsIsError); err != nil {
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
	if volumeId == nil {
		return ErrMissingVolumeId
	}
	targetPath := request.GetTargetPath()
	if targetPath == "" {
		return ErrMissingTargetPath
	}
	return nil
}

func (s *Server) validateGetNodeIDRequest(request *csi.GetNodeIDRequest) (*csi.GetNodeIDResponse, bool) {
	if err := s.validateRemoving(); err != nil {
		response := &csi.GetNodeIDResponse{
			&csi.GetNodeIDResponse_Error{
				err,
			},
		}
		log.Printf("GetNodeID: failed: %+v", err)
		return response, false
	}
	if err := s.validateVersion(request.GetVersion()); err != nil {
		response := &csi.GetNodeIDResponse{
			&csi.GetNodeIDResponse_Error{
				err,
			},
		}
		log.Printf("GetNodeID: failed: %+v", err)
		return response, false
	}
	return nil, true
}

func (s *Server) validateControllerProbeRequest(request *csi.ControllerProbeRequest) error {
	if err := s.validateVersion(request.GetVersion()); err != nil {
		return err
	}
	return nil
}

func (s *Server) validateNodeGetCapabilitiesRequest(request *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, bool) {
	if err := s.validateRemoving(); err != nil {
		response := &csi.NodeGetCapabilitiesResponse{
			&csi.NodeGetCapabilitiesResponse_Error{
				err,
			},
		}
		log.Printf("NodeGetCapabilities: failed: %+v", err)
		return response, false
	}
	if err := s.validateVersion(request.GetVersion()); err != nil {
		response := &csi.NodeGetCapabilitiesResponse{
			&csi.NodeGetCapabilitiesResponse_Error{
				err,
			},
		}
		log.Printf("NodeGetCapabilities: failed: %+v", err)
		return response, false
	}
	return nil, true
}
