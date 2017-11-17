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
	if volumeCapabilities == nil {
		return ErrMissingVolumeCapabilities
	} else {
		// This still requires clarification. See
		// https://github.com/container-storage-interface/spec/issues/90
		if len(volumeCapabilities) == 0 {
			return ErrMissingVolumeCapabilities
		}
		for _, volumeCapability := range volumeCapabilities {
			if err := s.validateVolumeCapability(volumeCapability, false); err != nil {
				return err
			}
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

func (s *Server) validateVolumeHandle(volumeHandle *csi.VolumeHandle) *csi.Error {
	if volumeHandle == nil {
		return &csi.Error{
			&csi.Error_GeneralError_{
				&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume handle must be specified."},
			},
		}
	} else {
		id := volumeHandle.GetId()
		if id == "" {
			return &csi.Error{
				&csi.Error_GeneralError_{
					&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_handle.id field must be specified."},
				},
			}
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

func (s *Server) validateValidateVolumeCapabilitiesRequest(request *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, bool) {
	if err := s.validateRemoving(); err != nil {
		response := &csi.ValidateVolumeCapabilitiesResponse{
			&csi.ValidateVolumeCapabilitiesResponse_Error{
				err,
			},
		}
		log.Printf("ValidateVolumeCapabilities: failed: %+v", err)
		return response, false
	}
	if err := s.validateVersion(request.GetVersion()); err != nil {
		response := &csi.ValidateVolumeCapabilitiesResponse{
			&csi.ValidateVolumeCapabilitiesResponse_Error{
				err,
			},
		}
		log.Printf("ValidateVolumeCapabilities: failed: %+v", err)
		return response, false
	}
	volumeInfo := request.GetVolumeInfo()
	if volumeInfo == nil {
		err := &csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_info field must be specified."}
		response := &csi.ValidateVolumeCapabilitiesResponse{
			&csi.ValidateVolumeCapabilitiesResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						err,
					},
				},
			},
		}
		log.Printf("ValidateVolumeCapabilities: failed: %+v", err)
		return response, false
	} else {
		if err := s.validateVolumeHandle(volumeInfo.GetHandle()); err != nil {
			response := &csi.ValidateVolumeCapabilitiesResponse{
				&csi.ValidateVolumeCapabilitiesResponse_Error{
					err,
				},
			}
			log.Printf("ValidateVolumeCapabilities: failed: %+v", err)
			return response, false
		}
	}
	if err := s.validateVolumeCapabilities(request.GetVolumeCapabilities()); err != nil {
		response := &csi.ValidateVolumeCapabilitiesResponse{
			&csi.ValidateVolumeCapabilitiesResponse_Error{
				err,
			},
		}
		log.Printf("ValidateVolumeCapabilities: failed: %+v", err)
		return response, false
	}
	return nil, true
}

func (s *Server) validateListVolumesRequest(request *csi.ListVolumesRequest) (*csi.ListVolumesResponse, bool) {
	if err := s.validateRemoving(); err != nil {
		response := &csi.ListVolumesResponse{
			&csi.ListVolumesResponse_Error{
				err,
			},
		}
		log.Printf("ListVolumes: failed: %+v", err)
		return response, false
	}
	if err := s.validateVersion(request.GetVersion()); err != nil {
		response := &csi.ListVolumesResponse{
			&csi.ListVolumesResponse_Error{
				err,
			},
		}
		log.Printf("ListVolumes: failed: %+v", err)
		return response, false
	}
	return nil, true
}

func (s *Server) validateGetCapacityRequest(request *csi.GetCapacityRequest) (*csi.GetCapacityResponse, bool) {
	if err := s.validateVersion(request.GetVersion()); err != nil {
		response := &csi.GetCapacityResponse{
			&csi.GetCapacityResponse_Error{
				err,
			},
		}
		log.Printf("GetCapacity: failed: %+v", err)
		return response, false
	}
	volumeCapabilities := request.GetVolumeCapabilities()
	if len(volumeCapabilities) == 0 {
		// This field is optional.
	} else {
		// If it is provided, the individual elements must be validated.
		for _, volumeCapability := range volumeCapabilities {
			// We don't treat "unsupported fs type" as an
			// error for GetCapacity. We just return '0'
			// capacity.
			if err := s.validateVolumeCapability(volumeCapability, true); err != nil {
				response := &csi.GetCapacityResponse{
					&csi.GetCapacityResponse_Error{
						err,
					},
				}
				log.Printf("GetCapacity: failed: %+v", err)
				return response, false
			}
			// Check for unsupported filesystem type in
			// order to return 0 capacity if it isn't
			// supported.
			if mnt := volumeCapability.GetMount(); mnt != nil {
				// This is a MOUNT_VOLUME request.
				fstype := mnt.GetFsType()
				if _, ok := s.supportedFilesystems[fstype]; !ok {
					// Zero capacity for unsupported filesystem type.
					response := &csi.GetCapacityResponse{
						&csi.GetCapacityResponse_Result_{
							&csi.GetCapacityResponse_Result{
								0,
							},
						},
					}
					return response, false
				}
			}
		}
	}
	return nil, true
}

func (s *Server) validateControllerGetCapabilitiesRequest(request *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, bool) {
	if err := s.validateRemoving(); err != nil {
		response := &csi.ControllerGetCapabilitiesResponse{
			&csi.ControllerGetCapabilitiesResponse_Error{
				err,
			},
		}
		log.Printf("ControllerGetCapabilities: failed: %+v", err)
		return response, false
	}
	if err := s.validateVersion(request.GetVersion()); err != nil {
		response := &csi.ControllerGetCapabilitiesResponse{
			&csi.ControllerGetCapabilitiesResponse_Error{
				err,
			},
		}
		log.Printf("ControllerGetCapabilities: failed: %+v", err)
		return response, false
	}
	return nil, true
}

// NodeService RPCs

func (s *Server) validateNodePublishVolumeRequest(request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, bool) {
	if err := s.validateRemoving(); err != nil {
		response := &csi.NodePublishVolumeResponse{
			&csi.NodePublishVolumeResponse_Error{
				err,
			},
		}
		log.Printf("NodePublishVolume: failed: %+v", err)
		return response, false
	}
	if err := s.validateVersion(request.GetVersion()); err != nil {
		response := &csi.NodePublishVolumeResponse{
			&csi.NodePublishVolumeResponse_Error{
				err,
			},
		}
		log.Printf("NodePublishVolume: failed: %+v", err)
		return response, false
	}
	if err := s.validateVolumeHandle(request.GetVolumeHandle()); err != nil {
		response := &csi.NodePublishVolumeResponse{
			&csi.NodePublishVolumeResponse_Error{
				err,
			},
		}
		log.Printf("NodePublishVolume: failed: %+v", err)
		return response, false
	}
	if request.GetPublishVolumeInfo() != nil {
		err := &csi.Error_GeneralError{csi.Error_GeneralError_UNDEFINED, callerMustNotRetry, "The publish_volume_info field must not be specified."}
		response := &csi.NodePublishVolumeResponse{
			&csi.NodePublishVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						err,
					},
				},
			},
		}
		log.Printf("NodePublishVolume: failed: %+v", err)
		return response, false
	}
	targetPath := request.GetTargetPath()
	if targetPath == "" {
		err := &csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The target_path field must be specified."}
		response := &csi.NodePublishVolumeResponse{
			&csi.NodePublishVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						err,
					},
				},
			},
		}
		log.Printf("NodePublishVolume: failed: %+v", err)
		return response, false
	}
	volumeCapability := request.GetVolumeCapability()
	if volumeCapability == nil {
		err := &csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_capability field must be specified."}
		response := &csi.NodePublishVolumeResponse{
			&csi.NodePublishVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						err,
					},
				},
			},
		}
		log.Printf("NodePublishVolume: failed: %+v", err)
		return response, false
	} else {
		if err := s.validateVolumeCapability(volumeCapability, false); err != nil {
			response := &csi.NodePublishVolumeResponse{
				&csi.NodePublishVolumeResponse_Error{
					err,
				},
			}
			log.Printf("NodePublishVolume: failed: %+v", err)
			return response, false
		}
	}
	return nil, true
}

func (s *Server) validateNodeUnpublishVolumeRequest(request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, bool) {
	if err := s.validateRemoving(); err != nil {
		response := &csi.NodeUnpublishVolumeResponse{
			&csi.NodeUnpublishVolumeResponse_Error{
				err,
			},
		}
		log.Printf("NodeUnpublishVolume: failed: %+v", err)
		return response, false
	}
	if err := s.validateVersion(request.GetVersion()); err != nil {
		response := &csi.NodeUnpublishVolumeResponse{
			&csi.NodeUnpublishVolumeResponse_Error{
				err,
			},
		}
		log.Printf("NodeUnpublishVolume: failed: %+v", err)
		return response, false
	}
	if err := s.validateVolumeHandle(request.GetVolumeHandle()); err != nil {
		response := &csi.NodeUnpublishVolumeResponse{
			&csi.NodeUnpublishVolumeResponse_Error{
				err,
			},
		}
		log.Printf("NodeUnpublishVolume: failed: %+v", err)
		return response, false
	}
	targetPath := request.GetTargetPath()
	if targetPath == "" {
		err := &csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The target_path field must be specified."}
		response := &csi.NodeUnpublishVolumeResponse{
			&csi.NodeUnpublishVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						err,
					},
				},
			},
		}
		log.Printf("NodeUnpublishVolume: failed: %+v", err)
		return response, false
	}
	return nil, true
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
