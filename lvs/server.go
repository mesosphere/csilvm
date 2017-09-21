package lvs

import (
	"golang.org/x/net/context"

	"github.com/container-storage-interface/spec/lib/go/csi"
)

const PluginName = "com.mesosphere/lvs"
const PluginVersion = "0.1.0"

type Server struct {
}

func (s *Server) supportedVersions() []*csi.Version {
	return []*csi.Version{
		&csi.Version{0, 1, 0},
	}
}

func NewServer() *Server {
	return new(Server)
}

// IdentityService RPCs

func (s *Server) GetSupportedVersions(
	ctx context.Context,
	request *csi.GetSupportedVersionsRequest) (*csi.GetSupportedVersionsResponse, error) {
	response := &csi.GetSupportedVersionsResponse{
		&csi.GetSupportedVersionsResponse_Result_{
			&csi.GetSupportedVersionsResponse_Result{
				s.supportedVersions(),
			},
		},
	}
	return response, nil
}

func (s *Server) GetPluginInfo(
	ctx context.Context,
	request *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	if response, ok := s.validateGetPluginInfoRequest(request); !ok {
		return response, nil
	}
	response := &csi.GetPluginInfoResponse{
		&csi.GetPluginInfoResponse_Result_{
			&csi.GetPluginInfoResponse_Result{PluginName, PluginVersion, nil},
		},
	}
	return response, nil
}

func (s *Server) validateGetPluginInfoRequest(request *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, bool) {
	version := request.GetVersion()
	if version == nil {
		response := &csi.GetPluginInfoResponse{
			&csi.GetPluginInfoResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The version must be specified."},
					},
				},
			},
		}
		return response, false
	}
	supportedVersion := false
	for _, v := range s.supportedVersions() {
		if *v == *version {
			supportedVersion = true
			break
		}
	}
	if !supportedVersion {
		response := &csi.GetPluginInfoResponse{
			&csi.GetPluginInfoResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION, true, "The requested version is not supported."},
					},
				},
			},
		}
		return response, false
	}
	return nil, true
}

// ControllerService RPCs

func (s *Server) CreateVolume(
	ctx context.Context,
	request *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if response, ok := s.validateCreateVolumeRequest(request); !ok {
		return response, nil
	}
	response := &csi.CreateVolumeResponse{}
	return response, nil
}

func (s *Server) validateCreateVolumeRequest(request *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, bool) {
	version := request.GetVersion()
	if version == nil {
		response := &csi.CreateVolumeResponse{
			&csi.CreateVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The version must be specified."},
					},
				},
			},
		}
		return response, false
	}
	supportedVersion := false
	for _, v := range s.supportedVersions() {
		if *v == *version {
			supportedVersion = true
			break
		}
	}
	if !supportedVersion {
		response := &csi.CreateVolumeResponse{
			&csi.CreateVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION, true, "The requested version is not supported."},
					},
				},
			},
		}
		return response, false
	}
	name := request.GetName()
	if name == "" {
		response := &csi.CreateVolumeResponse{
			&csi.CreateVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The name field must be specified."},
					},
				},
			},
		}
		return response, false
	}
	volumeCapabilities := request.GetVolumeCapabilities()
	if volumeCapabilities == nil {
		response := &csi.CreateVolumeResponse{
			&csi.CreateVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The volume_capabilities field must be specified."},
					},
				},
			},
		}
		return response, false
	} else {
		// This still requires clarification. See
		// https://github.com/container-storage-interface/spec/issues/90
		if len(volumeCapabilities) == 0 {
			response := &csi.CreateVolumeResponse{
				&csi.CreateVolumeResponse_Error{
					&csi.Error{
						&csi.Error_GeneralError_{
							&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "One or more volume_capabilities must be specified."},
						},
					},
				},
			}
			return response, false
		}
		for _, volumeCapability := range volumeCapabilities {
			accessType := volumeCapability.GetAccessType()
			if accessType == nil {
				response := &csi.CreateVolumeResponse{
					&csi.CreateVolumeResponse_Error{
						&csi.Error{
							&csi.Error_GeneralError_{
								&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The volume_capabilities.access_type field must be specified."},
							},
						},
					},
				}
				return response, false
			}
			accessMode := volumeCapability.GetAccessMode()
			if accessMode == nil {
				response := &csi.CreateVolumeResponse{
					&csi.CreateVolumeResponse_Error{
						&csi.Error{
							&csi.Error_GeneralError_{
								&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The volume_capabilities.access_mode field must be specified."},
							},
						},
					},
				}
				return response, false
			} else {
				mode := accessMode.GetMode()
				if mode == csi.VolumeCapability_AccessMode_UNKNOWN {
					response := &csi.CreateVolumeResponse{
						&csi.CreateVolumeResponse_Error{
							&csi.Error{
								&csi.Error_GeneralError_{
									&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The volume_capabilities.access_mode.mode field must be specified."},
								},
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

func (s *Server) DeleteVolume(
	ctx context.Context,
	request *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if response, ok := s.validateDeleteVolumeRequest(request); !ok {
		return response, nil
	}
	response := &csi.DeleteVolumeResponse{}
	return response, nil
}

func (s *Server) validateDeleteVolumeRequest(request *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, bool) {
	version := request.GetVersion()
	if version == nil {
		response := &csi.DeleteVolumeResponse{
			&csi.DeleteVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The version must be specified."},
					},
				},
			},
		}
		return response, false
	}
	supportedVersion := false
	for _, v := range s.supportedVersions() {
		if *v == *version {
			supportedVersion = true
			break
		}
	}
	if !supportedVersion {
		response := &csi.DeleteVolumeResponse{
			&csi.DeleteVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION, true, "The requested version is not supported."},
					},
				},
			},
		}
		return response, false
	}
	volumeHandle := request.GetVolumeHandle()
	if volumeHandle == nil {
		response := &csi.DeleteVolumeResponse{
			&csi.DeleteVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The volume_handle field must be specified."},
					},
				},
			},
		}
		return response, false
	} else {
		id := volumeHandle.GetId()
		if id == "" {
			response := &csi.DeleteVolumeResponse{
				&csi.DeleteVolumeResponse_Error{
					&csi.Error{
						&csi.Error_GeneralError_{
							&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The volume_handle.id field must be specified."},
						},
					},
				},
			}
			return response, false
		}
	}
	return nil, true
}

func (s *Server) ControllerPublishVolume(
	ctx context.Context,
	request *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	response := &csi.ControllerPublishVolumeResponse{
		&csi.ControllerPublishVolumeResponse_Error{
			&csi.Error{
				&csi.Error_ControllerPublishVolumeError_{
					&csi.Error_ControllerPublishVolumeError{csi.Error_ControllerPublishVolumeError_CALL_NOT_IMPLEMENTED, "The ControllerPublishVolume RPC is not supported.", nil},
				},
			},
		},
	}
	return response, nil
}

func (s *Server) ControllerUnpublishVolume(
	ctx context.Context,
	request *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	response := &csi.ControllerUnpublishVolumeResponse{
		&csi.ControllerUnpublishVolumeResponse_Error{
			&csi.Error{
				&csi.Error_ControllerUnpublishVolumeError_{
					&csi.Error_ControllerUnpublishVolumeError{csi.Error_ControllerUnpublishVolumeError_CALL_NOT_IMPLEMENTED, "The ControllerUnpublishVolume RPC is not supported."},
				},
			},
		},
	}
	return response, nil
}

func (s *Server) ValidateVolumeCapabilities(
	ctx context.Context,
	request *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if response, ok := s.validateValidateVolumeCapabilitiesRequest(request); !ok {
		return response, nil
	}
	response := &csi.ValidateVolumeCapabilitiesResponse{}
	return response, nil
}

func (s *Server) validateValidateVolumeCapabilitiesRequest(request *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, bool) {
	version := request.GetVersion()
	if version == nil {
		response := &csi.ValidateVolumeCapabilitiesResponse{
			&csi.ValidateVolumeCapabilitiesResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The version must be specified."},
					},
				},
			},
		}
		return response, false
	}
	supportedVersion := false
	for _, v := range s.supportedVersions() {
		if *v == *version {
			supportedVersion = true
			break
		}
	}
	if !supportedVersion {
		response := &csi.ValidateVolumeCapabilitiesResponse{
			&csi.ValidateVolumeCapabilitiesResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION, true, "The requested version is not supported."},
					},
				},
			},
		}
		return response, false
	}
	volumeInfo := request.GetVolumeInfo()
	if volumeInfo == nil {
		response := &csi.ValidateVolumeCapabilitiesResponse{
			&csi.ValidateVolumeCapabilitiesResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The volume_info field must be specified."},
					},
				},
			},
		}
		return response, false
	} else {
		volumeHandle := volumeInfo.GetHandle()
		if volumeHandle == nil {
			response := &csi.ValidateVolumeCapabilitiesResponse{
				&csi.ValidateVolumeCapabilitiesResponse_Error{
					&csi.Error{
						&csi.Error_GeneralError_{
							&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The volume_info.handle field must be specified."},
						},
					},
				},
			}
			return response, false
		} else {
			id := volumeHandle.GetId()
			if id == "" {
				response := &csi.ValidateVolumeCapabilitiesResponse{
					&csi.ValidateVolumeCapabilitiesResponse_Error{
						&csi.Error{
							&csi.Error_GeneralError_{
								&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The volume_info.handle.id field must be specified."},
							},
						},
					},
				}
				return response, false
			}
		}

	}
	volumeCapabilities := request.GetVolumeCapabilities()
	if volumeCapabilities == nil {
		response := &csi.ValidateVolumeCapabilitiesResponse{
			&csi.ValidateVolumeCapabilitiesResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The volume_capabilities field must be specified."},
					},
				},
			},
		}
		return response, false
	} else {
		// This still requires clarification. See
		// https://github.com/container-storage-interface/spec/issues/90
		if len(volumeCapabilities) == 0 {
			response := &csi.ValidateVolumeCapabilitiesResponse{
				&csi.ValidateVolumeCapabilitiesResponse_Error{
					&csi.Error{
						&csi.Error_GeneralError_{
							&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "One or more volume_capabilities must be specified."},
						},
					},
				},
			}
			return response, false
		}
		for _, volumeCapability := range volumeCapabilities {
			accessType := volumeCapability.GetAccessType()
			if accessType == nil {
				response := &csi.ValidateVolumeCapabilitiesResponse{
					&csi.ValidateVolumeCapabilitiesResponse_Error{
						&csi.Error{
							&csi.Error_GeneralError_{
								&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The volume_capabilities.access_type field must be specified."},
							},
						},
					},
				}
				return response, false
			}
			accessMode := volumeCapability.GetAccessMode()
			if accessMode == nil {
				response := &csi.ValidateVolumeCapabilitiesResponse{
					&csi.ValidateVolumeCapabilitiesResponse_Error{
						&csi.Error{
							&csi.Error_GeneralError_{
								&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The volume_capabilities.access_mode field must be specified."},
							},
						},
					},
				}
				return response, false
			} else {
				mode := accessMode.GetMode()
				if mode == csi.VolumeCapability_AccessMode_UNKNOWN {
					response := &csi.ValidateVolumeCapabilitiesResponse{
						&csi.ValidateVolumeCapabilitiesResponse_Error{
							&csi.Error{
								&csi.Error_GeneralError_{
									&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The volume_capabilities.access_mode.mode field must be specified."},
								},
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

func (s *Server) ListVolumes(
	ctx context.Context,
	request *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	if response, ok := s.validateListVolumesRequest(request); !ok {
		return response, nil
	}
	response := &csi.ListVolumesResponse{}
	return response, nil
}

func (s *Server) validateListVolumesRequest(request *csi.ListVolumesRequest) (*csi.ListVolumesResponse, bool) {
	version := request.GetVersion()
	if version == nil {
		response := &csi.ListVolumesResponse{
			&csi.ListVolumesResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The version must be specified."},
					},
				},
			},
		}
		return response, false
	}
	supportedVersion := false
	for _, v := range s.supportedVersions() {
		if *v == *version {
			supportedVersion = true
			break
		}
	}
	if !supportedVersion {
		response := &csi.ListVolumesResponse{
			&csi.ListVolumesResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION, true, "The requested version is not supported."},
					},
				},
			},
		}
		return response, false
	}
	return nil, true
}

func (s *Server) GetCapacity(
	ctx context.Context,
	request *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	if response, ok := s.validateGetCapacityRequest(request); !ok {
		return response, nil
	}
	response := &csi.GetCapacityResponse{}
	return response, nil
}

func (s *Server) validateGetCapacityRequest(request *csi.GetCapacityRequest) (*csi.GetCapacityResponse, bool) {
	version := request.GetVersion()
	if version == nil {
		response := &csi.GetCapacityResponse{
			&csi.GetCapacityResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The version must be specified."},
					},
				},
			},
		}
		return response, false
	}
	supportedVersion := false
	for _, v := range s.supportedVersions() {
		if *v == *version {
			supportedVersion = true
			break
		}
	}
	if !supportedVersion {
		response := &csi.GetCapacityResponse{
			&csi.GetCapacityResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION, true, "The requested version is not supported."},
					},
				},
			},
		}
		return response, false
	}
	volumeCapabilities := request.GetVolumeCapabilities()
	if len(volumeCapabilities) == 0 {
		// This field is optional.
	} else {
		// If it is provided, the individual elements must be validated.
		for _, volumeCapability := range volumeCapabilities {
			accessType := volumeCapability.GetAccessType()
			if accessType == nil {
				response := &csi.GetCapacityResponse{
					&csi.GetCapacityResponse_Error{
						&csi.Error{
							&csi.Error_GeneralError_{
								&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The volume_capabilities.access_type field must be specified."},
							},
						},
					},
				}
				return response, false
			}
			accessMode := volumeCapability.GetAccessMode()
			if accessMode == nil {
				response := &csi.GetCapacityResponse{
					&csi.GetCapacityResponse_Error{
						&csi.Error{
							&csi.Error_GeneralError_{
								&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The volume_capabilities.access_mode field must be specified."},
							},
						},
					},
				}
				return response, false
			} else {
				mode := accessMode.GetMode()
				if mode == csi.VolumeCapability_AccessMode_UNKNOWN {
					response := &csi.GetCapacityResponse{
						&csi.GetCapacityResponse_Error{
							&csi.Error{
								&csi.Error_GeneralError_{
									&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The volume_capabilities.access_mode.mode field must be specified."},
								},
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

func (s *Server) ControllerGetCapabilities(
	ctx context.Context,
	request *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	if response, ok := s.validateControllerGetCapabilitiesRequest(request); !ok {
		return response, nil
	}
	capabilities := []*csi.ControllerServiceCapability{
		// CREATE_DELETE_VOLUME
		{
			&csi.ControllerServiceCapability_Rpc{
				&csi.ControllerServiceCapability_RPC{
					csi.ControllerServiceCapability_RPC_CREATE_DELETE_VOLUME,
				},
			},
		},
		// PUBLISH_UNPUBLISH_VOLUME
		//
		//     Not supported by Controller service. This is
		//     performed by the Node service for the Logical
		//     Volume Service.
		//
		// LIST_VOLUMES
		{
			&csi.ControllerServiceCapability_Rpc{
				&csi.ControllerServiceCapability_RPC{
					csi.ControllerServiceCapability_RPC_LIST_VOLUMES,
				},
			},
		},
		// GET_CAPACITY
		{
			&csi.ControllerServiceCapability_Rpc{
				&csi.ControllerServiceCapability_RPC{
					csi.ControllerServiceCapability_RPC_GET_CAPACITY,
				},
			},
		},
	}
	response := &csi.ControllerGetCapabilitiesResponse{
		&csi.ControllerGetCapabilitiesResponse_Result_{
			&csi.ControllerGetCapabilitiesResponse_Result{
				capabilities,
			},
		},
	}
	return response, nil
}

func (s *Server) validateControllerGetCapabilitiesRequest(request *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, bool) {
	version := request.GetVersion()
	if version == nil {
		response := &csi.ControllerGetCapabilitiesResponse{
			&csi.ControllerGetCapabilitiesResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The version must be specified."},
					},
				},
			},
		}
		return response, false
	}
	supportedVersion := false
	for _, v := range s.supportedVersions() {
		if *v == *version {
			supportedVersion = true
			break
		}
	}
	if !supportedVersion {
		response := &csi.ControllerGetCapabilitiesResponse{
			&csi.ControllerGetCapabilitiesResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION, true, "The requested version is not supported."},
					},
				},
			},
		}
		return response, false
	}
	return nil, true
}

// NodeService RPCs

func (s *Server) NodePublishVolume(
	ctx context.Context,
	request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if response, ok := s.validateNodePublishVolumeRequest(request); !ok {
		return response, nil
	}
	response := &csi.NodePublishVolumeResponse{}
	return response, nil
}

func (s *Server) validateNodePublishVolumeRequest(request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, bool) {
	version := request.GetVersion()
	if version == nil {
		response := &csi.NodePublishVolumeResponse{
			&csi.NodePublishVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The version must be specified."},
					},
				},
			},
		}
		return response, false
	}
	supportedVersion := false
	for _, v := range s.supportedVersions() {
		if *v == *version {
			supportedVersion = true
			break
		}
	}
	if !supportedVersion {
		response := &csi.NodePublishVolumeResponse{
			&csi.NodePublishVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION, true, "The requested version is not supported."},
					},
				},
			},
		}
		return response, false
	}
	volumeHandle := request.GetVolumeHandle()
	if volumeHandle == nil {
		response := &csi.NodePublishVolumeResponse{
			&csi.NodePublishVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The volume_handle must be specified."},
					},
				},
			},
		}
		return response, false
	}
	if request.GetPublishVolumeInfo() != nil {
		response := &csi.NodePublishVolumeResponse{
			&csi.NodePublishVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_UNDEFINED, true, "The publish_volume_info field must not be set."},
					},
				},
			},
		}
		return response, false
	}
	targetPath := request.GetTargetPath()
	if targetPath == "" {
		response := &csi.NodePublishVolumeResponse{
			&csi.NodePublishVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The target_path field must be specified."},
					},
				},
			},
		}
		return response, false
	}
	volumeCapability := request.GetVolumeCapability()
	if volumeCapability == nil {
		response := &csi.NodePublishVolumeResponse{
			&csi.NodePublishVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The volume_capability field must be specified."},
					},
				},
			},
		}
		return response, false
	}
	return nil, true
}

func (s *Server) NodeUnpublishVolume(
	ctx context.Context,
	request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if response, ok := s.validateNodeUnpublishVolumeRequest(request); !ok {
		return response, nil
	}
	response := &csi.NodeUnpublishVolumeResponse{}
	return response, nil
}

func (s *Server) validateNodeUnpublishVolumeRequest(request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, bool) {
	version := request.GetVersion()
	if version == nil {
		response := &csi.NodeUnpublishVolumeResponse{
			&csi.NodeUnpublishVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The version must be specified."},
					},
				},
			},
		}
		return response, false
	}
	supportedVersion := false
	for _, v := range s.supportedVersions() {
		if *v == *version {
			supportedVersion = true
			break
		}
	}
	if !supportedVersion {
		response := &csi.NodeUnpublishVolumeResponse{
			&csi.NodeUnpublishVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION, true, "The requested version is not supported."},
					},
				},
			},
		}
		return response, false
	}
	volumeHandle := request.GetVolumeHandle()
	if volumeHandle == nil {
		response := &csi.NodeUnpublishVolumeResponse{
			&csi.NodeUnpublishVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The volume_handle must be specified."},
					},
				},
			},
		}
		return response, false
	}
	targetPath := request.GetTargetPath()
	if targetPath == "" {
		response := &csi.NodeUnpublishVolumeResponse{
			&csi.NodeUnpublishVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The target_path field must be specified."},
					},
				},
			},
		}
		return response, false
	}
	return nil, true
}

func (s *Server) GetNodeID(
	ctx context.Context,
	request *csi.GetNodeIDRequest) (*csi.GetNodeIDResponse, error) {
	if response, ok := s.validateGetNodeIDRequest(request); !ok {
		return response, nil
	}
	response := &csi.GetNodeIDResponse{}
	return response, nil
}

func (s *Server) validateGetNodeIDRequest(request *csi.GetNodeIDRequest) (*csi.GetNodeIDResponse, bool) {
	version := request.GetVersion()
	if version == nil {
		response := &csi.GetNodeIDResponse{
			&csi.GetNodeIDResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The version must be specified."},
					},
				},
			},
		}
		return response, false
	}
	supportedVersion := false
	for _, v := range s.supportedVersions() {
		if *v == *version {
			supportedVersion = true
			break
		}
	}
	if !supportedVersion {
		response := &csi.GetNodeIDResponse{
			&csi.GetNodeIDResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION, true, "The requested version is not supported."},
					},
				},
			},
		}
		return response, false
	}
	return nil, true
}

func (s *Server) ProbeNode(
	ctx context.Context,
	request *csi.ProbeNodeRequest) (*csi.ProbeNodeResponse, error) {
	if response, ok := s.validateProbeNodeRequest(request); !ok {
		return response, nil
	}
	response := &csi.ProbeNodeResponse{}
	return response, nil
}

func (s *Server) validateProbeNodeRequest(request *csi.ProbeNodeRequest) (*csi.ProbeNodeResponse, bool) {
	version := request.GetVersion()
	if version == nil {
		response := &csi.ProbeNodeResponse{
			&csi.ProbeNodeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The version must be specified."},
					},
				},
			},
		}
		return response, false
	}
	supportedVersion := false
	for _, v := range s.supportedVersions() {
		if *v == *version {
			supportedVersion = true
			break
		}
	}
	if !supportedVersion {
		response := &csi.ProbeNodeResponse{
			&csi.ProbeNodeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION, true, "The requested version is not supported."},
					},
				},
			},
		}
		return response, false
	}
	return nil, true
}

func (s *Server) NodeGetCapabilities(
	ctx context.Context,
	request *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	if response, ok := s.validateNodeGetCapabilitiesRequest(request); !ok {
		return response, nil
	}
	response := &csi.NodeGetCapabilitiesResponse{}
	return response, nil
}

func (s *Server) validateNodeGetCapabilitiesRequest(request *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, bool) {
	version := request.GetVersion()
	if version == nil {
		response := &csi.NodeGetCapabilitiesResponse{
			&csi.NodeGetCapabilitiesResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, false, "The version must be specified."},
					},
				},
			},
		}
		return response, false
	}
	supportedVersion := false
	for _, v := range s.supportedVersions() {
		if *v == *version {
			supportedVersion = true
			break
		}
	}
	if !supportedVersion {
		response := &csi.NodeGetCapabilitiesResponse{
			&csi.NodeGetCapabilitiesResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION, true, "The requested version is not supported."},
					},
				},
			},
		}
		return response, false
	}
	return nil, true
}
