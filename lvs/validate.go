package lvs

import (
	"github.com/container-storage-interface/spec/lib/go/csi"
)

const (
	callerMustNotRetry = true
	callerMayRetry     = false
)

type versionGetter interface {
	GetVersion() *csi.Version
}

func (s *Server) validateVersion(v versionGetter) *csi.Error {
	version := v.GetVersion()
	if version == nil {
		return &csi.Error{
			&csi.Error_GeneralError_{
				&csi.Error_GeneralError{
					csi.Error_GeneralError_MISSING_REQUIRED_FIELD,
					callerMayRetry,
					"The version field must be specified.",
				},
			},
		}
	}
	supportedVersion := false
	for _, v := range s.supportedVersions() {
		if *v == *version {
			supportedVersion = true
			break
		}
	}
	if !supportedVersion {
		return &csi.Error{
			&csi.Error_GeneralError_{
				&csi.Error_GeneralError{
					csi.Error_GeneralError_UNSUPPORTED_REQUEST_VERSION,
					callerMustNotRetry,
					"The requested version is not supported.",
				},
			},
		}
	}
	return nil
}

// IdentityService RPCs

func (s *Server) validateGetPluginInfoRequest(request *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, bool) {
	if err := s.validateVersion(request); err != nil {
		response := &csi.GetPluginInfoResponse{
			&csi.GetPluginInfoResponse_Error{
				err,
			},
		}
		return response, false
	}
	return nil, true
}

// ControllerService RPCs

func (s *Server) validateCreateVolumeRequest(request *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, bool) {
	if err := s.validateVersion(request); err != nil {
		response := &csi.CreateVolumeResponse{
			&csi.CreateVolumeResponse_Error{
				err,
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
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The name field must be specified."},
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
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_capabilities field must be specified."},
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
							&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "One or more volume_capabilities must be specified."},
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
								&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_capabilities.access_type field must be specified."},
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
								&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_capabilities.access_mode field must be specified."},
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
									&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_capabilities.access_mode.mode field must be specified."},
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

func (s *Server) validateDeleteVolumeRequest(request *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, bool) {
	if err := s.validateVersion(request); err != nil {
		response := &csi.DeleteVolumeResponse{
			&csi.DeleteVolumeResponse_Error{
				err,
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
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_handle field must be specified."},
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
							&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_handle.id field must be specified."},
						},
					},
				},
			}
			return response, false
		}
	}
	return nil, true
}

func (s *Server) validateValidateVolumeCapabilitiesRequest(request *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, bool) {
	if err := s.validateVersion(request); err != nil {
		response := &csi.ValidateVolumeCapabilitiesResponse{
			&csi.ValidateVolumeCapabilitiesResponse_Error{
				err,
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
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_info field must be specified."},
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
							&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_info.handle field must be specified."},
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
								&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_info.handle.id field must be specified."},
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
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_capabilities field must be specified."},
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
							&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "One or more volume_capabilities must be specified."},
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
								&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_capabilities.access_type field must be specified."},
							},
						},
					},
				}
				return response, false
			}
			if mnt := volumeCapability.GetMount(); mnt != nil {
				// This is a MOUNT_VOLUME request.
				fstype := mnt.GetFsType()
				if _, ok := s.supportedFilesystems[fstype]; !ok {
					return ErrValidateVolumeCapabilities_UnsupportedFsType(), false
				}
			}
			accessMode := volumeCapability.GetAccessMode()
			if accessMode == nil {
				response := &csi.ValidateVolumeCapabilitiesResponse{
					&csi.ValidateVolumeCapabilitiesResponse_Error{
						&csi.Error{
							&csi.Error_GeneralError_{
								&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_capabilities.access_mode field must be specified."},
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
									&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_capabilities.access_mode.mode field must be specified."},
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

func (s *Server) validateListVolumesRequest(request *csi.ListVolumesRequest) (*csi.ListVolumesResponse, bool) {
	if err := s.validateVersion(request); err != nil {
		response := &csi.ListVolumesResponse{
			&csi.ListVolumesResponse_Error{
				err,
			},
		}
		return response, false
	}
	return nil, true
}

func (s *Server) validateGetCapacityRequest(request *csi.GetCapacityRequest) (*csi.GetCapacityResponse, bool) {
	if err := s.validateVersion(request); err != nil {
		response := &csi.GetCapacityResponse{
			&csi.GetCapacityResponse_Error{
				err,
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
								&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_capabilities.access_type field must be specified."},
							},
						},
					},
				}
				return response, false
			}
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
			accessMode := volumeCapability.GetAccessMode()
			if accessMode == nil {
				response := &csi.GetCapacityResponse{
					&csi.GetCapacityResponse_Error{
						&csi.Error{
							&csi.Error_GeneralError_{
								&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_capabilities.access_mode field must be specified."},
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
									&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_capabilities.access_mode.mode field must be specified."},
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

func (s *Server) validateControllerGetCapabilitiesRequest(request *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, bool) {
	if err := s.validateVersion(request); err != nil {
		response := &csi.ControllerGetCapabilitiesResponse{
			&csi.ControllerGetCapabilitiesResponse_Error{
				err,
			},
		}
		return response, false
	}
	return nil, true
}

// NodeService RPCs

func (s *Server) validateNodePublishVolumeRequest(request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, bool) {
	if err := s.validateVersion(request); err != nil {
		response := &csi.NodePublishVolumeResponse{
			&csi.NodePublishVolumeResponse_Error{
				err,
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
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_handle field must be specified."},
					},
				},
			},
		}
		return response, false
	} else {
		id := volumeHandle.GetId()
		if id == "" {
			response := &csi.NodePublishVolumeResponse{
				&csi.NodePublishVolumeResponse_Error{
					&csi.Error{
						&csi.Error_GeneralError_{
							&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_handle.id field must be specified."},
						},
					},
				},
			}
			return response, false
		}
	}
	if request.GetPublishVolumeInfo() != nil {
		response := &csi.NodePublishVolumeResponse{
			&csi.NodePublishVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_UNDEFINED, callerMustNotRetry, "The publish_volume_info field must not be specified."},
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
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The target_path field must be specified."},
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
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_capability field must be specified."},
					},
				},
			},
		}
		return response, false
	} else {
		accessType := volumeCapability.GetAccessType()
		if accessType == nil {
			response := &csi.NodePublishVolumeResponse{
				&csi.NodePublishVolumeResponse_Error{
					&csi.Error{
						&csi.Error_GeneralError_{
							&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_capability.access_type field must be specified."},
						},
					},
				},
			}
			return response, false
		}
		if mnt := volumeCapability.GetMount(); mnt != nil {
			// This is a MOUNT_VOLUME request.
			fstype := mnt.GetFsType()
			if _, ok := s.supportedFilesystems[fstype]; !ok {
				return ErrNodePublishVolume_UnsupportedFsType(), false
			}
		}
		accessMode := volumeCapability.GetAccessMode()
		if accessMode == nil {
			response := &csi.NodePublishVolumeResponse{
				&csi.NodePublishVolumeResponse_Error{
					&csi.Error{
						&csi.Error_GeneralError_{
							&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_capability.access_mode field must be specified."},
						},
					},
				},
			}
			return response, false
		} else {
			mode := accessMode.GetMode()
			if mode == csi.VolumeCapability_AccessMode_UNKNOWN {
				response := &csi.NodePublishVolumeResponse{
					&csi.NodePublishVolumeResponse_Error{
						&csi.Error{
							&csi.Error_GeneralError_{
								&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_capability.access_mode.mode field must be specified."},
							},
						},
					},
				}
				return response, false
			}
		}
	}
	return nil, true
}

func (s *Server) validateNodeUnpublishVolumeRequest(request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, bool) {
	if err := s.validateVersion(request); err != nil {
		response := &csi.NodeUnpublishVolumeResponse{
			&csi.NodeUnpublishVolumeResponse_Error{
				err,
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
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_handle field must be specified."},
					},
				},
			},
		}
		return response, false
	} else {
		id := volumeHandle.GetId()
		if id == "" {
			response := &csi.NodeUnpublishVolumeResponse{
				&csi.NodeUnpublishVolumeResponse_Error{
					&csi.Error{
						&csi.Error_GeneralError_{
							&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The volume_handle.id field must be specified."},
						},
					},
				},
			}
			return response, false
		}
	}
	targetPath := request.GetTargetPath()
	if targetPath == "" {
		response := &csi.NodeUnpublishVolumeResponse{
			&csi.NodeUnpublishVolumeResponse_Error{
				&csi.Error{
					&csi.Error_GeneralError_{
						&csi.Error_GeneralError{csi.Error_GeneralError_MISSING_REQUIRED_FIELD, callerMayRetry, "The target_path field must be specified."},
					},
				},
			},
		}
		return response, false
	}
	return nil, true
}

func (s *Server) validateGetNodeIDRequest(request *csi.GetNodeIDRequest) (*csi.GetNodeIDResponse, bool) {
	if err := s.validateVersion(request); err != nil {
		response := &csi.GetNodeIDResponse{
			&csi.GetNodeIDResponse_Error{
				err,
			},
		}
		return response, false
	}
	return nil, true
}

func (s *Server) validateProbeNodeRequest(request *csi.ProbeNodeRequest) (*csi.ProbeNodeResponse, bool) {
	if err := s.validateVersion(request); err != nil {
		response := &csi.ProbeNodeResponse{
			&csi.ProbeNodeResponse_Error{
				err,
			},
		}
		return response, false
	}
	return nil, true
}

func (s *Server) validateNodeGetCapabilitiesRequest(request *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, bool) {
	if err := s.validateVersion(request); err != nil {
		response := &csi.NodeGetCapabilitiesResponse{
			&csi.NodeGetCapabilitiesResponse_Error{
				err,
			},
		}
		return response, false
	}
	return nil, true
}
