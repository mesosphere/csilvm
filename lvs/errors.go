package lvs

import (
	"github.com/container-storage-interface/spec/lib/go/csi"
)

// CreateVolume errors

func ErrCreateVolume_VolumeAlreadyExists(err error) *csi.CreateVolumeResponse {
	return &csi.CreateVolumeResponse{
		&csi.CreateVolumeResponse_Error{
			&csi.Error{
				&csi.Error_CreateVolumeError_{
					&csi.Error_CreateVolumeError{
						csi.Error_CreateVolumeError_VOLUME_ALREADY_EXISTS,
						"A logical volume with that name already exists.",
					},
				},
			},
		},
	}
}

func ErrCreateVolume_UnsupportedCapacityRange() *csi.CreateVolumeResponse {
	return &csi.CreateVolumeResponse{
		&csi.CreateVolumeResponse_Error{
			&csi.Error{
				&csi.Error_CreateVolumeError_{
					&csi.Error_CreateVolumeError{
						csi.Error_CreateVolumeError_UNSUPPORTED_CAPACITY_RANGE,
						"Not enough free space.",
					},
				},
			},
		},
	}
}

func ErrCreateVolume_InvalidVolumeName(err error) *csi.CreateVolumeResponse {
	return &csi.CreateVolumeResponse{
		&csi.CreateVolumeResponse_Error{
			&csi.Error{
				&csi.Error_CreateVolumeError_{
					&csi.Error_CreateVolumeError{
						csi.Error_CreateVolumeError_INVALID_VOLUME_NAME,
						err.Error(),
					},
				},
			},
		},
	}
}

func ErrCreateVolume_GeneralError_Undefined(err error) *csi.CreateVolumeResponse {
	return &csi.CreateVolumeResponse{
		&csi.CreateVolumeResponse_Error{
			&csi.Error{
				&csi.Error_GeneralError_{
					&csi.Error_GeneralError{
						csi.Error_GeneralError_UNDEFINED,
						callerMayRetry,
						err.Error(),
					},
				},
			},
		},
	}
}

// DeleteVolume errors

func ErrDeleteVolume_VolumeDoesNotExist(err error) *csi.DeleteVolumeResponse {
	return &csi.DeleteVolumeResponse{
		&csi.DeleteVolumeResponse_Error{
			&csi.Error{
				&csi.Error_DeleteVolumeError_{
					&csi.Error_DeleteVolumeError{
						csi.Error_DeleteVolumeError_VOLUME_DOES_NOT_EXIST,
						err.Error(),
					},
				},
			},
		},
	}
}

func ErrDeleteVolume_GeneralError_Undefined(err error) *csi.DeleteVolumeResponse {
	return &csi.DeleteVolumeResponse{
		&csi.DeleteVolumeResponse_Error{
			&csi.Error{
				&csi.Error_GeneralError_{
					&csi.Error_GeneralError{
						csi.Error_GeneralError_UNDEFINED,
						callerMayRetry,
						err.Error(),
					},
				},
			},
		},
	}
}

// NodePublishVolume errors

func ErrNodePublishVolume_VolumeDoesNotExist(err error) *csi.NodePublishVolumeResponse {
	return &csi.NodePublishVolumeResponse{
		&csi.NodePublishVolumeResponse_Error{
			&csi.Error{
				&csi.Error_NodePublishVolumeError_{
					&csi.Error_NodePublishVolumeError{
						csi.Error_NodePublishVolumeError_VOLUME_DOES_NOT_EXIST,
						err.Error(),
					},
				},
			},
		},
	}
}

func ErrNodePublishVolume_UnsupportedFsType() *csi.NodePublishVolumeResponse {
	return &csi.NodePublishVolumeResponse{
		&csi.NodePublishVolumeResponse_Error{
			&csi.Error{
				&csi.Error_NodePublishVolumeError_{
					&csi.Error_NodePublishVolumeError{
						csi.Error_NodePublishVolumeError_UNSUPPORTED_FS_TYPE,
						"Requested filesystem type is not supported.",
					},
				},
			},
		},
	}
}

func ErrNodePublishVolume_MountError(err error) *csi.NodePublishVolumeResponse {
	return &csi.NodePublishVolumeResponse{
		&csi.NodePublishVolumeResponse_Error{
			&csi.Error{
				&csi.Error_NodePublishVolumeError_{
					&csi.Error_NodePublishVolumeError{
						csi.Error_NodePublishVolumeError_MOUNT_ERROR,
						err.Error(),
					},
				},
			},
		},
	}
}

func ErrNodePublishVolume_GeneralError_Undefined(err error) *csi.NodePublishVolumeResponse {
	return &csi.NodePublishVolumeResponse{
		&csi.NodePublishVolumeResponse_Error{
			&csi.Error{
				&csi.Error_GeneralError_{
					&csi.Error_GeneralError{
						csi.Error_GeneralError_UNDEFINED,
						callerMayRetry,
						err.Error(),
					},
				},
			},
		},
	}
}

// NodeUnpublishVolume errors

func ErrNodeUnpublishVolume_VolumeDoesNotExist(err error) *csi.NodeUnpublishVolumeResponse {
	return &csi.NodeUnpublishVolumeResponse{
		&csi.NodeUnpublishVolumeResponse_Error{
			&csi.Error{
				&csi.Error_NodeUnpublishVolumeError_{
					&csi.Error_NodeUnpublishVolumeError{
						csi.Error_NodeUnpublishVolumeError_VOLUME_DOES_NOT_EXIST,
						err.Error(),
					},
				},
			},
		},
	}
}

func ErrNodeUnpublishVolume_UnmountError(err error) *csi.NodeUnpublishVolumeResponse {
	return &csi.NodeUnpublishVolumeResponse{
		&csi.NodeUnpublishVolumeResponse_Error{
			&csi.Error{
				&csi.Error_NodeUnpublishVolumeError_{
					&csi.Error_NodeUnpublishVolumeError{
						csi.Error_NodeUnpublishVolumeError_UNMOUNT_ERROR,
						err.Error(),
					},
				},
			},
		},
	}
}

func ErrNodeUnpublishVolume_InvalidVolumeHandle(err error) *csi.NodeUnpublishVolumeResponse {
	return &csi.NodeUnpublishVolumeResponse{
		&csi.NodeUnpublishVolumeResponse_Error{
			&csi.Error{
				&csi.Error_NodeUnpublishVolumeError_{
					&csi.Error_NodeUnpublishVolumeError{
						csi.Error_NodeUnpublishVolumeError_INVALID_VOLUME_HANDLE,
						err.Error(),
					},
				},
			},
		},
	}
}

func ErrNodeUnpublishVolume_GeneralError_Undefined(err error) *csi.NodeUnpublishVolumeResponse {
	return &csi.NodeUnpublishVolumeResponse{
		&csi.NodeUnpublishVolumeResponse_Error{
			&csi.Error{
				&csi.Error_GeneralError_{
					&csi.Error_GeneralError{
						csi.Error_GeneralError_UNDEFINED,
						callerMayRetry,
						err.Error(),
					},
				},
			},
		},
	}
}

// ValdiateVolumeCapabilities errors

func ErrValidateVolumeCapabilities_VolumeDoesNotExist(err error) *csi.ValidateVolumeCapabilitiesResponse {
	return &csi.ValidateVolumeCapabilitiesResponse{
		&csi.ValidateVolumeCapabilitiesResponse_Error{
			&csi.Error{
				&csi.Error_ValidateVolumeCapabilitiesError_{
					&csi.Error_ValidateVolumeCapabilitiesError{
						csi.Error_ValidateVolumeCapabilitiesError_VOLUME_DOES_NOT_EXIST,
						err.Error(),
					},
				},
			},
		},
	}
}

func ErrValidateVolumeCapabilities_UnsupportedFsType() *csi.ValidateVolumeCapabilitiesResponse {
	return &csi.ValidateVolumeCapabilitiesResponse{
		&csi.ValidateVolumeCapabilitiesResponse_Error{
			&csi.Error{
				&csi.Error_ValidateVolumeCapabilitiesError_{
					&csi.Error_ValidateVolumeCapabilitiesError{
						csi.Error_ValidateVolumeCapabilitiesError_UNSUPPORTED_FS_TYPE,
						"Requested filesystem type is not supported.",
					},
				},
			},
		},
	}
}

func ErrValidateVolumeCapabilities_GeneralError_Undefined(err error) *csi.ValidateVolumeCapabilitiesResponse {
	return &csi.ValidateVolumeCapabilitiesResponse{
		&csi.ValidateVolumeCapabilitiesResponse_Error{
			&csi.Error{
				&csi.Error_GeneralError_{
					&csi.Error_GeneralError{
						csi.Error_GeneralError_UNDEFINED,
						callerMayRetry,
						err.Error(),
					},
				},
			},
		},
	}
}
