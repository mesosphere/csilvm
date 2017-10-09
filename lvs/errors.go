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
