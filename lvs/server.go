package lvs

import (
	"golang.org/x/net/context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/mesosphere/csilvm/lvm"
)

const PluginName = "com.mesosphere/lvs"
const PluginVersion = "0.1.0"

type Server struct {
	VolumeGroup *lvm.VolumeGroup
}

func (s *Server) supportedVersions() []*csi.Version {
	return []*csi.Version{
		&csi.Version{0, 1, 0},
	}
}

// NewServer returns a new Server that will manage the given LVM
// volume group.
func NewServer(vg *lvm.VolumeGroup) *Server {
	return &Server{vg}
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

// ControllerService RPCs

func (s *Server) CreateVolume(
	ctx context.Context,
	request *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if response, ok := s.validateCreateVolumeRequest(request); !ok {
		return response, nil
	}
	// Check whether a logical volume with the given name already
	// exists in this volume group.
	name := request.GetName()
	if _, err := s.VolumeGroup.LookupLogicalVolume(name); err == nil {
		return ErrCreateVolume_VolumeAlreadyExists(err), nil
	}
	// Determine the capacity, default to maximum size.
	size := lvm.MaxSize
	if capacityRange := request.GetCapacityRange(); capacityRange != nil {
		bytesFree, err := s.VolumeGroup.BytesFree()
		if err != nil {
			return ErrCreateVolume_GeneralError_Undefined(err), nil
		}
		// Check whether there is enough free space available.
		if bytesFree < capacityRange.GetRequiredBytes() {
			return ErrCreateVolume_UnsupportedCapacityRange(), nil
		}
		// Set the volume size to the minimum requested  size.
		size = capacityRange.GetRequiredBytes()
	}
	lv, err := s.VolumeGroup.CreateLogicalVolume(name, size)
	if err != nil {
		if lvm.IsInvalidName(err) {
			return ErrCreateVolume_InvalidVolumeName(err), nil
		}
		if err == lvm.ErrNoSpace {
			return ErrCreateVolume_UnsupportedCapacityRange(), nil
		}
		return ErrCreateVolume_GeneralError_Undefined(err), nil
	}
	response := &csi.CreateVolumeResponse{
		&csi.CreateVolumeResponse_Result_{
			&csi.CreateVolumeResponse_Result{
				&csi.VolumeInfo{
					lv.SizeInBytes(),
					&csi.VolumeHandle{
						name,
						nil,
					},
				},
			},
		},
	}
	return response, nil
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

func (s *Server) ListVolumes(
	ctx context.Context,
	request *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	if response, ok := s.validateListVolumesRequest(request); !ok {
		return response, nil
	}
	response := &csi.ListVolumesResponse{}
	return response, nil
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

func (s *Server) NodeUnpublishVolume(
	ctx context.Context,
	request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if response, ok := s.validateNodeUnpublishVolumeRequest(request); !ok {
		return response, nil
	}
	response := &csi.NodeUnpublishVolumeResponse{}
	return response, nil
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

func (s *Server) ProbeNode(
	ctx context.Context,
	request *csi.ProbeNodeRequest) (*csi.ProbeNodeResponse, error) {
	if response, ok := s.validateProbeNodeRequest(request); !ok {
		return response, nil
	}
	response := &csi.ProbeNodeResponse{}
	return response, nil
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
