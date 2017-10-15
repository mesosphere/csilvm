package lvs

import (
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"syscall"

	"golang.org/x/net/context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/mesosphere/csilvm/lvm"
)

const PluginName = "com.mesosphere/lvs"
const PluginVersion = "0.1.0"

type Server struct {
	VolumeGroup          *lvm.VolumeGroup
	defaultVolumeSize    uint64
	supportedFilesystems map[string]string
}

func (s *Server) supportedVersions() []*csi.Version {
	return []*csi.Version{
		&csi.Version{0, 1, 0},
	}
}

// NewServer returns a new Server that will manage the given LVM
// volume group. It accepts a variadic list of ServerOpt with which
// the server's default options can be overwritten.
func NewServer(vg *lvm.VolumeGroup, defaultFs string, opts ...ServerOpt) *Server {
	const (
		// Unless overwritten by the DefaultVolumeSize
		// ServerOpt the default size for new volumes is
		// 10GiB.
		defaultVolumeSize = 10 << 30
	)
	s := &Server{
		vg,
		defaultVolumeSize,
		map[string]string{
			"":        defaultFs,
			defaultFs: defaultFs,
		},
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

type ServerOpt func(*Server)

// DefaultVolumeSize sets the default size in bytes of new volumes if
// no volume capacity is specified. To specify that a new volume
// should consist of all available space on the volume group you can
// pass `lvm.MaxSize` to this option.
func DefaultVolumeSize(size uint64) ServerOpt {
	return func(s *Server) {
		s.defaultVolumeSize = size
	}
}

func SupportedFilesystem(fstype string) ServerOpt {
	if fstype == "" {
		panic("lvs: SupportedFilesystem: filesystem type not provided")
	}
	return func(s *Server) {
		s.supportedFilesystems[fstype] = fstype
	}
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
	volumeId := s.VolumeGroup.Name() + "_" + request.GetName()
	if _, err := s.VolumeGroup.LookupLogicalVolume(volumeId); err == nil {
		return ErrCreateVolume_VolumeAlreadyExists(err), nil
	}
	// Determine the capacity, default to maximum size.
	size := s.defaultVolumeSize
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
	lv, err := s.VolumeGroup.CreateLogicalVolume(volumeId, size)
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
						volumeId,
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
	id := request.GetVolumeHandle().GetId()
	lv, err := s.VolumeGroup.LookupLogicalVolume(id)
	if err != nil {
		return ErrDeleteVolume_VolumeDoesNotExist(err), nil
	}
	if err := lv.Remove(); err != nil {
		return ErrDeleteVolume_GeneralError_Undefined(err), nil
	}
	response := &csi.DeleteVolumeResponse{
		&csi.DeleteVolumeResponse_Result_{
			&csi.DeleteVolumeResponse_Result{},
		},
	}
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
	id := request.GetVolumeInfo().GetHandle().GetId()
	lv, err := s.VolumeGroup.LookupLogicalVolume(id)
	if err != nil {
		return ErrValidateVolumeCapabilities_VolumeDoesNotExist(err), nil
	}
	sourcePath, err := lv.Path()
	if err != nil {
		return ErrValidateVolumeCapabilities_GeneralError_Undefined(err), nil
	}
	existingFstype, err := determineFilesystemType(sourcePath)
	if err != nil {
		return ErrValidateVolumeCapabilities_GeneralError_Undefined(err), nil
	}
	for _, capability := range request.GetVolumeCapabilities() {
		if mnt := capability.GetMount(); mnt != nil {
			if existingFstype != "" {
				// The volume has already been formatted.
				if mnt.GetFsType() != "" && existingFstype != mnt.GetFsType() {
					// The requested fstype does not match the existing one.
					response := &csi.ValidateVolumeCapabilitiesResponse{
						&csi.ValidateVolumeCapabilitiesResponse_Result_{
							&csi.ValidateVolumeCapabilitiesResponse_Result{
								false,
								"The requested fs_type does not match the existing filesystem on the volume.",
							},
						},
					}
					return response, nil
				}
			}
		}
	}
	response := &csi.ValidateVolumeCapabilitiesResponse{
		&csi.ValidateVolumeCapabilitiesResponse_Result_{
			&csi.ValidateVolumeCapabilitiesResponse_Result{
				true,
				"",
			},
		},
	}
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
	id := request.GetVolumeHandle().GetId()
	lv, err := s.VolumeGroup.LookupLogicalVolume(id)
	if err != nil {
		return ErrNodePublishVolume_VolumeDoesNotExist(err), nil
	}
	sourcePath, err := lv.Path()
	if err != nil {
		return ErrNodePublishVolume_GeneralError_Undefined(err), nil
	}
	targetPath := request.GetTargetPath()
	switch accessType := request.GetVolumeCapability().GetAccessType().(type) {
	case *csi.VolumeCapability_Block:
		// Perform a bind mount of the raw block device. The
		// `filesystemtype` and `data` parameters to the
		// mount(2) system call are ignored in this case.
		flags := uintptr(syscall.MS_BIND)
		readonly := request.GetVolumeCapability().GetAccessMode().GetMode() == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY
		if readonly {
			flags |= syscall.MS_RDONLY
		}
		if err := syscall.Mount(sourcePath, targetPath, "", flags, ""); err != nil {
			_, ok := err.(syscall.Errno)
			if !ok {
				return ErrNodePublishVolume_GeneralError_Undefined(err), nil
			}
			return ErrNodePublishVolume_MountError(err), nil
		}
	case *csi.VolumeCapability_Mount:
		var flags uintptr
		readonly := request.GetVolumeCapability().GetAccessMode().GetMode() == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY
		if readonly {
			flags |= syscall.MS_RDONLY
		}
		fstype := request.GetVolumeCapability().GetMount().GetFsType()
		// Request validation ensures that the fstype is among
		// our list of supported filesystems.
		if fstype == "" {
			// If the fstype was not specified, pick the default.
			fstype = s.supportedFilesystems[""]
		}
		existingFstype, err := determineFilesystemType(sourcePath)
		if err != nil {
			return ErrNodePublishVolume_GeneralError_Undefined(err), nil
		}
		if existingFstype == "" {
			// There is no existing filesystem on the
			// device, format it with the requested
			// filesystem.
			if err := formatDevice(sourcePath, fstype); err != nil {
				return ErrNodePublishVolume_GeneralError_Undefined(err), nil
			}
			existingFstype = fstype
		}
		if fstype != existingFstype {
			err := errors.New("The volume's existing filesystem does not match the one requested.")
			return ErrNodePublishVolume_MountError(err), nil
		}
		mountOptions := request.GetVolumeCapability().GetMount().GetMountFlags()
		mountOptionsStr := strings.Join(mountOptions, ",")
		// Try to mount the volume by assuming it is correctly formatted.
		if err := syscall.Mount(sourcePath, targetPath, fstype, flags, mountOptionsStr); err != nil {
			_, ok := err.(syscall.Errno)
			if !ok {
				return ErrNodePublishVolume_GeneralError_Undefined(err), nil
			}
			return ErrNodePublishVolume_MountError(err), nil
		}
	default:
		panic(fmt.Sprintf("lvm: unknown access_type: %+v", accessType))
	}
	response := &csi.NodePublishVolumeResponse{
		&csi.NodePublishVolumeResponse_Result_{
			&csi.NodePublishVolumeResponse_Result{},
		},
	}
	return response, nil
}

func determineFilesystemType(devicePath string) (string, error) {
	output, err := exec.Command("lsblk", "-o", "FSTYPE", devicePath).CombinedOutput()
	if err != nil {
		return "", err
	}
	lines := strings.Split(string(output), "\n")
	if len(lines) != 3 || strings.TrimSpace(lines[0]) != "FSTYPE" {
		return "", errors.New("Cannot parse output of lsblk.")
	}
	return strings.TrimSpace(lines[1]), nil
}

func formatDevice(devicePath, fstype string) error {
	output, err := exec.Command("mkfs", "-t", fstype, devicePath).CombinedOutput()
	if err != nil {
		return errors.New("lvs: formatDevice: " + string(output))
	}
	return nil
}

func (s *Server) NodeUnpublishVolume(
	ctx context.Context,
	request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	if response, ok := s.validateNodeUnpublishVolumeRequest(request); !ok {
		return response, nil
	}
	id := request.GetVolumeHandle().GetId()
	_, err := s.VolumeGroup.LookupLogicalVolume(id)
	if err != nil {
		return ErrNodeUnpublishVolume_VolumeDoesNotExist(err), nil
	}
	targetPath := request.GetTargetPath()
	const umountFlags = 0
	if err := syscall.Unmount(targetPath, umountFlags); err != nil {
		_, ok := err.(syscall.Errno)
		if !ok {
			return ErrNodeUnpublishVolume_GeneralError_Undefined(err), nil
		}
		return ErrNodeUnpublishVolume_UnmountError(err), nil
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
