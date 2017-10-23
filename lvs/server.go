package lvs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"syscall"

	"golang.org/x/net/context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/mesosphere/csilvm/lvm"
)

const PluginName = "io.mesosphere.dcos.storage/lvs"
const PluginVersion = "1.11.0"

type Server struct {
	vgname               string
	pvnames              []string
	volumeGroup          *lvm.VolumeGroup
	defaultVolumeSize    uint64
	supportedFilesystems map[string]string
	removingVolumeGroup  bool
}

func (s *Server) supportedVersions() []*csi.Version {
	return []*csi.Version{
		&csi.Version{0, 1, 0},
	}
}

// NewServer returns a new Server that will manage the given LVM
// volume group. It accepts a variadic list of ServerOpt with which
// the server's default options can be overwritten.
func NewServer(vgname string, pvnames []string, defaultFs string, opts ...ServerOpt) *Server {
	const (
		// Unless overwritten by the DefaultVolumeSize
		// ServerOpt the default size for new volumes is
		// 10GiB.
		defaultVolumeSize = 10 << 30
	)
	s := &Server{
		vgname,
		pvnames,
		nil,
		defaultVolumeSize,
		map[string]string{
			"":        defaultFs,
			defaultFs: defaultFs,
		},
		false,
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

// RemoveVolumeGroup configures the Server to operate in "remove"
// mode. The volume group will be removed when ProbeNode is
// called. All RPCs other than GetSupportedVersions, GetPluginInfo and
// ProbeNode will fail if the plugin is started in this mode.
func RemoveVolumeGroup() ServerOpt {
	return func(s *Server) {
		s.removingVolumeGroup = true
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
	volumeId := s.volumeNameToId(request.GetName())
	if _, err := s.volumeGroup.LookupLogicalVolume(volumeId); err == nil {
		return ErrCreateVolume_VolumeAlreadyExists(err), nil
	}
	// Determine the capacity, default to maximum size.
	size := s.defaultVolumeSize
	if capacityRange := request.GetCapacityRange(); capacityRange != nil {
		bytesFree, err := s.volumeGroup.BytesFree()
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
	lv, err := s.volumeGroup.CreateLogicalVolume(volumeId, size)
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
	lv, err := s.volumeGroup.LookupLogicalVolume(id)
	if err != nil {
		return ErrDeleteVolume_VolumeDoesNotExist(err), nil
	}
	path, err := lv.Path()
	if err != nil {
		return ErrDeleteVolume_VolumeDoesNotExist(err), nil
	}
	if err := deleteDataOnDevice(path); err != nil {
		return ErrDeleteVolume_GeneralError_Undefined(err), nil
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

func deleteDataOnDevice(devicePath string) error {
	// This method is the go equivalent of
	// `dd if=/dev/zero of=PhysicalVolume`.
	file, err := os.OpenFile(devicePath, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	devzero, err := os.Open("/dev/zero")
	if err != nil {
		return err
	}
	defer devzero.Close()
	if _, err := io.Copy(file, devzero); err != nil {
		// We expect to stop when we get ENOSPC.
		if perr, ok := err.(*os.PathError); ok && perr.Err == syscall.ENOSPC {
			return nil
		}
		return err
	}
	panic("lvs: expected ENOSPC when erasing data")
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
	lv, err := s.volumeGroup.LookupLogicalVolume(id)
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

func (s *Server) volumeNameToId(volname string) string {
	return s.volumeGroup.Name() + "_" + volname
}

func (s *Server) ListVolumes(
	ctx context.Context,
	request *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	if response, ok := s.validateListVolumesRequest(request); !ok {
		return response, nil
	}
	volnames, err := s.volumeGroup.ListLogicalVolumeNames()
	if err != nil {
		return ErrListVolumes_GeneralError_Undefined(err), nil
	}
	var entries []*csi.ListVolumesResponse_Result_Entry
	for _, volname := range volnames {
		lv, err := s.volumeGroup.LookupLogicalVolume(volname)
		if err != nil {
			return ErrListVolumes_GeneralError_Undefined(err), nil
		}
		info := &csi.VolumeInfo{
			lv.SizeInBytes(),
			&csi.VolumeHandle{
				volname,
				nil,
			},
		}
		entry := &csi.ListVolumesResponse_Result_Entry{info}
		entries = append(entries, entry)
	}
	response := &csi.ListVolumesResponse{
		&csi.ListVolumesResponse_Result_{
			&csi.ListVolumesResponse_Result{
				entries,
				"",
			},
		},
	}
	return response, nil
}

func (s *Server) GetCapacity(
	ctx context.Context,
	request *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	if response, ok := s.validateGetCapacityRequest(request); !ok {
		return response, nil
	}
	bytesFree, err := s.volumeGroup.BytesFree()
	if err != nil {
		return ErrGetCapacity_GeneralError_Undefined(err), nil
	}
	response := &csi.GetCapacityResponse{
		&csi.GetCapacityResponse_Result_{
			&csi.GetCapacityResponse_Result{
				bytesFree,
			},
		},
	}
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
	lv, err := s.volumeGroup.LookupLogicalVolume(id)
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
	_, err := s.volumeGroup.LookupLogicalVolume(id)
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
	response := &csi.GetNodeIDResponse{
		&csi.GetNodeIDResponse_Result_{
			&csi.GetNodeIDResponse_Result{},
		},
	}
	return response, nil
}

func zeroPartitionTable(devicePath string) error {
	// This method is the go equivalent of
	// `dd if=/dev/zero of=PhysicalVolume bs=512 count=1`.
	file, err := os.OpenFile(devicePath, os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.Write(bytes.Repeat([]byte{0}, 512)); err != nil {
		return err
	}
	return nil
}

func statDevice(devicePath string) error {
	_, err := os.Stat(devicePath)
	return err
}

// ProbeNode initializes the Server by creating or opening the VolumeGroup.
func (s *Server) ProbeNode(
	ctx context.Context,
	request *csi.ProbeNodeRequest) (*csi.ProbeNodeResponse, error) {
	if response, ok := s.validateProbeNodeRequest(request); !ok {
		return response, nil
	}
	volumeGroup, err := lvm.LookupVolumeGroup(s.vgname)
	if err == lvm.ErrVolumeGroupNotFound {
		if s.removingVolumeGroup {
			// We've been instructed to remove the volume
			// group but it already does not exist. Return
			// success.
			response := &csi.ProbeNodeResponse{
				&csi.ProbeNodeResponse_Result_{
					&csi.ProbeNodeResponse_Result{},
				},
			}
			return response, nil
		}
		// The volume group does not exist yet so see if we can create it.
		// We check if the physical volumes are available.
		var pvs []*lvm.PhysicalVolume
		for _, pvname := range s.pvnames {
			pv, err := lvm.LookupPhysicalVolume(pvname)
			if err == nil {
				pvs = append(pvs, pv)
				continue
			}
			if err == lvm.ErrPhysicalVolumeNotFound {
				// The physical volume cannot be found. Try to create it.
				// First, wipe the partition table on the device in accordance
				// with the `pvcreate` man page.
				if err := statDevice(pvname); err != nil {
					return ErrProbeNode_BadPluginConfig(err), nil
				}
				if err := zeroPartitionTable(pvname); err != nil {
					return ErrProbeNode_GeneralError_Undefined(err), nil
				}
				pv, err := lvm.CreatePhysicalVolume(pvname)
				if err != nil {
					return ErrProbeNode_BadPluginConfig(err), nil
				}
				pvs = append(pvs, pv)
				continue
			}
			return ErrProbeNode_GeneralError_Undefined(err), nil
		}
		volumeGroup, err = lvm.CreateVolumeGroup(s.vgname, pvs)
		if err != nil {
			return ErrProbeNode_GeneralError_Undefined(err), nil
		}

	} else if err != nil {
		return ErrProbeNode_GeneralError_Undefined(err), nil
	}
	// The volume group already exists. We check that the list of
	// physical volumes matches the provided list.
	existing, err := volumeGroup.ListPhysicalVolumeNames()
	if err != nil {
		return ErrProbeNode_GeneralError_Undefined(err), nil
	}
	missing := []string{}
	unexpected := []string{}
	for _, epvname := range existing {
		had := false
		for _, pvname := range s.pvnames {
			if epvname == pvname {
				had = true
				break
			}
		}
		if !had {
			unexpected = append(unexpected, epvname)
		}
	}
	for _, pvname := range s.pvnames {
		had := false
		for _, epvname := range existing {
			if epvname == pvname {
				had = true
				break
			}
		}
		if !had {
			missing = append(missing, pvname)
		}
	}
	if len(missing) != 0 || len(unexpected) != 0 {
		err := fmt.Errorf("Volume group contains unexpected volumes %v and is missing volumes %v", unexpected, missing)
		return ErrProbeNode_BadPluginConfig(err), nil
	}
	// The volume group is configured as expected.
	if s.removingVolumeGroup {
		// The volume group matches our config. We remove it
		// as requested in the startup flags.
		if err := volumeGroup.Remove(); err != nil {
			return ErrProbeNode_GeneralError_Undefined(err), nil
		}
		response := &csi.ProbeNodeResponse{
			&csi.ProbeNodeResponse_Result_{
				&csi.ProbeNodeResponse_Result{},
			},
		}
		return response, nil
	}
	s.volumeGroup = volumeGroup
	response := &csi.ProbeNodeResponse{
		&csi.ProbeNodeResponse_Result_{
			&csi.ProbeNodeResponse_Result{},
		},
	}
	return response, nil
}

func (s *Server) NodeGetCapabilities(
	ctx context.Context,
	request *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	if response, ok := s.validateNodeGetCapabilitiesRequest(request); !ok {
		return response, nil
	}
	response := &csi.NodeGetCapabilitiesResponse{
		&csi.NodeGetCapabilitiesResponse_Result_{
			&csi.NodeGetCapabilitiesResponse_Result{},
		},
	}
	return response, nil
}
