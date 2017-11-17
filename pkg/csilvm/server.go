package csilvm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	stdlog "log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"golang.org/x/net/context"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/mesosphere/csilvm/pkg/lvm"
)

const PluginName = "io.mesosphere.dcos.storage/csilvm"
const PluginVersion = "1.11.0"

type logger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
}

var log logger = stdlog.New(os.Stderr, "", stdlog.LstdFlags|stdlog.Lshortfile)

func SetLogger(l logger) {
	log = l
}

type Server struct {
	vgname               string
	pvnames              []string
	volumeGroup          *lvm.VolumeGroup
	defaultVolumeSize    uint64
	supportedFilesystems map[string]string
	removingVolumeGroup  bool
	tags                 []string
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
		nil,
	}
	for _, opt := range opts {
		opt(s)
	}
	log.Printf("NewServer: %v", s)
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
		panic("csilvm: SupportedFilesystem: filesystem type not provided")
	}
	return func(s *Server) {
		s.supportedFilesystems[fstype] = fstype
	}
}

// RemoveVolumeGroup configures the Server to operate in "remove"
// mode. The volume group will be removed when NodeProbe is
// called. All RPCs other than GetSupportedVersions, GetPluginInfo and
// NodeProbe will fail if the plugin is started in this mode.
func RemoveVolumeGroup() ServerOpt {
	return func(s *Server) {
		s.removingVolumeGroup = true
	}
}

// Tag configures the volume group with the specified tag. Any volumes
// that are created will be tagged with the volume group tags.
func Tag(tag string) ServerOpt {
	return func(s *Server) {
		s.tags = append(s.tags, tag)
	}
}

// IdentityService RPCs

func (s *Server) GetSupportedVersions(
	ctx context.Context,
	request *csi.GetSupportedVersionsRequest) (*csi.GetSupportedVersionsResponse, error) {
	log.Printf("Serving GetSupportedVersions: %v", request)
	response := &csi.GetSupportedVersionsResponse{
		s.supportedVersions(),
	}
	return response, nil
}

func (s *Server) GetPluginInfo(
	ctx context.Context,
	request *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	log.Printf("Serving GetPluginInfo: %v", request)
	if err := s.validateGetPluginInfoRequest(request); err != nil {
		log.Printf("GetPluginInfo: failed: %v", err)
		return nil, err
	}
	response := &csi.GetPluginInfoResponse{PluginName, PluginVersion, nil}
	log.Printf("Served GetSupportedVersions: %+v", result)
	return response, nil
}

// ControllerService RPCs

func (s *Server) ControllerProbe(
	ctx context.Context,
	request *csi.ControllerProbeRequest) (*csi.ControllerProbeResponse, error) {
	log.Printf("Serving ControllerProbe: %v", request)
	if err := s.validateControllerProbeRequest(request); err != nil {
		log.Printf("ControllerProbe: failed: %v", err)
		return nil, err
	}
	response := &csi.ControllerProbeResponse{}
	log.Printf("ControllerProbe succeeded")
	return response, nil
}

var ErrVolumeAlreadyExists = status.Error(codes.AlreadyExists, "The volume already exists")
var ErrInsufficientCapacity = status.Error(codes.OutOfRange, "Not enough free space")

func (s *Server) CreateVolume(
	ctx context.Context,
	request *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	log.Printf("Serving CreateVolume: %v", request)
	if err := s.validateCreateVolumeRequest(request); err != nil {
		log.Printf("CreateVolume: failed: %v", err)
		return nil, err
	}
	// Check whether a logical volume with the given name already
	// exists in this volume group.
	volumeId := s.volumeNameToId(request.GetName())
	log.Printf("Determining whether volume with id=%v already exists", volumeId)
	if lv, err := s.volumeGroup.LookupLogicalVolume(volumeId); err == nil {
		log.Printf("Volume %s already exists.", request.GetName())
		response := &csi.CreateVolumeResponse{
			&csi.VolumeInfo{
				lv.SizeInBytes(),
				volumeId,
				nil,
			},
		}
		return response, ErrVolumeAlreadyExists
	}
	log.Printf("Volume with id=%v does not already exist", volumeId)
	// Determine the capacity, default to maximum size.
	size := s.defaultVolumeSize
	if capacityRange := request.GetCapacityRange(); capacityRange != nil {
		bytesFree, err := s.volumeGroup.BytesFree()
		if err != nil {
			log.Printf("Error in BytesFree: err=%v", err)
			return nil, status.Errorf(
				codes.Internal,
				"Error in BytesFree: err=%v",
				err)
		}
		log.Printf("BytesFree: %v", bytesFree)
		// Check whether there is enough free space available.
		if bytesFree < capacityRange.GetRequiredBytes() {
			log.Printf("BytesFree < required_bytes (%d < %d)", bytesFree, capacityRange.GetRequiredBytes())
			return nil, ErrInsufficientCapacity
		}
		// Set the volume size to the minimum requested  size.
		size = capacityRange.GetRequiredBytes()
	}
	log.Printf("Creating logical volume id=%v, size=%v, tags=%v", volumeId, size, s.tags)
	lv, err := s.volumeGroup.CreateLogicalVolume(volumeId, size, s.tags)
	if err != nil {
		if lvm.IsInvalidName(err) {
			log.Printf("Invalid volume name: %v", err)
			return nil, status.Errorf(
				codes.InvalidArgument,
				"The volume name is invalid: err=%v",
				err)
		}
		if err == lvm.ErrNoSpace {
			// Somehow, despite checking for sufficient space
			// above, we still have insuffient free space.
			log.Printf("Not enough free space.")
			return nil, ErrInsufficientCapacity
		}
		log.Printf("Error in CreateLogicalVolume: err=%v", err)
		return nil, status.Errorf(
			codes.Internal,
			"Error in CreateLogicalVolume: err=%v",
			err)
	}
	response := &csi.CreateVolumeResponse{
		&csi.VolumeInfo{
			lv.SizeInBytes(),
			volumeId,
			nil,
		},
	}
	log.Printf("CreateVolume succeeded: volumeId=%v, size=%v", volumeId, lv.SizeInBytes())
	return response, nil
}

func (s *Server) DeleteVolume(
	ctx context.Context,
	request *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	log.Printf("Serving DeleteVolume: %v", request)
	if response, ok := s.validateDeleteVolumeRequest(request); !ok {
		return response, nil
	}
	id := request.GetVolumeHandle().GetId()
	log.Printf("Looking up volume with id=%v", id)
	lv, err := s.volumeGroup.LookupLogicalVolume(id)
	if err != nil {
		log.Printf("Cannot find volume with id=%v", id)
		return ErrDeleteVolume_VolumeDoesNotExist(err), nil
	}
	log.Printf("Determining volume path")
	path, err := lv.Path()
	if err != nil {
		log.Printf("Cannot determine volume path: err=%v", err)
		return ErrDeleteVolume_VolumeDoesNotExist(err), nil
	}
	log.Printf("Deleting data on device %v", path)
	if err := deleteDataOnDevice(path); err != nil {
		log.Printf("Failed to delete data from device: err=%v", err)
		return ErrDeleteVolume_GeneralError_Undefined(err), nil
	}
	log.Printf("Removing volume")
	if err := lv.Remove(); err != nil {
		log.Printf("Failed to remove volume: err=%v", err)
		return ErrDeleteVolume_GeneralError_Undefined(err), nil
	}
	response := &csi.DeleteVolumeResponse{
		&csi.DeleteVolumeResponse_Result_{
			&csi.DeleteVolumeResponse_Result{},
		},
	}
	log.Printf("DeleteVolume succeeded")
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
	panic("csilvm: expected ENOSPC when erasing data")
}

func (s *Server) ControllerPublishVolume(
	ctx context.Context,
	request *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	log.Printf("Serving ControllerPublishVolume: %v", request)
	response := &csi.ControllerPublishVolumeResponse{
		&csi.ControllerPublishVolumeResponse_Error{
			&csi.Error{
				&csi.Error_ControllerPublishVolumeError_{
					&csi.Error_ControllerPublishVolumeError{csi.Error_ControllerPublishVolumeError_CALL_NOT_IMPLEMENTED, "The ControllerPublishVolume RPC is not supported.", nil},
				},
			},
		},
	}
	log.Printf("ControllerPublishVolume not supported")
	return response, nil
}

func (s *Server) ControllerUnpublishVolume(
	ctx context.Context,
	request *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	log.Printf("Serving ControllerUnpublishVolume: %v", request)
	response := &csi.ControllerUnpublishVolumeResponse{
		&csi.ControllerUnpublishVolumeResponse_Error{
			&csi.Error{
				&csi.Error_ControllerUnpublishVolumeError_{
					&csi.Error_ControllerUnpublishVolumeError{csi.Error_ControllerUnpublishVolumeError_CALL_NOT_IMPLEMENTED, "The ControllerUnpublishVolume RPC is not supported."},
				},
			},
		},
	}
	log.Printf("ControllerUnpublishVolume not supported")
	return response, nil
}

func (s *Server) ValidateVolumeCapabilities(
	ctx context.Context,
	request *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	log.Printf("Serving ValidateVolumeCapabilitiesRequest: %v", request)
	if response, ok := s.validateValidateVolumeCapabilitiesRequest(request); !ok {
		return response, nil
	}
	id := request.GetVolumeInfo().GetHandle().GetId()
	log.Printf("Looking up volume with id=%v", id)
	lv, err := s.volumeGroup.LookupLogicalVolume(id)
	if err != nil {
		log.Printf("Cannot find volume with id=%v err=%v", id, err)
		return ErrValidateVolumeCapabilities_VolumeDoesNotExist(err), nil
	}
	log.Printf("Determining volume path")
	sourcePath, err := lv.Path()
	if err != nil {
		log.Printf("Cannot determine volume path: err=%v", err)
		return ErrValidateVolumeCapabilities_GeneralError_Undefined(err), nil
	}
	log.Printf("Determining filesystem type at %v", sourcePath)
	existingFstype, err := determineFilesystemType(sourcePath)
	if err != nil {
		log.Printf("Cannot determine filesystem type: err=%v", err)
		return ErrValidateVolumeCapabilities_GeneralError_Undefined(err), nil
	}
	log.Printf("Existing filesystem type is '%v'", existingFstype)
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
					log.Printf("The volume already has a filesystem. Requested type is %v", mnt.GetFsType())
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
	log.Printf("ValidateVolumeCapabilities succeeded")
	return response, nil
}

func (s *Server) volumeNameToId(volname string) string {
	return s.volumeGroup.Name() + "_" + volname
}

func (s *Server) ListVolumes(
	ctx context.Context,
	request *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	log.Printf("Serving ListVolumes: %v", request)
	if response, ok := s.validateListVolumesRequest(request); !ok {
		return response, nil
	}
	volnames, err := s.volumeGroup.ListLogicalVolumeNames()
	if err != nil {
		log.Printf("Cannot list volume names: err=%v", err)
		return ErrListVolumes_GeneralError_Undefined(err), nil
	}
	var entries []*csi.ListVolumesResponse_Result_Entry
	for _, volname := range volnames {
		log.Printf("Looking up volume '%v'", volname)
		lv, err := s.volumeGroup.LookupLogicalVolume(volname)
		if err != nil {
			log.Printf("Cannot lookup volume '%v': err=%v", volname, err)
			return ErrListVolumes_GeneralError_Undefined(err), nil
		}
		info := &csi.VolumeInfo{
			lv.SizeInBytes(),
			&csi.VolumeHandle{
				volname,
				nil,
			},
		}
		log.Printf("Found volume %v (%v bytes)", volname, lv.SizeInBytes())
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
	log.Printf("ListVolumes succeeded")
	return response, nil
}

func (s *Server) GetCapacity(
	ctx context.Context,
	request *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	log.Printf("Serving GetCapacity: %v", request)
	if response, ok := s.validateGetCapacityRequest(request); !ok {
		return response, nil
	}
	if s.removingVolumeGroup {
		log.Printf("Running with '-remove-volume-group', reporting 0 capacity")
		// We report 0 capacity if configured to remove the volume group.
		response := &csi.GetCapacityResponse{
			&csi.GetCapacityResponse_Result_{
				&csi.GetCapacityResponse_Result{
					0,
				},
			},
		}
		return response, nil
	}
	bytesFree, err := s.volumeGroup.BytesFree()
	if err != nil {
		log.Printf("Error in BytesFree: err=%v", err)
		return ErrGetCapacity_GeneralError_Undefined(err), nil
	}
	log.Printf("BytesFree: %v", bytesFree)
	response := &csi.GetCapacityResponse{
		&csi.GetCapacityResponse_Result_{
			&csi.GetCapacityResponse_Result{
				bytesFree,
			},
		},
	}
	log.Printf("GetCapacity succeeded")
	return response, nil
}

func (s *Server) ControllerGetCapabilities(
	ctx context.Context,
	request *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	if response, ok := s.validateControllerGetCapabilitiesRequest(request); !ok {
		return response, nil
	}
	log.Printf("Serving ControllerGetCapabilities: %v", request)
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
	log.Printf("ControllerGetCapabilities: [CREATE_DELETE_VOLUME, LIST_VOLUMES, GET_CAPACITY]")
	response := &csi.ControllerGetCapabilitiesResponse{
		&csi.ControllerGetCapabilitiesResponse_Result_{
			&csi.ControllerGetCapabilitiesResponse_Result{
				capabilities,
			},
		},
	}
	log.Printf("ControllerGetCapabilities succeeded")
	return response, nil
}

// NodeService RPCs

type simpleError string

func (s simpleError) Error() string { return string(s) }

const (
	ErrFilesystemMismatch = simpleError("The volume's existing filesystem does not match the one requested.")
	ErrTargetPathRO       = simpleError("The targetPath is already mounted read-only.")
	ErrTargetPathRW       = simpleError("The targetPath is already mounted read-write.")
	ErrBlockVolNoRO       = simpleError("Cannot publish block volume with AccessMode SINGLE_NODE_READER_ONLY.")
	ErrTargetPathNotEmpty = simpleError("Unexpected device already mounted at targetPath.")
)

func (s *Server) NodePublishVolume(
	ctx context.Context,
	request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	log.Printf("Serving NodePublishVolume: %v", request)
	if response, ok := s.validateNodePublishVolumeRequest(request); !ok {
		return response, nil
	}
	id := request.GetVolumeHandle().GetId()
	log.Printf("Looking up volume with id=%v", id)
	lv, err := s.volumeGroup.LookupLogicalVolume(id)
	if err != nil {
		log.Printf("Cannot find volume with id=%v", id)
		return ErrNodePublishVolume_VolumeDoesNotExist(err), nil
	}
	log.Printf("Determining volume path")
	sourcePath, err := lv.Path()
	if err != nil {
		log.Printf("Cannot determine volume path: err=%v", err)
		return ErrNodePublishVolume_GeneralError_Undefined(err), nil
	}
	log.Printf("Volume path is %v", sourcePath)
	targetPath := request.GetTargetPath()
	log.Printf("Target path is %v", targetPath)
	switch accessType := request.GetVolumeCapability().GetAccessType().(type) {
	case *csi.VolumeCapability_Block:
		log.Printf("Attempting to publish volume %v as BLOCK_DEVICE to %v", sourcePath, targetPath)
		readonly := request.GetVolumeCapability().GetAccessMode().GetMode() == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY
		if readonly {
			log.Printf("Cannot publish volume: err=%v", ErrBlockVolNoRO)
			return ErrNodePublishVolume_MountError(ErrBlockVolNoRO), nil
		}
		log.Printf("Determining mount info at %v", targetPath)
		// Check whether something is already mounted at targetPath.
		mp, err := getMountAt(targetPath)
		if err != nil {
			log.Printf("Cannot get mount info at %v", targetPath)
			return ErrNodePublishVolume_GeneralError_Undefined(err), nil
		}
		log.Printf("Mount info at %v: %+v", targetPath, mp)
		if mp != nil {
			// With lvm2, the sourcePath is typically a symlink to a
			// devicemapper device, for example:
			//   /dev/some-volume-group/some-logical-volume -> /dev/dm-4
			//
			// However, the mountpoint root shows the actual device, not
			// the symlink. As such, to determine whether or not the
			// device mounted at targetPath is the expected one, we need
			// to resolve the symlink and compare the targets.
			log.Printf("Following symlinks at %v", sourcePath)
			sourceDevicePath, err := filepath.EvalSymlinks(sourcePath)
			if err != nil {
				log.Printf("Failed to follow symlinks at %v: err=%v", sourcePath, err)
				return ErrNodePublishVolume_GeneralError_Undefined(err), nil
			}
			log.Printf("Determined that %v -> %v", sourcePath, sourceDevicePath)
			// For bindmounts, we use the mountpoint root
			// in the current filesystem.
			mpdev := "/dev" + mp.root
			if mpdev != sourceDevicePath {
				return ErrNodePublishVolume_MountError(ErrTargetPathNotEmpty), nil
			}
			log.Printf("The volume %v is already bind mounted to %v", sourcePath, targetPath)
			// For bind mounts, the filesystemtype and
			// mount options are ignored.
			response := &csi.NodePublishVolumeResponse{
				&csi.NodePublishVolumeResponse_Result_{
					&csi.NodePublishVolumeResponse_Result{},
				},
			}
			log.Printf("NodePublishVolume succeeded (idempotent)")
			return response, nil
		}
		log.Printf("Nothing mounted at targetPath %v yet", targetPath)
		// Perform a bind mount of the raw block device. The
		// `filesystemtype` and `data` parameters to the
		// mount(2) system call are ignored in this case.
		flags := uintptr(syscall.MS_BIND)
		log.Printf("Performing bind mount of %s -> %s", sourcePath, targetPath)
		if err := syscall.Mount(sourcePath, targetPath, "", flags, ""); err != nil {
			log.Printf("Failed to perform bind mount: err=%v", err)
			_, ok := err.(syscall.Errno)
			if !ok {
				return ErrNodePublishVolume_GeneralError_Undefined(err), nil
			}
			return ErrNodePublishVolume_MountError(err), nil
		}
	case *csi.VolumeCapability_Mount:
		log.Printf("Attempting to publish volume %v as MOUNT_DEVICE to %v", sourcePath, targetPath)
		var flags uintptr
		readonly := request.GetVolumeCapability().GetAccessMode().GetMode() == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY
		if readonly {
			flags |= syscall.MS_RDONLY
		}
		fstype := request.GetVolumeCapability().GetMount().GetFsType()
		// Request validation ensures that the fstype is among
		// our list of supported filesystems.
		log.Printf("Requested filesystem type is '%v'", fstype)
		if fstype == "" {
			// If the fstype was not specified, pick the default.
			fstype = s.supportedFilesystems[""]
			log.Printf("No specific filesystem type requested, defaulting to %v", fstype)
		}
		// Check whether something is already mounted at targetPath.
		log.Printf("Determining mount info at %v", targetPath)
		mp, err := getMountAt(targetPath)
		if err != nil {
			log.Printf("Cannot get mount info at %v", targetPath)
			return ErrNodePublishVolume_GeneralError_Undefined(err), nil
		}
		log.Printf("Mount info at %v: %+v", targetPath, mp)
		if mp != nil {
			// For regular mounts, we use the mount source.
			if mp.mountsource != sourcePath {
				log.Printf("TargetPath %s not empty: err=%v", targetPath, ErrTargetPathNotEmpty)
				return ErrNodePublishVolume_MountError(ErrTargetPathNotEmpty), nil
			}
			// Something is mounted at targetPath. We
			// check that the filesystem matches the
			// requested one and that the readonly status
			// matches the requested readonly status. If
			// so, to support idempotency we return
			// success, otherwise we return an error as
			// the targetPath is not mounted in the
			// requested way.
			if mp.fstype != fstype {
				log.Printf("Device mounted at %v has unexpected filesystem type (%v != %v)", mp.fstype, fstype)
				return ErrNodePublishVolume_MountError(ErrFilesystemMismatch), nil
			}
			if mp.isReadonly() != readonly {
				if mp.isReadonly() {
					log.Printf("%v", ErrTargetPathRO)
					return ErrNodePublishVolume_MountError(ErrTargetPathRO), nil
				} else {
					log.Printf("%v", ErrTargetPathRW)
					return ErrNodePublishVolume_MountError(ErrTargetPathRW), nil
				}
			}
			// The device, fstype and readonly option of
			// the filesystem at targetPath matches that
			// which is requested, to support idempotency
			// we return success.
			response := &csi.NodePublishVolumeResponse{
				&csi.NodePublishVolumeResponse_Result_{
					&csi.NodePublishVolumeResponse_Result{},
				},
			}
			log.Printf("NodePublishVolume succeeded (idempotent)")
			return response, nil
		}
		log.Printf("Determining filesystem type at %v", sourcePath)
		existingFstype, err := determineFilesystemType(sourcePath)
		if err != nil {
			log.Printf("Cannot determine filesystem type: err=%v", err)
			return ErrNodePublishVolume_GeneralError_Undefined(err), nil
		}
		log.Printf("Existing filesystem type is '%v'", existingFstype)
		if existingFstype == "" {
			// There is no existing filesystem on the
			// device, format it with the requested
			// filesystem.
			log.Printf("The device %v has no existing filesystem, formatting with %v", sourcePath, fstype)
			if err := formatDevice(sourcePath, fstype); err != nil {
				log.Printf("formatDevice failed: err=%v", err)
				return ErrNodePublishVolume_GeneralError_Undefined(err), nil
			}
			existingFstype = fstype
		}
		if fstype != existingFstype {
			log.Printf("Requested fstype %v does not match existing fstype %v", fstype, existingFstype)
			return ErrNodePublishVolume_MountError(ErrFilesystemMismatch), nil
		}
		mountOptions := request.GetVolumeCapability().GetMount().GetMountFlags()
		mountOptionsStr := strings.Join(mountOptions, ",")
		// Try to mount the volume by assuming it is correctly formatted.
		log.Printf("Mounting %v at %v fstype=%v, flags=%v mountOptions=%v", sourcePath, targetPath, fstype, flags, mountOptionsStr)
		if err := syscall.Mount(sourcePath, targetPath, fstype, flags, mountOptionsStr); err != nil {
			log.Printf("Failed to perform mount: err=%v", err)
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
	log.Printf("NodePublishVolume succeeded")
	return response, nil
}

func determineFilesystemType(devicePath string) (string, error) {
	output, err := exec.Command("lsblk", "-P", "-o", "FSTYPE", devicePath).CombinedOutput()
	if err != nil {
		return "", err
	}
	parseErr := errors.New("Cannot parse output of lsblk.")
	lines := strings.Split(string(output), "\n")
	if len(lines) != 2 {
		return "", parseErr
	}
	if lines[1] != "" {
		return "", parseErr
	}
	line := lines[0]
	const prefix = "FSTYPE=\""
	const suffix = "\""
	if !strings.HasPrefix(line, prefix) || !strings.HasSuffix(line, suffix) {
		return "", parseErr
	}
	line = strings.TrimPrefix(line, prefix)
	line = strings.TrimSuffix(line, suffix)
	return line, nil
}

func formatDevice(devicePath, fstype string) error {
	output, err := exec.Command("mkfs", "-t", fstype, devicePath).CombinedOutput()
	if err != nil {
		return errors.New("csilvm: formatDevice: " + string(output))
	}
	return nil
}

func (s *Server) NodeUnpublishVolume(
	ctx context.Context,
	request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	log.Printf("Serving NodeUnpublishVolume: %v", request)
	if response, ok := s.validateNodeUnpublishVolumeRequest(request); !ok {
		return response, nil
	}
	id := request.GetVolumeHandle().GetId()
	log.Printf("Looking up volume with id=%v", id)
	_, err := s.volumeGroup.LookupLogicalVolume(id)
	if err != nil {
		log.Printf("Cannot find volume with id=%v", id)
		return ErrNodeUnpublishVolume_VolumeDoesNotExist(err), nil
	}
	targetPath := request.GetTargetPath()
	log.Printf("Determining mount info at %v", targetPath)
	mp, err := getMountAt(targetPath)
	if err != nil {
		log.Printf("Cannot get mount info at %v", targetPath)
		return ErrNodeUnpublishVolume_GeneralError_Undefined(err), nil
	}
	log.Printf("Mount info at %v: %+v", targetPath, mp)
	if mp == nil {
		log.Printf("Nothing mounted at %v", targetPath)
		// There is nothing mounted at targetPath, to support
		// idempotency we return success.
		response := &csi.NodeUnpublishVolumeResponse{}
		log.Printf("NodeUnpublishVolume succeeded (idempotent)")
		return response, nil
	}
	const umountFlags = 0
	log.Printf("Unmounting %v", targetPath)
	if err := syscall.Unmount(targetPath, umountFlags); err != nil {
		log.Printf("Failed to perform unmount: err=%v", err)
		_, ok := err.(syscall.Errno)
		if !ok {
			return ErrNodeUnpublishVolume_GeneralError_Undefined(err), nil
		}
		return ErrNodeUnpublishVolume_UnmountError(err), nil
	}
	response := &csi.NodeUnpublishVolumeResponse{}
	log.Printf("NodeUnpublishVolume succeeded")
	return response, nil
}

func (s *Server) GetNodeID(
	ctx context.Context,
	request *csi.GetNodeIDRequest) (*csi.GetNodeIDResponse, error) {
	log.Printf("Serving GetNodeID: %v", request)
	if response, ok := s.validateGetNodeIDRequest(request); !ok {
		return response, nil
	}
	response := &csi.GetNodeIDResponse{
		&csi.GetNodeIDResponse_Result_{
			&csi.GetNodeIDResponse_Result{},
		},
	}
	log.Printf("GetNodeID succeeded")
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

// NodeProbe initializes the Server by creating or opening the VolumeGroup.
func (s *Server) NodeProbe(
	ctx context.Context,
	request *csi.NodeProbeRequest) (*csi.NodeProbeResponse, error) {
	log.Printf("Serving NodeProbe: %v", request)
	if response, ok := s.validateNodeProbeRequest(request); !ok {
		return response, nil
	}
	log.Printf("Validating tags: %v", s.tags)
	for _, tag := range s.tags {
		if err := lvm.ValidateTag(tag); err != nil {
			return ErrNodeProbe_BadPluginConfig(err), nil
		}
	}
	log.Printf("Looking up volume group %v", s.vgname)
	volumeGroup, err := lvm.LookupVolumeGroup(s.vgname)
	if err == lvm.ErrVolumeGroupNotFound {
		if s.removingVolumeGroup {
			// We've been instructed to remove the volume
			// group but it already does not exist. Return
			// success.
			response := &csi.NodeProbeResponse{
				&csi.NodeProbeResponse_Result_{
					&csi.NodeProbeResponse_Result{},
				},
			}
			log.Printf("Running in '-remove-volume-group' mode and volume group cannot be found.")
			log.Printf("NodeProbe succeeded")
			return response, nil
		}
		log.Printf("Cannot find volume group %v", s.vgname)
		// The volume group does not exist yet so see if we can create it.
		// We check if the physical volumes are available.
		log.Printf("Getting LVM2 physical volumes %v", s.pvnames)
		var pvs []*lvm.PhysicalVolume
		for _, pvname := range s.pvnames {
			log.Printf("Looking up LVM2 physical volume %v", pvname)
			pv, err := lvm.LookupPhysicalVolume(pvname)
			if err == nil {
				log.Printf("Found LVM2 physical volume %v", pvname)
				pvs = append(pvs, pv)
				continue
			}
			if err == lvm.ErrPhysicalVolumeNotFound {
				log.Printf("Cannot find LVM2 physical volume %v", pvname)
				// The physical volume cannot be found. Try to create it.
				// First, wipe the partition table on the device in accordance
				// with the `pvcreate` man page.
				if err := statDevice(pvname); err != nil {
					log.Printf("Stat device %v: err=%v", pvname, err)
					return ErrNodeProbe_BadPluginConfig(err), nil
				}
				log.Printf("Stat device %v", pvname)
				log.Printf("Zeroing partition table on %v", pvname)
				if err := zeroPartitionTable(pvname); err != nil {
					log.Printf("Cannot zero partition table on %v: err=%v", pvname, err)
					return ErrNodeProbe_GeneralError_Undefined(err), nil
				}
				log.Printf("Creating LVM2 physical volume %v", pvname)
				pv, err := lvm.CreatePhysicalVolume(pvname)
				if err != nil {
					log.Printf("Cannot create LVM2 physical volume %v: err=%v", pvname, err)
					return ErrNodeProbe_BadPluginConfig(err), nil
				}
				log.Printf("Created LVM2 physical volume %v", pvname)
				pvs = append(pvs, pv)
				continue
			}
			return ErrNodeProbe_GeneralError_Undefined(err), nil
		}
		log.Printf("Creating volume group %v with physical volumes %v and tags %v", s.vgname, s.pvnames, s.tags)
		volumeGroup, err = lvm.CreateVolumeGroup(s.vgname, pvs, s.tags)
		if err != nil {
			log.Printf("Cannot create volume group %v: err=%v", s.vgname, err)
			return ErrNodeProbe_GeneralError_Undefined(err), nil
		}
		log.Printf("Created volume group %v", s.vgname)
	} else if err != nil {
		log.Printf("Failed to lookup volume group %v: err=%v", s.vgname, err)
		return ErrNodeProbe_GeneralError_Undefined(err), nil
	}
	log.Printf("Found volume group %v", s.vgname)
	// The volume group already exists. We check that the list of
	// physical volumes matches the provided list.
	log.Printf("Listing physical volumes in volume group %s", s.vgname)
	existing, err := volumeGroup.ListPhysicalVolumeNames()
	if err != nil {
		log.Printf("Failed to list physical volumes: err=%v", err)
		return ErrNodeProbe_GeneralError_Undefined(err), nil
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
		log.Printf("Unexpected physical volumes in volume group: err=%v", err)
		return ErrNodeProbe_BadPluginConfig(err), nil
	}
	// We check that the volume group tags match those we expect.
	log.Printf("Looking up volume group tags")
	tags, err := volumeGroup.Tags()
	if err != nil {
		log.Printf("Failed to lookup tags: err=%v", err)
		return ErrNodeProbe_GeneralError_Undefined(err), nil
	}
	log.Printf("Volume group tags: %v", tags)
	if err := s.checkVolumeGroupTags(tags); err != nil {
		log.Printf("Volume group tags did not match expected: err=%v", err)
		return err, nil
	}
	// The volume group is configured as expected.
	log.Printf("Volume group matches configuration")
	if s.removingVolumeGroup {
		log.Printf("Running with '-remove-volume-group'.")
		// The volume group matches our config. We remove it
		// as requested in the startup flags.
		log.Printf("Removing volume group %v", s.vgname)
		if err := volumeGroup.Remove(); err != nil {
			log.Printf("Failed to remove volume group: err=%v", err)
			return ErrNodeProbe_GeneralError_Undefined(err), nil
		}
		log.Printf("Removed volume group %v", s.vgname)
		response := &csi.NodeProbeResponse{
			&csi.NodeProbeResponse_Result_{
				&csi.NodeProbeResponse_Result{},
			},
		}
		log.Printf("NodeProbe succeeded")
		return response, nil
	}
	s.volumeGroup = volumeGroup
	response := &csi.NodeProbeResponse{
		&csi.NodeProbeResponse_Result_{
			&csi.NodeProbeResponse_Result{},
		},
	}
	log.Printf("NodeProbe succeeded")
	return response, nil
}

func (s *Server) checkVolumeGroupTags(tags []string) *csi.NodeProbeResponse {
	if len(tags) != len(s.tags) {
		err := fmt.Errorf("csilvm: Configured tags don't match existing tags: %v != %v", s.tags, tags)
		return ErrNodeProbe_BadPluginConfig(err)
	}
	for _, t1 := range tags {
		had := false
		for _, t2 := range s.tags {
			if t1 == t2 {
				had = true
				break
			}
		}
		if !had {
			err := fmt.Errorf("csilvm: Configured tags don't match existing tags: %v != %v", s.tags, tags)
			return ErrNodeProbe_BadPluginConfig(err)
		}
	}
	return nil
}

func (s *Server) NodeGetCapabilities(
	ctx context.Context,
	request *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	log.Printf("Serving NodeGetCapabilities: %v", request)
	if response, ok := s.validateNodeGetCapabilitiesRequest(request); !ok {
		return response, nil
	}
	response := &csi.NodeGetCapabilitiesResponse{
		&csi.NodeGetCapabilitiesResponse_Result_{
			&csi.NodeGetCapabilitiesResponse_Result{},
		},
	}
	log.Printf("NodeGetCapabilities succeeded")
	return response, nil
}
