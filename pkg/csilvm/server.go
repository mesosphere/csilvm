package csilvm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/container-storage-interface/spec/lib/go/csi"
	"github.com/mesosphere/csilvm/pkg/lvm"
	"golang.org/x/net/context"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const PluginName = "io.mesosphere.dcos.storage/csilvm"
const PluginVersion = "1.11.0"

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
	response := &csi.GetSupportedVersionsResponse{
		s.supportedVersions(),
	}
	return response, nil
}

func (s *Server) GetPluginInfo(
	ctx context.Context,
	request *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {
	if err := s.validateGetPluginInfoRequest(request); err != nil {
		return nil, err
	}
	response := &csi.GetPluginInfoResponse{PluginName, PluginVersion, nil}
	return response, nil
}

// ControllerService RPCs

func (s *Server) ControllerProbe(
	ctx context.Context,
	request *csi.ControllerProbeRequest) (*csi.ControllerProbeResponse, error) {
	if err := s.validateControllerProbeRequest(request); err != nil {
		return nil, err
	}
	response := &csi.ControllerProbeResponse{}
	return response, nil
}

var ErrVolumeAlreadyExists = status.Error(codes.AlreadyExists, "The volume already exists")
var ErrInsufficientCapacity = status.Error(codes.OutOfRange, "Not enough free space")

func (s *Server) CreateVolume(
	ctx context.Context,
	request *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	if err := s.validateCreateVolumeRequest(request); err != nil {
		return nil, err
	}
	// Check whether a logical volume with the given name already
	// exists in this volume group.
	volumeId := s.volumeNameToId(request.GetName())
	log.Printf("Determining whether volume with id=%v already exists", volumeId)
	if lv, err := s.volumeGroup.LookupLogicalVolume(volumeId); err == nil {
		log.Printf("Volume %s already exists.", request.GetName())
		// The volume already exists. Determine whether or not the
		// existing volume satisfies the request. If so, return a
		// successful response. If not, return ErrVolumeAlreadyExists.
		if err := s.validateExistingVolume(lv, request); err != nil {
			return nil, err
		}
		response := &csi.CreateVolumeResponse{
			&csi.VolumeInfo{
				lv.SizeInBytes(),
				lv.Name(),
				nil,
			},
		}
		return response, nil
	}
	log.Printf("Volume with id=%v does not already exist", volumeId)
	// Determine the capacity, default to maximum size.
	size := s.defaultVolumeSize
	if capacityRange := request.GetCapacityRange(); capacityRange != nil {
		bytesFree, err := s.volumeGroup.BytesFree()
		if err != nil {
			return nil, status.Errorf(
				codes.Internal,
				"Error in BytesFree: err=%v",
				err)
		}
		log.Printf("BytesFree: %v", bytesFree)
		// Check whether there is enough free space available.
		if bytesFree < capacityRange.GetRequiredBytes() {
			return nil, ErrInsufficientCapacity
		}
		// Set the volume size to the minimum requested  size.
		size = capacityRange.GetRequiredBytes()
	}
	log.Printf("Creating logical volume id=%v, size=%v, tags=%v", volumeId, size, s.tags)
	lv, err := s.volumeGroup.CreateLogicalVolume(volumeId, size, s.tags)
	if err != nil {
		if lvm.IsInvalidName(err) {
			return nil, status.Errorf(
				codes.InvalidArgument,
				"The volume name is invalid: err=%v",
				err)
		}
		if err == lvm.ErrNoSpace {
			// Somehow, despite checking for sufficient space
			// above, we still have insuffient free space.
			return nil, ErrInsufficientCapacity
		}
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
	return response, nil
}

func (s *Server) validateExistingVolume(lv *lvm.LogicalVolume, request *csi.CreateVolumeRequest) error {
	// Determine whether the existing volume satisfies the capacity_range
	// of the current request.
	if capacityRange := request.GetCapacityRange(); capacityRange != nil {
		// If required_bytes is specified, is that requirement
		// satisfied by the existing volume?
		if requiredBytes := capacityRange.GetRequiredBytes(); requiredBytes != 0 {
			if requiredBytes > lv.SizeInBytes() {
				log.Printf("Existing volume does not satisfy request: required_bytes > volume size (%d > %d)", requiredBytes, lv.SizeInBytes())
				// The existing volume is not big enough.
				return ErrVolumeAlreadyExists
			}
		}
		if limitBytes := capacityRange.GetLimitBytes(); limitBytes != 0 {
			if limitBytes < lv.SizeInBytes() {
				log.Printf("Existing volume does not satisfy request: limit_bytes < volume size (%d < %d)", limitBytes, lv.SizeInBytes())
				// The existing volume is too big.
				return ErrVolumeAlreadyExists
			}
		}
		// We know that one of limit_bytes or required_bytes was
		// specified, thanks to the specification and the request
		// validation logic.
	}
	// The existing volume matches the requested capacity_range.  We
	// determine whether the existing volume satisfies all requested
	// volume_capabilities.
	sourcePath, err := lv.Path()
	if err != nil {
		return status.Errorf(
			codes.Internal,
			"Error in Path(): err=%v",
			err)
	}
	log.Printf("Volume path is %v", sourcePath)
	existingFsType, err := determineFilesystemType(sourcePath)
	if err != nil {
		return status.Errorf(
			codes.Internal,
			"Cannot determine filesystem type: err=%v",
			err)
	}
	log.Printf("Existing filesystem type is '%v'", existingFsType)
	for _, volumeCapability := range request.GetVolumeCapabilities() {
		if mnt := volumeCapability.GetMount(); mnt != nil {
			// This is a MOUNT_VOLUME capability. We know that the
			// requested filesystem type is supported on this host
			// thanks to the request validation logic.
			if existingFsType != "" {
				// The volume has already been formatted with
				// some filesystem. If the requested
				// volume_capability.fs_type is different to
				// the filesystem already on the volume, then
				// this volume_capability is unsatisfiable
				// using the existing volume and we return an
				// error.
				requestedFstype := mnt.GetFsType()
				if requestedFstype != "" && requestedFstype != existingFsType {
					// The existing volume is already
					// formatted with a filesystem that
					// does not match the requested
					// volume_capability so it does not
					// satisfy the request.
					log.Printf("Existing volume does not satisfy request: fs_type != volume fs (%v != %v)", requestedFstype, existingFsType)
					return ErrVolumeAlreadyExists
				}
				// The existing volume satisfies this
				// volume_capability.
			} else {
				// The existing volume has not been formatted
				// with a filesystem and can therefore satisfy
				// this volume_capability (by formatting it
				// with the specified fs_type, whatever it is).
			}
			// We ignore whether or not the volume_capability
			// specifies readonly as any filesystem can be mounted
			// readonly or not depending on how it gets published.
		}
	}
	return nil
}

var ErrVolumeNotFound = status.Error(codes.NotFound, "The volume does not exist.")

func (s *Server) DeleteVolume(
	ctx context.Context,
	request *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	if err := s.validateDeleteVolumeRequest(request); err != nil {
		return nil, err
	}
	id := request.GetVolumeId()
	log.Printf("Looking up volume with id=%v", id)
	lv, err := s.volumeGroup.LookupLogicalVolume(id)
	if err != nil {
		return nil, ErrVolumeNotFound
	}
	log.Printf("Determining volume path")
	path, err := lv.Path()
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Error in Path(): err=%v",
			err)
	}
	log.Printf("Deleting data on device %v", path)
	if err := deleteDataOnDevice(path); err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Cannot delete data from device: err=%v",
			err)
	}
	log.Printf("Removing volume")
	if err := lv.Remove(); err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Failed to remove volume: err=%v",
			err)
	}
	response := &csi.DeleteVolumeResponse{}
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

var ErrCallNotImplemented = status.Error(codes.Unimplemented, "That RPC is not implemented.")

func (s *Server) ControllerPublishVolume(
	ctx context.Context,
	request *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	log.Printf("ControllerPublishVolume not supported")
	return nil, ErrCallNotImplemented
}

func (s *Server) ControllerUnpublishVolume(
	ctx context.Context,
	request *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	log.Printf("ControllerUnpublishVolume not supported")
	return nil, ErrCallNotImplemented
}

var ErrMismatchedFilesystemType = status.Error(
	codes.InvalidArgument,
	"The requested fs_type does not match the existing filesystem on the volume.")

func (s *Server) ValidateVolumeCapabilities(
	ctx context.Context,
	request *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	if err := s.validateValidateVolumeCapabilitiesRequest(request); err != nil {
		return nil, err
	}
	id := request.GetVolumeId()
	log.Printf("Looking up volume with id=%v", id)
	lv, err := s.volumeGroup.LookupLogicalVolume(id)
	if err != nil {
		return nil, ErrVolumeNotFound
	}
	log.Printf("Determining volume path")
	sourcePath, err := lv.Path()
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Error in Path(): err=%v",
			err)
	}
	log.Printf("Determining filesystem type at %v", sourcePath)
	existingFstype, err := determineFilesystemType(sourcePath)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Cannot determine filesystem type: err=%v",
			err)
	}
	log.Printf("Existing filesystem type is '%v'", existingFstype)
	for _, capability := range request.GetVolumeCapabilities() {
		if mnt := capability.GetMount(); mnt != nil {
			if existingFstype != "" {
				// The volume has already been formatted.
				if mnt.GetFsType() != "" && existingFstype != mnt.GetFsType() {
					// The requested fstype does not match the existing one.
					return nil, ErrMismatchedFilesystemType
				}
			}
		}
	}
	response := &csi.ValidateVolumeCapabilitiesResponse{
		true,
		"",
	}
	return response, nil
}

func (s *Server) volumeNameToId(volname string) string {
	return s.volumeGroup.Name() + "_" + volname
}

func (s *Server) ListVolumes(
	ctx context.Context,
	request *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	if err := s.validateListVolumesRequest(request); err != nil {
		return nil, err
	}
	volnames, err := s.volumeGroup.ListLogicalVolumeNames()
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Cannot list volume names: err=%v",
			err)
	}
	var entries []*csi.ListVolumesResponse_Entry
	for _, volname := range volnames {
		log.Printf("Looking up volume '%v'", volname)
		lv, err := s.volumeGroup.LookupLogicalVolume(volname)
		if err != nil {
			return nil, ErrVolumeNotFound
		}
		info := &csi.VolumeInfo{
			lv.SizeInBytes(),
			volname,
			nil,
		}
		log.Printf("Found volume %v (%v bytes)", volname, lv.SizeInBytes())
		entry := &csi.ListVolumesResponse_Entry{info}
		entries = append(entries, entry)
	}
	response := &csi.ListVolumesResponse{
		entries,
		"",
	}
	return response, nil
}

func (s *Server) GetCapacity(
	ctx context.Context,
	request *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	if err := s.validateGetCapacityRequest(request); err != nil {
		return nil, err
	}
	if s.removingVolumeGroup {
		log.Printf("Running with '-remove-volume-group', reporting 0 capacity")
		// We report 0 capacity if configured to remove the volume group.
		response := &csi.GetCapacityResponse{0}
		return response, nil
	}
	for _, volumeCapability := range request.GetVolumeCapabilities() {
		// Check for unsupported filesystem type in order to return 0
		// capacity if it isn't supported.
		if mnt := volumeCapability.GetMount(); mnt != nil {
			// This is a MOUNT_VOLUME request.
			fstype := mnt.GetFsType()
			if _, ok := s.supportedFilesystems[fstype]; !ok {
				// Zero capacity for unsupported filesystem type.
				response := &csi.GetCapacityResponse{0}
				return response, nil
			}
		}
	}
	bytesFree, err := s.volumeGroup.BytesFree()
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Error in BytesFree: err=%v",
			err)
	}
	log.Printf("BytesFree: %v", bytesFree)
	response := &csi.GetCapacityResponse{bytesFree}
	return response, nil
}

func (s *Server) ControllerGetCapabilities(
	ctx context.Context,
	request *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
	if err := s.validateControllerGetCapabilitiesRequest(request); err != nil {
		return nil, err
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
	response := &csi.ControllerGetCapabilitiesResponse{capabilities}
	return response, nil
}

// NodeService RPCs

var ErrTargetPathNotEmpty = status.Error(
	codes.InvalidArgument,
	"Unexpected device already mounted at targetPath.")

var ErrTargetPathRO = status.Error(
	codes.InvalidArgument,
	"The targetPath is already mounted readonly.")

var ErrTargetPathRW = status.Error(
	codes.InvalidArgument,
	"The targetPath is already mounted read-write.")

func (s *Server) NodePublishVolume(
	ctx context.Context,
	request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	if err := s.validateNodePublishVolumeRequest(request); err != nil {
		return nil, err
	}
	id := request.GetVolumeId()
	log.Printf("Looking up volume with id=%v", id)
	lv, err := s.volumeGroup.LookupLogicalVolume(id)
	if err != nil {
		return nil, ErrVolumeNotFound
	}
	log.Printf("Determining volume path")
	sourcePath, err := lv.Path()
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Error in Path(): err=%v",
			err)
	}
	log.Printf("Volume path is %v", sourcePath)
	targetPath := request.GetTargetPath()
	log.Printf("Target path is %v", targetPath)
	readonly := request.GetVolumeCapability().GetAccessMode().GetMode() == csi.VolumeCapability_AccessMode_SINGLE_NODE_READER_ONLY
	readonly = readonly || request.GetReadonly()
	log.Printf("Mounting readonly: %v", readonly)
	switch accessType := request.GetVolumeCapability().GetAccessType().(type) {
	case *csi.VolumeCapability_Block:
		if err := s.nodePublishVolume_Block(sourcePath, targetPath, readonly); err != nil {
			return nil, err
		}
	case *csi.VolumeCapability_Mount:
		fstype := request.GetVolumeCapability().GetMount().GetFsType()
		mountOptions := request.GetVolumeCapability().GetMount().GetMountFlags()
		if err := s.nodePublishVolume_Mount(sourcePath, targetPath, readonly, fstype, mountOptions); err != nil {
			return nil, err
		}
	default:
		panic(fmt.Sprintf("lvm: unknown access_type: %+v", accessType))
	}
	response := &csi.NodePublishVolumeResponse{}
	return response, nil
}

func (s *Server) nodePublishVolume_Block(sourcePath, targetPath string, readonly bool) error {
	log.Printf("Attempting to publish volume %v as BLOCK_DEVICE to %v", sourcePath, targetPath)
	log.Printf("Determining mount info at %v", targetPath)
	// Check whether something is already mounted at targetPath.
	mp, err := getMountAt(targetPath)
	if err != nil {
		return status.Errorf(
			codes.Internal,
			"Cannot get mount info at %v: err=%v",
			targetPath, err)
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
			return status.Errorf(
				codes.Internal,
				"Failed to follow symlinks at %v: err=%v",
				sourcePath, err)
		}
		log.Printf("Determined that %v -> %v", sourcePath, sourceDevicePath)
		// For bindmounts, we use the mountpoint root
		// in the current filesystem.
		mpdev := "/dev" + mp.root
		if mpdev != sourceDevicePath {
			return ErrTargetPathNotEmpty
		}
		log.Printf("The volume %v is already bind mounted to %v", sourcePath, targetPath)
		// For bind mounts, the filesystemtype and
		// mount options are ignored.
		return nil
	}
	log.Printf("Nothing mounted at targetPath %v yet", targetPath)
	// Perform a bind mount of the raw block device. The
	// `filesystemtype` and `data` parameters to the
	// mount(2) system call are ignored in this case.
	flags := uintptr(syscall.MS_BIND)
	log.Printf("Performing bind mount of %s -> %s", sourcePath, targetPath)
	if err := syscall.Mount(sourcePath, targetPath, "", flags, ""); err != nil {
		_, ok := err.(syscall.Errno)
		if !ok {
			return status.Errorf(
				codes.Internal,
				"Failed to perform bind mount: err=%v",
				err)
		}
		return status.Errorf(
			codes.FailedPrecondition,
			"Failed to perform bind mount: err=%v",
			err)
	}
	return nil
}

func (s *Server) nodePublishVolume_Mount(sourcePath, targetPath string, readonly bool, fstype string, mountOptions []string) error {
	log.Printf("Attempting to publish volume %v as MOUNT_DEVICE to %v", sourcePath, targetPath)
	var flags uintptr
	if readonly {
		flags |= syscall.MS_RDONLY
	}
	// Request validation ensures that the fstype is in our list of
	// supported filesystems.
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
		return status.Errorf(
			codes.Internal,
			"Cannot get mount info at %v: err=%v",
			targetPath, err)
	}
	log.Printf("Mount info at %v: %+v", targetPath, mp)
	if mp != nil {
		// For regular mounts, we use the mount source.
		if mp.mountsource != sourcePath {
			return ErrTargetPathNotEmpty
		}
		// Something is mounted at targetPath. We check that
		// the filesystem matches the requested one and that
		// the readonly status matches the requested readonly
		// status. If so, to support idempotency we return
		// success, otherwise we return an error as the
		// targetPath is not mounted in the requested way.
		if mp.fstype != fstype {
			return ErrMismatchedFilesystemType
		}
		if mp.isReadonly() != readonly {
			if mp.isReadonly() {
				return ErrTargetPathRO
			} else {
				return ErrTargetPathRW
			}
		}
		// The device, fstype and readonly option of
		// the filesystem at targetPath matches that
		// which is requested, to support idempotency
		// we return success.
		return nil
	}
	log.Printf("Determining filesystem type at %v", sourcePath)
	existingFstype, err := determineFilesystemType(sourcePath)
	if err != nil {
		return status.Errorf(
			codes.Internal,
			"Cannot determine filesystem type: err=%v",
			err)
	}
	log.Printf("Existing filesystem type is '%v'", existingFstype)
	if existingFstype == "" {
		// There is no existing filesystem on the
		// device, format it with the requested
		// filesystem.
		log.Printf("The device %v has no existing filesystem, formatting with %v", sourcePath, fstype)
		if err := formatDevice(sourcePath, fstype); err != nil {
			return status.Errorf(
				codes.Internal,
				"formatDevice failed: err=%v",
				err)
		}
		existingFstype = fstype
	}
	if fstype != existingFstype {
		return ErrMismatchedFilesystemType
	}
	mountOptionsStr := strings.Join(mountOptions, ",")
	// Try to mount the volume by assuming it is correctly formatted.
	log.Printf("Mounting %v at %v fstype=%v, flags=%v mountOptions=%v", sourcePath, targetPath, fstype, flags, mountOptionsStr)
	if err := syscall.Mount(sourcePath, targetPath, fstype, flags, mountOptionsStr); err != nil {
		_, ok := err.(syscall.Errno)
		if !ok {
			return status.Errorf(
				codes.Internal,
				"Failed to perform mount: err=%v",
				err)
		}
		return status.Errorf(
			codes.FailedPrecondition,
			"Failed to perform mount: err=%v",
			err)
	}
	return nil
}

func determineFilesystemType(devicePath string) (string, error) {
	// This is a best-effort function. It is possible that a device may
	// have be formatted with a new filesystem in the very recent past, in
	// which case the udev event may not have been processed yet and this
	// function will erroneously report that the device has no filesystem.
	// The only way around that (using lsblk) would be to use `udevadm
	// settle` with a timeout, but that comes with its own issues (how long
	// to set the timeout, how to determine whether the command failed due
	// to timeout or some other reason, what to do if the timeout fires and
	// the event still has not been processed, etc.)  As such, we hope for
	// the best. Fortunately, the consequence of getting this wrong is
	// minimal and amount to the client seeing an error in
	// NodePublishVolume that could have been seen in
	// ValidateVolumeCapabilities already, or temporary loss of idempotency
	// (two NodePublishVolume requests for the same volume shortly after
	// each other may both attempt to format the device and the second will
	// fail and must be retried anyway.)
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
	if err := s.validateNodeUnpublishVolumeRequest(request); err != nil {
		return nil, err
	}
	id := request.GetVolumeId()
	log.Printf("Looking up volume with id=%v", id)
	_, err := s.volumeGroup.LookupLogicalVolume(id)
	if err != nil {
		return nil, ErrVolumeNotFound
	}
	targetPath := request.GetTargetPath()
	log.Printf("Determining mount info at %v", targetPath)
	mp, err := getMountAt(targetPath)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Cannot get mount info at %v: err=%v",
			targetPath, err)
	}
	log.Printf("Mount info at %v: %+v", targetPath, mp)
	if mp == nil {
		log.Printf("Nothing mounted at %v", targetPath)
		// There is nothing mounted at targetPath, to support
		// idempotency we return success.
		response := &csi.NodeUnpublishVolumeResponse{}
		return response, nil
	}
	const umountFlags = 0
	log.Printf("Unmounting %v", targetPath)
	if err := syscall.Unmount(targetPath, umountFlags); err != nil {
		_, ok := err.(syscall.Errno)
		if !ok {
			return nil, status.Errorf(
				codes.Internal,
				"Failed to perform unmount: err=%v",
				err)
		}
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"Failed to perform unmount: err=%v",
			err)
	}
	response := &csi.NodeUnpublishVolumeResponse{}
	return response, nil
}

func (s *Server) GetNodeID(
	ctx context.Context,
	request *csi.GetNodeIDRequest) (*csi.GetNodeIDResponse, error) {
	if err := s.validateGetNodeIDRequest(request); err != nil {
		return nil, err
	}
	response := &csi.GetNodeIDResponse{}
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
	if err := s.validateNodeProbeRequest(request); err != nil {
		return nil, err
	}
	log.Printf("Validating tags: %v", s.tags)
	for _, tag := range s.tags {
		if err := lvm.ValidateTag(tag); err != nil {
			return nil, status.Errorf(
				codes.FailedPrecondition,
				"Invalid tag '%v': err=%v",
				tag,
				err)
		}
	}
	log.Printf("Looking up volume group %v", s.vgname)
	volumeGroup, err := lvm.LookupVolumeGroup(s.vgname)
	if err == lvm.ErrVolumeGroupNotFound {
		if s.removingVolumeGroup {
			// We've been instructed to remove the volume
			// group but it already does not exist. Return
			// success.
			log.Printf("Running in '-remove-volume-group' mode and volume group cannot be found.")
			response := &csi.NodeProbeResponse{}
			return response, nil
		}
		log.Printf("Cannot find volume group %v", s.vgname)
		// The volume group does not exist yet so see if we can create it.
		// We check if the physical volumes are available.
		log.Printf("Getting LVM2 physical volumes %v", s.pvnames)
		var pvs []*lvm.PhysicalVolume
		for _, pvname := range s.pvnames {
			log.Printf("Looking up LVM2 physical volume %v", pvname)
			var pv *lvm.PhysicalVolume
			pv, err = lvm.LookupPhysicalVolume(pvname)
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
					return nil, status.Errorf(
						codes.FailedPrecondition,
						"Could not stat device %v: err=%v",
						pvname, err)
				}
				log.Printf("Stat device %v", pvname)
				log.Printf("Zeroing partition table on %v", pvname)
				if err := zeroPartitionTable(pvname); err != nil {
					return nil, status.Errorf(
						codes.Internal,
						"Cannot zero partition table on %v: err=%v",
						pvname, err)
				}
				log.Printf("Creating LVM2 physical volume %v", pvname)
				pv, err = lvm.CreatePhysicalVolume(pvname)
				if err != nil {
					return nil, status.Errorf(
						codes.FailedPrecondition,
						"Cannot create LVM2 physical volume %v: err=%v",
						pvname, err)
				}
				log.Printf("Created LVM2 physical volume %v", pvname)
				pvs = append(pvs, pv)
				continue
			}
			return nil, status.Errorf(
				codes.Internal,
				"Cannot lookup physical volume %v: err=%v",
				pvname, err)
		}
		log.Printf("Creating volume group %v with physical volumes %v and tags %v", s.vgname, s.pvnames, s.tags)
		volumeGroup, err = lvm.CreateVolumeGroup(s.vgname, pvs, s.tags)
		if err != nil {
			return nil, status.Errorf(
				codes.FailedPrecondition,
				"Cannot create volume group %v: err=%v",
				s.vgname, err)
		}
		log.Printf("Created volume group %v", s.vgname)
	} else if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Cannot lookup volume group %v: err=%v",
			s.vgname, err)
	}
	log.Printf("Found volume group %v", s.vgname)
	// The volume group already exists. We check that the list of
	// physical volumes matches the provided list.
	log.Printf("Listing physical volumes in volume group %s", s.vgname)
	existing, err := volumeGroup.ListPhysicalVolumeNames()
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Cannot list physical volumes: err=%v",
			err)
	}
	missing, unexpected := calculatePVDiff(existing, s.pvnames)
	if len(missing) != 0 || len(unexpected) != 0 {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"Volume group contains unexpected volumes %v and is missing volumes %v",
			unexpected, missing)
	}
	// We check that the volume group tags match those we expect.
	log.Printf("Looking up volume group tags")
	tags, err := volumeGroup.Tags()
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Cannot lookup tags: err=%v",
			err)
	}
	log.Printf("Volume group tags: %v", tags)
	if err := s.checkVolumeGroupTags(tags); err != nil {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"Volume group tags did not match expected: err=%v",
			err)
	}
	// The volume group is configured as expected.
	log.Printf("Volume group matches configuration")
	if s.removingVolumeGroup {
		log.Printf("Running with '-remove-volume-group'.")
		// The volume group matches our config. We remove it
		// as requested in the startup flags.
		log.Printf("Removing volume group %v", s.vgname)
		if err := volumeGroup.Remove(); err != nil {
			return nil, status.Errorf(
				codes.Internal,
				"Failed to remove volume group: err=%v",
				err)
		}
		log.Printf("Removed volume group %v", s.vgname)
		response := &csi.NodeProbeResponse{}
		return response, nil
	}
	s.volumeGroup = volumeGroup
	response := &csi.NodeProbeResponse{}
	return response, nil
}

func calculatePVDiff(existing, pvnames []string) (missing, unexpected []string) {
	for _, epvname := range existing {
		had := false
		for _, pvname := range pvnames {
			if epvname == pvname {
				had = true
				break
			}
		}
		if !had {
			unexpected = append(unexpected, epvname)
		}
	}
	for _, pvname := range pvnames {
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
	return missing, unexpected
}

func (s *Server) checkVolumeGroupTags(tags []string) error {
	if len(tags) != len(s.tags) {
		return fmt.Errorf("csilvm: Configured tags don't match existing tags: %v != %v", s.tags, tags)
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
			return fmt.Errorf("csilvm: Configured tags don't match existing tags: %v != %v", s.tags, tags)
		}
	}
	return nil
}

func (s *Server) NodeGetCapabilities(
	ctx context.Context,
	request *csi.NodeGetCapabilitiesRequest) (*csi.NodeGetCapabilitiesResponse, error) {
	if err := s.validateNodeGetCapabilitiesRequest(request); err != nil {
		return nil, err
	}
	response := &csi.NodeGetCapabilitiesResponse{}
	return response, nil
}
