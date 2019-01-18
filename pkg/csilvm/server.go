package csilvm

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	csi "github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/mesosphere/csilvm/pkg/lvm"
	"github.com/mesosphere/csilvm/pkg/version"
	"golang.org/x/net/context"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Server struct {
	vgname               string
	pvnames              []string
	volumeGroup          *lvm.VolumeGroup
	defaultVolumeSize    uint64
	supportedFilesystems map[string]string
	removingVolumeGroup  bool
	tags                 []string
}

// NewServer returns a new Server that will manage the given LVM volume
// group. It accepts a variadic list of ServerOpt with which the server's
// default options can be overwritten. The Setup method must be called before
// any other further method calls are performed in order to setup/remove the
// volume group.
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

func (s *Server) SupportedFilesystems() map[string]string {
	m := make(map[string]string)
	for k, v := range s.supportedFilesystems {
		m[k] = v
	}
	return m
}

func (s *Server) RemovingVolumeGroup() bool {
	return s.removingVolumeGroup
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

// RemoveVolumeGroup configures the Server to operate in "remove" mode. The
// volume group will be removed when the server starts. Most RPCs will return
// an error if the plugin is started in this mode.
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

// Setup checks that the specified volume group exists, creating it if it does
// not. If the RemoveVolumeGroup option is set this method removes the volume
// group.
func (s *Server) Setup() error {
	log.Printf("Validating tags: %v", s.tags)
	for _, tag := range s.tags {
		if err := lvm.ValidateTag(tag); err != nil {
			return fmt.Errorf(
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
			return nil
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
					return fmt.Errorf(
						"Could not stat device %v: err=%v",
						pvname, err)
				}
				log.Printf("Stat device %v", pvname)
				log.Printf("Zeroing partition table on %v", pvname)
				if err := zeroPartitionTable(pvname); err != nil {
					return fmt.Errorf(
						"Cannot zero partition table on %v: err=%v",
						pvname, err)
				}
				log.Printf("Creating LVM2 physical volume %v", pvname)
				pv, err = lvm.CreatePhysicalVolume(pvname)
				if err != nil {
					return fmt.Errorf(
						"Cannot create LVM2 physical volume %v: err=%v",
						pvname, err)
				}
				log.Printf("Created LVM2 physical volume %v", pvname)
				pvs = append(pvs, pv)
				continue
			}
			return fmt.Errorf(
				"Cannot lookup physical volume %v: err=%v",
				pvname, err)
		}
		log.Printf("Creating volume group %v with physical volumes %v and tags %v", s.vgname, s.pvnames, s.tags)
		volumeGroup, err = lvm.CreateVolumeGroup(s.vgname, pvs, s.tags)
		if err != nil {
			return fmt.Errorf(
				"Cannot create volume group %v: err=%v",
				s.vgname, err)
		}
		log.Printf("Created volume group %v", s.vgname)
	} else if err != nil {
		return fmt.Errorf(
			"Cannot lookup volume group %v: err=%v",
			s.vgname, err)
	}
	log.Printf("Found volume group %v", s.vgname)
	// The volume group already exists. We check that the list of
	// physical volumes matches the provided list.
	log.Printf("Listing physical volumes in volume group %s", s.vgname)
	existing, err := volumeGroup.ListPhysicalVolumeNames()
	if err != nil {
		return fmt.Errorf(
			"Cannot list physical volumes: err=%v",
			err)
	}
	missing, unexpected := calculatePVDiff(existing, s.pvnames)
	if len(missing) != 0 || len(unexpected) != 0 {
		return fmt.Errorf(
			"Volume group contains unexpected volumes %v and is missing volumes %v",
			unexpected, missing)
	}
	// We check that the volume group tags match those we expect.
	log.Printf("Looking up volume group tags")
	tags, err := volumeGroup.Tags()
	if err != nil {
		return fmt.Errorf(
			"Cannot lookup tags: err=%v",
			err)
	}
	log.Printf("Volume group tags: %v", tags)
	if err := s.checkVolumeGroupTags(tags); err != nil {
		return fmt.Errorf(
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
			return fmt.Errorf(
				"Failed to remove volume group: err=%v",
				err)
		}
		log.Printf("Removed volume group %v", s.vgname)
		return nil
	}
	s.volumeGroup = volumeGroup
	return nil
}

// IdentityService RPCs

const (
	manifestBuildSHA  = "buildSHA"
	manifestBuildTime = "buildTime"
)

func (s *Server) GetPluginInfo(
	ctx context.Context,
	request *csi.GetPluginInfoRequest) (*csi.GetPluginInfoResponse, error) {

	v := version.Get()
	m := make(map[string]string)
	if v.BuildSHA != "" {
		m[manifestBuildSHA] = v.BuildSHA
	}
	if v.BuildTime != "" {
		m[manifestBuildTime] = v.BuildTime
	}

	response := &csi.GetPluginInfoResponse{v.Product, v.Version, m}
	return response, nil
}

func (s *Server) GetPluginCapabilities(
	ctx context.Context,
	request *csi.GetPluginCapabilitiesRequest) (*csi.GetPluginCapabilitiesResponse, error) {
	response := &csi.GetPluginCapabilitiesResponse{
		Capabilities: []*csi.PluginCapability{
			{
				Type: &csi.PluginCapability_Service_{
					Service: &csi.PluginCapability_Service{
						Type: csi.PluginCapability_Service_CONTROLLER_SERVICE,
					},
				},
			},
		},
	}
	return response, nil
}

// Probe is currently a no-op.
func (s *Server) Probe(
	ctx context.Context,
	request *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	if s.removingVolumeGroup {
		// We're busy removing the volume-group so no need to perform health checks.
		response := &csi.ProbeResponse{}
		return response, nil
	}
	log.Printf("Checking LVM2 physical volumes")
	for _, pvname := range s.pvnames {
		// Check that the LVM2 metadata written to the start of the PV parses.
		log.Printf("Looking up LVM2 physical volume %v", pvname)
		_, err := lvm.LookupPhysicalVolume(pvname)
		if err != nil {
			return nil, status.Errorf(
				codes.FailedPrecondition,
				"Cannot lookup physical volume %v: err=%v",
				pvname, err)
		}
	}
	log.Printf("Looking up volume group %v", s.vgname)
	_, err := lvm.LookupVolumeGroup(s.vgname)
	if err != nil {
		return nil, status.Errorf(
			codes.FailedPrecondition,
			"Cannot find volume group %v",
			s.vgname)
	}
	response := &csi.ProbeResponse{}
	return response, nil
}

// ControllerService RPCs

var ErrVolumeAlreadyExists = status.Error(codes.AlreadyExists, "The volume already exists")
var ErrInsufficientCapacity = status.Error(codes.OutOfRange, "Not enough free space")
var ErrTooFewDisks = status.Error(codes.OutOfRange, "The volume group does not have enough underlying physical devices to support the requested RAID configuration")

func (s *Server) CreateVolume(
	ctx context.Context,
	request *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
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
			&csi.Volume{
				int64(lv.SizeInBytes()),
				lv.Name(),
				nil,
			},
		}
		return response, nil
	}
	log.Printf("Volume with id=%v does not already exist", volumeId)
	layout, err := takeVolumeLayoutFromParameters(dupParams(request.GetParameters()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Invalid volume layout: err=%v", err)
	}
	// Determine the capacity, default to maximum size.
	size := s.defaultVolumeSize
	if capacityRange := request.GetCapacityRange(); capacityRange != nil {
		bytesFree, err := s.volumeGroup.BytesFree(layout)
		if err != nil {
			return nil, status.Errorf(
				codes.Internal,
				"Error in BytesFree: err=%v",
				err)
		}
		log.Printf("BytesFree: %v", bytesFree)
		// Check whether there is enough free space available.
		if int64(bytesFree) < capacityRange.GetRequiredBytes() {
			return nil, ErrInsufficientCapacity
		}
		// Set the volume size to the minimum requested  size.
		size = uint64(capacityRange.GetRequiredBytes())
	}
	lvopts, err := volumeOptsFromParameters(request.GetParameters())
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid parameters: %v", err)
	}
	log.Printf("Creating logical volume id=%v, size=%v, tags=%v, params=%v", volumeId, size, s.tags, request.GetParameters())
	lv, err := s.volumeGroup.CreateLogicalVolume(volumeId, size, s.tags, lvopts...)
	if err != nil {
		if err == lvm.ErrInvalidLVName {
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
		if err == lvm.ErrTooFewDisks {
			return nil, ErrTooFewDisks
		}
		return nil, status.Errorf(
			codes.Internal,
			"Error in CreateLogicalVolume: err=%v",
			err)
	}
	response := &csi.CreateVolumeResponse{
		&csi.Volume{
			int64(lv.SizeInBytes()),
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
			if requiredBytes > int64(lv.SizeInBytes()) {
				log.Printf("Existing volume does not satisfy request: required_bytes > volume size (%d > %d)", requiredBytes, lv.SizeInBytes())
				// The existing volume is not big enough.
				return ErrVolumeAlreadyExists
			}
		}
		if limitBytes := capacityRange.GetLimitBytes(); limitBytes != 0 {
			if limitBytes < int64(lv.SizeInBytes()) {
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
	id := request.GetVolumeId()
	log.Printf("Looking up volume with id=%v", id)
	lv, err := s.volumeGroup.LookupLogicalVolume(id)
	if err != nil {
		// It is idempotent to succeed if a volume is not found.
		response := &csi.DeleteVolumeResponse{}
		return response, nil
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
	if s.removingVolumeGroup {
		log.Printf("Running with '-remove-volume-group', reporting no volumes")
		response := &csi.ListVolumesResponse{}
		return response, nil
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
		info := &csi.Volume{
			int64(lv.SizeInBytes()),
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
	layout, err := takeVolumeLayoutFromParameters(dupParams(request.GetParameters()))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Invalid volume layout: err=%v", err)
	}
	bytesFree, err := s.volumeGroup.BytesFree(layout)
	if err != nil {
		return nil, status.Errorf(
			codes.Internal,
			"Error in BytesFree: err=%v",
			err)
	}
	log.Printf("BytesFree: %v", bytesFree)
	response := &csi.GetCapacityResponse{int64(bytesFree)}
	return response, nil
}

func (s *Server) ControllerGetCapabilities(
	ctx context.Context,
	request *csi.ControllerGetCapabilitiesRequest) (*csi.ControllerGetCapabilitiesResponse, error) {
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

func (s *Server) NodeStageVolume(
	ctx context.Context,
	request *csi.NodeStageVolumeRequest) (*csi.NodeStageVolumeResponse, error) {
	log.Printf("NodeStageVolume not supported")
	return nil, ErrCallNotImplemented
}

func (s *Server) NodeUnstageVolume(
	ctx context.Context,
	request *csi.NodeUnstageVolumeRequest) (*csi.NodeUnstageVolumeResponse, error) {
	log.Printf("NodeUnstageVolume not supported")
	return nil, ErrCallNotImplemented
}

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
		// For bind mounts, the filesystemtype and mount options are
		// ignored. As this RPC is idempotent, we respond with success.
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
	// We use `file -bsL` to determine whether any filesystem type is detected.
	// If a filesystem is detected (ie., the output is not "data", we use
	// `blkid` to determine what the filesystem is. We use `blkid` as `file`
	// has inconvenient output.
	// We do *not* use `lsblk` as that requires udev to be up-to-date which
	// is often not the case when a device is erased using `dd`.
	output, err := exec.Command("file", "-bsL", devicePath).CombinedOutput()
	if err != nil {
		return "", err
	}
	if strings.TrimSpace(string(output)) == "data" {
		// No filesystem detected.
		return "", nil
	}
	// Some filesystem was detected, we use blkid to figure out what it is.
	output, err = exec.Command("blkid", "-c", "/dev/null", "-o", "export", devicePath).CombinedOutput()
	if err != nil {
		return "", err
	}
	parseErr := errors.New("Cannot parse output of blkid.")
	lines := strings.Split(string(output), "\n")
	for _, line := range lines {
		fields := strings.Split(strings.TrimSpace(line), "=")
		if len(fields) != 2 {
			return "", parseErr
		}
		if fields[0] == "TYPE" {
			return fields[1], nil
		}
	}
	return "", parseErr
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

func (s *Server) NodeGetId(
	ctx context.Context,
	request *csi.NodeGetIdRequest) (*csi.NodeGetIdResponse, error) {
	log.Printf("NodeGetId not supported")
	return nil, ErrCallNotImplemented
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
	response := &csi.NodeGetCapabilitiesResponse{}
	return response, nil
}

// takeVolumeLayoutFromParameters removes and returns RAID-related parameters from the input.
func takeVolumeLayoutFromParameters(params map[string]string) (layout lvm.VolumeLayout, err error) {
	voltype, ok := params["type"]
	if ok {
		// Consume the 'type' key from the parameters.
		delete(params, "type")
		// We only support 'linear' and 'raid1' volume types at the moment.
		switch voltype {
		case "linear":
			layout.Type = lvm.VolumeTypeLinear
		case "raid1":
			layout.Type = lvm.VolumeTypeRAID1
			smirrors, ok := params["mirrors"]
			if ok {
				delete(params, "mirrors")
				mirrors, err := strconv.ParseUint(smirrors, 10, 64)
				if err != nil || mirrors < 1 {
					return layout, fmt.Errorf("The 'mirrors' parameter must be a positive integer: err=%v", err)
				}
				layout.Mirrors = mirrors
			}
		default:
			return layout, errors.New("The 'type' parameter must be one of 'linear' or 'raid1'.")
		}
	}
	return layout, nil
}

func dupParams(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	params := make(map[string]string, len(in))
	for k, v := range in {
		params[k] = v
	}
	return params
}

// volumeOptsFromParameters parses volume create parameters into
// lvm.CreateLogicalVolumeOpt funcs.  If returns an error if there are
// unconsumed parameters or if validation fails.
func volumeOptsFromParameters(in map[string]string) (opts []lvm.CreateLogicalVolumeOpt, err error) {
	// Create a duplicate map so we don't mutate the input.
	params := dupParams(in)
	// Transform any 'type' parameter into an opt.
	layout, err := takeVolumeLayoutFromParameters(params)
	if err != nil {
		return nil, err
	}
	opts = append(opts, lvm.VolumeLayoutOpt(layout))

	if len(params) > 0 {
		var keys []string
		for k := range params {
			keys = append(keys, k)
		}
		return nil, fmt.Errorf("Unexpected parameters: %v", keys)
	}
	return opts, nil
}

// Serialize all requests. This avoids issues observed when deleting 80 logical
// volumes in parallel where calls to `lvs` appear to hang.
//
// See https://jira.mesosphere.com/browse/DCOS_OSS-4642
func SerializingInterceptor() grpc.UnaryServerInterceptor {
	// Instead of a mutex, use a weighted semaphore because it's sensitive to context cancellation and/or deadline
	// expiration, which is important for maintaining a healthy request queue, and also helps prevent execution of
	// operations that the calling CO is no longer interested in.
	sem := semaphore.NewWeighted(1)
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		err := sem.Acquire(ctx, 1)
		if err != nil {
			return nil, err
		}
		// Acquire can still succeed if the context is canceled, double-check it.
		select {
		case <-ctx.Done():
			sem.Release(1)
			return nil, ctx.Err()
		default:
		}
		defer sem.Release(1)
		return handler(ctx, req)
	}
}

// RequestLimitInterceptor limits the number of pending requests in flight at any given time. If an incoming request
// would exceed the specified requestLimit then an Unavailable gRPC error is returned.
func RequestLimitInterceptor(requestLimit int) grpc.UnaryServerInterceptor {
	sem := semaphore.NewWeighted(int64(requestLimit))
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if !sem.TryAcquire(1) {
			return nil, status.Error(codes.Unavailable, "Too many pending requests. Please retry later.")
		}
		defer sem.Release(1)
		return handler(ctx, req)
	}
}
