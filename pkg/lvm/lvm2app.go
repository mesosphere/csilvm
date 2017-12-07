package lvm

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
)

// IsInvalidName returns true if the error is due to an invalid name
// and false otherwise.
func IsInvalidName(err error) bool {
	const prefix = "Name contains invalid character"
	lines := strings.Split(err.Error(), "\n")
	if len(lines) == 0 {
		return false
	}
	line := strings.TrimSpace(lines[0])
	if strings.HasPrefix(line, prefix) {
		return true
	}
	return false
}

// MaxSize states that all available space should be used by the
// create operation.
const MaxSize uint64 = 0

type simpleError string

func (s simpleError) Error() string { return string(s) }

const ErrNoSpace = simpleError("lvm: not enough free space")
const ErrPhysicalVolumeNotFound = simpleError("lvm: physical volume not found")
const ErrVolumeGroupNotFound = simpleError("lvm: volume group not found")

var tagRegexp = regexp.MustCompile("^[A-Za-z0-9_+.][A-Za-z0-9_+.-]*$")

const ErrTagInvalidLength = simpleError("lvm: tag length must be between 1 and 1024 characters")
const ErrTagHasInvalidChars = simpleError("lvm: tag must consist of only [A-Za-z0-9_+.-] and cannot start with a '-'")

type PhysicalVolume struct {
	dev string
}

// Remove removes the physical volume.
func (pv *PhysicalVolume) Remove() error {
	if err := run("pvremove", nil, pv.dev); err != nil {
		return err
	}
	return nil
}

type VolumeGroup struct {
	name string
}

func (vg *VolumeGroup) Name() string {
	return vg.name
}

// BytesTotal returns the current size in bytes of the volume group.
func (vg *VolumeGroup) BytesTotal() (uint64, error) {
	result := new(vgsOutput)
	if err := run("vgs", result, "--options=vg_size", vg.name); err != nil {
		if IsVolumeGroupNotFound(err) {
			return 0, ErrVolumeGroupNotFound
		}
		return 0, err
	}
	for _, report := range result.Report {
		for _, vg := range report.Vg {
			return vg.VgSize, nil
		}
	}
	return 0, ErrVolumeGroupNotFound
}

// BytesFree returns the unallocated space in bytes of the volume group.
func (vg *VolumeGroup) BytesFree() (uint64, error) {
	result := new(vgsOutput)
	if err := run("vgs", result, "--options=vg_free", vg.name); err != nil {
		if IsVolumeGroupNotFound(err) {
			return 0, ErrVolumeGroupNotFound
		}
		return 0, err
	}
	for _, report := range result.Report {
		for _, vg := range report.Vg {
			return vg.VgFree, nil
		}
	}
	return 0, ErrVolumeGroupNotFound
}

// ExtentSize returns the size in bytes of a single extent.
func (vg *VolumeGroup) ExtentSize() (uint64, error) {
	result := new(vgsOutput)
	if err := run("vgs", result, "--options=vg_extent_size", vg.name); err != nil {
		if IsVolumeGroupNotFound(err) {
			return 0, ErrVolumeGroupNotFound
		}
		return 0, err
	}
	for _, report := range result.Report {
		for _, vg := range report.Vg {
			return vg.VgExtentSize, nil
		}
	}
	return 0, ErrVolumeGroupNotFound
}

// ExtentCount returns the number of extents.
func (vg *VolumeGroup) ExtentCount() (uint64, error) {
	result := new(vgsOutput)
	if err := run("vgs", result, "--options=vg_extent_count", vg.name); err != nil {
		if IsVolumeGroupNotFound(err) {
			return 0, ErrVolumeGroupNotFound
		}
		return 0, err
	}
	for _, report := range result.Report {
		for _, vg := range report.Vg {
			return vg.VgExtentCount, nil
		}
	}
	return 0, ErrVolumeGroupNotFound
}

// ExtentFreeCount returns the number of free extents.
func (vg *VolumeGroup) ExtentFreeCount() (uint64, error) {
	result := new(vgsOutput)
	if err := run("vgs", result, "--options=vg_free_count", vg.name); err != nil {
		if IsVolumeGroupNotFound(err) {
			return 0, ErrVolumeGroupNotFound
		}
		return 0, err
	}
	for _, report := range result.Report {
		for _, vg := range report.Vg {
			return vg.VgFreeExtentCount, nil
		}
	}
	return 0, ErrVolumeGroupNotFound
}

// CreateLogicalVolume creates a logical volume of the given device
// and size.
//
// The actual size may be larger than asked for as the smallest
// increment is the size of an extent on the volume group in question.
//
// If sizeInBytes is zero the entire available space is allocated.
func (vg *VolumeGroup) CreateLogicalVolume(name string, sizeInBytes uint64, tag string) (*LogicalVolume, error) {
	// Validate the tag.
	var args []string
	if tag != "" {
		if err := ValidateTag(tag); err != nil {
			return nil, err
		}
		args = append(args, "--add-tag="+tag)
	}
	args = append(args, vg.name)
	args = append(args, name)
	if err := run("lvcreate", nil, args...); err != nil {
		return nil, err
	}
	return &LogicalVolume{name, vg}, nil
}

const ErrLogicalVolumeNotFound = simpleError("lvm: logical volume not found")

type lvsOutput struct {
	Report []struct {
		Lv []struct {
			Name   string `json:"name"`
			VgName string `json:"vg_name"`
			LvPath string `json:"lv_path"`
			LvSize uint64 `json:"lv_size,string"`
			LvTags string `json:"lv_tags"`
		} `json:"lv"`
	} `json:"report"`
}

func IsLogicalVolumeNotFound(err error) bool {
	const prefix = "Failed to find logical volume"
	lines := strings.Split(err.Error(), "\n")
	if len(lines) == 0 {
		return false
	}
	line := strings.TrimSpace(lines[0])
	if strings.HasPrefix(line, prefix) {
		return true
	}
	return false
}

// LookupLogicalVolume looks up the logical volume in the volume group
// with the given name.
func (vg *VolumeGroup) LookupLogicalVolume(name string) (*LogicalVolume, error) {
	result := new(lvsOutput)
	if err := run("lvs", result, "--options=lv_name,vg_name", name); err != nil {
		if IsLogicalVolumeNotFound(err) {
			return nil, ErrLogicalVolumeNotFound
		}
		return nil, err
	}
	for _, report := range result.Report {
		for _, lv := range report.Lv {
			vg := &VolumeGroup{lv.VgName}
			return &LogicalVolume{lv.Name, vg}, nil
		}
	}
	return nil, ErrLogicalVolumeNotFound
}

// ListLogicalVolumes returns the names of the logical volumes in this volume group.
func (vg *VolumeGroup) ListLogicalVolumeNames() ([]string, error) {
	var names []string
	result := new(lvsOutput)
	if err := run("lvs", result, "--options=lv_name,vg_name", vg.name); err != nil {
		return nil, err
	}
	for _, report := range result.Report {
		for _, lv := range report.Lv {
			if lv.VgName == vg.name {
				names = append(names, lv.Name)
			}
		}
	}
	return names, nil
}

func IsPhysicalVolumeNotFound(err error) bool {
	const prefix = "Failed to find device"
	lines := strings.Split(err.Error(), "\n")
	if len(lines) == 0 {
		return false
	}
	line := strings.TrimSpace(lines[0])
	if strings.HasPrefix(line, prefix) {
		return true
	}
	return false
}

func IsVolumeGroupNotFound(err error) bool {
	const prefix = "Volume group"
	const suffix = "not found"
	lines := strings.Split(err.Error(), "\n")
	if len(lines) == 0 {
		return false
	}
	line := strings.TrimSpace(lines[0])
	if strings.HasPrefix(line, prefix) && strings.HasSuffix(line, suffix) {
		return true
	}
	return false
}

// ListPhysicalVolumeNames returns the names of the physical volumes in this volume group.
func (vg *VolumeGroup) ListPhysicalVolumeNames() ([]string, error) {
	var names []string
	result := new(pvsOutput)
	if err := run("pvs", result, "--options=pv_name,vg_name", vg.name); err != nil {
		if IsVolumeGroupNotFound(err) {
			return nil, ErrVolumeGroupNotFound
		}
		return nil, err
	}
	for _, report := range result.Report {
		for _, pv := range report.Pv {
			if pv.VgName == vg.name {
				names = append(names, pv.Name)
			}
		}
	}
	return names, nil
}

// Tags returns the volume group tags.
func (vg *VolumeGroup) Tags() ([]string, error) {
	result := new(vgsOutput)
	if err := run("vgs", result, "--options=vg_tags", vg.name); err != nil {
		if IsVolumeGroupNotFound(err) {
			return nil, ErrVolumeGroupNotFound
		}

		return nil, err
	}
	for _, report := range result.Report {
		for _, vg := range report.Vg {
			return strings.Split(vg.VgTags, ","), nil
		}
	}
	return nil, ErrVolumeGroupNotFound
}

// Remove removes the volume group from disk.
//
// It calls `lvm_vg_remove` followed by `lvm_vg_write` to persist the
// change.
func (vg *VolumeGroup) Remove() error {
	if err := run("vgremove", nil, vg.name); err != nil {
		return err
	}
	return nil
}

const (
	openReadWrite = false
	openReadOnly  = true
)

type LogicalVolume struct {
	name string
	vg   *VolumeGroup
}

func (lv *LogicalVolume) Name() string {
	return lv.name
}

func (lv *LogicalVolume) SizeInBytes() (uint64, error) {
	result := new(lvsOutput)
	if err := run("lvs", result, "--options=lv_size", lv.vg.name+"/"+lv.name); err != nil {
		if IsLogicalVolumeNotFound(err) {
			return 0, ErrLogicalVolumeNotFound
		}
		return 0, err
	}
	for _, report := range result.Report {
		for _, lv := range report.Lv {
			return lv.LvSize, nil
		}
	}
	return 0, ErrLogicalVolumeNotFound
}

// Path returns the device path for the logical volume.
func (lv *LogicalVolume) Path() (string, error) {
	result := new(lvsOutput)
	if err := run("lvs", result, "--options=lv_path", lv.vg.name+"/"+lv.name); err != nil {
		if IsLogicalVolumeNotFound(err) {
			return "", ErrLogicalVolumeNotFound
		}
		return "", err
	}
	for _, report := range result.Report {
		for _, lv := range report.Lv {
			return lv.LvPath, nil
		}
	}
	return "", ErrLogicalVolumeNotFound
}

// Tags returns the volume group tags.
func (lv *LogicalVolume) Tags() ([]string, error) {
	result := new(lvsOutput)
	if err := run("lvs", result, "--options=lv_tags", lv.vg.name+"/"+lv.name); err != nil {
		if IsLogicalVolumeNotFound(err) {
			return nil, ErrLogicalVolumeNotFound
		}
		return nil, err
	}
	for _, report := range result.Report {
		for _, lv := range report.Lv {
			return strings.Split(lv.LvTags, ","), nil
		}
	}
	return nil, ErrLogicalVolumeNotFound
}

func (lv *LogicalVolume) Remove() error {
	if err := run("lvremove", nil, lv.vg.name+"/"+lv.name); err != nil {
		return err
	}
	return nil
}

// PVScan runs the `pvscan --cache <dev>` command. It scans for the
// device at `dev` and adds it to the LVM metadata cache if `lvmetad`
// is running. If `dev` is an empty string, it scans all devices.
func PVScan(dev string) error {
	args := []string{"--cache"}
	if dev != "" {
		args = append(args, dev)
	}
	return run("pvscan", nil, args...)
}

// VGScan runs the `vgscan --cache <name>` command. It scans for the
// volume group and adds it to the LVM metadata cache if `lvmetad`
// is running. If `name` is an empty string, it scans all volume groups.
func VGScan(name string) error {
	args := []string{"--cache"}
	if name != "" {
		args = append(args, name)
	}
	return run("vgscan", nil, args...)
}

// CreateVolumeGroup creates a new volume group.
func CreateVolumeGroup(
	name string,
	pvs []*PhysicalVolume,
	tag string) (*VolumeGroup, error) {
	var args []string
	if tag != "" {
		if err := ValidateTag(tag); err != nil {
			return nil, err
		}
		args = append(args, "--add-tag="+tag)
	}
	args = append(args, name)
	for _, pv := range pvs {
		args = append(args, pv.dev)
	}
	if err := run("vgcreate", nil, args...); err != nil {
		return nil, err
	}
	// Perform a best-effort scan to trigger a lvmetad cache refresh.
	// We ignore errors as for better or worse, the volume group now exists.
	// Without this lvmetad can fail to pickup newly created volume groups.
	// See https://bugzilla.redhat.com/show_bug.cgi?id=837599
	PVScan("")
	VGScan("")
	return &VolumeGroup{name}, nil
}

/*
ValidateTag validates a tag.

LVM tags are strings of up to 1024 characters. LVM tags cannot
start with a hyphen.

A valid tag can consist of a limited range of characters only. The
allowed characters are [A-Za-z0-9_+.-]. As of the Red Hat Enterprise
Linux 6.1 release, the list of allowed characters was extended, and
tags can contain the /, =, !, :, #, and & characters.

~ https://access.redhat.com/documentation/en-us/red_hat_enterprise_linux/7/html/logical_volume_manager_administration/lvm_tags
*/
func ValidateTag(tag string) error {
	if len(tag) > 1024 {
		return ErrTagInvalidLength
	}
	if !tagRegexp.MatchString(tag) {
		return ErrTagHasInvalidChars
	}
	return nil
}

type vgsOutput struct {
	Report []struct {
		Vg []struct {
			Name              string `json:"vg_name"`
			UUID              string `json:"vg_uuid"`
			VgSize            uint64 `json:"vg_size,string"`
			VgFree            uint64 `json:"vg_free,string"`
			VgExtentSize      uint64 `json:"vg_extent_size,string"`
			VgExtentCount     uint64 `json:"vg_extent_count,string"`
			VgFreeExtentCount uint64 `json:"vg_free_count,string"`
			VgTags            string `json:"vg_tags"`
		} `json:"vg"`
	} `json:"report"`
}

// LookupVolumeGroup returns the volume group with the given name.
func LookupVolumeGroup(name string) (*VolumeGroup, error) {
	result := new(vgsOutput)
	if err := run("vgs", result, "--options=vg_name", name); err != nil {
		if IsVolumeGroupNotFound(err) {
			return nil, ErrVolumeGroupNotFound
		}
		return nil, err
	}
	for _, report := range result.Report {
		for _, vg := range report.Vg {
			return &VolumeGroup{vg.Name}, nil
		}
	}
	return nil, ErrVolumeGroupNotFound
}

// ListVolumeGroupNames returns the names of the list of volume groups.
//
// It is equivalent to `lvm_list_vg_names` followed by
// `dm_list_iterate_items` to accumulate the string values.
//
// This does not normally scan for devices. To scan for devices, use
// the `Scan()` function.
func ListVolumeGroupNames() ([]string, error) {
	result := new(vgsOutput)
	if err := run("vgs", result); err != nil {
		return nil, err
	}
	var names []string
	for _, report := range result.Report {
		for _, vg := range report.Vg {
			names = append(names, vg.Name)
		}
	}
	return names, nil
}

// ListVolumeGroupUUIDs returns the UUIDs of the list of volume groups.
//
// It is equivalent to `lvm_list_vg_names` followed by
// `dm_list_iterate_items` to accumulate the string values.
//
// This does not normally scan for devices. To scan for devices, use
// the `Scan()` function.
func ListVolumeGroupUUIDs() ([]string, error) {
	result := new(vgsOutput)
	if err := run("vgs", result, "--options=vg_uuid"); err != nil {
		return nil, err
	}
	var uuids []string
	for _, report := range result.Report {
		for _, vg := range report.Vg {
			uuids = append(uuids, vg.UUID)
		}
	}
	return uuids, nil
}

// CreatePhysicalVolume creates a physical volume of the given device.
func CreatePhysicalVolume(dev string) (*PhysicalVolume, error) {
	if err := run("pvcreate", nil, dev); err != nil {
		return nil, err
	}
	return &PhysicalVolume{dev}, nil
}

type pvsOutput struct {
	Report []struct {
		Pv []struct {
			Name   string `json:"pv_name"`
			VgName string `json:"vg_name"`
		} `json:"pv"`
	} `json:"report"`
}

// ListPhysicalVolumes lists all physical volumes.
func ListPhysicalVolumes() ([]*PhysicalVolume, error) {
	result := new(pvsOutput)
	if err := run("pvs", result); err != nil {
		return nil, err
	}
	var pvs []*PhysicalVolume
	for _, report := range result.Report {
		for _, pv := range report.Pv {
			pvs = append(pvs, &PhysicalVolume{pv.Name})
		}
	}
	return pvs, nil
}

// LookupPhysicalVolume returns a physical volume with the given name.
func LookupPhysicalVolume(name string) (*PhysicalVolume, error) {
	result := new(pvsOutput)
	if err := run("pvs", result, "--options=pv_name", name); err != nil {
		if IsPhysicalVolumeNotFound(err) {
			return nil, ErrPhysicalVolumeNotFound
		}
		return nil, err
	}
	for _, report := range result.Report {
		for _, pv := range report.Pv {
			return &PhysicalVolume{pv.Name}, nil
		}
	}
	return nil, ErrPhysicalVolumeNotFound
}

// Extent sizing for linear logical volumes:
// https://github.com/Jajcus/lvm2/blob/266d6564d7a72fcff5b25367b7a95424ccf8089e/lib/metadata/metadata.c#L983

func run(cmd string, v interface{}, extraArgs ...string) error {
	var args []string
	if v != nil {
		args = append(args, "--reportformat=json")
		args = append(args, "--units=b")
		args = append(args, "--nosuffix")
	}
	args = append(args, extraArgs...)
	c := exec.Command(cmd, args...)
	fmt.Println("executing", c)
	stdout, stderr := new(bytes.Buffer), new(bytes.Buffer)
	c.Stdout = stdout
	c.Stderr = stderr
	if err := c.Run(); err != nil {
		buf := stdout.Bytes()
		buf2 := stderr.Bytes()
		fmt.Println(string(buf))
		fmt.Println(string(buf2))
		return errors.New(strings.TrimSpace(stderr.String()))
	}
	if v != nil {
		buf := stdout.Bytes()
		buf2 := stderr.Bytes()
		fmt.Println(string(buf))
		fmt.Println(string(buf2))
		if err := json.Unmarshal(buf, v); err != nil {
			return fmt.Errorf("%v: [%v]", err, string(buf))
		}
	}
	buf := stdout.Bytes()
	buf2 := stderr.Bytes()
	fmt.Println(string(buf))
	fmt.Println(string(buf2))
	return nil
}
