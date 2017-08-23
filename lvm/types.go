package lvm

import (
	"context"
	"fmt"
	"strings"
)

// Physical Volumes

// PhysicalVolume represents a physical volume created using
// `pvcreate /dev/somedev`.
type PhysicalVolume struct {
	name string
}

// CreatePhysicalVolume issues a `pvcreate` command and returns a
// `*PhysicalVolume` reference to it or an error. It associates the
// volume with the given `context.Context`.
func CreatePhysicalVolume(ctx context.Context, name string) (*PhysicalVolume, error) {
	if err := run(ctx, fmt.Sprintf("pvcreate %s", name)); err != nil {
		return nil, err
	}
	return &PhysicalVolume{name}, nil
}

// Name returns the name of the physical volume.
func (pv *PhysicalVolume) Name() string {
	return pv.name
}

// Remove calls `RemovePhysicalVolume` on the current volume.
func (pv *PhysicalVolume) Remove(ctx context.Context) error {
	return RemovePhysicalVolume(ctx, pv.name)
}

// RemovePhysicalVolume issues a `pvremove` command to remove the physical
// volume.
func RemovePhysicalVolume(ctx context.Context, name string) error {
	return run(ctx, fmt.Sprintf("pvremove %s", name))
}

// Volume Groups

// VolumeGroup represents a volume group.
type VolumeGroup struct {
	name string
	pvs  []*PhysicalVolume
}

// CreateVolumeGroup creates a volume group called `name` consisting
// of the physical volumes provided as `pvs`.
func CreateVolumeGroup(ctx context.Context, name string, pvs ...*PhysicalVolume) (*VolumeGroup, error) {
	pvnames := []string{}
	for _, pv := range pvs {
		pvnames = append(pvnames, pv.Name())
	}
	cmd := fmt.Sprintf("vgcreate %s %s", name, strings.Join(pvnames, " "))
	if err := run(ctx, cmd); err != nil {
		return nil, err
	}
	return &VolumeGroup{name, pvs}, nil
}

// Name returns the name of the volume group.
func (vg *VolumeGroup) Name() string {
	return vg.name
}

// Remove call `RemoveVolumeGroup` on the current volume group.
func (vg *VolumeGroup) Remove(ctx context.Context) error {
	return RemoveVolumeGroup(ctx, vg.name)
}

// RemoveVolumeGroup removes the volume group with the given `name`.
func RemoveVolumeGroup(ctx context.Context, name string) error {
	return run(ctx, fmt.Sprintf("vgremove %s", name))
}

// Logical Volumes

type LogicalVolume struct {
	name string
	vg   *VolumeGroup
	size int
}

// CreateLogicalVolume creates a new logical volume called `name` from
// the given volume group `vg` of the specified `size` given in
// megabytes.
func CreateLogicalVolume(ctx context.Context, name string, vg *VolumeGroup, size int) (*LogicalVolume, error) {
	cmd := fmt.Sprintf("lvcreate --name %s --size %d %s", name, size, vg.Name())
	if err := run(ctx, cmd); err != nil {
		return nil, err
	}
	return &LogicalVolume{name, vg, size}, nil
}

// Name returns the name of the logical volume.
func (lv *LogicalVolume) Name() string {
	return lv.name
}

// Remove calls `RemoveLogicalVolume` on the current volume.
func (lv *LogicalVolume) Remove(ctx context.Context) error {
	return RemoveLogicalVolume(ctx, lv.vg.name, lv.name)
}

// RemoveLogicalVolume removes the logical volume `lvname` in the
// volume group `vgname`. Unused volumes will be deactivated before
// being removed.
func RemoveLogicalVolume(ctx context.Context, vgname, lvname string) error {
	return run(ctx, fmt.Sprintf("lvremove -f %s/%s", vgname, lvname))
}
