package lvm

import (
	"context"
	"os"
	"testing"
)

// Prepare two physical volumes
const pvsize = 100 << 20 // 20 MiB

func TestCreatePhysicalVolume(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("`lvm` tests must be run as root.")
	}
	dev, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer checkError(dev.Close)

	ctx := context.Background
	pd, err := CreatePhysicalVolume(ctx(), dev.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer checkError(func() error { return pd.Remove(ctx()) })
}

func TestCreateVolumeGroup(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("`lvm` tests must be run as root.")
	}
	ctx := context.Background

	// Prepare two physical volumes
	loop1, pv1 := newTestPhysicalVolume(t, ctx(), pvsize)
	defer checkError(loop1.Close)
	defer checkError(func() error { return pv1.Remove(ctx()) })

	loop2, pv2 := newTestPhysicalVolume(t, ctx(), pvsize)
	defer checkError(loop2.Close)
	defer checkError(func() error { return pv2.Remove(ctx()) })

	// Create a volume group using the two physical volumes
	vg, err := CreateVolumeGroup(ctx(), "test-vg", pv1, pv2)
	if err != nil {
		t.Fatal(err)
	}
	defer checkError(func() error { return vg.Remove(ctx()) })
}

func newTestPhysicalVolume(t *testing.T, ctx context.Context, sizeInBytes int) (*LoopDevice, *PhysicalVolume) {
	loop, err := CreateLoopDevice(sizeInBytes)
	if err != nil {
		t.Fatal(err)
	}
	pd, err := CreatePhysicalVolume(ctx, loop.Path())
	if err != nil {
		t.Fatal(err)
	}
	return loop, pd
}

func TestCreateLogicalVolume(t *testing.T) {
	if os.Getuid() != 0 {
		t.Skip("`lvm` tests must be run as root.")
	}
	ctx := context.Background
	loop1, pv1 := newTestPhysicalVolume(t, ctx(), pvsize)
	defer checkError(loop1.Close)
	defer checkError(func() error { return pv1.Remove(ctx()) })

	loop2, pv2 := newTestPhysicalVolume(t, ctx(), pvsize)
	defer checkError(loop2.Close)
	defer checkError(func() error { return pv2.Remove(ctx()) })

	// Create a volume group using the two physical volumes
	vg, err := CreateVolumeGroup(ctx(), "test-vg", pv1, pv2)
	if err != nil {
		t.Fatal(err)
	}
	defer checkError(func() error { return vg.Remove(ctx()) })

	// Create a logical volume from the physical volumes.
	lv, err := CreateLogicalVolume(ctx(), "test-lv", vg, 10)
	if err != nil {
		t.Fatal(err)
	}
	defer checkError(func() error { return lv.Remove(ctx()) })
}
