package lvm

import (
	"context"
	"os/exec"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/mesosphere/csilvm"
)

const (
	// We use 100MiB test volumes.
	pvsize = 100 << 20
)

func TestLibraryGetVersion(t *testing.T) {
	cmd := exec.Command("lvm", "version")
	out, err := cmd.Output()
	if err != nil {
		t.Skipf("Cannot run `lvm version`: %v", err)
	}
	version := ""
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		fields := strings.Split(line, ":")
		if len(fields) != 2 {
			continue
		}
		if fields[0] != "LVM version" {
			continue
		}
		version = strings.TrimSpace(fields[1])
	}
	if version == "" {
		t.Skip("Could not determine version using lvm command.")
	}
	exp := version
	got := LibraryGetVersion()
	if exp != got {
		t.Fatalf("Expected '%s', got '%s'", exp, got)
	}
}

func TestNewLibHandle(t *testing.T) {
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	handle.Close()
}

func TestCreatePhysicalDevice(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	if err := ScanForDevice(context.Background(), loop.Path()); err != nil {
		t.Fatal(err)
	}
	// Create a physical volume using the loop device.
	pv, err := handle.CreatePhysicalVolume(loop.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer pv.Remove()
}

func TestListPhysicalVolumes(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	if err := ScanForDevice(context.Background(), loop.Path()); err != nil {
		t.Fatal(err)
	}
	// Create a physical volume using the loop device.
	pv, err := handle.CreatePhysicalVolume(loop.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer pv.Remove()
	pvs, err := handle.ListPhysicalVolumes()
	if err != nil {
		t.Fatal(err)
	}
	for _, pv2 := range pvs {
		if pv2.dev == pv.dev {
			return
		}
	}
	t.Fatal("Expected to find physical volume but did not.")
}

func TestLookupPhysicalVolume(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	if err := ScanForDevice(context.Background(), loop.Path()); err != nil {
		t.Fatal(err)
	}
	// Create a physical volume using the loop device.
	pv, err := handle.CreatePhysicalVolume(loop.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer pv.Remove()
	pv2, err := handle.LookupPhysicalVolume(pv.dev)
	if err != nil {
		t.Fatal(err)
	}
	if pv2.dev != pv.dev {
		t.Fatal("Expected to find physical volume but did not.")
	}
}

func TestLookupPhysicalVolumeNonExistent(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	// Create a physical volume using the loop device.
	if err := ScanForDevice(context.Background(), loop.Path()); err != nil {
		t.Fatal(err)
	}
	pv, err := handle.CreatePhysicalVolume(loop.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer pv.Remove()
	pv2, err := handle.LookupPhysicalVolume(pv.dev + "a")
	if err != ErrPhysicalVolumeNotFound {
		t.Fatal("Expected 'not found' error.")
	}
	if pv2 != nil {
		t.Fatal("Expected result to be nil.")
	}
}

func TestValidateVolumeGroupName(t *testing.T) {
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	err = handle.validateVolumeGroupName("some bad name ^^^ // ")
	if err == nil {
		t.Fatalf("Expected validation to fail")
	}
	err = handle.validateVolumeGroupName("simplename")
	if err != nil {
		t.Fatalf("Expected validation to succeed but got '%v'", err.Error())
	}
}

func TestListVolumeGroupNames(t *testing.T) {
	loop1, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop1.Close()
	loop2, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop2.Close()
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	// Create the first volume group.
	vg1, cleanup1, err := createVolumeGroup(handle, loop1)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup1()
	// Create the second volume group.
	vg2, cleanup2, err := createVolumeGroup(handle, loop2)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup2()
	// Scan for new devices and volume groups so the new ones show up.
	if err := handle.Scan(); err != nil {
		t.Fatal(err)
	}
	names, err := handle.ListVolumeGroupNames()
	if err != nil {
		t.Fatal(err)
	}
	for _, vg := range []*VolumeGroup{vg1, vg2} {
		had := false
		for _, name := range names {
			if name == vg.name {
				had = true
			}
		}
		if !had {
			t.Fatalf("Expected volume group '%s'", vg.name)
		}
	}
}

func TestCreateVolumeGroup(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	// Create the volume group.
	vg, cleanup, err := createVolumeGroup(handle, loop)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	// Confirm that the volume group exists.
	names, err := handle.ListVolumeGroupNames()
	if err != nil {
		t.Fatal(err)
	}
	had := false
	for _, name := range names {
		if name == vg.name {
			had = true
		}
	}
	if !had {
		t.Fatalf("Expected volume group '%s'", vg.name)
	}
}

func TestCreateVolumeGroupInvalidName(t *testing.T) {
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	// Try to create the volume group with a bad name.
	vg, err := handle.CreateVolumeGroup("bad name :)", nil)
	if !IsInvalidName(err) {
		vg.Remove()
		t.Fatal("Expected invalidNameError got %#v.", err)
	}
	if vg != nil {
		vg.Remove()
		t.Fatal("Expected no volume group in response")
	}
	// Perform a known good operation to ensure that the error was
	// cleared from the handle.
	if _, err := handle.ListPhysicalVolumes(); err != nil {
		t.Fatal(err)
	}
}

func TestLookupVolumeGroup(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	vg, cleanup, err := createVolumeGroup(handle, loop)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	vg2, err := handle.LookupVolumeGroup(vg.name)
	if err != nil {
		t.Fatal(err)
	}
	if vg2 == nil {
		t.Fatal("Expected to find volume group but did not.")
	}
}

func TestLookupVolumeGroupNonExistent(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	vg, cleanup, err := createVolumeGroup(handle, loop)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	vg2, err := handle.LookupVolumeGroup(vg.name + "a")
	if err != ErrVolumeGroupNotFound {
		t.Fatal("Expected 'not found' error.")
	}
	if vg2 != nil {
		t.Fatal("Expected result to be nil.")
	}
}

func TestVolumeGroupBytesTotal(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	// Create the first volume group.
	vg, cleanup, err := createVolumeGroup(handle, loop)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesTotal()
	if err != nil {
		t.Fatal(err)
	}
	extentSize, err := vg.ExtentSize()
	if err != nil {
		t.Fatal(err)
	}
	// Metadata claims a single extent.
	exp := uint64(pvsize - extentSize)
	if size != exp {
		t.Fatalf("Expected size %d but got %d", exp, size)
	}
}

func TestVolumeGroupBytesFree(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	// Create the first volume group.
	vg, cleanup, err := createVolumeGroup(handle, loop)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesFree()
	if err != nil {
		t.Fatal(err)
	}
	extentSize, err := vg.ExtentSize()
	if err != nil {
		t.Fatal(err)
	}
	// Metadata claims a single extent.
	exp := uint64(pvsize - extentSize)
	if size != exp {
		t.Fatalf("Expected size %d but got %d", exp, size)
	}
}

func TestCreateLogicalVolume(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	vg, cleanup, err := createVolumeGroup(handle, loop)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesFree()
	if err != nil {
		t.Fatal(err)
	}
	name := "test-lv-" + uuid.New().String()
	lv, err := vg.CreateLogicalVolume(name, size)
	if err != nil {
		t.Fatal(err)
	}
	defer lv.Remove()
}

func TestCreateLogicalVolumeDuplicateNameIsAllowed(t *testing.T) {
	loop1, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop1.Close()
	loop2, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop2.Close()
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	vg1, cleanup, err := createVolumeGroup(handle, loop1)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg1.BytesFree()
	if err != nil {
		t.Fatal(err)
	}
	name := "test-lv-" + uuid.New().String()
	lv1, err := vg1.CreateLogicalVolume(name, size)
	if err != nil {
		t.Fatal(err)
	}
	defer lv1.Remove()
	vg2, cleanup, err := createVolumeGroup(handle, loop2)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err = vg2.BytesFree()
	if err != nil {
		t.Fatal(err)
	}
	lv2, err := vg2.CreateLogicalVolume(name, size)
	if err != nil {
		t.Fatal(err)
	}
	defer lv2.Remove()
}

func TestCreateLogicalVolumeInvalidName(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	vg, cleanup, err := createVolumeGroup(handle, loop)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesFree()
	if err != nil {
		t.Fatal(err)
	}
	lv, err := vg.CreateLogicalVolume("bad name :)", size)
	if !IsInvalidName(err) {
		lv.Remove()
		t.Fatal("Expected an invalidNameError but got %#v.", err)
	}
	if lv != nil {
		lv.Remove()
		t.Fatal("Expected no logical volume in response.")
	}
	// It appears that there is a bug in lvm2app where an error
	// returned by lvm_lv_name_validate does not get automatically
	// cleared when a subsequent call to lvm_vg_list_lvs is made.
	// Instead the error must be read by the caller explicitly.
	// We do this by calling handle.err() after
	// lvm_lv_name_validate and discarding the result. Here we
	// test that this works as intended by calling
	// ListLogicalVolumeNames after logical volume name validation
	// has failed and checking that no error gets returned.
	if _, err := vg.ListLogicalVolumeNames(); err != nil {
		t.Fatal(err)
	}
}

func TestCreateLogicalVolumeTooLarge(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	vg, cleanup, err := createVolumeGroup(handle, loop)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesFree()
	if err != nil {
		t.Fatal(err)
	}
	lv, err := vg.CreateLogicalVolume("testvol", size*2)
	if err != ErrNoSpace {
		lv.Remove()
		t.Fatal("Expected ErrNoSpace.")
	}
	if lv != nil {
		lv.Remove()
		t.Fatal("Expected no logical volume in response.")
	}
}

func TestLookupLogicalVolume(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	vg, cleanup, err := createVolumeGroup(handle, loop)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesFree()
	if err != nil {
		t.Fatal(err)
	}
	name := "test-lv-" + uuid.New().String()
	lv, err := vg.CreateLogicalVolume(name, size)
	if err != nil {
		t.Fatal(err)
	}
	defer lv.Remove()
	lv2, err := vg.LookupLogicalVolume(lv.name)
	if err != nil {
		t.Fatal(err)
	}
	if lv2 == nil {
		t.Fatal("Expected to find logical volume but did not.")
	}
}

func TestLookupLogicalVolumeNonExistent(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	vg, cleanup, err := createVolumeGroup(handle, loop)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesFree()
	if err != nil {
		t.Fatal(err)
	}
	name := "test-lv-" + uuid.New().String()
	lv, err := vg.CreateLogicalVolume(name, size)
	if err != nil {
		t.Fatal(err)
	}
	defer lv.Remove()
	lv2, err := vg.LookupLogicalVolume(lv.name + "a")
	if err != ErrLogicalVolumeNotFound {
		t.Fatal("Expected 'not found' error.")
	}
	if lv2 != nil {
		t.Fatal("Expected result to be nil.")
	}
}

func TestLogicalVolumeSizeInBytes(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	vg, cleanup, err := createVolumeGroup(handle, loop)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesFree()
	if err != nil {
		t.Fatal(err)
	}
	name := "test-lv-" + uuid.New().String()
	lv, err := vg.CreateLogicalVolume(name, size)
	if err != nil {
		t.Fatal(err)
	}
	defer lv.Remove()
	if lv.SizeInBytes() != size {
		t.Fatalf("Expected size %v but got %v.", size, lv.SizeInBytes())
	}
}

func TestVolumeGroupListLogicalVolumeNames(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	vg, cleanup, err := createVolumeGroup(handle, loop)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesFree()
	if err != nil {
		t.Fatal(err)
	}
	name1 := "test-lv-" + uuid.New().String()
	lv1, err := vg.CreateLogicalVolume(name1, size/2)
	if err != nil {
		t.Fatal(err)
	}
	defer lv1.Remove()
	name2 := "test-lv-" + uuid.New().String()
	lv2, err := vg.CreateLogicalVolume(name2, size/2)
	if err != nil {
		t.Fatal(err)
	}
	defer lv2.Remove()
	lvnames, err := vg.ListLogicalVolumeNames()
	if err != nil {
		t.Fatal(err)
	}
	if len(lvnames) != 2 {
		t.Fatalf("Expected 2 logical volume but got %d.", len(lvnames))
	}
	for _, name := range []string{name1, name2} {
		had := false
		for _, lvname := range lvnames {
			if name == lvname {
				had = true
			}
		}
		if !had {
			t.Fatalf("Expected to find logical volume %v but did not.", name)
		}
	}
}

func createVolumeGroup(handle *LibHandle, loop *LoopDevice) (*VolumeGroup, func(), error) {
	var err error
	var cleanup csilvm.CleanupSteps
	defer func() {
		if err != nil {
			cleanup.Unwind()
		}
	}()
	if err := ScanForDevice(context.Background(), loop.Path()); err != nil {
		return nil, nil, err
	}
	// Create a physical volume using the loop device.
	var pvs []*PhysicalVolume
	pv, err := handle.CreatePhysicalVolume(loop.Path())
	if err != nil {
		return nil, nil, err
	}
	cleanup.Add(func() error { return pv.Remove() })
	pvs = append(pvs, pv)
	// Create a volume group containing the physical volume.
	vgname := "test-vg-" + uuid.New().String()
	vg, err := handle.CreateVolumeGroup(vgname, pvs)
	if err != nil {
		return nil, nil, err
	}
	cleanup.Add(vg.Remove)
	return vg, cleanup.Unwind, nil
}
