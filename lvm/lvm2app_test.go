package lvm

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/google/uuid"
)

// We use 100MiB test volumes.
const pvsize = 100 << 20

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

func TestErr(t *testing.T) {
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	exp := `lvm: validateVolumeGroupName: Name contains invalid character, valid set includes: [a-zA-Z0-9.-_+]. New volume group name "bad ^^^" is invalid. (-1)`
	err = handle.validateVolumeGroupName("bad ^^^")
	if err.Error() != exp {
		t.Fatalf("Expected '%s' but got '%s'", exp, err.Error())
	}
}

func TestListVolumeGroupNames(t *testing.T) {
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	// Create the first volume group.
	vg1, cleanup1, err := createVolumeGroup(handle, pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup1()
	// Create the second volume group.
	vg2, cleanup2, err := createVolumeGroup(handle, pvsize)
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
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	// Create the volume group.
	vg, cleanup, err := createVolumeGroup(handle, 100<<20)
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
	if err == nil {
		t.Fatal("Expected error due to bad volume group name.")
	}
	if vg != nil {
		t.Fatal("Expected no volume group in response")
	}
}

func TestVolumeGroupBytesTotal(t *testing.T) {
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	// Create the first volume group.
	vg, cleanup, err := createVolumeGroup(handle, 100<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesTotal()
	if err != nil {
		t.Fatal(err)
	}
	// Experimentally determined - this is probably really brittle.
	exp := 100663296
	if size != exp {
		t.Fatalf("Expected size %d but got %d", 100<<20, exp)
	}
}

func TestVolumeGroupBytesFree(t *testing.T) {
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	// Create the first volume group.
	vg, cleanup, err := createVolumeGroup(handle, 100<<20)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesFree()
	if err != nil {
		t.Fatal(err)
	}
	// This size was experimentally determined - this is probably
	// quite brittle. Consider multiplying number of extents with
	// extent size and comparing that.
	exp := 100663296
	if size != exp {
		t.Fatalf("Expected size %d but got %d", 100<<20, exp)
	}
}

func TestCreateLogicalVolume(t *testing.T) {
	handle, err := NewLibHandle()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	vg, cleanup, err := createVolumeGroup(handle, pvsize)
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

func createVolumeGroup(handle *LibHandle, size int) (*VolumeGroup, func(), error) {
	// Create a loop device to back the physical volume.
	loop, err := CreateLoopDevice(size)
	if err != nil {
		return nil, nil, err
	}
	var cleanup cleanupSteps
	defer func() {
		if err != nil {
			cleanup.unwind()
		}
	}()
	cleanup.add(loop.Close)

	// Create a physical volume using the loop device.
	var pvs []*PhysicalVolume
	const allAvailableSpace = 0
	pv, err := handle.CreatePhysicalVolume(loop.Path(), allAvailableSpace)
	if err != nil {
		return nil, nil, err
	}
	cleanup.add(func() error { return pv.Remove() })
	pvs = append(pvs, pv)

	// Create a volume group containing the physical volume.
	vgname := "test-vg-" + uuid.New().String()
	vg, err := handle.CreateVolumeGroup(vgname, pvs)
	if err != nil {
		return nil, nil, err
	}
	cleanup.add(vg.Remove)
	return vg, cleanup.unwind, nil
}
