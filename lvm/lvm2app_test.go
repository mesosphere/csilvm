package lvm

import (
	"os/exec"
	"strings"
	"testing"

	"github.com/google/uuid"
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
	vg1, cleanup1, err := createVolumeGroup(handle)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup1()
	// Create the second volume group.
	vg2, cleanup2, err := createVolumeGroup(handle)
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

func createVolumeGroup(handle *LibHandle) (*VolumeGroup, func(), error) {
	// Create a loop device to back the physical volume.
	loop, err := CreateLoopDevice(100 << 20)
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
	cleanup.add(func() error { return pv.Remove(handle) })
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
