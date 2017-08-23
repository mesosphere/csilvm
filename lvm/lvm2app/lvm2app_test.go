package lvm2app

import (
	"fmt"
	"os/exec"
	"strings"

	"testing"
)

var _ = fmt.Sprintf

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

func TestInit(t *testing.T) {
	handle, err := Init()
	if err != nil {
		t.Fatal(err)
	}
	handle.Close()
}

func TestValidateVolumeGroupName(t *testing.T) {
	handle, err := Init()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	err = ValidateVolumeGroupName(handle, "some bad name ^^^ // ")
	if err == nil {
		t.Fatalf("Expected validation to fail")
	}
	err = ValidateVolumeGroupName(handle, "simplename")
	if err != nil {
		t.Fatalf("Expected validation to succeed but got '%v'", err.Error())
	}
}

func TestErr(t *testing.T) {
	handle, err := Init()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	exp := `lvm: ValidateVolumeGroupName: Name contains invalid character, valid set includes: [a-zA-Z0-9.-_+]. New volume group name "bad ^^^" is invalid. (-1)`
	err = ValidateVolumeGroupName(handle, "bad ^^^")
	if err.Error() != exp {
		t.Fatalf("Expected '%s' but got '%s'", exp, err.Error())
	}
}

func TestListVolumeGroupNames(t *testing.T) {
	handle, err := Init()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	names, err := ListVolumeGroupNames(handle)
	if err != nil {
		t.Fatal(err)
	}
	//TODO(gpaul): this test should create a new volume group then
	// confirm that its name is contained in the results.
	fmt.Println(names)
}

func TestListVolumeGroupUUIDs(t *testing.T) {
	handle, err := Init()
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	uuids, err := ListVolumeGroupUUIDs(handle)
	if err != nil {
		t.Fatal(err)
	}
	//TODO(gpaul): this test should create a new volume group then
	// confirm that its UUID is contained in the results.
	fmt.Println(uuids)
}
