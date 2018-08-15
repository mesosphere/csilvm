package lvm

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/mesosphere/csilvm/pkg/cleanup"
)

const (
	// We use 100MiB test volumes.
	pvsize = 100 << 20
)

func TestCreatePhysicalDevice(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	if err = PVScan(loop.Path()); err != nil {
		t.Fatal(err)
	}
	// Create a physical volume using the loop device.
	pv, err := CreatePhysicalVolume(loop.Path())
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
	if err = PVScan(loop.Path()); err != nil {
		t.Fatal(err)
	}
	// Create a physical volume using the loop device.
	pv, err := CreatePhysicalVolume(loop.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer pv.Remove()
	pvs, err := ListPhysicalVolumes()
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
	if err = PVScan(loop.Path()); err != nil {
		t.Fatal(err)
	}
	// Create a physical volume using the loop device.
	pv, err := CreatePhysicalVolume(loop.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer pv.Remove()
	pv2, err := LookupPhysicalVolume(pv.dev)
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
	// Create a physical volume using the loop device.
	if err = PVScan(loop.Path()); err != nil {
		t.Fatal(err)
	}
	pv, err := CreatePhysicalVolume(loop.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer pv.Remove()
	pv2, err := LookupPhysicalVolume(pv.dev + "a")
	if err != ErrPhysicalVolumeNotFound {
		t.Fatal("Expected 'not found' error.")
	}
	if pv2 != nil {
		t.Fatal("Expected result to be nil.")
	}
}

func TestPhysicalVolumeCheck(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	if err = PVScan(loop.Path()); err != nil {
		t.Fatal(err)
	}
	// Create a physical volume using the loop device.
	pv, err := CreatePhysicalVolume(loop.Path())
	if err != nil {
		t.Fatal(err)
	}
	defer pv.Remove()
	if err := pv.Check(); err != nil {
		t.Fatal(err)
	}
}

func TestValidateTag(t *testing.T) {
	if err := ValidateTag(strings.Repeat("a", 1025)); err != ErrTagInvalidLength {
		t.Fatalf("Expected tag to fail validation")
	}
	if err := ValidateTag(strings.Repeat("a", 1)); err != nil {
		t.Fatalf("Expected tag to pass validation")
	}
	if err := ValidateTag(strings.Repeat("a", 1024)); err != nil {
		t.Fatalf("Expected tag to pass validation")
	}
	if err := ValidateTag("some:tag"); err != ErrTagHasInvalidChars {
		t.Fatalf("Expected tag to fail validation")
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
	// Create the first volume group.
	vg1, cleanup1, err := createVolumeGroup([]*LoopDevice{loop1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup1()
	// Create the second volume group.
	vg2, cleanup2, err := createVolumeGroup([]*LoopDevice{loop2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup2()
	// Scan for new devices and volume groups so the new ones show up.
	names, err := ListVolumeGroupNames()
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("found", names)
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
	// Create the volume group.
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	// Confirm that the volume group exists.
	names, err := ListVolumeGroupNames()
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

func TestCreateVolumeGroup_Tagged(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	// Create the volume group.
	tag := "dcos-tag"
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, []string{tag})
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	// Confirm that the volume group exists.
	names, err := ListVolumeGroupNames()
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
	tags, err := vg.Tags()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual([]string{tag}, tags) {
		t.Fatalf("Expected tags %v but got %v", []string{tag}, tags)
	}
}

func TestCreateVolumeGroup_BadTag(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	// Create the volume group.
	_, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, []string{"{\"some\": \"json\"}"})
	if err != ErrTagHasInvalidChars {
		t.Fatalf("Expected invalid tag error, got %v", err)
	}
	if err == nil {
		cleanup()
	}
}

func TestCreateVolumeGroupInvalidName(t *testing.T) {
	// Try to create the volume group with a bad name.
	vg, err := CreateVolumeGroup("bad name :)", nil, nil)
	if err != ErrInvalidVGName {
		vg.Remove()
		t.Fatalf("Expected invalidNameError got %#v.", err)
	}
	if vg != nil {
		vg.Remove()
		t.Fatal("Expected no volume group in response")
	}
}

func TestLookupVolumeGroup(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	vg2, err := LookupVolumeGroup(vg.name)
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
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	vg2, err := LookupVolumeGroup(vg.name + "a")
	if err != ErrVolumeGroupNotFound {
		t.Fatal("Expected 'not found' error.")
	}
	if vg2 != nil {
		t.Fatal("Expected result to be nil.")
	}
}

func TestVolumeGroupCheck(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	if err := vg.Check(); err != nil {
		t.Fatal(err)
	}
}

func TestVolumeGroupBytesTotal(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	// Create the first volume group.
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, nil)
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
	// Create the first volume group.
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesFree(RAIDConfig{})
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
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesFree(RAIDConfig{})
	if err != nil {
		t.Fatal(err)
	}
	name := "test-lv-" + uuid.New().String()
	lv, err := vg.CreateLogicalVolume(name, size, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lv.Remove()
}

func TestCreateLogicalVolume_Tagged(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesFree(RAIDConfig{})
	if err != nil {
		t.Fatal(err)
	}
	name := "test-lv-" + uuid.New().String()
	tag := "dcos-tag"
	lv, err := vg.CreateLogicalVolume(name, size, []string{tag})
	if err != nil {
		t.Fatal(err)
	}
	defer lv.Remove()
	tags, err := lv.Tags()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual([]string{tag}, tags) {
		t.Fatalf("Expected tags %v but got %v", []string{tag}, tags)
	}
}

func TestCreateLogicalVolume_BadTag(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesFree(RAIDConfig{})
	if err != nil {
		t.Fatal(err)
	}
	name := "test-lv-" + uuid.New().String()
	lv, err := vg.CreateLogicalVolume(name, size, []string{"{\"some\": \"json\"}"})
	if err != ErrTagHasInvalidChars {
		t.Fatalf("Expected invalid tag error, got %v", err)
	}
	if err == nil {
		lv.Remove()
	}
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
	vg1, cleanup, err := createVolumeGroup([]*LoopDevice{loop1}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg1.BytesFree(RAIDConfig{})
	if err != nil {
		t.Fatal(err)
	}
	name := "test-lv-" + uuid.New().String()
	lv1, err := vg1.CreateLogicalVolume(name, size, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lv1.Remove()
	vg2, cleanup, err := createVolumeGroup([]*LoopDevice{loop2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err = vg2.BytesFree(RAIDConfig{})
	if err != nil {
		t.Fatal(err)
	}
	lv2, err := vg2.CreateLogicalVolume(name, size, nil)
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
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesFree(RAIDConfig{})
	if err != nil {
		t.Fatal(err)
	}
	lv, err := vg.CreateLogicalVolume("bad name :)", size, nil)
	if err != ErrInvalidLVName {
		lv.Remove()
		t.Fatalf("Expected an invalidNameError but got %#v.", err)
	}
	if lv != nil {
		lv.Remove()
		t.Fatal("Expected no logical volume in response.")
	}
}

func TestCreateLogicalVolumeTooLarge(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesFree(RAIDConfig{})
	if err != nil {
		t.Fatal(err)
	}
	lv, err := vg.CreateLogicalVolume("testvol", size*2, nil)
	if err != ErrNoSpace {
		lv.Remove()
		t.Fatal("Expected ErrNoSpace.")
	}
	if lv != nil {
		lv.Remove()
		t.Fatal("Expected no logical volume in response.")
	}
}

func TestCreateLogicalVolume_RAIDConfig_Empty(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	raid := RAIDConfig{}
	size, err := vg.BytesFree(raid)
	if err != nil {
		t.Fatal(err)
	}
	name := "test-lv-" + uuid.New().String()
	tag := "dcos-tag"
	lv, err := vg.CreateLogicalVolume(name, size, []string{tag}, RAIDOpt(raid))
	if err != nil {
		t.Fatal(err)
	}
	defer lv.Remove()
	tags, err := lv.Tags()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual([]string{tag}, tags) {
		t.Fatalf("Expected tags %v but got %v", []string{tag}, tags)
	}
}

func TestCreateLogicalVolume_RAIDConfig_Linear(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	raid := RAIDConfig{Type: VolumeTypeLinear}
	size, err := vg.BytesFree(raid)
	if err != nil {
		t.Fatal(err)
	}
	name := "test-lv-" + uuid.New().String()
	tag := "dcos-tag"
	lv, err := vg.CreateLogicalVolume(name, size, []string{tag}, RAIDOpt(raid))
	if err != nil {
		t.Fatal(err)
	}
	defer lv.Remove()
	tags, err := lv.Tags()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual([]string{tag}, tags) {
		t.Fatalf("Expected tags %v but got %v", []string{tag}, tags)
	}
}

func TestCreateLogicalVolume_RAIDConfig_RAID1(t *testing.T) {
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
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop1, loop2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	raid := RAIDConfig{Type: VolumeTypeRAID1}
	size, err := vg.BytesFree(raid)
	if err != nil {
		t.Fatal(err)
	}
	name := "test-lv-" + uuid.New().String()
	tag := "dcos-tag"
	lv, err := vg.CreateLogicalVolume(name, size/2, []string{tag}, RAIDOpt(raid))
	if err != nil {
		t.Fatal(err)
	}
	defer lv.Remove()
	tags, err := lv.Tags()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual([]string{tag}, tags) {
		t.Fatalf("Expected tags %v but got %v", []string{tag}, tags)
	}
}

func TestCreateLogicalVolume_RAIDConfig_RAID1_Mirrors2(t *testing.T) {
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
	loop3, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop3.Close()
	loop4, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop4.Close()
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop1, loop2, loop3, loop4}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	raid := RAIDConfig{Type: VolumeTypeRAID1, Mirrors: 2}
	size, err := vg.BytesFree(raid)
	if err != nil {
		t.Fatal(err)
	}
	name := "test-lv-" + uuid.New().String()
	tag := "dcos-tag"
	lv, err := vg.CreateLogicalVolume(name, size/4, []string{tag}, RAIDOpt(raid))
	if err != nil {
		t.Fatal(err)
	}
	defer lv.Remove()
	tags, err := lv.Tags()
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual([]string{tag}, tags) {
		t.Fatalf("Expected tags %v but got %v", []string{tag}, tags)
	}
}

func TestCreateLogicalVolume_RAIDConfig_RAID1_NotEnoughSpace(t *testing.T) {
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
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop1, loop2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	raid := RAIDConfig{Type: VolumeTypeRAID1, Mirrors: 1}
	size, err := vg.BytesFree(raid)
	if err != nil {
		t.Fatal(err)
	}
	name := "test-lv-" + uuid.New().String()
	tag := "dcos-tag"
	lv, err := vg.CreateLogicalVolume(name, size*2, []string{tag}, RAIDOpt(raid))
	if err == nil {
		defer lv.Remove()
		t.Fatalf("Expected error due to too few disks")
	}
	if err != ErrNoSpace {
		t.Fatalf("Expected ErrNoSpace but got %v", err)
	}
}

func TestCreateLogicalVolume_RAIDConfig_RAID1_TooFewDisks(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	// Make sure there's enough space so we are sure we hit the issue with too
	// few underlying disks, instead.
	size := uint64(10 << 20)
	name := "test-lv-" + uuid.New().String()
	tag := "dcos-tag"
	raid := RAIDConfig{Type: VolumeTypeRAID1, Mirrors: 1}
	lv, err := vg.CreateLogicalVolume(name, size, []string{tag}, RAIDOpt(raid))
	if err == nil {
		defer lv.Remove()
		t.Fatalf("Expected error due to too few disks")
	}
	if err != ErrTooFewDisks {
		t.Fatalf("Expected ErrTooFewDisks but got %v", err)
	}
}

func TestLookupLogicalVolume(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesFree(RAIDConfig{})
	if err != nil {
		t.Fatal(err)
	}
	name := "test-lv-" + uuid.New().String()
	lv, err := vg.CreateLogicalVolume(name, size, nil)
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
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesFree(RAIDConfig{})
	if err != nil {
		t.Fatal(err)
	}
	name := "test-lv-" + uuid.New().String()
	lv, err := vg.CreateLogicalVolume(name, size, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lv.Remove()
	lv2, err := vg.LookupLogicalVolume(lv.name + "a")
	if err != ErrLogicalVolumeNotFound {
		t.Fatalf("Expected 'not found' error got %s", err)
	}
	if lv2 != nil {
		t.Fatal("Expected result to be nil.")
	}
}

func TestLogicalVolumeName(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesFree(RAIDConfig{})
	if err != nil {
		t.Fatal(err)
	}
	name := "test-lv-" + uuid.New().String()
	lv, err := vg.CreateLogicalVolume(name, size, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lv.Remove()
	if lv.Name() != name {
		t.Fatalf("Expected name %v but got %v.", name, lv.Name())
	}
}

func TestLogicalVolumeSizeInBytes(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesFree(RAIDConfig{})
	if err != nil {
		t.Fatal(err)
	}
	name := "test-lv-" + uuid.New().String()
	lv, err := vg.CreateLogicalVolume(name, size, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lv.Remove()
	if lv.SizeInBytes() != size {
		t.Fatalf("Expected size %v but got %v.", size, lv.SizeInBytes())
	}
	lv2, err := vg.LookupLogicalVolume(lv.Name())
	if err != nil {
		t.Fatal(err)
	}
	if lv2.SizeInBytes() != lv.SizeInBytes() {
		t.Fatalf("Expected size %v but got %v.", lv.SizeInBytes(), lv2.SizeInBytes())
	}
}

func TestLogicalVolumePath(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesFree(RAIDConfig{})
	if err != nil {
		t.Fatal(err)
	}
	name := "test-lv-" + uuid.New().String()
	lv, err := vg.CreateLogicalVolume(name, size, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lv.Remove()
	path, err := lv.Path()
	if err != nil {
		t.Fatal(err)
	}
	exp := fmt.Sprintf("/dev/%s/%s", vg.Name(), lv.Name())
	if path != exp {
		t.Fatalf("Expected path %v but got %v.", exp, path)
	}
}

func TestVolumeGroupListLogicalVolumeNames(t *testing.T) {
	loop, err := CreateLoopDevice(pvsize)
	if err != nil {
		t.Fatal(err)
	}
	defer loop.Close()
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	size, err := vg.BytesFree(RAIDConfig{})
	if err != nil {
		t.Fatal(err)
	}
	name1 := "test-lv-" + uuid.New().String()
	lv1, err := vg.CreateLogicalVolume(name1, size/2, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer lv1.Remove()
	name2 := "test-lv-" + uuid.New().String()
	lv2, err := vg.CreateLogicalVolume(name2, size/2, nil)
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

func TestVolumeGroupListPhysicalVolumeNames(t *testing.T) {
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
	vg, cleanup, err := createVolumeGroup([]*LoopDevice{loop1, loop2}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	exp := []string{loop1.Path(), loop2.Path()}
	sort.Strings(exp)
	pvnames, err := vg.ListPhysicalVolumeNames()
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(pvnames)
	if !reflect.DeepEqual(exp, pvnames) {
		t.Fatalf("Expected pvs %+v but got %+v.", exp, pvnames)
	}
}

func createVolumeGroup(loopdevs []*LoopDevice, tags []string) (*VolumeGroup, func(), error) {
	var err error
	var cleanup cleanup.Steps
	defer func() {
		if err != nil {
			cleanup.Unwind()
		}
	}()
	// Create a physical volume using the loop device.
	var pvs []*PhysicalVolume
	for _, loop := range loopdevs {
		if err = PVScan(loop.Path()); err != nil {
			return nil, nil, err
		}
		var pv *PhysicalVolume
		pv, err = CreatePhysicalVolume(loop.Path())
		if err != nil {
			return nil, nil, err
		}
		cleanup.Add(func() error { return pv.Remove() })
		pvs = append(pvs, pv)
	}
	// Create a volume group containing the physical volume.
	vgname := "test-vg-" + uuid.New().String()
	vg, err := CreateVolumeGroup(vgname, pvs, tags)
	if err != nil {
		return nil, nil, err
	}
	cleanup.Add(vg.Remove)
	return vg, cleanup.Unwind, nil
}
