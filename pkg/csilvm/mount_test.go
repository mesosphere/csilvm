package csilvm

import (
	"reflect"
	"testing"
)

func TestParseMountinfoOptionalFields(t *testing.T) {
	buf := []byte("36 35 98:0 /mnt1 /mnt2 rw,noatime master:1 - ext3 /dev/root rw,errors=continue")
	mounts, err := parseMountinfo(buf)
	if err != nil {
		panic(err)
	}
	exp := []mountpoint{
		{
			root:        "/mnt1",
			path:        "/mnt2",
			fstype:      "ext3",
			mountopts:   []string{"rw", "noatime"},
			mountsource: "/dev/root",
		},
	}
	if !reflect.DeepEqual(mounts, exp) {
		t.Fatalf("Expected %#v but got %#v", exp, mounts)
	}
}

func TestParseMountinfoNoOptionalFields(t *testing.T) {
	buf := []byte("228 381 253:4 / /mnt/volume-1 rw,relatime - xfs /mnt/volume-1 rw,seclabel,attr2,inode64,noquota")
	mounts, err := parseMountinfo(buf)
	if err != nil {
		panic(err)
	}
	exp := []mountpoint{
		{
			root:        "/",
			path:        "/mnt/volume-1",
			fstype:      "xfs",
			mountopts:   []string{"rw", "relatime"},
			mountsource: "/mnt/volume-1",
		},
	}
	if !reflect.DeepEqual(mounts, exp) {
		t.Fatalf("Expected %#v but got %#v", exp, mounts)
	}
}
