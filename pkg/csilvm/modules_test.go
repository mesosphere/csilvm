package csilvm

import (
	"reflect"
	"strings"
	"testing"
)

func TestParseModules(t *testing.T) {
	src := `ttm 98304 1 cirrus, Live 0x0000000000000000
psmouse 131072 0 - Unloading 0x0000000000000000
drm 364544 4 cirrus,ttm,drm_kms_helper, Live 0x0000000000000000
pata_acpi 16384 0 - Loading 0x0000000000000000
floppy 73728 0 - Live 0x0000000000000000`

	mods, err := parseModules(strings.NewReader(src))
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{"ttm", "drm", "floppy"}
	if !reflect.DeepEqual(mods, expected) {
		t.Fatalf("expected %v instead of %v", expected, mods)
	}
}

func TestParseModulesWithoutOffsets(t *testing.T) {
	src := `ttm 98304 1 cirrus, Live
psmouse 131072 0 - Unloading
drm 364544 4 cirrus,ttm,drm_kms_helper, Live
pata_acpi 16384 0 - Loading
floppy 73728 0 - Live`

	mods, err := parseModules(strings.NewReader(src))
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{"ttm", "drm", "floppy"}
	if !reflect.DeepEqual(mods, expected) {
		t.Fatalf("expected %v instead of %v", expected, mods)
	}
}

func TestParseModulesWithoutUnloading(t *testing.T) {
	src := `ttm 98304
psmouse 131072
drm 364544
pata_acpi 16384
floppy 73728`

	mods, err := parseModules(strings.NewReader(src))
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{"ttm", "psmouse", "drm", "pata_acpi", "floppy"}
	if !reflect.DeepEqual(mods, expected) {
		t.Fatalf("expected %v instead of %v", expected, mods)
	}
}
