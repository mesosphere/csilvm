package lvm

import (
	"os/exec"
)

// PVScan runs the `pvscan --cache <dev>` command. It scans for the
// device at `dev` and adds it to the LVM metadata cache if `lvmetad`
// is running. If `dev` is an empty string, it scans all devices.
func PVScan(dev string) error {
	args := []string{"--cache"}
	if dev != "" {
		args = append(args, dev)
	}
	return exec.Command("pvscan", args...).Run()
}
