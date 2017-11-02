package lvs

import (
	"io/ioutil"
	"strings"
)

type mountpoint struct {
	device    string
	path      string
	fstype    string
	mountopts []string
}

func (m *mountpoint) isReadonly() bool {
	for _, opt := range m.mountopts {
		if opt == "ro" {
			return true
		}
	}
	return false
}

func listMounts() (mounts []mountpoint, err error) {
	buf, err := ioutil.ReadFile("/proc/mounts")
	for _, line := range strings.Split(string(buf), "\n") {
		if line == "" {
			continue
		}
		line := string(line)
		fields := strings.Fields(line)
		mount := mountpoint{
			device:    fields[0],
			path:      fields[1],
			fstype:    fields[2],
			mountopts: strings.Split(fields[3], ","),
		}
		mounts = append(mounts, mount)
	}
	return mounts, nil
}

func getMountAt(path string) (*mountpoint, error) {
	mounts, err := listMounts()
	if err != nil {
		return nil, err
	}
	for _, mp := range mounts {
		if mp.path == path {
			return &mp, nil
		}
	}
	return nil, nil
}
