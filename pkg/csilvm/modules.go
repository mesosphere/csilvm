package csilvm

import (
	"bufio"
	"bytes"
	"io"
	"io/ioutil"
)

func listModules() ([]string, error) {
	buf, err := ioutil.ReadFile("/proc/modules")
	if err != nil {
		return nil, err
	}
	return parseModules(bytes.NewReader(buf))
}

func parseModules(r io.Reader) (mods []string, err error) {
	// see https://access.redhat.com/documentation/en-US/Red_Hat_Enterprise_Linux/4/html/Reference_Guide/s2-proc-modules.html
	/*
		$ tail /proc/modules
		ttm 98304 1 cirrus, Live 0x0000000000000000
		drm_kms_helper 155648 1 cirrus, Live 0x0000000000000000
		syscopyarea 16384 1 drm_kms_helper, Live 0x0000000000000000
		sysfillrect 16384 1 drm_kms_helper, Live 0x0000000000000000
		sysimgblt 16384 1 drm_kms_helper, Live 0x0000000000000000
		fb_sys_fops 16384 1 drm_kms_helper, Live 0x0000000000000000
		psmouse 131072 0 - Live 0x0000000000000000
		drm 364544 4 cirrus,ttm,drm_kms_helper, Live 0x0000000000000000
		pata_acpi 16384 0 - Live 0x0000000000000000
		floppy 73728 0 - Live 0x0000000000000000
	*/
	s := bufio.NewScanner(r)
	for s.Scan() {
		b := s.Bytes()
		i := bytes.IndexAny(b, " \t")
		name, b := string(b[:i]), b[i+1:]
		i = bytes.IndexAny(b, " \t")
		if i < 0 {
			// !unloading
			mods = append(mods, name)
			continue
		}
		b = b[i+1:] // skip memory field
		i = bytes.IndexAny(b, " \t")
		if i < 0 {
			mods = append(mods, name)
			continue
		}
		b = b[i+1:] // skip instances col
		i = bytes.IndexAny(b, " \t")
		if i < 0 {
			mods = append(mods, name)
			continue
		}
		b = b[i+1:] // skip dependencies col
		i = bytes.IndexAny(b, " \t")
		var status []byte
		if i < 0 {
			status = b
		} else {
			status = b[:i]
		}
		if !bytes.Equal(status, []byte("Live")) {
			continue
		}
		mods = append(mods, name)
	}
	err = s.Err()
	return
}
