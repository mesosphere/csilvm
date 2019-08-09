package lvm

import (
	"fmt"

	"github.com/gofrs/flock"
)

var lvmlock *flock.Flock

// SetLockFilePath sets the path to the LOCK file to use for preventing
// concurrent invocations of LVM command-line utilities.
//
// See
// - https://jira.mesosphere.com/browse/DCOS_OSS-5434
// - https://github.com/lvmteam/lvm2/issues/23
func SetLockFilePath(filepath string) {
	log.Printf("using lock file at %q", filepath)
	lvmlock = flock.New(filepath)
	log.Printf("checking if lock can be acquired")
	err := lvmlock.Lock()
	if err != nil {
		panic(fmt.Sprintf("cannot acquire lock: %v", err))
	}
	log.Printf("lock acquired, releasing lock")
	if err := lvmlock.Unlock(); err != nil {
		panic(fmt.Sprintf("cannot release lock: %v", err))
	}
	log.Printf("configured lock file")
}
