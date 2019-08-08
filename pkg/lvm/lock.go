package lvm

import (
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
	lvmlock = flock.New(filepath)
}
