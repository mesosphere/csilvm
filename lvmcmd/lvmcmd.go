package lvmcmd

import "unsafe"

// #cgo LDFLAGS: -llvm2cmd
// #include <stdlib.h>
// #include <lvm2cmd.h>
//
// void logging_bridge(int level, const char *file, int line, int dm_errno, const char *message) {
//   logCallback(level, file, line, dm_errno, message);
// }
import "C"

func init() {
	C.lvm2_log_fn(C.lvm2_log_fn_t(C.logging_bridge))
}

var lvm = unsafe.Pointer(C.lvm2_init())

// run invokes an LVM2 command line and returns the raw result
func run(cmdline string) C.int {
	cmd := C.CString(cmdline)
	defer C.free(unsafe.Pointer(cmd))
	return C.lvm2_run(lvm, cmd)
}

// exit releases the LVM2 handle
func exit() {
	C.lvm2_exit(lvm)
}
