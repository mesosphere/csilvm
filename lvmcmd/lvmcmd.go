package lvmcmd

import "unsafe"

// #cgo LDFLAGS: -llvm2cmd
// #include <stdlib.h>
// #include <lvm2cmd.h>
//
// // Return values; older versions of the lib header don't define these
// #ifndef LVM2_COMMAND_SUCCEEDED
// #define LVM2_COMMAND_SUCCEEDED  1       // ECMD_PROCESSED
// #endif
//
// #ifndef LVM2_NO_SUCH_COMMAND
// #define LVM2_NO_SUCH_COMMAND    2       // ENO_SUCH_CMD
// #endif
//
// #ifndef LVM2_INVALID_PARAMETERS
// #define LVM2_INVALID_PARAMETERS 3       // EINVALID_CMD_LINE
// #endif
//
// #ifndef LVM2_PROCESSING_FAILED
// #define LVM2_PROCESSING_FAILED  5       // ECMD_FAILED
// #endif
//
// void logging_bridge(int level, const char *file, int line, int dm_errno, const char *message) {
//   logCallback(level, file, line, dm_errno, message);
// }
import "C"

type Error int

const (
	ErrorNoSuchCommand     = Error(C.LVM2_NO_SUCH_COMMAND)
	ErrorInvalidParameters = Error(C.LVM2_INVALID_PARAMETERS)
	ErrorProcessingFailed  = Error(C.LVM2_PROCESSING_FAILED)
)

func (e Error) Error() string {
	msg := errorMessages[e]
	if msg == "" {
		return "unknown error"
	}
	return msg
}

func init() {
	C.lvm2_log_fn(C.lvm2_log_fn_t(C.logging_bridge))
}

var (
	// TODO(jdef) apparently liblvm maintains a VG cache (maybe among other things).
	// it's not actually required to obtain a handle, we may pass in NULL instead to
	// lvm2_run. consider abstracting the use of this handle instead of mandating it.
	lvm = unsafe.Pointer(C.lvm2_init())

	logLevels = map[C.int]LogLevel{
		C.LVM2_LOG_FATAL:        LogLevelFatal,
		C.LVM2_LOG_ERROR:        LogLevelError,
		C.LVM2_LOG_PRINT:        LogLevelWarn,
		C.LVM2_LOG_VERBOSE:      LogLevelInfo,
		C.LVM2_LOG_VERY_VERBOSE: LogLevelDebug,
		C.LVM2_LOG_DEBUG:        LogLevelTrace,
	}

	errorMessages = map[Error]string{
		ErrorNoSuchCommand:     "no such command",
		ErrorInvalidParameters: "invalid parameters",
		ErrorProcessingFailed:  "processing failed",
	}
)

// run invokes an LVM2 command line and returns the raw result
func run(cmdline string) error {
	cmd := C.CString(cmdline)
	defer C.free(unsafe.Pointer(cmd))
	rc := C.lvm2_run(lvm, cmd)
	if rc == C.LVM2_COMMAND_SUCCEEDED {
		return nil
	}
	return Error(rc)
}

// onexit releases the LVM2 handle
func onexit() {
	C.lvm2_exit(lvm)
}
