package lvm

import (
	"log"
	"os/exec"
	"unsafe"
)

// #cgo LDFLAGS: -llvm2cmd
// #cgo CFLAGS: -Wno-implicit-function-declaration
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

type LVM2CMDError int

const (
	ErrorNoSuchCommand     = LVM2CMDError(C.LVM2_NO_SUCH_COMMAND)
	ErrorInvalidParameters = LVM2CMDError(C.LVM2_INVALID_PARAMETERS)
	ErrorProcessingFailed  = LVM2CMDError(C.LVM2_PROCESSING_FAILED)
)

func (e LVM2CMDError) Error() string {
	msg := errorMessages[e]
	if msg == "" {
		return "unknown error"
	}
	return msg
}

type LogLevel int

const (
	LogLevelFatal = LogLevel(iota)
	LogLevelError
	LogLevelWarn
	LogLevelInfo
	LogLevelDebug
	LogLevelTrace
)

func (l LogLevel) log(file string, line, dmErrno int, message string) {
	LogFunc(l, file, line, dmErrno, message)
}

var (
	logLevels = map[C.int]LogLevel{
		C.LVM2_LOG_FATAL:        LogLevelFatal,
		C.LVM2_LOG_ERROR:        LogLevelError,
		C.LVM2_LOG_PRINT:        LogLevelWarn,
		C.LVM2_LOG_VERBOSE:      LogLevelInfo,
		C.LVM2_LOG_VERY_VERBOSE: LogLevelDebug,
		C.LVM2_LOG_DEBUG:        LogLevelTrace,
	}

	levelStr = map[LogLevel]string{
		LogLevelFatal: "F",
		LogLevelError: "E",
		LogLevelWarn:  "W",
		LogLevelInfo:  "I",
		LogLevelDebug: "D",
		LogLevelTrace: "T",
	}

	LogFunc = func(level LogLevel, file string, line, dmErrno int, message string) {
		// TODO(jdef) eventually tie into some better logging subsystem
		if level > LogLevelInfo {
			return
		}
		if x := len(file); x > 15 {
			file = file[x-15:]
		}
		log.Printf("%s lvm [%15s:%5d] (%3d) %s", levelStr[level], file, line, dmErrno, message)
	}

	errorMessages = map[LVM2CMDError]string{
		ErrorNoSuchCommand:     "no such command",
		ErrorInvalidParameters: "invalid parameters",
		ErrorProcessingFailed:  "processing failed",
	}
)

func init() {
	C.lvm2_log_fn(C.lvm2_log_fn_t(C.logging_bridge))
}

// run_lvm2cmd invokes an LVM2 command line and returns the raw result
func run_lvm2cmd(cmdline string) error {
	panic("lvm: calling this function leaks memory, see DCOS-19141")
	cmd := C.CString(cmdline)
	defer C.free(unsafe.Pointer(cmd))
	rc := C.lvm2_run(nil, cmd)
	if rc == C.LVM2_COMMAND_SUCCEEDED {
		return nil
	}
	return LVM2CMDError(rc)
}

// PVScan runs the `pvscan --cache <dev>` command. It scans for the
// device at `dev` and adds it to the LVM metadata cache if `lvmetad`
// is running. If `dev` is an empty string, it scans all devices.
func PVScan(dev string) error {
	// This function used to call run_lvm2cmd but it was determined that that
	// function leaks memory. We now call the binary instead.
	args := []string{"--cache"}
	if dev != "" {
		args = append(args, dev)
	}
	return exec.Command("pvscan", args...).Run()
}
