package lvmcmd

import (
	"context"
	"log"
	"sync/atomic"
	"unsafe"
)

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
		if x := len(file); x > 15 {
			file = file[x-15:]
		}
		log.Printf("%s lvm [%15s:%5d] (%3d) %s", levelStr[level], file, line, dmErrno, message)
	}

	errorMessages = map[Error]string{
		ErrorNoSuchCommand:     "no such command",
		ErrorInvalidParameters: "invalid parameters",
		ErrorProcessingFailed:  "processing failed",
	}
)

type contextKey int

const (
	contextKeyHandle = contextKey(iota) // lvm2 handle
)

var handleCount int32

func newContext(ctx context.Context) (context.Context, func()) {
	if ctx == nil {
		return nil, func() {}
	}

	// TODO(jdef): unsure about the thread-safety of `handle`; if I close it at the same time
	// that someone else is using it, what happens?
	handle := unsafe.Pointer(C.lvm2_init())

	if handle == nil {
		return ctx, func() {}
	}

	atomic.AddInt32(&handleCount, 1)

	ctx, cancel := context.WithCancel(context.WithValue(ctx, contextKeyHandle, handle))

	return ctx, func() {
		defer func() {
			C.lvm2_exit(handle)
			c := atomic.AddInt32(&handleCount, -1)
			if c < 0 {
				panic("lvm: handle count should never fall below zero")
			}
		}()
		cancel()
	}
}

func fromContext(ctx context.Context) (p unsafe.Pointer, ok bool) {
	if ctx != nil {
		p, ok = ctx.Value(contextKeyHandle).(unsafe.Pointer)
	}
	return
}

func init() {
	C.lvm2_log_fn(C.lvm2_log_fn_t(C.logging_bridge))
}

// run invokes an LVM2 command line and returns the raw result
func run(ctx context.Context, cmdline string) error {
	cmd := C.CString(cmdline)
	defer C.free(unsafe.Pointer(cmd))

	var (
		handle, _ = fromContext(ctx) // handle may be nil, lvm2 API says that's OK
		rc        = C.lvm2_run(handle, cmd)
	)
	if rc == C.LVM2_COMMAND_SUCCEEDED {
		return nil
	}
	return Error(rc)
}
