package lvmcmd

import (
	"C"
	"log"
)

type LogLevel int

const (
	LogLevelFatal = LogLevel(iota)
	LogLevelError
	LogLevelWarn
	LogLevelInfo
	LogLevelDebug
	LogLevelTrace
)

var (
	MaxLevel = LogLevelInfo // MaxLevel is the threshold for the noisiest log level, set once at init time

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
		if x := len(file); x > 12 {
			file = file[x-12:]
		}
		log.Printf("%s lvm [%12s:%5d] (%3d) %s", levelStr[level], file, line, dmErrno, message)
	}
)

//export logCallback
func logCallback(level C.int, file *C.char, line, dmErrno C.int, message *C.char) {
	logLevel, ok := logLevels[level]
	if !ok {
		// this seems like a reasonable default if we see an unexpected log level
		logLevel = LogLevelError
	}
	logLevel.log(C.GoString(file), int(line), int(dmErrno), C.GoString(message))
}

func (l LogLevel) log(file string, line, dmErrno int, message string) {
	if l <= MaxLevel {
		LogFunc(l, file, line, dmErrno, message)
	}
}
