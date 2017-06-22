package lvmcmd

import "C"

//export logCallback
func logCallback(level C.int, file *C.char, line, dmErrno C.int, message *C.char) {
	logLevel, ok := logLevels[level]
	if !ok {
		// this seems like a reasonable default if we see an unexpected log level
		logLevel = LogLevelError
	}
	logLevel.log(C.GoString(file), int(line), int(dmErrno), C.GoString(message))
}
