package lvmcmd

import (
	"C"
	"log"
)

//export logCallback
func logCallback(level C.int, file *C.char, line, dmErrno C.int, message *C.char) {
	log.Printf("%d %s %d %d %s", level, C.GoString(file), line, dmErrno, C.GoString(message))
}
