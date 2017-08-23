package lvm

// #cgo LDFLAGS: -llvm2app
// #cgo CFLAGS: -Wno-implicit-function-declaration
// #include <stdlib.h>
// #include <lvm2app.h>
import "C"

// LibraryGetVersion corresponds to `lvm_library_get_version` in `lvm2app.h`.
func LibraryGetVersion() string {
	return C.GoString(C.lvm_library_get_version())
}
