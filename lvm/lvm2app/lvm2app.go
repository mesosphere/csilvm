package lvm2app

import (
	"errors"
	"unsafe"
)

// #cgo LDFLAGS: -llvm2app
// #cgo CFLAGS: -Wno-implicit-function-declaration
// #include <stdlib.h>
// #include <lvm2app.h>
import "C"

// LibraryGetVersion corresponds to `lvm_library_get_version` in `lvm2app.h`.
func LibraryGetVersion() string {
	return C.GoString(C.lvm_library_get_version())
}

// LibHandle holds a handle to the `lvm2app` library. The caller must
// call `Close()` when completely done with the library.
type LibHandle struct {
	handle C.lvm_t
}

// Close releases the underlying handle by calling `lvm_quit`.
func (h *LibHandle) Close() error {
	C.lvm_quit(h.handle)
	return nil
}

// Init returns a new lvm2app library handle. It corresponds to
// `lvm_init`. The caller must call `Close()` on the *LibHandle that
// is returned to free the allocated memory.
func Init() (*LibHandle, error) {
	handle := C.lvm_init(nil)
	if handle == nil {
		return nil, errors.New("lvm2app: Init: failed to initialize handle")
	}
	return &LibHandle{handle}, nil
}
