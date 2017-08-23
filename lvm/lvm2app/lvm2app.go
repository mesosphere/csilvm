package lvm2app

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"unsafe"
)

/*
#cgo LDFLAGS: -llvm2app -ldevmapper
#cgo CFLAGS: -Wno-implicit-function-declaration
#cgo pkg-config: --libs glib-2.0
#include <stdlib.h>
#include <lvm2app.h>
#include <libdevmapper.h>

// Returns an array of strings obtained from a `dm_list` with entries of type `lvm_str_list`.
// This is useful for obtaining a list of strings from eg., `lvm_list_vg_names()`.
char** csilvm_get_strings_from_lvm_str_list(const struct dm_list *list)
{
	struct lvm_str_list *strl;
	char **results;
	unsigned int list_size;

	list_size = dm_list_size(list);
	results = malloc(sizeof (char *) * list_size);
	for(unsigned int ii=0; ii<list_size; ii++)
	{
		dm_list_iterate_items(strl, list) {
			results[ii] = strdup(strl->str);
		}
	}
	return results;
}

*/
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

// Err returns the error returned for the last operation, if any. The
// return type is `*Error`.
func (h *LibHandle) Err() error {
	errno := C.lvm_errno(h.handle)
	if errno == 0 {
		return nil
	}
	errmsg := C.GoString(C.lvm_errmsg(h.handle))
	// Concatenate multi-line errors.
	errmsg = strings.Replace(errmsg, "\n", " ", -1)
	// Get the name of the calling function.
	pc, _, _, ok := runtime.Caller(1)
	details := runtime.FuncForPC(pc)
	caller := ""
	if ok && details != nil {
		tokens := strings.Split(details.Name(), ".")
		caller = tokens[len(tokens)-1]
	}
	return &Error{caller, errmsg, int(errno)}
}

type Error struct {
	Caller string
	Errmsg string
	Errno  int
}

func (e *Error) Error() string {
	caller := e.Caller + ": "
	return fmt.Sprintf("lvm: %s%s (%d)", caller, e.Errmsg, e.Errno)
}

func ValidateVolumeGroupName(handle *LibHandle, name string) error {
	cname := C.CString(name)
	defer C.free(unsafe.Pointer(cname))
	res := C.lvm_vg_name_validate(handle.handle, cname)
	if res != 0 {
		return handle.Err()
	}
	return nil
}

// GoStrings converts an array of C strings to a slice of go strings.
// See https://stackoverflow.com/questions/36188649/cgo-char-to-slice-string
func GoStrings(argc C.uint, argv **C.char) []string {
	length := int(argc)
	tmpslice := (*[1 << 30]*C.char)(unsafe.Pointer(argv))[:length:length]
	gostrings := make([]string, length)
	for i, s := range tmpslice {
		gostrings[i] = C.GoString(s)
	}
	return gostrings
}

// ListVolumeGroupNames returns the names of the list of volume groups.
//
// It is equivalent to `lvm_list_vg_names` followed by
// `dm_list_iterate_items` to accumulate the string values.
//
// This does not normally scan for devices. To scan for devices, use
// the `Scan()` function.
func ListVolumeGroupNames(handle *LibHandle) ([]string, error) {
	// Get the list of volume group names
	// The memory for the `dm_list` is freed when the handle is freed.
	dm_list_names := C.lvm_list_vg_names(handle.handle)
	if dm_list_names == nil {
		// We failed to allocate memory for the list.
		return nil, handle.Err()
	}
	// If the list is empty, return nil.
	// See https://github.com/twitter/bittern/blob/a95aab6d4a43c7961d36bacd9f4e23387a4cb9d7/lvm2/libdm/datastruct/list.c#L81
	if int(C.dm_list_empty(dm_list_names)) != 0 {
		return nil, nil
	}
	size := C.dm_list_size(dm_list_names)
	if int(size) == 0 {
		// We just checked that the lists is non-empty so we
		// expect it's size to be greater than zero.
		panic("lvm2app: unexpected zero-length list")
	}
	cvgnames := C.csilvm_get_strings_from_lvm_str_list(dm_list_names)
	defer C.free(unsafe.Pointer(cvgnames))
	// Transform the array of C strings into a []string.
	vgnames := GoStrings(size, cvgnames)
	return vgnames, nil
}

// ListVolumeGroupUUIDs returns the UUIDs of the list of volume groups.
//
// It is equivalent to `lvm_list_vg_names` followed by
// `dm_list_iterate_items` to accumulate the string values.
//
// This does not normally scan for devices. To scan for devices, use
// the `Scan()` function.
func ListVolumeGroupUUIDs(handle *LibHandle) ([]string, error) {
	// Get the list of volume group UUIDs
	// The memory for the `dm_list` is freed when the handle is freed.
	dm_list_uuids := C.lvm_list_vg_uuids(handle.handle)
	if dm_list_uuids == nil {
		// We failed to allocate memory for the list.
		return nil, handle.Err()
	}
	// If the list is empty, return nil.
	// See https://github.com/twitter/bittern/blob/a95aab6d4a43c7961d36bacd9f4e23387a4cb9d7/lvm2/libdm/datastruct/list.c#L81
	if int(C.dm_list_empty(dm_list_uuids)) != 0 {
		return nil, nil
	}
	size := C.dm_list_size(dm_list_uuids)
	if int(size) == 0 {
		// We just checked that the lists is non-empty so we
		// expect it's size to be greater than zero.
		panic("lvm2app: unexpected zero-length list")
	}
	cvguuids := C.csilvm_get_strings_from_lvm_str_list(dm_list_uuids)
	defer C.free(unsafe.Pointer(cvguuids))
	// Transform the array of C strings into a []string.
	vguuids := GoStrings(size, cvguuids)
	return vguuids, nil
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
