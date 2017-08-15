package csilvm

import (
	"context"
	"github.com/mesosphere/csilvm/lvmcmd"
	losetup "gopkg.in/freddierice/go-losetup.v1"
	"io/ioutil"
	"log"
	"os"
)

// This file abstracts operations regarding LVM on loopback devices.
// See http://www.anthonyldechiaro.com/blog/2010/12/19/lvm-loopback-how-to/

// newTestDevice returns a file-backed loop-device.  The caller is
// responsible for calling `Close()` on the result when done with it.
func newTestDevice() (device *Device, err error) {
	var cleanup cleanupSteps
	defer func() {
		// We wrap `unwindOnError` in a closure to bind the
		// `err` named variably lazily.
		log.Printf("unwind on err: err=%v", err)
		cleanup.unwindOnError(err)
	}()

	// Create a tempfile to use as the target of our loop device.
	file, err := ioutil.TempFile("", "test-dev")
	if err != nil {
		return nil, err
	}
	// If anything goes wrong, remove the tempfile.
	cleanup.add(func() error { return os.Remove(file.Name()) })
	// Close the file as we're not going to manipulate its
	// contents manually.
	if err := file.Close(); err != nil {
		return nil, err
	}

	// Resize the file.
	const MiB = 1 << 20
	if err := os.Truncate(file.Name(), 10*MiB); err != nil {
		return nil, err
	}

	// Attach a loop device
	const (
		offset = 0
		ro     = false
	)
	lodev, err := losetup.Attach(file.Name(), offset, ro)
	if err != nil {
		return nil, err
	}
	cleanup.add(func() error { return lodev.Detach() })
	// https://www.howtogeek.com/howto/40702/how-to-manage-and-use-lvm-logical-volume-management-in-ubuntu/
	return &Device{lodev, file.Name()}, nil
}

type Device struct {
	lodev           losetup.Device
	backingFilePath string
}

func (d *Device) Path() string {
	return d.lodev.Path()
}

func (d *Device) String() string {
	return d.lodev.Path()
}

// Close detaches the loop device and removes the backing file.
func (d *Device) Close() error {
	if err := d.lodev.Detach(); err != nil {
		return err
	}
	return os.Remove(d.backingFilePath)
}

type PhysicalVolume struct {
	dev *Device
}

func newPhysicalVolume(dev *Device) (*PhysicalVolume, error) {
	if err := lvmcmd.Run(context.Background(), "pvcreate "+dev.Path()); err != nil {
		return nil, err
	}
	return &PhysicalVolume{dev}, nil
}

func (pv *PhysicalVolume) Close() error {
	return lvmcmd.Run(context.Background(), "pvremove "+pv.dev.Path())
}

// cleanupSteps performs deferred cleanup on condition that an error
// was returned in the caller. This simplifies code where earlier
// steps need to be undone if a later step fails.  It is not currently
// resilient to panics as this library is not expected to panic.
type cleanupSteps []func() error

// add appends the given cleanup function to those that will be called
// if an error occurs.
func (fns *cleanupSteps) add(fn func() error) {
	*fns = append(*fns, fn)
}

// unwindOnError calls the cleanup funcions in LIFO order. It panics
// if any of them return an error as failure during recovery is
// itself unrecoverable.
func (fns *cleanupSteps) unwindOnError(err error) {
	if err == nil {
		// No error was returned so we return without
		// performing any cleanup.
		return
	}
	// There was some error. Preform cleanup and return. If any of
	// the cleanup functions return and error, we do the only
	// sensible thing and panic.
	for _, fn := range *fns {
		defer func(clean func() error) { checkError(clean) }(fn)
	}
}

// checkError calls `fn` and panics if it returns an error.
func checkError(fn func() error) {
	if err := fn(); err != nil {
		panic(err)
	}
}
