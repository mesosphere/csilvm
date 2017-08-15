package csilvm

import (
	"log"
	"os"
	"os/exec"
	"syscall"
	"testing"
)

// TestMain attempts to re-execute the tests in a separate mount
// namespace.  Doing so requires root privileges. If doing so fails
// due to insufficient permissions those tests that require a separate
// mount namespace are skipped and the remaining tests are executed
// normally.
func TestMain(m *testing.M) {
	if isInSeparateMountNamespace() {
		// We're executing in a separate mount namespace.
		// Still, the '/' mountpoint may have been marked as
		// 'shared' in which case any changes this process
		// makes will be reflected in the mount namespace of
		// the parent process. This defeats the purpose of
		// using mount namespaces in these tests, but it is
		// the default in systemd.
		//
		// To account for this, we remount '/' as private for
		// the current process.
		if err := makeRootPrivate(); err != nil {
			panic(err)
		}
		os.Exit(m.Run())
	}
	// We are not executing in a separate mount namespace.
	if err := runTestsInSeparateMountNamespace(); err != nil {
		if os.IsPermission(err) {
			log.Printf("Failed  to execute tests in a separate mount namespace: operation not permitted")
			log.Printf("Tests that require a separate mount namespace will be skipped.")
			os.Exit(m.Run())
		}
		if exitErr, ok := err.(*exec.ExitError); ok {
			if status, ok := exitErr.Sys().(syscall.WaitStatus); ok {
				os.Exit(status.ExitStatus())
			}
			os.Exit(1)
		}
		log.Fatalf("Unexpected error running tests: err=%v", err)
	}
}

func TestNormal(t *testing.T) {
	t.Log("I'm a normal test that requires no root privileges!")
}

func TestCreatePhysicalVolume(t *testing.T) {
	if !isInSeparateMountNamespace() {
		t.Skip("Test requires a separate mount namespace.")
	}
	dev, err := newTestDevice()
	if err != nil {
		t.Fatal(err)
	}
	defer checkError(dev.Close)

	pd, err := newPhysicalVolume(dev)
	if err != nil {
		t.Fatal(err)
	}
	defer checkError(pd.Close)
}
