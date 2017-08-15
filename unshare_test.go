package csilvm

import (
	"bufio"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"
	"syscall"
)

// isInSeparateMountNamespace determines whether or not the currenet
// process is executing in a separate mount namespace.  It does so by
// comparing the mnt namespace entry for '/' of the current process to
// that of the init daemon. This works because the first thing we do
// after unsharing is to disable shared mount propogation which is
// enabled by default in systemd.
//
// See:
// strace -f unshare -m -- cat /proc/self/mountinfo
// http://man7.org/linux/man-pages/man1/unshare.1.html
// https://unix.stackexchange.com/questions/153665/per-process-private-file-system-mount-points
// http://blog.endpoint.com/2012/01/linux-unshare-m-for-per-process-private.html
func isInSeparateMountNamespace() bool {
	// Determine the root mountinfo of the init daemon
	pid1mounts, err := os.Open("/proc/1/mountinfo")
	if err != nil {
		panic(err)
	}
	defer pid1mounts.Close()
	pid1line, err := getRootMountEntry(pid1mounts)
	if err != nil {
		log.Printf("Could not find entry for '/' in /proc/1/mountinfo: err=%v", err)
		return false
	}
	// Determine the root mountinfo of the current process.
	selfmounts, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		panic(err)
	}
	defer selfmounts.Close()
	selfline, err := getRootMountEntry(selfmounts)
	if err != nil {
		log.Printf("Could not find entry for '/' in /proc/self/mountinfo: err=%v", err)
		return false
	}
	// If the two are different then the current process is
	// executing in a separate mount namespace.
	return pid1line != selfline
}

func getRootMountEntry(r io.Reader) (line string, err error) {
	br := bufio.NewReader(r)
	for {
		line, err = br.ReadString('\n')
		if err != nil {
			return "", err
		}
		fields := strings.Fields(line)
		if len(fields) < 5 {
			continue
		}
		if fields[4] == "/" {
			return line, nil
		}
	}
}

// makeRootPrivate recursively mount the '/' filesystem as private
// to avoid moutn propogation back to the parent process.
//
// See
// strace -f unshare -m -- cat /proc/self/mountinfo
// http://man7.org/linux/man-pages/man1/unshare.1.html
func makeRootPrivate() error {
	return syscall.Mount("none", "/", "", syscall.MS_REC|syscall.MS_PRIVATE, "")
}

// runTestsInSeparateMountNamespace execute the tests in a separate
// mount namespace, passing command-line flags to the child process.
func runTestsInSeparateMountNamespace() error {
	return newUnsharedMountCmd(os.Args[0], os.Args[1:]...).Run()
}

// newUnsharedMountCmd prepares a `*exec.Cmd` that will execute the child
// process in a separate mount namespace.  As the mount namespace of
// the current process may have various mounts marked as "shared", the
// child process should immediately remount '/' as private (eg., by
// calling `mountRootPrivate`).
func newUnsharedMountCmd(name string, args ...string) *exec.Cmd {
	cmd := exec.Command(name, args...)
	// The new process will execute in a separate mount namespace.
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Cloneflags: syscall.CLONE_NEWNS,
	}
	// Redirect stdout and stderr to that of the current process
	// so verbose test output gets printed interactively.
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	return cmd
}
