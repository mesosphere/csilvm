// +build e2e_test

package lvmcmd

import (
	"os"
	"testing"
)

func TestMain(m *testing.M) {
	rc := m.Run()
	onexit() // shut down liblvm2
	os.Exit(rc)
}

func TestRun(t *testing.T) {
	err := run("version")
	if err != nil {
		t.Errorf("unexpected error %q", err)
	}

	err = run("foobar")
	if err != ErrorNoSuchCommand {
		t.Errorf("unexpected error %q", err)
	}
}
