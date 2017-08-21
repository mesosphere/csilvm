// +build e2e_test

package lvm

import (
	"context"
	"sync/atomic"
	"testing"
)

func TestRun(t *testing.T) {
	for ti, tc := range []struct {
		ctx context.Context
	}{
		{nil},
		{context.Background()},
	} {
		ctx, cancel := newContext(tc.ctx)
		if cancel == nil {
			t.Fatalf("test case %d failed: expected non-nil context from NewContext", ti)
		}
		func() {
			defer cancel()

			err := run(ctx, "version")
			if err != nil {
				t.Errorf("test case %d failed: unexpected error %q", ti, err)
			}

			err = run(ctx, "foobar")
			if err != ErrorNoSuchCommand {
				t.Errorf("test case %d failed: unexpected error %q", ti, err)
			}

			if x := atomic.LoadInt32(&handleCount); tc.ctx != nil && x != 1 {
				t.Fatalf("expected one open lvm handle, instead of %d", x)
			}
		}()
	}
	// assert that no handles remain
	if x := atomic.LoadInt32(&handleCount); x != 0 {
		t.Fatalf("expected zero open lvm handles, instead of %d", x)
	}
}
