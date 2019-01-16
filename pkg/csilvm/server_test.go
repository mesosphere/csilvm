package csilvm

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRequestLimitInterceptor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handled := int32(0)
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		atomic.AddInt32(&handled, 1)
		<-ctx.Done()
		return nil, nil
	}
	limiter := RequestLimitInterceptor(5)
	var (
		m sync.Mutex
		e []error
		g sync.WaitGroup
	)
	for i := 0; i < 10; i++ {
		g.Add(1)
		go func() {
			defer g.Done()
			_, err := limiter(ctx, nil, nil, handler)
			if err == nil {
				return
			}
			m.Lock()
			e = append(e, err)
			if len(e) == 5 {
				// the first 5 requests should be handled, the second 5
				// should error. once we see 5 errors, unblock the handlers.
				cancel()
			}
			m.Unlock()
		}()
	}
	g.Wait()

	if x := len(e); x != 5 {
		t.Fatalf("expected 5 errors instead of %d", x)
	}
	for _, err := range e {
		st, ok := status.FromError(err)
		if !ok {
			t.Fatal("unexpected error", err)
		}
		if c := st.Code(); c != codes.Unavailable {
			t.Fatal("unexpected RPC error code", c)
		}
	}
	if handled != 5 {
		t.Fatalf("expected 5 requests to be handled instead of %d", handled)
	}
}
