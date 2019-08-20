package csilvm

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

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

func TestSerializingInterceptor(t *testing.T) {
	const workers = 100
	var g sync.WaitGroup
	g.Add(workers)
	calls := 0
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		defer g.Done()
		// this should be safe to do without additional synchronization since
		// invocations of this handler should be serialized by interceptor.
		// requires testing with the -race flag.
		calls++
		return nil, nil
	}
	si := SerializingInterceptor()
	for i := 0; i < workers; i++ {
		go func() {
			if _, err := si(context.Background(), nil, nil, handler); err != nil {
				panic(err)
			}
		}()
	}
	g.Wait()
	if calls != 100 {
		t.Fatalf("expected %d calls instead of %d", workers, calls)
	}
}

func TestSerializingInterceptorCanceled(t *testing.T) {
	const workers = 100
	calls := 0
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		// this should be safe to do without additional synchronization since
		// invocations of this handler should be serialized by interceptor.
		// requires testing with the -race flag.
		calls++
		return nil, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	si := SerializingInterceptor()
	for i := 0; i < workers; i++ {
		go func() {
			if _, err := si(ctx, nil, nil, handler); err != context.Canceled {
				panic(err)
			}
		}()
	}
	if calls != 0 {
		t.Fatalf("expected %d calls instead of %d", 0, calls)
	}
}

func TestRequestQueuingWithInterceptors(t *testing.T) {
	icept := ChainUnaryServer(
		RequestLimitInterceptor(2), // queue length of 2 includes the in-flight request
		SerializingInterceptor(),
	)
	errUnreachable := errors.New("this func should never be called")
	errors := make(chan error, 4)
	report := func(err error) {
		if err == nil {
			return
		}
		select {
		case errors <- err:
		default:
			t.Logf("failed to report error: %v", err)
		}
	}

	// request 1 blocks, forcing request 2 to enqueue
	bg := context.Background()
	var g sync.WaitGroup
	r1accepted := make(chan struct{})
	r1blocks := make(chan struct{})
	r1completed := make(chan struct{})
	g.Add(1)
	go func() {
		defer g.Done()
		_, err := icept(bg, nil, nil, func(context.Context, interface{}) (interface{}, error) {
			defer close(r1completed)
			close(r1accepted)
			t.Log("r1 is blocking")
			<-r1blocks
			t.Log("r1 completing")
			return nil, nil
		})
		report(err)
	}()
	<-r1accepted

	// request 2 should join the queue
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g.Add(1)
	r2queued := make(chan struct{})
	r2exiting := make(chan struct{})
	go func() {
		defer close(r2exiting)
		defer g.Done()
		close(r2queued)
		_, err := icept(ctx, nil, nil, func(context.Context, interface{}) (interface{}, error) {
			t.Log("r2 called")
			return nil, errUnreachable
		})
		report(err)
	}()
	<-r2queued
	for i := 0; i < 100; i++ {
		runtime.Gosched()
	}

	// attempt to enqueue request 3; this fails because the queue is full; this should not block
	_, err := icept(bg, nil, nil, func(context.Context, interface{}) (interface{}, error) {
		t.Fatal("request 3 should never be executed")
		return nil, nil // unreachable
	})
	if st, ok := status.FromError(err); !ok || st.Code() != codes.Unavailable {
		t.Fatalf("unexpected error: %v", err)
	}

	// cancel request 2, it should drop from the queue and generated context.Canceled
	cancel()
	select {
	case <-r2exiting:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for r2 attempt to exit")
	}

	// attempt to enqueue request 4, it should join the queue since there's room now
	r4queued := make(chan struct{})
	r4completed := make(chan struct{})
	g.Add(1)
	go func() {
		defer g.Done()
		close(r4queued)
		_, err := icept(bg, nil, nil, func(context.Context, interface{}) (interface{}, error) {
			defer close(r4completed)
			t.Log("r4 called")
			return nil, nil
		})
		report(err)
	}()
	<-r4queued
	for i := 0; i < 100; i++ {
		runtime.Gosched()
	}

	// r1 completes and r4 is then executed.
	t.Log("completing r1")
	close(r1blocks)
	<-r1completed
	select {
	case <-r4completed:
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for request 4 to complete")
	}

	// we should see a single context.Completed error
	g.Wait()
	close(errors)

	select {
	case err, ok := <-errors:
		if !ok {
			t.Fatal("missing context.Canceled error")
		}
		if err != context.Canceled {
			t.Fatalf("unexpected error: %v", err)
		}
	default:
		t.Fatal("missing context.Canceled error")
	}
	for err := range errors {
		t.Fatalf("unexpected error: %v", err)
	}
}
