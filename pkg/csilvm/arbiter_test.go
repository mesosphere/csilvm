package csilvm

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
)

const patience = 5 * time.Second // XXX bump this for CI

type fakeIdentityServer struct {
	csi.IdentityServer
	probe func(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error)
}

func (f *fakeIdentityServer) Probe(
	ctx context.Context,
	request *csi.ProbeRequest) (*csi.ProbeResponse, error) {
	return f.probe(ctx, request)
}

var tooManyCalls = errors.New("rpc called too many times")

func calledAtMostOnce(t *testing.T) (func(), <-chan error) {
	var calls int32
	errors := make(chan error, 1)
	return func() {
		latest := atomic.AddInt32(&calls, 1)
		if latest > 1 {
			// should only be called once
			t.Log("called too many times")
			select {
			case errors <- tooManyCalls:
			default:
			}
		}
	}, errors
}

type result struct {
	response interface{}
	err      error
}

func newResult(response interface{}, err error) result {
	if err != nil {
		return result{err: err}
	}
	return result{response: response}
}

func newCounter() (get, inc func() int64) {
	var val int64
	return func() int64 { return atomic.LoadInt64(&val) },
		func() int64 { return atomic.AddInt64(&val, 1) }
}

func arbiterCounters() (_ ArbiterOption, issued func() int64, shared func() int64) {
	getIssued, incIssued := newCounter()
	getShared, incShared := newCounter()
	return func(g *GenericArbiter) {
		g.IssuedCall = incIssued
		g.SharedCall = incShared
	}, getIssued, getShared
}

func waitForCounter(t *testing.T, val int64, counter func() int64, d time.Duration) {
	t.Helper()
	deadline := time.Now().Add(d)
	var found bool
	var last int64
	for {
		last = counter()
		if last == val {
			found = true
			break
		}
		if !time.Now().Before(deadline) {
			break
		}
		time.Sleep(250 * time.Millisecond)
	}
	if !found {
		t.Errorf("timed out waiting for counter (%d) to match expectations (%d)", last, val)
	}
}

var errFailedRPC = errors.New("someRPCError")

func TestIdentityArbiterProbe(t *testing.T) {
	rpcComplete := make(chan struct{})
	checkCalledOnce, errors := calledAtMostOnce(t)
	const workers = 5
	fakeWrapped := &fakeIdentityServer{
		probe: func(context.Context, *csi.ProbeRequest) (*csi.ProbeResponse, error) {
			checkCalledOnce()
			<-rpcComplete
			return nil, errFailedRPC
		},
	}
	opt, issuedCalls, sharedCalls := arbiterCounters()
	arb := IdentityArbiter(fakeWrapped, opt) // this is the arbiter we're testing
	var (
		m       sync.Mutex
		g       sync.WaitGroup
		results []result
	)
	g.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer g.Done()
			resp, err := arb.Probe(context.Background(), nil)
			m.Lock()
			defer m.Unlock()
			results = append(results, newResult(resp, err))
		}()
	}

	// wait for issued calls == number of workers
	waitForCounter(t, workers, issuedCalls, patience)
	close(rpcComplete)
	g.Wait()

	// check sharedCalls
	waitForCounter(t, workers, sharedCalls, 0)

	// check for expected errors
	if len(results) != workers {
		t.Fatalf("unexpected number of results: %v", results)
	}
	for _, r := range results {
		if r.err == errFailedRPC {
			continue
		}
		t.Fatalf("unexpected error: %v", r.err)
	}

	// check for unexpected errors
	select {
	case err := <-errors:
		t.Fatal(err)
	default:
	}
}

type fakeControllerServer struct {
	csi.ControllerServer
	createVolume               func(context.Context, *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error)
	deleteVolume               func(context.Context, *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error)
	controllerPublishVolume    func(context.Context, *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error)
	controllerUnpublishVolume  func(context.Context, *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error)
	validateVolumeCapabilities func(context.Context, *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error)
	listVolumes                func(context.Context, *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error)
	getCapacity                func(context.Context, *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error)
}

func (f *fakeControllerServer) CreateVolume(
	ctx context.Context,
	request *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
	return f.createVolume(ctx, request)
}

func (f *fakeControllerServer) DeleteVolume(
	ctx context.Context,
	request *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
	return f.deleteVolume(ctx, request)
}

func (f *fakeControllerServer) ControllerPublishVolume(
	ctx context.Context,
	request *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
	return f.controllerPublishVolume(ctx, request)
}

func (f *fakeControllerServer) ControllerUnpublishVolume(
	ctx context.Context,
	request *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
	return f.controllerUnpublishVolume(ctx, request)
}

func (f *fakeControllerServer) ValidateVolumeCapabilities(
	ctx context.Context,
	request *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
	return f.validateVolumeCapabilities(ctx, request)
}

func (f *fakeControllerServer) ListVolumes(
	ctx context.Context,
	request *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
	return f.listVolumes(ctx, request)
}

func (f *fakeControllerServer) GetCapacity(
	ctx context.Context,
	request *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
	return f.getCapacity(ctx, request)
}

func TestControllerArbiter(t *testing.T) {
	for ti, tc := range []struct {
		conf func(*fakeControllerServer, func() (interface{}, error))
		exec func(csi.ControllerServer) (interface{}, error)
	}{
		{
			conf: func(f *fakeControllerServer, action func() (interface{}, error)) {
				f.createVolume = func(context.Context, *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {
					v, err := action()
					if v != nil {
						return v.(*csi.CreateVolumeResponse), err
					}
					return nil, err
				}
			},
			exec: func(arb csi.ControllerServer) (interface{}, error) {
				return arb.CreateVolume(context.Background(), &csi.CreateVolumeRequest{})
			},
		},
		{
			conf: func(f *fakeControllerServer, action func() (interface{}, error)) {
				f.deleteVolume = func(context.Context, *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {
					v, err := action()
					if v != nil {
						return v.(*csi.DeleteVolumeResponse), err
					}
					return nil, err
				}
			},
			exec: func(arb csi.ControllerServer) (interface{}, error) {
				return arb.DeleteVolume(context.Background(), &csi.DeleteVolumeRequest{})
			},
		},
		{
			conf: func(f *fakeControllerServer, action func() (interface{}, error)) {
				f.controllerPublishVolume = func(context.Context, *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {
					v, err := action()
					if v != nil {
						return v.(*csi.ControllerPublishVolumeResponse), err
					}
					return nil, err
				}
			},
			exec: func(arb csi.ControllerServer) (interface{}, error) {
				return arb.ControllerPublishVolume(context.Background(), &csi.ControllerPublishVolumeRequest{})
			},
		},
		{
			conf: func(f *fakeControllerServer, action func() (interface{}, error)) {
				f.controllerUnpublishVolume = func(context.Context, *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {
					v, err := action()
					if v != nil {
						return v.(*csi.ControllerUnpublishVolumeResponse), err
					}
					return nil, err
				}
			},
			exec: func(arb csi.ControllerServer) (interface{}, error) {
				return arb.ControllerUnpublishVolume(context.Background(), &csi.ControllerUnpublishVolumeRequest{})
			},
		},
		{
			conf: func(f *fakeControllerServer, action func() (interface{}, error)) {
				f.validateVolumeCapabilities = func(context.Context, *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {
					v, err := action()
					if v != nil {
						return v.(*csi.ValidateVolumeCapabilitiesResponse), err
					}
					return nil, err
				}
			},
			exec: func(arb csi.ControllerServer) (interface{}, error) {
				return arb.ValidateVolumeCapabilities(context.Background(), &csi.ValidateVolumeCapabilitiesRequest{})
			},
		},
		{
			conf: func(f *fakeControllerServer, action func() (interface{}, error)) {
				f.listVolumes = func(context.Context, *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {
					v, err := action()
					if v != nil {
						return v.(*csi.ListVolumesResponse), err
					}
					return nil, err
				}
			},
			exec: func(arb csi.ControllerServer) (interface{}, error) {
				return arb.ListVolumes(context.Background(), &csi.ListVolumesRequest{})
			},
		},
		{
			conf: func(f *fakeControllerServer, action func() (interface{}, error)) {
				f.getCapacity = func(context.Context, *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {
					v, err := action()
					if v != nil {
						return v.(*csi.GetCapacityResponse), err
					}
					return nil, err
				}
			},
			exec: func(arb csi.ControllerServer) (interface{}, error) {
				return arb.GetCapacity(context.Background(), &csi.GetCapacityRequest{})
			},
		},
	} {
		t.Run(strconv.Itoa(ti), func(t *testing.T) {
			testControllerArbiter(t, tc.conf, tc.exec)
		})
	}
}

func testControllerArbiter(t *testing.T, conf func(*fakeControllerServer, func() (interface{}, error)), exec func(csi.ControllerServer) (interface{}, error)) {
	rpcComplete := make(chan struct{})
	checkCalledOnce, errors := calledAtMostOnce(t)
	const workers = 5
	var fakeWrapped fakeControllerServer
	conf(&fakeWrapped, func() (interface{}, error) {
		checkCalledOnce()
		<-rpcComplete
		return nil, errFailedRPC
	})
	opt, issuedCalls, sharedCalls := arbiterCounters()
	arb, _ := ControllerArbiter(&fakeWrapped, opt) // this is the arbiter we're testing
	var (
		m       sync.Mutex
		g       sync.WaitGroup
		results []result
	)
	g.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer g.Done()
			resp, err := exec(arb)
			m.Lock()
			defer m.Unlock()
			results = append(results, newResult(resp, err))
		}()
	}

	// wait for issued calls == number of workers
	waitForCounter(t, workers, issuedCalls, patience)
	close(rpcComplete)
	g.Wait()

	// check sharedCalls
	waitForCounter(t, workers, sharedCalls, 0)

	// check for expected errors
	if len(results) != workers {
		t.Fatalf("unexpected number of results: %v", results)
	}
	for _, r := range results {
		if r.err == errFailedRPC {
			continue
		}
		t.Fatalf("unexpected error: %v", r.err)
	}

	// check for unexpected errors
	select {
	case err := <-errors:
		t.Fatal(err)
	default:
	}
}

type fakeNodeServer struct {
	csi.NodeServer
	nodePublishVolume   func(context.Context, *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error)
	nodeUnpublishVolume func(context.Context, *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error)
}

func (f *fakeNodeServer) NodePublishVolume(
	ctx context.Context,
	request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
	return f.nodePublishVolume(ctx, request)
}

func (f *fakeNodeServer) NodeUnpublishVolume(
	ctx context.Context,
	request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
	return f.nodeUnpublishVolume(ctx, request)
}

func TestNodeArbiter(t *testing.T) {
	for ti, tc := range []struct {
		conf func(*fakeNodeServer, func() (interface{}, error))
		exec func(csi.NodeServer) (interface{}, error)
	}{
		{
			conf: func(f *fakeNodeServer, action func() (interface{}, error)) {
				f.nodePublishVolume = func(context.Context, *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {
					v, err := action()
					if v != nil {
						return v.(*csi.NodePublishVolumeResponse), err
					}
					return nil, err
				}
			},
			exec: func(arb csi.NodeServer) (interface{}, error) {
				return arb.NodePublishVolume(context.Background(), &csi.NodePublishVolumeRequest{})
			},
		},
		{
			conf: func(f *fakeNodeServer, action func() (interface{}, error)) {
				f.nodeUnpublishVolume = func(context.Context, *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {
					v, err := action()
					if v != nil {
						return v.(*csi.NodeUnpublishVolumeResponse), err
					}
					return nil, err
				}
			},
			exec: func(arb csi.NodeServer) (interface{}, error) {
				return arb.NodeUnpublishVolume(context.Background(), &csi.NodeUnpublishVolumeRequest{})
			},
		},
	} {
		t.Run(strconv.Itoa(ti), func(t *testing.T) {
			testNodeArbiter(t, tc.conf, tc.exec)
		})
	}
}

func testNodeArbiter(t *testing.T, conf func(*fakeNodeServer, func() (interface{}, error)), exec func(csi.NodeServer) (interface{}, error)) {
	rpcComplete := make(chan struct{})
	checkCalledOnce, errors := calledAtMostOnce(t)
	const workers = 5
	var fakeWrapped fakeNodeServer
	conf(&fakeWrapped, func() (interface{}, error) {
		checkCalledOnce()
		<-rpcComplete
		return nil, errFailedRPC
	})
	opt, issuedCalls, sharedCalls := arbiterCounters()
	arb := NodeArbiter(&fakeWrapped, new(sync.Mutex), opt) // this is the arbiter we're testing
	var (
		m       sync.Mutex
		g       sync.WaitGroup
		results []result
	)
	g.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer g.Done()
			resp, err := exec(arb)
			m.Lock()
			defer m.Unlock()
			results = append(results, newResult(resp, err))
		}()
	}

	// wait for issued calls == number of workers
	waitForCounter(t, workers, issuedCalls, patience)
	close(rpcComplete)
	g.Wait()

	// check sharedCalls
	waitForCounter(t, workers, sharedCalls, 0)

	// check for expected errors
	if len(results) != workers {
		t.Fatalf("unexpected number of results: %v", results)
	}
	for _, r := range results {
		if r.err == errFailedRPC {
			continue
		}
		t.Fatalf("unexpected error: %v", r.err)
	}

	// check for unexpected errors
	select {
	case err := <-errors:
		t.Fatal(err)
	default:
	}
}

func TestCtxValues(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(100*time.Second))
	ctx = context.WithValue(ctx, "key", "value")
	cancel()

	ctx2 := ctxValues(ctx)
	select {
	case <-ctx2.Done():
		t.Fatal("ctx2 unexpectedly completed")
	default:
	}

	if d, ok := ctx2.Deadline(); ok {
		t.Fatalf("ctx2 has an unexpected deadline: %v", d)
	}

	v := ctx2.Value("key")
	if v.(string) != "value" {
		t.Fatalf("unexpected value %v", v)
	}

	ctx2 = context.WithValue(ctx2, "key", "value2")
	v = ctx2.Value("key")
	if v.(string) != "value2" {
		t.Fatalf("unexpected value %v", v)
	}
}
