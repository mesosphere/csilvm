package csilvm

import (
	"context"
	"encoding/json"
	"sort"
	"sync"

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/mesosphere/csilvm/pkg/singleflight"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var _ = csi.IdentityServer(&identityArbiter{}) // sanity check

type RPCCounters struct {
	IssuedCall func() int64 // callback useful for testing
	SharedCall func() int64 // callback useful for testing
}

type GenericArbiter struct {
	RPCCounters
	singleflight.Group
}

func (g *GenericArbiter) Do(ctx context.Context, key string, nonce []byte, fn func() (interface{}, error)) (interface{}, error) {
	ch, ok := g.DoChan(key, nonce, fn)
	if !ok {
		return nil, status.Error(codes.Aborted, nonceMismatch)
	}
	if g.IssuedCall != nil {
		g.IssuedCall()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-ch:
		if res.Shared && g.SharedCall != nil {
			g.SharedCall()
		}
		return res.Val, res.Err
	}
}

type identityArbiter struct {
	GenericArbiter
	csi.IdentityServer
}

type ArbiterOption func(*GenericArbiter)

type preserveValuesCtx struct {
	context.Context
	inherited context.Context
}

func (p *preserveValuesCtx) Value(key interface{}) (val interface{}) {
	val = p.Context.Value(key)
	if val == nil {
		val = p.inherited.Value(key)
	}
	return
}

func IdentityArbiter(server csi.IdentityServer, opt ...ArbiterOption) csi.IdentityServer {
	var generic GenericArbiter
	for _, f := range opt {
		f(&generic)
	}
	return &identityArbiter{
		GenericArbiter: generic,
		IdentityServer: server,
	}
}

// ctxValues returns a Context that reflects the values from `ctx` without any of the
// deadlines/cancellation associated with it.
func ctxValues(ctx context.Context) context.Context {
	return &preserveValuesCtx{
		Context:   context.Background(),
		inherited: ctx,
	}
}

// Probe can take some time to execute: merge all in-flight Probe requests.
func (v *identityArbiter) Probe(
	ctx context.Context,
	request *csi.ProbeRequest) (*csi.ProbeResponse, error) {

	const key = "prb" // all probe requests are identical, merge all concurrent req's
	worker := func() (interface{}, error) {
		return v.IdentityServer.Probe(ctxValues(ctx), request)
	}
	res, err := v.Do(ctx, key, nil, worker)
	if err != nil {
		return nil, err
	}
	return res.(*csi.ProbeResponse), nil
}

var _ = csi.ControllerServer(&controllerArbiter{}) // sanity check

type controllerArbiter struct {
	GenericArbiter
	csi.ControllerServer
	removeLock sync.RWMutex // removeLock is write-locked for remove ops and read-locked for non-remove ops that could conflict w/ remove.
}

// ControllerArbiter decorates a ControllerServer and also returns a Locker that can be
// used to coordinate operations that should not overlap w/ specific controller ops.
func ControllerArbiter(server csi.ControllerServer, opt ...ArbiterOption) (csi.ControllerServer, sync.Locker) {
	var generic GenericArbiter
	for _, f := range opt {
		f(&generic)
	}
	arb := &controllerArbiter{
		GenericArbiter:   generic,
		ControllerServer: server,
	}
	return arb, arb.removeLock.RLocker()
}

const nonceMismatch = "A competing operation, with conflicting parameters, is already in progress."

// CreateVolume merges idempotent requests; waits for any pending RemoveVolume ops to complete.
func (v *controllerArbiter) CreateVolume(
	ctx context.Context,
	request *csi.CreateVolumeRequest) (*csi.CreateVolumeResponse, error) {

	worker := func() (interface{}, error) {
		v.removeLock.RLock()
		defer v.removeLock.RUnlock()
		return v.ControllerServer.CreateVolume(ctxValues(ctx), request)
	}

	type Nonce struct { // json encoding has predictable key order
		Name             string            `json:"nm,omitempty"`
		MountFS          []string          `json:"fs,omitempty"`
		Parameters       map[string]string `json:"p,omitempty"`
		RequiredCapacity int64             `json:"cr,omitempty"`
		LimitCapacity    int64             `json:"cl,omitempty"`
	}
	buf, err := json.Marshal(&Nonce{
		Name:             request.Name,
		MountFS:          squashCaps(request.GetVolumeCapabilities()...),
		Parameters:       request.Parameters,
		RequiredCapacity: request.GetCapacityRange().GetRequiredBytes(),
		LimitCapacity:    request.GetCapacityRange().GetLimitBytes(),
	})
	if err != nil {
		return nil, err
	}
	const ns = "new/"
	res, err := v.Do(ctx, ns+request.Name, buf, worker)
	if err != nil {
		return nil, err
	}
	return res.(*csi.CreateVolumeResponse), nil
}

func (v *controllerArbiter) DeleteVolume(
	ctx context.Context,
	request *csi.DeleteVolumeRequest) (*csi.DeleteVolumeResponse, error) {

	worker := func() (interface{}, error) {
		v.removeLock.Lock()
		defer v.removeLock.Unlock()
		return v.ControllerServer.DeleteVolume(ctxValues(ctx), request)
	}

	const ns = "del/"
	res, err := v.Do(ctx, ns+request.VolumeId, nil, worker)
	if err != nil {
		return nil, err
	}
	return res.(*csi.DeleteVolumeResponse), nil
}

func (v *controllerArbiter) ControllerPublishVolume(
	ctx context.Context,
	request *csi.ControllerPublishVolumeRequest) (*csi.ControllerPublishVolumeResponse, error) {

	worker := func() (interface{}, error) {
		v.removeLock.RLock()
		defer v.removeLock.RUnlock()
		return v.ControllerServer.ControllerPublishVolume(ctxValues(ctx), request)
	}
	type Nonce struct { // json encoding has predictable key order
		VolID    string   `json:"id,omitempty"`
		NodeID   string   `json:"ni,omitempty"`
		MountFS  []string `json:"fs,omitempty"`
		ReadOnly bool     `json:"ro,omitempty"`
	}
	buf, err := json.Marshal(&Nonce{
		VolID:    request.VolumeId,
		NodeID:   request.NodeId,
		MountFS:  squashCaps(request.VolumeCapability),
		ReadOnly: request.Readonly,
	})
	if err != nil {
		return nil, err
	}
	const ns = "pub/"
	res, err := v.Do(ctx, ns+request.VolumeId, buf, worker)
	if err != nil {
		return nil, err
	}
	return res.(*csi.ControllerPublishVolumeResponse), nil
}

func (v *controllerArbiter) ControllerUnpublishVolume(
	ctx context.Context,
	request *csi.ControllerUnpublishVolumeRequest) (*csi.ControllerUnpublishVolumeResponse, error) {

	worker := func() (interface{}, error) {
		v.removeLock.RLock()
		defer v.removeLock.RUnlock()
		return v.ControllerServer.ControllerUnpublishVolume(ctxValues(ctx), request)
	}
	type Nonce struct { // json encoding has predictable key order
		VolID  string `json:"id,omitempty"`
		NodeID string `json:"ni,omitempty"`
	}
	buf, err := json.Marshal(&Nonce{
		VolID:  request.VolumeId,
		NodeID: request.NodeId,
	})
	if err != nil {
		return nil, err
	}
	const ns = "unpub/"
	res, err := v.Do(ctx, ns+request.VolumeId, buf, worker)
	if err != nil {
		return nil, err
	}
	return res.(*csi.ControllerUnpublishVolumeResponse), nil
}

func (v *controllerArbiter) ValidateVolumeCapabilities(
	ctx context.Context,
	request *csi.ValidateVolumeCapabilitiesRequest) (*csi.ValidateVolumeCapabilitiesResponse, error) {

	worker := func() (interface{}, error) {
		v.removeLock.RLock()
		defer v.removeLock.RUnlock()
		return v.ControllerServer.ValidateVolumeCapabilities(ctxValues(ctx), request)
	}
	type Nonce struct { // json encoding has predictable key order
		VolID   string   `json:"id,omitempty"`
		MountFS []string `json:"fs,omitempty"`
	}
	buf, err := json.Marshal(&Nonce{
		VolID:   request.VolumeId,
		MountFS: squashCaps(request.GetVolumeCapabilities()...),
	})
	if err != nil {
		return nil, err
	}
	const ns = "validate/"
	res, err := v.Do(ctx, ns+request.VolumeId, buf, worker)
	if err != nil {
		return nil, err
	}
	return res.(*csi.ValidateVolumeCapabilitiesResponse), nil
}

func (v *controllerArbiter) ListVolumes(
	ctx context.Context,
	request *csi.ListVolumesRequest) (*csi.ListVolumesResponse, error) {

	worker := func() (interface{}, error) {
		v.removeLock.RLock()
		defer v.removeLock.RUnlock()
		return v.ControllerServer.ListVolumes(ctxValues(ctx), request)
	}
	const ns = "list/"
	res, err := v.Do(ctx, ns+request.StartingToken, nil, worker)
	if err != nil {
		return nil, err
	}
	return res.(*csi.ListVolumesResponse), nil
}

func squashCaps(caps ...*csi.VolumeCapability) (fs []string) {
	for _, vc := range caps {
		if m := vc.GetMount(); m != nil {
			if m.FsType != "" {
				fs = append(fs, m.FsType)
			}
		}
		if b := vc.GetBlock(); b != nil {
			// we need a placeholder...
			fs = append(fs, "**block")
		}
		fs = append(fs, vc.GetAccessMode().GetMode().String())
	}
	sort.Strings(fs)
	return
}

func (v *controllerArbiter) GetCapacity(
	ctx context.Context,
	request *csi.GetCapacityRequest) (*csi.GetCapacityResponse, error) {

	type Key struct { // json encoding has predictable key order
		MountFS    []string          `json:"fs,omitempty"`
		Parameters map[string]string `json:"p,omitempty"`
	}
	key := Key{
		MountFS:    squashCaps(request.GetVolumeCapabilities()...),
		Parameters: request.Parameters,
	}
	buf, err := json.Marshal(&key)
	if err != nil {
		return nil, err
	}

	worker := func() (interface{}, error) {
		v.removeLock.RLock()
		defer v.removeLock.RUnlock()
		return v.ControllerServer.GetCapacity(ctxValues(ctx), request)
	}
	const ns = "cap/"
	res, err := v.Do(ctx, ns+string(buf), nil, worker)
	if err != nil {
		return nil, err
	}
	return res.(*csi.GetCapacityResponse), nil
}

var _ = csi.NodeServer(&nodeArbiter{}) // sanity check

type nodeArbiter struct {
	GenericArbiter
	csi.NodeServer
	g    *singleflight.Group
	lock sync.Locker
}

func NodeArbiter(server csi.NodeServer, lock sync.Locker, opt ...ArbiterOption) csi.NodeServer {
	var generic GenericArbiter
	for _, f := range opt {
		f(&generic)
	}
	return &nodeArbiter{
		GenericArbiter: generic,
		NodeServer:     server,
		g:              new(singleflight.Group),
		lock:           lock,
	}
}
func (v *nodeArbiter) NodePublishVolume(
	ctx context.Context,
	request *csi.NodePublishVolumeRequest) (*csi.NodePublishVolumeResponse, error) {

	worker := func() (interface{}, error) {
		v.lock.Lock()
		defer v.lock.Unlock()
		return v.NodeServer.NodePublishVolume(ctxValues(ctx), request)
	}
	const ns = "pub/"
	type Key struct {
		VolumeID          string `json:"id,omitempty"`
		StagingTargetPath string `json:"sp,omitempty"`
		TargetPath        string `json:"tp,omitempty"`
	}
	buf, err := json.Marshal(&Key{
		VolumeID:          request.VolumeId,
		StagingTargetPath: request.StagingTargetPath,
		TargetPath:        request.TargetPath,
	})
	if err != nil {
		return nil, err
	}
	key := ns + string(buf)
	type Nonce struct {
		MountFS  []string `json:"fs,omitempty"`
		ReadOnly bool     `json:"ro,omitempty"`
	}
	nb, err := json.Marshal(&Nonce{
		MountFS:  squashCaps(request.VolumeCapability),
		ReadOnly: request.Readonly,
	})
	if err != nil {
		return nil, err
	}
	res, err := v.Do(ctx, key, nb, worker)
	if err != nil {
		return nil, err
	}
	return res.(*csi.NodePublishVolumeResponse), nil
}

func (v *nodeArbiter) NodeUnpublishVolume(
	ctx context.Context,
	request *csi.NodeUnpublishVolumeRequest) (*csi.NodeUnpublishVolumeResponse, error) {

	worker := func() (interface{}, error) {
		v.lock.Lock()
		defer v.lock.Unlock()
		return v.NodeServer.NodeUnpublishVolume(ctxValues(ctx), request)
	}
	const ns = "unpub/"
	type Key struct {
		VolumeID   string `json:"id,omitempty"`
		TargetPath string `json:"tp,omitempty"`
	}
	buf, err := json.Marshal(&Key{
		VolumeID:   request.VolumeId,
		TargetPath: request.TargetPath,
	})
	if err != nil {
		return nil, err
	}
	key := ns + string(buf)
	res, err := v.Do(ctx, key, nil, worker)
	if err != nil {
		return nil, err
	}
	return res.(*csi.NodeUnpublishVolumeResponse), nil
}
