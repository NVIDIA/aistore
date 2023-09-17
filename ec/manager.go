// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"errors"
	"fmt"
	"io"
	"sync"
	"unsafe"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type Manager struct {
	mu sync.RWMutex

	t         cluster.Target
	smap      *meta.Smap
	targetCnt atomic.Int32 // atomic, to avoid races between read/write on smap
	bmd       *meta.BMD    // bmd owner

	xacts map[string]*BckXacts // bckName -> xctn map, only ais buckets allowed, no naming collisions

	bundleEnabled atomic.Bool // to disable and enable on the fly
	netReq        string      // network used to send object request
	netResp       string      // network used to send/receive slices
	reqBundle     atomic.Pointer
	respBundle    atomic.Pointer
}

var (
	ECM        *Manager
	errSkipped = errors.New("skipped") // CT is skipped due to EC unsupported for the content type
)

func initManager(t cluster.Target) error {
	var (
		netReq, netResp = cmn.NetIntraControl, cmn.NetIntraData
		sowner          = t.Sowner()
		smap            = sowner.Get()
	)
	ECM = &Manager{
		netReq:    netReq,
		netResp:   netResp,
		t:         t,
		smap:      smap,
		targetCnt: *atomic.NewInt32(int32(smap.CountActiveTs())),
		bmd:       t.Bowner().Get(),
		xacts:     make(map[string]*BckXacts),
	}

	if ECM.bmd.IsECUsed() {
		return ECM.initECBundles()
	}
	return nil
}

func (mgr *Manager) req() *bundle.Streams {
	return (*bundle.Streams)(mgr.reqBundle.Load())
}

func (mgr *Manager) resp() *bundle.Streams {
	return (*bundle.Streams)(mgr.respBundle.Load())
}

func (mgr *Manager) initECBundles() error {
	if !mgr.bundleEnabled.CAS(false, true) {
		return nil
	}
	if err := transport.Handle(ReqStreamName, ECM.recvRequest); err != nil {
		return fmt.Errorf("failed to register recvRequest: %v", err)
	}
	if err := transport.Handle(RespStreamName, ECM.recvResponse); err != nil {
		return fmt.Errorf("failed to register respResponse: %v", err)
	}
	cbReq := func(hdr transport.ObjHdr, reader io.ReadCloser, _ any, err error) {
		if err != nil {
			nlog.Errorf("failed to request %s: %v", hdr.Cname(), err)
		}
	}
	var (
		client      = transport.NewIntraDataClient()
		config      = cmn.GCO.Get()
		compression = config.EC.Compression
		extraReq    = transport.Extra{Callback: cbReq, Compression: compression}
	)
	reqSbArgs := bundle.Args{
		Multiplier: config.EC.SbundleMult,
		Extra:      &extraReq,
		Net:        mgr.netReq,
		Trname:     ReqStreamName,
	}
	respSbArgs := bundle.Args{
		Multiplier: config.EC.SbundleMult,
		Trname:     RespStreamName,
		Net:        mgr.netResp,
		Extra:      &transport.Extra{Compression: compression},
	}

	sowner := mgr.t.Sowner()
	mgr.reqBundle.Store(unsafe.Pointer(bundle.New(sowner, mgr.t.Snode(), client, reqSbArgs)))
	mgr.respBundle.Store(unsafe.Pointer(bundle.New(sowner, mgr.t.Snode(), client, respSbArgs)))

	mgr.smap = sowner.Get()
	mgr.targetCnt.Store(int32(mgr.smap.CountActiveTs()))
	sowner.Listeners().Reg(mgr)
	return nil
}

func (mgr *Manager) closeECBundles() {
	if !mgr.bundleEnabled.CAS(true, false) {
		return
	}
	mgr.t.Sowner().Listeners().Unreg(mgr)
	mgr.req().Close(false)
	mgr.resp().Close(false)
	transport.Unhandle(ReqStreamName)
	transport.Unhandle(RespStreamName)
}

func (mgr *Manager) NewGetXact(bck *cmn.Bck) *XactGet {
	return NewGetXact(mgr.t, bck, mgr)
}

func (mgr *Manager) NewPutXact(bck *cmn.Bck) *XactPut {
	return NewPutXact(mgr.t, bck, mgr)
}

func (mgr *Manager) NewRespondXact(bck *cmn.Bck) *XactRespond {
	return NewRespondXact(mgr.t, bck, mgr)
}

func (mgr *Manager) RestoreBckGetXact(bck *meta.Bck) (xget *XactGet) {
	xctn, err := _renewXact(bck, apc.ActECGet)
	debug.AssertNoErr(err) // TODO: handle, here and elsewhere
	xget = xctn.(*XactGet)
	mgr.getBckXacts(bck.Name).SetGet(xget)
	return
}

func (mgr *Manager) RestoreBckPutXact(bck *meta.Bck) (xput *XactPut) {
	xctn, err := _renewXact(bck, apc.ActECPut)
	debug.AssertNoErr(err)
	xput = xctn.(*XactPut)
	mgr.getBckXacts(bck.Name).SetPut(xput)
	return
}

func (mgr *Manager) RestoreBckRespXact(bck *meta.Bck) (xrsp *XactRespond) {
	xctn, err := _renewXact(bck, apc.ActECRespond)
	debug.AssertNoErr(err)
	xrsp = xctn.(*XactRespond)
	mgr.getBckXacts(bck.Name).SetReq(xrsp)
	return
}

func _renewXact(bck *meta.Bck, kind string) (cluster.Xact, error) {
	rns := xreg.RenewBucketXact(kind, bck, xreg.Args{})
	if rns.Err != nil {
		return nil, rns.Err
	}
	return rns.Entry.Get(), nil
}

func (mgr *Manager) getBckXacts(bckName string) *BckXacts {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()
	return mgr.getBckXactsUnlocked(bckName)
}

func (mgr *Manager) getBckXactsUnlocked(bckName string) *BckXacts {
	xacts, ok := mgr.xacts[bckName]
	if !ok {
		xacts = &BckXacts{}
		mgr.xacts[bckName] = xacts
	}
	return xacts
}

// A function to process command requests from other targets
func (mgr *Manager) recvRequest(hdr transport.ObjHdr, object io.Reader, err error) error {
	defer transport.FreeRecv(object)
	if err != nil {
		nlog.Errorf("request failed: %v", err)
		return err
	}
	// check if the header contains a valid request
	if len(hdr.Opaque) == 0 {
		err := fmt.Errorf("invalid header: [%+v]", hdr)
		nlog.Errorln(err)
		return err
	}

	unpacker := cos.NewUnpacker(hdr.Opaque)
	iReq := intraReq{}
	if err := unpacker.ReadAny(&iReq); err != nil {
		nlog.Errorf("failed to unmarshal request: %v", err)
		return err
	}

	// command requests should not have a body, but if it has,
	// the body must be drained to avoid errors
	if hdr.ObjAttrs.Size != 0 {
		if _, err := io.ReadAll(object); err != nil {
			nlog.Errorf("failed to read request body: %v", err)
			return err
		}
	}
	bck := meta.CloneBck(&hdr.Bck)
	if err = bck.Init(mgr.t.Bowner()); err != nil {
		if _, ok := err.(*cmn.ErrRemoteBckNotFound); !ok { // is ais
			nlog.Errorf("failed to init bucket %s: %v", bck, err)
			return err
		}
	}
	mgr.RestoreBckRespXact(bck).DispatchReq(iReq, &hdr, bck)
	return nil
}

// A function to process big chunks of data (replica/slice/meta) sent from other targets
func (mgr *Manager) recvResponse(hdr transport.ObjHdr, object io.Reader, err error) error {
	defer transport.DrainAndFreeReader(object)
	if err != nil {
		nlog.Errorf("receive failed: %v", err)
		return err
	}
	// check if the request is valid
	if len(hdr.Opaque) == 0 {
		err := fmt.Errorf("invalid header: [%+v]", hdr)
		nlog.Errorln(err)
		return err
	}

	unpacker := cos.NewUnpacker(hdr.Opaque)
	iReq := intraReq{}
	if err := unpacker.ReadAny(&iReq); err != nil {
		nlog.Errorf("Failed to unmarshal request: %v", err)
		return err
	}
	bck := meta.CloneBck(&hdr.Bck)
	if err = bck.Init(mgr.t.Bowner()); err != nil {
		if _, ok := err.(*cmn.ErrRemoteBckNotFound); !ok { // is ais
			nlog.Errorln(err)
			return err
		}
	}
	switch hdr.Opcode {
	case reqPut:
		mgr.RestoreBckRespXact(bck).DispatchResp(iReq, &hdr, object)
	case respPut:
		// Process the request even if the number of targets is insufficient
		// (might've started when we had enough)
		mgr.RestoreBckGetXact(bck).DispatchResp(iReq, &hdr, bck, object)
	default:
		debug.Assertf(false, "unknown EC response action %d", hdr.Opcode)
	}
	return nil
}

// EncodeObject generates slices using Reed-Solom algorithm:
//   - lom - object to encode
//   - intra - if true, it is internal request and has low priority
//   - cb - optional callback that is called after the object is encoded
func (mgr *Manager) EncodeObject(lom *cluster.LOM, cb ...cluster.OnFinishObj) error {
	if !lom.Bprops().EC.Enabled {
		return ErrorECDisabled
	}
	if cs := fs.Cap(); cs.Err != nil {
		return cs.Err
	}
	isECCopy := IsECCopy(lom.SizeBytes(), &lom.Bprops().EC)
	targetCnt := mgr.targetCnt.Load()

	// compromise: encoding a small object requires fewer targets
	if required := lom.Bprops().EC.RequiredEncodeTargets(); !isECCopy && int(targetCnt) < required {
		return fmt.Errorf("%v: %d targets required to erasure code %s (have %d)",
			cmn.ErrNotEnoughTargets, required, lom, targetCnt)
	}
	spec, _ := fs.CSM.FileSpec(lom.FQN)
	if spec != nil && !spec.PermToProcess() {
		return errSkipped
	}

	req := allocateReq(ActSplit, lom.LIF())
	req.IsCopy = IsECCopy(lom.SizeBytes(), &lom.Bprops().EC)
	if len(cb) != 0 {
		req.rebuild = true
		req.Callback = cb[0]
	}

	mgr.RestoreBckPutXact(lom.Bck()).encode(req, lom)

	return nil
}

func (mgr *Manager) CleanupObject(lom *cluster.LOM) {
	if !lom.Bprops().EC.Enabled {
		return
	}
	debug.Assert(lom.FQN != "" && lom.Mountpath().Path != "")
	req := allocateReq(ActDelete, lom.LIF())
	mgr.RestoreBckPutXact(lom.Bck()).cleanup(req, lom)
}

func (mgr *Manager) RestoreObject(lom *cluster.LOM) error {
	if !lom.Bprops().EC.Enabled {
		return ErrorECDisabled
	}

	if cs := fs.Cap(); cs.Err != nil {
		return cs.Err
	}
	targetCnt := mgr.targetCnt.Load()
	// NOTE: Restore replica object is done with GFN, safe to always abort.
	if required := lom.Bprops().EC.RequiredRestoreTargets(); int(targetCnt) < required {
		return fmt.Errorf("%v: %d targets required to EC-restore %s (have %d)",
			cmn.ErrNotEnoughTargets, required, lom, targetCnt)
	}

	debug.Assert(lom.Mountpath() != nil && lom.Mountpath().Path != "")
	req := allocateReq(ActRestore, lom.LIF())
	errCh := make(chan error) // unbuffered
	req.ErrCh = errCh
	mgr.RestoreBckGetXact(lom.Bck()).decode(req, lom)

	// wait for EC completes restoring the object
	return <-errCh
}

// disableBck starts to reject new EC requests, rejects pending ones
func (mgr *Manager) disableBck(bck *meta.Bck) {
	mgr.RestoreBckGetXact(bck).ClearRequests()
	mgr.RestoreBckPutXact(bck).ClearRequests()
}

// enableBck aborts xctn disable and starts to accept new EC requests
// enableBck uses the same channel as disableBck, so order of executing them is the same as
// order which they arrived to a target in
func (mgr *Manager) enableBck(bck *meta.Bck) {
	mgr.RestoreBckGetXact(bck).EnableRequests()
	mgr.RestoreBckPutXact(bck).EnableRequests()
}

func (mgr *Manager) BucketsMDChanged() error {
	mgr.mu.Lock()
	newBckMD := mgr.t.Bowner().Get()
	oldBckMD := mgr.bmd
	if newBckMD.Version <= mgr.bmd.Version {
		mgr.mu.Unlock()
		return nil
	}
	mgr.bmd = newBckMD
	mgr.mu.Unlock()

	if newBckMD.IsECUsed() && !oldBckMD.IsECUsed() {
		if err := mgr.initECBundles(); err != nil {
			return err
		}
	} else if !newBckMD.IsECUsed() && oldBckMD.IsECUsed() {
		mgr.closeECBundles()
	}
	provider := apc.AIS
	newBckMD.Range(&provider, nil, func(nbck *meta.Bck) bool {
		oprops, ok := oldBckMD.Get(nbck)
		if !ok {
			if nbck.Props.EC.Enabled {
				mgr.enableBck(nbck)
			}
			return false
		}
		if !oprops.EC.Enabled && nbck.Props.EC.Enabled {
			mgr.enableBck(nbck)
		} else if oprops.EC.Enabled && !nbck.Props.EC.Enabled {
			mgr.disableBck(nbck)
		}

		return false
	})
	return nil
}

func (mgr *Manager) ListenSmapChanged() {
	smap := mgr.t.Sowner().Get()
	if smap.Version <= mgr.smap.Version {
		return
	}

	mgr.smap = mgr.t.Sowner().Get()
	targetCnt := mgr.smap.CountActiveTs()
	mgr.targetCnt.Store(int32(targetCnt))

	mgr.mu.Lock()

	// Manager is initialized before being registered for smap changes
	// bckMD will be present at this point
	// stopping relevant EC xactions which can't be satisfied with current number of targets
	// respond xaction is never stopped as it should respond regardless of the other targets
	provider := apc.AIS
	mgr.bmd.Range(&provider, nil, func(bck *meta.Bck) bool {
		bckName, bckProps := bck.Name, bck.Props
		bckXacts := mgr.getBckXactsUnlocked(bckName)
		if !bckProps.EC.Enabled {
			return false
		}
		if required := bckProps.EC.RequiredEncodeTargets(); targetCnt < required {
			nlog.Warningf("not enough targets for EC encoding for bucket %s; actual: %v, expected: %v",
				bckName, targetCnt, required)
			bckXacts.AbortPut()
		}
		// NOTE: this doesn't guarantee that present targets are sufficient to restore an object
		// if one target was killed, and a new one joined, this condition will be satisfied even though
		// slices of the object are not present on the new target
		if required := bckProps.EC.RequiredRestoreTargets(); targetCnt < required {
			nlog.Warningf("not enough targets for EC restoring for bucket %s; actual: %v, expected: %v",
				bckName, targetCnt, required)
			bckXacts.AbortGet()
		}
		return false
	})

	mgr.mu.Unlock()
}

// implementing cluster.Slistener interface
func (*Manager) String() string { return "ecmanager" }
