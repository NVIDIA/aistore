// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"sync"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xaction/registry"
)

// nolint:maligned // no performance critical code
type Manager struct {
	sync.RWMutex

	t         cluster.Target
	smap      *cluster.Smap
	targetCnt atomic.Int32 // atomic, to avoid races between read/write on smap
	bmd       *cluster.BMD // bmd owner

	xacts map[string]*BckXacts // bckName -> xact map, only ais buckets allowed, no naming collisions

	bundleEnabled atomic.Bool // to disable and enable on the fly
	netReq        string      // network used to send object request
	netResp       string      // network used to send/receive slices
	reqBundle     *bundle.Streams
	respBundle    *bundle.Streams
}

var ECM *Manager

func initManager(t cluster.Target) error {
	config := cmn.GCO.Get()
	netReq, netResp := cmn.NetworkIntraControl, cmn.NetworkIntraData
	if !config.Net.UseIntraControl {
		netReq = cmn.NetworkPublic
	}
	if !config.Net.UseIntraData {
		netResp = cmn.NetworkPublic
	}

	sowner := t.Sowner()
	smap := sowner.Get()

	ECM = &Manager{
		netReq:    netReq,
		netResp:   netResp,
		t:         t,
		smap:      smap,
		targetCnt: *atomic.NewInt32(int32(smap.CountTargets())),
		bmd:       t.Bowner().Get(),
		xacts:     make(map[string]*BckXacts),
	}

	if ECM.bmd.IsECUsed() {
		ECM.initECBundles()
	}

	var err error
	if _, err = transport.Register(ECM.netReq, ReqStreamName, ECM.recvRequest); err != nil {
		return fmt.Errorf("failed to register recvRequest: %v", err)
	}
	if _, err = transport.Register(ECM.netResp, RespStreamName, ECM.recvResponse); err != nil {
		return fmt.Errorf("failed to register respResponse: %v", err)
	}
	return nil
}

func (mgr *Manager) initECBundles() {
	if !mgr.bundleEnabled.CAS(false, true) {
		return
	}
	cmn.AssertMsg(mgr.reqBundle == nil && mgr.respBundle == nil, "EC Bundles have been already initialized")

	cbReq := func(hdr transport.ObjHdr, reader io.ReadCloser, _ unsafe.Pointer, err error) {
		if err != nil {
			glog.Errorf("failed to request %s/%s: %v", hdr.Bck, hdr.ObjName, err)
		}
	}

	client := transport.NewIntraDataClient()
	compression := cmn.GCO.Get().EC.Compression
	extraReq := transport.Extra{
		Callback:    cbReq,
		Compression: compression,
	}

	reqSbArgs := bundle.Args{
		Multiplier: bundle.Multiplier,
		Extra:      &extraReq,
		Network:    mgr.netReq,
		Trname:     ReqStreamName,
	}

	respSbArgs := bundle.Args{
		Multiplier: bundle.Multiplier,
		Trname:     RespStreamName,
		Network:    mgr.netResp,
		Extra:      &transport.Extra{Compression: compression},
	}

	sowner := mgr.t.Sowner()
	mgr.reqBundle = bundle.NewStreams(sowner, mgr.t.Snode(), client, reqSbArgs)
	mgr.respBundle = bundle.NewStreams(sowner, mgr.t.Snode(), client, respSbArgs)

	mgr.smap = sowner.Get()
	mgr.targetCnt.Store(int32(mgr.smap.CountTargets()))
	sowner.Listeners().Reg(mgr)
}

func (mgr *Manager) closeECBundles() {
	// XactCount is the number of currently active xaction. It increases
	// on every xaction(ECPut,ECGet,ECRespond ones) creation, and decreases
	// when an xaction is stopping(on abort or after some idle time(by default
	// 3*timeout.Sendfile = 15 minutes).
	if XactCount.Load() > 0 {
		return
	}
	if !mgr.bundleEnabled.CAS(true, false) {
		return
	}
	mgr.t.Sowner().Listeners().Unreg(mgr)
	mgr.reqBundle.Close(false)
	mgr.reqBundle = nil
	mgr.respBundle.Close(false)
	mgr.respBundle = nil
}

func (mgr *Manager) NewGetXact(bck cmn.Bck) *XactGet {
	return NewGetXact(mgr.t, bck, mgr.reqBundle, mgr.respBundle)
}

func (mgr *Manager) NewPutXact(bck cmn.Bck) *XactPut {
	return NewPutXact(mgr.t, bck, mgr.reqBundle, mgr.respBundle)
}

func (mgr *Manager) NewRespondXact(bck cmn.Bck) *XactRespond {
	return NewRespondXact(mgr.t, bck, mgr.reqBundle, mgr.respBundle)
}

func (mgr *Manager) RestoreBckGetXact(bck *cluster.Bck) *XactGet {
	xact := mgr.getBckXacts(bck.Name).Get()
	if xact == nil || xact.Finished() {
		xact = registry.Registry.RenewBucketXact(cmn.ActECGet, bck).(*XactGet)
		mgr.getBckXacts(bck.Name).SetGet(xact)
	}
	return xact
}

func (mgr *Manager) RestoreBckPutXact(bck *cluster.Bck) *XactPut {
	xact := mgr.getBckXacts(bck.Name).Put()
	if xact == nil || xact.Finished() {
		xact = registry.Registry.RenewBucketXact(cmn.ActECPut, bck).(*XactPut)
		mgr.getBckXacts(bck.Name).SetPut(xact)
	}
	return xact
}

func (mgr *Manager) RestoreBckRespXact(bck *cluster.Bck) *XactRespond {
	xact := mgr.getBckXacts(bck.Name).Req()
	if xact == nil || xact.Finished() {
		xact = registry.Registry.RenewBucketXact(cmn.ActECRespond, bck).(*XactRespond)
		mgr.getBckXacts(bck.Name).SetReq(xact)
	}
	return xact
}

func (mgr *Manager) getBckXacts(bckName string) *BckXacts {
	mgr.Lock()
	defer mgr.Unlock()
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
func (mgr *Manager) recvRequest(w http.ResponseWriter, hdr transport.ObjHdr, object io.Reader, err error) {
	if err != nil {
		glog.Errorf("request failed: %v", err)
		return
	}
	// check if the header contains a valid request
	if len(hdr.Opaque) == 0 {
		glog.Error("empty request")
		return
	}

	unpacker := cmn.NewUnpacker(hdr.Opaque)
	iReq := intraReq{}
	if err := unpacker.ReadAny(&iReq); err != nil {
		glog.Errorf("failed to unmarshal request: %v", err)
		return
	}

	// command requests should not have a body, but if it has,
	// the body must be drained to avoid errors
	if hdr.ObjAttrs.Size != 0 {
		if _, err := ioutil.ReadAll(object); err != nil {
			glog.Errorf("failed to read request body: %v", err)
			return
		}
	}
	bck := cluster.NewBckEmbed(hdr.Bck)
	if err = bck.Init(mgr.t.Bowner(), mgr.t.Snode()); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); !ok { // is ais
			glog.Errorf("failed to init bucket %s: %v", bck, err)
			return
		}
	}
	mgr.RestoreBckRespXact(bck).DispatchReq(iReq, bck, hdr.ObjName)
}

// A function to process big chunks of data (replica/slice/meta) sent from other targets
func (mgr *Manager) recvResponse(w http.ResponseWriter, hdr transport.ObjHdr, object io.Reader, err error) {
	if err != nil {
		glog.Errorf("receive failed: %v", err)
		return
	}
	// check if the request is valid
	if len(hdr.Opaque) == 0 {
		glog.Error("empty request")
		cmn.DrainReader(object)
		return
	}

	unpacker := cmn.NewUnpacker(hdr.Opaque)
	iReq := intraReq{}
	if err := unpacker.ReadAny(&iReq); err != nil {
		glog.Errorf("Failed to unmarshal request: %v", err)
		cmn.DrainReader(object)
		return
	}
	bck := cluster.NewBckEmbed(hdr.Bck)
	if err = bck.Init(mgr.t.Bowner(), mgr.t.Snode()); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); !ok { // is ais
			glog.Error(err)
			cmn.DrainReader(object)
			return
		}
	}
	switch iReq.act {
	case reqPut:
		mgr.RestoreBckRespXact(bck).DispatchResp(iReq, hdr, object)
	case respPut:
		// Process the request even if the number of targets is insufficient
		// (might've started when we had enough)
		mgr.RestoreBckGetXact(bck).DispatchResp(iReq, bck, hdr.ObjName, hdr.ObjAttrs, object)
	default:
		glog.Errorf("unknown EC response action %d", iReq.act)
		cmn.DrainReader(object)
	}
}

// Encode the object:
//   - lom - object to encode
//   - intra - if true, it is internal request and has low priority
//   - cb - optional callback that is called after the object is encoded
func (mgr *Manager) EncodeObject(lom *cluster.LOM, cb ...cluster.OnFinishObj) error {
	if !lom.Bprops().EC.Enabled {
		return ErrorECDisabled
	}

	if cs := fs.GetCapStatus(); cs.Err != nil {
		return cs.Err
	}

	isECCopy := IsECCopy(lom.Size(), &lom.Bprops().EC)
	targetCnt := mgr.targetCnt.Load()

	// tradeoff: encoding small object might require just 1 additional target available
	// we will start xaction to satisfy this request
	if required := lom.Bprops().EC.RequiredEncodeTargets(); !isECCopy && int(targetCnt) < required {
		glog.Warningf("not enough targets to encode the object; actual: %v, required: %v", targetCnt, required)
		return ErrorInsufficientTargets
	}

	cmn.Assert(lom.FQN != "")
	cmn.Assert(lom.ParsedFQN.MpathInfo != nil && lom.ParsedFQN.MpathInfo.Path != "")
	spec, _ := fs.CSM.FileSpec(lom.FQN)
	if spec != nil && !spec.PermToProcess() {
		return nil
	}

	req := &Request{
		Action:  ActSplit,
		IsCopy:  IsECCopy(lom.Size(), &lom.Bprops().EC),
		LOM:     lom,
		rebuild: len(cb) != 0,
	}
	if len(cb) != 0 {
		req.Callback = cb[0]
	}

	mgr.RestoreBckPutXact(lom.Bck()).Encode(req)

	return nil
}

func (mgr *Manager) CleanupObject(lom *cluster.LOM) {
	if !lom.Bprops().EC.Enabled {
		return
	}
	cmn.Assert(lom.FQN != "")
	cmn.Assert(lom.ParsedFQN.MpathInfo != nil && lom.ParsedFQN.MpathInfo.Path != "")
	req := &Request{
		Action: ActDelete,
		LOM:    lom,
	}

	mgr.RestoreBckPutXact(lom.Bck()).Cleanup(req)
}

func (mgr *Manager) RestoreObject(lom *cluster.LOM) error {
	if !lom.Bprops().EC.Enabled {
		return ErrorECDisabled
	}

	if cs := fs.GetCapStatus(); cs.Err != nil {
		return cs.Err
	}
	targetCnt := mgr.targetCnt.Load()
	// note: restore replica object is done with GFN, safe to always abort
	if required := lom.Bprops().EC.RequiredRestoreTargets(); int(targetCnt) < required {
		glog.Warningf("not enough targets to restore the object; actual: %v, required: %v", targetCnt, required)
		return ErrorInsufficientTargets
	}

	cmn.Assert(lom.ParsedFQN.MpathInfo != nil && lom.ParsedFQN.MpathInfo.Path != "")
	req := &Request{
		Action: ActRestore,
		LOM:    lom,
		ErrCh:  make(chan error), // unbuffered
	}

	mgr.RestoreBckGetXact(lom.Bck()).Decode(req)

	// wait for EC completes restoring the object
	return <-req.ErrCh
}

// disableBck starts to reject new EC requests, rejects pending ones
func (mgr *Manager) disableBck(bck *cluster.Bck) {
	mgr.RestoreBckGetXact(bck).ClearRequests()
	mgr.RestoreBckPutXact(bck).ClearRequests()
}

// enableBck aborts xact disable and starts to accept new EC requests
// enableBck uses the same channel as disableBck, so order of executing them is the same as
// order which they arrived to a target in
func (mgr *Manager) enableBck(bck *cluster.Bck) {
	mgr.RestoreBckGetXact(bck).EnableRequests()
	mgr.RestoreBckPutXact(bck).EnableRequests()
}

func (mgr *Manager) BucketsMDChanged() {
	mgr.Lock()
	newBckMD := mgr.t.Bowner().Get()
	oldBckMD := mgr.bmd
	if newBckMD.Version <= mgr.bmd.Version {
		mgr.Unlock()
		return
	}
	mgr.bmd = newBckMD
	mgr.Unlock()

	if newBckMD.IsECUsed() && !oldBckMD.IsECUsed() {
		// init EC streams if they were not initialized at startup;
		// no need to close when the last EC bucket is disabled -
		// streams timeout (when idle) and auto-close.
		mgr.initECBundles()
	} else if !newBckMD.IsECUsed() && oldBckMD.IsECUsed() {
		mgr.closeECBundles()
	}
	provider := cmn.ProviderAIS
	newBckMD.Range(&provider, nil, func(nbck *cluster.Bck) bool {
		oldBckMD.Range(&provider, nil, func(obck *cluster.Bck) bool {
			if !obck.Props.EC.Enabled && nbck.Props.EC.Enabled {
				mgr.enableBck(nbck)
			} else if obck.Props.EC.Enabled && !nbck.Props.EC.Enabled {
				mgr.disableBck(nbck)
			}
			return false
		})
		return false
	})
}

func (mgr *Manager) ListenSmapChanged() {
	smap := mgr.t.Sowner().Get()
	if smap.Version <= mgr.smap.Version {
		return
	}

	mgr.smap = mgr.t.Sowner().Get()
	targetCnt := mgr.smap.CountTargets()
	mgr.targetCnt.Store(int32(targetCnt))

	mgr.Lock()

	// Manager is initialized before being registered for smap changes
	// bckMD will be present at this point
	// stopping relevant EC xactions which can't be satisfied with current number of targets
	// respond xaction is never stopped as it should respond regardless of the other targets
	provider := cmn.ProviderAIS
	mgr.bmd.Range(&provider, nil, func(bck *cluster.Bck) bool {
		bckName, bckProps := bck.Name, bck.Props
		bckXacts := mgr.getBckXactsUnlocked(bckName)
		if !bckProps.EC.Enabled {
			return false
		}
		if required := bckProps.EC.RequiredEncodeTargets(); targetCnt < required {
			glog.Warningf("not enough targets for EC encoding for bucket %s; actual: %v, expected: %v",
				bckName, targetCnt, required)
			bckXacts.AbortPut()
		}
		// NOTE: this doesn't guarantee that present targets are sufficient to restore an object
		// if one target was killed, and a new one joined, this condition will be satisfied even though
		// slices of the object are not present on the new target
		if required := bckProps.EC.RequiredRestoreTargets(); targetCnt < required {
			glog.Warningf("not enough targets for EC restoring for bucket %s; actual: %v, expected: %v",
				bckName, targetCnt, required)
			bckXacts.AbortGet()
		}
		return false
	})

	mgr.Unlock()
}

// implementing cluster.Slistener interface
func (mgr *Manager) String() string {
	return "ecmanager"
}
