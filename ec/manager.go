// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
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
)

//nolint:maligned
type Manager struct {
	sync.RWMutex

	t         cluster.Target
	reg       XactRegistry
	smap      *cluster.Smap
	targetCnt atomic.Int32 // atomic, to avoid races between read/write on smap
	bmd       *cluster.BMD // bmd owner

	xacts map[string]*BckXacts // bckName -> xact map, only ais buckets allowed, no naming collisions

	bundleEnabled atomic.Bool // to disable and enable on the fly
	netReq        string      // network used to send object request
	netResp       string      // network used to send/receive slices
	reqBundle     *transport.StreamBundle
	respBundle    *transport.StreamBundle
}

var (
	ECM *Manager
)

func initManager(t cluster.Target, reg XactRegistry) error {
	config := cmn.GCO.Get()
	netReq, netResp := cmn.NetworkIntraControl, cmn.NetworkIntraData
	if !config.Net.UseIntraControl {
		netReq = cmn.NetworkPublic
	}
	if !config.Net.UseIntraData {
		netResp = cmn.NetworkPublic
	}

	sowner := t.GetSowner()
	smap := sowner.Get()

	ECM = &Manager{
		netReq:    netReq,
		netResp:   netResp,
		t:         t,
		reg:       reg,
		smap:      smap,
		targetCnt: *atomic.NewInt32(int32(smap.CountTargets())),
		bmd:       t.GetBowner().Get(),
		xacts:     make(map[string]*BckXacts),
	}

	sowner.Listeners().Reg(ECM)

	if ECM.bmd.IsECUsed() {
		ECM.initECBundles()
	}

	var err error
	if _, err = transport.Register(ECM.netReq, ReqStreamName, ECM.recvRequest); err != nil {
		return fmt.Errorf("Failed to register recvRequest: %v", err)
	}
	if _, err = transport.Register(ECM.netResp, RespStreamName, ECM.recvResponse); err != nil {
		return fmt.Errorf("Failed to register respResponse: %v", err)
	}
	return nil
}

func (mgr *Manager) initECBundles() {
	if !mgr.bundleEnabled.CAS(false, true) {
		return
	}
	cmn.AssertMsg(mgr.reqBundle == nil && mgr.respBundle == nil, "EC Bundles have been already initialized")

	cbReq := func(hdr transport.Header, reader io.ReadCloser, _ unsafe.Pointer, err error) {
		if err != nil {
			glog.Errorf("Failed to request %s/%s: %v", hdr.Bucket, hdr.Objname, err)
		}
	}

	client := transport.NewIntraDataClient()
	compression := cmn.GCO.Get().EC.Compression
	extraReq := transport.Extra{
		Callback:    cbReq,
		Compression: compression,
	}

	reqSbArgs := transport.SBArgs{
		Multiplier: transport.IntraBundleMultiplier,
		Extra:      &extraReq,
		Network:    mgr.netReq,
		Trname:     ReqStreamName,
	}

	respSbArgs := transport.SBArgs{
		Multiplier: transport.IntraBundleMultiplier,
		Trname:     RespStreamName,
		Network:    mgr.netResp,
		Extra:      &transport.Extra{Compression: compression},
	}

	sowner := mgr.t.GetSowner()
	mgr.reqBundle = transport.NewStreamBundle(sowner, mgr.t.Snode(), client, reqSbArgs)
	mgr.respBundle = transport.NewStreamBundle(sowner, mgr.t.Snode(), client, respSbArgs)
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
	mgr.reqBundle.Close(false)
	mgr.reqBundle = nil
	mgr.respBundle.Close(false)
	mgr.respBundle = nil
}

func (mgr *Manager) NewGetXact(bucket string) *XactGet {
	return NewGetXact(mgr.t, mgr.t.GetSowner(), mgr.t.Snode(),
		bucket, mgr.reqBundle, mgr.respBundle)
}

func (mgr *Manager) NewPutXact(bucket string) *XactPut {
	return NewPutXact(mgr.t, mgr.t.GetSowner(), mgr.t.Snode(),
		bucket, mgr.reqBundle, mgr.respBundle)
}

func (mgr *Manager) NewRespondXact(bucket string) *XactRespond {
	return NewRespondXact(mgr.t, mgr.t.GetSowner(), mgr.t.Snode(),
		bucket, mgr.reqBundle, mgr.respBundle)
}

func (mgr *Manager) RestoreBckGetXact(bck *cluster.Bck) *XactGet {
	xact := mgr.getBckXacts(bck.Name).Get()
	if xact == nil || xact.Finished() {
		xact = mgr.reg.RenewGetEC(bck)
		mgr.getBckXacts(bck.Name).SetGet(xact)
	}

	return xact
}

func (mgr *Manager) RestoreBckPutXact(bck *cluster.Bck) *XactPut {
	xact := mgr.getBckXacts(bck.Name).Put()
	if xact == nil || xact.Finished() {
		xact = mgr.reg.RenewPutEC(bck)
		mgr.getBckXacts(bck.Name).SetPut(xact)
	}

	return xact
}

func (mgr *Manager) RestoreBckRespXact(bck *cluster.Bck) *XactRespond {
	xact := mgr.getBckXacts(bck.Name).Req()
	if xact == nil || xact.Finished() {
		xact = mgr.reg.RenewRespondEC(bck)
		mgr.getBckXacts(bck.Name).SetReq(xact)
	}

	return xact
}

func (mgr *Manager) getBckXacts(bckName string) *BckXacts {
	mgr.RLock()
	defer mgr.RUnlock()

	xacts, ok := mgr.xacts[bckName]

	if !ok {
		xacts = &BckXacts{}
		mgr.xacts[bckName] = xacts
	}

	return xacts
}

// A function to process command requests from other targets
func (mgr *Manager) recvRequest(w http.ResponseWriter, hdr transport.Header, object io.Reader, err error) {
	if err != nil {
		glog.Errorf("Request failed: %v", err)
		return
	}
	// check if the header contains a valid request
	if len(hdr.Opaque) == 0 {
		glog.Error("Empty request")
		return
	}

	iReq := IntraReq{}
	if err := iReq.Unmarshal(hdr.Opaque); err != nil {
		glog.Errorf("Failed to unmarshal request: %v", err)
		return
	}

	// command requests should not have a body, but if it has,
	// the body must be drained to avoid errors
	if hdr.ObjAttrs.Size != 0 {
		if _, err := ioutil.ReadAll(object); err != nil {
			glog.Errorf("Failed to read request body: %v", err)
			return
		}
	}
	bck := &cluster.Bck{Name: hdr.Bucket, Provider: cmn.ProviderFromBool(hdr.BckIsAIS)}
	if err = bck.Init(mgr.t.GetBowner()); err != nil {
		if _, ok := err.(*cmn.ErrorCloudBucketDoesNotExist); !ok { // is ais
			glog.Errorf("Failed to init bucket %s: %v", bck, err)
			return
		}
	}
	mgr.RestoreBckRespXact(bck).DispatchReq(iReq, bck, hdr.Objname)
}

// A function to process big chunks of data (replica/slice/meta) sent from other targets
func (mgr *Manager) recvResponse(w http.ResponseWriter, hdr transport.Header, object io.Reader, err error) {
	if err != nil {
		glog.Errorf("Receive failed: %v", err)
		return
	}
	// check if the request is valid
	if len(hdr.Opaque) == 0 {
		glog.Error("Empty request")
		cmn.DrainReader(object)
		return
	}

	iReq := IntraReq{}
	if err := iReq.Unmarshal(hdr.Opaque); err != nil {
		glog.Errorf("Failed to unmarshal request: %v", err)
		cmn.DrainReader(object)
		return
	}
	bck := &cluster.Bck{Name: hdr.Bucket, Provider: cmn.ProviderFromBool(hdr.BckIsAIS)}
	if err = bck.Init(mgr.t.GetBowner()); err != nil {
		if _, ok := err.(*cmn.ErrorCloudBucketDoesNotExist); !ok { // is ais
			glog.Errorf("Failed to init bucket %s: %v", bck, err)
			cmn.DrainReader(object)
			return
		}
	}
	switch iReq.Act {
	case ReqPut:
		mgr.RestoreBckRespXact(bck).DispatchResp(iReq, bck, hdr.Objname, hdr.ObjAttrs, object)
	case ReqMeta, RespPut:
		// Process this request even if there might not be enough targets. It might have been started when there was,
		// so there is a chance to complete restore successfully
		mgr.RestoreBckGetXact(bck).DispatchResp(iReq, bck, hdr.Objname, hdr.ObjAttrs, object)
	default:
		glog.Errorf("Unknown EC response action %d", iReq.Act)
		cmn.DrainReader(object)
	}
}

// Encode the object. `wg` is optional - a caller passes WaitGroup when it
// wants to be notified after the object is done
func (mgr *Manager) EncodeObject(lom *cluster.LOM, cb ...cluster.OnFinishObj) error {
	if !lom.Bprops().EC.Enabled {
		return ErrorECDisabled
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

	// TODO -- FIXME: all targets must check t.AvgCapUsed() for high watermark *prior* to starting
	if capInfo := lom.T.AvgCapUsed(nil); capInfo.OOS {
		return capInfo.Err
	}
	spec, _ := fs.CSM.FileSpec(lom.FQN)
	if spec != nil && !spec.PermToProcess() {
		return nil
	}

	req := &Request{
		Action: ActSplit,
		IsCopy: IsECCopy(lom.Size(), &lom.Bprops().EC),
		LOM:    lom,
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
	newBckMD := mgr.t.GetBowner().Get()
	oldBckMD := mgr.bmd
	if newBckMD.Version <= mgr.bmd.Version {
		return
	}

	mgr.Lock()
	mgr.bmd = newBckMD
	mgr.Unlock()

	if newBckMD.IsECUsed() && !oldBckMD.IsECUsed() {
		// init EC streams if there were not initialized on the start
		// no need to close them when last EC bucket is disabled
		// as they close itself on idle
		mgr.initECBundles()
	} else if !newBckMD.IsECUsed() {
		mgr.closeECBundles()
	}

	for bckName, newBck := range newBckMD.LBmap {
		bck := &cluster.Bck{Name: bckName, Provider: cmn.ProviderFromBool(true /* is ais */)}
		// Disable EC for buckets that existed and have changed EC.Enabled to false
		// Enable EC for buckets that existed and have change EC.Enabled to true
		if oldBck, existed := oldBckMD.LBmap[bckName]; existed {
			if !oldBck.EC.Enabled && newBck.EC.Enabled {
				mgr.enableBck(bck)
			} else if oldBck.EC.Enabled && !newBck.EC.Enabled {
				mgr.disableBck(bck)
			}
		}
	}
}

func (mgr *Manager) ListenSmapChanged(newSmapVersionChannel chan int64) {
	for {
		newSmapVersion, ok := <-newSmapVersionChannel

		if !ok {
			// channel closed by Unreg
			// We should end xactions and stop listening
			for _, bck := range mgr.xacts {
				bck.StopGet()
				bck.StopPut()
			}

			return
		}

		if newSmapVersion <= mgr.smap.Version {
			continue
		}

		mgr.smap = mgr.t.GetSowner().Get()
		targetCnt := mgr.smap.CountTargets()
		mgr.targetCnt.Store(int32(targetCnt))

		mgr.RLock()

		// Manager is initialized before being registered for smap changes
		// bckMD will be present at this point
		// stopping relevant EC xactions which can't be satisfied with current number of targets
		// respond xaction is never stopped as it should respond regardless of the other targets
		for bckName, bckProps := range mgr.bmd.LBmap {
			bckXacts := mgr.getBckXacts(bckName)
			if !bckProps.EC.Enabled {
				continue
			}
			if required := bckProps.EC.RequiredEncodeTargets(); targetCnt < required {
				glog.Warningf("Not enough targets for EC encoding for bucket %s; actual: %v, expected: %v",
					bckName, targetCnt, required)
				bckXacts.StopPut()
			}
			// NOTE: this doesn't guarantee that present targets are sufficient to restore an object
			// if one target was killed, and a new one joined, this condition will be satisfied even though
			// slices of the object are not present on the new target
			if required := bckProps.EC.RequiredRestoreTargets(); targetCnt < required {
				glog.Warningf("Not enough targets for EC restoring for bucket %s; actual: %v, expected: %v", bckName, targetCnt, required)
				bckXacts.StopGet()
			}
		}

		mgr.RUnlock()
	}
}

// implementing cluster.Slistener interface
func (mgr *Manager) String() string {
	return "ecmanager"
}
