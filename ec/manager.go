// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"errors"
	"fmt"
	"io"
	ratomic "sync/atomic"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type Manager struct {
	bmd *meta.BMD

	netReq  string // network used to send object request
	netResp string // network used to send/receive slices

	// streams
	reqBundle  ratomic.Pointer[bundle.Streams]
	respBundle ratomic.Pointer[bundle.Streams]

	// ref count
	_refc atomic.Int32

	bundleEnabled atomic.Bool // to disable and enable on the fly
}

var (
	ECM        *Manager
	errSkipped = errors.New("skipped") // CT is skipped due to EC unsupported for the content type
)

func initManager() error {
	ECM = &Manager{
		netReq:  cmn.NetIntraControl,
		netResp: cmn.NetIntraData,
		bmd:     core.T.Bowner().Get(),
	}
	// EC `trnames` (ReqStreamName, RespStreamName) are constants, receive handlers static
	if err := transport.Handle(ReqStreamName, ECM.recvRequest); err != nil {
		return fmt.Errorf("failed to register recvRequest: %v", err)
	}
	if err := transport.Handle(RespStreamName, ECM.recvResponse); err != nil {
		return fmt.Errorf("failed to register respResponse: %v", err)
	}
	return nil
}

func (mgr *Manager) req() *bundle.Streams  { return mgr.reqBundle.Load() }
func (mgr *Manager) resp() *bundle.Streams { return mgr.respBundle.Load() }

func (mgr *Manager) IsActive() bool { return mgr._refc.Load() != 0 }

func (mgr *Manager) incActive(xctn core.Xact) {
	mgr._refc.Inc()
	mgr.OpenStreams(false)
	notif := &xact.NotifXact{
		Base: nl.Base{When: core.UponTerm, F: mgr.notifyTerm},
		Xact: xctn,
	}
	xctn.AddNotif(notif)
}

func (mgr *Manager) notifyTerm(core.Notif, error, bool) {
	rc := mgr._refc.Dec()
	debug.Assert(rc >= 0, "rc: ", rc)
}

func cbReq(hdr *transport.ObjHdr, _ io.ReadCloser, _ any, err error) {
	if err != nil {
		nlog.Errorln("failed to request", hdr.Cname(), "err: [", err, "]")
	}
}

func (mgr *Manager) OpenStreams(withRefc bool) {
	if withRefc {
		mgr._refc.Inc()
	}
	if !mgr.bundleEnabled.CAS(false, true) {
		return
	}
	nlog.InfoDepth(1, core.T.String(), "ECM", apc.ActEcOpen)
	var (
		client      = transport.NewIntraDataClient()
		config      = cmn.GCO.Get()
		compression = config.EC.Compression
		extraReq    = transport.Extra{Callback: cbReq, Compression: compression, Config: config}
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
		Extra:      &transport.Extra{Compression: compression, Config: config},
	}

	mgr.reqBundle.Store(bundle.New(client, reqSbArgs))
	mgr.respBundle.Store(bundle.New(client, respSbArgs))
}

func (mgr *Manager) CloseStreams(justRefc bool) {
	if justRefc {
		mgr._refc.Dec()
		return
	}
	if !mgr.bundleEnabled.CAS(true, false) {
		return
	}
	nlog.InfoDepth(1, core.T.String(), "ECM", apc.ActEcClose)
	mgr.req().Close(false)
	mgr.resp().Close(false)
}

func (mgr *Manager) NewGetXact(bck *cmn.Bck) *XactGet         { return newGetXact(bck, mgr) }
func (mgr *Manager) NewPutXact(bck *cmn.Bck) *XactPut         { return newPutXact(bck, mgr) }
func (mgr *Manager) NewRespondXact(bck *cmn.Bck) *XactRespond { return newRespondXact(bck, mgr) }

func (*Manager) RestoreBckGetXact(bck *meta.Bck) *XactGet {
	xctn, err := _renewXact(bck, apc.ActECGet)
	debug.AssertNoErr(err) // TODO: handle, here and elsewhere
	return xctn.(*XactGet)
}

func (*Manager) RestoreBckPutXact(bck *meta.Bck) *XactPut {
	xctn, err := _renewXact(bck, apc.ActECPut)
	debug.AssertNoErr(err)
	return xctn.(*XactPut)
}

func (*Manager) RestoreBckRespXact(bck *meta.Bck) *XactRespond {
	xctn, err := _renewXact(bck, apc.ActECRespond)
	debug.AssertNoErr(err)
	return xctn.(*XactRespond)
}

func _renewXact(bck *meta.Bck, kind string) (core.Xact, error) {
	rns := xreg.RenewBucketXact(kind, bck, xreg.Args{})
	if rns.Err != nil {
		return nil, rns.Err
	}
	return rns.Entry.Get(), nil
}

// A function to process command requests from other targets
func (mgr *Manager) recvRequest(hdr *transport.ObjHdr, objReader io.Reader, err error) error {
	defer transport.FreeRecv(objReader)
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
		n, err := io.Copy(io.Discard, objReader)
		if err != nil && !cos.IsEOF(err) {
			nlog.Errorf("failed to read request body: %v", err)
			return err
		}
		debug.Assert(n == 0, "command requests should not have a body: ", n)
	}
	bck := meta.CloneBck(&hdr.Bck)
	if err = bck.Init(core.T.Bowner()); err != nil {
		if _, ok := err.(*cmn.ErrRemoteBckNotFound); !ok { // is ais
			nlog.Errorf("failed to init bucket %s: %v", bck, err)
			return err
		}
	}
	xctn := mgr.RestoreBckRespXact(bck)
	xctn.dispatchReq(iReq, hdr, bck)
	return nil
}

// A function to process big chunks of data (replica/slice/meta) sent from other targets
func (mgr *Manager) recvResponse(hdr *transport.ObjHdr, objReader io.Reader, err error) error {
	defer transport.DrainAndFreeReader(objReader)
	if err != nil {
		nlog.Errorln("failed to receive response:", err)
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
		nlog.Errorln("failed to unpack request:", err)
		return err
	}
	bck := meta.CloneBck(&hdr.Bck)
	if err := bck.Init(core.T.Bowner()); err != nil {
		if !cmn.IsErrRemoteBckNotFound(err) { // is ais://
			nlog.Errorln(err)
			return err
		}
	}
	switch hdr.Opcode {
	case reqPut:
		xctn := mgr.RestoreBckRespXact(bck)
		xctn.IncPending()
		xctn.dispatchResp(iReq, hdr, objReader)
		xctn.DecPending()
	case respPut:
		// Process the request even if the number of targets is insufficient
		// (might've started when we had enough)
		xctn := mgr.RestoreBckGetXact(bck)
		xctn.dispatchResp(iReq, hdr, bck, objReader)
	default:
		debug.Assertf(false, "unknown EC response action %d", hdr.Opcode)
	}
	return nil
}

// EncodeObject generates slices using Reed-Solom algorithm:
//   - lom - object to encode
//   - intra - if true, it is internal request and has low priority
//   - cb - optional callback that is called after the object is encoded
func (mgr *Manager) EncodeObject(lom *core.LOM, cb core.OnFinishObj) error {
	if !lom.ECEnabled() {
		return ErrorECDisabled
	}
	cs := fs.Cap()
	if err := cs.Err(); err != nil {
		return err
	}
	spec, _ := fs.CSM.FileSpec(lom.FQN)
	if spec != nil && !spec.PermToProcess() {
		return errSkipped
	}

	req := allocateReq(ActSplit, lom.LIF())
	req.IsCopy = IsECCopy(lom.Lsize(), &lom.Bprops().EC)
	if cb != nil {
		req.rebuild = true
		req.Callback = cb
	}

	mgr.RestoreBckPutXact(lom.Bck()).encode(req, lom)

	return nil
}

func (mgr *Manager) CleanupObject(lom *core.LOM) {
	if !lom.ECEnabled() {
		return
	}
	debug.Assert(lom.FQN != "" && lom.Mountpath().Path != "")
	req := allocateReq(ActDelete, lom.LIF())
	mgr.RestoreBckPutXact(lom.Bck()).cleanup(req, lom)
}

func (mgr *Manager) RestoreObject(lom *core.LOM, cb core.OnFinishObj) error {
	if !lom.ECEnabled() {
		return ErrorECDisabled
	}
	cs := fs.Cap()
	if err := cs.Err(); err != nil {
		return err
	}

	debug.Assert(lom.Mountpath() != nil && lom.Mountpath().Path != "")
	req := allocateReq(ActRestore, lom.LIF())
	errCh := make(chan error) // unbuffered
	req.ErrCh = errCh
	req.Callback = cb
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

func (mgr *Manager) BMDChanged() error {
	newBMD := core.T.Bowner().Get()
	oldBMD := mgr.bmd
	if newBMD.Version <= mgr.bmd.Version {
		return nil
	}
	mgr.bmd = newBMD

	// by bucket
	newBMD.Range(nil, nil, func(nbck *meta.Bck) bool {
		oprops, ok := oldBMD.Get(nbck)
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

// TODO -- FIXME: joggers, etc.
func (mgr *Manager) TryRecoverObj(lom *core.LOM, cb core.OnFinishObj) {
	go func() {
		err := mgr.RestoreObject(lom, cb)
		if err != nil {
			nlog.Errorln(core.T.String(), "failed to recover", lom.Cname(), "err:", err)
		}
		cb(lom, err)
		core.FreeLOM(lom)
	}()
}
