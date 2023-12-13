// Package ec provides erasure coding (EC) based data protection for AIStore.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ec

import (
	"errors"
	"fmt"
	"io"
	ratomic "sync/atomic"

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
	bmd *meta.BMD

	netReq  string // network used to send object request
	netResp string // network used to send/receive slices

	// streams
	reqBundle  ratomic.Pointer[bundle.Streams]
	respBundle ratomic.Pointer[bundle.Streams]

	bundleEnabled atomic.Bool // to disable and enable on the fly
}

var (
	ECM        *Manager
	errSkipped = errors.New("skipped") // CT is skipped due to EC unsupported for the content type
)

func initManager() (err error) {
	ECM = &Manager{
		netReq:  cmn.NetIntraControl,
		netResp: cmn.NetIntraData,
		bmd:     g.t.Bowner().Get(),
	}
	if ECM.bmd.IsECUsed() {
		err = ECM.initECBundles()
	}
	return err
}

func (mgr *Manager) req() *bundle.Streams  { return mgr.reqBundle.Load() }
func (mgr *Manager) resp() *bundle.Streams { return mgr.respBundle.Load() }

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
	cbReq := func(hdr *transport.ObjHdr, reader io.ReadCloser, _ any, err error) {
		if err != nil {
			nlog.Errorf("failed to request %s: %v", hdr.Cname(), err)
		}
	}
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

	sowner := g.t.Sowner()
	mgr.reqBundle.Store(bundle.New(sowner, g.t.Snode(), client, reqSbArgs))
	mgr.respBundle.Store(bundle.New(sowner, g.t.Snode(), client, respSbArgs))

	return nil
}

func (mgr *Manager) closeECBundles() {
	if !mgr.bundleEnabled.CAS(true, false) {
		return
	}
	mgr.req().Close(false)
	mgr.resp().Close(false)
	transport.Unhandle(ReqStreamName)
	transport.Unhandle(RespStreamName)
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

func _renewXact(bck *meta.Bck, kind string) (cluster.Xact, error) {
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
		if _, err := io.ReadAll(objReader); err != nil {
			nlog.Errorf("failed to read request body: %v", err)
			return err
		}
	}
	bck := meta.CloneBck(&hdr.Bck)
	if err = bck.Init(g.t.Bowner()); err != nil {
		if _, ok := err.(*cmn.ErrRemoteBckNotFound); !ok { // is ais
			nlog.Errorf("failed to init bucket %s: %v", bck, err)
			return err
		}
	}
	mgr.RestoreBckRespXact(bck).DispatchReq(iReq, hdr, bck)
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
	if err = bck.Init(g.t.Bowner()); err != nil {
		if _, ok := err.(*cmn.ErrRemoteBckNotFound); !ok { // is ais
			nlog.Errorln(err)
			return err
		}
	}
	switch hdr.Opcode {
	case reqPut:
		mgr.RestoreBckRespXact(bck).DispatchResp(iReq, hdr, objReader)
	case respPut:
		// Process the request even if the number of targets is insufficient
		// (might've started when we had enough)
		mgr.RestoreBckGetXact(bck).DispatchResp(iReq, hdr, bck, objReader)
	default:
		debug.Assertf(false, "unknown EC response action %d", hdr.Opcode)
	}
	return nil
}

// EncodeObject generates slices using Reed-Solom algorithm:
//   - lom - object to encode
//   - intra - if true, it is internal request and has low priority
//   - cb - optional callback that is called after the object is encoded
func (mgr *Manager) EncodeObject(lom *cluster.LOM, cb cluster.OnFinishObj) error {
	if !lom.Bprops().EC.Enabled {
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
	req.IsCopy = IsECCopy(lom.SizeBytes(), &lom.Bprops().EC)
	if cb != nil {
		req.rebuild = true
		req.Callback = cb
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
	cs := fs.Cap()
	if err := cs.Err(); err != nil {
		return err
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

func (mgr *Manager) BMDChanged() error {
	newBMD := g.t.Bowner().Get()
	oldBMD := mgr.bmd
	if newBMD.Version <= mgr.bmd.Version {
		return nil
	}
	mgr.bmd = newBMD

	// globally
	if newBMD.IsECUsed() && !oldBMD.IsECUsed() {
		if err := mgr.initECBundles(); err != nil {
			return err
		}
	} else if !newBMD.IsECUsed() && oldBMD.IsECUsed() {
		mgr.closeECBundles()
		return nil
	}

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
