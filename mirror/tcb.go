// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	tcbFactory struct {
		xreg.RenewBase
		xctn  *XactTCB
		kind  string
		phase string // (see "transition")
		args  *xreg.TCBArgs
	}
	XactTCB struct {
		xact.BckJog
		t    cluster.Target
		dm   *bundle.DataMover
		args xreg.TCBArgs
		// starting up
		wg sync.WaitGroup
		// finishing
		rxlast atomic.Int64
		refc   atomic.Int32
		err    cos.ErrValue
	}
)

const OpcTxnDone = 27182

const etlBucketParallelCnt = 2

// interface guard
var (
	_ cluster.Xact   = (*XactTCB)(nil)
	_ xreg.Renewable = (*tcbFactory)(nil)
)

////////////////
// tcbFactory //
////////////////

func (e *tcbFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	custom := args.Custom.(*xreg.TCBArgs)
	p := &tcbFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, kind: e.kind, phase: custom.Phase, args: custom}
	return p
}

func (e *tcbFactory) Start() error {
	var (
		config    = cmn.GCO.Get()
		slab, err = e.T.PageMM().GetSlab(memsys.MaxPageSlabSize) // TODO: estimate
	)
	debug.AssertNoErr(err)
	e.xctn = newXactTCB(e, slab, config)

	// refcount OpcTxnDone; this target must ve active (ref: ignoreMaintenance)
	smap := e.T.Sowner().Get()
	if err := e.xctn.InMaintOrDecomm(smap, e.T.Snode()); err != nil {
		return err
	}
	nat := smap.CountActiveTs()
	e.xctn.refc.Store(int32(nat - 1))
	e.xctn.wg.Add(1)

	var sizePDU int32
	if e.kind == apc.ActETLBck {
		sizePDU = memsys.DefaultBufSize
	}
	err = e.newDM(config, e.UUID(), sizePDU)
	return err
}

func (e *tcbFactory) newDM(config *cmn.Config, uuid string, sizePDU int32) error {
	const trname = "tcb"
	dmExtra := bundle.Extra{
		RecvAck:     nil, // no ACKs
		Compression: config.TCB.Compression,
		Multiplier:  config.TCB.SbundleMult,
		SizePDU:     sizePDU,
	}
	dm, err := bundle.NewDataMover(e.T, trname+"-"+uuid, e.xctn.recv, cmn.OwtPut, dmExtra)
	if err != nil {
		return err
	}
	if err := dm.RegRecv(); err != nil {
		return err
	}
	dm.SetXact(e.xctn)
	e.xctn.dm = dm
	return nil
}

func (e *tcbFactory) Kind() string      { return e.kind }
func (e *tcbFactory) Get() cluster.Xact { return e.xctn }

func (e *tcbFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	prev := prevEntry.(*tcbFactory)
	if e.UUID() != prev.UUID() {
		err = cmn.NewErrXactUsePrev(prevEntry.Get().String())
		return
	}
	bckEq := prev.args.BckFrom.Equal(e.args.BckFrom, true /*same BID*/, true /* same backend */)
	debug.Assert(bckEq)
	debug.Assert(prev.phase == apc.ActBegin && e.phase == apc.ActCommit)
	prev.args.Phase = apc.ActCommit // transition
	wpr = xreg.WprUse
	return
}

/////////////
// XactTCB //
/////////////

// limited pre-run abort
func (r *XactTCB) TxnAbort() {
	err := cmn.NewErrAborted(r.Name(), "tcb: txn-abort", nil)
	r.dm.CloseIf(err)
	r.dm.UnregRecv()
	r.Base.Finish(err)
}

// XactTCB copies one bucket _into_ another with or without transformation.
// args.DP.Reader() is the reader to receive transformed bytes; when nil we do a plain bucket copy.
func newXactTCB(e *tcbFactory, slab *memsys.Slab, config *cmn.Config) (r *XactTCB) {
	var parallel int
	r = &XactTCB{t: e.T, args: *e.args}
	if e.kind == apc.ActETLBck {
		parallel = etlBucketParallelCnt // TODO: optimize with respect to disk bw and transforming computation
	}
	mpopts := &mpather.JgroupOpts{
		T:        e.T,
		CTs:      []string{fs.ObjectType},
		VisitObj: r.copyObject,
		Prefix:   e.args.Msg.Prefix,
		Slab:     slab,
		Parallel: parallel,
		DoLoad:   mpather.Load,
		Throttle: true, // NOTE: always trottling
	}
	mpopts.Bck.Copy(e.args.BckFrom.Bucket())
	r.BckJog.Init(e.UUID(), e.kind, e.args.BckTo, mpopts, config)
	return
}

func (r *XactTCB) WaitRunning() { r.wg.Wait() }

func (r *XactTCB) Run(wg *sync.WaitGroup) {
	r.dm.SetXact(r)
	r.dm.Open()
	wg.Done()

	r.wg.Done()

	r.BckJog.Run()
	glog.Infoln(r.Name())

	err := r.BckJog.Wait()

	o := transport.AllocSend()
	o.Hdr.Opcode = OpcTxnDone
	r.dm.Bcast(o, nil)

	q := r.Quiesce(cmn.Timeout.CplaneOperation(), r.qcb)
	if err == nil && q == cluster.QuiAborted {
		err = r.AbortErr()
		debug.Assert(err != nil)
	}
	if err == nil {
		debug.Assert(!r.IsAborted())
		err = r.err.Err()
	}
	if err == nil && q == cluster.QuiTimeout {
		err = fmt.Errorf("%s: %v", r, cmn.ErrQuiesceTimeout)
	}

	// close
	r.dm.Close(err)
	r.dm.UnregRecv()

	r.Finish(err)
}

func (r *XactTCB) qcb(tot time.Duration) cluster.QuiRes {
	if r.err.Err() != nil {
		// to break quiescence - r.err.Err() will take precedence
		return cluster.QuiTimeout
	}
	since := mono.Since(r.rxlast.Load())
	if r.refc.Load() > 0 {
		if since > cmn.Timeout.MaxKeepalive() {
			// idle on the Rx side despite having some (refc > 0) senders
			if tot > r.BckJog.Config.Timeout.SendFile.D() {
				return cluster.QuiTimeout
			}
		}
		return cluster.QuiActive
	}
	if since > cmn.Timeout.CplaneOperation() {
		return cluster.QuiDone
	}
	return cluster.QuiInactiveCB
}

func (r *XactTCB) copyObject(lom *cluster.LOM, buf []byte) (err error) {
	objNameTo := r.args.Msg.ToName(lom.ObjName)
	if r.BckJog.Config.FastV(5, cos.SmoduleMirror) {
		glog.Infof("%s: %s => %s", r.Base.Name(), lom.Cname(), r.args.BckTo.Cname(objNameTo))
	}
	params := cluster.AllocCpObjParams()
	{
		params.BckTo = r.args.BckTo
		params.ObjNameTo = objNameTo
		params.Buf = buf
		params.DM = r.dm
		params.DP = r.args.DP
		params.Xact = r
	}
	_, err = r.T.CopyObject(lom, params, r.args.Msg.DryRun)
	if err != nil && cos.IsErrOOS(err) {
		err = cmn.NewErrAborted(r.Name(), "tcb", err)
	}
	cluster.FreeCpObjParams(params)
	return
}

// NOTE: strict(est) error handling: abort on any of the errors below
func (r *XactTCB) recv(hdr transport.ObjHdr, objReader io.Reader, err error) error {
	if err != nil && !cos.IsEOF(err) {
		glog.Error(err)
		return err
	}
	// ref-count done-senders
	if hdr.Opcode == OpcTxnDone {
		refc := r.refc.Dec()
		debug.Assert(refc >= 0)
		return nil
	}

	debug.Assert(hdr.Opcode == 0)
	lom := cluster.AllocLOM(hdr.ObjName)
	err = r._recv(hdr, objReader, lom)
	cluster.FreeLOM(lom)
	transport.DrainAndFreeReader(objReader)
	return err
}

func (r *XactTCB) _recv(hdr transport.ObjHdr, objReader io.Reader, lom *cluster.LOM) error {
	if err := lom.InitBck(&hdr.Bck); err != nil {
		r.err.Store(err)
		glog.Error(err)
		return err
	}
	lom.CopyAttrs(&hdr.ObjAttrs, true /*skip cksum*/)
	params := cluster.AllocPutObjParams()
	{
		params.WorkTag = fs.WorkfilePut
		params.Reader = io.NopCloser(objReader)
		params.Cksum = hdr.ObjAttrs.Cksum
		params.Xact = r

		// Transaction is used only by CopyBucket and ETL. In both cases new objects
		// are created at the destination. Setting `OwtPut` type informs `t.PutObject()`
		// that it must PUT the object to the remote backend as well
		// (but only after the local transaction is done and finalized).
		params.OWT = cmn.OwtPut
	}
	if lom.AtimeUnix() == 0 {
		// TODO: sender must be setting it, remove this `if` when fixed
		lom.SetAtimeUnix(time.Now().UnixNano())
	}
	params.Atime = lom.Atime()

	erp := r.t.PutObject(lom, params)
	cluster.FreePutObjParams(params)
	if erp != nil {
		r.err.Store(erp)
		glog.Error(erp)
		return erp // NOTE: non-nil signals transport to terminate
	}
	r.rxlast.Store(mono.NanoTime())
	return nil
}

func (r *XactTCB) Args() *xreg.TCBArgs { return &r.args }

func (r *XactTCB) String() string {
	return fmt.Sprintf("%s <= %s", r.Base.String(), r.args.BckFrom)
}

func (r *XactTCB) Name() string {
	return fmt.Sprintf("%s <= %s", r.Base.Name(), r.args.BckFrom)
}

func (r *XactTCB) FromTo() (*meta.Bck, *meta.Bck) {
	return r.args.BckFrom, r.args.BckTo
}

func (r *XactTCB) Snap() (snap *cluster.Snap) {
	snap = &cluster.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	f, t := r.FromTo()
	snap.SrcBck, snap.DstBck = f.Clone(), t.Clone()
	return
}
