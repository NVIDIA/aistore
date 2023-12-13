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

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
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
		p      *tcbFactory
		dm     *bundle.DataMover
		rxlast atomic.Int64 // finishing
		xact.BckJog
		wg   sync.WaitGroup // starting up
		refc atomic.Int32   // finishing
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

func (p *tcbFactory) New(args xreg.Args, bck *meta.Bck) xreg.Renewable {
	custom := args.Custom.(*xreg.TCBArgs)
	return &tcbFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, kind: p.kind, phase: custom.Phase, args: custom}
}

func (p *tcbFactory) Start() error {
	var (
		config    = cmn.GCO.Get()
		slab, err = p.T.PageMM().GetSlab(memsys.MaxPageSlabSize) // TODO: estimate
	)
	debug.AssertNoErr(err)
	p.xctn = newTCB(p, slab, config)

	// refcount OpcTxnDone; this target must ve active (ref: ignoreMaintenance)
	smap := p.T.Sowner().Get()
	if err := p.xctn.InMaintOrDecomm(smap, p.T.Snode()); err != nil {
		return err
	}
	nat := smap.CountActiveTs()
	p.xctn.refc.Store(int32(nat - 1))
	p.xctn.wg.Add(1)

	var sizePDU int32
	if p.kind == apc.ActETLBck {
		sizePDU = memsys.DefaultBufSize
	}
	err = p.newDM(config, p.UUID(), sizePDU)
	return err
}

func (p *tcbFactory) newDM(config *cmn.Config, uuid string, sizePDU int32) error {
	const trname = "tcb"
	dmExtra := bundle.Extra{
		RecvAck:     nil, // no ACKs
		Config:      config,
		Compression: config.TCB.Compression,
		Multiplier:  config.TCB.SbundleMult,
		SizePDU:     sizePDU,
	}
	dm, err := bundle.NewDataMover(p.T, trname+"-"+uuid, p.xctn.recv, cmn.OwtPut, dmExtra)
	if err != nil {
		return err
	}
	if err := dm.RegRecv(); err != nil {
		return err
	}
	dm.SetXact(p.xctn)
	p.xctn.dm = dm
	return nil
}

func (p *tcbFactory) Kind() string      { return p.kind }
func (p *tcbFactory) Get() cluster.Xact { return p.xctn }

func (p *tcbFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	prev := prevEntry.(*tcbFactory)
	if p.UUID() != prev.UUID() {
		err = cmn.NewErrXactUsePrev(prevEntry.Get().String())
		return
	}
	bckEq := prev.args.BckFrom.Equal(p.args.BckFrom, true /*same BID*/, true /* same backend */)
	debug.Assert(bckEq)
	debug.Assert(prev.phase == apc.ActBegin && p.phase == apc.ActCommit)
	prev.args.Phase = apc.ActCommit // transition
	wpr = xreg.WprUse
	return
}

/////////////
// XactTCB //
/////////////

// copies one bucket _into_ another with or without transformation.
// args.DP.Reader() is the reader to receive transformed bytes; when nil we do a plain bucket copy.

// limited pre-run abort
func (r *XactTCB) TxnAbort(err error) {
	err = cmn.NewErrAborted(r.Name(), "tcb: txn-abort", err)
	r.dm.CloseIf(err)
	r.dm.UnregRecv()
	r.AddErr(err)
	r.Base.Finish()
}

func newTCB(p *tcbFactory, slab *memsys.Slab, config *cmn.Config) (r *XactTCB) {
	var parallel int
	r = &XactTCB{p: p}
	if p.kind == apc.ActETLBck {
		parallel = etlBucketParallelCnt // TODO: optimize with respect to disk bw and transforming computation
	}
	mpopts := &mpather.JgroupOpts{
		T:        p.T,
		CTs:      []string{fs.ObjectType},
		VisitObj: r.copyObject,
		Prefix:   p.args.Msg.Prefix,
		Slab:     slab,
		Parallel: parallel,
		DoLoad:   mpather.Load,
		Throttle: true, // NOTE: always trottling
	}
	mpopts.Bck.Copy(p.args.BckFrom.Bucket())
	r.BckJog.Init(p.UUID(), p.kind, p.args.BckTo, mpopts, config)
	return
}

func (r *XactTCB) WaitRunning() { r.wg.Wait() }

func (r *XactTCB) Run(wg *sync.WaitGroup) {
	r.dm.SetXact(r)
	r.dm.Open()
	wg.Done()

	r.wg.Done()

	r.BckJog.Run()
	nlog.Infoln(r.Name())

	err := r.BckJog.Wait()

	o := transport.AllocSend()
	o.Hdr.Opcode = OpcTxnDone
	r.dm.Bcast(o, nil)

	q := r.Quiesce(cmn.Rom.CplaneOperation(), r.qcb)
	if q == cluster.QuiTimeout {
		r.AddErr(fmt.Errorf("%s: %v", r, cmn.ErrQuiesceTimeout))
	}

	// close
	r.dm.Close(err)
	r.dm.UnregRecv()

	r.Finish()
}

func (r *XactTCB) qcb(tot time.Duration) cluster.QuiRes {
	// TODO -- FIXME =======================
	if cnt := r.ErrCnt(); cnt > 0 {
		// to break quiescence - the waiter will look at r.Err() first anyway
		return cluster.QuiTimeout
	}

	since := mono.Since(r.rxlast.Load())
	if r.refc.Load() > 0 {
		if since > cmn.Rom.MaxKeepalive() {
			// idle on the Rx side despite having some (refc > 0) senders
			if tot > r.BckJog.Config.Timeout.SendFile.D() {
				return cluster.QuiTimeout
			}
		}
		return cluster.QuiActive
	}
	if since > cmn.Rom.CplaneOperation() {
		return cluster.QuiDone
	}
	return cluster.QuiInactiveCB
}

func (r *XactTCB) copyObject(lom *cluster.LOM, buf []byte) (err error) {
	var (
		args   = r.p.args // TCBArgs
		toName = r.p.args.Msg.ToName(lom.ObjName)
	)
	if r.BckJog.Config.FastV(5, cos.SmoduleMirror) {
		nlog.Infof("%s: %s => %s", r.Base.Name(), lom.Cname(), args.BckTo.Cname(toName))
	}
	_, err = r.p.T.CopyObject(lom, r.dm, args.DP, r, args.BckTo, toName, buf, args.Msg.DryRun)
	if err != nil {
		if cos.IsErrOOS(err) {
			// TODO: call r.Abort() instead
			err = cmn.NewErrAborted(r.Name(), "tcb", err)
		}
		r.AddErr(err)
		if r.BckJog.Config.FastV(5, cos.SmoduleMirror) {
			nlog.Infof("Error: %v", err)
		}
	}
	return
}

// NOTE: strict(est) error handling: abort on any of the errors below
func (r *XactTCB) recv(hdr *transport.ObjHdr, objReader io.Reader, err error) error {
	if err != nil && !cos.IsEOF(err) {
		nlog.Errorln(err)
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

func (r *XactTCB) _recv(hdr *transport.ObjHdr, objReader io.Reader, lom *cluster.LOM) error {
	if err := lom.InitBck(&hdr.Bck); err != nil {
		r.AddErr(err)
		nlog.Errorln(err)
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

	erp := r.p.T.PutObject(lom, params)
	cluster.FreePutObjParams(params)
	if erp != nil {
		r.AddErr(erp)
		nlog.Errorln(erp)
		return erp // NOTE: non-nil signals transport to terminate
	}
	r.rxlast.Store(mono.NanoTime())
	return nil
}

func (r *XactTCB) Args() *xreg.TCBArgs { return r.p.args }

func (r *XactTCB) String() string {
	return fmt.Sprintf("%s <= %s", r.Base.String(), r.p.args.BckFrom)
}

func (r *XactTCB) Name() string {
	return fmt.Sprintf("%s <= %s", r.Base.Name(), r.p.args.BckFrom)
}

func (r *XactTCB) FromTo() (*meta.Bck, *meta.Bck) {
	return r.p.args.BckFrom, r.p.args.BckTo
}

func (r *XactTCB) Snap() (snap *cluster.Snap) {
	snap = &cluster.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	f, t := r.FromTo()
	snap.SrcBck, snap.DstBck = f.Clone(), t.Clone()
	return
}
