// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
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
		owt   cmn.OWT
	}
	XactTCB struct {
		p      *tcbFactory
		dm     *bundle.DataMover
		rxlast atomic.Int64 // finishing
		xact.BckJog
		prune    prune
		nam, str string
		wg       sync.WaitGroup // starting up
		refc     atomic.Int32   // finishing
	}
)

const OpcTxnDone = 27182

const etlBucketParallelCnt = 2

// interface guard
var (
	_ core.Xact      = (*XactTCB)(nil)
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
		slab, err = core.T.PageMM().GetSlab(memsys.MaxPageSlabSize) // TODO: estimate
	)
	debug.AssertNoErr(err)

	p.owt = cmn.OwtCopy
	if p.kind == apc.ActETLBck {
		p.owt = cmn.OwtTransform
	}

	smap := core.T.Sowner().Get()
	p.xctn = newTCB(p, slab, config, smap)

	// refcount OpcTxnDone; this target must ve active (ref: ignoreMaintenance)
	if err := core.InMaintOrDecomm(smap, core.T.Snode(), p.xctn); err != nil {
		return err
	}
	nat := smap.CountActiveTs()
	p.xctn.refc.Store(int32(nat - 1))
	p.xctn.wg.Add(1)

	var sizePDU int32
	if p.kind == apc.ActETLBck {
		sizePDU = memsys.DefaultBufSize
	}
	if nat <= 1 {
		return nil
	}
	return p.newDM(config, p.UUID(), sizePDU)
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
	// in re cmn.OwtPut: see comment inside _recv()
	dm, err := bundle.NewDataMover(trname+"-"+uuid, p.xctn.recv, p.owt, dmExtra)
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

func (p *tcbFactory) Kind() string   { return p.kind }
func (p *tcbFactory) Get() core.Xact { return p.xctn }

func (p *tcbFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	prev := prevEntry.(*tcbFactory)
	if p.UUID() != prev.UUID() {
		err = cmn.NewErrXactUsePrev(prevEntry.Get().String())
		return
	}
	bckEq := prev.args.BckFrom.Equal(p.args.BckFrom, true /*same BID*/, true /*same backend*/)
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
	r.dm.Close(err)
	r.dm.UnregRecv()
	r.AddErr(err)
	r.Base.Finish()
}

func newTCB(p *tcbFactory, slab *memsys.Slab, config *cmn.Config, smap *meta.Smap) (r *XactTCB) {
	r = &XactTCB{p: p}

	s1, s2 := r._str(), r.p.args.BckFrom.String()
	r.nam = r.Base.Name() + " <= " + s2 + s1
	r.str = r.Base.String() + " <= " + s2 + s1

	var parallel int
	if p.kind == apc.ActETLBck {
		parallel = etlBucketParallelCnt // TODO: optimize with respect to disk bw and transforming computation
	}
	mpopts := &mpather.JgroupOpts{
		CTs:      []string{fs.ObjectType},
		VisitObj: r.do,
		Prefix:   p.args.Msg.Prefix,
		Slab:     slab,
		Parallel: parallel,
		DoLoad:   mpather.Load,
		Throttle: true, // always trottling
	}
	mpopts.Bck.Copy(p.args.BckFrom.Bucket())
	r.BckJog.Init(p.UUID(), p.kind, p.args.BckTo, mpopts, config)

	if p.args.Msg.Sync {
		debug.Assert(p.args.Msg.Prepend == "", p.args.Msg.Prepend) // validated (cli, P)
		{
			r.prune.parent = r
			r.prune.smap = smap
			r.prune.bckFrom = p.args.BckFrom
			r.prune.bckTo = p.args.BckTo
			r.prune.prefix = p.args.Msg.Prefix
		}
		r.prune.init(config)
	}
	return
}

func (r *XactTCB) WaitRunning() { r.wg.Wait() }

func (r *XactTCB) Run(wg *sync.WaitGroup) {
	if r.dm != nil {
		r.dm.SetXact(r)
		r.dm.Open()
	}
	wg.Done()

	r.wg.Done()

	r.BckJog.Run()
	if r.p.args.Msg.Sync {
		r.prune.run() // the 2nd jgroup
	}
	nlog.Infoln(r.Name())

	err := r.BckJog.Wait()

	if r.dm != nil {
		o := transport.AllocSend()
		o.Hdr.Opcode = OpcTxnDone
		r.dm.Bcast(o, nil)

		q := r.Quiesce(cmn.Rom.CplaneOperation(), r.qcb)
		if q == core.QuiTimeout {
			r.AddErr(fmt.Errorf("%s: %v", r, cmn.ErrQuiesceTimeout))
		}

		// close
		r.dm.Close(err)
		r.dm.UnregRecv()
	}
	if r.p.args.Msg.Sync {
		r.prune.wait()
	}
	r.Finish()
}

func (r *XactTCB) qcb(tot time.Duration) core.QuiRes {
	// TODO -- FIXME =======================
	if cnt := r.ErrCnt(); cnt > 0 {
		// to break quiescence - the waiter will look at r.Err() first anyway
		return core.QuiTimeout
	}

	since := mono.Since(r.rxlast.Load())
	if r.refc.Load() > 0 {
		if since > cmn.Rom.MaxKeepalive() {
			// idle on the Rx side despite having some (refc > 0) senders
			if tot > r.BckJog.Config.Timeout.SendFile.D() {
				return core.QuiTimeout
			}
		}
		return core.QuiActive
	}
	if since > cmn.Rom.CplaneOperation() {
		return core.QuiDone
	}
	return core.QuiInactiveCB
}

func (r *XactTCB) do(lom *core.LOM, buf []byte) (err error) {
	var (
		args   = r.p.args // TCBArgs
		toName = args.Msg.ToName(lom.ObjName)
	)
	if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infoln(r.Base.Name()+":", lom.Cname(), "=>", args.BckTo.Cname(toName))
	}
	coiParams := AllocCOI()
	{
		coiParams.DP = args.DP
		coiParams.Xact = r
		coiParams.Config = r.Config
		coiParams.BckTo = args.BckTo
		coiParams.ObjnameTo = toName
		coiParams.Buf = buf
		coiParams.DryRun = args.Msg.DryRun
		coiParams.LatestVer = args.Msg.LatestVer
		coiParams.Sync = args.Msg.Sync
		coiParams.OWT = r.p.owt
		coiParams.Finalize = false
		if coiParams.ObjnameTo == "" {
			coiParams.ObjnameTo = lom.ObjName
		}
	}
	_, err = gcoi.CopyObject(lom, r.dm, coiParams)
	FreeCOI(coiParams)
	switch {
	case err == nil:
		if args.Msg.Sync {
			r.prune.filter.Insert(cos.UnsafeB(lom.Uname()))
		}
	case cos.IsNotExist(err, 0):
		// do nothing
	case cos.IsErrOOS(err):
		r.Abort(err)
	default:
		r.AddErr(err, 5, cos.SmoduleXs)
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
	lom := core.AllocLOM(hdr.ObjName)
	err = r._recv(hdr, objReader, lom)
	core.FreeLOM(lom)
	transport.DrainAndFreeReader(objReader)
	return err
}

func (r *XactTCB) _recv(hdr *transport.ObjHdr, objReader io.Reader, lom *core.LOM) error {
	if err := lom.InitBck(&hdr.Bck); err != nil {
		r.AddErr(err, 0)
		return err
	}
	lom.CopyAttrs(&hdr.ObjAttrs, true /*skip cksum*/)
	params := core.AllocPutParams()
	{
		params.WorkTag = fs.WorkfilePut
		params.Reader = io.NopCloser(objReader)
		params.Cksum = hdr.ObjAttrs.Cksum
		params.Xact = r
		params.Size = hdr.ObjAttrs.Size
		params.OWT = r.p.owt
	}
	if lom.AtimeUnix() == 0 {
		// TODO: sender must be setting it, remove this `if` when fixed
		lom.SetAtimeUnix(time.Now().UnixNano())
	}
	params.Atime = lom.Atime()

	erp := core.T.PutObject(lom, params)
	core.FreePutParams(params)
	if erp != nil {
		r.AddErr(erp, 0)
		return erp // NOTE: non-nil signals transport to terminate
	}
	r.rxlast.Store(mono.NanoTime())
	return nil
}

func (r *XactTCB) Args() *xreg.TCBArgs { return r.p.args }

func (r *XactTCB) _str() (s string) {
	msg := &r.p.args.Msg.CopyBckMsg
	if msg.Prefix != "" {
		s = ", prefix " + r.p.args.Msg.Prefix
	}
	if msg.Prepend != "" {
		s = ", prepend " + r.p.args.Msg.Prepend
	}
	if msg.LatestVer {
		s = ", latest-ver"
	}
	if msg.Sync {
		s = ", synchronize"
	}
	return s
}

func (r *XactTCB) String() string { return r.str }
func (r *XactTCB) Name() string   { return r.nam }

func (r *XactTCB) FromTo() (*meta.Bck, *meta.Bck) {
	return r.p.args.BckFrom, r.p.args.BckTo
}

func (r *XactTCB) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	f, t := r.FromTo()
	snap.SrcBck, snap.DstBck = f.Clone(), t.Clone()
	return
}
