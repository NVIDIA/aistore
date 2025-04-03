// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"io"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	tcbFactory struct {
		xreg.RenewBase
		xctn  *XactTCB
		args  *xreg.TCBArgs
		kind  string
		phase string // (see "transition")
		owt   cmn.OWT
	}
	XactTCB struct {
		p     *tcbFactory
		dm    *bundle.DataMover
		sntl  sentinel
		prune prune
		nam   string
		xact.BckJog
		transform etl.Session // stateful etl Session
		copier
	}
)

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
		smap      = core.T.Sowner().Get()
		nat       = smap.CountActiveTs()
		config    = cmn.GCO.Get()
		slab, err = core.T.PageMM().GetSlab(memsys.MaxPageSlabSize) // estimate
	)
	debug.AssertNoErr(err)

	r := newTCB(p, slab, config, smap, nat)

	p.owt = cmn.OwtCopy
	if p.kind == apc.ActETLBck {
		// TODO upon abort: call r.transform.Finish() to cleanup communicator's state
		p.owt = cmn.OwtTransform
		r.copier.getROC, r.transform, err = etl.GetOfflineTransform(p.args.Msg.Transform.Name, r)
		if err != nil {
			return err
		}
	}

	if err := core.InMaintOrDecomm(smap, core.T.Snode(), r); err != nil {
		return err
	}

	// single-node cluster
	if nat <= 1 {
		return nil
	}

	// data mover and sentinel
	var sizePDU int32
	if p.kind == apc.ActETLBck {
		sizePDU = memsys.DefaultBufSize // `transport` to generate PDU-based traffic
	}
	if err := p.newDM(config, p.UUID(), sizePDU); err != nil {
		return err
	}
	r.sntl.init(r, r.dm, smap, nat-1)
	if l := len(r.sntl.pend.m); l != nat-1 {
		// (unlikely)
		err := fmt.Errorf("%s: encountered membership changes during startup (%d, %d)", r.Name(), l, nat-1)
		debug.AssertNoErr(err)
		r.TxnAbort(err)
		r.dm = nil
		return err
	}

	return nil
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
	dm := bundle.NewDM(trname+"-"+uuid, p.xctn.recv, p.owt, dmExtra)
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

// limited pre-run abort
func (r *XactTCB) TxnAbort(err error) {
	err = cmn.NewErrAborted(r.Name(), "tcb: txn-abort", err)
	if r.dm != nil {
		r.dm.Close(err)
		r.dm.UnregRecv()
	}
	r.AddErr(err)
	r.Base.Finish()
}

func newTCB(p *tcbFactory, slab *memsys.Slab, config *cmn.Config, smap *meta.Smap, nat int) (r *XactTCB) {
	var (
		args     = p.args
		msg      = args.Msg
		parallel int
	)
	if p.kind == apc.ActETLBck {
		parallel = etlBucketParallelCnt // TODO: optimize with respect to disk bw and transforming computation
	}

	r = &XactTCB{p: p}
	mpopts := &mpather.JgroupOpts{
		CTs:      []string{fs.ObjectType},
		VisitObj: r.do,
		Prefix:   msg.Prefix,
		Slab:     slab,
		Parallel: parallel,
		DoLoad:   mpather.Load,
		Throttle: false, // superseded by destination rate-limiting (v3.28)
	}
	mpopts.Bck.Copy(args.BckFrom.Bucket())

	// ctlmsg
	var (
		sb        strings.Builder
		fromCname = args.BckFrom.Cname(msg.Prefix)
		toCname   = args.BckTo.Cname(msg.Prepend)
	)
	sb.Grow(80)
	msg.Str(&sb, fromCname, toCname)

	// init
	r.BckJog.Init(p.UUID(), p.kind, sb.String() /*ctlmsg*/, args.BckTo, mpopts, config)

	// xname
	r._name(fromCname, toCname)

	r.rate.init(args.BckFrom, args.BckTo, nat)

	if msg.Sync {
		debug.Assert(msg.Prepend == "", msg.Prepend) // validated (cli, P)
		{
			r.prune.parent = r
			r.prune.smap = smap
			r.prune.bckFrom = args.BckFrom
			r.prune.bckTo = args.BckTo
			r.prune.prefix = msg.Prefix
		}
		r.prune.init(config)
	}

	r.copier.r = r

	debug.Assert(args.BckFrom.Props != nil)
	// (rgetstats)
	if bck := args.BckFrom; bck.IsRemote() {
		r.bp = core.T.Backend(bck)
		r.vlabs = map[string]string{
			stats.VlabBucket: bck.Cname(""),
			stats.VlabXkind:  r.Kind(),
		}
	}

	p.xctn = r
	return r
}

func (r *XactTCB) Run(wg *sync.WaitGroup) {
	if r.dm != nil {
		r.dm.SetXact(r)
		r.dm.Open()
	}
	wg.Done()

	r.BckJog.Run()
	if r.p.args.Msg.Sync {
		r.prune.run() // the 2nd jgroup
	}
	nlog.Infoln(r.Name())

	err := r.BckJog.Wait()

	if r.dm != nil {
		//
		// broadcast (done | abort) and wait for others
		//
		r.sntl.bcast(r.AbortErr())
		q := r.Quiesce(r.Config.Timeout.MaxHostBusy.D(), r.qcb)
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
	nwait := r.sntl.pend.n.Load()
	if nwait > 0 {
		r.sntl.rarelog(tot, r.Config.Timeout.MaxHostBusy.D(), r.ErrCnt())
		return core.QuiActive
	}
	return core.QuiDone
}

func (r *XactTCB) do(lom *core.LOM, buf []byte) error {
	args := r.p.args // TCBArgs
	a := r.copier.prepare(lom, args.BckTo, args.Msg)
	a.Config, a.Buf, a.OWT = r.Config, buf, r.p.owt

	err := r.copier.do(a, lom, r.dm)
	if err == nil && args.Msg.Sync {
		r.prune.filter.Insert(cos.UnsafeB(lom.Uname()))
	}
	return err
}

// NOTE: strict(est) error handling: abort on any of the errors below
func (r *XactTCB) recv(hdr *transport.ObjHdr, objReader io.Reader, err error) error {
	if err != nil && !cos.IsEOF(err) {
		nlog.Errorln(err)
		return err
	}

	switch hdr.Opcode {
	case opdone:
		r.sntl.rxdone(hdr)
		return nil
	case opabrt:
		r.sntl.rxabrt(hdr)
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

	return nil
}

func (r *XactTCB) Args() *xreg.TCBArgs { return r.p.args }

func (r *XactTCB) _name(fromCname, toCname string) {
	var sb strings.Builder
	sb.Grow(80)
	sb.WriteString(r.Base.Cname())
	sb.WriteByte('-')
	sb.WriteString(fromCname)
	sb.WriteString("=>")
	sb.WriteString(toCname)
	r.nam = sb.String()
}

func (r *XactTCB) String() string { return r.nam }
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
