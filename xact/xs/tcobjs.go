// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"io"
	"runtime"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

const PrefixTcoID = "tco-"

type (
	tcoFactory struct {
		args *xreg.TCObjsArgs
		streamingF
	}
	XactTCObjs struct {
		pending struct {
			m   map[string]*tcowi
			mtx sync.RWMutex
		}
		args     *xreg.TCObjsArgs
		workCh   chan *cmn.TCObjsMsg
		chanFull atomic.Int64
		streamingX
	}
	tcowi struct {
		r   *XactTCObjs
		msg *cmn.TCObjsMsg
		// finishing
		refc atomic.Int32
	}
)

// interface guard
var (
	_ core.Xact      = (*XactTCObjs)(nil)
	_ xreg.Renewable = (*tcoFactory)(nil)
	_ lrwi           = (*tcowi)(nil)
)

////////////////
// tcoFactory //
////////////////

func (p *tcoFactory) New(args xreg.Args, bckFrom *meta.Bck) xreg.Renewable {
	np := &tcoFactory{streamingF: streamingF{RenewBase: xreg.RenewBase{Args: args, Bck: bckFrom}, kind: p.kind}}
	np.args = args.Custom.(*xreg.TCObjsArgs)
	return np
}

func (p *tcoFactory) Start() error {
	//
	// target-local generation of a global UUID
	//
	uuid, err := p.genBEID(p.args.BckFrom, p.args.BckTo)
	if err != nil {
		return err
	}
	p.Args.UUID = PrefixTcoID + uuid

	// new x-tco
	workCh := make(chan *cmn.TCObjsMsg, maxNumInParallel)
	r := &XactTCObjs{streamingX: streamingX{p: &p.streamingF, config: cmn.GCO.Get()}, args: p.args, workCh: workCh}
	r.pending.m = make(map[string]*tcowi, maxNumInParallel)
	p.xctn = r
	r.DemandBase.Init(p.UUID(), p.Kind(), p.Bck, xact.IdleDefault)

	var sizePDU int32
	if p.kind == apc.ActETLObjects {
		// unlike apc.ActCopyObjects (where we know the size)
		// apc.ActETLObjects (transform) generates arbitrary sizes where we use PDU-based transport
		sizePDU = memsys.DefaultBufSize
	}
	if err := p.newDM(p.Args.UUID /*trname*/, r.recv, r.config, sizePDU); err != nil {
		return err
	}
	if r.p.dm != nil {
		p.dm.SetXact(r)
		p.dm.Open()
	}
	xact.GoRunW(r)
	return nil
}

////////////////
// XactTCObjs //
////////////////

func (r *XactTCObjs) Name() string {
	return fmt.Sprintf("%s => %s", r.streamingX.Name(), r.args.BckTo)
}

func (r *XactTCObjs) String() string {
	return r.streamingX.String() + " => " + r.args.BckTo.String()
}

func (r *XactTCObjs) FromTo() (*meta.Bck, *meta.Bck) { return r.args.BckFrom, r.args.BckTo }

func (r *XactTCObjs) Snap() (snap *core.Snap) {
	snap = &core.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	f, t := r.FromTo()
	snap.SrcBck, snap.DstBck = f.Clone(), t.Clone()
	return
}

func (r *XactTCObjs) Begin(msg *cmn.TCObjsMsg) {
	wi := &tcowi{r: r, msg: msg}
	r.pending.mtx.Lock()
	r.pending.m[msg.TxnUUID] = wi
	r.wiCnt.Inc()
	r.pending.mtx.Unlock()
}

func (r *XactTCObjs) Run(wg *sync.WaitGroup) {
	var err error
	nlog.Infoln(r.Name())
	wg.Done()
	for {
		select {
		case msg := <-r.workCh:
			var (
				smap = core.T.Sowner().Get()
				lrit = &lriterator{}
			)
			r.pending.mtx.Lock()
			wi, ok := r.pending.m[msg.TxnUUID]
			r.pending.mtx.Unlock()
			if !ok {
				debug.Assert(r.ErrCnt() > 0) // see cleanup
				goto fin
			}

			// this target must be active (ref: ignoreMaintenance)
			if err = core.InMaintOrDecomm(smap, core.T.Snode(), r); err != nil {
				nlog.Errorln(err)
				goto fin
			}
			nat := smap.CountActiveTs()
			wi.refc.Store(int32(nat - 1))

			lrit.init(r, &msg.ListRange)
			if msg.IsList() {
				err = lrit.iterList(wi, smap)
			} else {
				err = lrit.rangeOrPref(wi, smap)
			}
			if r.IsAborted() || err != nil {
				goto fin
			}
			r.sendTerm(wi.msg.TxnUUID, nil, nil)
			r.DecPending()
		case <-r.IdleTimer():
			goto fin
		case <-r.ChanAbort():
			goto fin
		}
	}
fin:
	r.fin(true /*unreg Rx*/)
	if r.Err() != nil {
		// cleanup: destroy destination iff it was created by this copy
		r.pending.mtx.Lock()
		clear(r.pending.m)
		r.pending.mtx.Unlock()
	}
}

// more work
func (r *XactTCObjs) Do(msg *cmn.TCObjsMsg) {
	r.IncPending()
	r.workCh <- msg

	if l, c := len(r.workCh), cap(r.workCh); l > c/2 {
		runtime.Gosched() // poor man's throttle
		if l == c {
			cnt := r.chanFull.Inc()
			if (cnt >= 10 && cnt <= 20) || (cnt > 0 && cmn.Rom.FastV(5, cos.SmoduleXs)) {
				nlog.Errorln("work channel full", r.Name())
			}
		}
	}
}

//
// Rx
//

// NOTE: strict(est) error handling: abort on any of the errors below
func (r *XactTCObjs) recv(hdr *transport.ObjHdr, objReader io.Reader, err error) error {
	if err != nil && !cos.IsEOF(err) {
		goto ex
	}

	r.IncPending()
	err = r._recv(hdr, objReader)
	r.DecPending()
	transport.DrainAndFreeReader(objReader)
ex:
	if err != nil && cmn.Rom.FastV(4, cos.SmoduleXs) {
		nlog.Errorln(err)
	}
	return err
}

func (r *XactTCObjs) _recv(hdr *transport.ObjHdr, objReader io.Reader) error {
	if hdr.Opcode == opcodeDone {
		txnUUID := string(hdr.Opaque)
		r.pending.mtx.Lock()
		wi, ok := r.pending.m[txnUUID]
		if !ok {
			r.pending.mtx.Unlock()
			_, err := r.JoinErr()
			return err
		}
		refc := wi.refc.Dec()
		if refc == 0 {
			delete(r.pending.m, txnUUID)
			r.wiCnt.Dec()
		}
		r.pending.mtx.Unlock()
		return nil
	}

	debug.Assert(hdr.Opcode == 0)
	lom := core.AllocLOM(hdr.ObjName)
	err := r._put(hdr, objReader, lom)
	core.FreeLOM(lom)
	return err
}

func (r *XactTCObjs) _put(hdr *transport.ObjHdr, objReader io.Reader, lom *core.LOM) (err error) {
	if err = lom.InitBck(&hdr.Bck); err != nil {
		return
	}
	lom.CopyAttrs(&hdr.ObjAttrs, true /*skip cksum*/)
	params := core.AllocPutParams()
	{
		params.WorkTag = fs.WorkfilePut
		params.Reader = io.NopCloser(objReader)
		params.Cksum = hdr.ObjAttrs.Cksum
		params.Xact = r
		params.Size = hdr.ObjAttrs.Size

		// Transaction is used only by CopyBucket and ETL. In both cases, new objects
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
	err = core.T.PutObject(lom, params)
	core.FreePutParams(params)

	if err != nil {
		r.AddErr(err, 5, cos.SmoduleXs)
	} else if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infof("%s: tco-Rx %s, size=%d", r.Base.Name(), lom.Cname(), hdr.ObjAttrs.Size)
	}
	return
}

///////////
// tcowi //
///////////

func (wi *tcowi) do(lom *core.LOM, lrit *lriterator) {
	var (
		objNameTo = wi.msg.ToName(lom.ObjName)
		buf, slab = core.T.PageMM().Alloc()
	)

	// under ETL, the returned sizes of transformed objects are unknown (`cos.ContentLengthUnknown`)
	// until after the transformation; here we are disregarding the size anyway as the stats
	// are done elsewhere

	coiParams := core.AllocCOI()
	{
		coiParams.DP = wi.r.args.DP
		coiParams.Xact = wi.r
		coiParams.Config = wi.r.config
		coiParams.BckTo = wi.r.args.BckTo
		coiParams.ObjnameTo = objNameTo
		coiParams.Buf = buf
		coiParams.OWT = cmn.OwtMigrateRepl
		coiParams.DryRun = wi.msg.DryRun
		coiParams.LatestVer = wi.msg.LatestVer
		coiParams.Sync = wi.msg.Sync
	}
	_, err := core.T.CopyObject(lom, wi.r.p.dm, coiParams)
	core.FreeCOI(coiParams)
	slab.Free(buf)

	if err != nil {
		if !cos.IsNotExist(err, 0) || lrit.lrp == lrpList {
			wi.r.AddErr(err, 5, cos.SmoduleXs)
		}
	} else if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infof("%s: tco-lr %s => %s", wi.r.Base.Name(), lom.Cname(), wi.r.args.BckTo.Cname(objNameTo))
	}
}
