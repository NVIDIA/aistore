// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
 */
package xs

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
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	tcoFactory struct {
		args *xreg.TCObjsArgs
		streamingF
	}
	XactTCObjs struct {
		pending struct {
			m map[string]*tcowi
			sync.RWMutex
		}
		args   *xreg.TCObjsArgs
		workCh chan *cmn.TCObjsMsg
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
	_ cluster.Xact   = (*XactTCObjs)(nil)
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

func (p *tcoFactory) Start() (err error) {
	//
	// target-local generation of a global UUID
	//
	p.Args.UUID, err = p.genBEID(p.args.BckFrom, p.args.BckTo)
	if err != nil {
		return
	}

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
	if err = p.newDM(p.Args.UUID /*trname*/, r.recv, sizePDU); err != nil {
		return
	}
	p.dm.SetXact(r)
	p.dm.Open()

	xact.GoRunW(r)
	return
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

func (r *XactTCObjs) Snap() (snap *cluster.Snap) {
	snap = &cluster.Snap{}
	r.ToSnap(snap)

	snap.IdleX = r.IsIdle()
	f, t := r.FromTo()
	snap.SrcBck, snap.DstBck = f.Clone(), t.Clone()
	return
}

func (r *XactTCObjs) Begin(msg *cmn.TCObjsMsg) {
	wi := &tcowi{r: r, msg: msg}
	r.pending.Lock()
	r.pending.m[msg.TxnUUID] = wi
	r.wiCnt.Inc()
	r.pending.Unlock()
}

func (r *XactTCObjs) Run(wg *sync.WaitGroup) {
	var err error
	nlog.Infoln(r.Name())
	wg.Done()
	for {
		select {
		case msg := <-r.workCh:
			var (
				smap = r.p.T.Sowner().Get()
				lrit = &lriterator{}
			)
			r.pending.RLock()
			wi, ok := r.pending.m[msg.TxnUUID]
			r.pending.RUnlock()
			if !ok {
				debug.Assert(r.ErrCnt() > 0) // see cleanup
				goto fin
			}

			// this target must be active (ref: ignoreMaintenance)
			if err = r.InMaintOrDecomm(smap, r.p.T.Snode()); err != nil {
				nlog.Errorln(err)
				goto fin
			}
			nat := smap.CountActiveTs()
			wi.refc.Store(int32(nat - 1))

			lrit.init(r, r.p.T, &msg.ListRange)
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
		r.pending.Lock()
		for uuid := range r.pending.m {
			delete(r.pending.m, uuid)
		}
		r.pending.Unlock()
	}
}

// more work
func (r *XactTCObjs) Do(msg *cmn.TCObjsMsg) {
	r.IncPending()
	r.workCh <- msg
}

//
// Rx
//

// NOTE: strict(est) error handling: abort on any of the errors below
func (r *XactTCObjs) recv(hdr transport.ObjHdr, objReader io.Reader, err error) error {
	if err != nil && !cos.IsEOF(err) {
		goto ex
	}

	r.IncPending()
	err = r._recv(&hdr, objReader)
	r.DecPending()
	transport.DrainAndFreeReader(objReader)
ex:
	if err != nil && r.config.FastV(4, cos.SmoduleXs) {
		nlog.Errorln(err)
	}
	return err
}

func (r *XactTCObjs) _recv(hdr *transport.ObjHdr, objReader io.Reader) error {
	if hdr.Opcode == opcodeDone {
		txnUUID := string(hdr.Opaque)
		r.pending.RLock()
		wi, ok := r.pending.m[txnUUID]
		r.pending.RUnlock()
		if !ok {
			_, err := r.JoinErr()
			return err
		}
		refc := wi.refc.Dec()
		if refc == 0 {
			r.pending.Lock()
			delete(r.pending.m, txnUUID)
			r.wiCnt.Dec()
			r.pending.Unlock()
		}
		return nil
	}

	debug.Assert(hdr.Opcode == 0)
	lom := cluster.AllocLOM(hdr.ObjName)
	err := r._put(hdr, objReader, lom)
	cluster.FreeLOM(lom)
	return err
}

func (r *XactTCObjs) _put(hdr *transport.ObjHdr, objReader io.Reader, lom *cluster.LOM) (err error) {
	if err = lom.InitBck(&hdr.Bck); err != nil {
		return
	}
	lom.CopyAttrs(&hdr.ObjAttrs, true /*skip cksum*/)
	params := cluster.AllocPutObjParams()
	{
		params.WorkTag = fs.WorkfilePut
		params.Reader = io.NopCloser(objReader)
		params.Cksum = hdr.ObjAttrs.Cksum
		params.Xact = r

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
	err = r.p.T.PutObject(lom, params)
	cluster.FreePutObjParams(params)

	if err != nil {
		r.AddErr(err)
		if r.config.FastV(5, cos.SmoduleXs) {
			nlog.Infoln("Error: ", err)
		}
	} else if r.config.FastV(5, cos.SmoduleXs) {
		nlog.Infof("%s: tco-Rx %s, size=%d", r.Base.Name(), lom.Cname(), hdr.ObjAttrs.Size)
	}
	return
}

///////////
// tcowi //
///////////

func (wi *tcowi) do(lom *cluster.LOM, lrit *lriterator) {
	objNameTo := wi.msg.ToName(lom.ObjName)
	buf, slab := lrit.t.PageMM().Alloc()
	params := cluster.AllocCpObjParams()
	{
		params.BckTo = wi.r.args.BckTo
		params.ObjNameTo = objNameTo
		params.DM = wi.r.p.dm
		params.Buf = buf
		params.DP = wi.r.args.DP
		params.Xact = wi.r
	}
	// NOTE:
	// under ETL, the returned sizes of transformed objects are unknown (cos.ContentLengthUnknown)
	// until after the transformation; here we are disregarding the size anyway as the stats
	// are done elsewhere
	_, err := lrit.t.CopyObject(lom, params, wi.msg.DryRun)
	slab.Free(buf)
	cluster.FreeCpObjParams(params)
	if err != nil {
		if !cmn.IsObjNotExist(err) || lrit.lrp != lrpList {
			wi.r.addErr(err, wi.msg.ContinueOnError)
		}
	} else if wi.r.config.FastV(5, cos.SmoduleXs) {
		nlog.Infof("%s: tco-lr %s => %s", wi.r.Base.Name(), lom.Cname(), wi.r.args.BckTo.Cname(objNameTo))
	}
}
