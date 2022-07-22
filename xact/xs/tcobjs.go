// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"io"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

type (
	tcoFactory struct {
		streamingF
		args *xreg.TCObjsArgs
	}
	XactTCObjs struct {
		streamingX
		args    *xreg.TCObjsArgs
		workCh  chan *cmn.TCObjsMsg
		pending struct {
			sync.RWMutex
			m map[string]*tcowi
		}
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
)

////////////////
// tcoFactory //
////////////////

func (p *tcoFactory) New(args xreg.Args, fromBck *cluster.Bck) xreg.Renewable {
	np := &tcoFactory{streamingF: streamingF{RenewBase: xreg.RenewBase{Args: args, Bck: fromBck}, kind: p.kind}}
	np.args = args.Custom.(*xreg.TCObjsArgs)
	return np
}

func (p *tcoFactory) Start() error {
	var sizePDU int32
	workCh := make(chan *cmn.TCObjsMsg, maxNumInParallel)
	r := &XactTCObjs{streamingX: streamingX{p: &p.streamingF}, args: p.args, workCh: workCh}
	r.pending.m = make(map[string]*tcowi, maxNumInParallel)
	p.xctn = r
	r.DemandBase.Init(p.UUID(), p.Kind(), p.Bck, 0 /*use default*/)
	if p.kind == apc.ActETLObjects {
		sizePDU = memsys.DefaultBufSize
	}
	if err := p.newDM("tco", r.recv, sizePDU); err != nil {
		return err
	}
	p.dm.SetXact(r)
	p.dm.Open()

	xact.GoRunW(r)
	return nil
}

////////////////
// XactTCObjs //
////////////////

func (r *XactTCObjs) Begin(msg *cmn.TCObjsMsg) {
	wi := &tcowi{r: r, msg: msg}
	r.pending.Lock()
	r.pending.m[msg.TxnUUID] = wi
	r.wiCnt.Inc()
	r.pending.Unlock()
}

func (r *XactTCObjs) Do(msg *cmn.TCObjsMsg) {
	r.IncPending()
	r.workCh <- msg
}

func (r *XactTCObjs) Run(wg *sync.WaitGroup) {
	var err error
	glog.Infoln(r.Name())
	wg.Done()
	for {
		select {
		case msg := <-r.workCh:
			var (
				smap    = r.p.T.Sowner().Get()
				lrit    = &lriterator{}
				freeLOM = false // not delegating
			)
			r.pending.RLock()
			wi, ok := r.pending.m[msg.TxnUUID]
			r.pending.RUnlock()
			if !ok {
				debug.Assert(!r.err.IsNil()) // see cleanup
				goto fin
			}
			wi.refc.Store(int32(smap.CountTargets() - 1))
			lrit.init(r, r.p.T, &msg.SelectObjsMsg, freeLOM)
			if msg.IsList() {
				err = lrit.iterateList(wi, smap)
			} else {
				err = lrit.iterateRange(wi, smap)
			}
			if r.IsAborted() || err != nil {
				goto fin
			}
			r.eoi(wi.msg.TxnUUID, nil)
			r.DecPending()
		case <-r.IdleTimer():
			goto fin
		case errCause := <-r.ChanAbort():
			if err == nil {
				err = errCause
			}
			goto fin
		}
	}
fin:
	err = r.fin(err)
	if err != nil {
		// cleanup: destroy destination iff it was created by this copy
		r.pending.Lock()
		for uuid := range r.pending.m {
			delete(r.pending.m, uuid)
		}
		r.pending.Unlock()
	}
}

// NOTE: strict(est) error handling: abort on any of the errors below
func (r *XactTCObjs) recv(hdr transport.ObjHdr, objReader io.Reader, err error) error {
	r.IncPending()
	defer func() {
		r.DecPending()
		transport.DrainAndFreeReader(objReader)
	}()
	if err != nil && !cos.IsEOF(err) {
		glog.Error(err)
		return err
	}
	if hdr.Opcode == OpcTxnDone {
		txnUUID := string(hdr.Opaque)
		r.pending.RLock()
		wi, ok := r.pending.m[txnUUID]
		r.pending.RUnlock()
		if !ok {
			debug.Assert(!r.err.IsNil()) // see cleanup
			return r.err.Err()
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
	defer cluster.FreeLOM(lom)
	if err := lom.InitBck(&hdr.Bck); err != nil {
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
		glog.Error(err)
	}
	return err
}

///////////
// tcowi //
///////////

func (wi *tcowi) do(lom *cluster.LOM, lri *lriterator) {
	objNameTo := wi.msg.ToName(lom.ObjName)
	buf, slab := lri.t.PageMM().Alloc()
	params := cluster.AllocCpObjParams()
	{
		params.BckTo = wi.r.args.BckTo
		params.ObjNameTo = objNameTo
		params.DM = wi.r.p.dm
		params.Buf = buf
		params.DP = wi.r.args.DP
		params.Xact = wi.r
	}
	size, err := lri.t.CopyObject(lom, params, wi.msg.DryRun)
	slab.Free(buf)
	cluster.FreeCpObjParams(params)
	if err != nil {
		if !cmn.IsObjNotExist(err) {
			wi.r.raiseErr(err, 0, wi.msg.ContinueOnError)
		}
		return
	}
	if size != cos.ContentLengthUnknown {
		return
	}
	// under ETL, sizes of transformed objects are unknown until after the transformation
	// TODO: support precise post-transform byte count
	if err := lom.Load(false /*cacheit*/, false /*locked*/); err != nil {
		wi.r.raiseErr(err, 0, wi.msg.ContinueOnError)
	}
}
