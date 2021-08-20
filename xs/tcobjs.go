// Package xs contains eXtended actions (xactions) except storage services
// (mirror, ec) and extensions (downloader, lru).
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xreg"
)

type (
	tcoFactory struct {
		xreg.RenewBase
		xact *XactTCObjs
		kind string
		args *xreg.TCObjsArgs
	}
	XactTCObjs struct {
		xaction.DemandBase
		t       cluster.Target
		args    *xreg.TCObjsArgs
		workCh  chan *cmn.TCObjsMsg
		config  *cmn.Config
		dm      *bundle.DataMover
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
	np := &tcoFactory{RenewBase: xreg.RenewBase{Args: args, Bck: fromBck}, kind: p.kind}
	np.args = args.Custom.(*xreg.TCObjsArgs)
	return np
}

func (p *tcoFactory) Start() error {
	var (
		workCh  = make(chan *cmn.TCObjsMsg, maxNumInParallel)
		err     error
		sizePDU int32
	)
	r := &XactTCObjs{t: p.T, args: p.args, workCh: workCh, config: cmn.GCO.Get()}
	r.pending.m = make(map[string]*tcowi, maxNumInParallel)
	p.xact = r
	r.DemandBase.Init(p.UUID(), p.Kind(), p.Bck, 0 /*use default*/)
	if p.kind == cmn.ActETLObjects {
		sizePDU = memsys.DefaultBufSize
	}
	if r.dm, err = newDM("tco", p.RenewBase, r.recv, sizePDU); err != nil {
		return err
	}
	r.dm.SetXact(r)
	r.dm.Open()

	xaction.GoRunW(r)
	return nil
}

func (p *tcoFactory) Kind() string      { return p.kind }
func (p *tcoFactory) Get() cluster.Xact { return p.xact }

func (p *tcoFactory) WhenPrevIsRunning(xprev xreg.Renewable) (xreg.WPR, error) {
	debug.Assertf(false, "%s vs %s", p.Str(p.Kind()), xprev) // xreg.usePrev() must've returned true
	return xreg.WprUse, nil
}

////////////////
// XactTCObjs //
////////////////

func (r *XactTCObjs) Begin(msg *cmn.TCObjsMsg) {
	wi := &tcowi{r: r, msg: msg}
	r.pending.Lock()
	r.pending.m[msg.TxnUUID] = wi
	r.pending.Unlock()
}

func (r *XactTCObjs) Do(msg *cmn.TCObjsMsg) {
	r.IncPending()
	r.workCh <- msg
}

func (r *XactTCObjs) Run(wg *sync.WaitGroup) {
	var err error
	glog.Infoln(r.String())
	wg.Done()
	for {
		select {
		case msg := <-r.workCh:
			var (
				smap    = r.t.Sowner().Get()
				lrit    = &lriterator{}
				freeLOM = false // not delegating
			)
			r.pending.RLock()
			wi := r.pending.m[msg.TxnUUID]
			r.pending.RUnlock()
			wi.refc.Store(int32(smap.CountTargets() - 1))
			lrit.init(r, r.t, &msg.ListRangeMsg, freeLOM)
			if msg.IsList() {
				err = lrit.iterateList(wi, smap)
			} else {
				err = lrit.iterateRange(wi, smap)
			}
			if r.Aborted() || err != nil {
				goto fin
			}
			r.eoi(wi)
			r.DecPending()
		case <-r.IdleTimer():
			goto fin
		case <-r.ChanAbort():
			goto fin
		}
	}
fin:
	r.fin(err)
}

func (r *XactTCObjs) fin(err error) {
	r.DemandBase.Stop()
	r.dm.Close(err)
	hk.Reg(r.ID(), r.unreg, waitUnregRecv)
	r.Finish(err)
}

func (r *XactTCObjs) unreg() (d time.Duration) {
	d = hk.UnregInterval
	if r._lpending() > 0 {
		d = waitUnregRecv
	}
	return
}

func (r *XactTCObjs) _lpending() (l int) {
	r.pending.RLock()
	l = len(r.pending.m)
	r.pending.RUnlock()
	return
}

// send EOI (end of iteration) to the responsible target
// TODO: see xs/tcobjs.go
func (r *XactTCObjs) eoi(wi *tcowi) {
	o := transport.AllocSend()
	o.Hdr.Opcode = doneSendingOpcode
	o.Hdr.Opaque = []byte(wi.msg.TxnUUID)
	r.dm.Bcast(o)
}

func (r *XactTCObjs) recv(hdr transport.ObjHdr, objReader io.Reader, err error) {
	r.IncPending()
	defer r.DecPending()
	defer transport.FreeRecv(objReader)
	if err != nil && !cos.IsEOF(err) {
		glog.Error(err)
		return
	}
	if hdr.Opcode == doneSendingOpcode {
		txnUUID := string(hdr.Opaque)
		r.pending.RLock()
		wi, ok := r.pending.m[txnUUID]
		r.pending.RUnlock()
		debug.Assert(ok)
		refc := wi.refc.Dec()
		if refc == 0 {
			r.pending.Lock()
			delete(r.pending.m, txnUUID)
			r.pending.Unlock()
		}
		return
	}
	debug.Assert(hdr.Opcode == 0)

	defer cos.DrainReader(objReader)
	lom := cluster.AllocLOM(hdr.ObjName)
	defer cluster.FreeLOM(lom)
	if err := lom.Init(hdr.Bck); err != nil {
		glog.Error(err)
		return
	}
	lom.CopyAttrs(&hdr.ObjAttrs, true /*skip cksum*/)
	params := cluster.PutObjectParams{
		Tag:    fs.WorkfilePut,
		Reader: io.NopCloser(objReader),
		// Transaction is used only by CopyBucket and ETL. In both cases new objects
		// are created at the destination. Setting `RegularPut` type informs `c.t.PutObject`
		// that it must PUT the object to the Cloud as well after the local data are
		// finalized
		RecvType: cluster.RegularPut,
		Cksum:    hdr.ObjAttrs.Cksum,
		Started:  time.Now(),
	}
	if err := r.t.PutObject(lom, params); err != nil {
		glog.Error(err)
	}
}

func (r *XactTCObjs) String() string {
	return fmt.Sprintf("%s=>%s", r.XactBase.String(), r.args.BckTo)
}

// limited pre-run abort
func (r *XactTCObjs) TxnAbort() {
	err := cmn.NewAbortedError(r.String())
	if r.dm.IsOpen() {
		r.dm.Close(err)
	}
	r.dm.UnregRecv()
	r.XactBase.Finish(err)
}

func (r *XactTCObjs) Stats() cluster.XactStats { return r.DemandBase.ExtStats() }

///////////
// tcowi //
///////////

func (wi *tcowi) do(lom *cluster.LOM, lri *lriterator) {
	objNameTo := wi.msg.ToName(lom.ObjName)
	buf, slab := lri.t.MMSA().Alloc()
	params := &cluster.CopyObjectParams{}
	{
		params.BckTo = wi.r.args.BckTo
		params.ObjNameTo = objNameTo
		params.DM = wi.r.dm
		params.Buf = buf
		params.DP = wi.r.args.DP
		params.DryRun = wi.msg.DryRun
	}
	size, err := lri.t.CopyObject(lom, params, false /*localOnly*/)
	slab.Free(buf)
	if err != nil {
		if cos.IsErrOOS(err) {
			what := fmt.Sprintf("%s(%q)", wi.r.Kind(), wi.r.ID())
			err = cmn.NewAbortedError(what, err.Error())
			// TODO -- FIXME: handle
		}
		return
	}
	wi.r.ObjectsInc()
	// TODO -- FIXME: Add precise post-transform byte count
	// (under ETL, sizes of transformed objects are unknown until after the transformation)
	if size == cos.ContentLengthUnknown {
		if err := lom.Load(false /*cacheit*/, false /*locked*/); err != nil {
			glog.Errorf("%s: %v", lom, err)
			size = 0
		} else {
			size = lom.SizeBytes()
		}
	}
	wi.r.BytesAdd(size)
}
