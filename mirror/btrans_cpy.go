// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

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
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xreg"
)

const (
	doneSendingOpcode = 27182
)

type (
	cpyFactory struct {
		xreg.RenewBase
		xact  *XactTransCpyBck
		kind  string
		phase string // (see "transition")
		args  *xreg.TransCpyBckArgs
	}
	XactTransCpyBck struct {
		xaction.XactBckJog
		t    cluster.Target
		dm   *bundle.DataMover
		args xreg.TransCpyBckArgs
		// starting up
		wg sync.WaitGroup
		// finishing
		refc atomic.Int32
	}
)

const etlBucketParallelCnt = 2

// interface guard
var (
	_ cluster.Xact   = (*XactTransCpyBck)(nil)
	_ xreg.Renewable = (*cpyFactory)(nil)
)

///////////////////////////////////
// cluster.CopyObjectParams pool //
///////////////////////////////////

var (
	cpObjPool sync.Pool
	cpObj0    cluster.CopyObjectParams
)

func allocCpObjParams() (a *cluster.CopyObjectParams) {
	if v := cpObjPool.Get(); v != nil {
		a = v.(*cluster.CopyObjectParams)
		return
	}
	return &cluster.CopyObjectParams{}
}

func freeCpObjParams(a *cluster.CopyObjectParams) {
	*a = cpObj0
	cpObjPool.Put(a)
}

////////////////
// cpyFactory //
////////////////

func (e *cpyFactory) New(args xreg.Args, bck *cluster.Bck) xreg.Renewable {
	custom := args.Custom.(*xreg.TransCpyBckArgs)
	p := &cpyFactory{RenewBase: xreg.RenewBase{Args: args, Bck: bck}, kind: e.kind, phase: custom.Phase, args: custom}
	return p
}

func (e *cpyFactory) Start() error {
	var (
		config    = cmn.GCO.Get()
		sizePDU   int32
		slab, err = e.T.MMSA().GetSlab(memsys.MaxPageSlabSize)
	)
	cos.AssertNoErr(err)
	e.xact = newXactTransCpyBck(e, slab)
	if e.kind == cmn.ActETLBck {
		sizePDU = memsys.DefaultBufSize
	}

	// TODO -- FIXME: revisit
	smap := e.T.Sowner().Get()
	e.xact.refc.Store(int32(smap.CountTargets() - 1))
	e.xact.wg.Add(1)

	// TODO: using rebalance config for a DM that copies objects.
	return e.newDM(&config.Rebalance, e.UUID(), sizePDU)
}

func (e *cpyFactory) newDM(rebcfg *cmn.RebalanceConf, uuid string, sizePDU int32) error {
	const trname = "transcpy" // copy&transform transport endpoint prefix
	dmExtra := bundle.Extra{
		RecvAck:     nil,                    // NOTE: no ACKs
		Compression: rebcfg.Compression,     // TODO: define separately
		Multiplier:  int(rebcfg.Multiplier), // ditto
	}
	dmExtra.SizePDU = sizePDU
	dm, err := bundle.NewDataMover(e.T, trname+"_"+uuid, e.xact.recv, cluster.RegularPut, dmExtra)
	if err != nil {
		return err
	}
	if err := dm.RegRecv(); err != nil {
		return err
	}
	e.xact.dm = dm
	return nil
}

func (e *cpyFactory) Kind() string      { return e.kind }
func (e *cpyFactory) Get() cluster.Xact { return e.xact }

func (e *cpyFactory) WhenPrevIsRunning(prevEntry xreg.Renewable) (wpr xreg.WPR, err error) {
	prev := prevEntry.(*cpyFactory)
	if e.UUID() != prev.UUID() {
		err = fmt.Errorf("%s is currently running - not starting new %q", prevEntry.Get(), e.Str(e.Kind()))
		return
	}
	bckEq := prev.args.BckFrom.Equal(e.args.BckFrom, true /*same BID*/, true /* same backend */)
	debug.Assert(bckEq)
	debug.Assert(prev.phase == cmn.ActBegin && e.phase == cmn.ActCommit)
	prev.args.Phase = cmn.ActCommit // transition
	wpr = xreg.WprUse
	return
}

/////////////////////
// XactTransCpyBck //
/////////////////////

func (r *XactTransCpyBck) Args() *xreg.TransCpyBckArgs { return &r.args }

func (r *XactTransCpyBck) String() string {
	return fmt.Sprintf("%s <= %s", r.XactBase.String(), r.args.BckFrom)
}

// limited pre-run abort
func (r *XactTransCpyBck) TxnAbort() {
	debug.Assert(!r.dm.IsOpen())
	r.dm.UnregRecv()
	r.XactBase.Finish(cmn.NewAbortedError(r.String()))
}

//
// XactTransCpyBck copies one bucket _into_ another with or without transformation.
// args.DP.Reader() is the reader to receive transformed bytes; when nil we do a plain bucket copy.
//
func newXactTransCpyBck(e *cpyFactory, slab *memsys.Slab) (r *XactTransCpyBck) {
	var parallel int
	r = &XactTransCpyBck{t: e.T, args: *e.args}
	if e.kind == cmn.ActETLBck {
		parallel = etlBucketParallelCnt // TODO: optimize with respect to disk bw and transforming computation
	}
	mpopts := &mpather.JoggerGroupOpts{
		Bck:      e.args.BckFrom.Bck,
		T:        e.T,
		CTs:      []string{fs.ObjectType},
		VisitObj: r.copyObject,
		Slab:     slab,
		Parallel: parallel,
		DoLoad:   mpather.Load,
		Throttle: true,
	}
	r.XactBckJog.Init(e.UUID(), e.kind, e.args.BckTo, mpopts)
	return
}

func (r *XactTransCpyBck) WaitRunning() { r.wg.Wait() }

func (r *XactTransCpyBck) Run() {
	r.dm.SetXact(r)
	r.dm.Open()

	r.wg.Done()

	r.XactBckJog.Run()
	glog.Infoln(r.String(), r.args.BckFrom.Bck, "=>", r.args.BckTo.Bck)

	err := r.XactBckJog.Wait()

	o := transport.AllocSend()
	o.Hdr.Opcode = doneSendingOpcode
	r.dm.Bcast(o)

	// NOTE: ref-counted quiescence, fairly short (optimal) waiting
	config := cmn.GCO.Get()
	optTime, maxTime := config.Timeout.MaxKeepalive.D(), config.Timeout.SendFile.D()/2
	q := r.Quiesce(optTime, func(tot time.Duration) cluster.QuiRes { return xaction.RefcntQuiCB(&r.refc, maxTime, tot) })
	if err == nil {
		if q == cluster.QuiAborted {
			err = cmn.NewAbortedError(r.String())
		} else if q == cluster.QuiTimeout {
			err = fmt.Errorf("%s: %v", r, cmn.ErrQuiesceTimeout)
		}
	}

	// close
	r.dm.Close(err)
	r.dm.UnregRecv()

	r.Finish(err)
}

func (r *XactTransCpyBck) copyObject(lom *cluster.LOM, buf []byte) (err error) {
	var size int64
	objNameTo := cmn.ObjNameFromBck2BckMsg(lom.ObjName, r.args.Msg)
	params := allocCpObjParams()
	{
		params.BckTo = r.args.BckTo
		params.ObjNameTo = objNameTo
		params.Buf = buf
		params.DM = r.dm
		params.DP = r.args.DP
		params.DryRun = r.args.Msg.DryRun
	}
	size, err = r.Target().CopyObject(lom, params, false /*localOnly*/)
	if err != nil {
		if cos.IsErrOOS(err) {
			what := fmt.Sprintf("%s(%q)", r.Kind(), r.ID())
			err = cmn.NewAbortedError(what, err.Error())
		}
		goto ret
	}
	r.ObjectsInc()
	r.BytesAdd(size)
	// keep checking remaining capacity
	if cs := fs.GetCapStatus(); cs.Err != nil {
		what := fmt.Sprintf("%s(%q)", r.Kind(), r.ID())
		err = cmn.NewAbortedError(what, cs.Err.Error())
	}
ret:
	freeCpObjParams(params)
	return
}

func (r *XactTransCpyBck) recv(hdr transport.ObjHdr, objReader io.Reader, err error) {
	defer transport.FreeRecv(objReader)
	if err != nil && !cos.IsEOF(err) {
		glog.Error(err)
		return
	}
	// NOTE: best-effort via ref-counting
	if hdr.Opcode == doneSendingOpcode {
		refc := r.refc.Dec()
		debug.Assert(refc >= 0)
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
		// finalized in case of destination is Cloud.
		RecvType: cluster.RegularPut,
		Cksum:    hdr.ObjAttrs.Cksum,
		Started:  time.Now(),
	}
	if err := r.t.PutObject(lom, params); err != nil {
		glog.Error(err)
	}
}
