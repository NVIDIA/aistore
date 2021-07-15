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
	"github.com/NVIDIA/aistore/xaction/xreg"
)

type (
	cpyFactory struct {
		xact  *XactTransCpyBck
		t     cluster.Target
		uuid  string
		kind  string
		phase string // (see "transition")
		args  *xreg.TransCpyBckArgs
	}
	XactTransCpyBck struct {
		xaction.XactBckJog
		t    cluster.Target
		dm   *bundle.DataMover
		args xreg.TransCpyBckArgs
	}
)

const etlBucketParallelCnt = 2

// interface guard
var (
	_ cluster.Xact    = (*XactTransCpyBck)(nil)
	_ xreg.BckFactory = (*cpyFactory)(nil)
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

func (e *cpyFactory) New(args xreg.Args) xreg.BucketEntry {
	custom := args.Custom.(*xreg.TransCpyBckArgs)
	return &cpyFactory{t: args.T, uuid: args.UUID, kind: e.kind, phase: custom.Phase, args: custom}
}

func (e *cpyFactory) Start(cmn.Bck) error {
	var (
		config    = cmn.GCO.Get()
		sizePDU   int32
		slab, err = e.t.MMSA().GetSlab(memsys.MaxPageSlabSize)
	)
	cos.AssertNoErr(err)
	e.xact = newXactTransCpyBck(e, slab)
	if e.kind == cmn.ActETLBck {
		sizePDU = memsys.DefaultBufSize
	}

	// TODO: using rebalance config for a DM that copies objects.
	return e.newDM(&config.Rebalance, e.uuid, sizePDU)
}

func (e *cpyFactory) newDM(rebcfg *cmn.RebalanceConf, uuid string, sizePDU int32) error {
	const trname = "transcpy" // copy&transform transport endpoint prefix
	dmExtra := bundle.Extra{
		RecvAck:     nil,                    // NOTE: no ACKs
		Compression: rebcfg.Compression,     // TODO: define separately
		Multiplier:  int(rebcfg.Multiplier), // ditto
	}
	dmExtra.SizePDU = sizePDU
	dm, err := bundle.NewDataMover(e.t, trname+"_"+uuid, e.xact.recv, cluster.RegularPut, dmExtra)
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

func (e *cpyFactory) PreRenewHook(previousEntry xreg.BucketEntry) (keep bool, err error) {
	prev := previousEntry.(*cpyFactory)
	if e.uuid != prev.uuid {
		err = fmt.Errorf("%s(%+v) != %s(%+v)", e.uuid, e.args, prev.uuid, prev.args)
		return
	}
	bckEq := prev.args.BckFrom.Equal(e.args.BckFrom, true /*same BID*/, true /* same backend */)
	debug.Assert(bckEq)
	debug.Assert(prev.phase == cmn.ActBegin && e.phase == cmn.ActCommit)
	prev.args.Phase = cmn.ActCommit // transition
	keep = true
	return
}

func (*cpyFactory) PostRenewHook(_ xreg.BucketEntry) {}

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
	r = &XactTransCpyBck{t: e.t, args: *e.args}
	if e.kind == cmn.ActETLBck {
		parallel = etlBucketParallelCnt // TODO: optimize with respect to disk bw and transforming computation
	}
	mpopts := &mpather.JoggerGroupOpts{
		Bck:      e.args.BckFrom.Bck,
		T:        e.t,
		CTs:      []string{fs.ObjectType},
		VisitObj: r.copyObject,
		Slab:     slab,
		Parallel: parallel,
		DoLoad:   mpather.Load,
		Throttle: true,
	}
	r.XactBckJog.Init(e.uuid, e.kind, e.args.BckTo.Bck, mpopts)
	return
}

func (r *XactTransCpyBck) Run() {
	r.dm.SetXact(r)
	r.dm.Open()

	r.XactBckJog.Run()
	glog.Infoln(r.String(), r.args.BckFrom.Bck, "=>", r.args.BckTo.Bck)
	err := r.XactBckJog.Wait()
	config := cmn.GCO.Get()
	if q := r.dm.Quiesce(config.Timeout.CplaneOperation.D()); q == cluster.QuiAborted {
		if err == nil {
			err = cmn.NewAbortedError(r.String())
		}
	}
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
