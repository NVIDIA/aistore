// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

type (
	cpyFactory struct {
		xact  *XactTransferBck
		t     cluster.Target
		uuid  string
		kind  string
		phase string
		args  *xreg.TransferBckArgs
	}
	XactTransferBck struct {
		xaction.XactBckJog
		bckFrom *cluster.Bck
		bckTo   *cluster.Bck
		dm      *bundle.DataMover
		dp      cluster.LomReaderProvider
		meta    *cmn.Bck2BckMsg
	}
)

const etlBucketParallelCnt = 2

// interface guard
var (
	_ cluster.Xact    = (*XactTransferBck)(nil)
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

func (e *cpyFactory) New(args *xreg.XactArgs) xreg.BucketEntry {
	return &cpyFactory{
		t:    args.T,
		uuid: args.UUID,
		kind: e.kind,
		args: args.Custom.(*xreg.TransferBckArgs),
	}
}

func (e *cpyFactory) Start(_ cmn.Bck) error {
	slab, err := e.t.MMSA().GetSlab(memsys.MaxPageSlabSize)
	cos.AssertNoErr(err)
	e.xact = NewXactTransferBck(e.uuid, e.kind, e.args.BckFrom, e.args.BckTo, e.t, slab,
		e.args.DM, e.args.DP, e.args.Meta)
	return nil
}

func (e *cpyFactory) Kind() string      { return e.kind }
func (e *cpyFactory) Get() cluster.Xact { return e.xact }

func (e *cpyFactory) PreRenewHook(previousEntry xreg.BucketEntry) (keep bool, err error) {
	prev := previousEntry.(*cpyFactory)
	bckEq := prev.args.BckFrom.Equal(e.args.BckFrom, true /*same BID*/, true /* same backend */)
	if prev.phase == cmn.ActBegin && e.phase == cmn.ActCommit && bckEq {
		prev.phase = cmn.ActCommit // transition
		keep = true
		return
	}
	err = fmt.Errorf("%s(%s=>%s, phase %s): cannot %s(%s=>%s)",
		prev.xact, prev.args.BckFrom, prev.args.BckTo, prev.phase, e.phase, e.args.BckFrom, e.args.BckTo)
	return
}

func (*cpyFactory) PostRenewHook(_ xreg.BucketEntry) {}

/////////////////////
// XactTransferBck //
/////////////////////

// XactTransferBck transfers (copies, transforms) one bucket to another.
// If dp is nil we do plain copy. Otherwise, (transformed) bytes are received from the dp.Reader().
func NewXactTransferBck(id, kind string, bckFrom, bckTo *cluster.Bck, t cluster.Target, slab *memsys.Slab,
	dm *bundle.DataMover, dp cluster.LomReaderProvider, meta *cmn.Bck2BckMsg) *XactTransferBck {
	xact := &XactTransferBck{
		bckFrom: bckFrom,
		bckTo:   bckTo,
		dm:      dm,
		dp:      dp,
		meta:    meta,
	}
	parallel := 0
	// TODO: optimize parallelism with respect to disk bandwidth and transforming computation
	if kind == cmn.ActETLBck {
		parallel = etlBucketParallelCnt
	}

	xact.XactBckJog = *xaction.NewXactBckJog(id, kind, bckTo.Bck, &mpather.JoggerGroupOpts{
		Bck:      bckFrom.Bck,
		T:        t,
		CTs:      []string{fs.ObjectType},
		VisitObj: xact.copyObject,
		Slab:     slab,
		Throttle: true,
		Parallel: parallel,
		DoLoad:   mpather.Load,
	})

	return xact
}

func (r *XactTransferBck) Run() {
	r.dm.SetXact(r)
	r.dm.Open()

	r.XactBckJog.Run()
	glog.Infoln(r.String(), r.bckFrom.Bck, "=>", r.bckTo.Bck)
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

func (r *XactTransferBck) String() string {
	return fmt.Sprintf("%s <= %s", r.XactBase.String(), r.bckFrom)
}

func (r *XactTransferBck) copyObject(lom *cluster.LOM, buf []byte) (err error) {
	var size int64
	objNameTo := cmn.ObjNameFromBck2BckMsg(lom.ObjName, r.meta)
	params := allocCpObjParams()
	{
		params.BckTo = r.bckTo
		params.ObjNameTo = objNameTo
		params.Buf = buf
		params.DM = r.dm
		params.DP = r.dp
		params.DryRun = r.meta.DryRun
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
