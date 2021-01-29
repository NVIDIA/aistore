// Package mirror provides local mirroring and replica management
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package mirror

import (
	"fmt"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/mpather"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

// XactTransferBck transfers a bucket locally within the same cluster. If xact.dp is empty, transfer bck is just copy
// bck. If xact.dp is not empty, transfer bck applies specified transformation to each object.

// Try to balance between downsides of synchronous coping and too many goroutines and concurrent fs access.
var etlBucketParallelCnt = 2

type (
	transferBckProvider struct {
		xact *XactTransferBck

		t     cluster.Target
		uuid  string
		kind  string
		phase string
		args  *xreg.TransferBckArgs
	}
	XactTransferBck struct {
		xactBckBase
		bckFrom *cluster.Bck
		bckTo   *cluster.Bck
		dm      *bundle.DataMover
		dp      cluster.LomReaderProvider
		meta    *cmn.Bck2BckMsg
	}
)

// interface guard
var _ cluster.Xact = (*XactTransferBck)(nil)

func (e *transferBckProvider) New(args xreg.XactArgs) xreg.BucketEntry {
	return &transferBckProvider{
		t:     args.T,
		uuid:  args.UUID,
		kind:  e.kind,
		phase: args.Phase,
		args:  args.Custom.(*xreg.TransferBckArgs),
	}
}

func (e *transferBckProvider) Start(_ cmn.Bck) error {
	slab, err := e.t.MMSA().GetSlab(memsys.MaxPageSlabSize)
	cmn.AssertNoErr(err)
	e.xact = NewXactTransferBck(e.uuid, e.kind, e.args.BckFrom, e.args.BckTo, e.t, slab, e.args.DM, e.args.DP, e.args.Meta)
	return nil
}
func (e *transferBckProvider) Kind() string      { return e.kind }
func (e *transferBckProvider) Get() cluster.Xact { return e.xact }
func (e *transferBckProvider) PreRenewHook(previousEntry xreg.BucketEntry) (keep bool, err error) {
	prev := previousEntry.(*transferBckProvider)
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
func (e *transferBckProvider) PostRenewHook(_ xreg.BucketEntry) {}

//
// public methods
//

// XactTransferBck transfers one bucket to another. If dp is not provided, transfer bucket just copies a bucket into
// another one. If dp is provided, bytes to save are taken from io.Reader from dp.Reader().
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
	// NOTE: Do not compete for disk when doing copy bucket. However, ETL `copyObject` callback does a transformation,
	// so more time is spend on computation.
	if kind == cmn.ActETLBck {
		parallel = etlBucketParallelCnt
	}

	xact.xactBckBase = *newXactBckBase(id, kind, bckTo.Bck, &mpather.JoggerGroupOpts{
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

	r.xactBckBase.runJoggers()
	glog.Infoln(r.String(), r.bckFrom.Bck, "=>", r.bckTo.Bck)
	err := r.xactBckBase.waitDone()
	r.dm.Close(err)
	r.dm.UnregRecv()

	r.Finish(err)
}

func (r *XactTransferBck) String() string {
	return fmt.Sprintf("%s <= %s", r.XactBase.String(), r.bckFrom)
}

//
// private methods
//

func (r *XactTransferBck) copyObject(lom *cluster.LOM, buf []byte) error {
	var (
		objNameTo = cmn.ObjNameFromBck2BckMsg(lom.ObjName, r.meta)
		params    = cluster.CopyObjectParams{
			BckTo:     r.bckTo,
			ObjNameTo: objNameTo,
			Buf:       buf,
			DM:        r.dm,
			DP:        r.dp,
			DryRun:    r.meta.DryRun,
		}
	)

	// TODO: If dry-run show to-be-copied objects.
	copied, size, err := r.Target().CopyObject(lom, params, false /*localOnly*/)
	if err != nil {
		if cmn.IsErrOOS(err) {
			what := fmt.Sprintf("%s(%q)", r.Kind(), r.ID())
			return cmn.NewAbortedErrorDetails(what, err.Error())
		}
		return err
	}

	if copied {
		r.ObjectsInc()
		r.BytesAdd(size)
	}

	if cs := fs.GetCapStatus(); cs.Err != nil {
		what := fmt.Sprintf("%s(%q)", r.Kind(), r.ID())
		return cmn.NewAbortedErrorDetails(what, cs.Err.Error())
	}

	return nil
}
