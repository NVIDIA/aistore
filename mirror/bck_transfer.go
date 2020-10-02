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
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xaction/registry"
)

// XactTransferBck transfers a bucket locally within the same cluster. If xact.dp is empty, transfer bck is just copy
// bck. If xact.dp is not empty, transfer bck applies specified transformation to each object.

type (
	transferBckProvider struct {
		xact *XactTransferBck

		t     cluster.Target
		uuid  string
		kind  string
		phase string
		args  *registry.TransferBckArgs
	}
	XactTransferBck struct {
		xactBckBase
		slab    *memsys.Slab
		bckFrom *cluster.Bck
		bckTo   *cluster.Bck
		dm      *bundle.DataMover
		dp      cluster.LomReaderProvider
		meta    *cmn.Bck2BckMsg
	}
	bckTransferJogger struct { // one per mountpath
		joggerBckBase
		parent *XactTransferBck
		buf    []byte
	}
)

func (e *transferBckProvider) New(args registry.XactArgs) registry.BucketEntry {
	return &transferBckProvider{
		t:     args.T,
		uuid:  args.UUID,
		kind:  e.kind,
		phase: args.Phase,
		args:  args.Custom.(*registry.TransferBckArgs),
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
func (e *transferBckProvider) PreRenewHook(previousEntry registry.BucketEntry) (keep bool, err error) {
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
func (e *transferBckProvider) PostRenewHook(_ registry.BucketEntry) {}

//
// public methods
//

// XactTransferBck transfers one bucket to another. If dp is not provided, transfer bucket just copies a bucket into
// another one. If dp is provided, bytes to save are taken from io.Reader from dp.Reader().
func NewXactTransferBck(id, kind string, bckFrom, bckTo *cluster.Bck, t cluster.Target, slab *memsys.Slab,
	dm *bundle.DataMover, dp cluster.LomReaderProvider, meta *cmn.Bck2BckMsg) *XactTransferBck {
	return &XactTransferBck{
		xactBckBase: *newXactBckBase(id, kind, bckTo.Bck, t),
		slab:        slab,
		bckFrom:     bckFrom,
		bckTo:       bckTo,
		dm:          dm,
		dp:          dp,
		meta:        meta,
	}
}

func (r *XactTransferBck) Run() (err error) {
	r.dm.SetXact(r)
	r.dm.Open()

	mpathCount := r.runJoggers()

	glog.Infoln(r.String(), r.bckFrom.Bck, "=>", r.bckTo.Bck)
	err = r.xactBckBase.waitDone(mpathCount)

	r.dm.Close()
	r.dm.UnregRecv()

	r.Finish(err)
	return
}

func (r *XactTransferBck) String() string {
	return fmt.Sprintf("%s <= %s", r.XactBase.String(), r.bckFrom)
}

//
// private methods
//

func (r *XactTransferBck) runJoggers() (mpathCount int) {
	var (
		availablePaths, _ = fs.Get()
		config            = cmn.GCO.Get()
	)
	mpathCount = len(availablePaths)
	r.xactBckBase.init(mpathCount)
	for _, mpathInfo := range availablePaths {
		bccJogger := newBCCJogger(r, mpathInfo, config)
		mpathLC := mpathInfo.MakePathCT(r.bckFrom.Bck, fs.ObjectType)
		r.mpathers[mpathLC] = bccJogger
		go bccJogger.jog()
	}
	return
}

//
// mpath bckTransferJogger - main
//

func newBCCJogger(parent *XactTransferBck, mpathInfo *fs.MountpathInfo, config *cmn.Config) *bckTransferJogger {
	j := &bckTransferJogger{
		joggerBckBase: joggerBckBase{
			parent:    &parent.xactBckBase,
			bck:       parent.bckFrom.Bck,
			mpathInfo: mpathInfo,
			config:    config,
			skipLoad:  true,
			stopCh:    cmn.NewStopCh(),
		},
		parent: parent,
	}
	j.joggerBckBase.callback = j.copyObject
	return j
}

func (j *bckTransferJogger) jog() {
	glog.Infof("jogger[%s/%s] started", j.mpathInfo, j.parent.bckFrom.Bck)
	j.buf = j.parent.slab.Alloc()
	j.joggerBckBase.jog()
	j.parent.slab.Free(j.buf)
}

func (j *bckTransferJogger) copyObject(lom *cluster.LOM) error {
	var (
		objNameTo = cmn.ObjNameFromBck2BckMsg(lom.ObjName, j.parent.meta)

		params = cluster.CopyObjectParams{
			BckTo:     j.parent.bckTo,
			ObjNameTo: objNameTo,
			Buf:       j.buf,
			DM:        j.parent.dm,
			DP:        j.parent.dp,
			DryRun:    j.parent.meta.DryRun,
		}

		// TODO: for dry-run, put object names in IC, so user can see exactly what is put where.
		copied, size, err = j.parent.Target().CopyObject(lom, params)
	)

	if copied {
		j.parent.ObjectsInc()
		j.parent.BytesAdd(size)
		j.num++

		if (j.num % throttleNumObjects) == 0 {
			if errstop := j.yieldTerm(); errstop != nil {
				return errstop
			}

			if cs := fs.GetCapStatus(); cs.Err != nil {
				what := fmt.Sprintf("%s(%q)", j.parent.Kind(), j.parent.ID())
				return cmn.NewAbortedErrorDetails(what, cs.Err.Error())
			}

			if (j.num % logNumProcessed) == 0 {
				glog.Infof("%s jogger[%s/%s] processed %d objects...", j.parent.Kind(), j.mpathInfo, j.parent.Bck(),
					j.num)
			}
		}
	}
	if cmn.IsErrOOS(err) {
		what := fmt.Sprintf("%s(%q)", j.parent.Kind(), j.parent.ID())
		return cmn.NewAbortedErrorDetails(what, err.Error())
	}
	return err
}
