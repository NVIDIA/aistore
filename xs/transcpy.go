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
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xreg"
)

type (
	transCopyObjsFactory struct {
		xreg.RenewBase
		xact *XactTransCopyObjs
		kind string
		msg  *xreg.TransCpyObjsArgs
	}
	XactTransCopyObjs struct {
		xaction.XactBase
		lriterator
		dm   *bundle.DataMover
		args *xreg.TransCpyObjsArgs
		// starting up
		wg sync.WaitGroup
		// finishing
		refc atomic.Int32
	}
)

// interface guard
var (
	_ cluster.Xact   = (*XactTransCopyObjs)(nil)
	_ xreg.Renewable = (*transCopyObjsFactory)(nil)
)

////////////////////////////
// transform/copy objects //
////////////////////////////

func (tco *transCopyObjsFactory) New(args xreg.Args, fromBck *cluster.Bck) xreg.Renewable {
	msg := args.Custom.(*xreg.TransCpyObjsArgs)
	debug.Assert(!msg.IsList() || !msg.HasTemplate())
	np := &transCopyObjsFactory{RenewBase: xreg.RenewBase{Args: args, Bck: fromBck}, kind: tco.kind, msg: msg}
	return np
}

func (tco *transCopyObjsFactory) Start() error {
	var (
		config  = cmn.GCO.Get()
		sizePDU int32
	)
	tco.xact = newTransCopyObjs(&tco.Args, tco.kind, tco.Bck, tco.msg)

	if tco.kind == cmn.ActETLBck {
		sizePDU = memsys.DefaultBufSize
	}

	// TODO -- FIXME: revisit
	smap := tco.T.Sowner().Get()
	tco.xact.refc.Store(int32(smap.CountTargets() - 1))
	tco.xact.wg.Add(1)

	return tco.newDM(&config.Rebalance, tco.UUID(), sizePDU)
}

func (tco *transCopyObjsFactory) newDM(rebcfg *cmn.RebalanceConf, uuid string, sizePDU int32) error {
	const trname = "transcpy" // copy&transform transport endpoint prefix
	dmExtra := bundle.Extra{
		RecvAck:     nil,                    // NOTE: no ACKs
		Compression: rebcfg.Compression,     // TODO: define separately
		Multiplier:  int(rebcfg.Multiplier), // ditto
	}
	dmExtra.SizePDU = sizePDU
	dm, err := bundle.NewDataMover(tco.T, trname+"_"+uuid, tco.xact.recv, cluster.RegularPut, dmExtra)
	if err != nil {
		return err
	}
	if err := dm.RegRecv(); err != nil {
		return err
	}
	tco.xact.dm = dm
	return nil
}

func (tco *transCopyObjsFactory) Kind() string      { return tco.kind }
func (tco *transCopyObjsFactory) Get() cluster.Xact { return tco.xact }
func (*transCopyObjsFactory) WhenPrevIsRunning(xreg.Renewable) (xreg.WPR, error) {
	return xreg.WprKeepAndStartNew, nil
}

func newTransCopyObjs(xargs *xreg.Args, kind string, bck *cluster.Bck, msg *xreg.TransCpyObjsArgs) (tco *XactTransCopyObjs) {
	tco = &XactTransCopyObjs{args: msg}
	tco.lriterator.init(tco, xargs.T, &msg.ListRangeMsg, true /*freeLOM*/)
	tco.lriterator.ignoreBackendErr = !msg.IsList() // NOTE: list defaults to aborting on errors other than non-existence
	tco.InitBase(xargs.UUID, kind, bck)
	return
}

func (r *XactTransCopyObjs) Args() *xreg.TransCpyObjsArgs { return r.args }

func (r *XactTransCopyObjs) recv(hdr transport.ObjHdr, objReader io.Reader, err error) {
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

func (r *XactTransCopyObjs) Run(*sync.WaitGroup) {
	var (
		err  error
		smap = r.t.Sowner().Get()
	)
	r.dm.SetXact(r)
	r.dm.Open()

	r.wg.Done()

	if r.msg.IsList() {
		err = r.iterateList(r, smap)
	} else {
		err = r.iterateRange(r, smap)
	}

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

func (r *XactTransCopyObjs) do(lom *cluster.LOM, lri *lriterator) (err error) {
	var size int64
	objNameTo := cmn.ObjNameFromBck2BckMsg(lom.ObjName, r.args.Msg)
	buf, slab := lri.t.MMSA().Alloc()
	params := &cluster.CopyObjectParams{}
	{
		params.BckTo = r.args.BckTo
		params.ObjNameTo = objNameTo
		params.DM = r.dm
		params.Buf = buf
		params.DP = r.args.DP
		params.DryRun = r.args.Msg.DryRun
	}
	size, err = lri.t.CopyObject(lom, params, false /*localOnly*/)
	slab.Free(buf)
	if err != nil {
		if cos.IsErrOOS(err) {
			what := fmt.Sprintf("%s(%q)", r.Kind(), r.ID())
			err = cmn.NewAbortedError(what, err.Error())
		}
		return
	}
	r.ObjectsInc()
	r.BytesAdd(size)
	return
}

func (r *XactTransCopyObjs) String() string {
	return fmt.Sprintf("%s <= %s", r.XactBase.String(), r.args.BckFrom)
}

// limited pre-run abort
func (r *XactTransCopyObjs) TxnAbort() {
	err := cmn.NewAbortedError(r.String())
	if r.dm.IsOpen() {
		r.dm.Close(err)
	}
	r.dm.UnregRecv()
	r.XactBase.Finish(err)
}
