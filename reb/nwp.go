// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"io"
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact/xs"
)

// TODO:
// - add chanFull log
// - extract/reuse across TCB and global rebalance, maybe more

type (
	rebWorker struct {
		m     *Reb
		rargs *rebArgs
	}
	nwp struct {
		workCh  chan *core.LOM
		workers []rebWorker
		wg      sync.WaitGroup
	}
)

func (reb *Reb) runNwp(rargs *rebArgs) {
	numWorkers, err := xs.TuneNumWorkers(rargs.xreb.Name(), xs.NwpDflt, len(rargs.avail))
	if err != nil {
		nlog.Errorln("Warning:", err)
		return
	}
	if numWorkers == xs.NwpNone || numWorkers < len(rargs.avail) {
		return
	}

	// init and run all
	nwp := &nwp{}
	rargs.nwp = nwp

	chsize := cos.ClampInt(numWorkers*xs.NwpBurstMult, rargs.config.Rebalance.Burst, xs.NwpBurstMax)
	nwp.workCh = make(chan *core.LOM, chsize)

	nwp.workers = make([]rebWorker, numWorkers)
	for i := range numWorkers {
		worker := rebWorker{m: reb, rargs: rargs}
		nwp.workers[i] = worker
		nwp.wg.Add(1)
		go worker.run()
	}

	nlog.Infoln(rargs.logHdr, "nwp workers:", numWorkers)
}

func (w *rebWorker) run() {
	var (
		rargs = w.rargs
		xreb  = rargs.xreb
	)
	for lom := range rargs.nwp.workCh {
		if xreb.IsAborted() {
			core.FreeLOM(lom)
			continue // drain
		}
		w.do(lom)
	}
	rargs.nwp.wg.Done()
}

func (w *rebWorker) do(lom *core.LOM) {
	var (
		m     = w.m
		rargs = w.rargs
		xreb  = rargs.xreb
	)
	tsi, err := rargs.smap.HrwHash2T(lom.Digest())
	if err != nil {
		core.FreeLOM(lom)
		xreb.AddErr(err)
		return
	}

	// _getReader: rlock, load, checksum, new roc
	roc, err := _getReader(lom)
	if err != nil {
		core.FreeLOM(lom)
		if err != cmn.ErrSkip {
			xreb.AddErr(err)
		}
		return
	}

	// transmit
	m.addLomAck(lom)
	if err := w.doSend(lom, tsi, roc); err != nil {
		m.cleanupLomAck(lom)
		if !xreb.IsAborted() {
			xreb.AddErr(err)
		}
	}
}

func (w *rebWorker) doSend(lom *core.LOM, tsi *meta.Snode, roc cos.ReadOpenCloser) error {
	debug.Assert(tsi.ID() != core.T.SID(), "must be checked by jogger")
	var (
		ack    = regularAck{rebID: w.m.rebID(), daemonID: core.T.SID()}
		o      = transport.AllocSend()
		opaque = ack.NewPack()
	)
	debug.Assert(ack.rebID != 0)
	o.Hdr.Bck.Copy(lom.Bucket())
	o.Hdr.ObjName = lom.ObjName
	o.Hdr.Opaque = opaque
	o.Hdr.ObjAttrs.CopyFrom(lom.ObjAttrs(), false /*skip cksum*/)
	o.SentCB, o.CmplArg = w.objSentCallback, lom
	return w.m.dm.Send(o, roc, tsi)
}

func (w *rebWorker) objSentCallback(hdr *transport.ObjHdr, _ io.ReadCloser, arg any, err error) {
	if err == nil {
		w.rargs.xreb.OutObjsAdd(1, hdr.ObjAttrs.Size)
		return
	}
	if cmn.Rom.V(4, cos.ModReb) || !cos.IsErrRetriableConn(err) {
		switch {
		case bundle.IsErrDestinationMissing(err):
			nlog.Errorf("%s: %v, %s", w.rargs.xreb.Name(), err, w.rargs.smap.StringEx())
		case cmn.IsErrStreamTerminated(err):
			w.rargs.xreb.Abort(err)
			nlog.Errorln("stream term-ed: [", err, w.rargs.xreb.Name(), "]")
		default:
			lom, ok := arg.(*core.LOM)
			debug.Assert(ok)
			nlog.Errorf("%s: %s failed to send %s: %v (%T)", core.T, w.rargs.xreb.Name(), lom, err, err)
		}
	}
}
