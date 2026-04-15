// Package reb provides global cluster-wide rebalance upon adding/removing storage nodes.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"sync"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact"
)

// TODO:
// - consider workCh <- core.LIF

type (
	rebWorker struct {
		m     *Reb
		rargs *rargs
	}
	// work item
	wi struct {
		lom *core.LOM
		tsi *meta.Snode
	}
	// num-workers parallelism
	nwp struct {
		workCh   chan wi
		workers  []rebWorker
		wg       sync.WaitGroup
		chanFull cos.ChanFull
	}
)

func (reb *Reb) runNwp(rargs *rargs) {
	numWorkers, err := xact.TuneNumWorkers(rargs.xreb.Name(), xact.NwpDflt, len(rargs.avail))
	if err != nil {
		nlog.Errorln("Warning:", err)
		return
	}
	if numWorkers == xact.NwpNone || numWorkers < len(rargs.avail) {
		return
	}

	// init and run all
	nwp := &nwp{}
	rargs.nwp = nwp

	chsize := cos.ClampInt(numWorkers*xact.NwpBurstMult, rargs.config.Rebalance.Burst, xact.NwpBurstMax)
	nwp.workCh = make(chan wi, chsize)

	nwp.workers = make([]rebWorker, numWorkers)
	for i := range numWorkers {
		worker := rebWorker{m: reb, rargs: rargs}
		nwp.workers[i] = worker
		nwp.wg.Add(1)
		go worker.run()
	}

	nlog.Infoln(rargs.logHdr, "nwp workers:", numWorkers)
}

func (worker *rebWorker) run() {
	var (
		rargs = worker.rargs
		xreb  = rargs.xreb
		nwp   = rargs.nwp
	)
	for wi := range nwp.workCh {
		if xreb.IsAborted() {
			core.FreeLOM(wi.lom)
			continue // drain
		}
		worker.do(wi)
	}
	nwp.wg.Done()
}

func (worker *rebWorker) do(wi wi) {
	var (
		m     = worker.m
		rargs = worker.rargs
		xreb  = rargs.xreb
	)
	// _getReader: rlock, load, checksum, new roc
	roc, err := getROC(wi.lom)
	if err != nil {
		core.FreeLOM(wi.lom)
		if err != cmn.ErrSkip {
			xreb.AddErr(err)
		}
		return
	}

	// transmit
	m.addLomAck(wi.lom)
	if err := rargs.doSend(wi.lom, wi.tsi, roc); err != nil {
		m.cleanupLomAck(wi.lom)
		xreb.Abort(err) // NOTE: failure to send == abort
	}
}
