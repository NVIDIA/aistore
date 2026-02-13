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
	"github.com/NVIDIA/aistore/xact/xs"
)

// TODO:
// - add chanFull log
// - extract/reuse across TCB and global rebalance, maybe more

type (
	rebWorker struct {
		m     *Reb
		rargs *rargs
	}
	nwp struct {
		workCh  chan *core.LOM
		workers []rebWorker
		wg      sync.WaitGroup
	}
)

func (reb *Reb) runNwp(rargs *rargs) {
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
	if err := w.rargs.doSend(lom, tsi, roc); err != nil {
		m.cleanupLomAck(lom)
		if !xreb.IsAborted() {
			xreb.AddErr(err)
		}
	}
}
