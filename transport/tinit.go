// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"
)

// internal tunables
const (
	dfltTickIdle = 256 * cmn.DfltTransportTick // ~4m (when there are no streams to _collect_)
)

const (
	dfltCollectLog = 10 * time.Minute

	iniCollectCap = 64

	dfltCollectCap     = 256 // initial mailbox capacity
	warnCollectPending = dfltCollectCap << 2
	maxCollectRecycle  = warnCollectPending
)

type global struct {
	tstats     cos.StatsUpdater // strict subset of stats.Tracker interface (the minimum required)
	mm         *memsys.MMSA
	sign       SignFn
	verify     VerifyFn
	preferIPv6 bool
}

var (
	g global
)

func Init(tstats cos.StatsUpdater, sign SignFn, verify VerifyFn, preferIPv6 bool) *StreamCollector {
	g.mm = memsys.PageMM()
	g.tstats = tstats
	g.sign = sign
	g.verify = verify
	g.preferIPv6 = preferIPv6

	nextSessionID.Store(100)
	for i := range numHmaps {
		hmaps[i] = make(hmap, 4)
	}

	// real stream collector
	gc = &collector{
		workCh:  make(chan struct{}, 1),
		pending: make([]ctrl, 0, dfltCollectCap),
		free:    make([]ctrl, 0, dfltCollectCap),
		streams: make(map[int64]*base, iniCollectCap),
	}
	gc.stopCh.Init()

	sc = &StreamCollector{}
	return sc
}
