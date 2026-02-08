// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"container/heap"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/memsys"
)

// internal tunables
const (
	dfltTickIdle = cmn.DfltTransportTick << 8 // (when there are no streams to _collect_)
)

const (
	dfltCollectLog  = 10 * time.Minute
	dfltCollectChan = 256

	iniCollectCap = 64
)

type global struct {
	tstats     cos.StatsUpdater // strict subset of stats.Tracker interface (the minimum required)
	mm         *memsys.MMSA
	preferIPv6 bool
}

var (
	g global
)

func Init(tstats cos.StatsUpdater, preferIPv6 bool) *StreamCollector {
	g.mm = memsys.PageMM()
	g.tstats = tstats
	g.preferIPv6 = preferIPv6

	nextSessionID.Store(100)
	for i := range numHmaps {
		hmaps[i] = make(hmap, 4)
	}

	// real stream collector
	gc = &collector{
		ctrlCh:  make(chan ctrl, dfltCollectChan),
		streams: make(map[int64]*base, iniCollectCap),
		heap:    make([]*base, 0, iniCollectCap), // min-heap sorted by stream.time.ticks
	}
	gc.stopCh.Init()
	heap.Init(gc)

	sc = &StreamCollector{}
	return sc
}
