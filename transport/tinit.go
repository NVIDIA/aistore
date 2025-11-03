// Package transport provides long-lived http/tcp connections for intra-cluster communications
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"container/heap"
	"os"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/memsys"
)

// transport defaults
const (
	dfltTick         = time.Second
	dfltTickIdle     = dfltTick << 8   // (when there are no streams to _collect_)
	dfltIdleTeardown = 4 * time.Second // (see config.Transport.IdleTeardown)
)

const (
	dfltCollectLog  = 10 * time.Minute
	dfltCollectChan = 256

	iniCollectCap = 64
)

type global struct {
	tstats cos.StatsUpdater // strict subset of stats.Tracker interface (the minimum required)
	mm     *memsys.MMSA
}

var (
	g global
)

func Init(tstats cos.StatsUpdater) *StreamCollector {
	g.mm = memsys.PageMM()
	g.tstats = tstats

	nextSessionID.Store(100)
	for i := range numHmaps {
		hmaps[i] = make(hmap, 4)
	}

	// real stream collector
	gc = &collector{
		ctrlCh:  make(chan ctrl, dfltCollectChan),
		streams: make(map[int64]*streamBase, iniCollectCap),
		heap:    make([]*streamBase, 0, iniCollectCap), // min-heap sorted by stream.time.ticks
	}
	gc.stopCh.Init()
	heap.Init(gc)

	sc = &StreamCollector{}
	return sc
}

func burst(extra *Extra) (burst int) {
	if extra.ChanBurst > 0 {
		debug.Assert(extra.ChanBurst <= cmn.MaxTransportBurst, extra.ChanBurst)
		return min(extra.ChanBurst, cmn.MaxTransportBurst)
	}
	if burst = extra.Config.Transport.Burst; burst == 0 {
		burst = cmn.DfltTransportBurst
	}

	// (feat)
	if a := os.Getenv("AIS_STREAM_BURST_NUM"); a != "" {
		if burst64, err := strconv.ParseInt(a, 10, 0); err != nil {
			nlog.Errorln(err)
		} else {
			burst = int(burst64)
		}
	}
	return
}
