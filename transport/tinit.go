// Package transport provides streaming object-based transport over http for intra-cluster continuous
// intra-cluster communications (see README for details and usage example).
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package transport

import (
	"container/heap"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

// transport defaults
const (
	dfltBurstNum     = 32 // burst size (see: config.Transport.Burst)
	dfltTick         = time.Second
	dfltIdleTeardown = 4 * time.Second // (see config.Transport.IdleTeardown)
)

var (
	dfltMaxHdr int64 // memsys.PageSize or cluster-configurable (`config.Transport.MaxHeaderSize`)
	verbose    bool
)

func init() {
	nextSessionID.Store(100)
	handlers = make(map[string]*handler, 32)
	mu = &sync.RWMutex{}
}

func Init(st cos.StatsUpdater, config *cmn.Config) *StreamCollector {
	verbose = config.FastV(5 /*super-verbose*/, cos.SmoduleTransport)
	dfltMaxHdr = dfltSizeHeader
	if config.Transport.MaxHeaderSize > 0 {
		dfltMaxHdr = int64(config.Transport.MaxHeaderSize)
	}

	statsTracker = st
	// real stream collector
	gc = &collector{
		ctrlCh:  make(chan ctrl, 64),
		streams: make(map[string]*streamBase, 64),
		heap:    make([]*streamBase, 0, 64), // min-heap sorted by stream.time.ticks
	}
	gc.stopCh.Init()
	heap.Init(gc)

	sc = &StreamCollector{}
	return sc
}

func burst(config *cmn.Config) (burst int) {
	if burst = config.Transport.Burst; burst == 0 {
		burst = dfltBurstNum
	}
	if a := os.Getenv("AIS_STREAM_BURST_NUM"); a != "" {
		if burst64, err := strconv.ParseInt(a, 10, 0); err != nil {
			nlog.Errorln(err)
		} else {
			burst = int(burst64)
		}
	}
	return
}
