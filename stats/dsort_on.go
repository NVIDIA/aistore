//go:build dsort

// Package stats provides methods and functionality to register, track, log,
// and export metrics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import "github.com/NVIDIA/aistore/core/meta"

const (
	DsortCreationReqCount    = "dsort.creation.req.n"
	DsortCreationRespCount   = "dsort.creation.resp.n"
	DsortCreationRespLatency = "dsort.creation.resp.ns"
	DsortExtractShardDskCnt  = "dsort.extract.shard.dsk.n"
	DsortExtractShardMemCnt  = "dsort.extract.shard.mem.n"
	DsortExtractShardSize    = "dsort.extract.shard.size" // uncompressed
)

func (r *Trunner) regDsort(snode *meta.Snode) {
	extra := &Extra{
		Help: "dsort: see https://github.com/NVIDIA/aistore/blob/main/docs/dsort.md#metrics",
	}
	r.reg(snode, DsortCreationReqCount, KindCounter, extra)
	r.reg(snode, DsortCreationRespCount, KindCounter, extra)
	r.reg(snode, DsortCreationRespLatency, KindLatency, extra)
	r.reg(snode, DsortExtractShardDskCnt, KindCounter, extra)
	r.reg(snode, DsortExtractShardMemCnt, KindCounter, extra)
	r.reg(snode, DsortExtractShardSize, KindSize, extra)
}
