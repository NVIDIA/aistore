// Package stats keeps track of all the different statistics collected by the report
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// This is intended to summarize primitives within a recipe
type RecipeStats struct {
	// Report package assumes responsibility for filling these
	RecipeName string    `json:"recipe_name"`
	RecipeNum  int       `json:"recipe_num"`
	OpType     string    `json:"type"`
	BeginTime  time.Time `json:"begin_time"`
	EndTime    time.Time `json:"end_time"`

	latencyTotal time.Duration
	minLatency   time.Duration
	maxLatency   time.Duration
	requestCount int64
	totalSize    int64 // bytes
	errorsCount  int64

	numFatal int
}

func (rs *RecipeStats) Add(p *PrimitiveStat) {
	if p.Fatal {
		rs.numFatal++
		return
	}
	rs.latencyTotal += time.Duration(p.RequestCount * int64(p.Latency))
	if p.LatencyMin > 0 && (p.LatencyMin < rs.minLatency || rs.minLatency == 0) {
		rs.minLatency = p.LatencyMin
	}
	rs.maxLatency = cos.MaxDuration(rs.maxLatency, p.LatencyMax)

	rs.requestCount += p.RequestCount
	rs.totalSize += p.TotalSize
	rs.errorsCount += p.ErrorsCount
}

func (rs *RecipeStats) HasData() bool {
	return rs.requestCount > 0
}

func (RecipeStats) getHeadingsText() map[string]string {
	return map[string]string{
		"beginTime": "Begin (excel timestamp)",
		"endTime":   "End (excel timestamp)",
		"recName":   "Recipe Name",
		"recNum":    "Recipe Num",
		"opType":    "Operation Type",

		"minLatency": "Min Latency (ms)",
		"avgLatency": "Avg Latency (ms)",
		"maxLatency": "Max Latency (ms)",
		"throughput": "Throughput (B/s)",

		"totSize":    "Total Size (B)",
		"reqCount":   "Request Count",
		"errCount":   "Error Count",
		"fatalCount": "Fatal Count",
	}
}

func (RecipeStats) getHeadingsOrder() []string {
	return []string{
		"beginTime", "endTime", "recName", "recNum", "opType",
		"minLatency", "avgLatency", "maxLatency", "throughput",
		"totSize", "reqCount", "errCount", "fatalCount",
	}
}

func (rs RecipeStats) getContents() map[string]interface{} {
	var safeLatency float64
	if rs.requestCount > 0 && rs.latencyTotal > 0 {
		safeLatency = getMilliseconds(rs.latencyTotal) / float64(rs.requestCount)
	}
	var safeThroughput float64
	duration := rs.EndTime.Sub(rs.BeginTime)
	if duration > 0 {
		safeThroughput = float64(rs.totalSize) / duration.Seconds()
	}

	return map[string]interface{}{
		"beginTime": getTimestamp(rs.BeginTime),
		"endTime":   getTimestamp(rs.EndTime),
		"recName":   rs.RecipeName,
		"recNum":    rs.RecipeNum,
		"opType":    rs.OpType,

		"minLatency": getMilliseconds(rs.minLatency),
		"avgLatency": safeLatency,
		"maxLatency": getMilliseconds(rs.maxLatency),
		"throughput": safeThroughput,

		"totSize":    rs.totalSize,
		"reqCount":   rs.requestCount,
		"errCount":   rs.errorsCount,
		"fatalCount": rs.numFatal,
	}
}
