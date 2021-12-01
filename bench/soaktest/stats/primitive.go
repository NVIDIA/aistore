// Package stats keeps track of all the different statistics collected by the report
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"time"
)

type PrimitiveStat struct {
	AISLoaderStat
	Fatal  bool   `json:"fatal"`
	OpType string `json:"op_type"`
	ID     string `json:"id"` // Soakprim assumes responsibility for filling

	RecipeName string `json:"recipe_name"` // Report package assumes responsibility for filling
	RecipeNum  int    `json:"recipe_num"`  // Report package assumes responsibility for filling
}

// AISLoaderStat is a response from AISLoader, keep json consistent with the `jsonStats` struct in AISLoader
type AISLoaderStat struct {
	LatencyMin   time.Duration `json:"min_latency"`
	Latency      time.Duration `json:"latency"` // Average
	LatencyMax   time.Duration `json:"max_latency"`
	Throughput   int64         `json:"throughput"` // bytes
	TotalSize    int64         `json:"bytes"`
	RequestCount int64         `json:"count"`
	ErrorsCount  int64         `json:"errors"`
	StartTime    time.Time     `json:"start_time"`
	Duration     time.Duration `json:"duration"`
}

func (PrimitiveStat) getHeadingsText() map[string]string {
	return map[string]string{
		"startTime": "Start (excel timestamp)",
		"endTime":   "End (excel timestamp)",
		"recName":   "Recipe Name",
		"recNum":    "Recipe Num",
		"primID":    "Primitive ID",

		"opType": "Operation Type",

		"minLatency": "Min Latency (ms)",
		"avgLatency": "Avg Latency (ms)",
		"maxLatency": "Max Latency (ms)",
		"throughput": "Throughput (B/s)",

		"totSize":  "Total Size (B)",
		"reqCount": "Request Count",
		"errCount": "Error Count",
		"fatal":    "Fatal",
	}
}

func (PrimitiveStat) getHeadingsOrder() []string {
	return []string{
		"startTime", "endTime", "recName", "recNum", "primID",
		"opType",
		"minLatency", "avgLatency", "maxLatency", "throughput",
		"totSize", "reqCount", "errCount", "fatal",
	}
}

func (ps PrimitiveStat) getContents() map[string]interface{} {
	if ps.Fatal {
		return map[string]interface{}{
			"startTime": getTimestamp(ps.StartTime),
			"recName":   ps.RecipeName,
			"recNum":    ps.RecipeNum,
			"primID":    ps.ID,

			"fatal": true,
		}
	}

	return map[string]interface{}{
		"startTime": getTimestamp(ps.StartTime),
		"endTime":   getTimestamp(ps.StartTime.Add(ps.Duration)),
		"recName":   ps.RecipeName,
		"recNum":    ps.RecipeNum,
		"primID":    ps.ID,

		"opType": ps.OpType,

		"minLatency": getMilliseconds(ps.LatencyMin),
		"avgLatency": getMilliseconds(ps.Latency),
		"maxLatency": getMilliseconds(ps.LatencyMax),
		"throughput": ps.Throughput,

		"totSize":  ps.TotalSize,
		"reqCount": ps.RequestCount,
		"errCount": ps.ErrorsCount,
		"fatal":    false,
	}
}
