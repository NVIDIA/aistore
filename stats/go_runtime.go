// Package stats provides methods and functionality to register, track, log,
// and export metrics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"regexp"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/feat"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// additional static label for Go runtime
const (
	ConstlabNodeType = "node_type"
)

type goRuntimeCollector struct {
	collector prometheus.Collector
}

var regexGoMetrics = regexp.MustCompile(
	`^/memory/classes/heap/objects:bytes$` + // heap in use
		`|^/memory/classes/total:bytes$` + // total from OS
		`|^/gc/heap/goal:bytes$` + // next GC target
		`|^/gc/heap/allocs:bytes$` + // cumulative alloc bytes
		`|^/cpu/classes/gc/total:cpu-seconds$`, // cumulative GC CPU time
)

func regGoRuntime(nodeType string) {
	reg := prometheus.WrapRegistererWith(
		prometheus.Labels{
			ConstlabNode:     staticLabs[ConstlabNode],
			ConstlabNodeType: nodeType,
		},
		promRegistry,
	)
	reg.MustRegister(newGoRuntimeCollector())
}

func newGoRuntimeCollector() *goRuntimeCollector {
	return &goRuntimeCollector{
		collector: collectors.NewGoCollector(
			// skip go_memstats_* block; expose selected runtime/metrics instead.
			collectors.WithGoCollectorMemStatsMetricsDisabled(),
			collectors.WithGoCollectorRuntimeMetrics(
				collectors.GoRuntimeMetricsRule{Matcher: regexGoMetrics},
			),
		),
	}
}

func (c *goRuntimeCollector) Describe(ch chan<- *prometheus.Desc) {
	c.collector.Describe(ch)
}

func (c *goRuntimeCollector) Collect(ch chan<- prometheus.Metric) {
	if cmn.Rom.Features().IsSet(feat.EnableGoRuntimeMetrics) {
		c.collector.Collect(ch)
	}
}
