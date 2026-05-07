// Package stats provides methods and functionality to register, track, log,
// and export metrics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"github.com/NVIDIA/aistore/core/meta"

	"github.com/prometheus/client_golang/prometheus"
)

const primaryIDLabel = "primary_id"

type primaryInfo struct {
	sowner meta.Sowner
	desc   *prometheus.Desc
}

func RegSmapMetrics(sowner meta.Sowner) {
	promRegistry.MustRegister(newPrimaryInfo(sowner))
}

func newPrimaryInfo(sowner meta.Sowner) *primaryInfo {
	return &primaryInfo{
		sowner: sowner,
		desc: prometheus.NewDesc(
			prometheus.BuildFQName("ais", "node", "primary_info"),
			"Primary proxy information for this node.",
			[]string{primaryIDLabel},
			prometheus.Labels{ConstlabNode: staticLabs[ConstlabNode]},
		),
	}
}

func (p *primaryInfo) Describe(ch chan<- *prometheus.Desc) {
	ch <- p.desc
}

func (p *primaryInfo) Collect(ch chan<- prometheus.Metric) {
	smap := p.sowner.Get()
	if smap == nil || smap.Primary == nil {
		return
	}

	ch <- prometheus.MustNewConstMetric(
		p.desc,
		prometheus.GaugeValue,
		1,
		smap.Primary.ID(),
	)
}
