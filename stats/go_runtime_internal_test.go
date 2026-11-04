// Package stats provides methods and functionality to register, track, log,
// and export metrics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestGoRuntimeSchedulerLatency(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(newGoRuntimeCollector().collector)

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != "go_sched_latencies_seconds" {
			continue
		}
		metrics := family.GetMetric()
		if len(metrics) != 1 || len(metrics[0].GetHistogram().GetBucket()) == 0 {
			t.Fatal("expected one scheduler latency histogram with buckets")
		}
		return
	}
	t.Fatal("go_sched_latencies_seconds not found")
}
