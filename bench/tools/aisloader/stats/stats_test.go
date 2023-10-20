// Package stats_test provides tests of aisloader stats package
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package stats_test

import (
	"testing"
	"time"

	"github.com/NVIDIA/aistore/bench/tools/aisloader/stats"
)

func verify(t *testing.T, msg string, exp, act int64) {
	if exp != act {
		t.Fatalf("Error: %s, expected = %d, actual = %d", msg, exp, act)
	}
}

func TestStats(t *testing.T) {
	start := time.Now()
	s := stats.NewHTTPReq(start)

	// basic put
	s.Add(100, 100*time.Millisecond)
	s.Add(200, 20*time.Millisecond)
	s.AddErr()
	s.Add(50, 30*time.Millisecond)

	verify(t, "Total", 3, s.Total())
	verify(t, "Total bytes", 350, s.TotalBytes())
	verify(t, "Min latency", 20000000, s.MinLatency())
	verify(t, "Avg latency", 50000000, s.AvgLatency())
	verify(t, "Max latency", 100000000, s.MaxLatency())
	verify(t, "Throughput", 5, s.Throughput(start, start.Add(70*time.Second)))
	verify(t, "Failed", 1, s.TotalErrs())

	// accumulate non empty stats on top of empty stats
	total := stats.NewHTTPReq(start)
	total.Aggregate(s)
	verify(t, "Total", 3, total.Total())
	verify(t, "Total bytes", 350, total.TotalBytes())
	verify(t, "Min latency", 20000000, total.MinLatency())
	verify(t, "Avg latency", 50000000, total.AvgLatency())
	verify(t, "Max latency", 100000000, total.MaxLatency())
	verify(t, "Throughput", 5, total.Throughput(start, start.Add(70*time.Second)))
	verify(t, "Failed", 1, total.TotalErrs())

	// accumulate empty stats on top of non empty stats
	s = stats.NewHTTPReq(start)
	total.Aggregate(s)
	verify(t, "Total", 3, total.Total())
	verify(t, "Total bytes", 350, total.TotalBytes())
	verify(t, "Min latency", 20000000, total.MinLatency())
	verify(t, "Avg latency", 50000000, total.AvgLatency())
	verify(t, "Max latency", 100000000, total.MaxLatency())
	verify(t, "Throughput", 5, total.Throughput(start, start.Add(70*time.Second)))
	verify(t, "Failed", 1, total.TotalErrs())

	// accumulate non empty stats on top of non empty stats
	s.AddErr()
	total.Aggregate(s)
	verify(t, "Failed err", 2, total.TotalErrs())
	s.Add(1, 5*time.Millisecond)
	total.Aggregate(s)
	verify(t, "Total", 4, total.Total())
	verify(t, "Total bytes", 351, total.TotalBytes())
	verify(t, "Min latency", 5000000, total.MinLatency())
	verify(t, "Avg latency", 38750000, total.AvgLatency())
	verify(t, "Max latency", 100000000, total.MaxLatency())
	verify(t, "Throughput", 5, total.Throughput(start, start.Add(70*time.Second)))
}
