// Package load provides 5-dimensional node-pressure readings and per-dimension grading.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package load

import (
	"math"
	"runtime/metrics"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
)

// scheduler latency: time runnable goroutines wait before running (runtime/metrics)
// - grade the p99 over a window (delta of the cumulative histogram)
// - current heuristic: Critical at cplane_operation/20, bounded; High at a tenth of that
// - best effort: under-estimate (bucket lower bound); sparse or stale windows mean Low
// - two independent windows:
//   - hot path: Sched() refreshes at most once per periodic.stats_time; no background sampling
//   - alerts: stats runner, one window per periodic.stats_time (see SchedWin)

const schedMetric = "/sched/latencies:seconds"

const (
	schedPct        = 0.99
	schedMinSamples = 128 // fewer scheduling observations per window: Low
)

type (
	// window over the cumulative histogram: delta between two consecutive reads
	// - caller serializes
	SchedWin struct {
		sample [1]metrics.Sample
		prev   []uint64
	}

	// hot path: throttled refresh and published grade
	schedSampler struct {
		win   SchedWin
		mu    sync.Mutex
		at    int64        // mono time of the previous read; under mu
		next  atomic.Int64 // mono time of the next sample
		grade atomic.Uint32
		p99   atomic.Int64 // last p99 (ns)
	}
)

var sch schedSampler

func init() { sch.win.Init() }

//////////////
// SchedWin //
//////////////

func (w *SchedWin) Init() { w.sample[0].Name = schedMetric }

// grade the delta since the previous call; baseline only when discard
func (w *SchedWin) Grade(discard bool) (Load, time.Duration) {
	metrics.Read(w.sample[:])
	if w.sample[0].Value.Kind() != metrics.KindFloat64Histogram {
		return Low, 0
	}
	h := w.sample[0].Value.Float64Histogram()
	if discard || len(w.prev) != len(h.Counts) {
		w.prev = append(w.prev[:0], h.Counts...)
		return Low, 0
	}
	p99, n := pctDelta(w.prev, h.Counts, h.Buckets)
	copy(w.prev, h.Counts) // (Read may reuse h)

	if n < schedMinSamples {
		return Low, 0
	}
	d := time.Duration(p99 * float64(time.Second))
	return gradeSched(d), d
}

//////////////////
// schedSampler //
//////////////////

// grade scheduler latency; refresh at most once per periodic.stats_time
func Sched() Load {
	return sch.load(mono.NanoTime())
}

// last hot-path p99 scheduler latency (zero when no sufficiently populated recent window)
func SchedP99() time.Duration { return time.Duration(sch.p99.Load()) }

func (s *schedSampler) load(now int64) Load {
	if now < s.next.Load() || !s.mu.TryLock() {
		return s.last()
	}
	defer s.mu.Unlock()
	next := s.next.Load()
	if now < next {
		return s.last()
	}
	ival := cmn.Rom.StatsTime()
	s.next.Store(now + int64(ival))
	// rebase: more than two intervals since the last read
	discard := s.at == 0 || time.Duration(now-s.at) > 2*ival
	s.at = now
	return s.refresh(discard)
}

func (s *schedSampler) last() Load {
	if g := Load(s.grade.Load()); g != 0 {
		return g
	}
	return Low
}

// caller owns s.mu
func (s *schedSampler) refresh(discard bool) Load {
	g, d := s.win.Grade(discard)
	s.p99.Store(int64(d))
	s.grade.Store(uint32(g))
	return g
}

//
// grading
//

func schedCrit() time.Duration {
	return cos.ClampDuration(cmn.Rom.CplaneOperation()/20, cos.SchedLatencyCritMin, cos.SchedLatencyCritMax)
}

func gradeSched(p99 time.Duration) Load {
	crit := schedCrit()
	switch {
	case p99 >= crit:
		return Critical
	case p99 >= crit/10:
		return High
	default:
		return Low
	}
}

// return the schedPct percentile's bucket lower bound (seconds) and the number of events
// in the delta (cur - prev) of a cumulative histogram
// - buckets: len(cur)+1 boundaries; -Inf lower bound => zero
func pctDelta(prev, cur []uint64, buckets []float64) (float64, uint64) {
	var total uint64
	for i := range cur {
		total += cur[i] - prev[i]
	}
	if total == 0 {
		return 0, 0
	}
	target := uint64(math.Ceil(schedPct * float64(total)))
	var cum uint64
	for i := range cur {
		cum += cur[i] - prev[i]
		if cum >= target {
			return max(buckets[i], 0), total
		}
	}
	return max(buckets[len(cur)-1], 0), total // (not reached)
}
