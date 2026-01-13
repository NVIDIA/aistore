// Package stats provides methods and functionality to register, track, log,
// and export metrics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2024-2026, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	ratomic "sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"

	"github.com/prometheus/client_golang/prometheus"
)

// NOTE on `iprom` _fat interface_ and why we keep it ======================================
//
// The idea to split it as (ipromVec | ipromVal) comes with a cost and an extra code (churn).
//
// * direct cost: +16 bytes in every `statsValue`
//
// * indirect cost: special-cases (latency, throughput) that may be
//   registered with (e.g., `put.ns`) or without (e.g., `kalive.ns`) variable labels,
//   which complicates wiring ==============================================================

type (
	iprom interface {
		inc(parent *statsValue)
		incWith(parent *statsValue, nv cos.NamedVal64)
		add(parent *statsValue, val int64)
		addWith(parent *statsValue, nv cos.NamedVal64)
		set(parent *statsValue, val int64)
		observe(parent *statsValue, val float64)
	}

	latency    struct{}
	throughput struct{}

	counter    struct{ prometheus.Counter }
	counterVec struct{ *prometheus.CounterVec }
	gauge      struct{ prometheus.Gauge }
	gaugeVec   struct{ *prometheus.GaugeVec }
	histogram  struct{ prometheus.Histogram }
)

// interface guard
var (
	_ iprom = (*latency)(nil)
	_ iprom = (*throughput)(nil)

	_ iprom = (*counter)(nil)
	_ iprom = (*counterVec)(nil)
	_ iprom = (*gauge)(nil)
	_ iprom = (*gaugeVec)(nil)
	_ iprom = (*histogram)(nil)
)

//
// internal (computed) latency & throughput -----
//

func (latency) inc(*statsValue) {
	debug.Assert(false, "not expecting to inc latency")
}

func (latency) add(parent *statsValue, val int64) {
	ratomic.AddInt64(&parent.numSamples, 1)
	ratomic.AddInt64(&parent.Value, val)
	ratomic.AddInt64(&parent.cumulative, val)
}

func (latency) incWith(*statsValue, cos.NamedVal64) {
	debug.Assert(false, "not expecting to inc latency")
}

func (v latency) addWith(parent *statsValue, nv cos.NamedVal64) {
	v.add(parent, nv.Value)
}

func (throughput) inc(*statsValue) {
	debug.Assert(false, "not expecting to inc throughput")
}

func (throughput) add(parent *statsValue, val int64) {
	ratomic.AddInt64(&parent.Value, val)
	ratomic.AddInt64(&parent.cumulative, val)
}

func (throughput) incWith(*statsValue, cos.NamedVal64) {
	debug.Assert(false, "not expecting to inc throughput")
}

func (v throughput) addWith(parent *statsValue, nv cos.NamedVal64) {
	v.add(parent, nv.Value)
}

//
// Prometheus ---------------------------------
// in re: datapath performance vs Prometheus counters:
// - https://github.com/prometheus/client_golang/blob/main/prometheus/counter.go
//

func (v counter) inc(parent *statsValue) {
	ratomic.AddInt64(&parent.Value, 1)
	v.Inc()
}

func (v counter) add(parent *statsValue, val int64) {
	ratomic.AddInt64(&parent.Value, val)
	v.Add(float64(val))
}

func (v counterVec) incWith(parent *statsValue, nv cos.NamedVal64) {
	ratomic.AddInt64(&parent.Value, 1)
	v.With(nv.VarLabs).Inc()
}

func (v counterVec) addWith(parent *statsValue, nv cos.NamedVal64) {
	ratomic.AddInt64(&parent.Value, nv.Value)
	v.With(nv.VarLabs).Add(float64(nv.Value))
}

func (v gauge) inc(parent *statsValue) {
	ratomic.AddInt64(&parent.Value, 1)
	v.Inc()
}

func (v gauge) add(parent *statsValue, val int64) {
	ratomic.AddInt64(&parent.Value, val)
	v.Add(float64(val))
}

func (v gauge) set(parent *statsValue, val int64) {
	ratomic.StoreInt64(&parent.Value, val)
	v.Set(float64(val))
}

func (v gaugeVec) incWith(parent *statsValue, nv cos.NamedVal64) {
	ratomic.AddInt64(&parent.Value, 1)
	v.With(nv.VarLabs).Inc()
}
func (v gaugeVec) addWith(parent *statsValue, nv cos.NamedVal64) {
	ratomic.AddInt64(&parent.Value, nv.Value)
	v.With(nv.VarLabs).Add(float64(nv.Value))
}

func (h histogram) observe(parent *statsValue, val float64) {
	ratomic.AddInt64(&parent.numSamples, 1)
	ratomic.AddInt64(&parent.cumulative, int64(val))
	h.Observe(val)
}

// illegal impl. placeholders - see "fat interface" note above

func (counter) incWith(*statsValue, cos.NamedVal64)   { debug.Assert(false) }
func (counter) addWith(*statsValue, cos.NamedVal64)   { debug.Assert(false) }
func (counter) set(*statsValue, int64)                { debug.Assert(false) }
func (counter) observe(*statsValue, float64)          { debug.Assert(false) }
func (counterVec) inc(*statsValue)                    { debug.Assert(false) }
func (counterVec) add(*statsValue, int64)             { debug.Assert(false) }
func (counterVec) set(*statsValue, int64)             { debug.Assert(false) }
func (counterVec) observe(*statsValue, float64)       { debug.Assert(false) }
func (gauge) incWith(*statsValue, cos.NamedVal64)     { debug.Assert(false) }
func (gauge) addWith(*statsValue, cos.NamedVal64)     { debug.Assert(false) }
func (gauge) observe(*statsValue, float64)            { debug.Assert(false) }
func (gaugeVec) inc(*statsValue)                      { debug.Assert(false) }
func (gaugeVec) add(*statsValue, int64)               { debug.Assert(false) }
func (gaugeVec) set(*statsValue, int64)               { debug.Assert(false) }
func (gaugeVec) observe(*statsValue, float64)         { debug.Assert(false) }
func (latency) set(*statsValue, int64)                { debug.Assert(false) }
func (latency) observe(*statsValue, float64)          { debug.Assert(false) }
func (throughput) set(*statsValue, int64)             { debug.Assert(false) }
func (throughput) observe(*statsValue, float64)       { debug.Assert(false) }
func (histogram) inc(*statsValue)                     { debug.Assert(false) }
func (histogram) incWith(*statsValue, cos.NamedVal64) { debug.Assert(false) }
func (histogram) add(*statsValue, int64)              { debug.Assert(false) }
func (histogram) addWith(*statsValue, cos.NamedVal64) { debug.Assert(false) }
func (histogram) set(*statsValue, int64)              { debug.Assert(false) }

// coreStats

func (s *coreStats) add(name string, val int64) {
	v, ok := s.Tracker[name]
	debug.Assertf(ok, "invalid metric name %q", name)

	v.iprom.add(v, val)
}

func (s *coreStats) inc(name string) {
	v, ok := s.Tracker[name]
	debug.Assertf(ok, "invalid metric name %q", name)

	v.iprom.inc(v)
}

func (s *coreStats) set(name string, val int64) {
	v, ok := s.Tracker[name]
	debug.Assertf(ok, "invalid metric name %q", name)

	v.iprom.set(v, val)
}

func (s *coreStats) observe(name string, val float64) {
	v, ok := s.Tracker[name]
	debug.Assertf(ok, "invalid metric name %q", name)
	v.iprom.observe(v, val)
}

func (s *coreStats) addWith(nv cos.NamedVal64) {
	v, ok := s.Tracker[nv.Name]
	debug.Assertf(ok, "invalid metric name %q", nv.Name)

	v.iprom.addWith(v, nv)
}

func (s *coreStats) incWith(nv cos.NamedVal64) {
	v, ok := s.Tracker[nv.Name]
	debug.Assertf(ok, "invalid metric name %q", nv.Name)

	v.iprom.incWith(v, nv)
}

func (s *coreStats) updateUptime(d time.Duration) {
	v := s.Tracker[Uptime]
	ratomic.StoreInt64(&v.Value, d.Nanoseconds())

	vprom, ok := v.iprom.(gauge)
	debug.Assert(ok, Uptime)

	vprom.Set(d.Seconds())
}
