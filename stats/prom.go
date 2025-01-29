//go:build !statsd

// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	ratomic "sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/prometheus/client_golang/prometheus"
)

type (
	iadd interface {
		inc(parent *statsValue)
		incWith(parent *statsValue, nv cos.NamedVal64)
		add(parent *statsValue, val int64)
		addWith(parent *statsValue, nv cos.NamedVal64)
	}

	latency    struct{}
	throughput struct{}

	counter    struct{ prometheus.Counter }
	counterVec struct{ *prometheus.CounterVec }
	gauge      struct{ prometheus.Gauge }
	gaugeVec   struct{ *prometheus.GaugeVec }
)

// interface guard
var (
	_ iadd = (*latency)(nil)
	_ iadd = (*throughput)(nil)

	_ iadd = (*counter)(nil)
	_ iadd = (*counterVec)(nil)
	_ iadd = (*gauge)(nil)
	_ iadd = (*gaugeVec)(nil)
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

func (v gaugeVec) incWith(parent *statsValue, nv cos.NamedVal64) {
	ratomic.AddInt64(&parent.Value, 1)
	v.With(nv.VarLabs).Inc()
}
func (v gaugeVec) addWith(parent *statsValue, nv cos.NamedVal64) {
	ratomic.AddInt64(&parent.Value, nv.Value)
	v.With(nv.VarLabs).Add(float64(nv.Value))
}

// illegal

func (counter) incWith(*statsValue, cos.NamedVal64) { debug.Assert(false) }
func (counter) addWith(*statsValue, cos.NamedVal64) { debug.Assert(false) }
func (counterVec) inc(*statsValue)                  { debug.Assert(false) }
func (counterVec) add(*statsValue, int64)           { debug.Assert(false) }
func (gauge) incWith(*statsValue, cos.NamedVal64)   { debug.Assert(false) }
func (gauge) addWith(*statsValue, cos.NamedVal64)   { debug.Assert(false) }
func (gaugeVec) inc(*statsValue)                    { debug.Assert(false) }
func (gaugeVec) add(*statsValue, int64)             { debug.Assert(false) }

// coreStats

func (s *coreStats) add(name string, val int64) {
	v, ok := s.Tracker[name]
	debug.Assertf(ok, "invalid metric name %q", name)

	v.iadd.add(v, val)
}

func (s *coreStats) inc(name string) {
	v, ok := s.Tracker[name]
	debug.Assertf(ok, "invalid metric name %q", name)

	v.iadd.inc(v)
}

func (s *coreStats) addWith(nv cos.NamedVal64) {
	v, ok := s.Tracker[nv.Name]
	debug.Assertf(ok, "invalid metric name %q", nv.Name)

	v.iadd.addWith(v, nv)
}

func (s *coreStats) incWith(nv cos.NamedVal64) {
	v, ok := s.Tracker[nv.Name]
	debug.Assertf(ok, "invalid metric name %q", nv.Name)

	v.iadd.incWith(v, nv)
}

func (s *coreStats) updateUptime(d time.Duration) {
	v := s.Tracker[Uptime]
	ratomic.StoreInt64(&v.Value, d.Nanoseconds())

	vprom, ok := v.iadd.(gauge)
	debug.Assert(ok, Uptime)

	vprom.Set(d.Seconds())
}
