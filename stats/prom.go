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
		add(val int64)
		addWith(val int64, vlabs map[string]string)
	}

	counter    struct{ prometheus.Counter }
	counterVec struct{ *prometheus.CounterVec }
	gauge      struct{ prometheus.Gauge }
	gaugeVec   struct{ *prometheus.GaugeVec }
)

func (v counter) add(val int64)                  { v.Add(float64(val)) }
func (counter) addWith(int64, map[string]string) { debug.Assert(false) }

func (counterVec) add(int64)                                    { debug.Assert(false) }
func (v counterVec) addWith(val int64, vlabs map[string]string) { v.With(vlabs).Add(float64(val)) }

func (v gauge) add(val int64)                  { v.Add(float64(val)) }
func (gauge) addWith(int64, map[string]string) { debug.Assert(false) }

func (gaugeVec) add(int64)                                    { debug.Assert(false) }
func (v gaugeVec) addWith(val int64, vlabs map[string]string) { v.With(vlabs).Add(float64(val)) }

//
// as adder
//

func (s *coreStats) add(name string, val int64) {
	v, ok := s.Tracker[name]
	debug.Assertf(ok, "invalid metric name %q", name)

	switch v.kind {
	// internal/computed + log
	case KindLatency:
		ratomic.AddInt64(&v.numSamples, 1)
		fallthrough
	case KindThroughput:
		ratomic.AddInt64(&v.Value, val)
		ratomic.AddInt64(&v.cumulative, val)

	// incl. prometheus; no variable labels
	case KindCounter, KindSize, KindTotal:
		ratomic.AddInt64(&v.Value, val)
		v.prom.add(val)

	default:
		debug.Assert(false, v.kind)
	}
}

func (s *coreStats) addWith(nv cos.NamedVal64) {
	v, ok := s.Tracker[nv.Name]
	debug.Assertf(ok, "invalid metric name %q", nv.Name)

	switch v.kind {
	// internal/computed + log
	case KindLatency:
		ratomic.AddInt64(&v.numSamples, 1)
		fallthrough
	case KindThroughput:
		ratomic.AddInt64(&v.Value, nv.Value)
		ratomic.AddInt64(&v.cumulative, nv.Value)

	// incl. prometheus; with variable labels
	case KindCounter, KindSize, KindTotal:
		ratomic.AddInt64(&v.Value, nv.Value)
		v.prom.addWith(nv.Value, nv.VarLabs)

	default:
		debug.Assert(false, v.kind)
	}
}

func (s *coreStats) updateUptime(d time.Duration) {
	v := s.Tracker[Uptime]
	ratomic.StoreInt64(&v.Value, d.Nanoseconds())

	vprom, ok := v.prom.(gauge)
	debug.Assert(ok, Uptime)

	vprom.Set(d.Seconds())
}
