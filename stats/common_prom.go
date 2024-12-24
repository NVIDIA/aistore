//go:build !statsd

// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"encoding/json"
	"strings"
	ratomic "sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/prometheus/client_golang/prometheus"
)

type (
	statsValue struct {
		prom       iadd
		kind       string // enum { KindCounter, ..., KindSpecial }
		Value      int64  `json:"v,string"`
		numSamples int64  // (average latency over stats_time)
		cumulative int64  // REST API
	}
	coreStats struct {
		Tracker   map[string]*statsValue
		sgl       *memsys.SGL
		statsTime time.Duration
	}
)

///////////////
// coreStats //
///////////////

// interface guard
var (
	_ json.Marshaler   = (*coreStats)(nil)
	_ json.Unmarshaler = (*coreStats)(nil)
)

var staticLabs = prometheus.Labels{ConstlabNode: ""}

func initLabel(snode *meta.Snode) {
	staticLabs[ConstlabNode] = strings.ReplaceAll(snode.ID(), ".", "_")
}

func (*coreStats) initStarted(*meta.Snode) { nlog.Infoln("Using Prometheus") }

// usage: log resulting `copyValue` numbers:
func (s *coreStats) copyT(out copyTracker, diskLowUtil ...int64) bool {
	idle := true
	intl := max(int64(s.statsTime.Seconds()), 1)
	s.sgl.Reset()
	for name, v := range s.Tracker {
		switch v.kind {
		case KindLatency:
			var lat int64
			if num := ratomic.SwapInt64(&v.numSamples, 0); num > 0 {
				lat = ratomic.SwapInt64(&v.Value, 0) / num // NOTE: log average latency (nanoseconds) over the last "periodic.stats_time" interval
				if !ignore(name) {
					idle = false
				}
			}
			out[name] = copyValue{lat}
		case KindThroughput:
			var throughput int64
			if throughput = ratomic.SwapInt64(&v.Value, 0); throughput > 0 {
				throughput /= intl // NOTE: log average throughput (bps) over the last "periodic.stats_time" interval
				if !ignore(name) {
					idle = false
				}
			}
			out[name] = copyValue{throughput}
		case KindComputedThroughput:
			if throughput := ratomic.SwapInt64(&v.Value, 0); throughput > 0 {
				out[name] = copyValue{throughput}
			}
		case KindCounter, KindSize, KindTotal:
			var (
				val     = ratomic.LoadInt64(&v.Value)
				changed bool
			)
			if prev, ok := out[name]; !ok || prev.Value != val {
				changed = true
			}
			if val > 0 {
				out[name] = copyValue{val}
				if changed && !ignore(name) {
					idle = false
				}
			}
		case KindGauge:
			val := ratomic.LoadInt64(&v.Value)
			out[name] = copyValue{val}
			if isDiskUtilMetric(name) && val > diskLowUtil[0] {
				idle = false
			}
		default:
			out[name] = copyValue{ratomic.LoadInt64(&v.Value)}
		}
	}
	return idle
}

// REST API what=stats query
// NOTE: reporting total cumulative values to compute throughput and latency by the client
// based on their respective time interval and request counts
// NOTE: not reporting zero counts
func (s *coreStats) copyCumulative(ctracker copyTracker) {
	for name, v := range s.Tracker {
		switch v.kind {
		case KindLatency:
			ctracker[name] = copyValue{ratomic.LoadInt64(&v.cumulative)}
		case KindThroughput:
			val := copyValue{ratomic.LoadInt64(&v.cumulative)}
			ctracker[name] = val

			// NOTE: effectively, add same-value metric that was never added/updated
			// via `runner.Add` and friends. Is OK to replace ".bps" suffix
			// as statsValue.cumulative _is_ the total size (aka, KindSize)
			n := name[:len(name)-3] + "size"
			ctracker[n] = val
		case KindCounter, KindSize, KindTotal:
			if val := ratomic.LoadInt64(&v.Value); val > 0 {
				ctracker[name] = copyValue{val}
			}
		default: // KindSpecial, KindComputedThroughput, KindGauge
			ctracker[name] = copyValue{ratomic.LoadInt64(&v.Value)}
		}
	}
}

func (s *coreStats) reset(errorsOnly bool) {
	if errorsOnly {
		for name, v := range s.Tracker {
			if IsErrMetric(name) {
				debug.Assert(v.kind == KindCounter || v.kind == KindSize, name)
				ratomic.StoreInt64(&v.Value, 0)
			}
		}
		return
	}

	for _, v := range s.Tracker {
		switch v.kind {
		case KindLatency:
			ratomic.StoreInt64(&v.numSamples, 0)
			fallthrough
		case KindThroughput:
			ratomic.StoreInt64(&v.Value, 0)
			ratomic.StoreInt64(&v.cumulative, 0)
		case KindCounter, KindSize, KindComputedThroughput, KindGauge, KindTotal:
			ratomic.StoreInt64(&v.Value, 0)
		default: // KindSpecial - do nothing
		}
	}
}

////////////
// runner //
////////////

func (r *runner) reg(snode *meta.Snode, name, kind string, extra *Extra) {
	var (
		metricName string
		help       string
		constLabs  = staticLabs
	)

	// static labels
	if len(extra.Labels) > 0 {
		constLabs = prometheus.Labels(extra.Labels)
		constLabs[ConstlabNode] = staticLabs[ConstlabNode]
	}

	// metric name
	if extra.StrName == "" {
		// when not explicitly specified: generate prometheus metric name
		switch kind {
		case KindCounter:
			debug.Assert(strings.HasSuffix(name, ".n"), name)
			metricName = strings.TrimSuffix(name, ".n") + "_count"
		case KindSize:
			debug.Assert(strings.HasSuffix(name, ".size"), name)
			metricName = strings.TrimSuffix(name, ".size") + "_bytes"
		case KindLatency:
			debug.Assert(strings.HasSuffix(name, ".ns"), name)
			metricName = strings.TrimSuffix(name, ".ns") + "_ms"
		case KindThroughput, KindComputedThroughput:
			debug.Assert(strings.HasSuffix(name, ".bps"), name)
			metricName = strings.TrimSuffix(name, ".bps") + "_mbps"
		default:
			metricName = name
		}
		metricName = strings.ReplaceAll(metricName, ".", "_")
	} else {
		metricName = extra.StrName
	}

	// help
	help = extra.Help

	// construct prometheus metric
	v := &statsValue{kind: kind}

	switch kind {
	case KindCounter, KindTotal, KindSize:
		opts := prometheus.CounterOpts{Namespace: "ais", Subsystem: snode.Type(), Name: metricName, Help: help, ConstLabels: constLabs}
		if len(extra.VarLabs) > 0 {
			metric := prometheus.NewCounterVec(opts, extra.VarLabs)
			v.prom = counterVec{metric}
			prometheus.MustRegister(metric)
		} else {
			metric := prometheus.NewCounter(opts)
			v.prom = counter{metric}
			prometheus.MustRegister(metric)
		}
	case KindLatency, KindThroughput:
		// these two _kinds_ or, more generally, metrics computed over fixed ('periodic.stats_time') interval
		// are now completely hidden from prometheus (v3.26)
	default:
		opts := prometheus.GaugeOpts{Namespace: "ais", Subsystem: snode.Type(), Name: metricName, Help: help, ConstLabels: constLabs}
		if len(extra.VarLabs) > 0 {
			metric := prometheus.NewGaugeVec(opts, extra.VarLabs)
			v.prom = gaugeVec{metric}
			prometheus.MustRegister(metric)
		} else {
			metric := prometheus.NewGauge(opts)
			v.prom = gauge{metric}
			prometheus.MustRegister(metric)
		}
	}

	r.core.Tracker[name] = v
}

func (*runner) IsPrometheus() bool { return true }
func (*runner) closeStatsD()       {} // build tag "statsd" stub
