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
	"sync"
	ratomic "sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"
	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/client_golang/prometheus"
)

type (
	promDesc map[string]*prometheus.Desc

	// Stats are tracked via a map of stats names (key) and statsValue (values).
	statsValue struct {
		kind       string // enum { KindCounter, ..., KindSpecial }
		Value      int64  `json:"v,string"`
		numSamples int64  // (average latency over stats_time)
		cumulative int64  // REST API
	}

	coreStats struct {
		Tracker   map[string]*statsValue
		promDesc  promDesc
		sgl       *memsys.SGL
		statsTime time.Duration
		cmu       sync.RWMutex // ctracker vs Prometheus Collect()
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

func (s *coreStats) init(size int) {
	s.Tracker = make(map[string]*statsValue, size)
	s.promDesc = make(promDesc, size)

	s.sgl = memsys.PageMM().NewSGL(memsys.DefaultBufSize)
}

var dfltLabels = prometheus.Labels{"node_id": ""}

func initDfltlabel(snode *meta.Snode) {
	dfltLabels["node_id"] = strings.ReplaceAll(snode.ID(), ".", "_")
}

// init Prometheus (not StatsD)
func (*coreStats) initStatsdOrProm(_ *meta.Snode, parent *runner) {
	nlog.Infoln("Using Prometheus")
	prometheus.MustRegister(parent) // as prometheus.Collector
}

// vs Collect()
func (s *coreStats) promRLock()   { s.cmu.RLock() }
func (s *coreStats) promRUnlock() { s.cmu.RUnlock() }
func (s *coreStats) promLock()    { s.cmu.Lock() }
func (s *coreStats) promUnlock()  { s.cmu.Unlock() }

func (s *coreStats) updateUptime(d time.Duration) {
	v := s.Tracker[Uptime]
	ratomic.StoreInt64(&v.Value, d.Nanoseconds())
}

func (s *coreStats) MarshalJSON() ([]byte, error) { return jsoniter.Marshal(s.Tracker) }
func (s *coreStats) UnmarshalJSON(b []byte) error { return jsoniter.Unmarshal(b, &s.Tracker) }

func (s *coreStats) get(name string) (val int64) {
	v := s.Tracker[name]
	val = ratomic.LoadInt64(&v.Value)
	return
}

func (s *coreStats) update(nv cos.NamedVal64) {
	v, ok := s.Tracker[nv.Name]
	debug.Assertf(ok, "invalid metric name %q", nv.Name)
	switch v.kind {
	case KindLatency:
		ratomic.AddInt64(&v.numSamples, 1)
		fallthrough
	case KindThroughput:
		ratomic.AddInt64(&v.Value, nv.Value)
		ratomic.AddInt64(&v.cumulative, nv.Value)
	case KindCounter, KindSize, KindTotal:
		ratomic.AddInt64(&v.Value, nv.Value)
	default:
		debug.Assert(false, v.kind)
	}
}

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

// interface guard
var (
	_ prometheus.Collector = (*runner)(nil)
)

func (r *runner) reg(snode *meta.Snode, name, kind string, extra *Extra) {
	v := &statsValue{kind: kind}
	r.core.Tracker[name] = v

	var (
		metricName  string
		help        string
		constLabels = dfltLabels
	)
	debug.Assert(extra != nil)
	debug.Assert(extra.Help != "")
	if len(extra.Labels) > 0 {
		constLabels = prometheus.Labels(extra.Labels)
		constLabels["node_id"] = dfltLabels["node_id"]
	}
	if extra.StrName == "" {
		// when not explicitly specified: generate prometheus name
		// from an internal name (compare with common_statsd reg() impl.)
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
	help = extra.Help

	fullqn := prometheus.BuildFQName("ais" /*namespace*/, snode.Type() /*subsystem*/, metricName)
	r.core.promDesc[name] = prometheus.NewDesc(fullqn, help, nil /*variableLabels*/, constLabels)
}

func (*runner) IsPrometheus() bool { return true }

func (r *runner) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range r.core.promDesc {
		ch <- desc
	}
}

func (r *runner) Collect(ch chan<- prometheus.Metric) {
	debug.Assert(r.StartedUp()) // via initStatsdOrProm()

	r.core.promRLock()
	for name, v := range r.core.Tracker {
		var (
			val int64
			fv  float64
		)
		copyV, okc := r.ctracker[name]
		if !okc {
			continue
		}
		// NOTE: skipping metrics that have not (yet) been updated
		// (and some of them may never be)
		if val = copyV.Value; val == 0 {
			continue
		}
		fv = float64(val)
		// 1. convert units
		switch v.kind {
		case KindCounter, KindTotal:
			// do nothing
		case KindSize:
			fv = float64(val)
		case KindLatency:
			millis := cos.DivRound(val, int64(time.Millisecond))
			fv = float64(millis)
		case KindThroughput:
			fv = roundMBs(val)
		default:
			if name == Uptime {
				seconds := cos.DivRound(val, int64(time.Second))
				fv = float64(seconds)
			}
		}
		// 2. convert kind
		promMetricType := prometheus.GaugeValue
		if v.kind == KindCounter || v.kind == KindSize || v.kind == KindTotal {
			promMetricType = prometheus.CounterValue
		}
		// 3. publish
		desc, ok := r.core.promDesc[name]
		debug.Assert(ok, name)
		m, err := prometheus.NewConstMetric(desc, promMetricType, fv)
		debug.AssertNoErr(err)
		ch <- m
	}
	r.core.promRUnlock()
}

func (r *runner) Stop(err error) {
	nlog.Infof("Stopping %s, err: %v", r.Name(), err)
	r.stopCh <- struct{}{}
	close(r.stopCh)
}
