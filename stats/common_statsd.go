//go:build statsd

// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	ratomic "sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats/statsd"
	jsoniter "github.com/json-iterator/go"
)

type (
	metric = statsd.Metric // type alias

	coreStats struct {
		Tracker   map[string]*statsValue
		statsdC   *statsd.Client
		sgl       *memsys.SGL
		statsTime time.Duration
	}
)

// interface guard
var (
	_ Tracker = (*Prunner)(nil)
	_ Tracker = (*Trunner)(nil)
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

	s.sgl = memsys.PageMM().NewSGL(memsys.PageSize)
}

// NOTE: nil StatsD client means that we provide metrics to Prometheus (see below)
func (s *coreStats) statsdDisabled() bool { return s.statsdC == nil }

// empty stabs
func (*coreStats) initProm(*meta.Snode) {}
func (*coreStats) promLock()            {}
func (*coreStats) promUnlock()          {}

// init MetricClient client: StatsD (default) or Prometheus
func (s *coreStats) initMetricClient(snode *meta.Snode, _ *runner) {
	var (
		port  = 8125  // StatsD default port, see https://github.com/etsy/stats
		probe = false // test-probe StatsD server at init time
	)
	if portStr := os.Getenv("AIS_STATSD_PORT"); portStr != "" {
		if portNum, err := cmn.ParsePort(portStr); err != nil {
			debug.AssertNoErr(err)
			nlog.Errorln(err)
		} else {
			port = portNum
		}
	}
	if probeStr := os.Getenv("AIS_STATSD_PROBE"); probeStr != "" {
		if probeBool, err := cos.ParseBool(probeStr); err != nil {
			nlog.Errorln(err)
		} else {
			probe = probeBool
		}
	}
	id := strings.ReplaceAll(snode.ID(), ":", "_") // ":" delineates name and value for StatsD
	statsD, err := statsd.New("localhost", port, "ais"+snode.Type()+"."+id, probe)
	if err != nil {
		nlog.Errorf("Starting up without StatsD: %v", err)
	} else {
		nlog.Infoln("Using StatsD")
	}
	s.statsdC = statsD
}

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
		// - non-empty suffix forces an immediate Tx with no aggregation (see below);
		// - suffix is an arbitrary string that can be defined at runtime;
		// - e.g. usage: per-mountpath error counters.
		if !s.statsdDisabled() && nv.NameSuffix != "" {
			s.statsdC.Send(v.label.comm+"."+nv.NameSuffix,
				1, metric{Type: statsd.Counter, Name: "count", Value: nv.Value})
		}
	default:
		debug.Assert(false, v.kind)
	}
}

// log + StatsD (Prometheus is done separately via `Collect`)
func (s *coreStats) copyT(out copyTracker, diskLowUtil ...int64) bool {
	idle := true
	intl := max(int64(s.statsTime.Seconds()), 1)
	s.sgl.Reset()
	for name, v := range s.Tracker {
		switch v.kind {
		case KindLatency:
			var lat int64
			if num := ratomic.SwapInt64(&v.numSamples, 0); num > 0 {
				lat = ratomic.SwapInt64(&v.Value, 0) / num
				if !ignore(name) {
					idle = false
				}
			}
			out[name] = copyValue{lat}
			// NOTE: ns => ms, and not reporting zeros
			millis := cos.DivRound(lat, int64(time.Millisecond))
			if !s.statsdDisabled() && millis > 0 {
				s.statsdC.AppMetric(metric{Type: statsd.Timer, Name: v.label.stsd, Value: float64(millis)}, s.sgl)
			}
		case KindThroughput:
			var throughput int64
			if throughput = ratomic.SwapInt64(&v.Value, 0); throughput > 0 {
				throughput /= intl
				if !ignore(name) {
					idle = false
				}
			}
			out[name] = copyValue{throughput}
			if !s.statsdDisabled() && throughput > 0 {
				fv := roundMBs(throughput)
				s.statsdC.AppMetric(metric{Type: statsd.Gauge, Name: v.label.stsd, Value: fv}, s.sgl)
			}
		case KindComputedThroughput:
			if throughput := ratomic.SwapInt64(&v.Value, 0); throughput > 0 {
				out[name] = copyValue{throughput}
				if !s.statsdDisabled() {
					fv := roundMBs(throughput)
					s.statsdC.AppMetric(metric{Type: statsd.Gauge, Name: v.label.stsd, Value: fv}, s.sgl)
				}
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
			// StatsD iff changed
			if !s.statsdDisabled() && changed {
				if v.kind == KindCounter {
					s.statsdC.AppMetric(metric{Type: statsd.Counter, Name: v.label.stsd, Value: val}, s.sgl)
				} else {
					// target only suffix
					metricType := statsd.Counter
					if v.label.comm == "dl" {
						metricType = statsd.PersistentCounter
					}
					s.statsdC.AppMetric(metric{Type: metricType, Name: v.label.stsd, Value: float64(val)}, s.sgl)
				}
			}
		case KindGauge:
			val := ratomic.LoadInt64(&v.Value)
			out[name] = copyValue{val}
			if !s.statsdDisabled() {
				s.statsdC.AppMetric(metric{Type: statsd.Gauge, Name: v.label.stsd, Value: float64(val)}, s.sgl)
			}
			if isDiskUtilMetric(name) && val > diskLowUtil[0] {
				idle = false
			}
		default:
			out[name] = copyValue{ratomic.LoadInt64(&v.Value)}
		}
	}
	if !s.statsdDisabled() {
		s.statsdC.SendSGL(s.sgl)
	}
	return idle
}

// REST API what=stats query
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

// common (target, proxy) metrics
func (r *runner) regCommon(snode *meta.Snode) {
	// basic counters
	r.reg(snode, GetCount, KindCounter)
	r.reg(snode, PutCount, KindCounter)
	r.reg(snode, AppendCount, KindCounter)
	r.reg(snode, DeleteCount, KindCounter)
	r.reg(snode, RenameCount, KindCounter)
	r.reg(snode, ListCount, KindCounter)

	// basic error counters, respectively
	r.reg(snode, errPrefix+GetCount, KindCounter)
	r.reg(snode, errPrefix+PutCount, KindCounter)
	r.reg(snode, errPrefix+AppendCount, KindCounter)
	r.reg(snode, errPrefix+DeleteCount, KindCounter)
	r.reg(snode, errPrefix+RenameCount, KindCounter)
	r.reg(snode, errPrefix+ListCount, KindCounter)

	// more error counters
	r.reg(snode, ErrHTTPWriteCount, KindCounter)
	r.reg(snode, ErrDownloadCount, KindCounter)
	r.reg(snode, ErrPutMirrorCount, KindCounter)

	// latency
	r.reg(snode, GetLatency, KindLatency)
	r.reg(snode, GetLatencyTotal, KindTotal)
	r.reg(snode, ListLatency, KindLatency)
	r.reg(snode, KeepAliveLatency, KindLatency)

	// special uptime
	r.reg(snode, Uptime, KindSpecial)

	// snode state flags
	r.reg(snode, NodeStateFlags, KindGauge)
}

// NOTE naming convention: ".n" for the count and ".ns" for duration (nanoseconds)
// compare with coreStats.initProm()
func (r *runner) reg(snode *meta.Snode, name, kind string) {
	v := &statsValue{kind: kind}
	// in StatsD metrics ":" delineates the name and the value - replace with underscore
	switch kind {
	case KindCounter:
		debug.Assert(strings.HasSuffix(name, ".n"), name) // naming convention
		v.label.comm = strings.TrimSuffix(name, ".n")
		v.label.comm = strings.ReplaceAll(v.label.comm, ":", "_")
		v.label.stsd = fmt.Sprintf("%s.%s.%s.%s", "ais"+snode.Type(), snode.ID(), v.label.comm, "count")
	case KindTotal:
		debug.Assert(strings.HasSuffix(name, ".total"), name) // naming convention
		v.label.comm = strings.ReplaceAll(v.label.comm, ":", "_")
		v.label.stsd = fmt.Sprintf("%s.%s.%s.%s", "ais"+snode.Type(), snode.ID(), v.label.comm, "total")
	case KindSize:
		debug.Assert(strings.HasSuffix(name, ".size"), name) // naming convention
		v.label.comm = strings.TrimSuffix(name, ".size")
		v.label.comm = strings.ReplaceAll(v.label.comm, ":", "_")
		v.label.stsd = fmt.Sprintf("%s.%s.%s.%s", "ais"+snode.Type(), snode.ID(), v.label.comm, "mbytes")
	case KindLatency:
		debug.Assert(strings.Contains(name, ".ns"), name) // ditto
		v.label.comm = strings.TrimSuffix(name, ".ns")
		v.label.comm = strings.ReplaceAll(v.label.comm, ".ns.", ".")
		v.label.comm = strings.ReplaceAll(v.label.comm, ":", "_")
		v.label.stsd = fmt.Sprintf("%s.%s.%s.%s", "ais"+snode.Type(), snode.ID(), v.label.comm, "ms")
	case KindThroughput, KindComputedThroughput:
		debug.Assert(strings.HasSuffix(name, ".bps"), name) // ditto
		v.label.comm = strings.TrimSuffix(name, ".bps")
		v.label.comm = strings.ReplaceAll(v.label.comm, ":", "_")
		v.label.stsd = fmt.Sprintf("%s.%s.%s.%s", "ais"+snode.Type(), snode.ID(), v.label.comm, "mbps")
	default:
		debug.Assert(kind == KindGauge || kind == KindSpecial)
		v.label.comm = name
		v.label.comm = strings.ReplaceAll(v.label.comm, ":", "_")
		if name == Uptime {
			v.label.comm = strings.ReplaceAll(v.label.comm, ".ns.", ".")
			v.label.stsd = fmt.Sprintf("%s.%s.%s.%s", "ais"+snode.Type(), snode.ID(), v.label.comm, "seconds")
		} else {
			v.label.stsd = fmt.Sprintf("%s.%s.%s", "ais"+snode.Type(), snode.ID(), v.label.comm)
		}
	}
	r.core.Tracker[name] = v
}

func (*runner) IsPrometheus() bool { return false }

func (r *runner) Stop(err error) {
	nlog.Infof("Stopping %s, err: %v", r.Name(), err)
	r.stopCh <- struct{}{}
	r.core.statsdC.Close()
	close(r.stopCh)
}
