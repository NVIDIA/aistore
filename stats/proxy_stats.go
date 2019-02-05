// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats/statsd"
	jsoniter "github.com/json-iterator/go"
)

//
// NOTE Naming Convention: "*.n" - counter, "*.µs" - latency, "*.size" - size (in bytes)
//

type (
	ProxyCoreStats struct {
		Tracker   statsTracker
		StatsdC   *statsd.Client `json:"-"`
		statsTime time.Duration
	}
	Prunner struct {
		statsRunner
		Core *ProxyCoreStats `json:"core"`
	}
	ClusterStats struct {
		Proxy  *ProxyCoreStats     `json:"proxy"`
		Target map[string]*Trunner `json:"target"`
	}
	ClusterStatsRaw struct {
		Proxy  *ProxyCoreStats                `json:"proxy"`
		Target map[string]jsoniter.RawMessage `json:"target"`
	}
)

//
// ProxyCoreStats
// all stats that proxy currently has are common and hardcoded at startup
//
func (s *ProxyCoreStats) init(size int) {
	s.Tracker = make(statsTracker, size)
	s.Tracker.registerCommonStats()
}

func (p *ProxyCoreStats) MarshalJSON() ([]byte, error) { return jsoniter.Marshal(p.Tracker) }
func (p *ProxyCoreStats) UnmarshalJSON(b []byte) error { return jsoniter.Unmarshal(b, &p.Tracker) }

//
// NOTE naming convention: ".n" for the count and ".µs" for microseconds
//
func (s *ProxyCoreStats) doAdd(name string, val int64) {
	v, ok := s.Tracker[name]
	cmn.Assert(ok, "Invalid stats name '"+name+"'")
	if v.kind == KindLatency {
		if strings.HasSuffix(name, ".µs") {
			nroot := strings.TrimSuffix(name, ".µs")
			s.StatsdC.Send(nroot, metric{statsd.Timer, "latency", float64(time.Duration(val) / time.Millisecond)})
		}
		v.Lock()
		v.numSamples++
		val = int64(time.Duration(val) / time.Microsecond)
		v.cumulative += val
		v.Value += val
		v.Unlock()
	} else if v.kind == KindThroughput {
		v.Lock()
		v.cumulative += val
		v.Value += val
		v.Unlock()
	} else if v.kind == KindCounter && strings.HasSuffix(name, ".n") {
		nroot := strings.TrimSuffix(name, ".n")
		s.StatsdC.Send(nroot, metric{statsd.Counter, "count", val})
		v.Lock()
		v.Value += val
		v.Unlock()
	}
}

func (s *ProxyCoreStats) copyZeroReset(ctracker copyTracker) {
	for name, v := range s.Tracker {
		if v.kind == KindLatency {
			v.Lock()
			if v.numSamples > 0 {
				ctracker[name] = &copyValue{Value: v.Value / v.numSamples} // note: int divide
			}
			v.Value = 0
			v.numSamples = 0
			v.Unlock()
		} else if v.kind == KindThroughput {
			v.Lock()
			cmn.Assert(s.statsTime.Seconds() > 0, "ProxyCoreStats: statsTime not set")
			throughput := v.Value / int64(s.statsTime.Seconds()) // note: int divide
			ctracker[name] = &copyValue{Value: throughput}
			v.Value = 0
			v.Unlock()
			if strings.HasSuffix(name, ".bps") {
				nroot := strings.TrimSuffix(name, ".bps")
				s.StatsdC.Send(nroot,
					metric{Type: statsd.Gauge, Name: "throughput", Value: throughput},
				)
			}
		} else if v.kind == KindCounter {
			v.RLock()
			if v.Value != 0 {
				ctracker[name] = &copyValue{Value: v.Value}
			}
			v.RUnlock()
		} else {
			ctracker[name] = &copyValue{Value: v.Value} // KindSpecial as is and wo/ lock
		}
	}
}

func (s *ProxyCoreStats) copyCumulative(ctracker copyTracker) {
	// serves to satisfy REST API what=stats query

	for name, v := range s.Tracker {
		v.RLock()
		if v.kind == KindLatency || v.kind == KindThroughput {
			ctracker[name] = &copyValue{Value: v.cumulative}
		} else if v.kind == KindCounter {
			if v.Value != 0 {
				ctracker[name] = &copyValue{Value: v.Value}
			}
		} else {
			ctracker[name] = &copyValue{Value: v.Value} // KindSpecial as is and wo/ lock
		}
		v.RUnlock()
	}
}

//
// Prunner
//
func (r *Prunner) Run() error { return r.runcommon(r) }

func (r *Prunner) Init() {
	r.Core = &ProxyCoreStats{}
	r.Core.init(24)
	r.Core.statsTime = cmn.GCO.Get().Periodic.StatsTime
	r.ctracker = make(copyTracker, 24)

	// subscribe to config changes
	cmn.GCO.Subscribe(r)
}

func (r *Prunner) ConfigUpdate(oldConf, newConf *cmn.Config) {
	r.statsRunner.ConfigUpdate(oldConf, newConf)
	r.Core.statsTime = newConf.Periodic.StatsTime
}

func (r *Prunner) GetWhatStats() ([]byte, error) {
	ctracker := make(copyTracker, 24)
	r.Core.copyCumulative(ctracker)
	return jsonCompat.Marshal(ctracker)
}

// statslogger interface impl
func (r *Prunner) log() (runlru bool) {
	// copy stats values while skipping zeros; reset latency stats
	r.Core.copyZeroReset(r.ctracker)

	b, err := jsonCompat.Marshal(r.ctracker)
	if err == nil {
		glog.Infoln(string(b))
	}
	return
}

func (r *Prunner) doAdd(nv NamedVal64) {
	s := r.Core
	s.doAdd(nv.Name, nv.Val)
}
