/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
package stats

import (
	"strings"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/stats/statsd"
	jsoniter "github.com/json-iterator/go"
)

//
// NOTE Naming Convention: "*.n" - counter, "*.μs" - latency, "*.size" - size (in bytes)
//

type (
	ProxyCoreStats struct {
		Tracker statsTracker
		StatsdC *statsd.Client `json:"-"`
		logged  bool
	}
	Prunner struct {
		statsrunner
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

// NOTE naming convention: ".n" for the count and ".μs" for microseconds
func (s *ProxyCoreStats) doAdd(name string, val int64) {
	v, ok := s.Tracker[name]
	cmn.Assert(ok, "Invalid stats name '"+name+"'")
	if v.kind == KindLatency {
		s.Tracker[name].numSamples++
		nroot := strings.TrimSuffix(name, ".μs")
		s.StatsdC.Send(nroot,
			metric{statsd.Counter, "count", 1},
			metric{statsd.Timer, "latency", float64(time.Duration(val) / time.Millisecond)})
		val = int64(time.Duration(val) / time.Microsecond)
	} else if v.kind == KindCounter && strings.HasSuffix(name, ".n") {
		nameLatency := strings.TrimSuffix(name, "n") + "μs"
		if _, ok = s.Tracker[nameLatency]; !ok {
			s.StatsdC.Send(name, metric{statsd.Counter, name, val})
		}
	}
	s.Tracker[name].Value += val
	s.logged = false
}

//
// Prunner
//
func (r *Prunner) Run() error { return r.runcommon(r) }

func (r *Prunner) Init() {
	r.Core = &ProxyCoreStats{}
	r.Core.init(24)
}

// statslogger interface impl
func (r *Prunner) log() (runlru bool) {
	r.Lock()
	if r.Core.logged {
		r.Unlock()
		return
	}
	for _, v := range r.Core.Tracker {
		if v.kind == KindLatency && v.numSamples > 0 {
			v.Value /= v.numSamples
		}
	}
	b, err := jsoniter.Marshal(r.Core)

	// reset all the latency stats only
	for _, v := range r.Core.Tracker {
		if v.kind == KindLatency {
			v.Value = 0
			v.numSamples = 0
		}
	}
	r.Unlock()

	if err == nil {
		glog.Infoln(string(b))
		r.Core.logged = true
	}
	return
}

func (r *Prunner) doAdd(nv NamedVal64) {
	r.Lock()
	s := r.Core
	s.doAdd(nv.Name, nv.Val)
	r.Unlock()
}
