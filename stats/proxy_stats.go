/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/stats/statsd"
	jsoniter "github.com/json-iterator/go"
)

type (
	ProxyCoreStats struct {
		Tracker statsTracker
		// omitempty
		StatsdC *statsd.Client
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

func (p *ProxyCoreStats) initStatsTracker() {
	p.Tracker = statsTracker(map[string]*statsInstance{})
	p.Tracker.registerCommonStats()
}

func (p *ProxyCoreStats) MarshalJSON() ([]byte, error) {
	return jsoniter.Marshal(p.Tracker)
}

func (p *ProxyCoreStats) UnmarshalJSON(b []byte) error {
	return jsoniter.Unmarshal(b, &p.Tracker)
}

//
// Prunner
//
func (r *Prunner) Run() error {
	return r.runcommon(r)
}
func (r *Prunner) Init() {
	r.Core = &ProxyCoreStats{}
	r.Core.initStatsTracker()
}

// statslogger interface impl
func (r *Prunner) log() (runlru bool) {
	r.Lock()
	if r.Core.logged {
		r.Unlock()
		return
	}
	for _, v := range r.Core.Tracker {
		if v.kind == statsKindLatency && v.associatedVal > 0 {
			v.Value /= v.associatedVal
		}
	}
	b, err := jsoniter.Marshal(r.Core)

	// reset all the latency stats only
	for _, v := range r.Core.Tracker {
		if v.kind == statsKindLatency {
			v.Value = 0
			v.associatedVal = 0
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

func (s *ProxyCoreStats) doAdd(name string, val int64) {
	if v, ok := s.Tracker[name]; !ok {
		cmn.Assert(false, "Invalid stats name "+name)
	} else if v.kind == statsKindLatency {
		s.Tracker[name].associatedVal++
		s.StatsdC.Send(name,
			metric{statsd.Counter, "count", 1},
			metric{statsd.Timer, "latency", float64(time.Duration(val) / time.Millisecond)})
		val = int64(time.Duration(val) / time.Microsecond)
	} else {
		switch name {
		case PostCount, DeleteCount, RenameCount:
			s.StatsdC.Send(name, metric{statsd.Counter, "count", val})
		}
	}
	s.Tracker[name].Value += val
	s.logged = false
}
