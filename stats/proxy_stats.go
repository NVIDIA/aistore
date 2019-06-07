// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

//
// NOTE Naming Convention: "*.n" - counter, "*.µs" - latency, "*.size" - size (in bytes)
//

type (
	Prunner struct {
		statsRunner
		Core *CoreStats `json:"core"`
	}
	ClusterStats struct {
		Proxy  *CoreStats          `json:"proxy"`
		Target map[string]*Trunner `json:"target"`
	}
	ClusterStatsRaw struct {
		Proxy  *CoreStats                     `json:"proxy"`
		Target map[string]jsoniter.RawMessage `json:"target"`
	}
)

//
// Prunner
//
func (r *Prunner) Run() error            { return r.runcommon(r) }
func (r *Prunner) Get(name string) int64 { return r.Core.Tracker[name].Value }

// All stats that proxy currently has are CoreStats which are registered at startup
func (r *Prunner) Init(daemonStr, daemonID string) {
	r.Core = &CoreStats{}
	r.Core.init(24)
	r.Core.statsTime = cmn.GCO.Get().Periodic.StatsTime
	r.ctracker = make(copyTracker, 24)
	r.Core.initStatsD(daemonStr, daemonID)

	r.statsRunner.logLimit = cmn.DivCeil(int64(logsMaxSizeCheckTime), int64(r.Core.statsTime))
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

func (r *Prunner) housekeep(runlru bool) {
	r.statsRunner.housekeep(runlru)
}
