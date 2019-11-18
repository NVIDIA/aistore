// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
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
		Proxy  *CoreStats      `json:"proxy"`
		Target cmn.JSONRawMsgs `json:"target"`
	}
)

// the items that change in every collection cycle; the system is idle when there are
// no other stats changes (see also targetLogIdleItems)
var proxyLogIdleItems = []string{"kalive.µs.min", "kalive.µs", "kalive.µs.max", "pst.n"}

//
// Prunner
//
func (r *Prunner) Run() error                  { return r.runcommon(r) }
func (r *Prunner) Get(name string) (val int64) { return r.Core.get(name) }

// All stats that proxy currently has are CoreStats which are registered at startup
func (r *Prunner) Init(daemonStr, daemonID string, daemonStarted *atomic.Bool) *atomic.Bool {
	r.Core = &CoreStats{}
	r.Core.init(24)
	r.Core.statsTime = cmn.GCO.Get().Periodic.StatsTime
	r.ctracker = make(copyTracker, 24)
	r.Core.initStatsD(daemonStr, daemonID)

	r.statsRunner.logLimit = cmn.DivCeil(int64(logsMaxSizeCheckTime), int64(r.Core.statsTime))
	r.statsRunner.daemonStarted = daemonStarted

	r.statsRunner.stopCh = make(chan struct{}, 4)
	r.statsRunner.workCh = make(chan NamedVal64, 256)

	// subscribe to config changes
	cmn.GCO.Subscribe(r)
	return &r.statsRunner.startedUp
}

func (r *Prunner) ConfigUpdate(oldConf, newConf *cmn.Config) {
	r.statsRunner.ConfigUpdate(oldConf, newConf)
	r.Core.statsTime = newConf.Periodic.StatsTime
}

func (r *Prunner) GetWhatStats() []byte {
	ctracker := make(copyTracker, 24)
	r.Core.copyCumulative(ctracker)
	return cmn.MustMarshal(ctracker)
}

// statslogger interface impl
func (r *Prunner) log(uptime time.Duration) (runlru bool) {
	r.Core.UpdateUptime(uptime)
	n := r.Core.copyT(r.ctracker)
	if n > len(proxyLogIdleItems) {
		b, _ := jsonCompat.Marshal(r.ctracker)
		glog.Infoln(string(b))
	}
	return
}

func (r *Prunner) doAdd(nv NamedVal64) {
	s := r.Core
	s.doAdd(nv.Name, nv.Value)
}

func (r *Prunner) housekeep(runlru bool) {
	r.statsRunner.housekeep(runlru)
}
