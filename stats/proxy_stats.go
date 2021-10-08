// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

type (
	Prunner struct {
		statsRunner
	}
	ClusterStats struct {
		Proxy  *CoreStats          `json:"proxy"`
		Target map[string]*Trunner `json:"target"`
	}
	ClusterStatsRaw struct {
		Proxy  *CoreStats      `json:"proxy"`
		Target cos.JSONRawMsgs `json:"target"`
	}
)

/////////////
// Prunner //
/////////////

// interface guard
var _ cos.Runner = (*Prunner)(nil)

func (r *Prunner) Run() error { return r.runcommon(r) }

// NOTE: have only common metrics (see regCommon()) - init only the Prometheus part if used
func (r *Prunner) RegMetrics(node *cluster.Snode) {
	r.Core.initProm(node)
}

// All stats that proxy currently has are CoreStats which are registered at startup
func (r *Prunner) Init(p cluster.Node) *atomic.Bool {
	r.Core = &CoreStats{}
	r.Core.init(p.Snode(), 24)
	r.Core.statsTime = cmn.GCO.Get().Periodic.StatsTime.D()
	r.ctracker = make(copyTracker, 24)

	r.statsRunner.name = "proxystats"
	r.statsRunner.daemon = p

	r.statsRunner.stopCh = make(chan struct{}, 4)
	r.statsRunner.workCh = make(chan NamedVal64, 256)

	r.Core.initMetricClient(p.Snode(), &r.statsRunner)

	return &r.statsRunner.startedUp
}

// TODO: fix the scope of the return type
func (r *Prunner) GetWhatStats() interface{} {
	ctracker := make(copyTracker, 24)
	r.Core.copyCumulative(ctracker)
	return ctracker
}

//
// statsLogger interface impl
//

func (r *Prunner) log(now int64, uptime time.Duration) {
	r.Core.updateUptime(uptime)
	r.Core.promLock()
	idle := r.Core.copyT(r.ctracker, []string{"kalive", Uptime})
	r.Core.promUnlock()
	if now >= r.nextLogTime && !idle {
		b := cos.MustMarshal(r.ctracker)
		glog.Infoln(string(b))
		r.nextLogTime = now + cos.MinI64(int64(r.Core.statsTime)*logIntervalMult, logIntervalMax)
	}
}

func (r *Prunner) doAdd(nv NamedVal64) {
	s := r.Core
	s.doAdd(nv.Name, nv.NameSuffix, nv.Value)
}

func (r *Prunner) statsTime(newval time.Duration) {
	r.Core.statsTime = newval
}

func (*Prunner) standingBy() bool { return false }
