// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
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

// NOTE: currently, proxy's stats == common and hardcoded

type Prunner struct {
	statsRunner
}

/////////////
// Prunner //
/////////////

// interface guard
var _ cos.Runner = (*Prunner)(nil)

func (r *Prunner) Run() error { return r.runcommon(r) }

// NOTE: have only common metrics (see regCommon()) - init only the Prometheus part if used
func (r *Prunner) RegMetrics(node *cluster.Snode) {
	r.core.initProm(node)
}

// All stats that proxy currently has are CoreStats which are registered at startup
func (r *Prunner) Init(p cluster.Node) *atomic.Bool {
	r.core = &coreStats{}
	r.core.init(p.Snode(), 24)
	r.core.statsTime = cmn.GCO.Get().Periodic.StatsTime.D()
	r.ctracker = make(copyTracker, 24)

	r.statsRunner.name = "proxystats"
	r.statsRunner.daemon = p

	r.statsRunner.stopCh = make(chan struct{}, 4)
	r.statsRunner.workCh = make(chan cos.NamedVal64, 256)

	r.core.initMetricClient(p.Snode(), &r.statsRunner)

	return &r.statsRunner.startedUp
}

//
// statsLogger interface impl
//

func (r *Prunner) log(now int64, uptime time.Duration, config *cmn.Config) {
	r.core.updateUptime(uptime)
	r.core.promLock()
	idle := r.core.copyT(r.ctracker)
	r.core.promUnlock()
	if now >= r.nextLogTime && !idle {
		b := cos.MustMarshal(r.ctracker)
		glog.Infoln(string(b))
		i := int64(config.Log.StatsTime)
		if i == 0 {
			i = dfltStatsLogInterval
		}
		r.nextLogTime = now + cos.MinI64(i, maxStatsLogInterval)
	}
}

func (r *Prunner) doAdd(nv cos.NamedVal64) {
	s := r.core
	s.doAdd(nv.Name, nv.NameSuffix, nv.Value)
}

func (r *Prunner) statsTime(newval time.Duration) {
	r.core.statsTime = newval
}

func (*Prunner) standingBy() bool { return false }
