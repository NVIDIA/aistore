// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
)

const numProxyStats = 24 // approx. initial

// NOTE: currently, proxy's stats == common and hardcoded

type Prunner struct {
	runner
}

/////////////
// Prunner //
/////////////

// interface guard
var (
	_ cos.Runner = (*Prunner)(nil)
	_ Tracker    = (*Prunner)(nil)
)

func (r *Prunner) Run() error { return r._run(r /*as statsLogger*/) }

// All stats that proxy currently has are CoreStats which are registered at startup
func (r *Prunner) Init(p core.Node) *atomic.Bool {
	r.core = &coreStats{}

	r.core.init(numProxyStats)

	r.regCommon(p.Snode()) // common metrics

	r.core.statsTime = cmn.GCO.Get().Periodic.StatsTime.D()
	r.ctracker = make(copyTracker, numProxyStats)

	r.runner.name = "proxystats"
	r.runner.node = p

	r.runner.stopCh = make(chan struct{}, 4)

	r.sorted = make([]string, 0, numProxyStats)
	return &r.runner.startedUp
}

//
// statsLogger interface impl
//

func (r *Prunner) log(now int64, uptime time.Duration, config *cmn.Config) {
	s := r.core
	s.updateUptime(uptime)
	s.promLock()
	idle := s.copyT(r.ctracker)
	s.promUnlock()

	if now >= r.next || !idle {
		s.sgl.Reset() // sharing w/ CoreStats.copyT
		r.ctracker.write(s.sgl, r.sorted, false /*target*/, idle)
		if l := s.sgl.Len(); l > 3 { // skip '{}'
			line := string(s.sgl.Bytes())
			debug.Assert(l < s.sgl.Slab().Size(), l, " vs slab ", s.sgl.Slab().Size())
			if line != r.prev {
				nlog.Infoln(line)
				r.prev = line
			}
		}
		if idle {
			r._next(config, now)
		}
	}

	r._mem(r.node.PageMM(), 0, 0)
}

func (r *Prunner) statsTime(newval time.Duration) {
	r.core.statsTime = newval
}

func (*Prunner) standingBy() bool { return false }
