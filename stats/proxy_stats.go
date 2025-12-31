// Package stats provides methods and functionality to register, track, log,
// and export metrics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/core/meta"
)

const numProxyStats = 35 // approx. initial

// Authentication and Authorization metrics
const (
	AuthTotalCount        = "auth.total.n"
	AuthSuccessCount      = "auth.success.n"
	AuthFailCount         = "auth.fail.n"
	AuthNoTokenCount      = "auth.notoken.n"
	AuthInvalidTokenCount = "auth.invalidtoken.n"
	AuthInvalidIssCount   = "auth.invalidiss.n"
	AuthInvalidKidCount   = "auth.invalidkid.n"
	AuthExpiredTokenCount = "auth.expiredtoken.n"
	ACLTotalCount         = "acl.total.n"
	ACLDeniedCount        = "acl.denied.n"

	AuthIssHist  = "auth.iss"
	AuthJWKSHist = "auth.jwks"
)

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
	r.regAuth(p.Snode())

	r.core.statsTime = cmn.GCO.Get().Periodic.StatsTime.D()
	r.ctracker = make(copyTracker, numProxyStats)

	r.runner.name = "proxystats"
	r.runner.node = p

	r.runner.stopCh = make(chan struct{}, 4)

	r.sorted = make([]string, 0, numProxyStats)
	return &r.runner.startedUp
}

func (r *Prunner) regAuth(snode *meta.Snode) {
	r.reg(snode, AuthTotalCount, KindCounter,
		&Extra{
			Help: "total number of token validation attempts",
		},
	)
	r.reg(snode, AuthSuccessCount, KindCounter,
		&Extra{
			Help: "total number of successful authentications",
		},
	)
	r.reg(snode, AuthFailCount, KindCounter,
		&Extra{
			Help: "total number of failed authentication attempts",
		},
	)
	r.reg(snode, AuthNoTokenCount, KindCounter,
		&Extra{
			Help: "authentication failures due to missing token",
		},
	)
	r.reg(snode, AuthInvalidTokenCount, KindCounter,
		&Extra{
			Help: "authentication failures due to invalid token",
		},
	)
	r.reg(snode, AuthInvalidIssCount, KindCounter,
		&Extra{
			Help: "authentication failures due to invalid token issuer",
		},
	)
	r.reg(snode, AuthInvalidKidCount, KindCounter,
		&Extra{
			Help: "authentication failures due to invalid token key ID",
		},
	)
	r.reg(snode, AuthExpiredTokenCount, KindCounter,
		&Extra{
			Help: "authentication failures due to expired token",
		},
	)
	r.reg(snode, ACLTotalCount, KindCounter,
		&Extra{
			Help: "total number of ACL permission checks",
		},
	)
	r.reg(snode, ACLDeniedCount, KindCounter,
		&Extra{
			Help: "total number of ACL permission denials",
		},
	)
	r.reg(snode, AuthIssHist, KindHistogram,
		&Extra{
			Help:    "histogram of request latency for dynamic issuers, in seconds",
			Buckets: []float64{.05, 0.2, 5},
		},
	)
	r.reg(snode, AuthJWKSHist, KindHistogram,
		&Extra{
			Help:    "histogram of request latency for JWKS fetches, in seconds",
			Buckets: []float64{.05, 0.2, 5},
		},
	)
}

//
// statsLogger interface impl
//

func (r *Prunner) log(now int64, uptime time.Duration, config *cmn.Config) {
	s := r.core
	s.updateUptime(uptime)
	idle := s.copyT(r.ctracker)

	verbose := cmn.Rom.V(4, cos.ModStats)

	if (!idle && now >= r.next) || verbose {
		s.sgl.Reset() // sharing w/ CoreStats.copyT
		r.write(s.sgl, false /*target*/, idle)
		if l := s.sgl.Len(); l > 3 { // skip '{}'
			line := string(s.sgl.Bytes())
			debug.Assert(l < s.sgl.Slab().Size(), l, " vs slab ", s.sgl.Slab().Size())
			if line != r.prev {
				nlog.Infoln(line)
				r.prev = line
			}
		}
		r._next(config, now)
	}

	// memory and CPU alerts
	r._memload(r.node.PageMM(), 0, 0)
}

func (r *Prunner) statsTime(newval time.Duration) {
	r.core.statsTime = newval
}

func (*Prunner) standingBy() bool { return false }
