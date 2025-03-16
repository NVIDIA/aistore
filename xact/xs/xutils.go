// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
)

func rgetstats(bp core.Backend, vlabs map[string]string, size, started int64) {
	tstats := core.T.StatsUpdater()
	delta := mono.SinceNano(started)
	tstats.IncWith(bp.MetricName(stats.GetCount), vlabs)
	tstats.AddWith(
		cos.NamedVal64{Name: bp.MetricName(stats.GetLatencyTotal), Value: delta, VarLabs: vlabs},
		cos.NamedVal64{Name: bp.MetricName(stats.GetSize), Value: size, VarLabs: vlabs},
	)
}

////////////
// tcrate //
////////////

// TODO:
// - support RateLimitConf.Verbs, here and elsewhere
// - `nat` with no respect to num-workers, ditto

type tcrate struct {
	src *cos.BurstRateLim
	dst *cos.BurstRateLim
}

func (rate *tcrate) init(src, dst *meta.Bck, nat int) {
	rate.src = src.NewFrontendRateLim(nat)
	if dst.Props != nil { // destination may not exist
		rate.dst = dst.NewFrontendRateLim(nat)
	}
}
