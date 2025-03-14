// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
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

// TODO: support RateLimitConf.Verbs, here and elsewhere

type tcrate struct {
	src struct {
		rl    *cos.AdaptRateLim
		sleep time.Duration
	}
	dst struct {
		rl    *cos.AdaptRateLim
		sleep time.Duration
	}
	onerr *cos.AdaptRateLim // the one we use to handle (409, 503)
}

func newrate(src, dst *meta.Bck, nat int) *tcrate {
	var rate tcrate
	rate.src.rl, rate.src.sleep = src.NewBackendRateLim(nat)
	if dst.Props != nil { // destination may not exist
		rate.dst.rl, rate.dst.sleep = dst.NewBackendRateLim(nat)
	}

	switch {
	case rate.src.rl != nil && rate.dst.rl != nil:
		nlog.Warningln("both source and destination buckets are rate limited:", src.Cname(""), dst.Cname(""))
		if src.IsRemote() {
			if dst.IsRemote() {
				nlog.Warningln("\tchoosing destination")
				rate.onerr = rate.dst.rl
			} else {
				nlog.Warningln("\tchoosing source")
				rate.onerr = rate.src.rl
			}
		} else {
			nlog.Warningln("\tchoosing destination")
			rate.onerr = rate.dst.rl
		}
	case rate.src.rl != nil:
		rate.onerr = rate.src.rl
	case rate.dst.rl != nil:
		rate.onerr = rate.dst.rl
	default:
		return nil // n/a
	}

	return &rate
}

// TODO -- FIXME: remove
func (rate *tcrate) onmanyreq(vlabs map[string]string) {
	arl := rate.onerr
	tstats := core.T.StatsUpdater()
	sleep := arl.OnErr()
	tstats.AddWith(
		cos.NamedVal64{Name: stats.GetRateRetryLatencyTotal, Value: int64(sleep), VarLabs: vlabs},
	)
}
