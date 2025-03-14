// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
)

// NOTE: backend.GetObjReader is handled elsewhere - by xactions

// TODO -- FIXME: nil context (below); callers to provide context

type (
	rlbackend struct {
		core.Backend
		t *target
	}
)

func (bp *rlbackend) GetObj(ctx context.Context, lom *core.LOM, owt cmn.OWT, origReq *http.Request) (ecode int, err error) {
	var tot time.Duration
	for tot < cos.DfltRateMaxWait {
		ecode, err = bp.Backend.GetObj(ctx, lom, owt, origReq)
		if err == nil {
			return ecode, nil
		}
		sleep := bp.retry(ctx, err, lom.Bck(), http.MethodGet, stats.GetRateRetryLatencyTotal)
		if sleep == 0 {
			return ecode, err
		}
		tot += sleep
	}
	return ecode, err
}

func (bp *rlbackend) PutObj(ctx context.Context, r io.ReadCloser, lom *core.LOM, origReq *http.Request) (ecode int, err error) {
	var tot time.Duration
	for tot < cos.DfltRateMaxWait {
		ecode, err = bp.Backend.PutObj(ctx, r, lom, origReq)
		if err == nil {
			return ecode, nil
		}
		sleep := bp.retry(ctx, err, lom.Bck(), http.MethodPut, stats.PutRateRetryLatencyTotal)
		if sleep == 0 {
			return ecode, err
		}
		tot += sleep
	}
	return ecode, err
}

func (bp *rlbackend) DeleteObj(ctx context.Context, lom *core.LOM) (ecode int, err error) {
	var tot time.Duration
	for tot < cos.DfltRateMaxWait {
		ecode, err = bp.Backend.DeleteObj(ctx, lom)
		if err == nil {
			return ecode, nil
		}
		sleep := bp.retry(ctx, err, lom.Bck(), http.MethodDelete, stats.DeleteRateRetryLatencyTotal)
		if sleep == 0 {
			return ecode, err
		}
		tot += sleep
	}
	return ecode, err
}

//
// apply (compare with ais/prate_limit)
//

func (bp *rlbackend) retry(ctx context.Context, err error, bck *meta.Bck, verb, metric string) time.Duration {
	if !cmn.IsErrTooManyRequests(err) || !bck.Props.RateLimit.Backend.Enabled {
		return 0
	}

	var (
		arl   *cos.AdaptRateLim
		uhash = bck.HashUname(verb) // see also (*RateLimitConf)verbs
		v, ok = bp.t.ratelim.Load(uhash)
	)
	if ok {
		arl = v.(*cos.AdaptRateLim)
	} else {
		smap := bp.t.owner.smap.get()
		arl, _ = bck.NewBackendRateLim(smap.CountActiveTs())
		bp.t.ratelim.Store(uhash, arl)
	}

	// onerr
	sleep := arl.OnErr()

	_ = ctx // TODO -- FIXME: in progress
	vlabs := map[string]string{stats.VarlabBucket: bck.Cname(""), stats.VarlabXactKind: ""}
	bp.t.StatsUpdater().AddWith(
		cos.NamedVal64{Name: metric, Value: int64(sleep), VarLabs: vlabs},
	)
	return sleep
}
