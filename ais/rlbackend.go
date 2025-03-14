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
	"github.com/NVIDIA/aistore/xact"
)

// NOTE: backend.GetObjReader is handled elsewhere - by xactions

type (
	rlbackend struct {
		core.Backend
		t *target
	}
)

func (bp *rlbackend) GetObj(ctx context.Context, lom *core.LOM, owt cmn.OWT, origReq *http.Request) (ecode int, err error) {
	ecode, err = bp.Backend.GetObj(ctx, lom, owt, origReq)
	if err == nil {
		return ecode, nil
	}

	var tot time.Duration
	for tot < cos.DfltRateMaxWait {
		sleep := bp.retry(err, lom.Bck(), http.MethodGet)
		if sleep == 0 {
			break
		}
		tot += sleep
		ecode, err = bp.Backend.GetObj(ctx, lom, owt, origReq)
		if err == nil {
			break
		}
	}
	bp.stats(ctx, lom.Bck(), stats.GetRateRetryLatencyTotal, tot)
	return ecode, err
}

func (bp *rlbackend) PutObj(ctx context.Context, r io.ReadCloser, lom *core.LOM, origReq *http.Request) (ecode int, err error) {
	ecode, err = bp.Backend.PutObj(ctx, r, lom, origReq)
	if err == nil {
		return ecode, nil
	}

	var tot time.Duration
	for tot < cos.DfltRateMaxWait {
		sleep := bp.retry(err, lom.Bck(), http.MethodPut)
		if sleep == 0 {
			break
		}
		tot += sleep
		ecode, err = bp.Backend.PutObj(ctx, r, lom, origReq)
		if err == nil {
			break
		}
	}
	bp.stats(ctx, lom.Bck(), stats.PutRateRetryLatencyTotal, tot)
	return ecode, err
}

func (bp *rlbackend) DeleteObj(ctx context.Context, lom *core.LOM) (ecode int, err error) {
	ecode, err = bp.Backend.DeleteObj(ctx, lom)
	if err == nil {
		return ecode, nil
	}

	var tot time.Duration
	for tot < cos.DfltRateMaxWait {
		sleep := bp.retry(err, lom.Bck(), http.MethodDelete)
		if sleep == 0 {
			break
		}
		tot += sleep
		ecode, err = bp.Backend.DeleteObj(ctx, lom)
		if err == nil {
			break
		}
	}
	bp.stats(ctx, lom.Bck(), stats.DeleteRateRetryLatencyTotal, tot)
	return ecode, err
}

//
// apply (compare with ais/prate_limit)
//

func (bp *rlbackend) retry(err error, bck *meta.Bck, verb string) time.Duration {
	if !cmn.IsErrTooManyRequests(err) || !bck.Props.RateLimit.Backend.Enabled {
		return 0 // not retrying
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
	return arl.OnErr()
}

func (bp *rlbackend) stats(ctx context.Context, bck *meta.Bck, metric string, total time.Duration) {
	if total == 0 {
		return
	}
	vlabs := xact.GetCtxVlabs(ctx) // fast path
	if vlabs == nil {
		vlabs = map[string]string{stats.VarlabBucket: bck.Cname(""), stats.VarlabXactKind: ""}
	}
	bp.t.statsT.AddWith(
		cos.NamedVal64{Name: metric, Value: int64(total), VarLabs: vlabs},
	)
}
