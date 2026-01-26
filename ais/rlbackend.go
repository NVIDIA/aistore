// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xact"
)

// rate-limit 4 backend APIs:
// - GetObj()
// - GetObjReader()
// - PutObj()
// - DeleteObj()

// stats:
// - not counting proactive delay    - only (reactive) retries
// - not counting individual retries - only totals (see "increment" comment below)
// - not counting delete retries     - only get and put

type (
	rlbackend struct {
		core.Backend
		t *target
	}
)

func (bp *rlbackend) GetObj(ctx context.Context, lom *core.LOM, owt cmn.OWT, origReq *http.Request) (int, error) {
	// proactive
	arl := bp.acquire(lom.Bck(), http.MethodGet)
	ecode, err := bp.Backend.GetObj(ctx, lom, owt, origReq)
	if err == nil || arl == nil || !cmn.IsErrTooManyRequests(err) {
		return ecode, err
	}

	cb := func() (int, error) {
		return bp.Backend.GetObj(ctx, lom, owt, origReq)
	}
	total, code, e := bp.retry(ctx, arl, cb)

	// increment retry count by 1, retry latency by `total`
	bp.stats(ctx, lom.Bck(), stats.RatelimGetRetryCount, stats.RatelimGetRetryLatencyTotal, total)
	return code, e
}

func (bp *rlbackend) GetObjReader(ctx context.Context, lom *core.LOM, offset, length int64) (res core.GetReaderResult) {
	// proactive
	arl := bp.acquire(lom.Bck(), http.MethodGet)
	res = bp.Backend.GetObjReader(ctx, lom, offset, length)
	if res.Err == nil || arl == nil || !cmn.IsErrTooManyRequests(res.Err) {
		return res
	}

	cb := func() (int, error) {
		res = bp.Backend.GetObjReader(ctx, lom, offset, length)
		return res.ErrCode, res.Err
	}
	total, code, e := bp.retry(ctx, arl, cb)
	debug.Assertf(res.ErrCode == code && res.Err == e, "(%d, %v) vs (%d, %v)", res.ErrCode, res.Err, code, e)

	// ditto
	bp.stats(ctx, lom.Bck(), stats.RatelimGetRetryCount, stats.RatelimGetRetryLatencyTotal, total)
	return res
}

func (bp *rlbackend) PutObj(ctx context.Context, r io.ReadCloser, lom *core.LOM, origReq *http.Request) (int, error) {
	// proactive
	arl := bp.acquire(lom.Bck(), http.MethodPut)
	ecode, err := bp.Backend.PutObj(ctx, r, lom, origReq)
	if err == nil || arl == nil || !cmn.IsErrTooManyRequests(err) {
		return ecode, err
	}

	cb := func() (int, error) {
		return bp.Backend.PutObj(ctx, r, lom, origReq)
	}
	total, code, e := bp.retry(ctx, arl, cb)

	bp.stats(ctx, lom.Bck(), stats.RatelimPutRetryCount, stats.RatelimPutRetryLatencyTotal, total)
	return code, e
}

// note: not counting stats
func (bp *rlbackend) DeleteObj(ctx context.Context, lom *core.LOM) (int, error) {
	// proactive
	arl := bp.acquire(lom.Bck(), http.MethodDelete)
	ecode, err := bp.Backend.DeleteObj(ctx, lom)
	if err == nil || arl == nil || !cmn.IsErrTooManyRequests(err) {
		return ecode, err
	}

	cb := func() (int, error) {
		return bp.Backend.DeleteObj(ctx, lom)
	}
	_, code, e := bp.retry(ctx, arl, cb)
	return code, e
}

//
// apply (compare with ais/prate_limit)
//

func (bp *rlbackend) acquire(bck *meta.Bck, verb string) (arl *cos.AdaptRateLim) {
	if !bck.Props.RateLimit.Backend.Enabled {
		return nil
	}
	var (
		uhash = bck.HashUname(verb) // see also (*RateLimitConf)verbs
		v, ok = bp.t.ratelim.Load(uhash)
	)
	if ok {
		arl = v.(*cos.AdaptRateLim)
	} else {
		smap := bp.t.owner.smap.get()
		arl = bck.NewBackendRateLim(smap.CountActiveTs())
		bp.t.ratelim.Store(uhash, arl)
	}

	arl.RetryAcquire(time.Second)
	return arl
}

func (*rlbackend) retry(ctx context.Context, arl *cos.AdaptRateLim, cb func() (int, error)) (total time.Duration, ecode int, err error) {
	for total < cos.DfltRateMaxWait {
		// reactive
		sleep := arl.OnErr()
		total += sleep
		if err = ctx.Err(); err != nil {
			break
		}
		ecode, err = cb()
		if err == nil || !cmn.IsErrTooManyRequests(err) {
			break
		}
	}
	return total, ecode, err
}

func (bp *rlbackend) stats(ctx context.Context, bck *meta.Bck, count, latency string, total time.Duration) {
	if total == 0 {
		return
	}
	vlabs := xact.GetCtxVlabs(ctx) // fast path
	if vlabs == nil {
		vlabs = map[string]string{stats.VlabBucket: bck.Cname(""), stats.VlabXkind: ""}
	}
	bp.t.statsT.IncWith(count, vlabs)
	bp.t.statsT.AddWith(
		cos.NamedVal64{Name: latency, Value: int64(total), VarLabs: vlabs},
	)
}
