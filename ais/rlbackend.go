// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"fmt"
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

// rate-limit 5 backend APIs:
// - HeadObj()
// - GetObj()
// - GetObjReader()
// - PutObj()
// - DeleteObj()

// The remaining backend APIs can be added if needed - remote providers may
// rate-limit them as well:
// - CreateBucket()
// - ListBuckets()
// - ListObjects()
// - HeadBucket()
// - StartMpt(), PutMptPart(), CompleteMpt(), AbortMpt()

// stats:
// - not counting proactive delay    - only (reactive) retries
// - not counting individual retries - only totals (see "increment" comment below)
// - not counting delete retries     - only get and put

type (
	rlbackend struct {
		core.Backend
		t *target
	}
	errBackendRetry struct {
		err     error
		retries int
		total   time.Duration
	}
)

// note: not counting stats
func (bp *rlbackend) HeadObj(ctx context.Context, lom *core.LOM, origReq *http.Request) (oa *cmn.ObjAttrs, ecode int, err error) {
	// proactive
	arl := bp.acquire(lom.Bck(), http.MethodHead)
	oa, ecode, err = bp.Backend.HeadObj(ctx, lom, origReq)
	if err == nil || arl == nil || !cmn.IsErrTooManyRequests(err) {
		return
	}

	cb := func() (int, error) {
		oa, ecode, err = bp.Backend.HeadObj(ctx, lom, origReq)
		return ecode, err
	}
	_, ecode, err = bp.retry(ctx, arl, cb)
	return
}

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
	var total time.Duration
	total, res.ErrCode, res.Err = bp.retry(ctx, arl, cb)

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
	var retries int
	for total < cos.DfltRateMaxWait {
		// reactive
		sleep := arl.OnErr()
		total += sleep
		if err = ctx.Err(); err != nil {
			break
		}
		ecode, err = cb()
		retries++
		if err == nil || !cmn.IsErrTooManyRequests(err) {
			break
		}
	}
	if err != nil && cmn.IsErrTooManyRequests(err) {
		err = &errBackendRetry{err: err, retries: retries, total: total}
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

func (e *errBackendRetry) Error() string {
	return fmt.Sprintf("backend rate limiter: request still throttled after %d retry attempt%s and %v total backoff: %v",
		e.retries, cos.Plural(e.retries), e.total, e.err)
}

func (e *errBackendRetry) Unwrap() error { return e.err }
