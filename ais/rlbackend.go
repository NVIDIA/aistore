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

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xact"
)

// rate-limit 6 backend APIs:
// - HeadObj()
// - ListObjects()
// - GetObj()
// - GetObjReader()
// - PutObj()
// - DeleteObj()

// The remaining backend APIs can be added if needed - remote providers may
// rate-limit them as well:
// - CreateBucket()
// - ListBuckets()
// - HeadBucket()
// - StartMpt(), PutMptPart(), CompleteMpt(), AbortMpt()

// retries:
// - proactive: acquire (i.e., shape) before every call
// - reactive:  on 429/503 only, and only where the call is safely repeatable;
//   PutObj requires a replayable (re-openable) body

// stats:
// - not counting proactive delay    - only (reactive) retries
// - not counting individual retries - only totals (see "increment" comment below)
// - not counting list/delete retries - only get and put

type (
	rlbackend struct {
		core.Backend
		t *target
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
	_, ecode, err = bp.retry(ctx, arl, ecode, err, cb)
	return
}

// Direct callers (all via core.T.Backend):
// - (*npgCtx).nextPageR (xact/xs/nextpage.go): remote LIST and bucket summary
// - (*lrit).lsoPage (xact/xs/lrit.go): prefix prefetch, evict/delete, copy/transform, and archive
// - (*XactNBI).Run (xact/xs/create_nbi.go): native bucket inventory
// - (*backendDlJob).getNextObjs (ext/dload/job.go): backend download
//
// TODO -- FIXME: (*lrit).lsoPage has its own retry loop - unify
func (bp *rlbackend) ListObjects(bck *meta.Bck, msg *apc.LsoMsg, lst *cmn.LsoRes) (ecode int, err error) {
	arl := bp.acquire(bck, apc.ActList)

	ecode, err = bp.Backend.ListObjects(bck, msg, lst)
	if err == nil || arl == nil || !cmn.IsErrTooManyRequests(err) {
		return
	}

	cb := func() (int, error) {
		return bp.Backend.ListObjects(bck, msg, lst)
	}
	// core.Backend.ListObjects has no request context (TODO: plumb it through - abortable xactions)
	_, ecode, err = bp.retry(context.Background(), arl, ecode, err, cb)
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
	total, code, e := bp.retry(ctx, arl, ecode, err, cb)

	// increment retry count by 1, retry latency by `total`
	bp.stats(ctx, lom.Bck(), stats.RatelimGetRetryCount, stats.RatelimGetRetryLatencyTotal, total)
	return code, e
}

func (bp *rlbackend) GetObjReader(ctx context.Context, lom *core.LOM, offset, length int64) (res core.GetReaderResult) {
	closeErrReader := func() {
		if res.Err != nil && res.R != nil {
			cos.Close(res.R)
			res.R = nil
		}
	}

	// proactive
	arl := bp.acquire(lom.Bck(), http.MethodGet)
	res = bp.Backend.GetObjReader(ctx, lom, offset, length)
	if res.Err == nil || arl == nil || !cmn.IsErrTooManyRequests(res.Err) {
		return res
	}
	closeErrReader()

	cb := func() (int, error) {
		res = bp.Backend.GetObjReader(ctx, lom, offset, length)
		closeErrReader()
		return res.ErrCode, res.Err
	}

	var total time.Duration
	total, res.ErrCode, res.Err = bp.retry(ctx, arl, res.ErrCode, res.Err, cb)

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

	// reactive: the first attempt drained `r` - retry only when the source
	// can be re-opened from scratch (LomHandle, FileHandle, ByteReader)
	// see also: lom.Open() and friends
	opener, ok := r.(cos.ReadOpenCloser)
	if !ok {
		return ecode, err
	}

	// TODO -- FIXME:
	// typecast above is not ideal, strictly speaking
	// nopOpener would be a no-op; ReaderWithArgs would panic (see cmn/cos/io.go)

	var roc cos.ReadOpenCloser
	cb := func() (int, error) {
		if roc != nil {
			cos.Close(roc)
			roc = nil
		}
		next, errO := opener.Open()
		if errO != nil {
			return 0, errO
		}
		roc = next
		return bp.Backend.PutObj(ctx, next, lom, origReq)
	}
	total, code, e := bp.retry(ctx, arl, ecode, err, cb)
	if roc != nil {
		cos.Close(roc)
	}

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
	_, code, e := bp.retry(ctx, arl, ecode, err, cb)
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
	if !ok {
		smap := bp.t.owner.smap.get()
		// concurrent workers on the same (bucket, verb)
		v, _ = bp.t.ratelim.LoadOrStore(uhash, bck.NewBackendRateLim(smap.CountActiveTs()))
	}
	arl = v.(*cos.AdaptRateLim)

	arl.RetryAcquire(time.Second)
	return arl
}

// reactive: back off and repeat `cb` while the remote keeps responding 429/503
// - (ecode, err) on entry: the failed attempt that brought us here
// - bounded by both `num_retries` and DfltRateMaxWait
func (*rlbackend) retry(ctx context.Context, arl *cos.AdaptRateLim, ecode int, err error,
	cb func() (int, error)) (total time.Duration, _ int, _ error) {
	var retries int
	for retries < arl.NumRetries() && total < cos.DfltRateMaxWait {
		sleep := arl.OnErr()
		total += sleep
		if cerr := ctx.Err(); cerr != nil {
			return total, 0, cerr
		}
		ecode, err = cb()
		retries++
		if err == nil || !cmn.IsErrTooManyRequests(err) {
			break
		}
	}
	if err != nil && cmn.IsErrTooManyRequests(err) {
		err = cmn.NewErrBackendRetry(err, retries, total)
		ecode = 0 // not returning 429 when exhausted
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
