// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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

// modes:
// - bucket-configured (enabled): acquire before every call
// - on-demand: same, but only after the first 429/503 activates the limiter (one way)

// retries (both modes):
// - on 429/503 only, and only where the call is safely repeatable;
//   PutObj requires a replayable (re-openable) body
// - each retry is a single attempt: the backend (SDK client) must not
//   retry on its own (see cmn.WithNoRetry)

// stats:
// - not counting shaping delay      - only retries
// - not counting individual retries - only totals (see "increment" comment below)
// - not counting list/delete retries - only get and put

type (
	backendRateKey struct {
		uname     uint64
		interval  cos.Duration
		maxTokens int
		retries   int
	}
	rlbackend struct {
		core.Backend
		t *target
	}
)

// note: not counting stats
func (bp *rlbackend) HeadObj(ctx context.Context, lom *core.LOM, origReq *http.Request) (oa *cmn.ObjAttrs, ecode int, err error) {
	//
	// pace the request when the limiter is active - here and elsewhere
	//
	arl := bp.acquire(lom.Bck(), http.MethodHead)
	oa, ecode, err = bp.Backend.HeadObj(ctx, lom, origReq)
	if err == nil || arl == nil || !cmn.IsErrTooManyRequests(err) {
		return
	}

	cb := func(ctx context.Context) (int, error) {
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
func (bp *rlbackend) ListObjects(ctx context.Context, bck *meta.Bck, msg *apc.LsoMsg, lst *cmn.LsoRes) (ecode int, err error) {
	arl := bp.acquire(bck, apc.ActList)

	ecode, err = bp.Backend.ListObjects(ctx, bck, msg, lst)
	if err == nil || arl == nil || !cmn.IsErrTooManyRequests(err) {
		return
	}

	cb := func(ctx context.Context) (int, error) {
		return bp.Backend.ListObjects(ctx, bck, msg, lst)
	}
	_, ecode, err = bp.retry(ctx, arl, ecode, err, cb)
	return
}

func (bp *rlbackend) GetObj(ctx context.Context, lom *core.LOM, owt cmn.OWT, origReq *http.Request) (int, error) {
	arl := bp.acquire(lom.Bck(), http.MethodGet)
	ecode, err := bp.Backend.GetObj(ctx, lom, owt, origReq)
	if err == nil || arl == nil || !cmn.IsErrTooManyRequests(err) {
		return ecode, err
	}

	cb := func(ctx context.Context) (int, error) {
		return bp.Backend.GetObj(ctx, lom, owt, origReq)
	}
	total, code, e := bp.retry(ctx, arl, ecode, err, cb)

	// increment retry count by 1, retry latency by `total`
	bp.stats(ctx, lom.Bck(), stats.RatelimGetRetryCount, stats.RatelimGetRetryLatencyTotal, total)
	return code, e
}

func (bp *rlbackend) GetObjReader(ctx context.Context, lom *core.LOM, offset, length int64, pin ...*core.ObjPin) (res core.GetReaderResult) {
	closeErrReader := func() {
		if res.Err != nil && res.R != nil {
			cmn.CloseRemote(res.R, lom, 4, cos.ModBackend)
			res.R = nil
		}
	}

	arl := bp.acquire(lom.Bck(), http.MethodGet)
	res = bp.Backend.GetObjReader(ctx, lom, offset, length, pin...)
	if res.Err == nil || arl == nil || !cmn.IsErrTooManyRequests(res.Err) {
		return res
	}
	closeErrReader()

	cb := func(ctx context.Context) (int, error) {
		res = bp.Backend.GetObjReader(ctx, lom, offset, length, pin...)
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
	arl := bp.acquire(lom.Bck(), http.MethodPut)
	ecode, err := bp.Backend.PutObj(ctx, r, lom, origReq)
	if err == nil || arl == nil || !cmn.IsErrTooManyRequests(err) {
		return ecode, err
	}

	// retry: the first attempt drained `r` - retry only when the source
	// can be re-opened from scratch (LomHandle, FileHandle, ByteReader)
	// see also: lom.Open() and friends
	opener, ok := r.(cos.ReadOpenCloser)
	if !ok {
		arl.Activate()
		return ecode, err
	}

	// TODO -- FIXME:
	// typecast above is not ideal, strictly speaking
	// nopOpener would be a no-op; ReaderWithArgs would panic (see cmn/cos/io.go)

	cb := func(ctx context.Context) (int, error) {
		next, err := opener.Open()
		if err != nil {
			return 0, err
		}
		code, err := bp.Backend.PutObj(ctx, next, lom, origReq)
		// release the reopened file even if the backend returns before consuming it
		if closeErr := next.Close(); !errors.Is(closeErr, os.ErrClosed) {
			debug.AssertNoErr(closeErr)
		}
		return code, err
	}
	total, code, e := bp.retry(ctx, arl, ecode, err, cb)

	bp.stats(ctx, lom.Bck(), stats.RatelimPutRetryCount, stats.RatelimPutRetryLatencyTotal, total)
	return code, e
}

// note: not counting stats
func (bp *rlbackend) DeleteObj(ctx context.Context, lom *core.LOM) (int, error) {
	arl := bp.acquire(lom.Bck(), http.MethodDelete)
	ecode, err := bp.Backend.DeleteObj(ctx, lom)
	if err == nil || arl == nil || !cmn.IsErrTooManyRequests(err) {
		return ecode, err
	}

	cb := func(ctx context.Context) (int, error) {
		return bp.Backend.DeleteObj(ctx, lom)
	}
	_, code, e := bp.retry(ctx, arl, ecode, err, cb)
	return code, e
}

//
// apply (compare with ais/prate_limit)
//

// mode changes retain the learned rate for the same budget
func rateKey(bck *meta.Bck, verb string) backendRateKey {
	conf := &bck.Props.RateLimit.Backend
	return backendRateKey{uname: bck.HashUname(verb), interval: conf.Interval, maxTokens: conf.MaxTokens, retries: conf.NumRetries}
}

func (bp *rlbackend) acquire(bck *meta.Bck, verb string) (arl *cos.AdaptRateLim) {
	conf := &bck.Props.RateLimit.Backend
	if !conf.On() {
		return nil
	}
	var (
		key   = rateKey(bck, verb)
		v, ok = bp.t.ratelim.Load(key)
	)
	if !ok {
		smap := bp.t.owner.smap.get()
		// concurrent workers on the same (bucket, verb)
		v, _ = bp.t.ratelim.LoadOrStore(key, bck.NewBackendRateLim(smap.CountActiveTs()))
	}
	arl = v.(*cos.AdaptRateLim)

	// on-demand: not pacing until activated (see retry)
	if !conf.Enabled && !arl.IsActive() {
		arl.Touch()
		return arl
	}

	// TODO: make acquisition context-aware so cancellation interrupts token waits
	arl.RetryAcquire(time.Second)
	return arl
}

// retry: back off and repeat `cb` while the remote keeps responding 429/503
// - (ecode, err) on entry: the failed attempt that brought us here
// - sleep grows from the previous wait by 2x/4x/8x based on error rate, capped at max(6s, token-derived interval)
// - bounded by both `num_retries` and DfltRateMaxWait
// - `cb` gets no-retry context: one attempt per SDK request
// - activate the limiter (on-demand: one way, see above)
func (*rlbackend) retry(ctx context.Context, arl *cos.AdaptRateLim, ecode int, err error,
	cb func(context.Context) (int, error)) (total time.Duration, _ int, _ error) {
	var (
		retries int
		sleep   time.Duration
		nctx    = cmn.WithNoRetry(ctx)
	)
	arl.Activate()
	for retries < arl.NumRetries() && total < cos.DfltRateMaxWait {
		sleep = arl.OnErr(sleep)
		total += sleep
		if cerr := ctx.Err(); cerr != nil {
			return total, 0, cerr
		}
		ecode, err = cb(nctx)
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
