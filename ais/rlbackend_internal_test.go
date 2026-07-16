// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/tools/tassert"
)

type throttleBackend struct {
	core.Backend
	calls int
}

type throttleReaderBackend struct {
	core.Backend
	err   error
	calls int
}

func (bp *throttleBackend) errOnce() error {
	bp.calls++
	if bp.calls == 1 {
		return cmn.NewErrTooManyRequests(errors.New("throttled"), http.StatusTooManyRequests)
	}
	return nil
}

func (bp *throttleBackend) HeadObj(context.Context, *core.LOM, *http.Request) (*cmn.ObjAttrs, int, error) {
	err := bp.errOnce()
	if err != nil {
		return nil, http.StatusTooManyRequests, err
	}
	return &cmn.ObjAttrs{Size: 1}, 0, nil
}

func (bp *throttleBackend) GetObj(context.Context, *core.LOM, cmn.OWT, *http.Request) (int, error) {
	err := bp.errOnce()
	if err != nil {
		return http.StatusTooManyRequests, err
	}
	return 0, nil
}

func (bp *throttleReaderBackend) GetObjReader(context.Context, *core.LOM, int64, int64) core.GetReaderResult {
	bp.calls++
	return core.GetReaderResult{Err: bp.err, ErrCode: http.StatusTooManyRequests}
}

func TestRLBackendGetHead(t *testing.T) {
	tests := []struct {
		name string
		verb string
		do   func(*rlbackend, *core.LOM) error
	}{
		{
			name: http.MethodGet,
			verb: http.MethodGet,
			do: func(bp *rlbackend, lom *core.LOM) error {
				_, err := bp.GetObj(context.Background(), lom, cmn.OwtGet, nil)
				return err
			},
		},
		{
			name: http.MethodHead,
			verb: http.MethodHead,
			do: func(bp *rlbackend, lom *core.LOM) error {
				oa, _, err := bp.HeadObj(context.Background(), lom, nil)
				if err == nil && (oa == nil || oa.Size != 1) {
					return errors.New("invalid object attributes")
				}
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lom := core.AllocLOM("rate-limit-" + test.name)
			defer core.FreeLOM(lom)
			err := lom.InitBck(meta.NewBck(testBucket, apc.AIS, cmn.NsGlobal))
			tassert.CheckFatal(t, err)

			conf := &lom.Bck().Props.RateLimit.Backend
			oldEnabled := conf.Enabled
			conf.Enabled = true
			defer func() { conf.Enabled = oldEnabled }()

			arl, err := cos.NewAdaptRateLim(100, 1, time.Second)
			tassert.CheckFatal(t, err)
			key := lom.Bck().HashUname(test.verb)
			mockTarget.ratelim.Store(key, arl)
			defer mockTarget.ratelim.Delete(key)

			backend := &throttleBackend{}
			bp := &rlbackend{Backend: backend, t: mockTarget}
			err = test.do(bp, lom)
			tassert.CheckFatal(t, err)
			tassert.Fatalf(t, backend.calls == 2, "expected one retry, got %d calls", backend.calls)
		})
	}
}

func TestRLBackendHeadReturnsRetryError(t *testing.T) {
	lom := core.AllocLOM("rate-limit-head-retry-error")
	defer core.FreeLOM(lom)
	err := lom.InitBck(meta.NewBck(testBucket, apc.AIS, cmn.NsGlobal))
	tassert.CheckFatal(t, err)

	conf := &lom.Bck().Props.RateLimit.Backend
	oldEnabled := conf.Enabled
	conf.Enabled = true
	defer func() { conf.Enabled = oldEnabled }()

	arl, err := cos.NewAdaptRateLim(100, 1, time.Second)
	tassert.CheckFatal(t, err)
	key := lom.Bck().HashUname(http.MethodHead)
	mockTarget.ratelim.Store(key, arl)
	defer mockTarget.ratelim.Delete(key)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	backend := &throttleBackend{}
	bp := &rlbackend{Backend: backend, t: mockTarget}
	oa, ecode, err := bp.HeadObj(ctx, lom, nil)
	tassert.Fatalf(t, errors.Is(err, context.Canceled), "expected retry error %v, got %v", context.Canceled, err)
	tassert.Fatalf(t, ecode == 0, "expected retry code 0, got %d", ecode)
	tassert.Fatalf(t, oa == nil, "expected no object attributes, got %v", oa)
	tassert.Fatalf(t, backend.calls == 1, "expected no retry call, got %d calls", backend.calls)
}

func TestRLBackendGetObjReaderReturnsRetryError(t *testing.T) {
	lom := core.AllocLOM("rate-limit-reader-retry-error")
	defer core.FreeLOM(lom)
	err := lom.InitBck(meta.NewBck(testBucket, apc.AIS, cmn.NsGlobal))
	tassert.CheckFatal(t, err)

	conf := &lom.Bck().Props.RateLimit.Backend
	oldEnabled := conf.Enabled
	conf.Enabled = true
	defer func() { conf.Enabled = oldEnabled }()

	arl, err := cos.NewAdaptRateLim(100, 1, time.Second)
	tassert.CheckFatal(t, err)
	key := lom.Bck().HashUname(http.MethodGet)
	mockTarget.ratelim.Store(key, arl)
	defer mockTarget.ratelim.Delete(key)

	err429 := cmn.NewErrTooManyRequests(errors.New("throttled"), http.StatusTooManyRequests)
	backend := &throttleReaderBackend{err: err429}
	bp := &rlbackend{Backend: backend, t: mockTarget}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	res := bp.GetObjReader(ctx, lom, 0, 0)
	tassert.Fatalf(t, errors.Is(res.Err, context.Canceled), "expected retry error %v, got %v", context.Canceled, res.Err)
	tassert.Fatalf(t, res.ErrCode == 0, "expected retry code 0, got %d", res.ErrCode)
	tassert.Fatalf(t, backend.calls == 1, "expected no retry call, got %d calls", backend.calls)
}

func TestRLBackendRetryErrorDetails(t *testing.T) {
	err429 := cmn.NewErrTooManyRequests(errors.New("throttled"), http.StatusTooManyRequests)
	var err error = &errBackendRetry{err: err429, retries: 3, total: 5 * time.Second}
	var retryErr *errBackendRetry
	tassert.Fatalf(t, errors.As(err, &retryErr), "expected backend retry error, got %T", err)
	tassert.Fatalf(t, retryErr.retries == 3, "expected 3 retries, got %d", retryErr.retries)
	tassert.Fatalf(t, retryErr.total == 5*time.Second, "expected 5s backoff, got %v", retryErr.total)
	tassert.Fatalf(t, errors.Is(err, err429), "expected wrapped error %v, got %v", err429, err)
	tassert.Fatalf(t, cmn.IsErrTooManyRequests(err), "expected throttling error, got %v", err)
}

type (
	listResult struct {
		err   error
		ecode int
	}
	listBackend struct {
		core.Backend
		results []listResult
		bcks    []*meta.Bck
		msgs    []*apc.LsoMsg
		lists   []*cmn.LsoRes
		tokens  []string
		calls   int
	}
)

func (bp *listBackend) ListObjects(bck *meta.Bck, msg *apc.LsoMsg, lst *cmn.LsoRes) (int, error) {
	bp.bcks = append(bp.bcks, bck)
	bp.msgs = append(bp.msgs, msg)
	bp.lists = append(bp.lists, lst)
	bp.tokens = append(bp.tokens, msg.ContinuationToken)
	result := bp.results[bp.calls]
	bp.calls++
	if result.err == nil {
		msg.PageSize = 17
		lst.Entries = append(lst.Entries[:0], &cmn.LsoEnt{Name: "listed-object"})
		lst.ContinuationToken = "next-page"
	}
	return result.ecode, result.err
}

func TestRLBackendListObjects(t *testing.T) {
	var (
		errThrottle = errors.New("throttled")
		err429      = cmn.NewErrTooManyRequests(errThrottle, http.StatusTooManyRequests)
		err503      = cmn.NewErrTooManyRequests(errThrottle, http.StatusServiceUnavailable)
		errFinal    = errors.New("retry failed")
		tests       = []struct {
			name      string
			results   []listResult
			enabled   bool
			wantCode  int
			wantErr   error
			wantCalls int
		}{
			{
				name:      "success",
				results:   []listResult{{}},
				enabled:   true,
				wantCalls: 1,
			},
			{
				name:      "status 429",
				results:   []listResult{{ecode: http.StatusTooManyRequests, err: err429}, {}},
				enabled:   true,
				wantCalls: 2,
			},
			{
				name:      "status 503",
				results:   []listResult{{ecode: http.StatusServiceUnavailable, err: err503}, {}},
				enabled:   true,
				wantCalls: 2,
			},
			{
				name: "final retry error",
				results: []listResult{
					{ecode: http.StatusTooManyRequests, err: err429},
					{ecode: http.StatusBadGateway, err: errFinal},
				},
				enabled:   true,
				wantCode:  http.StatusBadGateway,
				wantErr:   errFinal,
				wantCalls: 2,
			},
			{
				name:      "non-throttle error",
				results:   []listResult{{ecode: http.StatusBadRequest, err: errFinal}},
				enabled:   true,
				wantCode:  http.StatusBadRequest,
				wantErr:   errFinal,
				wantCalls: 1,
			},
			{
				name:      "disabled",
				results:   []listResult{{ecode: http.StatusTooManyRequests, err: err429}},
				wantCode:  http.StatusTooManyRequests,
				wantErr:   err429,
				wantCalls: 1,
			},
		}
	)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			bck := meta.NewBck("rate-limit-list", apc.AIS, cmn.NsGlobal)
			bck.Props = &cmn.Bprops{}
			bck.Props.RateLimit.Backend.Enabled = test.enabled
			if test.enabled {
				arl, err := cos.NewAdaptRateLim(100, 1, time.Second)
				tassert.CheckFatal(t, err)
				key := bck.HashUname(apc.ActList)
				mockTarget.ratelim.Store(key, arl)
				t.Cleanup(func() { mockTarget.ratelim.Delete(key) })
			}

			backend := &listBackend{results: test.results}
			bp := &rlbackend{Backend: backend, t: mockTarget}
			lst := &cmn.LsoRes{}
			msg := &apc.LsoMsg{ContinuationToken: "current-page"}
			ecode, err := bp.ListObjects(bck, msg, lst)
			tassert.Fatalf(t, ecode == test.wantCode, "expected code %d, got %d", test.wantCode, ecode)
			tassert.Fatalf(t, errors.Is(err, test.wantErr), "expected error %v, got %v", test.wantErr, err)
			tassert.Fatalf(t, backend.calls == test.wantCalls, "expected %d calls, got %d", test.wantCalls, backend.calls)
			for i := range backend.calls {
				tassert.Fatalf(t, backend.bcks[i] == bck, "call %d: backend received a different bucket", i+1)
				tassert.Fatalf(t, backend.msgs[i] == msg, "call %d: backend received a different list message", i+1)
				tassert.Fatalf(t, backend.lists[i] == lst, "call %d: backend received a different list result", i+1)
				tassert.Fatalf(t, backend.tokens[i] == "current-page", "call %d: continuation token changed to %q",
					i+1, backend.tokens[i])
			}
			if test.wantErr == nil {
				tassert.Fatalf(t, msg.PageSize == 17, "expected backend message update, got page size %d", msg.PageSize)
				tassert.Fatalf(t, len(lst.Entries) == 1 && lst.Entries[0].Name == "listed-object",
					"unexpected entries: %v", lst.Entries)
				tassert.Fatalf(t, lst.ContinuationToken == "next-page", "unexpected continuation token %q", lst.ContinuationToken)
			}
		})
	}
}
