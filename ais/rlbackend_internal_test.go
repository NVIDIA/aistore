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
