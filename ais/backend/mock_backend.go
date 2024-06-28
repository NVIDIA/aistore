// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"io"
	"math"
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
)

const mock = "mock-backend"

type mockbp struct {
	t core.TargetPut
	base
}

// interface guard
var _ core.Backend = (*mockbp)(nil)

func NewDummyBackend(t core.TargetPut) (core.Backend, error) {
	return &mockbp{
		t:    t,
		base: base{mock},
	}, nil
}

func (*mockbp) MaxPageSize(*meta.Bck) uint { return math.MaxUint32 }

func (*mockbp) HeadBucket(_ context.Context, bck *meta.Bck) (cos.StrKVs, int, error) {
	return cos.StrKVs{}, http.StatusNotFound, cmn.NewErrRemoteBckOffline(bck.Bucket())
}

func (*mockbp) ListObjects(bck *meta.Bck, _ *apc.LsoMsg, _ *cmn.LsoRes) (int, error) {
	return http.StatusNotFound, cmn.NewErrRemoteBckOffline(bck.Bucket())
}

// cannot fail - return empty list
func (*mockbp) ListBuckets(cmn.QueryBcks) (bcks cmn.Bcks, ecode int, err error) {
	return
}

func (*mockbp) HeadObj(_ context.Context, lom *core.LOM, _ *http.Request) (*cmn.ObjAttrs, int, error) {
	return &cmn.ObjAttrs{}, http.StatusNotFound, cmn.NewErrRemoteBckNotFound(lom.Bucket())
}

func (*mockbp) GetObj(_ context.Context, lom *core.LOM, _ cmn.OWT, _ *http.Request) (int, error) {
	return http.StatusNotFound, cmn.NewErrRemoteBckNotFound(lom.Bucket())
}

func (*mockbp) GetObjReader(_ context.Context, lom *core.LOM, _, _ int64) (res core.GetReaderResult) {
	res.Err = cmn.NewErrRemoteBckOffline(lom.Bucket())
	res.ErrCode = http.StatusNotFound
	return
}

func (*mockbp) PutObj(_ io.ReadCloser, lom *core.LOM, _ *http.Request) (int, error) {
	return http.StatusNotFound, cmn.NewErrRemoteBckNotFound(lom.Bucket())
}

func (*mockbp) DeleteObj(lom *core.LOM) (int, error) {
	return http.StatusNotFound, cmn.NewErrRemoteBckNotFound(lom.Bucket())
}
