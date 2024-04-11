// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
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
}

// interface guard
var _ core.Backend = (*mockbp)(nil)

func NewDummyBackend(t core.TargetPut) (core.Backend, error) { return &mockbp{t: t}, nil }

func (*mockbp) Provider() string           { return mock }
func (*mockbp) MaxPageSize(*meta.Bck) uint { return math.MaxUint32 }

func (*mockbp) CreateBucket(*meta.Bck) (int, error) {
	return http.StatusBadRequest, cmn.NewErrUnsupp("create", mock+" bucket")
}

func (*mockbp) HeadBucket(_ context.Context, bck *meta.Bck) (cos.StrKVs, int, error) {
	return cos.StrKVs{}, http.StatusNotFound, cmn.NewErrRemoteBckOffline(bck.Bucket())
}

func (*mockbp) ListObjectsInv(bck *meta.Bck, _ *apc.LsoMsg, _ *cmn.LsoRes, _ *core.LsoInvCtx) (int, error) {
	return http.StatusNotFound, cmn.NewErrRemoteBckOffline(bck.Bucket())
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

func (*mockbp) GetObjReader(context.Context, *core.LOM, int64, int64) (res core.GetReaderResult) {
	return
}

func (*mockbp) PutObj(_ io.ReadCloser, lom *core.LOM, _ *http.Request) (int, error) {
	return http.StatusNotFound, cmn.NewErrRemoteBckNotFound(lom.Bucket())
}

func (*mockbp) DeleteObj(lom *core.LOM) (int, error) {
	return http.StatusNotFound, cmn.NewErrRemoteBckNotFound(lom.Bucket())
}
