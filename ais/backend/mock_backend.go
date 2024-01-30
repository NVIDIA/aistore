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

type mockBP struct {
	t core.TargetPut
}

// interface guard
var _ core.BackendProvider = (*mockBP)(nil)

func NewDummyBackend(t core.TargetPut) (core.BackendProvider, error) { return &mockBP{t: t}, nil }

func (*mockBP) Provider() string  { return mock }
func (*mockBP) MaxPageSize() uint { return math.MaxUint32 }

func (*mockBP) CreateBucket(*meta.Bck) (int, error) {
	return http.StatusBadRequest, cmn.NewErrUnsupp("create", mock+" bucket")
}

func (*mockBP) HeadBucket(_ ctx, bck *meta.Bck) (cos.StrKVs, int, error) {
	return cos.StrKVs{}, http.StatusNotFound, cmn.NewErrRemoteBckOffline(bck.Bucket())
}

func (*mockBP) ListObjects(bck *meta.Bck, _ *apc.LsoMsg, _ *cmn.LsoResult) (int, error) {
	return http.StatusNotFound, cmn.NewErrRemoteBckOffline(bck.Bucket())
}

// cannot fail - return empty list
func (*mockBP) ListBuckets(cmn.QueryBcks) (bcks cmn.Bcks, errCode int, err error) {
	return
}

func (*mockBP) HeadObj(_ ctx, lom *core.LOM) (*cmn.ObjAttrs, int, error) {
	return &cmn.ObjAttrs{}, http.StatusNotFound, cmn.NewErrRemoteBckNotFound(lom.Bucket())
}

func (*mockBP) GetObj(_ ctx, lom *core.LOM, _ cmn.OWT) (int, error) {
	return http.StatusNotFound, cmn.NewErrRemoteBckNotFound(lom.Bucket())
}

func (*mockBP) GetObjReader(context.Context, *core.LOM, int64, int64) (res core.GetReaderResult) {
	return
}

func (*mockBP) PutObj(_ io.ReadCloser, lom *core.LOM) (int, error) {
	return http.StatusNotFound, cmn.NewErrRemoteBckNotFound(lom.Bucket())
}

func (*mockBP) DeleteObj(lom *core.LOM) (int, error) {
	return http.StatusNotFound, cmn.NewErrRemoteBckNotFound(lom.Bucket())
}
