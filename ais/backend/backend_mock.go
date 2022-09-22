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
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

const mock = "mock-backend"

type mockBP struct {
	t cluster.Target
}

// interface guard
var _ cluster.BackendProvider = (*mockBP)(nil)

func NewDummyBackend(t cluster.Target) (cluster.BackendProvider, error) {
	return &mockBP{t: t}, nil
}
func (*mockBP) Provider() string  { return mock }
func (*mockBP) MaxPageSize() uint { return math.MaxUint32 }

func (*mockBP) CreateBucket(*cluster.Bck) (int, error) {
	return http.StatusBadRequest, cmn.NewErrUnsupp("create", mock+" bucket")
}

func (*mockBP) HeadBucket(_ ctx, bck *cluster.Bck) (cos.SimpleKVs, int, error) {
	return cos.SimpleKVs{}, http.StatusNotFound, cmn.NewErrRemoteBckOffline(bck.Bucket())
}

func (*mockBP) ListObjects(bck *cluster.Bck, _ *apc.ListObjsMsg) (*cmn.ListObjects, int, error) {
	return nil, http.StatusNotFound, cmn.NewErrRemoteBckOffline(bck.Bucket())
}

// cannot fail - return empty list
func (*mockBP) ListBuckets(cmn.QueryBcks) (bcks cmn.Bcks, errCode int, err error) {
	return
}

func (*mockBP) HeadObj(_ ctx, lom *cluster.LOM) (*cmn.ObjAttrs, int, error) {
	return &cmn.ObjAttrs{}, http.StatusNotFound, cmn.NewErrRemoteBckNotFound(lom.Bucket())
}

func (*mockBP) GetObj(_ ctx, lom *cluster.LOM, _ cmn.OWT) (int, error) {
	return http.StatusNotFound, cmn.NewErrRemoteBckNotFound(lom.Bucket())
}

func (*mockBP) GetObjReader(context.Context, *cluster.LOM) (io.ReadCloser, *cos.Cksum, int, error) {
	return nil, nil, 0, nil
}

func (*mockBP) PutObj(_ io.ReadCloser, lom *cluster.LOM) (int, error) {
	return http.StatusNotFound, cmn.NewErrRemoteBckNotFound(lom.Bucket())
}

func (*mockBP) DeleteObj(lom *cluster.LOM) (int, error) {
	return http.StatusNotFound, cmn.NewErrRemoteBckNotFound(lom.Bucket())
}
