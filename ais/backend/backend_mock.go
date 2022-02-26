// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
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

type (
	dummyBackendProvider struct {
		t cluster.Target
	}
)

// interface guard
var _ cluster.BackendProvider = (*dummyBackendProvider)(nil)

func NewDummyBackend(t cluster.Target) (cluster.BackendProvider, error) {
	return &dummyBackendProvider{t: t}, nil
}

func (*dummyBackendProvider) Provider() string  { return "dummy" }
func (*dummyBackendProvider) MaxPageSize() uint { return math.MaxUint32 }

func (*dummyBackendProvider) CreateBucket(*cluster.Bck) (errCode int, err error) {
	return creatingBucketNotSupportedErr("backend")
}

func (*dummyBackendProvider) HeadBucket(_ ctx, bck *cluster.Bck) (bckProps cos.SimpleKVs, errCode int, err error) {
	return cos.SimpleKVs{}, http.StatusNotFound, cmn.NewErrRemoteBckOffline(bck.Bck)
}

func (*dummyBackendProvider) ListObjects(bck *cluster.Bck, _ *apc.ListObjsMsg) (bckList *cmn.BucketList, errCode int, err error) {
	return nil, http.StatusNotFound, cmn.NewErrRemoteBckOffline(bck.Bck)
}

// The function must not fail - it should return empty list.
func (*dummyBackendProvider) ListBuckets(cmn.QueryBcks) (bcks cmn.Bcks, errCode int, err error) {
	return
}

func (*dummyBackendProvider) HeadObj(_ ctx, lom *cluster.LOM) (*cmn.ObjAttrs, int, error) {
	return &cmn.ObjAttrs{}, http.StatusNotFound, cmn.NewErrRemoteBckNotFound(lom.Bucket())
}

func (*dummyBackendProvider) GetObj(_ ctx, lom *cluster.LOM, _ cmn.OWT) (errCode int, err error) {
	return http.StatusNotFound, cmn.NewErrRemoteBckNotFound(lom.Bucket())
}

func (*dummyBackendProvider) GetObjReader(context.Context, *cluster.LOM) (r io.ReadCloser, expectedCksm *cos.Cksum,
	errCode int, err error) {
	return nil, nil, 0, nil
}

func (*dummyBackendProvider) PutObj(_ io.ReadCloser, lom *cluster.LOM) (errCode int, err error) {
	return http.StatusNotFound, cmn.NewErrRemoteBckNotFound(lom.Bucket())
}

func (*dummyBackendProvider) DeleteObj(lom *cluster.LOM) (errCode int, err error) {
	return http.StatusNotFound, cmn.NewErrRemoteBckNotFound(lom.Bucket())
}
