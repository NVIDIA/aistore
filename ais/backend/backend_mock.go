// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"io"
	"math"
	"net/http"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
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

func (m *dummyBackendProvider) Provider() string  { return "dummy" }
func (m *dummyBackendProvider) MaxPageSize() uint { return math.MaxUint32 }

func (m *dummyBackendProvider) CreateBucket(ctx context.Context, bck *cluster.Bck) (errCode int, err error) {
	return creatingBucketNotSupportedErr("backend")
}

func (m *dummyBackendProvider) HeadBucket(ctx context.Context, bck *cluster.Bck) (bckProps cmn.SimpleKVs, errCode int, err error) {
	return cmn.SimpleKVs{}, http.StatusNotFound, cmn.NewErrorRemoteBucketOffline(bck.Bck)
}

func (m *dummyBackendProvider) ListObjects(ctx context.Context, bck *cluster.Bck, msg *cmn.SelectMsg) (bckList *cmn.BucketList, errCode int, err error) {
	return nil, http.StatusNotFound, cmn.NewErrorRemoteBucketOffline(bck.Bck)
}

// The function must not fail - it should return empty list.
func (m *dummyBackendProvider) ListBuckets(ctx context.Context, query cmn.QueryBcks) (buckets cmn.BucketNames, errCode int, err error) {
	return cmn.BucketNames{}, 0, nil
}

func (m *dummyBackendProvider) HeadObj(ctx context.Context, lom *cluster.LOM) (objMeta cmn.SimpleKVs, errCode int, err error) {
	return cmn.SimpleKVs{}, http.StatusNotFound, cmn.NewErrorRemoteBucketDoesNotExist(lom.Bucket())
}

func (m *dummyBackendProvider) GetObj(ctx context.Context, lom *cluster.LOM) (errCode int, err error) {
	return http.StatusNotFound, cmn.NewErrorRemoteBucketDoesNotExist(lom.Bucket())
}

func (m *dummyBackendProvider) GetObjReader(ctx context.Context, lom *cluster.LOM) (r io.ReadCloser, expectedCksm *cmn.Cksum, errCode int, err error) {
	return nil, nil, 0, nil
}

func (m *dummyBackendProvider) PutObj(ctx context.Context, r io.ReadCloser, lom *cluster.LOM) (version string, errCode int, err error) {
	return "", http.StatusNotFound, cmn.NewErrorRemoteBucketDoesNotExist(lom.Bucket())
}

func (m *dummyBackendProvider) DeleteObj(ctx context.Context, lom *cluster.LOM) (errCode int, err error) {
	return http.StatusNotFound, cmn.NewErrorRemoteBucketDoesNotExist(lom.Bucket())
}
