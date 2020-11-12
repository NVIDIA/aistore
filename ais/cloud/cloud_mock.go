// Package cloud contains implementation of various cloud providers.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cloud

import (
	"context"
	"io"
	"math"
	"net/http"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

type (
	dummyCloudProvider struct {
		t cluster.Target
	}
)

// interface guard
var _ cluster.CloudProvider = (*dummyCloudProvider)(nil)

func NewDummyCloud(t cluster.Target) (cluster.CloudProvider, error) {
	return &dummyCloudProvider{t: t}, nil
}

func (m *dummyCloudProvider) _dummyNode() string { return m.t.Snode().String() }
func (m *dummyCloudProvider) Provider() string   { return "dummy" }
func (m *dummyCloudProvider) MaxPageSize() uint  { return math.MaxUint32 }

func (m *dummyCloudProvider) ListObjects(ctx context.Context, bck *cluster.Bck, msg *cmn.SelectMsg) (bckList *cmn.BucketList, errCode int, err error) {
	return nil, http.StatusNotFound, cmn.NewErrorCloudBucketOffline(bck.Bck, "")
}

func (m *dummyCloudProvider) HeadBucket(ctx context.Context, bck *cluster.Bck) (bckProps cmn.SimpleKVs, errCode int, err error) {
	return cmn.SimpleKVs{}, http.StatusNotFound, cmn.NewErrorCloudBucketOffline(bck.Bck, "")
}

// the function must not fail - it should return empty list
func (m *dummyCloudProvider) ListBuckets(ctx context.Context, query cmn.QueryBcks) (buckets cmn.BucketNames, errCode int, err error) {
	return cmn.BucketNames{}, 0, nil
}

func (m *dummyCloudProvider) HeadObj(ctx context.Context, lom *cluster.LOM) (objMeta cmn.SimpleKVs, errCode int, err error) {
	node := m._dummyNode()
	return cmn.SimpleKVs{}, http.StatusNotFound, cmn.NewErrorRemoteBucketDoesNotExist(lom.Bck().Bck, node)
}

func (m *dummyCloudProvider) GetObj(ctx context.Context, lom *cluster.LOM) (workFQN string, errCode int, err error) {
	bck, node := lom.Bck().Bck, m._dummyNode()
	return "", http.StatusNotFound, cmn.NewErrorRemoteBucketDoesNotExist(bck, node)
}

func (m *dummyCloudProvider) GetObjReader(ctx context.Context, lom *cluster.LOM) (r io.ReadCloser, expectedCksm *cmn.Cksum, errCode int, err error) {
	return nil, nil, 0, nil
}

func (m *dummyCloudProvider) PutObj(ctx context.Context, r io.Reader, lom *cluster.LOM) (version string, errCode int, err error) {
	bck, node := lom.Bck().Bck, m._dummyNode()
	return "", http.StatusNotFound, cmn.NewErrorRemoteBucketDoesNotExist(bck, node)
}

func (m *dummyCloudProvider) DeleteObj(ctx context.Context, lom *cluster.LOM) (errCode int, err error) {
	bck, node := lom.Bck().Bck, m._dummyNode()
	return http.StatusNotFound, cmn.NewErrorRemoteBucketDoesNotExist(bck, node)
}
