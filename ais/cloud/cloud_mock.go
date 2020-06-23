// Package cloud contains implementation of various cloud providers.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cloud

import (
	"context"
	"io"
	"net/http"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

type (
	dummyCloudProvider struct {
		t cluster.Target
	}
)

var (
	_ cluster.CloudProvider = &dummyCloudProvider{}
)

func NewDummyCloud(t cluster.Target) (cluster.CloudProvider, error) {
	return &dummyCloudProvider{t: t}, nil
}

func (m *dummyCloudProvider) _dummyNode() string {
	return m.t.Snode().String()
}

func (m *dummyCloudProvider) Provider() string {
	return "dummy"
}

func (m *dummyCloudProvider) ListObjects(ctx context.Context, bck *cluster.Bck,
	msg *cmn.SelectMsg) (bckList *cmn.BucketList, err error, errCode int) {
	return nil, cmn.NewErrorCloudBucketOffline(bck.Bck, ""), http.StatusNotFound
}
func (m *dummyCloudProvider) HeadBucket(ctx context.Context, bck *cluster.Bck) (bckProps cmn.SimpleKVs, err error, errCode int) {
	return cmn.SimpleKVs{}, cmn.NewErrorCloudBucketOffline(bck.Bck, ""), http.StatusNotFound
}

// the function must not fail - it should return empty list
func (m *dummyCloudProvider) ListBuckets(ctx context.Context, _ cmn.QueryBcks) (buckets cmn.BucketNames, err error, errCode int) {
	return cmn.BucketNames{}, nil, 0
}
func (m *dummyCloudProvider) HeadObj(ctx context.Context, lom *cluster.LOM) (objMeta cmn.SimpleKVs, err error, errCode int) {
	node := m._dummyNode()
	return cmn.SimpleKVs{}, cmn.NewErrorRemoteBucketDoesNotExist(lom.Bck().Bck, node), http.StatusNotFound
}
func (m *dummyCloudProvider) GetObj(ctx context.Context, fqn string, lom *cluster.LOM) (err error, errCode int) {
	bck, node := lom.Bck().Bck, m._dummyNode()
	return cmn.NewErrorRemoteBucketDoesNotExist(bck, node), http.StatusNotFound
}
func (m *dummyCloudProvider) PutObj(ctx context.Context, r io.Reader, lom *cluster.LOM) (version string, err error, errCode int) {
	bck, node := lom.Bck().Bck, m._dummyNode()
	return "", cmn.NewErrorRemoteBucketDoesNotExist(bck, node), http.StatusNotFound
}
func (m *dummyCloudProvider) DeleteObj(ctx context.Context, lom *cluster.LOM) (err error, errCode int) {
	bck, node := lom.Bck().Bck, m._dummyNode()
	return cmn.NewErrorRemoteBucketDoesNotExist(bck, node), http.StatusNotFound
}
