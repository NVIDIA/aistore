// Package cloud contains implementation of various cloud providers.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
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
	dummyCloudProvider struct{}
)

var (
	_ cluster.CloudProvider = &dummyCloudProvider{}
)

func NewDummyCloud() (cluster.CloudProvider, error) { return &dummyCloudProvider{}, nil }

func _dummyNode(lom *cluster.LOM) string {
	if lom.T == nil || lom.T.Snode() == nil {
		return ""
	}
	return lom.T.Snode().String()
}

func (m *dummyCloudProvider) ListObjects(ctx context.Context, bck cmn.Bck,
	msg *cmn.SelectMsg) (bckList *cmn.BucketList, err error, errCode int) {
	return nil, cmn.NewErrorCloudBucketOffline(bck, ""), http.StatusNotFound
}
func (m *dummyCloudProvider) HeadBucket(ctx context.Context, bck cmn.Bck) (bckProps cmn.SimpleKVs, err error, errCode int) {
	return cmn.SimpleKVs{}, cmn.NewErrorCloudBucketOffline(bck, ""), http.StatusNotFound
}

// the function must not fail - it should return empty list
func (m *dummyCloudProvider) ListBuckets(ctx context.Context) (buckets cmn.BucketNames, err error, errCode int) {
	return cmn.BucketNames{}, nil, 0
}
func (m *dummyCloudProvider) HeadObj(ctx context.Context, lom *cluster.LOM) (objMeta cmn.SimpleKVs, err error, errCode int) {
	bck, node := lom.Bck().Bck, _dummyNode(lom)
	return cmn.SimpleKVs{}, cmn.NewErrorRemoteBucketDoesNotExist(bck, node), http.StatusNotFound
}
func (m *dummyCloudProvider) GetObj(ctx context.Context, fqn string, lom *cluster.LOM) (err error, errCode int) {
	bck, node := lom.Bck().Bck, _dummyNode(lom)
	return cmn.NewErrorRemoteBucketDoesNotExist(bck, node), http.StatusNotFound
}
func (m *dummyCloudProvider) PutObj(ctx context.Context, r io.Reader, lom *cluster.LOM) (version string, err error, errCode int) {
	bck, node := lom.Bck().Bck, _dummyNode(lom)
	return "", cmn.NewErrorRemoteBucketDoesNotExist(bck, node), http.StatusNotFound
}
func (m *dummyCloudProvider) DeleteObj(ctx context.Context, lom *cluster.LOM) (err error, errCode int) {
	bck, node := lom.Bck().Bck, _dummyNode(lom)
	return cmn.NewErrorRemoteBucketDoesNotExist(bck, node), http.StatusNotFound
}
