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
	emptyCloudProvider struct{}
)

var (
	_ cluster.CloudProvider = &emptyCloudProvider{}
)

func NewEmptyCloud() (cluster.CloudProvider, error) { return &emptyCloudProvider{}, nil }

func _emptyNode(lom *cluster.LOM) string {
	if lom.T == nil || lom.T.Snode() == nil {
		return ""
	}
	return lom.T.Snode().String()
}

func (m *emptyCloudProvider) ListObjects(ctx context.Context, bucket string,
	msg *cmn.SelectMsg) (bckList *cmn.BucketList, err error, errCode int) {
	return nil, cmn.NewErrorCloudBucketOffline(cmn.Bck{Name: bucket}, ""), http.StatusNotFound
}
func (m *emptyCloudProvider) HeadBucket(ctx context.Context, bucket string) (bckProps cmn.SimpleKVs, err error, errCode int) {
	return cmn.SimpleKVs{}, cmn.NewErrorCloudBucketOffline(cmn.Bck{Name: bucket}, ""), http.StatusNotFound
}

// the function must not fail - it should return empty list
func (m *emptyCloudProvider) ListBuckets(ctx context.Context) (buckets []string, err error, errCode int) {
	return []string{}, nil, 0
}
func (m *emptyCloudProvider) HeadObj(ctx context.Context, lom *cluster.LOM) (objMeta cmn.SimpleKVs, err error, errCode int) {
	bck, node := lom.Bck().Bck, _emptyNode(lom)
	return cmn.SimpleKVs{}, cmn.NewErrorCloudBucketDoesNotExist(bck, node), http.StatusNotFound
}
func (m *emptyCloudProvider) GetObj(ctx context.Context, fqn string, lom *cluster.LOM) (err error, errCode int) {
	bck, node := lom.Bck().Bck, _emptyNode(lom)
	return cmn.NewErrorCloudBucketDoesNotExist(bck, node), http.StatusNotFound
}
func (m *emptyCloudProvider) PutObj(ctx context.Context, r io.Reader, lom *cluster.LOM) (version string, err error, errCode int) {
	bck, node := lom.Bck().Bck, _emptyNode(lom)
	return "", cmn.NewErrorCloudBucketDoesNotExist(bck, node), http.StatusNotFound
}
func (m *emptyCloudProvider) DeleteObj(ctx context.Context, lom *cluster.LOM) (err error, errCode int) {
	bck, node := lom.Bck().Bck, _emptyNode(lom)
	return cmn.NewErrorCloudBucketDoesNotExist(bck, node), http.StatusNotFound
}
