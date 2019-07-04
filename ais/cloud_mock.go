// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

type (
	emptyCloudProvider struct{}
)

const bucketDoesNotExist = "bucket %q %s"

var (
	_ cloudProvider = &emptyCloudProvider{}
)

func newEmptyCloud() cloudProvider { return &emptyCloudProvider{} }

func (m *emptyCloudProvider) ListBucket(ctx context.Context, bucket string, msg *cmn.SelectMsg) (bckList *cmn.BucketList, err error, errCode int) {
	return nil, fmt.Errorf(bucketDoesNotExist, bucket, cmn.DoesNotExist), http.StatusNotFound
}
func (m *emptyCloudProvider) headBucket(ctx context.Context, bucket string) (bckProps cmn.SimpleKVs, err error, errCode int) {
	return cmn.SimpleKVs{}, fmt.Errorf(bucketDoesNotExist, bucket, cmn.DoesNotExist), http.StatusNotFound
}

// the function must not fail - it should return empty list
func (m *emptyCloudProvider) getBucketNames(ctx context.Context) (buckets []string, err error, errCode int) {
	return []string{}, nil, 0
}
func (m *emptyCloudProvider) headObj(ctx context.Context, lom *cluster.LOM) (objMeta cmn.SimpleKVs, err error, errCode int) {
	return cmn.SimpleKVs{}, fmt.Errorf(bucketDoesNotExist, lom.Bucket, cmn.DoesNotExist), http.StatusNotFound
}
func (m *emptyCloudProvider) getObj(ctx context.Context, fqn string, lom *cluster.LOM) (err error, errCode int) {
	return fmt.Errorf(bucketDoesNotExist, lom.Bucket, cmn.DoesNotExist), http.StatusNotFound
}
func (m *emptyCloudProvider) putObj(ctx context.Context, r io.Reader, lom *cluster.LOM) (version string, err error, errCode int) {
	return "", fmt.Errorf(bucketDoesNotExist, lom.Bucket, cmn.DoesNotExist), http.StatusNotFound
}
func (m *emptyCloudProvider) deleteObj(ctx context.Context, lom *cluster.LOM) (err error, errCode int) {
	return fmt.Errorf(bucketDoesNotExist, lom.Bucket, cmn.DoesNotExist), http.StatusNotFound
}
