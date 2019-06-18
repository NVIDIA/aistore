// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

type (
	emptyCloud struct{} // mock
)

const bucketDoesNotExist = "bucket %q %s"

var (
	_ cloudif = &emptyCloud{}
)

func newEmptyCloud() *emptyCloud { return &emptyCloud{} }

func (m *emptyCloud) ListBucket(ctx context.Context, bucket string, msg *cmn.SelectMsg) (bckList *cmn.BucketList, err error, errcode int) {
	return nil, fmt.Errorf(bucketDoesNotExist, bucket, cmn.DoesNotExist), http.StatusNotFound
}
func (m *emptyCloud) headbucket(ctx context.Context, bucket string) (bucketprops cmn.SimpleKVs, err error, errcode int) {
	return cmn.SimpleKVs{}, fmt.Errorf(bucketDoesNotExist, bucket, cmn.DoesNotExist), http.StatusNotFound
}
func (m *emptyCloud) headobject(ctx context.Context, lom *cluster.LOM) (objmeta cmn.SimpleKVs, err error, errcode int) {
	return cmn.SimpleKVs{}, fmt.Errorf(bucketDoesNotExist, lom.Bucket, cmn.DoesNotExist), http.StatusNotFound
}
func (m *emptyCloud) getobj(ctx context.Context, fqn string, lom *cluster.LOM) (err error, errcode int) {
	return fmt.Errorf(bucketDoesNotExist, lom.Bucket, cmn.DoesNotExist), http.StatusNotFound
}
func (m *emptyCloud) putobj(ctx context.Context, file *os.File, lom *cluster.LOM) (version string, err error, errcode int) {
	return "", fmt.Errorf(bucketDoesNotExist, lom.Bucket, cmn.DoesNotExist), http.StatusNotFound
}
func (m *emptyCloud) deleteobj(ctx context.Context, lom *cluster.LOM) (err error, errcode int) {
	return fmt.Errorf(bucketDoesNotExist, lom.Bucket, cmn.DoesNotExist), http.StatusNotFound
}

// the function must not fail - it should return empty list
func (m *emptyCloud) getbucketnames(ctx context.Context) (buckets []string, err error, errcode int) {
	return []string{}, nil, 0
}
