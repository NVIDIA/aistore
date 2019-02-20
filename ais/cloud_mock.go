// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"os"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

type (
	emptyCloud struct{} // mock
)

var (
	_ cloudif = &emptyCloud{}
)

func newEmptyCloud() *emptyCloud { return &emptyCloud{} }

func (m *emptyCloud) listbucket(ctx context.Context, bucket string, msg *cmn.GetMsg) (jsbytes []byte, errstr string, errcode int) {
	return []byte{}, "", 0
}
func (m *emptyCloud) headbucket(ctx context.Context, bucket string) (bucketprops cmn.SimpleKVs, errstr string, errcode int) {
	return
}
func (m *emptyCloud) getbucketnames(ctx context.Context) (buckets []string, errstr string, errcode int) {
	return []string{}, "", 0
}
func (m *emptyCloud) headobject(ctx context.Context, bucket string, objname string) (objmeta cmn.SimpleKVs, errstr string, errcode int) {
	return cmn.SimpleKVs{}, "", 0
}
func (m *emptyCloud) getobj(ctx context.Context, fqn, bucket, objname string) (props *cluster.LOM, errstr string, errcode int) {
	return
}
func (m *emptyCloud) putobj(ctx context.Context, file *os.File, bucket, objname string, cksum cmn.CksumProvider) (version string, errstr string, errcode int) {
	return
}
func (m *emptyCloud) deleteobj(ctx context.Context, bucket, objname string) (errstr string, errcode int) {
	return
}
