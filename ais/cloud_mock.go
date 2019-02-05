// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"net/http"
	"os"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

const (
	notEnabledCloud = "3rd party cloud is not enabled in this build"
)

type (
	emptyCloud struct { // mock
		errstr string
	}
)

var (
	_ cloudif = &emptyCloud{}
)

func newEmptyCloud() *emptyCloud { return &emptyCloud{notEnabledCloud} }

func (m *emptyCloud) listbucket(ctx context.Context, bucket string, msg *cmn.GetMsg) (jsbytes []byte, errstr string, errcode int) {
	errstr = m.errstr
	errcode = http.StatusBadRequest
	return
}
func (m *emptyCloud) headbucket(ctx context.Context, bucket string) (bucketprops cmn.SimpleKVs, errstr string, errcode int) {
	errstr = m.errstr
	errcode = http.StatusBadRequest
	return
}
func (m *emptyCloud) getbucketnames(ctx context.Context) (buckets []string, errstr string, errcode int) {
	errstr = m.errstr
	errcode = http.StatusBadRequest
	return
}
func (m *emptyCloud) headobject(ctx context.Context, bucket string, objname string) (objmeta cmn.SimpleKVs, errstr string, errcode int) {
	errstr = m.errstr
	errcode = http.StatusBadRequest
	return
}
func (m *emptyCloud) getobj(ctx context.Context, fqn, bucket, objname string) (props *cluster.LOM, errstr string, errcode int) {
	errstr = m.errstr
	errcode = http.StatusBadRequest
	return
}
func (m *emptyCloud) putobj(ctx context.Context, file *os.File, bucket, objname string, ohobj cmn.CksumValue) (version string, errstr string, errcode int) {
	errstr = m.errstr
	errcode = http.StatusBadRequest
	return
}
func (m *emptyCloud) deleteobj(ctx context.Context, bucket, objname string) (errstr string, errcode int) {
	errstr = m.errstr
	errcode = http.StatusBadRequest
	return
}
