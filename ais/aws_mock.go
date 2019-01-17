// +build !aws

// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"net/http"
	"os"

	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
)

const (
	notEnabledAWS = "AWS cloud is not enabled in this build"
)

type (
	awsimpl struct { // mock
		t *targetrunner
	}
)

var (
	_ cloudif = &awsimpl{}
)

func (*awsimpl) listbucket(ctx context.Context, bucket string, msg *cmn.GetMsg) (jsbytes []byte, errstr string, errcode int) {
	errstr = notEnabledAWS
	errcode = http.StatusBadRequest
	return
}
func (*awsimpl) headbucket(ctx context.Context, bucket string) (bucketprops cmn.SimpleKVs, errstr string, errcode int) {
	errstr = notEnabledAWS
	errcode = http.StatusBadRequest
	return
}
func (*awsimpl) getbucketnames(ctx context.Context) (buckets []string, errstr string, errcode int) {
	errstr = notEnabledAWS
	errcode = http.StatusBadRequest
	return
}
func (*awsimpl) headobject(ctx context.Context, bucket string, objname string) (objmeta cmn.SimpleKVs, errstr string, errcode int) {
	errstr = notEnabledAWS
	errcode = http.StatusBadRequest
	return
}
func (*awsimpl) getobj(ctx context.Context, fqn, bucket, objname string) (props *cluster.LOM, errstr string, errcode int) {
	errstr = notEnabledAWS
	errcode = http.StatusBadRequest
	return
}
func (*awsimpl) putobj(ctx context.Context, file *os.File, bucket, objname string, ohobj cmn.CksumValue) (version string, errstr string, errcode int) {
	errstr = notEnabledAWS
	errcode = http.StatusBadRequest
	return
}
func (*awsimpl) deleteobj(ctx context.Context, bucket, objname string) (errstr string, errcode int) {
	errstr = notEnabledAWS
	errcode = http.StatusBadRequest
	return
}
