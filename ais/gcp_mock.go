// +build !gcp

// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dfc

import (
	"context"
	"net/http"
	"os"

	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
)

const (
	notEnabledGCP = "GCP cloud is not enabled in this build"
)

type (
	gcpimpl struct {
		t *targetrunner
	}
)

var (
	_ cloudif = &gcpimpl{}
)

func (*gcpimpl) listbucket(ctx context.Context, bucket string, msg *cmn.GetMsg) (jsbytes []byte, errstr string, errcode int) {
	errstr = notEnabledGCP
	errcode = http.StatusBadRequest
	return
}
func (*gcpimpl) headbucket(ctx context.Context, bucket string) (bucketprops cmn.SimpleKVs, errstr string, errcode int) {
	errstr = notEnabledGCP
	errcode = http.StatusBadRequest
	return
}
func (*gcpimpl) getbucketnames(ctx context.Context) (buckets []string, errstr string, errcode int) {
	errstr = notEnabledGCP
	errcode = http.StatusBadRequest
	return
}
func (*gcpimpl) headobject(ctx context.Context, bucket string, objname string) (objmeta cmn.SimpleKVs, errstr string, errcode int) {
	errstr = notEnabledGCP
	errcode = http.StatusBadRequest
	return
}
func (*gcpimpl) getobj(ctx context.Context, fqn, bucket, objname string) (props *cluster.LOM, errstr string, errcode int) {
	errstr = notEnabledGCP
	errcode = http.StatusBadRequest
	return
}
func (*gcpimpl) putobj(ctx context.Context, file *os.File, bucket, objname string, ohobj cmn.CksumValue) (version string, errstr string, errcode int) {
	errstr = notEnabledGCP
	errcode = http.StatusBadRequest
	return
}
func (*gcpimpl) deleteobj(ctx context.Context, bucket, objname string) (errstr string, errcode int) {
	errstr = notEnabledGCP
	errcode = http.StatusBadRequest
	return
}
