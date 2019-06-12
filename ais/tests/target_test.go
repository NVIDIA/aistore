// Package ais_test contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

import (
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
)

func TestPutObjectNoDaemonID(t *testing.T) {
	const (
		bucket  = TestLocalBucketName
		objName = "someObject"
	)
	var (
		sid          string
		objDummyData = []byte("testing is so much fun")
		proxyURL     = getPrimaryURL(t, proxyURLReadOnly)
		smap         = getClusterMap(t, proxyURL)
	)

	for sid = range smap.Tmap {
		break
	}

	url := smap.Tmap[sid].URL(cmn.NetworkPublic)
	baseParams := tutils.BaseAPIParams(url)
	reader := tutils.NewBytesReader(objDummyData)
	putArgs := api.PutObjectArgs{
		BaseParams: baseParams,
		Bucket:     bucket,
		Object:     objName,
		Hash:       reader.XXHash(),
		Reader:     reader,
	}
	if err := api.PutObject(putArgs); err == nil {
		t.Errorf("Error is nil, expected Bad Request error on a PUT to target with no daemon ID query string")
	}
}

func TestDeleteInvalidDaemonID(t *testing.T) {
	var (
		sid = "abcde:abcde"
	)
	if err := api.UnregisterNode(tutils.DefaultBaseAPIParams(t), sid); err == nil {
		t.Errorf("Error is nil, expected NotFound error on a delete of a non-existing target")
	}
}
