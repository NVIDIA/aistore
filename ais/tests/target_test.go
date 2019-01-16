/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

package dfc_test

import (
	"testing"

	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/tutils"
)

func TestPutObjectNoDaemonID(t *testing.T) {
	const (
		bucket  = TestLocalBucketName
		objName = "someObject"
	)
	var (
		sid          string
		objDummyData = []byte("testing is so much fun")
		proxyURL     = getPrimaryURL(t, proxyURLRO)
		smap         = getClusterMap(t, proxyURL)
	)

	for sid = range smap.Tmap {
		break
	}

	url := smap.Tmap[sid].URL(cmn.NetworkPublic)
	baseParams := tutils.BaseAPIParams(url)
	reader := tutils.NewBytesReader(objDummyData)
	if err := api.PutObject(baseParams, bucket, objName, reader.XXHash(), reader); err == nil {
		t.Errorf("Error is nil, expected Bad Request error on a PUT to target with no daemon ID query string")
	}
}

func TestDeleteInvalidDaemonID(t *testing.T) {
	var (
		sid = "abcde:abcde"
	)
	if err := api.UnregisterTarget(tutils.DefaultBaseAPIParams(t), sid); err == nil {
		t.Errorf("Error is nil, expected NotFound error on a delete of a non-existing target")
	}
}
