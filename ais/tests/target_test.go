// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/readers"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func TestPutObjectNoDaemonID(t *testing.T) {
	const (
		objName = "someObject"
	)
	var (
		sid          string
		objDummyData = []byte("testing is so much fun")
		proxyURL     = tutils.RandomProxyURL()
		smap         = tutils.GetClusterMap(t, proxyURL)
		bck          = cmn.Bck{
			Name:     testBucketName,
			Provider: cmn.ProviderAIS,
		}
	)

	si, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	sid = si.ID()

	url := smap.Tmap[sid].URL(cmn.NetworkPublic)
	baseParams := tutils.BaseAPIParams(url)
	reader := readers.NewBytesReader(objDummyData)
	putArgs := api.PutObjectArgs{
		BaseParams: baseParams,
		Bck:        bck,
		Object:     objName,
		Cksum:      reader.Cksum(),
		Reader:     reader,
	}
	if err := api.PutObject(putArgs); err == nil {
		t.Errorf("Error is nil, expected Bad Request error on a PUT to target with no daemon ID query string")
	}
}

func TestDeleteInvalidDaemonID(t *testing.T) {
	val := &cmn.ActValDecommision{
		DaemonID:      "abcde:abcde",
		SkipRebalance: true,
	}
	if _, err := api.Decommission(tutils.BaseAPIParams(), val); err == nil {
		t.Errorf("Error is nil, expected NotFound error on a delete of a non-existing target")
	}
}
