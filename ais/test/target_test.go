// Package integration_test.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
)

func TestPutObjectNoDaemonID(t *testing.T) {
	const (
		objName = "someObject"
	)
	var (
		sid          string
		objDummyData = []byte("testing is so much fun")
		proxyURL     = tools.RandomProxyURL()
		smap         = tools.GetClusterMap(t, proxyURL)
		bck          = cmn.Bck{
			Name:     testBucketName,
			Provider: apc.AIS,
		}
	)

	si, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	sid = si.ID()

	url := smap.Tmap[sid].URL(cmn.NetPublic)
	baseParams := tools.BaseAPIParams(url)
	reader := readers.NewBytes(objDummyData)
	putArgs := api.PutArgs{
		BaseParams: baseParams,
		Bck:        bck,
		ObjName:    objName,
		Cksum:      reader.Cksum(),
		Reader:     reader,
	}
	if _, err := api.PutObject(&putArgs); err == nil {
		t.Error("Error is nil, expected Bad Request error on a PUT to target with no daemon ID query string")
	}
}

func TestDeleteInvalidDaemonID(t *testing.T) {
	val := &apc.ActValRmNode{
		DaemonID:          "abcde:abcde",
		SkipRebalance:     true,
		KeepInitialConfig: true,
	}
	tlog.Logfln("Decommission invalid node %s (expecting to fail)", val.DaemonID)
	if _, err := tools.DecommissionNode(tools.BaseAPIParams(), val); err == nil {
		t.Error("Error is nil, expected NotFound error on a delete of a non-existing target")
	}
}
