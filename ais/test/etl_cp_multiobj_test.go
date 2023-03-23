// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tetl"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"
)

func TestETLMultiObj(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	const (
		objCnt      = 50
		copyCnt     = 20
		rangeStart  = 10
		transformer = tetl.MD5
		etlCommType = etl.Hpush
		objSize     = cos.KiB
		cksumType   = cos.ChecksumMD5
	)
	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)

		bck   = cmn.Bck{Name: "etloffline", Provider: apc.AIS}
		bckTo = cmn.Bck{Name: "etloffline-out-" + trand.String(5), Provider: apc.AIS}
	)

	tools.CreateBucketWithCleanup(t, proxyURL, bck, nil)
	tools.CreateBucketWithCleanup(t, proxyURL, bckTo, nil)

	for i := 0; i < objCnt; i++ {
		r, _ := readers.NewRandReader(objSize, cksumType)
		_, err := api.PutObject(api.PutArgs{
			BaseParams: baseParams,
			Bck:        bck,
			ObjName:    fmt.Sprintf("test/a-%04d", i),
			Reader:     r,
			Size:       objSize,
		})
		tassert.CheckFatal(t, err)
	}

	_ = tetl.InitSpec(t, baseParams, transformer, etlCommType)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, transformer) })

	for _, ty := range []string{"range", "list"} {
		t.Run(ty, func(t *testing.T) {
			testETLMultiObj(t, transformer, bck, bckTo,
				"test/a-"+fmt.Sprintf("{%04d..%04d}", rangeStart, rangeStart+copyCnt-1), ty)
		})
	}
}

func testETLMultiObj(t *testing.T, etlName string, bckFrom, bckTo cmn.Bck, fileRange, opType string) {
	pt, err := cos.ParseBashTemplate(fileRange)
	tassert.CheckFatal(t, err)

	var (
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)

		objList        = pt.ToSlice()
		objCnt         = len(objList)
		requestTimeout = 30 * time.Second
		tcomsg         = cmn.TCObjsMsg{
			TCBMsg: apc.TCBMsg{
				Transform: apc.Transform{
					Name:    etlName,
					Timeout: cos.Duration(requestTimeout),
				},
			},
			ToBck: bckTo,
		}
	)
	if opType == "list" {
		tcomsg.ListRange.ObjNames = objList
	} else {
		tcomsg.ListRange.Template = fileRange
	}

	tlog.Logf("Start offline ETL[%s]\n", etlName)
	xid, err := api.ETLMultiObj(baseParams, bckFrom, tcomsg)
	tassert.CheckFatal(t, err)

	wargs := xact.ArgsMsg{ID: xid, Kind: apc.ActETLObjects}
	err = api.WaitForXactionIdle(baseParams, wargs)
	tassert.CheckFatal(t, err)

	list, err := api.ListObjects(baseParams, bckTo, nil, 0)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(list.Entries) == objCnt, "expected %d objects from offline ETL, got %d", objCnt, len(list.Entries))
	for _, objName := range objList {
		err := api.DeleteObject(baseParams, bckTo, objName)
		tassert.CheckError(t, err)
		tlog.Logf("%s\n", bckTo.Cname(objName))
	}
}
