// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"math/rand"
	"sync/atomic"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/readers"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tetl"
	"github.com/NVIDIA/aistore/devtools/tlog"
	"github.com/NVIDIA/aistore/devtools/trand"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/etl"
)

func TestCopyObjRange(t *testing.T) {
	const (
		objCnt      = 100
		copyCnt     = 20
		rangeStart  = 10
		objSize     = cos.KiB
		cksumType   = cos.ChecksumMD5
		waitTimeout = 45 * time.Second
	)
	var (
		proxyURL   = tutils.RandomProxyURL(t)
		bckFrom    = cmn.Bck{Name: "cp-range-from", Provider: apc.ProviderAIS}
		bckTo      = cmn.Bck{Name: "cp-range-to", Provider: apc.ProviderAIS}
		objList    = make([]string, 0, objCnt)
		baseParams = tutils.BaseAPIParams(proxyURL)
		xactID     string
		err        error
	)
	tutils.CreateBucketWithCleanup(t, proxyURL, bckFrom, nil)
	tutils.CreateBucketWithCleanup(t, proxyURL, bckTo, nil)
	for i := 0; i < objCnt; i++ {
		objList = append(objList,
			fmt.Sprintf("test/a-%04d", i),
		)
	}
	for i := 0; i < 5; i++ {
		for _, objName := range objList {
			r, _ := readers.NewRandReader(objSize, cksumType)
			err := api.PutObject(api.PutObjectArgs{
				BaseParams: baseParams,
				Bck:        bckFrom,
				Object:     objName,
				Reader:     r,
				Size:       objSize,
			})
			tassert.CheckFatal(t, err)
		}

		template := "test/a-" + fmt.Sprintf("{%04d..%04d}", rangeStart, rangeStart+copyCnt-1)
		tlog.Logf("template: [%s]\n", template)
		msg := cmn.TCObjsMsg{SelectObjsMsg: cmn.SelectObjsMsg{Template: template}, ToBck: bckTo}
		xactID, err = api.CopyMultiObj(baseParams, bckFrom, msg)
		tassert.CheckFatal(t, err)
	}

	wargs := api.XactReqArgs{ID: xactID, Kind: apc.ActCopyObjects}
	api.WaitForXactionIdle(baseParams, wargs)

	msg := &apc.ListObjsMsg{Prefix: "test/"}
	bckList, err := api.ListObjects(baseParams, bckTo, msg, 0)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(bckList.Entries) == copyCnt, "%d != %d", copyCnt, len(bckList.Entries))
	for i := rangeStart; i < rangeStart+copyCnt; i++ {
		objName := fmt.Sprintf("test/a-%04d", i)
		err := api.DeleteObject(baseParams, bckTo, objName)
		tassert.CheckError(t, err)
		tlog.Logf("%s/%s\n", bckTo.Name, objName)
	}
}

func TestCopyMultiObj(t *testing.T) {
	runProviderTests(t, func(t *testing.T, bck *cluster.Bck) {
		testCopyMobj(t, bck)
	})
}

func testCopyMobj(t *testing.T, bck *cluster.Bck) {
	const objCnt = 200
	var (
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)

		m = ioContext{
			t:       t,
			bck:     bck.Clone(),
			num:     objCnt,
			prefix:  "copy-multiobj/",
			ordered: true,
		}
		toBck     = cmn.Bck{Name: trand.String(10), Provider: apc.ProviderAIS}
		numToCopy = cos.Min(m.num/2, 13)
		fmtRange  = "%s{%d..%d}"
		subtests  = []struct {
			list bool
		}{
			{true}, {false},
		}
	)
	for _, test := range subtests {
		tname := "list"
		if !test.list {
			tname = "range"
		}
		t.Run(tname, func(t *testing.T) {
			if m.bck.IsRemote() {
				m.num = objCnt / 3
			}
			m.initWithCleanup()
			m.puts()
			if m.bck.IsRemote() {
				defer m.del()
			}
			if !toBck.Equal(&m.bck) && toBck.IsAIS() {
				tutils.CreateBucketWithCleanup(t, proxyURL, toBck, nil)
			}
			var erv atomic.Value
			if test.list {
				for i := 0; i < numToCopy && erv.Load() == nil; i++ {
					list := make([]string, 0, numToCopy)
					for j := 0; j < numToCopy; j++ {
						list = append(list, m.objNames[rand.Intn(m.num)])
					}
					go func(list []string) {
						msg := cmn.TCObjsMsg{SelectObjsMsg: cmn.SelectObjsMsg{ObjNames: list}, ToBck: toBck}
						if _, err := api.CopyMultiObj(baseParams, m.bck, msg); err != nil {
							erv.Store(err)
						}
					}(list)
				}
			} else {
				for i := 0; i < numToCopy && erv.Load() == nil; i++ {
					start := rand.Intn(m.num - numToCopy)
					go func(start int) {
						template := fmt.Sprintf(fmtRange, m.prefix, start, start+numToCopy-1)
						msg := cmn.TCObjsMsg{SelectObjsMsg: cmn.SelectObjsMsg{Template: template}, ToBck: toBck}
						if _, err := api.CopyMultiObj(baseParams, m.bck, msg); err != nil {
							erv.Store(err)
						}
					}(start)
				}
			}
			if erv.Load() != nil {
				tassert.CheckFatal(t, erv.Load().(error))
			}
			wargs := api.XactReqArgs{Kind: apc.ActCopyObjects, Bck: m.bck}
			api.WaitForXactionIdle(baseParams, wargs)

			msg := &apc.ListObjsMsg{Prefix: m.prefix}
			msg.AddProps(apc.GetPropsName, apc.GetPropsSize)
			objList, err := api.ListObjects(baseParams, toBck, msg, 0)
			tassert.CheckFatal(t, err)
			tlog.Logf("%s => %s: copied %d objects\n", m.bck, toBck, len(objList.Entries))
		})
	}
}

func TestETLMultiObj(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{RequiredDeployment: tutils.ClusterTypeK8s})
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
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)

		bck   = cmn.Bck{Name: "etloffline", Provider: apc.ProviderAIS}
		toBck = cmn.Bck{Name: "etloffline-out-" + trand.String(5), Provider: apc.ProviderAIS}
	)

	tutils.CreateBucketWithCleanup(t, proxyURL, bck, nil)
	tutils.CreateBucketWithCleanup(t, proxyURL, toBck, nil)

	for i := 0; i < objCnt; i++ {
		r, _ := readers.NewRandReader(objSize, cksumType)
		err := api.PutObject(api.PutObjectArgs{
			BaseParams: baseParams,
			Bck:        bck,
			Object:     fmt.Sprintf("test/a-%04d", i),
			Reader:     r,
			Size:       objSize,
		})
		tassert.CheckFatal(t, err)
	}

	uuid := tetl.Init(t, baseParams, transformer, etlCommType)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, uuid) })

	for _, ty := range []string{"range", "list"} {
		t.Run(ty, func(t *testing.T) {
			testETLMultiObj(t, uuid, bck, toBck, "test/a-"+fmt.Sprintf("{%04d..%04d}", rangeStart, rangeStart+copyCnt-1), ty)
		})
	}
}

func testETLMultiObj(t *testing.T, uuid string, fromBck, toBck cmn.Bck, fileRange, opType string) {
	pt, err := cos.ParseBashTemplate(fileRange)
	tassert.CheckFatal(t, err)

	var (
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)

		objList        = pt.ToSlice()
		objCnt         = len(objList)
		requestTimeout = 30 * time.Second
		tcoMsg         = cmn.TCObjsMsg{
			TCBMsg: apc.TCBMsg{
				ID:             uuid,
				RequestTimeout: cos.Duration(requestTimeout),
			},
			ToBck: toBck,
		}
	)
	if opType == "list" {
		tcoMsg.SelectObjsMsg.ObjNames = objList
	} else {
		tcoMsg.SelectObjsMsg.Template = fileRange
	}

	tlog.Logf("Start offline ETL %q\n", uuid)
	xactID, err := api.ETLMultiObj(baseParams, fromBck, tcoMsg)
	tassert.CheckFatal(t, err)

	wargs := api.XactReqArgs{ID: xactID, Kind: apc.ActETLObjects}
	err = api.WaitForXactionIdle(baseParams, wargs)
	tassert.CheckFatal(t, err)

	list, err := api.ListObjects(baseParams, toBck, nil, 0)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(list.Entries) == objCnt, "expected %d objects from offline ETL, got %d", objCnt, len(list.Entries))
	for _, objName := range objList {
		err := api.DeleteObject(baseParams, toBck, objName)
		tassert.CheckError(t, err)
		tlog.Logf("%s/%s\n", toBck.Name, objName)
	}
}
