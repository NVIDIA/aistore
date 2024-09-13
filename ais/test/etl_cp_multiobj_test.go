// Package integration_test.
/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tetl"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/xact"
)

func TestETLMultiObj(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	const (
		objCnt      = 100
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
		bcktests   = []struct {
			srcRemote      bool
			evictRemoteSrc bool
			dstRemote      bool
		}{
			{false, false, false},
			{true, false, false},
			{true, true, false},
			{false, false, true},
		}
	)

	_ = tetl.InitSpec(t, baseParams, transformer, etlCommType)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, transformer) })

	for _, bcktest := range bcktests {
		m := ioContext{
			t:         t,
			num:       objCnt,
			fileSize:  512,
			fixedSize: true, // see checkETLStats below
		}
		if bcktest.srcRemote {
			m.bck = cliBck
			m.deleteRemoteBckObjs = true
		} else {
			m.bck = cmn.Bck{Name: "etlsrc_" + cos.GenTie(), Provider: apc.AIS}
			tools.CreateBucket(t, proxyURL, m.bck, nil, true /*cleanup*/)
		}
		m.init(true /*cleanup*/)

		if bcktest.srcRemote {
			if bcktest.evictRemoteSrc {
				tlog.Logf("evicting %s\n", m.bck)
				//
				// evict all _cached_ data from the "local" cluster
				// keep the src bucket in the "local" BMD though
				//
				err := api.EvictRemoteBucket(baseParams, m.bck, true /*keep empty src bucket in the BMD*/)
				tassert.CheckFatal(t, err)
			}
		}

		tlog.Logf("PUT %d objects (size %d) => %s/test/a-*\n", objCnt, objSize, m.bck)
		for i := range objCnt {
			r, _ := readers.NewRand(objSize, cksumType)
			_, err := api.PutObject(&api.PutArgs{
				BaseParams: baseParams,
				Bck:        m.bck,
				ObjName:    fmt.Sprintf("test/a-%04d", i),
				Reader:     r,
				Size:       objSize,
			})
			tassert.CheckFatal(t, err)
		}

		for _, ty := range []string{"range", "list"} {
			tname := fmt.Sprintf("%s-%s-%s", transformer, strings.TrimSuffix(etlCommType, "://"), ty)
			if bcktest.srcRemote {
				if bcktest.evictRemoteSrc {
					tname += "/from-evicted-remote"
				} else {
					tname += "/from-remote"
				}
			} else {
				debug.Assert(!bcktest.evictRemoteSrc)
				tname += "/from-ais"
			}
			if bcktest.dstRemote {
				tname += "/to-remote"
			} else {
				tname += "/to-ais"
			}
			t.Run(tname, func(t *testing.T) {
				var bckTo cmn.Bck
				if bcktest.dstRemote {
					bckTo = cliBck
					dstm := ioContext{t: t, bck: bckTo}
					dstm.del()
					t.Cleanup(func() { dstm.del() })
				} else {
					bckTo = cmn.Bck{Name: "etldst_" + cos.GenTie(), Provider: apc.AIS}
					// NOTE: ais will create dst bucket on the fly

					t.Cleanup(func() { tools.DestroyBucket(t, proxyURL, bckTo) })
				}
				template := "test/a-" +
					fmt.Sprintf("{%04d..%04d}", rangeStart, rangeStart+copyCnt-1)
				testETLMultiObj(t, transformer, m.bck, bckTo, template, ty, bcktest.evictRemoteSrc)
			})
		}
	}
}

func testETLMultiObj(t *testing.T, etlName string, bckFrom, bckTo cmn.Bck, fileRange, opType string, evictRemoteSrc bool) {
	pt, err := cos.ParseBashTemplate(fileRange)
	tassert.CheckFatal(t, err)

	var (
		xid        string
		proxyURL   = tools.RandomProxyURL(t)
		baseParams = tools.BaseAPIParams(proxyURL)

		objList        = pt.ToSlice()
		objCnt         = len(objList)
		requestTimeout = 30 * time.Second
		tcomsg         = cmn.TCOMsg{ToBck: bckTo}
	)
	tcomsg.Transform.Name = etlName
	tcomsg.Transform.Timeout = cos.Duration(requestTimeout)

	if opType == "list" {
		tcomsg.ListRange.ObjNames = objList
	} else {
		tcomsg.ListRange.Template = fileRange
	}

	tlog.Logf("Starting multi-object ETL[%s] ...\n", etlName)
	if evictRemoteSrc {
		xid, err = api.ETLMultiObj(baseParams, bckFrom, &tcomsg, apc.FltExists)
	} else {
		xid, err = api.ETLMultiObj(baseParams, bckFrom, &tcomsg)
	}
	tassert.CheckFatal(t, err)

	tlog.Logf("Running x-etl[%s]: %s => %s ...\n", xid, bckFrom.Cname(""), bckTo.Cname(""))

	wargs := xact.ArgsMsg{ID: xid, Kind: apc.ActETLObjects}
	err = api.WaitForXactionIdle(baseParams, &wargs)
	tassert.CheckFatal(t, err)

	list, err := api.ListObjects(baseParams, bckTo, nil, api.ListArgs{})
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, len(list.Entries) == objCnt, "expected %d objects from offline ETL, got %d", objCnt, len(list.Entries))
	for _, objName := range objList {
		err := api.DeleteObject(baseParams, bckTo, objName)
		tassert.CheckError(t, err)
		tlog.Logf("%s\n", bckTo.Cname(objName))
	}
}
