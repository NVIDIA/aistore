// Package integration_test.
/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tetl"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/xact"
)

func TestETLMultiObj(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s})
	// tetl.CheckNoRunningETLContainers(t, baseParams)

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
		prefix     = "prefix-" + cos.GenUUID()
		bcktests   = []testBucketConfig{{false, false, false}}
	)

	if cliBck.IsRemote() {
		bcktests = append(bcktests,
			testBucketConfig{true, false, false},
			testBucketConfig{true, true, false},
			testBucketConfig{false, false, true},
		)
	}

	msg := tetl.InitSpec(t, baseParams, transformer, etlCommType, etl.ArgTypeDefault)
	t.Cleanup(func() { tetl.StopAndDeleteETL(t, baseParams, msg.Name()) })

	for _, bcktest := range bcktests {
		bckFrom, tname := bcktest.setupBckFrom(t, prefix, objCnt)

		for _, ty := range []string{"range", "list"} {
			t.Run(tname+fmt.Sprintf("/%s-%s-%s", msg.Name(), strings.TrimSuffix(etlCommType, "://"), ty), func(t *testing.T) {
				template := fmt.Sprintf("%s/{%04d..%04d}", prefix, rangeStart, rangeStart+copyCnt-1)
				testETLMultiObj(t, msg.Name(), prefix, bckFrom, template, ty, bcktest, tetl.MD5Transform)
			})
		}
	}
}

func testETLMultiObj(t *testing.T, etlName, prefix string, bckFrom cmn.Bck, fileRange, opType string, bcktest testBucketConfig, transform transformFunc) {
	pt, err := cos.ParseBashTemplate(fileRange)
	tassert.CheckFatal(t, err)

	var (
		xid            string
		proxyURL       = tools.RandomProxyURL(t)
		baseParams     = tools.BaseAPIParams(proxyURL)
		lst            []string
		requestTimeout = 30 * time.Second
		tcomsg         = cmn.TCOMsg{}
	)

	lst, err = pt.Expand()
	tassert.CheckFatal(t, err)
	objCnt := len(lst)

	bckTo := bcktest.setupBckTo(t, prefix, objCnt)
	tcomsg.ToBck = bckTo
	tcomsg.Transform.Name = etlName
	tcomsg.Transform.Timeout = cos.Duration(requestTimeout)

	if opType == "list" {
		tcomsg.ListRange.ObjNames = lst
	} else {
		tcomsg.ListRange.Template = fileRange
	}

	tlog.Logf("Starting multi-object ETL[%s] ...\n", etlName)
	if bcktest.evictRemoteSrc {
		tools.EvictObjects(t, proxyURL, bckFrom, prefix)
		xid, err = api.ETLMultiObj(baseParams, bckFrom, &tcomsg, apc.FltExists)
	} else {
		xid, err = api.ETLMultiObj(baseParams, bckFrom, &tcomsg)
	}
	tassert.CheckFatal(t, err)

	tlog.Logf("Running x-etl[%s]: %s => %s ...\n", xid, bckFrom.Cname(""), bckTo.Cname(""))

	wargs := xact.ArgsMsg{ID: xid, Kind: apc.ActETLObjects}
	err = api.WaitForXactionIdle(baseParams, &wargs)
	tassert.CheckFatal(t, err)

	err = tetl.ListObjectsWithRetry(baseParams, bckTo, prefix, objCnt, tools.WaitRetryOpts{MaxRetries: 5, Interval: time.Second * 3})
	tassert.CheckFatal(t, err)
	if !testing.Short() && transform != nil {
		tlog.Logfln("Verifying %d object content", objCnt)
		for i, objName := range lst {
			if i > 0 && i%max(1, objCnt/10) == 0 {
				tlog.Logf("Verified %d/%d objects\n", i, objCnt)
			}
			err := tools.WaitForCondition(func() bool {
				r1, _, err := api.GetObjectReader(baseParams, bckFrom, objName, &api.GetArgs{})
				if err != nil {
					return false
				}
				defer r1.Close()

				r2, _, err := api.GetObjectReader(baseParams, bckTo, objName, &api.GetArgs{})
				if err != nil {
					return false
				}
				defer r2.Close()

				return tools.ReaderEqual(transform(r1), r2)
			}, tools.WaitRetryOpts{MaxRetries: 5, Interval: time.Second})
			tassert.Fatalf(t, err == nil, "object content mismatch after retries: %s vs %s", bckFrom.Cname(objName), bckTo.Cname(objName))
		}
	}
}
