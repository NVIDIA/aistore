// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/devtools/tutils/tassert"
	"github.com/NVIDIA/aistore/devtools/tutils/tetl"
	"github.com/NVIDIA/aistore/etl"
	"github.com/NVIDIA/aistore/etl/runtime"
)

func TestETLConnectionError(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{K8s: true})

	// ETL should survive occasional failures and successfully transform all objects.
	const timeoutFunc = `
import random, requests, hashlib

failures = {}

def transform(input_bytes):
	md5 = hashlib.md5(input_bytes).hexdigest()
	failures_cnt = failures.get(md5, 0)
	# Fail at most 2 times, otherwise ETL will be stopped.
	if random.randint(0,50) == 0 and failures_cnt < 2:
		failures[md5] = failures_cnt + 1
		raise requests.exceptions.ConnectionError("fake connection error")

	return input_bytes
`

	m := ioContext{
		t:        t,
		num:      10_000,
		fileSize: cmn.KiB,
		bck:      cmn.Bck{Name: "etl_build_connection_err", Provider: cmn.ProviderAIS},
	}

	tutils.Logln("Preparing source bucket")
	tutils.CreateFreshBucket(t, proxyURL, m.bck, nil)

	m.init()
	tutils.Logln("Putting objects to source bucket")
	m.puts()

	uuid, err := api.ETLBuild(baseParams, etl.BuildMsg{
		Code:        []byte(timeoutFunc),
		Runtime:     runtime.Python3,
		WaitTimeout: cmn.DurationJSON(5 * time.Minute),
	})
	tassert.CheckFatal(t, err)
	testETLBucket(t, uuid, m.bck, m.num, 0 /*skip bytes check*/, 5*time.Minute)
}

func TestETLTargetDown(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{K8s: true, Long: true, MinTargets: 2})

	var (
		bckFrom = cmn.Bck{Name: "etltargetdown", Provider: cmn.ProviderAIS}
		bckTo   = cmn.Bck{Name: "etloffline-out-" + cmn.RandString(5), Provider: cmn.ProviderAIS}

		m = ioContext{
			t:         t,
			num:       10000,
			fileSize:  512,
			fixedSize: true,
			bck:       bckFrom,
		}
	)

	tutils.Logln("Preparing source bucket")
	tutils.CreateFreshBucket(t, proxyURL, bckFrom, nil)
	defer tutils.DestroyBucket(t, proxyURL, bckFrom)
	m.saveClusterState()

	tutils.Logln("Putting objects to source bucket")
	m.puts()

	uuid, err := etlInit(tetl.Echo, etl.RedirectCommType)
	tassert.CheckFatal(t, err)

	tutils.Logf("Start offline ETL %q\n", uuid)
	defer tutils.DestroyBucket(t, proxyURL, bckTo)
	xactID, err := api.ETLBucket(baseParams, bckFrom, bckTo, &cmn.Bck2BckMsg{ID: uuid})
	tassert.CheckFatal(t, err)

	tutils.Logf("Waiting for ETL to process a few objects")
	time.Sleep(5 * time.Second)
	args := api.XactReqArgs{ID: xactID, Kind: cmn.ActETLBck, Timeout: time.Minute}
	err = api.WaitForXactionToStart(baseParams, args)
	tassert.CheckFatal(t, err)

	tutils.Logf("Unregistering a target")
	unregistered := m.unregisterTarget()
	defer m.reregisterTarget(unregistered)

	tutils.Logf("Waiting for ETL to be aborted")
	args = api.XactReqArgs{ID: xactID, Kind: cmn.ActETLBck, Timeout: 30 * time.Second}
	status, err := api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, status.Aborted(), "expected etl bucket to be aborted")

	// Give some time for ETLs to be stopped.
	time.Sleep(20 * time.Second)

	etls, err := api.ETLList(baseParams)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, len(etls) == 0, "expected ETLs to be stopped when a targets membership changed, got %v", etls)
}

func TestETLBigBucket(t *testing.T) {
	// The test takes a lot of time if it's run against a single target deployment.
	tutils.CheckSkip(t, tutils.SkipTestArgs{K8s: true, Long: true, MinTargets: 2})

	const echoPythonTransform = `
def transform(input_bytes):
	return input_bytes
`

	var (
		bckFrom = cmn.Bck{Provider: cmn.ProviderAIS, Name: "etlbig"}
		bckTo   = cmn.Bck{Provider: cmn.ProviderAIS, Name: "etlbigout-" + cmn.RandString(5)}

		etlDoneCh = cmn.NewStopCh()

		m = ioContext{
			t:         t,
			num:       200_000,
			fileSize:  20 * cmn.KiB, // 4Gib total
			fixedSize: true,
			bck:       bckFrom,
		}

		tests = []struct {
			name      string
			ty        string
			initDesc  string
			buildDesc etl.BuildMsg
		}{
			{name: "init-echo-python", ty: cmn.ETLInit, initDesc: tetl.Echo},
			{name: "init-echo-golang", ty: cmn.ETLInit, initDesc: tetl.EchoGolang},

			{
				name: "build-echo-python2",
				ty:   cmn.ETLBuild,
				buildDesc: etl.BuildMsg{
					Code:        []byte(echoPythonTransform),
					Runtime:     runtime.Python2,
					WaitTimeout: cmn.DurationJSON(10 * time.Minute),
				},
			},
			{
				name: "build-echo-python3",
				ty:   cmn.ETLBuild,
				buildDesc: etl.BuildMsg{
					Code:        []byte(echoPythonTransform),
					Runtime:     runtime.Python3,
					WaitTimeout: cmn.DurationJSON(10 * time.Minute),
				},
			},
		}
	)

	tutils.Logf("Preparing source bucket (%d objects, %s each)\n", m.num, cmn.B2S(int64(m.fileSize), 2))
	tutils.CreateFreshBucket(t, proxyURL, bckFrom, nil)
	m.saveClusterState()

	tutils.Logln("Putting objects to source bucket")

	m.puts()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var (
				uuid string
				err  error
			)
			switch test.ty {
			case cmn.ETLInit:
				uuid, err = etlInit(test.initDesc, etl.RedirectCommType)
			case cmn.ETLBuild:
				uuid, err = api.ETLBuild(baseParams, test.buildDesc)
			default:
				panic(test.ty)
			}
			tassert.CheckFatal(t, err)
			defer api.ETLStop(baseParams, uuid)

			tutils.Logf("Start offline ETL %q\n", uuid)
			xactID, err := api.ETLBucket(baseParams, bckFrom, bckTo, &cmn.Bck2BckMsg{ID: uuid})
			tassert.CheckFatal(t, err)

			go func() {
				var (
					xactStart = time.Now()
					etlTicker = time.NewTicker(2 * time.Minute)
				)
				defer etlTicker.Stop()
				for {
					select {
					case <-etlTicker.C:
						// Check number of objects transformed.
						stats, err := api.GetXactionStatsByID(baseParams, xactID)
						if err != nil {
							tutils.Logf("Failed to get xaction stats; err %v\n", err)
							continue
						}
						bps := float64(stats.BytesCount()) / time.Since(xactStart).Seconds()
						bpsStr := fmt.Sprintf("%s/s", cmn.B2S(int64(bps), 2))
						tutils.Logf("ETL %q already transformed %d/%d objects (%s) (%s)\n", xactID, stats.ObjCount(), m.num, cmn.B2S(stats.BytesCount(), 2), bpsStr)
					case <-etlDoneCh.Listen():
						return
					}
				}
			}()

			tutils.Logln("Waiting for ETL to finish")
			args := api.XactReqArgs{ID: xactID, Kind: cmn.ActETLBck, Timeout: 30 * time.Minute /* total timeout */}
			_, err = api.WaitForXaction(baseParams, args, time.Minute /* refresh interval */)
			etlDoneCh.Close()
			tassert.CheckFatal(t, err)

			stats, err := api.GetXactionStatsByID(baseParams, xactID)
			tassert.CheckFatal(t, err)
			tutils.Logf("ETL Bucket took %s\n", stats.TotalRunningTime())

			objList, err := api.ListObjects(baseParams, bckTo, nil, 0)
			tassert.CheckFatal(t, err)
			tassert.Fatalf(
				t, len(objList.Entries) == m.num,
				"expected %d objects to be transformed, got %d", m.num, len(objList.Entries),
			)
		})
	}
}
