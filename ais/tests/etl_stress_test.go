// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2021-2022, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"math/rand"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/ext/etl/runtime"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tetl"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
)

func TestETLConnectionError(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s, Long: true})
	tetl.CheckNoRunningETLContainers(t, baseParams)

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
		fileSize: cos.KiB,
		bck:      cmn.Bck{Name: "etl_build_connection_err", Provider: apc.AIS},
	}

	tlog.Logln("Preparing source bucket")
	tools.CreateBucketWithCleanup(t, proxyURL, m.bck, nil)

	m.initWithCleanup()
	m.puts()

	msg := etl.InitCodeMsg{
		InitMsgBase: etl.InitMsgBase{IDX: "etl-build-conn-err", Timeout: cos.Duration(5 * time.Minute)},
		Code:        []byte(timeoutFunc),
		Runtime:     runtime.Py38,
		ChunkSize:   0,
	}
	msg.Funcs.Transform = "transform"

	uuid := tetl.InitCode(t, baseParams, msg)

	testETLBucket(t, uuid, m.bck, m.num, 0 /*skip bytes check*/, 5*time.Minute)
}

func TestETLBucketAbort(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s, Long: true})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	m := &ioContext{
		t:         t,
		num:       1000,
		fileSize:  512,
		fixedSize: true,
	}

	xactID := etlPrepareAndStart(t, m, tetl.Echo, etl.Hpull)
	args := api.XactReqArgs{ID: xactID, Kind: apc.ActETLBck}
	time.Sleep(time.Duration(rand.Intn(5)) * time.Second)

	tlog.Logf("Aborting ETL xaction %q\n", xactID)
	err := api.AbortXaction(baseParams, args)
	tassert.CheckFatal(t, err)
	err = tetl.WaitForAborted(baseParams, xactID, 5*time.Minute)
	tassert.CheckFatal(t, err)
	etls, err := api.ETLList(baseParams)
	tassert.CheckFatal(t, err)
	// ETL stopped via etlPrepareAndStart.
	tassert.Fatalf(t, len(etls) == 1, "expected exactly 1 etl running, got %+v", etls)
}

func TestETLTargetDown(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s, MinTargets: 2})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	m := &ioContext{
		t:         t,
		num:       10000,
		fileSize:  512,
		fixedSize: true,
	}
	if testing.Short() {
		m.num /= 100
	} else {
		// TODO: otherwise, error executing LSOF command
		t.Skipf("skipping %s long test (kill-node vs maintenance vs ETL)", t.Name())
	}
	m.initWithCleanupAndSaveState()
	xactID := etlPrepareAndStart(t, m, tetl.Echo, etl.Hpull)

	tlog.Logln("Waiting for ETL to process a few objects...")
	time.Sleep(5 * time.Second)

	targetNode, _ := m.smap.GetRandTarget()
	tlog.Logf("Killing %s\n", targetNode.StringEx())
	tcmd, err := tools.KillNode(targetNode) // TODO: alternatively, m.startMaintenanceNoRebalance()
	tassert.CheckFatal(t, err)

	t.Cleanup(func() {
		time.Sleep(4 * time.Second)
		tools.RestoreNode(tcmd, false, "target")
		m.waitAndCheckCluState()

		args := api.XactReqArgs{Kind: apc.ActRebalance, Timeout: rebalanceTimeout}
		_, _ = api.WaitForXactionIC(baseParams, args)

		tetl.CheckNoRunningETLContainers(t, baseParams)
	})

	err = tetl.WaitForAborted(baseParams, xactID, 5*time.Minute)
	tassert.CheckFatal(t, err)
	tetl.WaitForContainersStopped(t, baseParams)
}

func TestETLBigBucket(t *testing.T) {
	// The test takes a lot of time if it's run against a single target deployment.
	tools.CheckSkip(t, tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s, Long: true, MinTargets: 2})

	const echoPythonTransform = `
def transform(input_bytes):
	return input_bytes
`

	var (
		bckFrom = cmn.Bck{Provider: apc.AIS, Name: "etlbig"}
		bckTo   = cmn.Bck{Provider: apc.AIS, Name: "etlbigout-" + trand.String(5)}

		m = ioContext{
			t:         t,
			num:       200_000,
			fileSize:  20 * cos.KiB, // 4Gib total
			fixedSize: true,
			bck:       bckFrom,
		}

		tests = []struct {
			name      string
			ty        string
			initDesc  string
			buildDesc etl.InitCodeMsg
		}{
			{name: "spec-echo-python", ty: apc.ETLInitSpec, initDesc: tetl.Echo},
			{name: "spec-echo-golang", ty: apc.ETLInitSpec, initDesc: tetl.EchoGolang},

			{
				name: "code-echo-py38",
				ty:   apc.ETLInitCode,
				buildDesc: etl.InitCodeMsg{
					Code:      []byte(echoPythonTransform),
					Runtime:   runtime.Py38,
					ChunkSize: 0,
				},
			},
			{
				name: "code-echo-py310",
				ty:   apc.ETLInitCode,
				buildDesc: etl.InitCodeMsg{
					Code:      []byte(echoPythonTransform),
					Runtime:   runtime.Py310,
					ChunkSize: 0,
				},
			},
		}
	)

	tlog.Logf("Preparing source bucket (%d objects, %s each)\n", m.num, cos.B2S(int64(m.fileSize), 2))
	tools.CreateBucketWithCleanup(t, proxyURL, bckFrom, nil)
	m.initWithCleanupAndSaveState()

	m.puts()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tetl.CheckNoRunningETLContainers(t, baseParams)
			var (
				uuid string
				err  error

				etlDoneCh      = cos.NewStopCh()
				requestTimeout = 30 * time.Second
			)
			switch test.ty {
			case apc.ETLInitSpec:
				uuid = tetl.InitSpec(t, baseParams, test.initDesc, etl.Hpull)
			case apc.ETLInitCode:
				test.buildDesc.IDX = test.name
				test.buildDesc.Timeout = cos.Duration(10 * time.Minute)
				test.buildDesc.Funcs.Transform = "transform"
				uuid = tetl.InitCode(t, baseParams, test.buildDesc)
			default:
				panic(test.ty)
			}
			t.Cleanup(func() {
				tetl.StopAndDeleteETL(t, baseParams, uuid)
				tetl.WaitForContainersStopped(t, baseParams)
			})

			tlog.Logf("Start offline ETL %q\n", uuid)
			xactID := tetl.ETLBucket(t, baseParams, bckFrom, bckTo, &apc.TCBMsg{
				ID:             uuid,
				RequestTimeout: cos.Duration(requestTimeout),
				CopyBckMsg:     apc.CopyBckMsg{Force: true},
			})
			tetl.ReportXactionStatus(baseParams, xactID, etlDoneCh, 2*time.Minute, m.num)

			tlog.Logln("Waiting for ETL to finish")
			err = tetl.WaitForFinished(baseParams, xactID, 15*time.Minute)
			etlDoneCh.Close()
			tassert.CheckFatal(t, err)

			snaps, err := api.QueryXactionSnaps(baseParams, api.XactReqArgs{ID: xactID})
			tassert.CheckFatal(t, err)
			total, err := snaps.TotalRunningTime(xactID)
			tassert.CheckFatal(t, err)
			tlog.Logf("Transforming bucket %s took %s\n", bckFrom.DisplayName(), total)

			objList, err := api.ListObjects(baseParams, bckTo, nil, 0)
			tassert.CheckFatal(t, err)
			tassert.Fatalf(
				t, len(objList.Entries) == m.num,
				"expected %d objects to be transformed, got %d", m.num, len(objList.Entries),
			)
		})
	}
}

// Responsible for cleaning all resources, except ETL xact.
func etlPrepareAndStart(t *testing.T, m *ioContext, name, comm string) (xactID string) {
	var (
		bckFrom = cmn.Bck{Name: "etl-in-" + trand.String(5), Provider: apc.AIS}
		bckTo   = cmn.Bck{Name: "etl-out-" + trand.String(5), Provider: apc.AIS}
	)

	m.bck = bckFrom

	tlog.Logf("Preparing source bucket %q\n", bckFrom.String())
	tools.CreateBucketWithCleanup(t, proxyURL, bckFrom, nil)
	m.initWithCleanupAndSaveState()

	m.puts()

	etlID := tetl.InitSpec(t, baseParams, name, comm)
	tlog.Logf("ETL init successful (%q)\n", etlID)
	t.Cleanup(func() {
		tetl.StopAndDeleteETL(t, baseParams, etlID)
	})

	tlog.Logf("Start offline ETL %q => %q\n", etlID, bckTo.String())
	return tetl.ETLBucket(t, baseParams, bckFrom, bckTo, &apc.TCBMsg{ID: etlID, CopyBckMsg: apc.CopyBckMsg{Force: true}})
}
