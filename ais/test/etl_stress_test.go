// Package integration_test.
/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/ext/etl/runtime"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tetl"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"
)

const etlBucketTimeout = cos.Duration(3 * time.Minute)

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
	tools.CreateBucket(t, proxyURL, m.bck, nil, true /*cleanup*/)

	m.init(true /*cleanup*/)
	m.puts()

	msg := etl.InitCodeMsg{
		InitMsgBase: etl.InitMsgBase{IDX: "etl-build-conn-err", Timeout: etlBucketTimeout},
		Code:        []byte(timeoutFunc),
		Runtime:     runtime.Py38,
		ChunkSize:   0,
	}
	msg.Funcs.Transform = "transform"

	_ = tetl.InitCode(t, baseParams, msg)

	bckTo := cmn.Bck{Name: "etldst_" + cos.GenTie(), Provider: apc.AIS}
	testETLBucket(t, baseParams, msg.Name(), &m, bckTo, time.Duration(etlBucketTimeout),
		true /* skip byte-count check*/, false /* remote src evicted */)
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

	xid := etlPrepareAndStart(t, m, tetl.Echo, etl.Hpull)

	time.Sleep(time.Duration(rand.Intn(5)) * time.Second)

	tlog.Logf("Aborting etl[%s]\n", xid)
	args := xact.ArgsMsg{ID: xid, Kind: apc.ActETLBck}
	err := api.AbortXaction(baseParams, &args)
	tassert.CheckFatal(t, err)

	err = tetl.WaitForAborted(baseParams, xid, apc.ActETLBck, 2*time.Minute)
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
	m.initAndSaveState(true /*cleanup*/)
	xid := etlPrepareAndStart(t, m, tetl.Echo, etl.Hpull)

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

		args := xact.ArgsMsg{Kind: apc.ActRebalance, Timeout: tools.RebalanceTimeout}
		_, _ = api.WaitForXactionIC(baseParams, &args)

		tetl.CheckNoRunningETLContainers(t, baseParams)
	})

	err = tetl.WaitForAborted(baseParams, xid, apc.ActETLBck, 5*time.Minute)
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
			fileSize:  20 * cos.KiB, // 4GiB total
			fixedSize: true,
			bck:       bckFrom,
		}

		tests = []struct {
			name        string
			ty          string
			etlSpecName string
			etlCodeMsg  etl.InitCodeMsg
		}{
			{name: "spec-echo-python", ty: etl.Spec, etlSpecName: tetl.Echo},
			{name: "spec-echo-golang", ty: etl.Spec, etlSpecName: tetl.EchoGolang},

			{
				name: "code-echo-py38",
				ty:   etl.Code,
				etlCodeMsg: etl.InitCodeMsg{
					Code:      []byte(echoPythonTransform),
					Runtime:   runtime.Py38,
					ChunkSize: 0,
				},
			},
			{
				name: "code-echo-py310",
				ty:   etl.Code,
				etlCodeMsg: etl.InitCodeMsg{
					Code:      []byte(echoPythonTransform),
					Runtime:   runtime.Py310,
					ChunkSize: 0,
				},
			},
		}
	)

	tlog.Logf("Preparing source bucket (%d objects, %s each)\n", m.num, cos.ToSizeIEC(int64(m.fileSize), 2))
	tools.CreateBucket(t, proxyURL, bckFrom, nil, true /*cleanup*/)
	m.initAndSaveState(true /*cleanup*/)

	m.puts()

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tetl.CheckNoRunningETLContainers(t, baseParams)
			var (
				err            error
				etlName        string
				etlDoneCh      = cos.NewStopCh()
				requestTimeout = 30 * time.Second
			)
			switch test.ty {
			case etl.Spec:
				etlName = test.etlSpecName
				_ = tetl.InitSpec(t, baseParams, etlName, etl.Hpull)
			case etl.Code:
				etlName = test.name
				{
					test.etlCodeMsg.IDX = etlName
					test.etlCodeMsg.Timeout = etlBucketTimeout
					test.etlCodeMsg.Funcs.Transform = "transform"
				}
				_ = tetl.InitCode(t, baseParams, test.etlCodeMsg)
			default:
				debug.Assert(false, test.ty)
			}
			t.Cleanup(func() {
				tetl.StopAndDeleteETL(t, baseParams, etlName)
				tetl.WaitForContainersStopped(t, baseParams)
			})

			tlog.Logf("Start offline ETL[%s]\n", etlName)
			msg := &apc.TCBMsg{
				Transform: apc.Transform{
					Name:    etlName,
					Timeout: cos.Duration(requestTimeout),
				},
				CopyBckMsg: apc.CopyBckMsg{Force: true},
			}
			xid := tetl.ETLBucketWithCleanup(t, baseParams, bckFrom, bckTo, msg)
			tetl.ReportXactionStatus(baseParams, xid, etlDoneCh, 2*time.Minute, m.num)

			tlog.Logln("Waiting for ETL to finish")
			err = tetl.WaitForFinished(baseParams, xid, apc.ActETLBck, 15*time.Minute)
			etlDoneCh.Close()
			tassert.CheckFatal(t, err)

			snaps, err := api.QueryXactionSnaps(baseParams, &xact.ArgsMsg{ID: xid})
			tassert.CheckFatal(t, err)
			total, err := snaps.TotalRunningTime(xid)
			tassert.CheckFatal(t, err)
			tlog.Logf("Transforming bucket %s took %v\n", bckFrom.Cname(""), total)

			objList, err := api.ListObjects(baseParams, bckTo, nil, api.ListArgs{})
			tassert.CheckFatal(t, err)
			tassert.Fatalf(
				t, len(objList.Entries) == m.num,
				"expected %d objects to be transformed, got %d", m.num, len(objList.Entries),
			)
		})
	}
}

// Responsible for cleaning all resources, except ETL xact.
func etlPrepareAndStart(t *testing.T, m *ioContext, etlName, comm string) (xid string) {
	var (
		bckFrom = cmn.Bck{Name: "etl-in-" + trand.String(5), Provider: apc.AIS}
		bckTo   = cmn.Bck{Name: "etl-out-" + trand.String(5), Provider: apc.AIS}
	)
	m.bck = bckFrom

	tlog.Logf("Preparing source bucket %s\n", bckFrom.Cname(""))
	tools.CreateBucket(t, proxyURL, bckFrom, nil, true /*cleanup*/)
	m.initAndSaveState(true /*cleanup*/)

	m.puts()

	_ = tetl.InitSpec(t, baseParams, etlName, comm)
	t.Cleanup(func() {
		tetl.StopAndDeleteETL(t, baseParams, etlName)
	})

	tlog.Logf("Start offline ETL[%s] => %s\n", etlName, bckTo.Cname(""))
	msg := &apc.TCBMsg{Transform: apc.Transform{Name: etlName}, CopyBckMsg: apc.CopyBckMsg{Force: true}}
	xid = tetl.ETLBucketWithCleanup(t, baseParams, bckFrom, bckTo, msg)
	return
}
