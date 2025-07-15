// Package integration_test.
/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
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
	"github.com/NVIDIA/aistore/tools/trand"
	"github.com/NVIDIA/aistore/xact"
)

func TestETLBucketAbort(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s, Long: true})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	m := &ioContext{
		t:         t,
		num:       10000,
		fileSize:  512,
		fixedSize: true,
	}

	xid := etlPrepareAndStart(t, m, tetl.Echo, etl.Hpull)

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
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s, MinTargets: 2})
	tetl.CheckNoRunningETLContainers(t, baseParams)

	m := &ioContext{
		t:         t,
		num:       100_000,
		fileSize:  512,
		fixedSize: true,
	}
	if testing.Short() {
		m.num /= 10
	}
	m.initAndSaveState(true /*cleanup*/)
	xid := etlPrepareAndStart(t, m, tetl.Echo, etl.Hpull)

	tlog.Logln("Waiting for ETL to process a few objects...")
	time.Sleep(1 * time.Second)

	_, removedTarget := tools.RmTargetSkipRebWait(t, proxyURL, m.smap)

	t.Cleanup(func() {
		time.Sleep(4 * time.Second)
		var rebID string
		rebID, _ = tools.RestoreTarget(t, proxyURL, removedTarget)
		tools.WaitForRebalanceByID(t, baseParams, rebID)
		m.waitAndCheckCluState()

		tetl.CheckNoRunningETLContainers(t, baseParams)
	})

	err := tetl.WaitForAborted(baseParams, xid, apc.ActETLBck, 5*time.Minute)
	tassert.CheckFatal(t, err)
	tetl.WaitForETLAborted(t, baseParams)
}

func TestETLBigBucket(t *testing.T) {
	// The test takes a lot of time if it's run against a single target deployment.
	tools.CheckSkip(t, &tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeK8s, Long: true, MinTargets: 2})

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
			etlSpecName string
		}{
			{name: "spec-echo-python", etlSpecName: tetl.Echo},
			{name: "spec-echo-golang", etlSpecName: tetl.EchoGolang},
		}
	)

	tlog.Logf("Preparing source bucket (%d objects, %s each)\n", m.num, cos.ToSizeIEC(int64(m.fileSize), 2))
	tools.CreateBucket(t, proxyURL, bckFrom, nil, true /*cleanup*/)
	m.initAndSaveState(true /*cleanup*/)

	m.puts()

	for i := range tests {
		test := tests[i]
		t.Run(test.name, func(t *testing.T) {
			tetl.CheckNoRunningETLContainers(t, baseParams)
			var (
				err            error
				etlName        string
				etlDoneCh      = cos.NewStopCh()
				requestTimeout = 30 * time.Second
			)
			_ = tetl.InitSpec(t, baseParams, etlName, etl.Hpush, etl.ArgTypeDefault)
			t.Cleanup(func() {
				tetl.StopAndDeleteETL(t, baseParams, etlName)
				tetl.WaitForETLAborted(t, baseParams)
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

			err = tetl.ListObjectsWithRetry(baseParams, bckTo, m.num, tools.WaitRetryOpts{MaxRetries: 5, Interval: time.Second * 3})
			tassert.CheckFatal(t, err)
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

	_ = tetl.InitSpec(t, baseParams, etlName, comm, etl.ArgTypeDefault)
	t.Cleanup(func() {
		tetl.StopAndDeleteETL(t, baseParams, etlName)
	})

	tlog.Logf("Start offline ETL[%s] => %s\n", etlName, bckTo.Cname(""))
	msg := &apc.TCBMsg{Transform: apc.Transform{Name: etlName}, CopyBckMsg: apc.CopyBckMsg{Force: true}}
	xid = tetl.ETLBucketWithCleanup(t, baseParams, bckFrom, bckTo, msg)
	return
}
