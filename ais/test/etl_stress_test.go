// Package integration_test.
/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
 */
package integration_test

import (
	"math/rand/v2"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
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

	_, xid := etlPrepareAndStart(t, m, tetl.Echo, etl.Hpull)

	tlog.Logfln("Aborting etl[%s]", xid)
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
		fileSize:  4 * cos.KiB,
		fixedSize: true,
	}
	m.initAndSaveState(true /*cleanup*/)

	t.Run("target_graceful_shutdown", func(t *testing.T) {
		name, xid := etlPrepareAndStart(t, m, tetl.Echo, etl.Hpull)

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

		tetl.WaitForETLAborted(t, baseParams, name)
		err := tetl.WaitForAborted(baseParams, xid, apc.ActETLBck, 5*time.Minute)
		tassert.CheckFatal(t, err)
	})

	t.Run("target_force_shutdown", func(t *testing.T) {
		name, xid := etlPrepareAndStart(t, m, tetl.Echo, etl.Hpull)

		tlog.Logln("Waiting for ETL to process a few objects...")
		time.Sleep(1 * time.Second)

		target, err := m.smap.GetRandTarget()
		tassert.CheckFatal(t, err)
		_, err = tools.KillNode(baseParams, target)
		tassert.CheckFatal(t, err)

		tetl.WaitForETLAborted(t, baseParams, name)
		err = tetl.WaitForAborted(baseParams, xid, apc.ActETLBck, 5*time.Minute)
		tassert.CheckFatal(t, err)
	})

	t.Run("target_force_shutdown_sequence_same_target", func(t *testing.T) {
		tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
		// 1. Start ETL, kill a target, check ETL aborted.
		name1, xid1 := etlPrepareAndStart(t, m, tetl.Echo, etl.Hpull)
		tlog.Logln("Waiting for ETL to process a few objects...")
		time.Sleep(1 * time.Second)

		target, err := m.smap.GetRandTarget()
		tassert.CheckFatal(t, err)
		_, err = tools.KillNode(baseParams, target)
		tassert.CheckFatal(t, err)

		tetl.WaitForETLAborted(t, baseParams, name1)
		err = tetl.WaitForAborted(baseParams, xid1, apc.ActETLBck, 5*time.Minute)
		tassert.CheckFatal(t, err)

		// 2. Start ETL again, kill the same target, check ETL aborted.
		name2, xid2 := etlPrepareAndStart(t, m, tetl.MD5, etl.Hpush)
		tlog.Logln("Waiting for ETL to process a few objects (second run)...")
		time.Sleep(1 * time.Second)

		_, err = tools.KillNode(baseParams, target)
		tassert.CheckFatal(t, err)

		tetl.WaitForETLAborted(t, baseParams, name1, name2)
		err = tetl.WaitForAborted(baseParams, xid2, apc.ActETLBck, 5*time.Minute)
		tassert.CheckFatal(t, err)
	})

	t.Run("target_force_shutdown_different_targets", func(t *testing.T) {
		tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
		// 1. Pick two random targets from the Tmap
		var t1, t2 *meta.Snode
		targets := make([]*meta.Snode, 0, len(m.smap.Tmap))
		for _, tgt := range m.smap.Tmap {
			targets = append(targets, tgt)
		}
		switch len(targets) {
		case 0:
			t1, t2 = nil, nil
		case 1:
			t1 = targets[0]
			t2 = targets[0]
		default:
			rand.Shuffle(len(targets), func(i, j int) { targets[i], targets[j] = targets[j], targets[i] })
			t1, t2 = targets[0], targets[1]
		}

		// 2. Start ETL, kill the first target, check ETL aborted.
		name1, xid1 := etlPrepareAndStart(t, m, tetl.Echo, etl.Hpull)
		tlog.Logln("Waiting for ETL to process a few objects...")
		time.Sleep(1 * time.Second)

		_, err := tools.KillNode(baseParams, t1)
		tassert.CheckFatal(t, err)

		tetl.WaitForETLAborted(t, baseParams, name1)
		err = tetl.WaitForAborted(baseParams, xid1, apc.ActETLBck, 5*time.Minute)
		tassert.CheckFatal(t, err)

		// 3. Start ETL again, kill the second target, check ETL aborted.
		name2, xid2 := etlPrepareAndStart(t, m, tetl.Echo, etl.Hpull)
		tlog.Logln("Waiting for ETL to process a few objects (second run)...")
		time.Sleep(1 * time.Second)

		_, err = tools.KillNode(baseParams, t2)
		tassert.CheckFatal(t, err)

		tetl.WaitForETLAborted(t, baseParams, name1, name2)
		err = tetl.WaitForAborted(baseParams, xid2, apc.ActETLBck, 5*time.Minute)
		tassert.CheckFatal(t, err)

		tetl.CheckNoRunningETLContainers(t, baseParams)
	})
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

	tlog.Logfln("Preparing source bucket (%d objects, %s each)", m.num, cos.ToSizeIEC(int64(m.fileSize), 2))
	tools.CreateBucket(t, proxyURL, bckFrom, nil, true /*cleanup*/)
	m.initAndSaveState(true /*cleanup*/)

	m.puts()

	for i := range tests {
		test := tests[i]
		t.Run(test.name, func(t *testing.T) {
			tetl.CheckNoRunningETLContainers(t, baseParams)
			var (
				err            error
				etlName        = test.etlSpecName
				etlDoneCh      = cos.NewStopCh()
				requestTimeout = 30 * time.Second
			)
			initMsg := tetl.InitSpec(t, baseParams, etlName, etl.Hpush, etl.ArgTypeDefault)
			t.Cleanup(func() {
				tetl.StopAndDeleteETL(t, baseParams, initMsg.Name())
				tetl.WaitForETLAborted(t, baseParams)
			})

			tlog.Logfln("Start offline ETL[%s]", etlName)
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
			tlog.Logfln("Transforming bucket %s took %v", bckFrom.Cname(""), total)

			err = tetl.ListObjectsWithRetry(baseParams, bckTo, "" /*prefix*/, m.num, tools.WaitRetryOpts{MaxRetries: 5, Interval: time.Second * 3})
			tassert.CheckFatal(t, err)
		})
	}
}

// Responsible for cleaning all resources, except ETL xact.
func etlPrepareAndStart(t *testing.T, m *ioContext, etlName, comm string) (name, xid string) {
	var (
		bckFrom = cmn.Bck{Name: "etl-in-" + trand.String(5), Provider: apc.AIS}
		bckTo   = cmn.Bck{Name: "etl-out-" + trand.String(5), Provider: apc.AIS}
	)
	m.bck = bckFrom

	tlog.Logfln("Preparing source bucket %s", bckFrom.Cname(""))
	tools.CreateBucket(t, proxyURL, bckFrom, nil, true /*cleanup*/)
	m.initAndSaveState(true /*cleanup*/)

	m.puts()

	initMsg := tetl.InitSpec(t, baseParams, etlName, comm, etl.ArgTypeDefault)
	t.Cleanup(func() {
		tetl.StopAndDeleteETL(t, baseParams, initMsg.Name())
	})

	tlog.Logfln("Start offline %s => %s", initMsg.Cname(), bckTo.Cname(""))
	msg := &apc.TCBMsg{Transform: apc.Transform{Name: initMsg.Name()}, CopyBckMsg: apc.CopyBckMsg{Force: true}}
	xid = tetl.ETLBucketWithCleanup(t, baseParams, bckFrom, bckTo, msg)
	return initMsg.Name(), xid
}
