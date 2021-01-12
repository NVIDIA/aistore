// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools/tutils"
	"github.com/NVIDIA/aistore/devtools/tutils/tassert"
	"github.com/NVIDIA/aistore/devtools/tutils/tetl"
	"github.com/NVIDIA/aistore/etl"
)

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
	tutils.CreateFreshBucket(t, proxyURL, bckFrom)
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
