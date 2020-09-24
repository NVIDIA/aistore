// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func TestMaintenanceOnOff(t *testing.T) {
	proxyURL := tutils.RandomProxyURL(t)
	smap := tutils.GetClusterMap(t, proxyURL)

	// Invalid target case
	msg := &cmn.ActValDecommision{DaemonID: "fakeID"}
	_, err := api.Maintenance(baseParams, cmn.ActSuspend, msg)
	tassert.Fatalf(t, err != nil, "Maintenance for invalid daemon ID succeeded")

	mntTarget := tutils.ExtractTargetNodes(smap)[0]
	baseParams := tutils.BaseAPIParams(proxyURL)
	msg.DaemonID = mntTarget.ID()
	_, err = api.Maintenance(baseParams, cmn.ActSuspend, msg)
	tassert.CheckError(t, err)
	_, err = api.Maintenance(baseParams, cmn.ActUnsuspend, msg)
	tassert.CheckError(t, err)
	_, err = api.Maintenance(baseParams, cmn.ActUnsuspend, msg)
	tassert.Fatalf(t, err != nil, "Canceling maintenance must fail for 'normal' daemon")
}

func TestMaintenanceRebalance(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	var (
		bck = cmn.Bck{Name: "maint-reb", Provider: cmn.ProviderAIS}
		m   = &ioContext{
			t:               t,
			num:             30,
			fileSize:        512,
			fixedSize:       true,
			bck:             bck,
			numGetsEachFile: 1,
		}
		actVal     = &cmn.ActValDecommision{Rebalance: true}
		proxyURL   = tutils.RandomProxyURL(t)
		smap       = tutils.GetClusterMap(t, proxyURL)
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	tutils.Logf("targets: %d, proxies: %d\n", smap.CountTargets(), smap.CountProxies())

	tutils.Logln("Preparing a bucket")
	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)
	m.init()

	tutils.Logln("Putting objects to the bucket")
	m.puts()

	tsi := tutils.ExtractTargetNodes(smap)[0]
	tutils.Logf("Removing target %s\n", tsi)
	restored := false
	actVal.DaemonID = tsi.ID()
	rebID, err := api.Maintenance(baseParams, cmn.ActDecommission, actVal)
	tassert.CheckError(t, err)
	defer func() {
		if !restored {
			tutils.RestoreTarget(t, proxyURL, smap, tsi)
		}
	}()
	tutils.Logf("Wait for rebalance %s\n", rebID)
	args := api.XactReqArgs{ID: rebID, Kind: cmn.ActRebalance, Timeout: time.Minute}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)
	smap, err = tutils.WaitForPrimaryProxy(
		proxyURL,
		"to target removed from the cluster",
		smap.Version, testing.Verbose(),
		smap.CountProxies(),
		smap.CountTargets()-1,
	)
	tassert.CheckFatal(t, err)

	m.gets()
	m.ensureNoErrors()

	tutils.Logf("Restoring target %s\n", tsi)
	tutils.RestoreTarget(t, proxyURL, smap, tsi)
	restored = true
	args = api.XactReqArgs{Kind: cmn.ActRebalance, Timeout: time.Minute}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckError(t, err)
}
