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
	msg := &cmn.ActValDecommision{DaemonID: "fakeID", SkipRebalance: true}
	_, err := api.StartMaintenance(baseParams, msg)
	tassert.Fatalf(t, err != nil, "Maintenance for invalid daemon ID succeeded")

	mntTarget := tutils.ExtractTargetNodes(smap)[0]
	baseParams := tutils.BaseAPIParams(proxyURL)
	msg.DaemonID = mntTarget.ID()
	_, err = api.StartMaintenance(baseParams, msg)
	tassert.CheckError(t, err)
	_, err = api.StopMaintenance(baseParams, msg)
	tassert.CheckError(t, err)
	_, err = api.StopMaintenance(baseParams, msg)
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
			proxyURL:        proxyURL,
		}
		actVal     = &cmn.ActValDecommision{}
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	m.saveClusterState()
	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	m.puts()
	tsi := tutils.ExtractTargetNodes(m.smap)[0]
	tutils.Logf("Removing target %s\n", tsi)
	restored := false
	actVal.DaemonID = tsi.ID()
	rebID, err := api.Decommission(baseParams, actVal)
	tassert.CheckError(t, err)
	defer func() {
		if !restored {
			tutils.RestoreTarget(t, proxyURL, m.smap, tsi)
		}
		tutils.ClearMaintenance(baseParams, tsi)
	}()
	tutils.Logf("Wait for rebalance %s\n", rebID)
	args := api.XactReqArgs{ID: rebID, Kind: cmn.ActRebalance, Timeout: time.Minute}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	smap2, err2 := tutils.WaitForPrimaryProxy(
		proxyURL,
		"to target removed from the cluster",
		m.smap.Version, testing.Verbose(),
		m.smap.CountProxies(),
		m.smap.CountTargets()-1,
	)
	tassert.CheckFatal(t, err2)
	m.smap = smap2

	m.gets()
	m.ensureNoErrors()

	tutils.Logf("Restoring target %s\n", tsi)
	tutils.RestoreTarget(t, proxyURL, m.smap, tsi)
	restored = true
	args = api.XactReqArgs{Kind: cmn.ActRebalance, Timeout: time.Minute}
	err = api.WaitForXactionToStart(baseParams, args)
	tassert.CheckFatal(t, err)
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckError(t, err)
}

func TestMaintenanceGetWhileRebalance(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{Long: true})
	var (
		bck = cmn.Bck{Name: "maint-get-reb", Provider: cmn.ProviderAIS}
		m   = &ioContext{
			t:               t,
			num:             5000,
			fileSize:        1024,
			fixedSize:       true,
			bck:             bck,
			numGetsEachFile: 1,
			proxyURL:        proxyURL,
		}
		actVal     = &cmn.ActValDecommision{}
		proxyURL   = tutils.RandomProxyURL(t)
		baseParams = tutils.BaseAPIParams(proxyURL)
	)

	m.saveClusterState()
	tutils.CreateFreshBucket(t, proxyURL, bck)
	defer tutils.DestroyBucket(t, proxyURL, bck)

	m.puts()
	go m.getsUntilStop()
	stopped := false

	tsi := tutils.ExtractTargetNodes(m.smap)[0]
	tutils.Logf("Removing target %s\n", tsi)
	restored := false
	actVal.DaemonID = tsi.ID()
	rebID, err := api.Decommission(baseParams, actVal)
	tassert.CheckFatal(t, err)
	defer func() {
		if !stopped {
			m.stopGets()
		}
		if !restored {
			tutils.RestoreTarget(t, proxyURL, m.smap, tsi)
		}
		tutils.ClearMaintenance(baseParams, tsi)
	}()
	tutils.Logf("Wait for rebalance %s\n", rebID)
	args := api.XactReqArgs{ID: rebID, Kind: cmn.ActRebalance, Timeout: time.Minute}
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckFatal(t, err)

	smap2, err2 := tutils.WaitForPrimaryProxy(
		proxyURL,
		"target removed from the cluster",
		m.smap.Version, testing.Verbose(),
		m.smap.CountProxies(),
		m.smap.CountTargets()-1,
	)
	tassert.CheckFatal(t, err2)
	m.smap = smap2

	m.stopGets()
	stopped = true
	m.ensureNoErrors()

	tutils.Logf("Restoring target %s\n", tsi)
	tutils.RestoreTarget(t, proxyURL, m.smap, tsi)
	restored = true
	args = api.XactReqArgs{Kind: cmn.ActRebalance, Timeout: time.Minute}
	err = api.WaitForXactionToStart(baseParams, args)
	tassert.CheckFatal(t, err)
	_, err = api.WaitForXaction(baseParams, args)
	tassert.CheckError(t, err)
}
