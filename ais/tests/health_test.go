// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools/tassert"
	"github.com/NVIDIA/aistore/devtools/tutils"
)

// health should respond with 200 even is node is unregistered
func unregisteredNodeHealth(t *testing.T, proxyURL string, si *cluster.Snode) {
	err := api.Health(tutils.BaseAPIParams(si.PublicNet.DirectURL))
	tassert.CheckError(t, err)

	smapOrig := tutils.GetClusterMap(t, proxyURL)
	args := &cmn.ActValRmNode{DaemonID: si.DaemonID, SkipRebalance: true}
	baseParams := tutils.BaseAPIParams(proxyURL)
	_, err = api.StartMaintenance(baseParams, args)
	tassert.CheckFatal(t, err)
	targetCount := smapOrig.CountActiveTargets()
	origTargetCnt := targetCount
	proxyCount := smapOrig.CountActiveProxies()
	if si.IsProxy() {
		proxyCount--
	} else {
		targetCount--
	}
	_, err = tutils.WaitForClusterState(proxyURL, "decommission node", smapOrig.Version, proxyCount, targetCount)
	tassert.CheckFatal(t, err)
	defer func() {
		val := &cmn.ActValRmNode{DaemonID: si.ID()}
		rebID, err := api.StopMaintenance(baseParams, val)
		tassert.CheckFatal(t, err)
		_, err = tutils.WaitForClusterState(proxyURL, "join node", smapOrig.Version, smapOrig.CountActiveProxies(),
			smapOrig.CountActiveTargets())
		tassert.CheckFatal(t, err)
		if rebID != "" {
			tutils.WaitForRebalanceByID(t, origTargetCnt, baseParams, rebID)
		}
	}()

	err = api.Health(tutils.BaseAPIParams(si.PublicNet.DirectURL))
	tassert.CheckError(t, err)
}

func TestPrimaryProxyHealth(t *testing.T) {
	smap := tutils.GetClusterMap(t, proxyURL)
	err := api.Health(tutils.BaseAPIParams(smap.Primary.PublicNet.DirectURL))
	tassert.CheckError(t, err)
}

func TestUnregisteredProxyHealth(t *testing.T) {
	tutils.CheckSkip(t, tutils.SkipTestArgs{RequiredDeployment: tutils.ClusterTypeLocal, MinProxies: 2})

	var (
		proxyURL = tutils.GetPrimaryURL()
		smap     = tutils.GetClusterMap(t, proxyURL)
	)

	proxyCnt := smap.CountActiveProxies()
	proxy, err := smap.GetRandProxy(true /*excludePrimary*/)
	tassert.CheckError(t, err)
	unregisteredNodeHealth(t, proxyURL, proxy)

	smap = tutils.GetClusterMap(t, proxyURL)
	tassert.Fatalf(t, proxyCnt == smap.CountActiveProxies(), "expected number of proxies to be the same after the test")
}

func TestTargetHealth(t *testing.T) {
	var (
		proxyURL = tutils.GetPrimaryURL()
		smap     = tutils.GetClusterMap(t, proxyURL)
	)
	tsi, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	err = api.Health(tutils.BaseAPIParams(tsi.PublicNet.DirectURL))
	tassert.CheckFatal(t, err)
}

func TestUnregisteredTargetHealth(t *testing.T) {
	var (
		proxyURL = tutils.GetPrimaryURL()
		smap     = tutils.GetClusterMap(t, proxyURL)
	)

	targetsCnt := smap.CountActiveTargets()
	target, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	unregisteredNodeHealth(t, proxyURL, target)

	smap = tutils.GetClusterMap(t, proxyURL)
	tassert.Fatalf(t, targetsCnt == smap.CountActiveTargets(), "expected number of targets to be the same after the test")
}
