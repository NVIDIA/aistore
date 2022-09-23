// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"testing"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
)

// health should respond with 200 even is node is unregistered
func unregisteredNodeHealth(t *testing.T, proxyURL string, si *cluster.Snode) {
	err := api.Health(tools.BaseAPIParams(si.PubNet.URL))
	tassert.CheckError(t, err)

	smapOrig := tools.GetClusterMap(t, proxyURL)
	args := &apc.ActValRmNode{DaemonID: si.ID(), SkipRebalance: true}
	baseParams := tools.BaseAPIParams(proxyURL)
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
	_, err = tools.WaitForClusterState(proxyURL, "decommission node", smapOrig.Version, proxyCount, targetCount)
	tassert.CheckFatal(t, err)
	defer func() {
		val := &apc.ActValRmNode{DaemonID: si.ID()}
		rebID, err := api.StopMaintenance(baseParams, val)
		tassert.CheckFatal(t, err)
		_, err = tools.WaitForClusterState(proxyURL, "join node", smapOrig.Version, smapOrig.CountActiveProxies(),
			smapOrig.CountActiveTargets())
		tassert.CheckFatal(t, err)
		if rebID != "" {
			tools.WaitForRebalanceByID(t, origTargetCnt, baseParams, rebID)
		}
	}()

	err = api.Health(tools.BaseAPIParams(si.PubNet.URL))
	tassert.CheckError(t, err)
}

func TestPrimaryProxyHealth(t *testing.T) {
	smap := tools.GetClusterMap(t, proxyURL)
	err := api.Health(tools.BaseAPIParams(smap.Primary.PubNet.URL))
	tassert.CheckError(t, err)
}

func TestUnregisteredProxyHealth(t *testing.T) {
	tools.CheckSkip(t, tools.SkipTestArgs{RequiredDeployment: tools.ClusterTypeLocal, MinProxies: 2})

	var (
		proxyURL = tools.GetPrimaryURL()
		smap     = tools.GetClusterMap(t, proxyURL)
	)

	proxyCnt := smap.CountActiveProxies()
	proxy, err := smap.GetRandProxy(true /*excludePrimary*/)
	tassert.CheckError(t, err)
	unregisteredNodeHealth(t, proxyURL, proxy)

	smap = tools.GetClusterMap(t, proxyURL)
	tassert.Fatalf(t, proxyCnt == smap.CountActiveProxies(), "expected number of proxies to be the same after the test")
}

func TestTargetHealth(t *testing.T) {
	var (
		proxyURL = tools.GetPrimaryURL()
		smap     = tools.GetClusterMap(t, proxyURL)
	)
	tsi, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	err = api.Health(tools.BaseAPIParams(tsi.PubNet.URL))
	tassert.CheckFatal(t, err)
}

func TestUnregisteredTargetHealth(t *testing.T) {
	var (
		proxyURL = tools.GetPrimaryURL()
		smap     = tools.GetClusterMap(t, proxyURL)
	)

	targetsCnt := smap.CountActiveTargets()
	target, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	unregisteredNodeHealth(t, proxyURL, target)

	smap = tools.GetClusterMap(t, proxyURL)
	tassert.Fatalf(t, targetsCnt == smap.CountActiveTargets(), "expected number of targets to be the same after the test")
}
