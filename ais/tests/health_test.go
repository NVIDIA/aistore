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
	"github.com/NVIDIA/aistore/tutils"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

// health should respond with 200 even is node is unregistered
func unregisteredNodeHealth(t *testing.T, proxyURL string, si *cluster.Snode) {
	err := api.Health(tutils.BaseAPIParams(si.PublicNet.DirectURL))
	tassert.CheckError(t, err)

	smapOrig := tutils.GetClusterMap(t, proxyURL)
	args := &cmn.ActValDecommision{DaemonID: si.DaemonID, Force: true}
	err = tutils.UnregisterNode(proxyURL, args)
	tassert.CheckFatal(t, err)
	targetCount := smapOrig.CountTargets()
	proxyCount := smapOrig.CountProxies()
	if si.IsProxy() {
		proxyCount--
	} else {
		targetCount--
	}
	_, err = tutils.WaitForPrimaryProxy(proxyURL, "to proxy decommission",
		smapOrig.Version, testing.Verbose(), proxyCount, targetCount)
	tassert.CheckFatal(t, err)
	defer func() {
		err = tutils.JoinCluster(proxyURL, si)
		tassert.CheckFatal(t, err)
		_, err = tutils.WaitForPrimaryProxy(proxyURL, "to proxy join",
			smapOrig.Version, testing.Verbose(), smapOrig.CountProxies(), smapOrig.CountTargets())
		tassert.CheckFatal(t, err)
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
	var (
		proxyURL = tutils.GetPrimaryURL()
		smap     = tutils.GetClusterMap(t, proxyURL)
	)

	proxyCnt := smap.CountProxies()
	proxy, err := smap.GetRandProxy(true /*excludePrimary*/)
	tassert.CheckError(t, err)
	unregisteredNodeHealth(t, proxyURL, proxy)

	smap = tutils.GetClusterMap(t, proxyURL)
	tassert.Fatalf(t, proxyCnt == smap.CountProxies(), "expected number of proxies to be the same after the test")
}

func TestTargetHealth(t *testing.T) {
	var (
		proxyURL = tutils.GetPrimaryURL()
		smap     = tutils.GetClusterMap(t, proxyURL)
	)
	tsi, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, tsi != nil, "no targets")
	err = api.Health(tutils.BaseAPIParams(tsi.PublicNet.DirectURL))
	tassert.CheckFatal(t, err)
}

func TestUnregisteredTargetHealth(t *testing.T) {
	var (
		proxyURL = tutils.GetPrimaryURL()
		smap     = tutils.GetClusterMap(t, proxyURL)
	)

	targetsCnt := smap.CountTargets()
	proxy, err := smap.GetRandTarget()
	tassert.CheckFatal(t, err)
	unregisteredNodeHealth(t, proxyURL, proxy)

	smap = tutils.GetClusterMap(t, proxyURL)
	tassert.Fatalf(t, targetsCnt == smap.CountTargets(), "expected number of targets to be the same after the test")
	tutils.WaitForRebalanceToComplete(t, tutils.BaseAPIParams(proxyURL))
}
