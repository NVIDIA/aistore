// Package tutils provides common low-level utilities for all aistore unit and integration tests
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package tutils

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/devtools"
	"github.com/NVIDIA/aistore/devtools/tutils/tassert"
)

type nodesCnt int

func (n nodesCnt) satisfied(actual int) bool {
	if n == 0 {
		return true
	}
	return int(n) == actual
}

func JoinCluster(proxyURL string, node *cluster.Snode) error {
	return devtools.JoinCluster(devtoolsCtx, proxyURL, node, registerTimeout)
}

// TODO: There is duplication between `UnregisterNode` and `RemoveTarget` - when to use which?
func RemoveTarget(t *testing.T, proxyURL string, smap *cluster.Smap) (*cluster.Smap, *cluster.Snode) {
	removeTarget := ExtractTargetNodes(smap)[0]
	Logf("Removing target %s from smap %s\n", removeTarget.ID(), smap.StringEx())
	args := &cmn.ActValDecommision{DaemonID: removeTarget.ID(), SkipRebalance: true}
	err := UnregisterNode(proxyURL, args)
	tassert.CheckFatal(t, err)
	newSmap, err := WaitForClusterState(
		proxyURL,
		"target is gone",
		smap.Version,
		smap.CountProxies(),
		smap.CountTargets()-1,
	)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, newSmap.CountTargets() == smap.CountTargets()-1,
		"new smap expected to have 1 target less: %d (v%d) vs %d (v%d)", newSmap.CountTargets(), smap.CountTargets(),
		newSmap.Version, smap.Version)

	return newSmap, removeTarget
}

// TODO: There is duplication between `JoinCluster` and `RestoreTarget` - when to use which?
func RestoreTarget(t *testing.T, proxyURL string, target *cluster.Snode) (newSmap *cluster.Smap) {
	smap := GetClusterMap(t, proxyURL)
	tassert.Fatalf(t, smap.GetTarget(target.DaemonID) == nil, "unexpected target %s in smap", target.ID())
	Logf("Reregistering target %s, current smap: %s\n", target, smap.StringEx())
	err := JoinCluster(proxyURL, target)
	tassert.CheckFatal(t, err)
	newSmap, err = WaitForClusterState(
		proxyURL,
		"to join target back",
		smap.Version,
		smap.CountProxies(),
		smap.CountTargets()+1,
	)
	tassert.CheckFatal(t, err)
	return newSmap
}

func ClearMaintenance(baseParams api.BaseParams, tsi *cluster.Snode) {
	val := &cmn.ActValDecommision{DaemonID: tsi.ID(), SkipRebalance: true}
	// it can fail if the node is not under maintenance but it is OK
	_, _ = api.StopMaintenance(baseParams, val)
}

func ExtractTargetNodes(smap *cluster.Smap) cluster.Nodes {
	targets := make(cluster.Nodes, 0, smap.CountTargets())
	for _, target := range smap.Tmap {
		targets = append(targets, target)
	}
	return targets
}

func ExtractProxyNodes(smap *cluster.Smap) cluster.Nodes {
	proxies := make(cluster.Nodes, 0, smap.CountProxies())
	for _, proxy := range smap.Pmap {
		proxies = append(proxies, proxy)
	}
	return proxies
}

func RandomProxyURL(ts ...*testing.T) (url string) {
	var (
		baseParams = BaseAPIParams(proxyURLReadOnly)
		smap, err  = waitForStartup(baseParams, ts...)
		retries    = 3
	)
	if err == nil {
		return _getRandomProxyURL(smap)
	}
	for _, node := range pmapReadOnly {
		url := node.URL(cmn.NetworkPublic)
		if url == proxyURLReadOnly {
			continue
		}
		if retries == 0 {
			return ""
		}
		baseParams = BaseAPIParams(url)
		if smap, err = waitForStartup(baseParams, ts...); err == nil {
			return _getRandomProxyURL(smap)
		}
		retries--
	}
	return ""
}

func _getRandomProxyURL(smap *cluster.Smap) string {
	proxies := ExtractProxyNodes(smap)
	return proxies[rand.Intn(len(proxies))].URL(cmn.NetworkPublic)
}

// WaitForClusterState waits until a cluster reaches specified state, meaning:
// - smap has version larger than origVersion
// - number of proxies is equal proxyCnt, unless proxyCnt == 0
// - number of targets is equal targetCnt, unless targetCnt == 0.
//
// It returns the smap which satisfies those requirements.
// NOTE: Upon successful return from this function cluster state might have already changed.
func WaitForClusterState(proxyURL, reason string, origVersion int64, proxyCnt, targetCnt int) (*cluster.Smap, error) {
	var (
		lastVersion                               int64
		smapChangeDeadline, timeStart, opDeadline time.Time

		expPrx = nodesCnt(proxyCnt)
		expTgt = nodesCnt(targetCnt)
	)

	timeStart = time.Now()
	smapChangeDeadline = timeStart.Add(proxyChangeLatency)
	opDeadline = timeStart.Add(3 * proxyChangeLatency)

	Logf("Waiting for cluster state = (p=%d, t=%d, version > v%d) %s\n", expPrx, expTgt, origVersion, reason)

	var (
		loopCnt    int
		satisfied  bool
		baseParams = BaseAPIParams(proxyURL)
	)

	// Repeat until success or timeout.
	for {
		smap, err := api.GetClusterMap(baseParams)
		if err != nil {
			if !cmn.IsErrConnectionRefused(err) {
				return nil, err
			}
			Logf("%v\n", err)
			goto next
		}

		satisfied = expTgt.satisfied(smap.CountTargets()) && expPrx.satisfied(smap.CountProxies()) && smap.Version > origVersion
		if !satisfied {
			d := time.Since(timeStart)
			Logf("Still waiting at %s, current smap %s, elapsed (%s) at \n", proxyURL, smap.StringEx(),
				d.Truncate(time.Second))
		}

		if smap.Version != lastVersion {
			smapChangeDeadline = cmn.MinTime(time.Now().Add(proxyChangeLatency), opDeadline)
		}

		// if the primary's map changed to the state we want, wait for the map get populated
		if satisfied {
			syncedSmap := &cluster.Smap{}
			cmn.CopyStruct(syncedSmap, smap)

			// skip primary proxy and mock targets
			var proxyID string
			for _, p := range smap.Pmap {
				if p.PublicNet.DirectURL == proxyURL {
					proxyID = p.ID()
				}
			}
			err = WaitMapVersionSync(smapChangeDeadline, syncedSmap, origVersion, []string{MockDaemonID, proxyID})
			if err != nil {
				return nil, err
			}

			if syncedSmap.Version != smap.Version {
				if !expTgt.satisfied(smap.CountTargets()) || !expPrx.satisfied(smap.CountProxies()) {
					return nil, fmt.Errorf("smap changed after sync and does not satisfy the state, %s vs %s",
						smap, syncedSmap)
				}
				Logf("Smap changed after sync, but satisfies the state, %s vs %s", smap.StringEx(), syncedSmap.StringEx())
			}

			return smap, nil
		}

		lastVersion = smap.Version
		loopCnt++
	next:
		if time.Now().After(smapChangeDeadline) {
			break
		}

		time.Sleep(cmn.MinDuration(time.Second*time.Duration(loopCnt), time.Second*7)) // sleep longer every loop
	}

	return nil, fmt.Errorf("timed out waiting for the cluster to stabilize")
}

func WaitNodeRestored(t *testing.T, proxyURL, reason, nodeID string, origVersion int64, proxyCnt,
	targetCnt int) *cluster.Smap {
	smap, err := WaitForClusterState(proxyURL, reason, origVersion, proxyCnt, targetCnt)
	tassert.CheckFatal(t, err)
	_, err = api.WaitNodeAdded(BaseAPIParams(proxyURL), nodeID)
	tassert.CheckFatal(t, err)
	return smap
}

func WaitForNewSmap(proxyURL string, prevVersion int64) (newSmap *cluster.Smap, err error) {
	return WaitForClusterState(proxyURL, "new smap version", prevVersion, 0, 0)
}

func WaitMapVersionSync(timeout time.Time, smap *cluster.Smap, prevVersion int64, idsToIgnore []string) error {
	return devtools.WaitMapVersionSync(devtoolsCtx, timeout, smap, prevVersion, idsToIgnore)
}

func GetTargetsMountpaths(t *testing.T, smap *cluster.Smap, params api.BaseParams) map[string][]string {
	mpathsByTarget := make(map[string][]string, smap.CountTargets())
	for _, target := range smap.Tmap {
		mpl, err := api.GetMountpaths(params, target)
		tassert.CheckError(t, err)
		mpathsByTarget[target.DaemonID] = mpl.Available
	}

	return mpathsByTarget
}
