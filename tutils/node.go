// Package tutils provides common low-level utilities for all aistore unit and integration tests
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package tutils

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/tutils/tassert"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

func RegisterNode(proxyURL string, targetNode *cluster.Snode, smap *cluster.Smap) error {
	_, ok := smap.Tmap[targetNode.ID()]
	baseParams := BaseAPIParams(proxyURL)
	if err := api.RegisterNode(baseParams, targetNode); err != nil {
		return err
	}

	// If target is already in cluster we should not wait for map version
	// sync because update will not be scheduled
	if !ok {
		return WaitMapVersionSync(time.Now().Add(registerTimeout), smap, smap.Version, []string{})
	}
	return nil
}

func RemoveTarget(t *testing.T, proxyURL string, smap *cluster.Smap) (*cluster.Smap, *cluster.Snode) {
	removeTarget := ExtractTargetNodes(smap)[0]
	Logf("Removing target %s\n", removeTarget.ID())
	err := UnregisterNode(proxyURL, removeTarget.ID())
	tassert.CheckFatal(t, err)
	smap, err = WaitForPrimaryProxy(
		proxyURL,
		"target is gone",
		smap.Version, testing.Verbose(),
		smap.CountProxies(),
		smap.CountTargets()-1,
	)
	tassert.CheckFatal(t, err)

	return smap, removeTarget
}

func RestoreTarget(t *testing.T, proxyURL string, smap *cluster.Smap, target *cluster.Snode) *cluster.Smap {
	Logf("Reregistering target %s...\n", target)
	err := RegisterNode(proxyURL, target, smap)
	tassert.CheckFatal(t, err)
	smap, err = WaitForPrimaryProxy(
		proxyURL,
		"to join target back",
		smap.Version, testing.Verbose(),
		smap.CountProxies(),
		smap.CountTargets()+1,
	)
	tassert.CheckFatal(t, err)
	return smap
}

func ExtractTargetNodes(smap *cluster.Smap) cluster.Nodes {
	targets := make(cluster.Nodes, 0, smap.CountTargets())
	for _, target := range smap.Tmap {
		targets = append(targets, target)
	}
	return targets
}

// WaitForPrimaryProxy reads the current primary proxy(which is proxyurl)'s smap until its
// version changed at least once from the original version and then settles down(doesn't change anymore)
// if primary proxy is successfully updated, wait until the map is populated to all members of the cluster
// returns the latest smap of the cluster
//
// The function always waits until SMap's version increases but there are
// two optional parameters for extra checks: proxyCount and targetCount(nodeCnt...).
// If they are not zeroes then besides version check the function waits for given
// number of proxies and/or targets are present in SMap.
// It is useful if the test kills more than one proxy/target. In this case the
// primary proxy may run two metasync calls and we cannot tell if the current SMap
// is what we are waiting for only by looking at its version.
func WaitForPrimaryProxy(proxyURL, reason string, origVersion int64, verbose bool, nodeCnt ...int) (*cluster.Smap, error) {
	var (
		lastVersion          int64
		timeUntil, timeStart time.Time
		totalProxies         int
		totalTargets         int
	)
	timeStart = time.Now()
	timeUntil = timeStart.Add(proxyChangeLatency)
	if len(nodeCnt) > 0 {
		totalProxies = nodeCnt[0]
	}
	if len(nodeCnt) > 1 {
		totalTargets = nodeCnt[1]
	}

	if verbose {
		if totalProxies > 0 {
			a := "proxies"
			if totalProxies == 1 {
				a = "proxy"
			}
			Logf("waiting for %d %s\n", totalProxies, a)
		}
		if totalTargets > 0 {
			a := "targets"
			if totalTargets == 1 {
				a = "target"
			}
			Logf("waiting for %d %s\n", totalTargets, a)
		}
		Logf("waiting for the cluster [%s, Smap version > %d]\n", reason, origVersion)
	}

	var loopCnt int
	baseParams := BaseAPIParams(proxyURL)
	for {
		smap, err := api.GetClusterMap(baseParams)
		if err != nil && !cmn.IsErrConnectionRefused(err) {
			return nil, err
		}
		ts, ps := smap.CountTargets(), smap.CountProxies()
		doCheckSMap := (totalTargets == 0 || ts == totalTargets) && (totalProxies == 0 || ps == totalProxies)
		if !doCheckSMap {
			d := time.Since(timeStart)
			expectedTargets, expectedProxies := totalTargets, totalProxies
			if totalTargets == 0 {
				expectedTargets = ts
			}
			if totalProxies == 0 {
				expectedProxies = ps
			}
			Logf("waiting for Smap at %s[t%d/%d, p%d/%d] (%v)\n", proxyURL, ts, expectedTargets, ps, expectedProxies,
				d.Truncate(time.Second))
		}

		// if the primary's map changed to the state we want, wait for the map get populated
		if err == nil && smap.Version == lastVersion && smap.Version > origVersion && doCheckSMap {
			for {
				smap, err = api.GetClusterMap(baseParams)
				if err == nil {
					break
				}

				if !cmn.IsErrConnectionRefused(err) {
					return smap, err
				}

				if time.Now().After(timeUntil) {
					return smap, fmt.Errorf("primary proxy's Smap timed out")
				}
				time.Sleep(time.Second)
			}

			// skip primary proxy and mock targets
			var proxyID string
			for _, p := range smap.Pmap {
				if p.PublicNet.DirectURL == proxyURL {
					proxyID = p.ID()
				}
			}
			err = WaitMapVersionSync(timeUntil, smap, origVersion, []string{MockDaemonID, proxyID})
			return smap, err
		}

		if time.Now().After(timeUntil) {
			break
		}

		lastVersion = smap.Version
		loopCnt++
		time.Sleep(time.Second * time.Duration(loopCnt)) // sleep longer every loop
	}

	return nil, fmt.Errorf("timed out waiting for the cluster to stabilize")
}

func WaitMapVersionSync(timeout time.Time, smap *cluster.Smap, prevVersion int64, idsToIgnore []string) error {
	inList := func(s string, values []string) bool {
		for _, v := range values {
			if s == v {
				return true
			}
		}

		return false
	}

	checkAwaitingDaemon := func(smap *cluster.Smap, idsToIgnore []string) (string, string, bool) {
		for _, d := range smap.Pmap {
			if !inList(d.ID(), idsToIgnore) {
				return d.ID(), d.PublicNet.DirectURL, true
			}
		}
		for _, d := range smap.Tmap {
			if !inList(d.ID(), idsToIgnore) {
				return d.ID(), d.PublicNet.DirectURL, true
			}
		}

		return "", "", false
	}

	for {
		sid, url, exists := checkAwaitingDaemon(smap, idsToIgnore)
		if !exists {
			break
		}
		baseParams := BaseAPIParams(url)
		daemonSmap, err := api.GetClusterMap(baseParams)
		if err != nil && !cmn.IsErrConnectionRefused(err) {
			return err
		}

		if err == nil && daemonSmap.Version > prevVersion {
			idsToIgnore = append(idsToIgnore, sid)
			smap = daemonSmap // update smap for newer version
			continue
		}

		if time.Now().After(timeout) {
			return fmt.Errorf("timed out waiting for sync-ed Smap version > %d from %s (v%d)", prevVersion, url, smap.Version)
		}

		// TODO: `WaitMapVersionSync` is imported/used in `soaktest` which
		//  prevents us from using `Logf` (`testing.Verbose` will panic
		//  because `testing.Init` was not called). We should somehow detect
		//  running this inside actual tests (`Test*`).
		fmt.Fprintf(os.Stderr, "waiting for Smap > v%d at %s (currently v%d)\n", prevVersion, sid, daemonSmap.Version)
		time.Sleep(time.Second)
	}
	return nil
}
