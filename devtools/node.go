// Package devtools provides common low-level utilities for AIStore development tools.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package devtools

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

func JoinCluster(ctx *Ctx, proxyURL string, node *cluster.Snode, timeout time.Duration) (rebID string, err error) {
	baseParams := BaseAPIParams(ctx, proxyURL)
	smap, err := api.GetClusterMap(baseParams)
	if err != nil {
		return "", err
	}
	if rebID, err = api.JoinCluster(baseParams, node); err != nil {
		return
	}

	// If node is already in cluster we should not wait for map version
	// sync because update will not be scheduled
	if node := smap.GetNode(node.ID()); node == nil {
		err = WaitMapVersionSync(ctx, time.Now().Add(timeout), smap, smap.Version, []string{})
		return
	}
	return
}

// Quick node removal: it does not start and wait for rebalance to complete
// before removing the node. Because node removal uses transactions, this
// function cannot be used for MOCK nodes as they do not implement required
// HTTP handlers. To unregister a mock, use `RemoveNodeFromSmap` instead.
func UnregisterNode(ctx *Ctx, proxyURL string, args *cmn.ActValDecommision, timeout time.Duration) error {
	baseParams := BaseAPIParams(ctx, proxyURL)
	smap, err := api.GetClusterMap(baseParams)
	node := smap.GetNode(args.DaemonID)
	if err != nil {
		return fmt.Errorf("api.GetClusterMap failed, err: %v", err)
	}

	if node != nil && smap.IsPrimary(node) {
		return fmt.Errorf("unregistering primary proxy is not allowed")
	}

	if _, err := api.Decommission(baseParams, args); err != nil {
		return err
	}

	// If node does not exists in cluster we should not wait for map version
	// sync because update will not be scheduled
	if node != nil {
		return WaitMapVersionSync(ctx, time.Now().Add(timeout), smap, smap.Version, []string{node.ID()})
	}
	return nil
}

func WaitMapVersionSync(ctx *Ctx, timeout time.Time, smap *cluster.Smap, prevVersion int64, idsToIgnore []string) error {
	ctx.Log("Waiting to sync Smap version > v%d, ignoring %+v\n", prevVersion, idsToIgnore)
	checkAwaitingDaemon := func(smap *cluster.Smap, idsToIgnore []string) (string, string, bool) {
		for _, d := range smap.Pmap {
			if !cmn.StringInSlice(d.ID(), idsToIgnore) {
				return d.ID(), d.PublicNet.DirectURL, true
			}
		}
		for _, d := range smap.Tmap {
			if !cmn.StringInSlice(d.ID(), idsToIgnore) {
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
		baseParams := BaseAPIParams(ctx, url)
		daemonSmap, err := api.GetClusterMap(baseParams)
		// NOTE: Retry if node returns `http.StatusServiceUnavailable`
		if err != nil && !cmn.IsErrConnectionRefused(err) && !cmn.IsStatusServiceUnavailable(err) {
			return err
		}

		if err == nil && daemonSmap.Version > prevVersion {
			idsToIgnore = append(idsToIgnore, sid)
			*smap = *daemonSmap // update smap for newer version
			continue
		}

		if time.Now().After(timeout) {
			return fmt.Errorf("timed out waiting for sync-ed Smap version > %d from %s (v%d)", prevVersion, url, smap.Version)
		}

		ctx.Log("waiting for Smap > v%d at %s (currently v%d)\n", prevVersion, sid, daemonSmap.Version)
		time.Sleep(time.Second)
	}
	return nil
}
