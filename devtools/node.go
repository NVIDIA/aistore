// Package devtools provides common low-level utilities for AIStore development tools.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package devtools

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/devtools/tlog"
)

func JoinCluster(ctx *Ctx, proxyURL string, node *cluster.Snode, timeout time.Duration) (rebID string, err error) {
	baseParams := BaseAPIParams(ctx, proxyURL)
	smap, err := api.GetClusterMap(baseParams)
	if err != nil {
		return "", err
	}
	if rebID, _, err = api.JoinCluster(baseParams, node); err != nil {
		return
	}

	// If node is already in cluster we should not wait for map version
	// sync because update will not be scheduled
	if node := smap.GetNode(node.ID()); node == nil {
		err = WaitMapVersionSync(baseParams, ctx, time.Now().Add(timeout), smap, smap.Version, cos.NewStringSet())
		return
	}
	return
}

func _nextNode(smap *cluster.Smap, idsToIgnore cos.StringSet) (string, string, bool) {
	for _, d := range smap.Pmap {
		if !idsToIgnore.Contains(d.ID()) {
			return d.ID(), d.PublicNet.DirectURL, true
		}
	}
	for _, d := range smap.Tmap {
		if !idsToIgnore.Contains(d.ID()) {
			return d.ID(), d.PublicNet.DirectURL, true
		}
	}

	return "", "", false
}

func WaitMapVersionSync(baseParams api.BaseParams, ctx *Ctx, timeout time.Time, smap *cluster.Smap, prevVersion int64,
	idsToIgnore cos.StringSet) error {
	var (
		prevSid string
		orig    = idsToIgnore.Clone()
	)
	for {
		sid, _, exists := _nextNode(smap, idsToIgnore)
		if !exists {
			break
		}
		if sid == prevSid {
			time.Sleep(time.Second)
		}
		daemonSmap, err := api.GetNodeClusterMap(baseParams, sid)
		if err != nil && !cos.IsRetriableConnErr(err) &&
			!cmn.IsStatusServiceUnavailable(err) && !cmn.IsStatusBadGateway(err) /* retry as well */ {
			return err
		}
		if err == nil && daemonSmap.Version > prevVersion {
			idsToIgnore.Add(sid)
			if daemonSmap.Version > smap.Version {
				*smap = *daemonSmap
			}
			if daemonSmap.Version > prevVersion+1 {
				// update Smap to a newer version and restart waiting
				if prevVersion < 0 {
					ctx.Log("%s (from node %s)\n", daemonSmap.StringEx(), sid)
				} else {
					ctx.Log("Updating Smap v%d to %s (from node %s)\n", prevVersion, daemonSmap.StringEx(), sid)
				}
				*smap = *daemonSmap
				prevVersion = smap.Version - 1
				idsToIgnore = orig.Clone()
				idsToIgnore.Add(sid)
			}
			continue
		}
		if time.Now().After(timeout) {
			return fmt.Errorf("timed out waiting for node %s to sync Smap > v%d", sid, prevVersion)
		}
		if daemonSmap != nil {
			if snode := daemonSmap.GetNode(sid); snode != nil {
				ctx.Log("Waiting for %s(%s) to sync Smap > v%d\n", snode.StringEx(), daemonSmap, prevVersion)
			} else {
				ctx.Log("Waiting for node %s(%s) to sync Smap > v%d\n", sid, daemonSmap, prevVersion)
			}
		}
		prevSid = sid
	}
	return nil
}

// Quick remove node from SMap
func RemoveNodeFromSmap(ctx *Ctx, proxyURL, sid string, timeout time.Duration) error {
	var (
		baseParams = BaseAPIParams(ctx, proxyURL)
		smap, err  = api.GetClusterMap(baseParams)
		node       = smap.GetNode(sid)
	)
	if err != nil {
		return fmt.Errorf("api.GetClusterMap failed, err: %v", err)
	}
	if node != nil && smap.IsPrimary(node) {
		return fmt.Errorf("unregistering primary proxy is not allowed")
	}
	tlog.Logf("Remove %s from %s\n", node.StringEx(), smap)

	err = api.RemoveNodeFromSmap(baseParams, sid)
	if err != nil {
		return err
	}

	// If node does not exist in cluster we should not wait for map version
	// sync because update will not be scheduled.
	if node != nil {
		return WaitMapVersionSync(
			baseParams,
			ctx,
			time.Now().Add(timeout),
			smap,
			smap.Version,
			cos.NewStringSet(node.ID()),
		)
	}
	return nil
}
