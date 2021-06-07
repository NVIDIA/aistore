// Package devtools provides common low-level utilities for AIStore development tools.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package devtools

import (
	"fmt"
	"net/http"
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

func WaitMapVersionSync(baseParams api.BaseParams, ctx *Ctx, timeout time.Time, smap *cluster.Smap, prevVersion int64,
	idsToIgnore cos.StringSet) error {
	ctx.Log("Waiting to sync Smap version > v%d, ignoring %+v\n", prevVersion, idsToIgnore)
	checkAwaitingDaemon := func(smap *cluster.Smap, idsToIgnore cos.StringSet) (string, string, bool) {
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
	var prevSid string
	for {
		sid, url, exists := checkAwaitingDaemon(smap, idsToIgnore)
		if !exists {
			break
		}
		if sid == prevSid {
			time.Sleep(time.Second)
		}
		daemonSmap, err := api.GetNodeClusterMap(baseParams, sid)
		// NOTE: Retry if node returns `http.StatusServiceUnavailable` or `http.StatusBadGateway`
		if err != nil &&
			!cos.IsErrConnectionRefused(err) &&
			!cmn.IsStatusServiceUnavailable(err) &&
			!cmn.IsStatusBadGateway(err) {
			return err
		}

		if err == nil && daemonSmap.Version > prevVersion && daemonSmap.Version >= smap.Version {
			idsToIgnore.Add(sid)
			*smap = *daemonSmap // update smap for newer version
			continue
		}
		if time.Now().After(timeout) {
			return fmt.Errorf("timed out waiting for sync-ed Smap version > %d from %s (v%d)",
				prevVersion, url, smap.Version)
		}
		if daemonSmap != nil {
			ctx.Log("waiting for Smap > v%d at %s (currently v%d)\n",
				prevVersion, sid, daemonSmap.Version)
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
	tlog.Logf("Remove %s from %s\n", node, smap)

	baseParams.Method = http.MethodDelete
	err = api.DoHTTPRequest(api.ReqParams{
		BaseParams: baseParams,
		Path:       cmn.URLPathClusterDaemon.Join(sid),
	})
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
