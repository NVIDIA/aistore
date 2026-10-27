// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains utility functions and types.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"

	"github.com/urfave/cli"
)

type nodeStatusCtx struct {
	err error
	mu  sync.Mutex
	ok  bool
}

// NOTE: target's metric names & kinds
func getMetricNames(c *cli.Context) (cos.StrKVs, error) {
	smap, err := getClusterMap(c)
	if err != nil {
		return nil, err
	}
	if smap.CountActiveTs() == 0 {
		return nil, nil
	}
	tsi, err := smap.GetRandTarget()
	if err != nil {
		return nil, err
	}
	return api.GetMetricNames(apiBP, tsi)
}

//
// teb.NodeStatusMap
//

func fillNodeStatusMap(c *cli.Context, daeType string) (smap *meta.Smap, tstatusMap, pstatusMap teb.NodeStatusMap, err error) {
	smap, tstatusMap, pstatusMap, err = fillStatusMapNoVersion(c, daeType)
	if err == nil {
		checkNodeStatusVersion(c, tstatusMap, pstatusMap)
	}
	return smap, tstatusMap, pstatusMap, err
}

func checkNodeStatusVersion(c *cli.Context, tstatusMap, pstatusMap teb.NodeStatusMap) {
	ok := checkVersionWarn(c, apc.Target, tstatusMap)
	if ok && pstatusMap != nil {
		_ = checkVersionWarn(c, apc.Proxy, pstatusMap)
	}
}

func fillStatusMapNoVersion(c *cli.Context, daeType string) (smap *meta.Smap, tstatusMap, pstatusMap teb.NodeStatusMap, err error) {
	if smap, err = getClusterMap(c); err != nil {
		return nil, nil, nil, err
	}
	var (
		wg         cos.WG
		ctx        = &nodeStatusCtx{}
		pcnt, tcnt = smap.CountProxies(), smap.CountTargets()
	)
	switch daeType {
	case apc.Target:
		wg = cos.NewLimitedWaitGroup(sys.NumCPU(), tcnt)
		tstatusMap = make(teb.NodeStatusMap, tcnt)
		daeStatus(smap.Tmap, tstatusMap, wg, ctx)
	case apc.Proxy:
		wg = cos.NewLimitedWaitGroup(sys.NumCPU(), pcnt)
		pstatusMap = make(teb.NodeStatusMap, pcnt)
		daeStatus(smap.Pmap, pstatusMap, wg, ctx)
	default:
		wg = cos.NewLimitedWaitGroup(sys.NumCPU(), pcnt+tcnt)
		tstatusMap = make(teb.NodeStatusMap, tcnt)
		pstatusMap = make(teb.NodeStatusMap, pcnt)
		daeStatus(smap.Tmap, tstatusMap, wg, ctx)
		daeStatus(smap.Pmap, pstatusMap, wg, ctx)
	}

	wg.Wait()
	if !ctx.ok && ctx.err != nil {
		return smap, tstatusMap, pstatusMap, ctx.err
	}
	return smap, tstatusMap, pstatusMap, nil
}

func isRebalancing(tstatusMap teb.NodeStatusMap) bool {
	for _, ds := range tstatusMap {
		if ds.RebSnap != nil {
			if !ds.RebSnap.IsAborted() && ds.RebSnap.EndTime.IsZero() {
				return true
			}
		}
	}
	return false
}

func daeStatus(nodeMap meta.NodeMap, out teb.NodeStatusMap, wg cos.WG, ctx *nodeStatusCtx) {
	for _, si := range nodeMap {
		wg.Add(1)
		go func(si *meta.Snode) {
			_addStatus(si, ctx, out)
			wg.Done()
		}(si)
	}
}

func _addStatus(node *meta.Snode, ctx *nodeStatusCtx, out teb.NodeStatusMap) {
	ds, err := _status(node)
	if err != nil {
		ds = &stats.NodeStatus{}
		ds.Snode = node
		if herr := cmn.AsErrHTTP(err); herr != nil && herr.TypeCode != "" {
			// TODO: show a meaningful network failure in the STATUS col (see related `bab474d19ccc`)
			ds.Status = herr.TypeCode
		} else if strings.HasPrefix(err.Error(), "errNodeNotFound") {
			ds.Status = "[errNodeNotFound]"
		} else {
			ds.Status = "[" + err.Error() + "]"
		}
	} else if ds.Status == "" {
		ds.Status = teb.FmtNodeStatus(node)
	}

	ctx.mu.Lock()
	out[node.ID()] = ds
	if err == nil {
		ctx.ok = true
	} else if ctx.err == nil && !node.InMaintOrDecomm() {
		ctx.err = err
	}
	ctx.mu.Unlock()
}

func _status(node *meta.Snode) (ds *stats.NodeStatus, err error) {
	return api.GetStatsAndStatus(apiBP, node)
}

//
// throughput
//

func _cluStatusBeginEnd(c *cli.Context, ini teb.NodeStatusMap, sleep time.Duration) (b, e teb.NodeStatusMap, err error) {
	b = ini
	if b == nil {
		// begin stats
		if _, b, _, err = fillNodeStatusMap(c, apc.Target); err != nil {
			return nil, nil, err
		}
	}

	time.Sleep(sleep)

	// post-interval (end) stats without checking cli version a second time
	_, e, _, err = fillStatusMapNoVersion(c, apc.Target)
	return
}
