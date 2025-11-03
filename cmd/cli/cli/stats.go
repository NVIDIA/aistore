// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains utility functions and types.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"

	"github.com/urfave/cli"
)

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
// teb.StstMap
//

const versionSepa = "."

func fillNodeStatusMap(c *cli.Context, daeType string) (smap *meta.Smap, tstatusMap, pstatusMap teb.StstMap, err error) {
	smap, tstatusMap, pstatusMap, err = fillStatusMapNoVersion(c, daeType)
	checkNodeStatusVersion(c, tstatusMap, pstatusMap)
	return smap, tstatusMap, pstatusMap, err
}

func checkNodeStatusVersion(c *cli.Context, tstatusMap, pstatusMap teb.StstMap) {
	mmc := strings.Split(cmn.VersionAIStore, versionSepa)
	debug.Assert(len(mmc) > 1)
	ok := checkVersionWarn(c, apc.Target, mmc, tstatusMap)
	if ok && pstatusMap != nil {
		_ = checkVersionWarn(c, apc.Proxy, mmc, pstatusMap)
	}
}

func fillStatusMapNoVersion(c *cli.Context, daeType string) (smap *meta.Smap, tstatusMap, pstatusMap teb.StstMap, err error) {
	if smap, err = getClusterMap(c); err != nil {
		return nil, nil, nil, err
	}
	var (
		wg         cos.WG
		mu         = &sync.Mutex{}
		pcnt, tcnt = smap.CountProxies(), smap.CountTargets()
	)
	switch daeType {
	case apc.Target:
		wg = cos.NewLimitedWaitGroup(sys.NumCPU(), tcnt)
		tstatusMap = make(teb.StstMap, tcnt)
		daeStatus(smap.Tmap, tstatusMap, wg, mu)
	case apc.Proxy:
		wg = cos.NewLimitedWaitGroup(sys.NumCPU(), pcnt)
		pstatusMap = make(teb.StstMap, pcnt)
		daeStatus(smap.Pmap, pstatusMap, wg, mu)
	default:
		wg = cos.NewLimitedWaitGroup(sys.NumCPU(), pcnt+tcnt)
		tstatusMap = make(teb.StstMap, tcnt)
		pstatusMap = make(teb.StstMap, pcnt)
		daeStatus(smap.Tmap, tstatusMap, wg, mu)
		daeStatus(smap.Pmap, pstatusMap, wg, mu)
	}

	wg.Wait()
	return smap, tstatusMap, pstatusMap, nil
}

func isRebalancing(tstatusMap teb.StstMap) bool {
	for _, ds := range tstatusMap {
		if ds.RebSnap != nil {
			if !ds.RebSnap.IsAborted() && ds.RebSnap.EndTime.IsZero() {
				return true
			}
		}
	}
	return false
}

func daeStatus(nodeMap meta.NodeMap, out teb.StstMap, wg cos.WG, mu *sync.Mutex) {
	for _, si := range nodeMap {
		wg.Add(1)
		go func(si *meta.Snode) {
			_addStatus(si, mu, out)
			wg.Done()
		}(si)
	}
}

func _addStatus(node *meta.Snode, mu *sync.Mutex, out teb.StstMap) {
	ds, err := _status(node)
	if err != nil {
		ds = &stats.NodeStatus{}
		ds.Snode = node
		if herr := cmn.AsErrHTTP(err); herr != nil {
			ds.Status = herr.TypeCode
		} else if strings.HasPrefix(err.Error(), "errNodeNotFound") {
			ds.Status = "[errNodeNotFound]"
		} else {
			ds.Status = "[" + err.Error() + "]"
		}
	} else if ds.Status == "" {
		ds.Status = teb.FmtNodeStatus(node)
	}

	mu.Lock()
	out[node.ID()] = ds
	mu.Unlock()
}

func _status(node *meta.Snode) (ds *stats.NodeStatus, err error) {
	return api.GetStatsAndStatus(apiBP, node)
}

//
// throughput
//

func _cluStatusBeginEnd(c *cli.Context, ini teb.StstMap, sleep time.Duration) (b, e teb.StstMap, err error) {
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
