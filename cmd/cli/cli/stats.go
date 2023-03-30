// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains util functions and types.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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

func fillNodeStatusMap(c *cli.Context, daeType string) (smap *cluster.Smap, tstatusMap, pstatusMap teb.StstMap, err error) {
	if smap, err = getClusterMap(c); err != nil {
		return
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

	mmc := strings.Split(cmn.VersionAIStore, versionSepa)
	debug.Assert(len(mmc) > 2)
	ok := checkVersionWarn(c, apc.Target, mmc, tstatusMap)
	if ok && pstatusMap != nil {
		_ = checkVersionWarn(c, apc.Target, mmc, pstatusMap)
	}
	return
}

func checkVersionWarn(c *cli.Context, role string, mmc []string, stmap teb.StstMap) bool {
	expected := mmc[0] + versionSepa + mmc[1]
	for _, ds := range stmap {
		mmx := strings.Split(ds.Version, versionSepa)
		if len(mmx) < 3 {
			debug.Assert(len(mmx) > 2)
			continue
		}
		// major
		if mmc[0] != mmx[0] {
			verWarn(c, ds.Node.Snode, role, ds.Version, expected, true)
			return false
		}
		// minor
		minc, err := strconv.Atoi(mmc[1])
		debug.AssertNoErr(err)
		minx, err := strconv.Atoi(mmx[1])
		debug.AssertNoErr(err)
		if minc != minx {
			incompat := minc-minx > 1 || minc-minx < 1
			verWarn(c, ds.Node.Snode, role, ds.Version, expected, incompat)
			return false
		}
	}
	return true
}

func verWarn(c *cli.Context, snode *cluster.Snode, role, version, expected string, incompat bool) {
	var (
		sname, warn string
	)
	if role == apc.Proxy {
		sname = cluster.Pname(snode.ID())
	} else {
		sname = cluster.Tname(snode.ID())
	}
	if incompat {
		warn = fmt.Sprintf("node %s runs software version %s which is not compatible with the CLI (expecting v%s)",
			sname, version, expected)
	} else {
		warn = fmt.Sprintf("node %s runs software version %s which _may_ not be fully compatible with the CLI (expecting v%s)",
			sname, version, expected)
	}
	actionWarn(c, warn+"\n")
}

func daeStatus(nodeMap cluster.NodeMap, out teb.StstMap, wg cos.WG, mu *sync.Mutex) {
	for _, si := range nodeMap {
		wg.Add(1)
		go func(si *cluster.Snode) {
			_status(si, mu, out)
			wg.Done()
		}(si)
	}
}

func _status(node *cluster.Snode, mu *sync.Mutex, out teb.StstMap) {
	daeStatus, err := api.GetStatsAndStatus(apiBP, node)
	if err != nil {
		daeStatus = &stats.NodeStatus{}
		daeStatus.Snode = node
		if herr, ok := err.(*cmn.ErrHTTP); ok {
			daeStatus.Status = herr.TypeCode
		} else if strings.HasPrefix(err.Error(), "errNodeNotFound") {
			daeStatus.Status = "[errNodeNotFound]"
		} else {
			daeStatus.Status = "[" + err.Error() + "]"
		}
	} else if daeStatus.Status == "" {
		daeStatus.Status = teb.NodeOnline
		switch {
		case node.Flags.IsSet(cluster.NodeFlagMaint):
			daeStatus.Status = apc.NodeMaintenance
		case node.Flags.IsSet(cluster.NodeFlagDecomm):
			daeStatus.Status = apc.NodeDecommission
		}
	}

	mu.Lock()
	out[node.ID()] = daeStatus
	mu.Unlock()
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

	// post-interval (end) stats
	_, e, _, err = fillNodeStatusMap(c, apc.Target)
	return
}
