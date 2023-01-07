// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains util functions and types.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"strings"
	"sync"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/urfave/cli"
)

// In this file:
// utility functions, wrappers, and helpers to work with cluster map (Smap) and clustered nodes.

var (
	curSmap      *cluster.Smap
	curPrxStatus stats.DaemonStatusMap
	curTgtStatus stats.DaemonStatusMap
)

// the implementation may look simplified, even naive, but in fact
// within-a-single-command lifecycle and singlethreaded-ness eliminate
// all scenarios that'd be otherwise commonly expected
func getClusterMap(c *cli.Context) (*cluster.Smap, error) {
	if curSmap != nil {
		return curSmap, nil
	}
	smap, err := api.GetClusterMap(apiBP)
	if err != nil {
		return nil, err
	}
	curSmap = smap
	if smap.Primary.PubNet.URL != apiBP.URL {
		actionWarn(c, fmt.Sprintf("changing %s from %q to %q",
			env.AIS.Endpoint, smap.Primary.PubNet.URL, apiBP.URL))
		apiBP.URL = smap.Primary.PubNet.URL
	}
	return curSmap, nil
}

func getNodeIDName(c *cli.Context, arg string) (sid, sname string, err error) {
	if arg == "" {
		err = missingArgumentsError(c, c.Command.ArgsUsage)
		return
	}
	smap, errV := getClusterMap(c)
	if errV != nil {
		err = errV
		return
	}
	if strings.HasPrefix(arg, cluster.TnamePrefix) || strings.HasPrefix(arg, cluster.PnamePrefix) {
		sname = arg
		sid = cluster.N2ID(arg)
	} else {
		sid = arg
	}
	node := smap.GetNode(sid)
	if node == nil {
		if sname == "" {
			err = fmt.Errorf("node ID=%s does not exist (see 'ais show cluster')", arg)
		} else {
			err = fmt.Errorf("node %s does not exist (see 'ais show cluster')", arg)
		}
		return
	}
	sname = node.StringEx()
	return
}

// Gets Smap from a given node (`daemonID`) and displays it
func smapFromNode(c *cli.Context, primarySmap *cluster.Smap, sid string, usejs bool) error {
	var (
		smap         = primarySmap
		err          error
		extendedURLs bool
	)
	if sid != "" {
		smap, err = api.GetNodeClusterMap(apiBP, sid)
		if err != nil {
			return err
		}
	}
	for _, m := range []cluster.NodeMap{smap.Tmap, smap.Pmap} {
		for _, v := range m {
			if v.PubNet != v.ControlNet || v.PubNet != v.DataNet {
				extendedURLs = true
			}
		}
	}
	body := tmpls.SmapTemplateHelper{
		Smap:         smap,
		ExtendedURLs: extendedURLs,
	}
	return tmpls.Print(body, c.App.Writer, tmpls.SmapTmpl, nil, usejs)
}

// Get cluster map and use it to retrieve node status for each clustered node
func fillNodeStatusMap(c *cli.Context) (*cluster.Smap, error) {
	smap, err := getClusterMap(c)
	if err != nil {
		return nil, err
	}
	proxyCount := smap.CountProxies()
	targetCount := smap.CountTargets()

	curPrxStatus = make(stats.DaemonStatusMap, proxyCount)
	curTgtStatus = make(stats.DaemonStatusMap, targetCount)

	wg := cos.NewLimitedWaitGroup(sys.NumCPU(), proxyCount+targetCount)
	mu := &sync.Mutex{}
	daeStatus(smap.Pmap, curPrxStatus, wg, mu)
	daeStatus(smap.Tmap, curTgtStatus, wg, mu)
	wg.Wait()
	return smap, nil
}

func daeStatus(nodeMap cluster.NodeMap, daeMap stats.DaemonStatusMap, wg cos.WG, mu *sync.Mutex) {
	for _, si := range nodeMap {
		wg.Add(1)
		go func(si *cluster.Snode) {
			_status(si, mu, daeMap)
			wg.Done()
		}(si)
	}
}

func _status(node *cluster.Snode, mu *sync.Mutex, daeMap stats.DaemonStatusMap) {
	daeInfo, err := api.GetDaemonStatus(apiBP, node)
	if err != nil {
		daeInfo = &stats.DaemonStatus{Snode: node, Status: "Error: " + err.Error()}
	} else if node.Flags.IsSet(cluster.NodeFlagMaint) {
		daeInfo.Status = "maintenance"
	} else if node.Flags.IsSet(cluster.NodeFlagDecomm) {
		daeInfo.Status = "decommission"
	}
	mu.Lock()
	daeMap[node.ID()] = daeInfo
	mu.Unlock()
}
