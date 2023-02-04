// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains util functions and types.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/urfave/cli"
)

// In this file:
// utility functions, wrappers, and helpers to work with cluster map (Smap) and clustered nodes.

var curSmap *cluster.Smap

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

// returns apc.Target, apc.Proxy
// returns "" on error of any kind
func getNodeType(c *cli.Context, sid string) (daeType string) {
	smap, err := getClusterMap(c)
	if err != nil {
		debug.AssertNoErr(err)
		return
	}
	node := smap.GetNode(sid)
	if node != nil {
		daeType = node.Type()
	}
	return
}

// compare with `argNode()` that makes the arg optional
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
		err = fmt.Errorf("node %q does not exist ("+tabHelpOpt+", or see 'ais show cluster')", arg)
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
