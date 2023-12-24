// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains util functions and types.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"os"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/urfave/cli"
)

// In this file:
// utility functions, wrappers, and helpers to work with cluster map (Smap) and clustered nodes.

var curSmap *meta.Smap

// the implementation may look simplified, even naive, but in fact
// within-a-single-command lifecycle and singlethreaded-ness eliminate
// all scenarios that'd be otherwise commonly expected
func getClusterMap(c *cli.Context) (*meta.Smap, error) {
	// when "long-running" refresh Smap as well
	// (see setLongRunParams)
	if curSmap != nil && !flagIsSet(c, refreshFlag) {
		return curSmap, nil
	}
	smap, err := api.GetClusterMap(apiBP)
	if err != nil {
		return nil, V(err)
	}
	curSmap = smap
	if smap.Primary.PubNet.URL != apiBP.URL {
		if cliConfVerbose() {
			what := env.AIS.Endpoint
			if os.Getenv(env.AIS.Endpoint) == "" {
				what = "CLI config URL"
			}
			warn := fmt.Sprintf("changing %s from %q to %q", what, apiBP.URL, smap.Primary.PubNet.URL)
			actionWarn(c, warn)
		}
		apiBP.URL = smap.Primary.PubNet.URL
	}
	return curSmap, nil
}

func getNode(c *cli.Context, arg string) (node *meta.Snode, sname string, err error) {
	var (
		sid  string
		smap *meta.Smap
	)
	if arg == "" {
		// NOTE: use arg0Node() when the node arg is optional
		err = missingArgumentsError(c, c.Command.ArgsUsage)
		return
	}
	if smap, err = getClusterMap(c); err != nil {
		return
	}
	if strings.HasPrefix(arg, meta.TnamePrefix) || strings.HasPrefix(arg, meta.PnamePrefix) {
		sid = meta.N2ID(arg)
	} else {
		sid = arg
	}
	if node = smap.GetNode(sid); node == nil {
		err = &errDoesNotExist{
			what:   "node",
			name:   arg,
			suffix: " (" + tabHelpOpt + ", or see 'ais show cluster')",
		}
		return
	}
	sname = node.StringEx()
	return
}

// Gets Smap from a given node (`daemonID`) and displays it
func smapFromNode(c *cli.Context, primarySmap *meta.Smap, sid string, usejs bool) error {
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
	for _, m := range []meta.NodeMap{smap.Tmap, smap.Pmap} {
		for _, v := range m {
			if v.PubNet != v.ControlNet || v.PubNet != v.DataNet {
				extendedURLs = true
			}
		}
	}
	body := teb.SmapHelper{
		Smap:         smap,
		ExtendedURLs: extendedURLs,
	}
	if flagIsSet(c, noHeaderFlag) {
		return teb.Print(body, teb.SmapTmplNoHdr, teb.Jopts(usejs))
	}
	return teb.Print(body, teb.SmapTmpl, teb.Jopts(usejs))
}
