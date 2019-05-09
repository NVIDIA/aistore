// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that interact with the cluster
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
	"github.com/urfave/cli"
)

var (
	proxy  = make(map[string]*stats.DaemonStatus)
	target = make(map[string]*stats.DaemonStatus)

	longRunFlags    = []cli.Flag{refreshFlag, countFlag}
	daecluBaseFlags = []cli.Flag{jsonFlag}

	daecluFlags = map[string][]cli.Flag{
		cmn.GetWhatSmap:         daecluBaseFlags,
		cmn.GetWhatDaemonStatus: append(daecluBaseFlags, longRunFlags...),
		cmn.GetWhatStats:        append(daecluBaseFlags, longRunFlags...),
	}

	// DaeCluCmds tracks available AIS API Information/Query Commands
	daeCluCmds = []cli.Command{
		{
			Name:         cmn.GetWhatSmap,
			Usage:        "returns cluster map",
			Action:       queryHandler,
			Flags:        daecluFlags[cmn.GetWhatSmap],
			BashComplete: daemonList,
		},
		{
			Name:         cmn.GetWhatDaemonStatus,
			Usage:        "returns status of daemon",
			Action:       queryHandler,
			Flags:        daecluFlags[cmn.GetWhatDaemonStatus],
			BashComplete: daemonList,
		},
		{
			Name:         cmn.GetWhatStats,
			Usage:        "returns stats of daemon",
			Action:       queryHandler,
			Flags:        daecluFlags[cmn.GetWhatStats],
			BashComplete: daemonList,
		},
	}
)

func updateLongRunVariables(c *cli.Context) {
	if flagIsSet(c, refreshFlag) {
		refreshRate = parseStrFlag(c, refreshFlag)
		// Run forever unless `count` is also specified
		count = Infinity
	}

	if flagIsSet(c, countFlag) {
		count = parseIntFlag(c, countFlag)
		if count <= 0 {
			fmt.Printf("Warning: '%s' set to %d, but expected value >= 1. Assuming '%s' = %d.\n",
				countFlag.Name, count, countFlag.Name, countDefault)
			count = countDefault
		}
	}
}

// Querying information
func queryHandler(c *cli.Context) (err error) {
	if err = fillMap(ClusterURL); err != nil {
		return errorHandler(err)
	}
	updateLongRunVariables(c)

	var (
		useJSON    = flagIsSet(c, jsonFlag)
		baseParams = cliAPIParams(ClusterURL)
		daemonID   = c.Args().First()
		req        = c.Command.Name
	)

	switch req {
	case cmn.GetWhatSmap:
		err = daecluSmap(baseParams, daemonID, useJSON)
	case cmn.GetWhatStats:
		err = daecluStats(baseParams, daemonID, useJSON)
	case cmn.GetWhatDaemonStatus:
		err = daecluStatus(daemonID, useJSON)
	default:
		return fmt.Errorf(invalidCmdMsg, req)
	}
	return errorHandler(err)
}

// Displays smap of single daemon
func daecluSmap(baseParams *api.BaseParams, daemonID string, useJSON bool) error {
	newURL, err := daemonDirectURL(daemonID)
	if err != nil {
		return err
	}

	baseParams.URL = newURL
	body, err := api.GetClusterMap(baseParams)
	if err != nil {
		return err
	}
	return templates.DisplayOutput(body, templates.SmapTmpl, useJSON)
}

// Displays the stats of a daemon
func daecluStats(baseParams *api.BaseParams, daemonID string, useJSON bool) error {
	if res, ok := proxy[daemonID]; ok {
		return templates.DisplayOutput(res, templates.ProxyStatsTmpl, useJSON)
	} else if res, ok := target[daemonID]; ok {
		return templates.DisplayOutput(res, templates.TargetStatsTmpl, useJSON)
	} else if daemonID == "" {
		body, err := api.GetClusterStats(baseParams)
		if err != nil {
			return err
		}
		return templates.DisplayOutput(body, templates.StatsTmpl, useJSON)
	}
	return fmt.Errorf(invalidDaemonMsg, daemonID)
}

// Displays the status of the cluster or daemon
func daecluStatus(daemonID string, useJSON bool) (err error) {
	if res, proxyOK := proxy[daemonID]; proxyOK {
		err = templates.DisplayOutput(res, templates.ProxyInfoSingleTmpl, useJSON)
	} else if res, targetOK := target[daemonID]; targetOK {
		err = templates.DisplayOutput(res, templates.TargetInfoSingleTmpl, useJSON)
	} else if daemonID == cmn.Proxy {
		err = templates.DisplayOutput(proxy, templates.ProxyInfoTmpl, useJSON)
	} else if daemonID == cmn.Target {
		err = templates.DisplayOutput(target, templates.TargetInfoTmpl, useJSON)
	} else if daemonID == "" {
		if err := templates.DisplayOutput(proxy, templates.ProxyInfoTmpl, useJSON); err != nil {
			return err
		}
		err = templates.DisplayOutput(target, templates.TargetInfoTmpl, useJSON)
	} else {
		return fmt.Errorf(invalidDaemonMsg, daemonID)
	}

	return err
}
