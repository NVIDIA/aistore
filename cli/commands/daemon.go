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

	daemonFlags = map[string][]cli.Flag{
		cmn.GetWhatSmap:         []cli.Flag{jsonFlag},
		cmn.GetWhatDaemonStatus: []cli.Flag{jsonFlag},
		cmn.GetWhatConfig:       []cli.Flag{jsonFlag},
		cmn.GetWhatStats:        []cli.Flag{jsonFlag},
		commandList:             []cli.Flag{verboseFlag},
	}

	// AIS API Query Commands
	DaemonCmds = []cli.Command{
		{
			Name:         cmn.GetWhatSmap,
			Usage:        "returns cluster map (Smap)",
			Action:       queryHandler,
			Flags:        daemonFlags[cmn.GetWhatSmap],
			BashComplete: daemonList,
		},
		{
			Name:         cmn.GetWhatDaemonStatus,
			Usage:        "returns status of AIS Daemon",
			Action:       queryHandler,
			Flags:        daemonFlags[cmn.GetWhatDaemonStatus],
			BashComplete: daemonList,
		},
		{
			Name:         cmn.GetWhatConfig,
			Usage:        "returns config of AIS daemon",
			Action:       queryHandler,
			Flags:        daemonFlags[cmn.GetWhatConfig],
			BashComplete: daemonList,
		},
		{
			Name:         cmn.GetWhatStats,
			Usage:        "returns stats of AIS daemon",
			Action:       queryHandler,
			Flags:        daemonFlags[cmn.GetWhatStats],
			BashComplete: daemonList,
		},
		{
			Name:    commandList,
			Aliases: []string{"ls"},
			Usage:   "returns list of AIS Daemons",
			Action:  queryHandler,
			Flags:   daemonFlags[commandList],
		},
	}
)

// Querying information
func queryHandler(c *cli.Context) (err error) {
	if err = fillMap(ClusterURL); err != nil {
		return err
	}
	baseParams := cliAPIParams(ClusterURL)
	daemonID := c.Args().First()
	req := c.Command.Name

	switch req {
	case cmn.GetWhatSmap:
		err = daemonSmap(c, baseParams, daemonID)
	case cmn.GetWhatConfig:
		err = daemonConfig(c, baseParams, daemonID)
	case cmn.GetWhatStats:
		err = daemonStats(c, baseParams, daemonID)
	case cmn.GetWhatDaemonStatus:
		err = daemonStatus(c, daemonID)
	case commandList:
		err = listAIS(c, daemonID)
	default:
		return fmt.Errorf(invalidCmdMsg, req)
	}
	return err
}

// Displays smap of single daemon
func daemonSmap(c *cli.Context, baseParams *api.BaseParams, daemonID string) error {
	newURL, err := daemonDirectURL(daemonID)
	if err != nil {
		return err
	}

	baseParams.URL = newURL
	body, err := api.GetClusterMap(baseParams)
	if err != nil {
		return err
	}
	return templates.DisplayOutput(body, templates.SmapTmpl, flagIsSet(c, jsonFlag.Name))
}

// Displays the config of a daemon
func daemonConfig(c *cli.Context, baseParams *api.BaseParams, daemonID string) error {
	newURL, err := daemonDirectURL(daemonID)
	if err != nil {
		return err
	}
	baseParams.URL = newURL
	body, err := api.GetDaemonConfig(baseParams)
	if err != nil {
		return err
	}
	return templates.DisplayOutput(body, templates.ConfigTmpl, flagIsSet(c, jsonFlag.Name))
}

// Displays the stats of a daemon
func daemonStats(c *cli.Context, baseParams *api.BaseParams, daemonID string) error {
	if res, ok := proxy[daemonID]; ok {
		return templates.DisplayOutput(res, templates.ProxyStatsTmpl, flagIsSet(c, jsonFlag.Name))
	} else if res, ok := target[daemonID]; ok {
		return templates.DisplayOutput(res, templates.TargetStatsTmpl, flagIsSet(c, jsonFlag.Name))
	} else if daemonID == "" {
		body, err := api.GetClusterStats(baseParams)
		if err != nil {
			return err
		}
		return templates.DisplayOutput(body, templates.StatsTmpl, flagIsSet(c, jsonFlag.Name))
	}
	return fmt.Errorf(invalidDaemonMsg, daemonID)
}

// Displays the status of the cluster or daemon
func daemonStatus(c *cli.Context, daemonID string) (err error) {
	if res, proxyOK := proxy[daemonID]; proxyOK {
		err = templates.DisplayOutput(res, templates.ProxyInfoSingleTmpl, flagIsSet(c, jsonFlag.Name))
	} else if res, targetOK := target[daemonID]; targetOK {
		err = templates.DisplayOutput(res, templates.TargetInfoSingleTmpl, flagIsSet(c, jsonFlag.Name))
	} else if daemonID == cmn.Proxy {
		err = templates.DisplayOutput(proxy, templates.ProxyInfoTmpl, flagIsSet(c, jsonFlag.Name))
	} else if daemonID == cmn.Target {
		err = templates.DisplayOutput(target, templates.TargetInfoTmpl, flagIsSet(c, jsonFlag.Name))
	} else if daemonID == "" {
		if err = templates.DisplayOutput(proxy, templates.ProxyInfoTmpl, flagIsSet(c, jsonFlag.Name)); err != nil {
			return err
		}
		err = templates.DisplayOutput(target, templates.TargetInfoTmpl, flagIsSet(c, jsonFlag.Name))
	} else {
		return fmt.Errorf(invalidDaemonMsg, daemonID)
	}

	return err
}

// Display smap-like information of each individual daemon in the entire cluster
func listAIS(c *cli.Context, whichDaemon string) (err error) {
	outputTemplate := templates.ListTmpl
	if c.Bool(verboseFlag.Name) {
		outputTemplate = templates.ListTmplVerbose
	}

	switch whichDaemon {
	case cmn.Proxy:
		err = templates.DisplayOutput(proxy, outputTemplate)
	case cmn.Target:
		err = templates.DisplayOutput(target, outputTemplate)
	case "", "all":
		err = templates.DisplayOutput(proxy, outputTemplate)
		if err != nil {
			return err
		}
		err = templates.DisplayOutput(target, outputTemplate)
	default:
		return fmt.Errorf("list usage: list ['%s' or '%s' or 'all']", cmn.Proxy, cmn.Target)
	}
	return err
}
