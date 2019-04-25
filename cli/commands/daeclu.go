// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that interact with the cluster
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"errors"
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

	daemonFlag      = cli.StringFlag{Name: cmn.Daemon, Usage: "daemon id"}
	daecluBaseFlags = []cli.Flag{watchFlag, refreshFlag}

	daecluFlags = map[string][]cli.Flag{
		cmn.GetWhatSmap:         {jsonFlag},
		cmn.GetWhatDaemonStatus: append([]cli.Flag{jsonFlag}, daecluBaseFlags...),
		cmn.GetWhatConfig:       {jsonFlag},
		cmn.GetWhatStats:        append([]cli.Flag{jsonFlag}, daecluBaseFlags...),
		commandList:             {verboseFlag},
		cmn.ActSetProps:         {daemonFlag},
	}

	daemonSetConfText = fmt.Sprintf("%s %s --daemon <value>", cliName, cmn.ActSetProps)

	// DaeCluCmds tracks available AIS API Information/Query Commands
	DaeCluCmds = []cli.Command{
		{
			Name:         cmn.GetWhatSmap,
			Usage:        "returns cluster map (Smap)",
			Action:       queryHandler,
			Flags:        daecluFlags[cmn.GetWhatSmap],
			BashComplete: daemonList,
		},
		{
			Name:         cmn.GetWhatDaemonStatus,
			Usage:        "returns status of AIS Daemon",
			Action:       queryHandler,
			Flags:        daecluFlags[cmn.GetWhatDaemonStatus],
			BashComplete: daemonList,
		},
		{
			Name:         cmn.GetWhatConfig,
			Usage:        "returns config of AIS daemon",
			Action:       queryHandler,
			Flags:        daecluFlags[cmn.GetWhatConfig],
			BashComplete: daemonList,
		},
		{
			Name:         cmn.GetWhatStats,
			Usage:        "returns stats of AIS daemon",
			Action:       queryHandler,
			Flags:        daecluFlags[cmn.GetWhatStats],
			BashComplete: daemonList,
		},
		{
			Name:    commandList,
			Aliases: []string{"ls"},
			Usage:   "returns list of AIS Daemons",
			Action:  queryHandler,
			Flags:   daecluFlags[commandList],
		},
		{
			Name:         cmn.ActSetConfig,
			Usage:        "sets configuration of daemon",
			UsageText:    daemonSetConfText,
			Flags:        daecluFlags[cmn.ActSetConfig],
			Action:       queryHandler,
			BashComplete: daemonList,
		},
	}
)

// Querying information
func queryHandler(c *cli.Context) (err error) {
	if err = fillMap(ClusterURL); err != nil {
		return errorHandler(err)
	}
	watch = flagIsSet(c, watchFlag)
	if flagIsSet(c, refreshFlag) {
		refreshRate = parseFlag(c, refreshFlag)
	}
	useJSON := flagIsSet(c, jsonFlag)

	baseParams := cliAPIParams(ClusterURL)
	daemonID := c.Args().First()
	req := c.Command.Name

	switch req {
	case cmn.GetWhatSmap:
		err = daecluSmap(baseParams, daemonID, useJSON)
	case cmn.GetWhatConfig:
		err = daecluConfig(baseParams, daemonID, useJSON)
	case cmn.GetWhatStats:
		err = daecluStats(baseParams, daemonID, useJSON)
	case cmn.GetWhatDaemonStatus:
		err = daecluStatus(daemonID, useJSON)
	case commandList:
		err = listAIS(c, daemonID)
	case cmn.ActSetConfig:
		err = setConfig(c, baseParams, daemonID)
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

// Displays the config of a daemon
func daecluConfig(baseParams *api.BaseParams, daemonID string, useJSON bool) error {
	newURL, err := daemonDirectURL(daemonID)
	if err != nil {
		return err
	}
	baseParams.URL = newURL
	body, err := api.GetDaemonConfig(baseParams)
	if err != nil {
		return err
	}
	return templates.DisplayOutput(body, templates.ConfigTmpl, useJSON)
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
		if err = templates.DisplayOutput(proxy, templates.ProxyInfoTmpl, useJSON); err != nil {
			return err
		}
		err = templates.DisplayOutput(target, templates.TargetInfoTmpl, useJSON)
	} else {
		return fmt.Errorf(invalidDaemonMsg, daemonID)
	}

	return err
}

// Display smap-like information of each individual daemon in the entire cluster
func listAIS(c *cli.Context, whichDaemon string) (err error) {
	outputTemplate := templates.ListTmpl
	if flagIsSet(c, verboseFlag) {
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

// Sets config of specific daemon or cluster
func setConfig(c *cli.Context, baseParams *api.BaseParams, daemonID string) (err error) {
	if c.NArg() < 2 {
		return errors.New("expecting at least one key-value pair")
	}

	nvs, err := makeKVS(c.Args().Tail(), "=")
	if err != nil {
		return
	}
	if daemonID == cmn.Cluster {
		if err = api.SetClusterConfig(baseParams, nvs); err != nil {
			return err
		}
		fmt.Printf("%d properties set for %s\n", c.NArg()-1, cmn.Cluster)
		return
	}

	daemonURL, err := daemonDirectURL(daemonID)
	if err != nil {
		return
	}

	baseParams.URL = daemonURL
	if err = api.SetDaemonConfig(baseParams, nvs); err != nil {
		return err
	}
	fmt.Printf("%d properties set for %q daemon\n", c.NArg()-1, daemonID)
	return err
}
