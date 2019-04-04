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

	daemonFlag = cli.StringFlag{Name: cmn.Daemon, Usage: "daemon id"}

	daecluFlags = map[string][]cli.Flag{
		cmn.GetWhatSmap:         []cli.Flag{jsonFlag},
		cmn.GetWhatDaemonStatus: []cli.Flag{jsonFlag},
		cmn.GetWhatConfig:       []cli.Flag{jsonFlag},
		cmn.GetWhatStats:        []cli.Flag{jsonFlag},
		commandList:             []cli.Flag{verboseFlag},
		cmn.ActSetProps:         []cli.Flag{daemonFlag},
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
		return err
	}
	baseParams := cliAPIParams(ClusterURL)
	daemonID := c.Args().First()
	req := c.Command.Name

	switch req {
	case cmn.GetWhatSmap:
		err = daecluSmap(c, baseParams, daemonID)
	case cmn.GetWhatConfig:
		err = daecluConfig(c, baseParams, daemonID)
	case cmn.GetWhatStats:
		err = daecluStats(c, baseParams, daemonID)
	case cmn.GetWhatDaemonStatus:
		err = daecluStatus(c, daemonID)
	case commandList:
		err = listAIS(c, daemonID)
	case cmn.ActSetConfig:
		err = setConfig(c, baseParams, daemonID)
	default:
		return fmt.Errorf(invalidCmdMsg, req)
	}
	return err
}

// Displays smap of single daemon
func daecluSmap(c *cli.Context, baseParams *api.BaseParams, daemonID string) error {
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
func daecluConfig(c *cli.Context, baseParams *api.BaseParams, daemonID string) error {
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
func daecluStats(c *cli.Context, baseParams *api.BaseParams, daemonID string) error {
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
func daecluStatus(c *cli.Context, daemonID string) (err error) {
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
	if flagIsSet(c, verboseFlag.Name) {
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
			return
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
		return
	}
	fmt.Printf("%d properties set for '%s' daemon\n", c.NArg()-1, daemonID)
	return err
}
