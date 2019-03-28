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

	DaemonFlags = map[string][]cli.Flag{
		cmn.GetWhatSmap:         []cli.Flag{jsonFlag},
		cmn.GetWhatDaemonStatus: []cli.Flag{jsonFlag},
		cmn.GetWhatConfig:       []cli.Flag{jsonFlag},
		cmn.GetWhatStats:        []cli.Flag{jsonFlag},
		CommandList:             []cli.Flag{verboseFlag},
	}
)

// Display smap individual daemon
func ListAIS(c *cli.Context) error {
	err := fillMap(ClusterURL)
	if err != nil {
		return err
	}
	outputTemplate := templates.ListTmpl
	if c.Bool("verbose") {
		outputTemplate = templates.ListTmplVerbose
	}

	whichDaemon := c.Args().First()
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

func DaemonStatus(c *cli.Context) error {
	daemonID := c.Args().First()
	err := fillMap(ClusterURL)
	if err != nil {
		return err
	}

	if res, proxyOK := proxy[daemonID]; proxyOK {
		err = templates.DisplayOutput(res, templates.ProxyInfoSingleTmpl, c.Bool("json"))
	} else if res, targetOK := target[daemonID]; targetOK {
		err = templates.DisplayOutput(res, templates.TargetInfoSingleTmpl, c.Bool("json"))
	} else if daemonID == cmn.Proxy {
		err = templates.DisplayOutput(proxy, templates.ProxyInfoTmpl, c.Bool("json"))
	} else if daemonID == cmn.Target {
		err = templates.DisplayOutput(target, templates.TargetInfoTmpl, c.Bool("json"))
	} else if daemonID == "" {
		if err = templates.DisplayOutput(proxy, templates.ProxyInfoTmpl, c.Bool("json")); err != nil {
			return err
		}
		err = templates.DisplayOutput(target, templates.TargetInfoTmpl, c.Bool("json"))
	} else {
		return fmt.Errorf("%s is not a valid DAEMON_ID", daemonID)
	}

	return err
}

// Querying information
func GetQueryHandler(c *cli.Context) error {
	if err := fillMap(ClusterURL); err != nil {
		return err
	}
	baseParams := cliAPIParams(ClusterURL)
	daemonID := c.Args().First()
	req := c.Command.Name
	switch req {
	case cmn.GetWhatSmap:
		newURL, err := daemonDirectURL(daemonID)
		if err != nil {
			return err
		}

		baseParams.URL = newURL
		body, err := api.GetClusterMap(baseParams)
		if err != nil {
			return err
		}
		return templates.DisplayOutput(body, templates.SmapTmpl, c.Bool("json"))
	case cmn.GetWhatConfig:
		newURL, err := daemonDirectURL(daemonID)
		if err != nil {
			return err
		}
		baseParams.URL = newURL
		body, err := api.GetDaemonConfig(baseParams)
		if err != nil {
			return err
		}
		return templates.DisplayOutput(body, templates.ConfigTmpl, c.Bool("json"))
	case cmn.GetWhatStats:
		if res, ok := proxy[daemonID]; ok {
			return templates.DisplayOutput(res, templates.ProxyStatsTmpl, c.Bool("json"))
		} else if res, ok := target[daemonID]; ok {
			return templates.DisplayOutput(res, templates.TargetStatsTmpl, c.Bool("json"))
		} else if daemonID == "" {
			body, err := api.GetClusterStats(baseParams)
			if err != nil {
				return err
			}
			return templates.DisplayOutput(body, templates.StatsTmpl, c.Bool("json"))
		}
		return fmt.Errorf("%s is not a valid DAEMON_ID", daemonID)
	default:
		return fmt.Errorf("invalid command name '%s'", req)
	}
}
