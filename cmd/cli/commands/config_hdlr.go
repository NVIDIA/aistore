// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/urfave/cli"
)

var (
	configCmdsFlags = map[string][]cli.Flag{
		subcmdCluster: {transientFlag},
		subcmdNode:    {transientFlag},
	}

	configCmd = cli.Command{
		Name:  commandConfig,
		Usage: "set local/global AIS cluster configurations",
		Subcommands: []cli.Command{
			makeAlias(showCmdConfig, "", true, commandShow), // alias for `ais show`
			{
				Name:         subcmdCluster,
				Usage:        "configure cluster",
				ArgsUsage:    keyValuePairsArgument,
				Flags:        configCmdsFlags[subcmdCluster],
				Action:       setConfigHandler,
				BashComplete: suggestUpdatableConfig,
			},
			{
				Name:         subcmdNode,
				Usage:        "configure a specific node",
				ArgsUsage:    nodeConfigArgument,
				Flags:        configCmdsFlags[subcmdNode],
				Action:       setConfigHandler,
				BashComplete: cluConfigCompletions,
			},
			{
				Name:         subcmdReset,
				Usage:        "reset to cluster configuration on all nodes or a specific node",
				ArgsUsage:    optionalDaemonIDArgument,
				Action:       resetConfigHandler,
				BashComplete: daemonCompletions(completeAllDaemons),
			},
			settingsCmd,
		},
	}
)

func setConfigHandler(c *cli.Context) (err error) {
	if _, err = fillMap(); err != nil {
		return
	}
	return setConfig(c)
}

func resetConfigHandler(c *cli.Context) (err error) {
	daemonID := argDaemonID(c)
	if daemonID == "" {
		if err := api.ResetClusterConfig(defaultAPIParams); err != nil {
			return err
		}

		fmt.Fprintf(c.App.Writer, "config successfully reset for all nodes\n")
		return nil
	}

	if err := api.ResetDaemonConfig(defaultAPIParams, daemonID); err != nil {
		return err
	}

	fmt.Fprintf(c.App.Writer, "config for node %q successfully reset\n", daemonID)
	return nil
}
