// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import "github.com/urfave/cli"

var rebalanceCmd = cli.Command{
	Name:  commandRebalance,
	Usage: "show rebalance details",
	Subcommands: []cli.Command{
		makeAlias(showCmdRebalance, "", true, commandShow), // alias for `ais show`
	},
}
