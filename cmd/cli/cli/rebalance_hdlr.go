// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import "github.com/urfave/cli"

var rebalanceCmd = cli.Command{
	Name:  commandRebalance,
	Usage: "show rebalance details",
	Subcommands: []cli.Command{
		makeAlias(showCmdRebalance, "", true, commandShow), // alias for `ais show`
	},
}
