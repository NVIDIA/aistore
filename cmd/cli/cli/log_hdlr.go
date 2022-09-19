// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import "github.com/urfave/cli"

var logCmd = cli.Command{
	Name:  commandLog,
	Usage: "show log",
	Subcommands: []cli.Command{
		makeAlias(showCmdLog, "", true, commandShow), // alias for `ais show`
	},
}
