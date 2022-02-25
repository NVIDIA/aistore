// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import "github.com/urfave/cli"

var logCmd = cli.Command{
	Name:  commandLog,
	Usage: "show log",
	Subcommands: []cli.Command{
		makeAlias(showCmdLog, "", true, commandShow), // alias for `ais show`
	},
}
