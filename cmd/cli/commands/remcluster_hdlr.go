// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import "github.com/urfave/cli"

var remClusterCmd = cli.Command{
	Name:  subcmdShowRemoteAIS,
	Usage: "show attached AIS clusters",
	Subcommands: []cli.Command{
		makeAlias(showCmdRemoteAIS, "", true, commandShow), // alias for `ais show`
	},
}
