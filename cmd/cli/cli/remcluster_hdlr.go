// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import "github.com/urfave/cli"

var remClusterCmd = cli.Command{
	Name:  cmdShowRemoteAIS,
	Usage: "show attached AIS clusters",
	Subcommands: []cli.Command{
		makeAlias(&showCmdRemoteAIS, &mkaliasOpts{newName: commandShow}),
	},
}
