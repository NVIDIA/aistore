// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import "github.com/urfave/cli"

var remClusterCmd = cli.Command{
	Name:  cmdShowRemoteAIS,
	Usage: "Show attached AIS clusters",
	Subcommands: []cli.Command{
		makeAlias(&showCmdRemote, &mkaliasOpts{newName: commandShow}),
	},
}
