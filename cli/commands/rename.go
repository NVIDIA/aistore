// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that rename entities in the cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	renameCmdsFlags = map[string][]cli.Flag{
		subcmdRenameBucket: {},
	}

	renameCmds = []cli.Command{
		{
			Name:  commandRename,
			Usage: "renames entities in the cluster",
			Subcommands: []cli.Command{
				{
					Name:         subcmdRenameBucket,
					Usage:        "renames ais buckets",
					ArgsUsage:    bucketOldNewArgumentText,
					Flags:        renameCmdsFlags[subcmdRenameBucket],
					Action:       renameBucketHandler,
					BashComplete: oldAndNewBucketList([]cli.BashCompleteFunc{}, cmn.AIS),
				},
			},
		},
	}
)

func renameBucketHandler(c *cli.Context) (err error) {
	baseParams := cliAPIParams(ClusterURL)
	return renameBucket(c, baseParams)
}
