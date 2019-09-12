// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that copy buckets and objects in the cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	copyCmdsFlags = map[string][]cli.Flag{
		subcmdCopyBucket: {},
	}

	copyCmds = []cli.Command{
		{
			Name:  commandCopy,
			Usage: "copies buckets and objects in the cluster",
			Subcommands: []cli.Command{
				{
					Name:         subcmdCopyBucket,
					Usage:        "copies ais buckets",
					ArgsUsage:    bucketOldNewArgumentText,
					Flags:        copyCmdsFlags[subcmdCopyBucket],
					Action:       copyBucketHandler,
					BashComplete: oldAndNewBucketList([]cli.BashCompleteFunc{}, cmn.AIS),
				},
			},
		},
	}
)

func copyBucketHandler(c *cli.Context) (err error) {
	baseParams := cliAPIParams(ClusterURL)
	return copyBucket(c, baseParams)
}
