// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that remove entities from the cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	removeCmdsFlags = map[string][]cli.Flag{
		subcmdRemoveBucket: {},
	}

	removeCmds = []cli.Command{
		{
			Name:  commandRemove,
			Usage: "removes entities from the cluster",
			Subcommands: []cli.Command{
				{
					Name:         subcmdRemoveBucket,
					Usage:        "removes ais buckets",
					ArgsUsage:    bucketsArgumentText,
					Flags:        removeCmdsFlags[subcmdRemoveBucket],
					Action:       removeBucketHandler,
					BashComplete: bucketList([]cli.BashCompleteFunc{}, true /* multiple */, cmn.AIS),
				},
			},
		},
	}
)

func removeBucketHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		buckets    []string
	)

	if buckets, err = bucketsFromArgsOrEnv(c); err != nil {
		return
	}

	return destroyBuckets(c, baseParams, buckets)
}
