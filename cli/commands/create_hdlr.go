// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that create entities in the cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"github.com/urfave/cli"
)

var (
	createCmdsFlags = map[string][]cli.Flag{
		subcmdCreateBucket: {},
	}

	createCmds = []cli.Command{
		{
			Name:  commandCreate,
			Usage: "creates entities in the cluster",
			Subcommands: []cli.Command{
				{
					Name:         subcmdCreateBucket,
					Usage:        "creates an ais buckets",
					ArgsUsage:    bucketsArgument,
					Flags:        createCmdsFlags[subcmdCreateBucket],
					Action:       createBucketHandler,
					BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, false /* multiple */, false /* separator */),
				},
			},
		},
	}
)

func createBucketHandler(c *cli.Context) (err error) {
	baseParams := cliAPIParams(ClusterURL)
	buckets, err := bucketsFromArgsOrEnv(c)
	if err != nil {
		return err
	}
	return createBuckets(c, baseParams, buckets)
}
