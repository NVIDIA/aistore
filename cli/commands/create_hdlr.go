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
			Usage: "create entities in the cluster",
			Subcommands: []cli.Command{
				{
					Name:         subcmdCreateBucket,
					Usage:        "create ais buckets",
					ArgsUsage:    bucketsArgument,
					Flags:        createCmdsFlags[subcmdCreateBucket],
					Action:       createBucketHandler,
					BashComplete: flagCompletions,
				},
			},
		},
	}
)

func createBucketHandler(c *cli.Context) (err error) {
	buckets, err := bucketsFromArgsOrEnv(c)
	if err != nil {
		return err
	}
	if err := validateOnlyLocalBuckets(buckets); err != nil {
		return err
	}
	return createBuckets(c, buckets)
}
