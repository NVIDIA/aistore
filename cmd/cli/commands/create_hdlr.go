// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles CLI commands that create AIS buckets.
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
			Usage: "create ais buckets",
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
	// TODO: remote AIS cluster: extend existing API with create-bucket, et al. (v4.x)
	if err := validateLocalBuckets(buckets, "creating"); err != nil {
		return err
	}
	return createBuckets(c, buckets)
}
