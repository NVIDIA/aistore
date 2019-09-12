// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands related to specific (not supported for other entities) bucket actions.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import "github.com/urfave/cli"

var (
	bucketSpecificCmdsFlags = map[string][]cli.Flag{
		commandSetCopies: append(
			baseBucketFlags,
			copiesFlag,
		),
	}

	bucketSpecificCmds = []cli.Command{
		{
			Name:         commandSetCopies,
			Usage:        "Configures buckets for n-way mirroring",
			ArgsUsage:    bucketArgumentText,
			Flags:        bucketSpecificCmdsFlags[commandSetCopies],
			Action:       setCopiesHandler,
			BashComplete: bucketList([]cli.BashCompleteFunc{}, false /*multiple*/),
		},
	}
)

func setCopiesHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		bucket     string
	)

	if bucket, err = bucketFromArgsOrEnv(c); err != nil {
		return
	}

	return configureNCopies(c, baseParams, bucket)
}
