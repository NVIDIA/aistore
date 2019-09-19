// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands related to specific (not supported for other entities) bucket actions.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"os"

	"github.com/urfave/cli"
)

var (
	bucketSpecificCmdsFlags = map[string][]cli.Flag{
		commandSetCopies: {
			providerFlag,
			copiesFlag,
		},
		commandShow: {
			providerFlag,
		},
	}

	bucketSpecificCmds = []cli.Command{
		{
			Name:         commandSetCopies,
			Usage:        "configures a bucket for n-way mirroring",
			ArgsUsage:    bucketArgument,
			Flags:        bucketSpecificCmdsFlags[commandSetCopies],
			Action:       setCopiesHandler,
			BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, false /* multiple */, false /* separator */),
		},
		{
			Name:         commandShow,
			Usage:        "show details about a bucket",
			ArgsUsage:    bucketArgument,
			Flags:        bucketSpecificCmdsFlags[commandShow],
			Action:       showHandler,
			BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, false /* multiple */, false /* separator */),
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

func showHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		provider   string
		bucket     string
	)

	if provider, err = bucketProvider(c); err != nil {
		return
	}

	bucket = c.Args().First()
	if bucket == "" {
		bucket, _ = os.LookupEnv(aisBucketEnvVar)
	}

	return bucketDetails(c, baseParams, bucket, provider)
}
