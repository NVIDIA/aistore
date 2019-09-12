// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that evict buckets and objects.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	evictCmdsFlags = map[string][]cli.Flag{
		subcmdEvictBucket: {},
	}

	evictCmds = []cli.Command{
		{
			Name:  commandEvict,
			Usage: "evicts cloud buckets and objects",
			Subcommands: []cli.Command{
				{
					Name:         subcmdEvictBucket,
					Usage:        "evicts cloud buckets",
					ArgsUsage:    bucketArgumentText,
					Flags:        evictCmdsFlags[subcmdEvictBucket],
					Action:       evictBucketHandler,
					BashComplete: bucketList([]cli.BashCompleteFunc{}, false /* multiple */, cmn.Cloud),
				},
			},
		},
	}
)

func evictBucketHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		bucket     string
	)

	if bucket, err = bucketFromArgsOrEnv(c); err != nil {
		return
	}

	return evictBucket(c, baseParams, bucket)
}
