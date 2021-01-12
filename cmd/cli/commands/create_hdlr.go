// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles CLI commands that create AIS buckets.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	createCmdsFlags = map[string][]cli.Flag{
		subcmdCreateBucket: {
			ignoreErrorFlag,
			bucketPropsFlag,
		},
	}

	createCmds = []cli.Command{
		{
			Name:  commandCreate,
			Usage: "create ais buckets",
			Subcommands: []cli.Command{
				{
					Name:      subcmdCreateBucket,
					Usage:     "create ais buckets",
					ArgsUsage: bucketsArgument,
					Flags:     createCmdsFlags[subcmdCreateBucket],
					Action:    createBucketHandler,
				},
			},
		},
	}
)

func createBucketHandler(c *cli.Context) (err error) {
	var props *cmn.BucketPropsToUpdate
	if flagIsSet(c, bucketPropsFlag) {
		propSingleBck, err := parseBckPropsFromContext(c)
		if err != nil {
			return err
		}
		props = &propSingleBck
	}
	buckets, err := bucketsFromArgsOrEnv(c)
	if err != nil {
		return err
	}
	for _, bck := range buckets {
		if props != nil {
			err = createBucket(c, bck, *props)
		} else {
			err = createBucket(c, bck)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
