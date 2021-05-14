// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"github.com/urfave/cli"
)

var (
	storageCmdFlags = map[string][]cli.Flag{
		subcmdStgSummary: {
			cachedFlag,
			fastFlag,
			verboseFlag,
		},
		subcmdStgValidate:  {},
		subcmdStgMountpath: {},
	}

	storageCmd = cli.Command{
		Name:  commandStorage,
		Usage: "monitor and manage AIS cluster storage: disk space usage, add/remove mountpath etc",
		Subcommands: []cli.Command{
			{
				Name:         subcmdStgSummary,
				Usage:        "fetch and display storage summary: bucket sizes, capacity percentage used by each bucket",
				ArgsUsage:    optionalBucketArgument,
				Flags:        storageCmdFlags[subcmdStgSummary],
				Action:       showBucketSizes,
				BashComplete: bucketCompletions(),
			},
			{
				Name:         subcmdStgValidate,
				Usage:        "check buckets for errors: detect misplaced or incomplete objects",
				ArgsUsage:    optionalBucketArgument,
				Flags:        storageCmdFlags[subcmdStgValidate],
				Action:       showObjectHealth,
				BashComplete: bucketCompletions(),
			},
			mpathCmd,
		},
	}
)
