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
		Usage: "monitor and manage AIS cluster storage: used and total capacity, add/remove mountpaths",
		Subcommands: []cli.Command{
			{
				Name:         subcmdStgSummary,
				Usage:        "show bucket sizes and percentages of used capacity on a per-bucket basis",
				ArgsUsage:    listCommandArgument,
				Flags:        storageCmdFlags[subcmdStgSummary],
				Action:       showBucketSizes,
				BashComplete: bucketCompletions(),
			},
			{
				Name:         subcmdStgValidate,
				Usage:        "check buckets for errors: detect misplaced objects and objects that have insufficient number of copies",
				ArgsUsage:    listCommandArgument,
				Flags:        storageCmdFlags[subcmdStgValidate],
				Action:       showObjectHealth,
				BashComplete: bucketCompletions(),
			},
			mpathCmd,
		},
	}
)
