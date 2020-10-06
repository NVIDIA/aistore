// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles commands that copy buckets and objects in the cluster.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	copyCmdsFlags = map[string][]cli.Flag{
		subcmdCopyBucket: {
			cpBckDryRunFlag,
			cpBckPrefixFlag,
		},
	}

	copyCmds = []cli.Command{
		{
			Name:  commandCopy,
			Usage: "copy buckets and objects in the cluster",
			Subcommands: []cli.Command{
				{
					Name:         subcmdCopyBucket,
					Usage:        "copy ais buckets",
					ArgsUsage:    bucketOldNewArgument,
					Flags:        copyCmdsFlags[subcmdCopyBucket],
					Action:       copyBucketHandler,
					BashComplete: oldAndNewBucketCompletions([]cli.BashCompleteFunc{}, false /* separator */),
				},
			},
		},
	}
)

func copyBucketHandler(c *cli.Context) (err error) {
	bucketName, newBucketName, err := getOldNewBucketName(c)
	if err != nil {
		return err
	}
	fromBck, err := parseBckURI(c, bucketName)
	if err != nil {
		return err
	}
	toBck, err := parseBckURI(c, newBucketName)
	if err != nil {
		return err
	}

	if fromBck.Equal(toBck) {
		return fmt.Errorf("cannot copy bucket %q onto itself", fromBck)
	}

	fromBck.Provider, toBck.Provider = cmn.ProviderAIS, cmn.ProviderAIS
	msg := &cmn.CopyBckMsg{
		Prefix: parseStrFlag(c, cpBckPrefixFlag),
		DryRun: flagIsSet(c, cpBckDryRunFlag),
	}

	if msg.DryRun {
		// TODO: once IC is integrated with copy-bck stats, show something more relevant, like stream of object names
		// with destination which they would have been copied to. Then additionally, make output consistent with etl
		// dry-run output.
		fmt.Fprintln(c.App.Writer, dryRunHeader+" "+dryRunExplanation)
	}

	return copyBucket(c, fromBck, toBck, msg)
}
