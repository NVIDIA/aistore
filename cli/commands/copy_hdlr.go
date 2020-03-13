// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that copy buckets and objects in the cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	copyCmdsFlags = map[string][]cli.Flag{
		subcmdCopyBucket: {},
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
					BashComplete: oldAndNewBucketCompletions([]cli.BashCompleteFunc{}, false /* separator */, cmn.ProviderAIS),
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
	fromBck, objName, err := parseBckObjectURI(bucketName)
	if err != nil {
		return err
	}
	toBck, newObjName, err := parseBckObjectURI(newBucketName)
	if err != nil {
		return err
	}

	if cmn.IsProviderCloud(fromBck, true) || cmn.IsProviderCloud(toBck, true) {
		return fmt.Errorf("copying of cloud buckets not supported")
	}
	if objName != "" {
		return objectNameArgumentNotSupported(c, objName)
	}
	if newObjName != "" {
		return objectNameArgumentNotSupported(c, objName)
	}

	fromBck.Provider, toBck.Provider = cmn.ProviderAIS, cmn.ProviderAIS

	return copyBucket(c, fromBck, toBck)
}
