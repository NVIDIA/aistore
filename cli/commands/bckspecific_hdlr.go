// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles specific bucket actions.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	bucketSpecificCmdsFlags = map[string][]cli.Flag{
		commandSetCopies: {
			copiesFlag,
		},
		commandECEncode: {},
	}

	bucketSpecificCmds = []cli.Command{
		{
			Name:         commandSetCopies,
			Usage:        "configure a bucket for n-way mirroring",
			ArgsUsage:    bucketArgument,
			Flags:        bucketSpecificCmdsFlags[commandSetCopies],
			Action:       setCopiesHandler,
			BashComplete: bucketCompletions(),
		},
		{
			Name:         commandECEncode,
			Usage:        "make all objects in a bucket erasure coded",
			ArgsUsage:    bucketArgument,
			Flags:        bucketSpecificCmdsFlags[commandECEncode],
			Action:       ecEncodeHandler,
			BashComplete: bucketCompletions(),
		},
	}
)

func setCopiesHandler(c *cli.Context) (err error) {
	var (
		bck        cmn.Bck
		objectName string
	)
	if bck, objectName, err = parseBckObjectURI(c.Args().First()); err != nil {
		return
	}
	if objectName != "" {
		return objectNameArgumentNotSupported(c, objectName)
	}
	if bck, err = validateBucket(c, bck, "", false); err != nil {
		return
	}
	return configureNCopies(c, bck)
}

func ecEncodeHandler(c *cli.Context) (err error) {
	var (
		bck        cmn.Bck
		objectName string
	)
	if bck, objectName, err = parseBckObjectURI(c.Args().First()); err != nil {
		return
	}
	if objectName != "" {
		return objectNameArgumentNotSupported(c, objectName)
	}
	if bck, err = validateBucket(c, bck, "", false); err != nil {
		return
	}
	return ecEncode(c, bck)
}
