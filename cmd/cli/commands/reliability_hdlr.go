// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles specific bucket actions.
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
	bucketSpecificCmdsFlags = map[string][]cli.Flag{
		commandSetCopies: {
			copiesFlag,
		},
		commandECEncode: {
			dataSlicesFlag,
			paritySlicesFlag,
		},
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
		p          *cmn.BucketProps
		objectName string
	)
	if bck, objectName, err = cmn.ParseBckObjectURI(c.Args().First()); err != nil {
		return
	}
	if objectName != "" {
		return objectNameArgumentNotSupported(c, objectName)
	}
	if bck, p, err = validateBucket(c, bck, "", false); err != nil {
		return
	}
	copies := c.Int(copiesFlag.Name)
	if p.Mirror.Copies == int64(copies) {
		if copies > 1 && p.Mirror.Enabled {
			fmt.Fprintf(c.App.Writer, "Bucket %q is already %d-way mirror, nothing to do\n", bck, copies)
			return
		} else if copies < 2 {
			fmt.Fprintf(c.App.Writer, "Bucket %q is already configured with no redundancy, nothing to do\n", bck)
			return
		}
	}
	return configureNCopies(c, bck, copies)
}

func ecEncodeHandler(c *cli.Context) (err error) {
	var (
		bck        cmn.Bck
		p          *cmn.BucketProps
		objectName string
	)
	if bck, objectName, err = cmn.ParseBckObjectURI(c.Args().First()); err != nil {
		return
	}
	if objectName != "" {
		return objectNameArgumentNotSupported(c, objectName)
	}
	if bck, p, err = validateBucket(c, bck, "", false); err != nil {
		return
	}
	dataSlices := c.Int(cleanFlag(dataSlicesFlag.Name))
	paritySlices := c.Int(cleanFlag(paritySlicesFlag.Name))
	if p.EC.Enabled {
		// EC-encode is called automatically when EC is enabled. Changing
		// data or parity numbers on the fly is unsupported yet.
		fmt.Fprintf(c.App.Writer, "Bucket %q is already erasure-coded\n", bck)
		return
	}

	return ecEncode(c, bck, dataSlices, paritySlices)
}
