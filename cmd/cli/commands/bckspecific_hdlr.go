// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles specific bucket actions.
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
		p          *cmn.BucketProps
		objectName string
	)
	if bck, objectName, err = parseBckObjectURI(c.Args().First()); err != nil {
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
		if copies > 1 {
			fmt.Fprintf(c.App.Writer, "Bucket %q is already %d-way mirror, nothing to do\n", bck, copies)
		} else {
			fmt.Fprintf(c.App.Writer, "Bucket %q is already configured with no redundancy, nothing to do\n", bck)
		}
		return
	}
	return configureNCopies(c, bck, copies)
}

func ecEncodeHandler(c *cli.Context) (err error) {
	var (
		bck        cmn.Bck
		p          *cmn.BucketProps
		objectName string
	)
	if bck, objectName, err = parseBckObjectURI(c.Args().First()); err != nil {
		return
	}
	if objectName != "" {
		return objectNameArgumentNotSupported(c, objectName)
	}
	if bck, p, err = validateBucket(c, bck, "", false); err != nil {
		return
	}
	if !p.EC.Enabled {
		fmt.Fprintf(c.App.Writer, "Bucket %q: erasure-coding is currently disabled (%+v)\n", bck, p.EC)
		fmt.Fprintln(c.App.Writer, "(use `set props` command to enable)")
		return
	}
	return ecEncode(c, bck)
}
