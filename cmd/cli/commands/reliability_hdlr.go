// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles specific bucket actions.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	storageSvcCmdsFlags = map[string][]cli.Flag{
		commandMirror: {
			copiesFlag,
		},
		commandECEncode: {
			dataSlicesFlag,
			paritySlicesFlag,
		},
	}

	storageSvcCmds = []cli.Command{
		{
			Name:         commandMirror,
			Usage:        "configure and start mirroring a bucket",
			ArgsUsage:    bucketArgument,
			Flags:        storageSvcCmdsFlags[commandMirror],
			Action:       setCopiesHandler,
			BashComplete: bucketCompletions(),
		},
		{
			Name:         commandECEncode,
			Usage:        "erasure code a bucket",
			ArgsUsage:    bucketArgument,
			Flags:        storageSvcCmdsFlags[commandECEncode],
			Action:       ecEncodeHandler,
			BashComplete: bucketCompletions(),
		},
	}
)

func setCopiesHandler(c *cli.Context) (err error) {
	var (
		bck cmn.Bck
		p   *cmn.BucketProps
	)
	if bck, err = parseBckURI(c, c.Args().First()); err != nil {
		return
	}
	if p, err = headBucket(bck); err != nil {
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
		bck cmn.Bck
		p   *cmn.BucketProps
	)
	if bck, err = parseBckURI(c, c.Args().First()); err != nil {
		return
	}
	if p, err = headBucket(bck); err != nil {
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
