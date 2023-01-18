// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles specific bucket actions.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

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
			BashComplete: bucketCompletions(bcmplop{}),
		},
		{
			Name:         commandECEncode,
			Usage:        "erasure code a bucket",
			ArgsUsage:    bucketArgument,
			Flags:        storageSvcCmdsFlags[commandECEncode],
			Action:       ecEncodeHandler,
			BashComplete: bucketCompletions(bcmplop{}),
		},
	}
)

func setCopiesHandler(c *cli.Context) (err error) {
	var (
		bck cmn.Bck
		p   *cmn.BucketProps
	)
	if bck, err = parseBckURI(c, c.Args().First(), true /*require provider*/); err != nil {
		return
	}
	if p, err = headBucket(bck, false /* don't add */); err != nil {
		return
	}

	copies := c.Int(copiesFlag.Name)
	if p.Mirror.Copies == int64(copies) {
		if copies > 1 && p.Mirror.Enabled {
			fmt.Fprintf(c.App.Writer, "Bucket %q is already %d-way mirror, nothing to do\n", bck.DisplayName(), copies)
			return
		} else if copies < 2 {
			fmt.Fprintf(c.App.Writer, "Bucket %q is already configured with no redundancy, nothing to do\n", bck.DisplayName())
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
	if bck, err = parseBckURI(c, c.Args().First(), true /*require provider*/); err != nil {
		return
	}
	if p, err = headBucket(bck, false /* don't add */); err != nil {
		return
	}

	dataSlices := c.Int(fl1n(dataSlicesFlag.Name))
	paritySlices := c.Int(fl1n(paritySlicesFlag.Name))
	if p.EC.Enabled {
		// EC-encode is called automatically when EC is enabled. Changing
		// data or parity numbers on the fly is unsupported yet.
		fmt.Fprintf(c.App.Writer, "Bucket %q is already erasure-coded\n", bck.DisplayName())
		return
	}

	return ecEncode(c, bck, dataSlices, paritySlices)
}
