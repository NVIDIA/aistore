// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles commands that list cluster metadata information.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"strings"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	listCmdFlags = []cli.Flag{
		regexFlag,
		templateFlag,
		prefixFlag,
		pageSizeFlag,
		objPropsFlag,
		objLimitFlag,
		showUnmatchedFlag,
		allItemsFlag,
		fastFlag,
		noHeaderFlag,
		pagedFlag,
		maxPagesFlag,
		markerFlag,
		cachedFlag,
	}

	listCmds = []cli.Command{
		{
			Name:         commandList,
			Usage:        "list buckets and objects",
			Action:       defaultListHandler,
			ArgsUsage:    listCommandArgument,
			Flags:        listCmdFlags,
			BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, false /*multiple*/, false /*separator*/),
		},
	}
)

func defaultListHandler(c *cli.Context) (err error) {
	var (
		bck     cmn.Bck
		objName string
	)
	if bck, objName, err = parseBckObjectURI(c.Args().First()); err != nil {
		return
	}
	if objName != "" {
		return objectNameArgumentNotSupported(c, objName)
	}

	if bck, err = validateBucket(c, bck, "ls", true); err != nil {
		return
	}

	if bck.Name == "" {
		return listBucketNames(c, bck)
	}

	bck.Name = strings.TrimSuffix(bck.Name, "/")
	return listBucketObj(c, bck)
}
