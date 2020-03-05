// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that list cluster metadata information.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"strings"

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
	bck, objName := parseBckObjectURI(c.Args().First())
	if objName != "" {
		return objectNameArgumentNotSupported(c, objName)
	}

	if bck.Name == "" {
		return listBucketNames(c, bck)
	}

	bck.Name = strings.TrimSuffix(bck.Name, "/")
	return listBucketObj(c, bck)
}
