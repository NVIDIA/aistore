// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles CLI commands that pertain to AIS objects.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"github.com/urfave/cli"
)

var (
	archCmdsFlags = map[string][]cli.Flag{
		commandCreate: {
			dryRunFlag,
			sourceBckFlag,
			templateFlag,
			listFlag,
			includeSrcBucketNameFlag,
			allowAppendToExistingFlag,
			continueOnErrorFlag,
		},
		subcmdAppend: {
			archpathFlag,
		},
		subcmdList: {
			objPropsFlag,
			allPropsFlag,
		},
	}

	archCmd = cli.Command{
		Name:  commandArch,
		Usage: "Create archive and append files to archive",
		Subcommands: []cli.Command{
			{
				Name:         commandCreate,
				Usage:        "create an archive",
				ArgsUsage:    "OBJECT_NAME",
				Flags:        archCmdsFlags[commandCreate],
				Action:       createArchMultiObjHandler,
				BashComplete: putPromoteObjectCompletions,
			},
			{
				Name:         subcmdAppend,
				Usage:        "create an archive",
				ArgsUsage:    "FILE_NAME OBJECT_NAME",
				Flags:        archCmdsFlags[subcmdAppend],
				Action:       putRegularObjHandler,
				BashComplete: putPromoteObjectCompletions,
			},
			{
				Name:         subcmdList,
				Usage:        "list archive content",
				ArgsUsage:    "OBJECT_NAME",
				Flags:        archCmdsFlags[subcmdList],
				Action:       listArchHandler,
				BashComplete: bucketCompletions(bcmplop{}),
			},
		},
	}
)

func listArchHandler(c *cli.Context) (err error) {
	bck, objName, err := parseBckObjectURI(c, c.Args().First(), true)
	if err != nil {
		return err
	}
	return listObjects(c, bck, objName, true /*list arch*/)
}
