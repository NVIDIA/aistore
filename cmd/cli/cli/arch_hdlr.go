// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles CLI commands that pertain to AIS objects.
/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
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
		cmdAppend: {
			archpathRequiredFlag,
		},
		cmdList: {
			objPropsFlag,
			allPropsFlag,
		},
	}

	archCmd = cli.Command{
		Name:  commandArch,
		Usage: "Create multi-object archive, append files to an existing archive",
		Subcommands: []cli.Command{
			{
				Name:         commandCreate,
				Usage:        "create multi-object (" + strings.Join(cos.ArchExtensions, ", ") + ") archive",
				ArgsUsage:    objectArgument,
				Flags:        archCmdsFlags[commandCreate],
				Action:       createArchMultiObjHandler,
				BashComplete: putPromoteObjectCompletions,
			},
			{
				Name: cmdAppend,
				Usage: "append file to an existing .tar archive, e.g.: " +
					"'append src-filename bucket/shard.tar --archpath dst-name'",
				ArgsUsage:    appendToArchArgument,
				Flags:        archCmdsFlags[cmdAppend],
				Action:       appendArchHandler,
				BashComplete: putPromoteObjectCompletions,
			},
			{
				Name:         cmdList,
				Usage:        "list archived content",
				ArgsUsage:    objectArgument,
				Flags:        archCmdsFlags[cmdList],
				Action:       listArchHandler,
				BashComplete: bucketCompletions(bcmplop{}),
			},
		},
	}
)

func appendArchHandler(c *cli.Context) error {
	// src
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	fileName := c.Args().Get(0)
	path, err := absPath(fileName)
	if err != nil {
		return err
	}
	finfo, err := os.Stat(path)
	if err != nil {
		return err
	}
	if finfo.IsDir() {
		return fmt.Errorf("%q is a directory", fileName)
	}

	// dst
	if c.NArg() < 2 {
		return missingArgSimple("destination archive name in the form " + optionalObjectsArgument)
	}
	uri := c.Args().Get(1)
	bck, objName, err := parseBckObjectURI(c, uri, true /*optional objName*/)
	if err != nil {
		return err
	}
	if objName == "" {
		// [CONVENTION]: if objName is not provided
		// we use the filename as the destination object name
		objName = filepath.Base(path)
	}

	if flagIsSet(c, dryRunFlag) {
		return putDryRun(c, bck, objName, fileName)
	}
	archPath := parseStrFlag(c, archpathRequiredFlag)
	if err := appendToArch(c, bck, objName, path, archPath, finfo); err != nil {
		return err
	}
	actionDone(c, fmt.Sprintf("APPEND %q to %s as %s\n", fileName, bck.Cname(objName), archPath))
	return nil
}

func listArchHandler(c *cli.Context) error {
	bck, objName, err := parseBckObjectURI(c, c.Args().First(), true)
	if err != nil {
		return err
	}
	return listObjects(c, bck, objName, true /*list arch*/)
}
