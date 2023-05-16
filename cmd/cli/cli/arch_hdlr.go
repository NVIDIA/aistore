// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles CLI commands that pertain to AIS objects.
/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/urfave/cli"
)

var (
	archCmdsFlags = map[string][]cli.Flag{
		commandCreate: {
			dryRunFlag,
			templateFlag,
			listFlag,
			includeSrcBucketNameFlag,
			appendArch1Flag, // NOTE: multi-object bck=>bck APPEND - not to confuse with cmdAppend (files)
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
				ArgsUsage:    bucketSrcArgument + " " + bucketDstArgument + "/OBJECT_NAME",
				Flags:        archCmdsFlags[commandCreate],
				Action:       archMultiObjHandler,
				BashComplete: putPromoteObjectCompletions,
			},
			{
				Name: cmdAppend,
				Usage: "append file to an existing tar-formatted object (aka \"shard\"), e.g.:\n" +
					indent4 + "'append src-filename bucket/shard-00123.tar --archpath dst-name-in-archive'",
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

func archMultiObjHandler(c *cli.Context) (err error) {
	// validate
	if c.NArg() < 1 {
		return missingArgumentsError(c, "destination object in the form "+optionalObjectsArgument)
	}
	return archMultiObj(c, flagIsSet(c, appendArch1Flag))
}
func archMultiObj(c *cli.Context, doAppend bool) (err error) {
	var (
		bckTo, bckFrom cmn.Bck
		objName        string
	)
	if !flagIsSet(c, listFlag) && !flagIsSet(c, templateFlag) {
		return missingArgumentsError(c,
			fmt.Sprintf("either a list of object names via %s or selection template (%s)",
				flprn(listFlag), flprn(templateFlag)))
	}
	if flagIsSet(c, listFlag) && flagIsSet(c, templateFlag) {
		return incorrectUsageMsg(c, fmt.Sprintf("%s and %s options are mutually exclusive",
			flprn(listFlag), flprn(templateFlag)))
	}
	if bckFrom, err = parseBckURI(c, c.Args().Get(0), false); err != nil {
		return
	}
	if bckTo, objName, err = parseBckObjURI(c, c.Args().Get(1), false /*optional objName*/); err != nil {
		if objName == "" {
			return fmt.Errorf("destination object name (in %q) cannot be empty", c.Args().Get(1))
		}
		bckFrom = bckTo
	}

	// api
	var (
		template = parseStrFlag(c, templateFlag)
		list     = parseStrFlag(c, listFlag)
		msg      = cmn.ArchiveMsg{ToBck: bckTo}
	)
	{
		msg.ArchName = objName
		msg.InclSrcBname = flagIsSet(c, includeSrcBucketNameFlag)
		msg.AppendToExisting = doAppend
		msg.ContinueOnError = flagIsSet(c, continueOnErrorFlag)
		if list != "" {
			msg.ListRange.ObjNames = splitCsv(list)
		} else {
			msg.ListRange.Template = template
		}
	}
	_, err = api.CreateArchMultiObj(apiBP, bckFrom, msg)
	if err != nil {
		return err
	}
	for i := 0; i < 3; i++ {
		time.Sleep(time.Second)
		_, err = api.HeadObject(apiBP, bckTo, objName, apc.FltPresentNoProps)
		if err == nil {
			fmt.Fprintf(c.App.Writer, "Created archive %q\n", bckTo.Cname(objName))
			return nil
		}
	}
	fmt.Fprintf(c.App.Writer, "Creating archive %q ...\n", bckTo.Cname(objName))
	return nil
}

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
		return missingArgSimple("destination archive name in the form " + objectArgument)
	}
	uri := c.Args().Get(1)
	bck, objName, err := parseBckObjURI(c, uri, false)
	if err != nil {
		return err
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
	bck, objName, err := parseBckObjURI(c, c.Args().Get(0), true)
	if err != nil {
		return err
	}
	return listObjects(c, bck, objName, true /*list arch*/)
}
