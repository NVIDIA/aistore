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
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/urfave/cli"
)

var (
	archCmdsFlags = map[string][]cli.Flag{
		commandCreate: { // TODO -- FIXME: remove 'create' verb
			dryRunFlag,
			templateFlag,
			listFlag,
			includeSrcBucketNameFlag,
			apndArchIfExistsFlag,
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
				Usage:        "create multi-object (" + strings.Join(archive.FileExtensions, ", ") + ") archive",
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
	a := archargs{apndIfExist: flagIsSet(c, apndArchIfExistsFlag)}
	if err = a.parse(c); err != nil {
		return
	}
	msg := cmn.ArchiveMsg{ToBck: a.dst.bck}
	{
		msg.ArchName = a.dst.oname
		msg.InclSrcBname = flagIsSet(c, includeSrcBucketNameFlag)
		msg.ContinueOnError = flagIsSet(c, continueOnErrorFlag)
		msg.AppendIfExists = a.apndIfExist
		msg.ListRange = a.rsrc.lr
	}
	_, err = api.ArchiveMultiObj(apiBP, a.rsrc.bck, msg)
	if err != nil {
		return err
	}
	for i := 0; i < 3; i++ {
		time.Sleep(time.Second)
		_, err = api.HeadObject(apiBP, a.dst.bck, a.dst.oname, apc.FltPresentNoProps)
		if err == nil {
			fmt.Fprintf(c.App.Writer, "Archived %q\n", a.dst.bck.Cname(a.dst.oname))
			return nil
		}
	}
	fmt.Fprintf(c.App.Writer, "Archiving %q ...\n", a.dst.bck.Cname(a.dst.oname))
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
