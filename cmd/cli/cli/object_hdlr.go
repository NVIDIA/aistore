// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles CLI commands that pertain to AIS objects.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	objectCmdsFlags = map[string][]cli.Flag{
		commandRemove: append(
			baseLstRngFlags,
			rmRfFlag,
			verboseFlag,
			yesFlag,
		),
		commandRename: {},
		commandGet: {
			offsetFlag,
			lengthFlag,
			archpathOptionalFlag,
			cksumFlag,
			checkObjCachedFlag,
		},

		commandPut: append(
			supportedCksumFlags,
			chunkSizeFlag,
			concurrencyFlag,
			dryRunFlag,
			progressBarFlag,
			recursiveFlag,
			refreshFlag,
			verboseFlag,
			yesFlag,
			computeCksumFlag,
			sourceBckFlag,
			templateFlag,
			listFlag,
			includeSrcBucketNameFlag,
			allowAppendToExistingFlag,
			continueOnErrorFlag,
			createArchFlag,
			archpathOptionalFlag,
			skipVerCksumFlag,
		),
		commandSetCustom: {
			setNewCustomMDFlag,
		},
		commandPromote: {
			recursiveFlag,
			overwriteFlag,
			notFshareFlag,
			deleteSrcFlag,
			targetIDFlag,
			verboseFlag,
		},
		commandConcat: {
			recursiveFlag,
			progressBarFlag,
		},
		commandCat: {
			offsetFlag,
			lengthFlag,
			archpathOptionalFlag,
			cksumFlag,
			forceFlag,
		},
	}

	// define separately to allow for aliasing (see alias_hdlr.go)
	objectCmdGet = cli.Command{
		Name:         commandGet,
		Usage:        "get object",
		ArgsUsage:    getObjectArgument,
		Flags:        objectCmdsFlags[commandGet],
		Action:       getHandler,
		BashComplete: bucketCompletions(bcmplop{separator: true}),
	}

	objectCmdPut = cli.Command{
		Name:         commandPut,
		Usage:        "put object(s)",
		ArgsUsage:    putPromoteObjectArgument,
		Flags:        objectCmdsFlags[commandPut],
		Action:       putHandler,
		BashComplete: putPromoteObjectCompletions,
	}

	objectCmdSetCustom = cli.Command{
		Name:      commandSetCustom,
		Usage:     "set object's custom properties",
		ArgsUsage: setCustomArgument,
		Flags:     objectCmdsFlags[commandSetCustom],
		Action:    setCustomPropsHandler,
	}

	objectCmd = cli.Command{
		Name:  commandObject,
		Usage: "put, get, list, rename, remove, and other operations on objects",
		Subcommands: []cli.Command{
			objectCmdGet,
			bucketsObjectsCmdList,
			objectCmdPut,
			objectCmdSetCustom,
			bucketObjCmdEvict,
			makeAlias(showCmdObject, "", true, commandShow), // alias for `ais show`
			{
				Name:         commandRename,
				Usage:        "move/rename object",
				ArgsUsage:    renameObjectArgument,
				Flags:        objectCmdsFlags[commandRename],
				Action:       mvObjectHandler,
				BashComplete: bucketCompletions(bcmplop{multiple: true, separator: true}),
			},
			{
				Name:         commandRemove,
				Usage:        "remove object(s) from the specified bucket",
				ArgsUsage:    optionalObjectsArgument,
				Flags:        objectCmdsFlags[commandRemove],
				Action:       removeObjectHandler,
				BashComplete: bucketCompletions(bcmplop{multiple: true, separator: true}),
			},
			{
				Name:         commandPromote,
				Usage:        "promote files and directories (i.e., replicate files and convert them to objects)",
				ArgsUsage:    putPromoteObjectArgument,
				Flags:        objectCmdsFlags[commandPromote],
				Action:       promoteHandler,
				BashComplete: putPromoteObjectCompletions,
			},
			{
				Name:      commandConcat,
				Usage:     "concatenate multiple files into a new, single object",
				ArgsUsage: concatObjectArgument,
				Flags:     objectCmdsFlags[commandConcat],
				Action:    concatHandler,
			},
			{
				Name:         commandCat,
				Usage:        "cat an object (i.e., print its contents to STDOUT)",
				ArgsUsage:    objectArgument,
				Flags:        objectCmdsFlags[commandCat],
				Action:       catHandler,
				BashComplete: bucketCompletions(bcmplop{separator: true}),
			},
		},
	}
)

func mvObjectHandler(c *cli.Context) (err error) {
	if c.NArg() != 2 {
		return incorrectUsageMsg(c, "invalid number of arguments")
	}
	var (
		oldObjFull = c.Args().Get(0)
		newObj     = c.Args().Get(1)

		oldObj string
		bck    cmn.Bck
	)

	if bck, oldObj, err = parseBckObjectURI(c, oldObjFull); err != nil {
		return
	}
	if oldObj == "" {
		return incorrectUsageMsg(c, "no object specified in %q", oldObjFull)
	}
	if bck.Name == "" {
		return incorrectUsageMsg(c, "no bucket specified for object %q", oldObj)
	}
	if !bck.IsAIS() {
		return incorrectUsageMsg(c, "provider %q not supported", bck.Provider)
	}

	if bckDst, objDst, err := parseBckObjectURI(c, newObj); err == nil && bckDst.Name != "" {
		if !bckDst.Equal(&bck) {
			return incorrectUsageMsg(c, "moving an object to another bucket(%s) is not supported", bckDst)
		}
		if oldObj == "" {
			return missingArgumentsError(c, "no object specified in %q", newObj)
		}
		newObj = objDst
	}

	if newObj == oldObj {
		return incorrectUsageMsg(c, "source and destination are the same object")
	}

	if err = api.RenameObject(apiBP, bck, oldObj, newObj); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "%q moved to %q\n", oldObj, newObj)
	return
}

func removeObjectHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}

	if c.NArg() == 1 {
		uri := c.Args().First()
		bck, objName, err := parseBckObjectURI(c, uri, true /* optional objName */)
		if err != nil {
			return err
		}

		if flagIsSet(c, listFlag) || flagIsSet(c, templateFlag) {
			// List or range operation on a given bucket.
			return listOrRangeOp(c, bck)
		}
		if flagIsSet(c, rmRfFlag) {
			if !flagIsSet(c, yesFlag) {
				warn := fmt.Sprintf("will remove all objects from %s. The operation cannot be undone!", bck)
				if ok := confirm(c, "Proceed?", warn); !ok {
					return nil
				}
			}
			return rmRfAllObjects(c, bck)
		}

		if objName == "" {
			return incorrectUsageMsg(c, "%q or %q flag not set with a single bucket argument",
				listFlag.Name, templateFlag.Name)
		}

		// ais rm BUCKET/OBJECT_NAME - pass, multiObjOp will handle it
	}

	// List and range flags are invalid with object argument(s).
	if flagIsSet(c, listFlag) || flagIsSet(c, templateFlag) {
		return incorrectUsageMsg(c, "flags %q, %q cannot be used together with object name arguments",
			listFlag.Name, templateFlag.Name)
	}

	// Object argument(s) given by the user; operation on given object(s).
	return multiObjOp(c, commandRemove)
}

func getHandler(c *cli.Context) (err error) {
	outFile := c.Args().Get(1) // empty string if arg not given
	return getObject(c, outFile, false /*silent*/)
}

func createArchMultiObjHandler(c *cli.Context) (err error) {
	var (
		bckTo, bckFrom cmn.Bck
		objName        string
		template       = parseStrFlag(c, templateFlag)
		list           = parseStrFlag(c, listFlag)
	)
	if c.NArg() < 1 {
		return missingArgumentsError(c, "object name in the form bucket/[object]")
	}
	if template == "" && list == "" {
		return missingArgumentsError(c, "either object list or template flag")
	}
	if template != "" && list != "" {
		return incorrectUsageMsg(c, "list and template options are mutually exclusive")
	}
	if bckTo, objName, err = parseBckObjectURI(c, c.Args().Get(0), true /*optional objName*/); err != nil {
		return
	}
	if srcURI := parseStrFlag(c, sourceBckFlag); srcURI != "" {
		if bckFrom, _, err = parseBckObjectURI(c, srcURI, true /*optional objName*/); err != nil {
			return
		}
	} else {
		bckFrom = bckTo
	}
	msg := cmn.ArchiveMsg{ToBck: bckTo, ArchName: objName}
	msg.InclSrcBname = flagIsSet(c, includeSrcBucketNameFlag)
	msg.AllowAppendToExisting = flagIsSet(c, allowAppendToExistingFlag)
	msg.ContinueOnError = flagIsSet(c, continueOnErrorFlag)

	if list != "" {
		msg.SelectObjsMsg.ObjNames = makeList(list)
		_, err = api.CreateArchMultiObj(apiBP, bckFrom, msg)
	} else {
		msg.SelectObjsMsg.Template = template
		_, err = api.CreateArchMultiObj(apiBP, bckFrom, msg)
	}
	if err != nil {
		return err
	}
	for i := 0; i < 3; i++ {
		time.Sleep(time.Second)
		_, err = api.HeadObject(apiBP, bckTo, objName, apc.FltPresentNoProps)
		if err == nil {
			fmt.Fprintf(c.App.Writer, "Created archive %q\n", bckTo.DisplayName()+"/"+objName)
			return nil
		}
	}
	fmt.Fprintf(c.App.Writer, "Creating archive %q ...\n", bckTo.DisplayName()+"/"+objName)
	return nil
}

func putRegularObjHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, "file to put", "destination object name in the form "+optionalObjectsArgument)
	}
	fileName := c.Args().Get(0)
	if c.NArg() < 2 {
		return missingArgumentsError(c, "destination object name in the form "+optionalObjectsArgument)
	}
	uri := c.Args().Get(1)
	if c.NArg() > 2 {
		return incorrectUsageMsg(c, "too many arguments _or_ unrecognized option '%+v'", c.Args()[2:])
	}

	bck, objName, err := parseBckObjectURI(c, uri, true /*optional objName*/)
	if err != nil {
		return err
	}
	if flagIsSet(c, dryRunFlag) {
		return putDryRun(c, bck, objName, fileName)
	}
	return putObject(c, bck, objName, fileName)
}

func putHandler(c *cli.Context) (err error) {
	if flagIsSet(c, createArchFlag) {
		return createArchMultiObjHandler(c)
	}

	return putRegularObjHandler(c)
}

func concatHandler(c *cli.Context) (err error) {
	var (
		bck     cmn.Bck
		objName string
	)
	if c.NArg() < 1 {
		return missingArgumentsError(c, "at least one file to put", "object name in the form bucket/[object]")
	}
	if c.NArg() < 2 {
		return missingArgumentsError(c, "object name in the form bucket/object")
	}

	fullObjName := c.Args().Get(len(c.Args()) - 1)
	fileNames := make([]string, len(c.Args())-1)
	for i := 0; i < len(c.Args())-1; i++ {
		fileNames[i] = c.Args().Get(i)
	}

	if bck, objName, err = parseBckObjectURI(c, fullObjName); err != nil {
		return
	}
	if _, err = headBucket(bck, false /* don't add */); err != nil {
		return
	}
	return concatObject(c, bck, objName, fileNames)
}

func promoteHandler(c *cli.Context) (err error) {
	if c.NArg() < 1 {
		return missingArgumentsError(c, "source file|directory to promote")
	}
	fqn := c.Args().Get(0)
	if !filepath.IsAbs(fqn) {
		return incorrectUsageMsg(c, "promoted source (file or directory) must have an absolute path")
	}

	if c.NArg() < 2 {
		return missingArgumentsError(c, "destination in the form bucket/[object]")
	}

	var (
		bck         cmn.Bck
		objName     string
		fullObjName = c.Args().Get(1)
	)
	if bck, objName, err = parseBckObjectURI(c, fullObjName, true /*optObjName*/); err != nil {
		return
	}
	if _, err = headBucket(bck, false /* don't add */); err != nil {
		return
	}
	return promote(c, bck, objName, fqn)
}

func setCustomPropsHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	uri := c.Args().First()
	bck, objName, err := parseBckObjectURI(c, uri, true /* optional objName */)
	if err != nil {
		return err
	}
	return setCustomProps(c, bck, objName)
}

func catHandler(c *cli.Context) (err error) {
	return getObject(c, fileStdIO, true /*silent*/)
}
