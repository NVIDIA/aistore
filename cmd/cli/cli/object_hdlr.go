// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles CLI commands that pertain to AIS objects.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/urfave/cli"
)

var (
	objectCmdsFlags = map[string][]cli.Flag{
		commandRemove: append(
			listrangeFlags,
			rmrfFlag,
			verboseFlag,
			yesFlag,
		),
		commandRename: {},
		commandGet: {
			offsetFlag,
			lengthFlag,
			archpathOptionalFlag,
			cksumFlag,
			yesFlag,
			checkObjCachedFlag,
			refreshFlag,
			progressFlag,
			// multi-object options (passed to list-objects)
			getObjPrefixFlag,
			getObjCachedFlag,
			listArchFlag,
			objLimitFlag,
			unitsFlag,
			verboseFlag,
		},

		commandPut: append(
			listrangeFileFlags,
			chunkSizeFlag,
			concurrencyFlag,
			dryRunFlag,
			recursFlag,
			verboseFlag,
			yesFlag,
			includeSrcBucketNameFlag,
			continueOnErrorFlag,
			unitsFlag,
			// arch
			archpathOptionalFlag,
			putArchFlag,
			apndArchIfExistsFlag,
			// cksum
			skipVerCksumFlag,
			putObjDfltCksumFlag,
		),
		commandSetCustom: {
			setNewCustomMDFlag,
		},
		commandPromote: {
			recursFlag,
			overwriteFlag,
			notFshareFlag,
			deleteSrcFlag,
			targetIDFlag,
			verboseFlag,
		},
		commandConcat: {
			recursFlag,
			unitsFlag,
			progressFlag,
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
		Name: commandGet,
		Usage: "get an object, an archived file, or a range of bytes from the above, and in addition:\n" +
			indent4 + "\t- write the content locally with destination options including: filename, directory, STDOUT ('-');\n" +
			indent4 + "\t- use '--prefix' to get multiple objects in one shot (empty prefix for the entire bucket).",
		ArgsUsage:    getObjectArgument,
		Flags:        objectCmdsFlags[commandGet],
		Action:       getHandler,
		BashComplete: bucketCompletions(bcmplop{separator: true}),
	}

	objectCmdPut = cli.Command{
		Name: commandPut,
		Usage: "PUT or APPEND one file, one directory, or multiple files and/or directories.\n" +
			indent4 + "\t- use optional shell filename pattern (wildcard) to match/select multiple sources, for example:\n" +
			indent4 + "\t\t$ ais put 'docs/*.md' ais://abc/markdown/  # notice single quotes\n" +
			indent4 + "\t- '--compute-checksum' to facilitate end-to-end protection;\n" +
			indent4 + "\t- progress bar via '--progress' to show runtime execution (uploaded files count and size);\n" +
			indent4 + "\t- when writing directly from standard input use Ctrl-D to terminate;\n" +
			indent4 + "\t- '--archpath' to APPEND to an existing tar-formatted object (\"shard\").",
		ArgsUsage:    putObjectArgument,
		Flags:        append(objectCmdsFlags[commandPut], putObjCksumFlags...),
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
				ArgsUsage:    promoteObjectArgument,
				Flags:        objectCmdsFlags[commandPromote],
				Action:       promoteHandler,
				BashComplete: putPromoteObjectCompletions,
			},
			{
				Name:      commandConcat,
				Usage:     "concatenate multiple files and/or directories (with or without matching pattern) as a new single object",
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

	if bck, oldObj, err = parseBckObjURI(c, oldObjFull, false); err != nil {
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

	if bckDst, objDst, err := parseBckObjURI(c, newObj, false); err == nil && bckDst.Name != "" {
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
		uri := c.Args().Get(0)
		bck, objName, err := parseBckObjURI(c, uri, true /*is optional*/)
		if err != nil {
			return err
		}
		if flagIsSet(c, listFlag) || flagIsSet(c, templateFlag) {
			// List or range operation on a given bucket.
			return listrange(c, bck)
		}
		if flagIsSet(c, rmrfFlag) {
			if !flagIsSet(c, yesFlag) {
				warn := fmt.Sprintf("will remove all objects from %s. The operation cannot be undone!", bck)
				if ok := confirm(c, "Proceed?", warn); !ok {
					return nil
				}
			}
			return rmRfAllObjects(c, bck)
		}

		if objName == "" {
			return incorrectUsageMsg(c, "use one of: (%s or %s or %s) to indicate _which_ objects to remove",
				qflprn(listFlag), qflprn(templateFlag), qflprn(rmrfFlag))
		}

		// ais rm BUCKET/OBJECT_NAME - pass, multiObjOp will handle it
	}

	// List and range flags are invalid with object argument(s).
	if flagIsSet(c, listFlag) || flagIsSet(c, templateFlag) {
		return incorrectUsageMsg(c, "flags %q, %q cannot be used together with object name arguments",
			listFlag.Name, templateFlag.Name)
	}

	// Object argument(s) given by the user; operation on given object(s).
	return multiobjArg(c, commandRemove)
}

func putHandler(c *cli.Context) (err error) {
	// destination: archive ? route to arch handler
	switch {
	case flagIsSet(c, putArchFlag):
		return archMultiObjHandler(c)
	case flagIsSet(c, archpathOptionalFlag):
		return appendArchHandler(c)
	}

	// main PUT switch
	var a putargs
	if err = a.parse(c); err != nil {
		return
	}
	switch {
	case len(a.src.fnames) > 0:
		// - csv embedded into the first arg, e.g. "f1[,f2...]" dst-bucket[/prefix], or
		// - csv from '--list' flag
		return putList(c, a.src.fnames, a.dst.bck, a.dst.oname /*virt subdir*/)
	case a.pt != nil:
		// - range via the first arg, e.g. "/tmp/www/test{0..2}{0..2}.txt" dst-bucket/www, or
		// - '--template' flag
		return putRange(c, a.pt, a.dst.bck, rangeTrimPrefix(a.pt), a.dst.oname)
	case a.src.stdin:
		return putStdin(c, &a)
	case !a.src.isdir: // reg file
		debug.Assert(a.src.finfo != nil)
		if err := putRegular(c, a.dst.bck, a.dst.oname, a.src.abspath, a.src.finfo); err != nil {
			return err
		}
		actionDone(c, fmt.Sprintf("PUT %q => %s\n", a.src.arg, a.dst.bck.Cname(a.dst.oname)))
		return nil
	default: // finally, a directory
		debug.Assert(a.src.finfo != nil)
		files, err := lsFobj(c, a.src.abspath, "", a.dst.oname, a.src.recurs)
		if err != nil {
			return err
		}
		tag := _putFrom(fmt.Sprintf(" from %q", a.src.arg), a.src.recurs)
		return putFobjs(c, files, a.dst.bck, tag)
	}
}

func putStdin(c *cli.Context, a *putargs) error {
	chunkSize, err := parseSizeFlag(c, chunkSizeFlag)
	if err != nil {
		return err
	}
	if flagIsSet(c, chunkSizeFlag) && chunkSize == 0 {
		return fmt.Errorf("chunk size (in %s) cannot be zero (%s recommended)",
			qflprn(chunkSizeFlag), teb.FmtSize(defaultChunkSize, cos.UnitsIEC, 0))
	}
	if chunkSize == 0 {
		chunkSize = defaultChunkSize
	}
	if flagIsSet(c, verboseFlag) {
		actionWarn(c, "To terminate input, press Ctrl-D two or more times")
	}
	cksum, err := cksumToCompute(c, a.dst.bck)
	if err != nil {
		return err
	}
	if err := putAppendChunks(c, a.dst.bck, a.dst.oname, os.Stdin, cksum.Type(), chunkSize); err != nil {
		return err
	}
	actionDone(c, fmt.Sprintf("PUT (standard input) => %s\n", a.dst.bck.Cname(a.dst.oname)))
	return nil
}

func concatHandler(c *cli.Context) (err error) {
	var (
		bck     cmn.Bck
		objName string
	)
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	if c.NArg() < 2 {
		return missingArgumentsError(c, "destination object in the form "+optionalObjectsArgument)
	}

	fullObjName := c.Args().Get(len(c.Args()) - 1)
	fileNames := make([]string, len(c.Args())-1)
	for i := 0; i < len(c.Args())-1; i++ {
		fileNames[i] = c.Args().Get(i)
	}

	if bck, objName, err = parseBckObjURI(c, fullObjName, false); err != nil {
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
		return missingArgumentsError(c, "destination in the form "+optionalObjectsArgument)
	}

	var (
		bck         cmn.Bck
		objName     string
		fullObjName = c.Args().Get(1)
	)
	if bck, objName, err = parseBckObjURI(c, fullObjName, true /*optObjName*/); err != nil {
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
	uri := c.Args().Get(0)
	bck, objName, err := parseBckObjURI(c, uri, true /* optional objName */)
	if err != nil {
		return err
	}
	return setCustomProps(c, bck, objName)
}
