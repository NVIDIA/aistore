// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"

	"github.com/urfave/cli"
)

// in this file: operations on objects

// (compare with  archGetUsage)
const objGetUsage = "Get an object, a shard, an archived file, or a range of bytes from all of the above;\n" +
	indent4 + "\twrite the content locally with destination options including: filename, directory, STDOUT ('-'), or '/dev/null' (discard);\n" +
	indent4 + "\tassorted options further include:\n" +
	indent4 + "\t- '--prefix' to get multiple objects in one shot (empty prefix for the entire bucket);\n" +
	indent4 + "\t- '--extract' or '--archpath' to extract archived content;\n" +
	indent4 + "\t- '--progress' and '--refresh' to watch progress bar;\n" +
	indent4 + "\t- '-v' to produce verbose output when getting multiple objects."

const objPutUsage = "PUT or append one file, one directory, or multiple files and/or directories.\n" +
	indent1 + "Use optional shell filename PATTERN (wildcard) to match/select multiple sources.\n" +
	indent1 + "Destination naming is consistent with 'ais object promote' command, whereby the optional OBJECT_NAME_or_PREFIX\n" +
	indent1 + "becomes either a name, a prefix, or a virtual destination directory (if it ends with a forward '/').\n" +
	indent1 + "Assorted examples and usage options follow (and see docs/cli/object.md for more):\n" +
	indent1 + "\t- upload matching files: 'ais put \"docs/*.md\" ais://abc/markdown/'\n" +
	indent1 + "\t- (notice quotation marks and a forward slash after 'markdown/' destination);\n" +
	indent1 + "\t- '--compute-checksum': use '--compute-checksum' to facilitate end-to-end protection;\n" +
	indent1 + "\t- '--progress': progress bar, to show running counts and sizes of uploaded files;\n" +
	indent1 + "\t- Ctrl-D: when writing directly from standard input use Ctrl-D to terminate;\n" +
	indent1 + "\t- '--append' to append (concatenate) files, e.g.: 'ais put docs ais://nnn/all-docs --append';\n" +
	indent1 + "\t- '--dry-run': see the results without making any changes.\n" +
	indent1 + "\tNotes:\n" +
	indent1 + "\t- to write or add files to " + archExts + "-formatted objects (\"shards\"), use 'ais archive'"

const objPromoteUsage = "PROMOTE target-accessible files and directories.\n" +
	indent1 + "The operation is intended for copying NFS and SMB shares mounted on any/all targets\n" +
	indent1 + "but can be also used to copy local files (again, on any/all targets in the cluster).\n" +
	indent1 + "Copied files and directories become regular stored objects that can be further listed and operated upon.\n" +
	indent1 + "Destination naming is consistent with 'ais put' command, e.g.:\n" +
	indent1 + "\t- 'promote /tmp/subdir/f1 ais://nnn'\t - ais://nnn/f1\n" +
	indent1 + "\t- 'promote /tmp/subdir/f2 ais://nnn/aaa'\t - ais://nnn/aaa\n" +
	indent1 + "\t- 'promote /tmp/subdir/f3 ais://nnn/aaa/'\t - ais://nnn/aaa/f3\n" +
	indent1 + "\t- 'promote /tmp/subdir ais://nnn'\t - ais://nnn/f1, ais://nnn/f2, ais://nnn/f3\n" +
	indent1 + "\t- 'promote /tmp/subdir ais://nnn/aaa/'\t - ais://nnn/aaa/f1, ais://nnn/aaa/f2, ais://nnn/aaa/f3\n" +
	indent1 + "Other supported options follow below."

const objRmUsage = "Remove object or selected objects from the specified bucket, or buckets - e.g.:\n" +
	indent1 + "\t- 'rm ais://nnn --all'\t- remove all objects from the bucket ais://nnn;\n" +
	indent1 + "\t- 'rm s3://abc' --all\t- remove all objects including those that are not _present_ in the cluster;\n" +
	indent1 + "\t- 'rm gs://abc --prefix images/'\t- remove all objects from the virtual subdirectory \"images\";\n" +
	indent1 + "\t- 'rm gs://abc/images/'\t- same as above;\n" +
	indent1 + "\t- 'rm gs://abc --template images/'\t- same as above;\n" +
	indent1 + "\t- 'rm gs://abc --template \"shard-{0000..9999}.tar.lz4\"'\t- remove the matching range (prefix + brace expansion);\n" +
	indent1 + "\t- 'rm \"gs://abc/shard-{0000..9999}.tar.lz4\"'\t- same as above (notice double quotes)"

const concatUsage = "Append a file, a directory, or multiple files and/or directories\n" +
	indent1 + "as a new " + objectArgument + " if doesn't exists, and to an existing " + objectArgument + " otherwise, e.g.:\n" +
	indent1 + "$ ais object concat docs ais://nnn/all-docs ### concatenate all files from docs/ directory."

const setCustomArgument = objectArgument + " " + jsonKeyValueArgument + " | " + keyValuePairsArgument + ", e.g.:\n" +
	indent1 + "mykey1=value1 mykey2=value2 OR (same) '{\"mykey1\":\"value1\", \"mykey2\":\"value2\"}'"

var (
	objectCmdsFlags = map[string][]cli.Flag{
		commandRemove: append(
			listRangeProgressWaitFlags,
			verbObjPrefixFlag, // to disambiguate bucket/prefix vs bucket/objName
			rmrfFlag,
			verboseFlag, // rm -rf
			nonverboseFlag,
			noRecursFlag, // (embedded prefix dopOLTP dop)
			yesFlag,
			dontHeadRemoteFlag,
		),
		commandRename: {},
		commandGet: {
			offsetFlag,
			lengthFlag,
			cksumFlag,
			yesFlag,
			headObjPresentFlag,
			latestVerFlag,
			refreshFlag,
			progressFlag,
			// blob-downloader
			blobDownloadFlag,
			chunkSizeFlag,
			numBlobWorkersFlag,
			// archive
			archpathGetFlag,
			archmimeFlag,
			archregxFlag,
			archmodeFlag,
			// archive, client side
			extractFlag,
			// bucket inventory
			useInventoryFlag,
			invNameFlag,
			invIDFlag,
			// multi-object options (note: passed to list-objects)
			getObjPrefixFlag,
			getObjCachedFlag,
			listArchFlag,
			objLimitFlag,
			//
			unitsFlag,   // raw (bytes), kb, mib, etc.
			verboseFlag, // client side
			silentFlag,  // server side
			dontHeadRemoteFlag,
			encodeObjnameFlag,
		},

		commandPut: append(
			listRangeProgressWaitFlags,
			chunkSizeFlag,
			numPutWorkersFlag,
			dryRunFlag,
			recursFlag,
			putSrcDirNameFlag,
			verboseFlag,
			yesFlag,
			continueOnErrorFlag,
			unitsFlag,
			putRetriesFlag,
			// cksum
			skipVerCksumFlag,
			putObjDfltCksumFlag,
			// append
			appendConcatFlag,
			dontHeadRemoteFlag,
			encodeObjnameFlag,
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
			dontHeadRemoteFlag,
		},
		commandConcat: {
			recursFlag,
			unitsFlag,
			progressFlag,
		},
		commandCat: {
			offsetFlag,
			lengthFlag,
			archpathGetFlag,
			cksumFlag,
			forceFlag,
			encodeObjnameFlag,
		},
	}

	// define separately to allow for aliasing (see alias_hdlr.go)
	objectCmdGet = cli.Command{
		Name:         commandGet,
		Usage:        objGetUsage,
		ArgsUsage:    getObjectArgument,
		Flags:        sortFlags(objectCmdsFlags[commandGet]),
		Action:       getHandler,
		BashComplete: bucketCompletions(bcmplop{separator: true}),
	}

	objectCmdPut = cli.Command{
		Name:         commandPut,
		Usage:        objPutUsage,
		ArgsUsage:    putObjectArgument,
		Flags:        sortFlags(append(objectCmdsFlags[commandPut], putObjCksumFlags...)),
		Action:       putHandler,
		BashComplete: putPromApndCompletions,
	}
	objectCmdPromote = cli.Command{
		Name:         commandPromote,
		Usage:        objPromoteUsage,
		ArgsUsage:    promoteObjectArgument,
		Flags:        sortFlags(objectCmdsFlags[commandPromote]),
		Action:       promoteHandler,
		BashComplete: putPromApndCompletions,
	}
	objectCmdConcat = cli.Command{
		Name:      commandConcat,
		Usage:     concatUsage,
		ArgsUsage: concatObjectArgument,
		Flags:     sortFlags(objectCmdsFlags[commandConcat]),
		Action:    concatHandler,
	}

	objectCmdSetCustom = cli.Command{
		Name:      commandSetCustom,
		Usage:     "Set object's custom properties",
		ArgsUsage: setCustomArgument,
		Flags:     sortFlags(objectCmdsFlags[commandSetCustom]),
		Action:    setCustomPropsHandler,
	}

	objectCmdPrefetch = cli.Command{
		Name:         commandPrefetch,
		Usage:        prefetchUsage,
		ArgsUsage:    bucketObjectOrTemplateMultiArg,
		Flags:        sortFlags(startSpecialFlags[commandPrefetch]),
		Action:       startPrefetchHandler,
		BashComplete: bucketCompletions(bcmplop{multiple: true}),
	}

	objectCmdRemove = cli.Command{
		Name:         commandRemove,
		Usage:        objRmUsage,
		ArgsUsage:    bucketObjectOrTemplateMultiArg,
		Flags:        sortFlags(objectCmdsFlags[commandRemove]),
		Action:       rmHandler,
		BashComplete: bucketCompletions(bcmplop{multiple: true, separator: true}),
	}

	objectCmd = cli.Command{
		Name:  commandObject,
		Usage: "PUT, GET, list, rename, remove, and other operations on objects",
		Subcommands: []cli.Command{
			objectCmdGet,
			bucketsObjectsCmdList,
			objectCmdPut,
			objectCmdPromote,
			makeAlias(bucketCmdCopy, "", true, commandCopy), // alias for `ais [bucket] cp`
			objectCmdConcat,
			objectCmdSetCustom,
			objectCmdRemove,
			objectCmdPrefetch,
			bucketObjCmdEvict,
			makeAlias(showCmdObject, "", true, commandShow), // alias for `ais show`
			{
				Name:         commandRename,
				Usage:        "Move (rename) object",
				ArgsUsage:    renameObjectArgument,
				Flags:        sortFlags(objectCmdsFlags[commandRename]),
				Action:       mvObjectHandler,
				BashComplete: bucketCompletions(bcmplop{multiple: true, separator: true}),
			},
			{
				Name:         commandCat,
				Usage:        "Print object's content to STDOUT (same as Linux shell 'cat')",
				ArgsUsage:    objectArgument,
				Flags:        sortFlags(objectCmdsFlags[commandCat]),
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
		return err
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
			return incorrectUsageMsg(c, "moving an object to another bucket (%s) is not supported", bckDst.Cname(""))
		}
		if oldObj == "" {
			return missingArgumentsError(c, "no object specified in %q", newObj)
		}
		newObj = objDst
	}

	if newObj == oldObj {
		return incorrectUsageMsg(c, "source and destination are the same object")
	}

	if err := api.RenameObject(apiBP, bck, oldObj, newObj); err != nil {
		return err
	}

	fmt.Fprintf(c.App.Writer, "%q moved to %q\n", oldObj, newObj)
	return nil
}

// main PUT handler: cases 1 through 4
func putHandler(c *cli.Context) error {
	if flagIsSet(c, appendConcatFlag) {
		return concatHandler(c)
	}

	var a putargs
	if err := a.parse(c, true /*empty dst oname*/); err != nil {
		return err
	}
	if flagIsSet(c, dryRunFlag) {
		dryRunCptn(c)
	}

	// 1. one file
	if a.srcIsRegular() {
		debug.Assert(a.src.abspath != "")
		if cos.IsLastB(a.dst.oname, '/') {
			a.dst.oname += a.src.arg
		}
		if err := putRegular(c, a.dst.bck, a.dst.oname, a.src.abspath, a.src.finfo); err != nil {
			e := stripErr(err)
			return fmt.Errorf("failed to %s %s => %s: %v", a.verb(), a.src.abspath, a.dst.bck.Cname(a.dst.oname), e)
		}
		actionDone(c, fmt.Sprintf("%s %q => %s\n", a.verb(), a.src.arg, a.dst.bck.Cname(a.dst.oname)))
		return nil
	}

	// 2. multi-file list & range
	incl := flagIsSet(c, putSrcDirNameFlag)
	switch {
	case len(a.src.fdnames) > 0:
		if len(a.src.fdnames) > 1 {
			if ok := warnMultiSrcDstPrefix(c, &a, fmt.Sprintf("from [%s ...]", a.src.fdnames[0])); !ok {
				return nil
			}
		}
		// a) csv of files and/or directories (names) embedded into the first arg, e.g. "f1[,f2...]" dst-bucket[/prefix]
		// b) csv from '--list' flag
		return verbList(c, &a, a.src.fdnames, a.dst.bck, a.dst.oname /*virt subdir*/, incl)
	case a.pt != nil && len(a.pt.Ranges) > 0:
		if ok := warnMultiSrcDstPrefix(c, &a, fmt.Sprintf("matching '%s'", a.src.tmpl)); !ok {
			return nil
		}
		// a) range via the first arg, e.g. "/tmp/www/test{0..2}{0..2}.txt" dst-bucket/www
		// b) range and prefix from the parsed '--template'
		var trimPrefix string
		if !incl {
			trimPrefix = rangeTrimPrefix(a.pt)
		}
		return verbRange(c, &a, a.pt, a.dst.bck, trimPrefix, a.dst.oname, incl)
	}

	// 3. STDIN
	if a.src.stdin {
		return putStdin(c, &a)
	}

	// 4. directory
	var (
		s       string
		ndir    int
		srcpath = a.src.arg
	)
	if a.pt != nil {
		debug.Assert(srcpath == "", srcpath)
		srcpath = a.pt.Prefix
	}
	if !strings.HasSuffix(srcpath, "/") {
		s = "/"
	}
	if ok := warnMultiSrcDstPrefix(c, &a, fmt.Sprintf("from '%s%s'", srcpath, s)); !ok {
		return nil
	}

	fobjs, err := lsFobj(c, srcpath, "", a.dst.oname, &ndir, a.src.recurs, incl, false /*globbed*/)
	if err != nil {
		return err
	}
	debug.Assert(ndir == 1)
	return verbFobjs(c, &a, fobjs, a.dst.bck, ndir, a.src.recurs)
}

func warnMultiSrcDstPrefix(c *cli.Context, a *putargs, from string) bool {
	if a.dst.oname == "" || cos.IsLastB(a.dst.oname, '/') {
		return true
	}
	warn := fmt.Sprintf("'%s' will be used as the destination name prefix for all files %s",
		a.dst.oname, from)
	actionWarn(c, warn)
	if _, err := archive.Mime(a.dst.oname, ""); err == nil {
		warn := fmt.Sprintf("did you want to use 'archive put' instead, with %q as the destination shard?",
			a.dst.oname)
		actionWarn(c, warn)
	}
	if !flagIsSet(c, yesFlag) {
		if ok := confirm(c, "Proceed anyway?"); !ok {
			return false
		}
	}
	return true
}

func putStdin(c *cli.Context, a *putargs) error {
	chunkSize, err := parseSizeFlag(c, chunkSizeFlag)
	if err != nil {
		return err
	}
	if flagIsSet(c, chunkSizeFlag) && chunkSize == 0 {
		return fmt.Errorf("chunk size (in %s) cannot be zero (%s recommended)",
			qflprn(chunkSizeFlag), teb.FmtSize(dfltStdinChunkSize, cos.UnitsIEC, 0))
	}
	if chunkSize == 0 {
		chunkSize = dfltStdinChunkSize
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
	for i := range len(c.Args()) - 1 {
		fileNames[i] = c.Args().Get(i)
	}

	if bck, objName, err = parseBckObjURI(c, fullObjName, false); err != nil {
		return
	}
	if shouldHeadRemote(c, bck) {
		if _, err = headBucket(bck, false /* don't add */); err != nil {
			return
		}
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
	if shouldHeadRemote(c, bck) {
		if _, err = headBucket(bck, false /* don't add */); err != nil {
			return
		}
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
