// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles CLI commands that pertain to AIS objects.
/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb/v4"
	"github.com/vbauerster/mpb/v4/decor"
	"golang.org/x/sync/errgroup"
)

var (
	archPutUsage = "archive a file, a directory, or multiple files and/or directories as\n" +
		indent1 + "\t" + archExts + "-formatted object - aka \"shard\".\n" +
		indent1 + "\tBoth APPEND (to an existing shard) and PUT (a new version of the shard) are supported.\n" +
		indent1 + "\tExamples:\n" +
		indent1 + "\t- 'local-file s3://q/shard-00123.tar.lz4 --append --archpath name-in-archive' - append file to a given shard,\n" +
		indent1 + "\t   optionally, rename it (inside archive) as specified;\n" +
		indent1 + "\t- 'local-file s3://q/shard-00123.tar.lz4 --append-or-put --archpath name-in-archive' - append file to a given shard if exists,\n" +
		indent1 + "\t   otherwise, create a new shard (and name it shard-00123.tar.lz4, as specified);\n" +
		indent1 + "\t- 'src-dir gs://w/shard-999.zip --append' - archive entire 'src-dir' directory; iff the destination .zip doesn't exist create a new one;\n" +
		indent1 + "\t- '\"sys, docs\" ais://dst/CCC.tar --dry-run -y -r --archpath ggg/' - dry-run to recursively archive two directories.\n" +
		indent1 + "\tTips:\n" +
		indent1 + "\t- use '--dry-run' if in doubt;\n" +
		indent1 + "\t- to archive objects from a ais:// or remote bucket, run 'ais archive bucket' (see --help for details)."
)

var (
	// flags
	archCmdsFlags = map[string][]cli.Flag{
		commandBucket: {
			archAppendOrPutFlag,
			continueOnErrorFlag,
			dontHeadSrcDstBucketsFlag,
			dryRunFlag,
			listFlag,
			templateFlag,
			verbObjPrefixFlag,
			inclSrcBucketNameFlag,
			waitFlag,
		},
		commandPut: append(
			listRangeProgressWaitFlags,
			archAppendOrPutFlag,
			archAppendOnlyFlag,
			archpathFlag,
			concurrencyFlag,
			dryRunFlag,
			recursFlag,
			verboseFlag,
			yesFlag,
			unitsFlag,
			inclSrcDirNameFlag,
			skipVerCksumFlag,
			continueOnErrorFlag, // TODO: revisit
		),
		cmdGenShards: {
			cleanupFlag,
			concurrencyFlag,
			fsizeFlag,
			fcountFlag,
			fextsFlag,
		},
	}

	// archive bucket
	archBucketCmd = cli.Command{
		Name: commandBucket,
		Usage: "archive selected or matching objects from " + bucketObjectSrcArgument + " as\n" +
			indent1 + archExts + "-formatted object (a.k.a. shard),\n" +
			indent1 + "e.g.:\n" +
			indent1 + "\t- 'archive bucket ais://src ais://dst/a.tar.lz4 --template \"shard-{001..997}\"'\n" +
			indent1 + "\t- 'archive bucket \"ais://src/shard-{001..997}\" ais://dst/a.tar.lz4'\t- same as above (notice double quotes)\n" +
			indent1 + "\t- 'archive bucket \"ais://src/shard-{998..999}\" ais://dst/a.tar.lz4 --append-or-put'\t- append (ie., archive) 2 more objects",
		ArgsUsage:    bucketObjectSrcArgument + " " + dstShardArgument,
		Flags:        archCmdsFlags[commandBucket],
		Action:       archMultiObjHandler,
		BashComplete: putPromApndCompletions,
	}

	// archive put
	archPutCmd = cli.Command{
		Name:         commandPut,
		Usage:        archPutUsage,
		ArgsUsage:    putApndArchArgument,
		Flags:        archCmdsFlags[commandPut],
		Action:       putApndArchHandler,
		BashComplete: putPromApndCompletions,
	}

	// archive get
	archGetCmd = cli.Command{
		Name: objectCmdGet.Name,
		// NOTE: compare with  objectCmdGet.Usage
		Usage: "get a shard and extract its content; get an archived file;\n" +
			indent4 + "\twrite the content locally with destination options including: filename, directory, STDOUT ('-'), or '/dev/null' (discard);\n" +
			indent4 + "\tassorted options further include:\n" +
			indent4 + "\t- '--prefix' to get multiple shards in one shot (empty prefix for the entire bucket);\n" +
			indent4 + "\t- '--progress' and '--refresh' to watch progress bar;\n" +
			indent4 + "\t- '-v' to produce verbose output when getting multiple objects.\n" +
			indent1 + "'ais archive get' examples:\n" +
			indent4 + "\t- ais://abc/trunk-0123.tar.lz4 /tmp/out - get and extract entire shard to /tmp/out/trunk/*\n" +
			indent4 + "\t- ais://abc/trunk-0123.tar.lz4 --archpath file45.jpeg /tmp/out - extract one named file\n" +
			indent4 + "\t- ais://abc/trunk-0123.tar.lz4/file45.jpeg /tmp/out - same as above (and note that '--archpath' is implied)\n" +
			indent4 + "\t- ais://abc/trunk-0123.tar.lz4/file45 /tmp/out/file456.new - same as above, with destination explicitly (re)named\n" +
			indent1 + "'ais archive get' multi-selection examples:\n" +
			indent4 + "\t- ais://abc/trunk-0123.tar 111.tar --archregx=jpeg --archmode=suffix - return 111.tar with all *.jpeg files from a given shard\n" +
			indent4 + "\t- ais://abc/trunk-0123.tar 222.tar --archregx=file45 --archmode=wdskey - return 222.tar with all file45.* files --/--\n" +
			indent4 + "\t- ais://abc/trunk-0123.tar 333.tar --archregx=subdir/ --archmode=prefix - 333.tar with all subdir/* files --/--",
		ArgsUsage:    getShardArgument,
		Flags:        rmFlags(objectCmdGet.Flags, headObjPresentFlag, lengthFlag, offsetFlag),
		Action:       getArchHandler,
		BashComplete: objectCmdGet.BashComplete,
	}

	// archive ls
	archLsCmd = cli.Command{
		Name:         cmdList,
		Usage:        "list archived content (supported formats: " + archFormats + ")",
		ArgsUsage:    optionalShardArgument,
		Flags:        rmFlags(bucketCmdsFlags[commandList], listArchFlag), // is implied
		Action:       listArchHandler,
		BashComplete: bucketCompletions(bcmplop{}),
	}

	// gen shards
	genShardsCmd = cli.Command{
		Name: cmdGenShards,
		Usage: "generate random " + archExts + "-formatted objects (\"shards\"), e.g.:\n" +
			indent4 + "\t- gen-shards 'ais://bucket1/shard-{001..999}.tar' - write 999 random shards (default sizes) to ais://bucket1\n" +
			indent4 + "\t- gen-shards \"gs://bucket2/shard-{01..20..2}.tgz\" - 10 random gzipped tarfiles to Cloud bucket\n" +
			indent4 + "\t(notice quotation marks in both cases)",
		ArgsUsage: `"BUCKET/TEMPLATE.EXT"`,
		Flags:     archCmdsFlags[cmdGenShards],
		Action:    genShardsHandler,
	}

	// main `ais archive`
	archCmd = cli.Command{
		Name:   commandArch,
		Usage:  "archive multiple objects from a given bucket; archive local files and directories; list archived content",
		Action: archUsageHandler,
		Subcommands: []cli.Command{
			archBucketCmd,
			archPutCmd,
			archGetCmd,
			archLsCmd,
			genShardsCmd,
		},
	}
)

func archUsageHandler(c *cli.Context) error {
	{
		// parse for put/append
		a := archput{}
		if err := a.parse(c); err == nil {
			msg := "missing " + commandArch + " subcommand"
			hint := strings.Join(findCmdMultiKeyAlt(commandArch, commandPut), " ")
			if c.NArg() > 0 {
				hint += " " + strings.Join(c.Args(), " ")
			}
			msg += " (did you mean: '" + hint + "')"
			return errors.New(msg)
		}
	}
	{
		// parse for x-archive multi-object
		src, dst := c.Args().Get(0), c.Args().Get(1)
		if _, _, err := parseBckObjURI(c, src, true); err == nil {
			if _, _, err := parseBckObjURI(c, dst, true); err == nil {
				msg := "missing " + commandArch + " subcommand"
				hint := strings.Join(findCmdMultiKeyAlt(commandArch, commandBucket), " ")
				if c.NArg() > 0 {
					hint += " " + strings.Join(c.Args(), " ")
				}
				msg += " (did you mean: '" + hint + "')"
				return errors.New(msg)
			}
		}
	}
	return fmt.Errorf("unrecognized or misplaced option '%+v', see %s for details", c.Args(), qflprn(cli.HelpFlag))
}

func archMultiObjHandler(c *cli.Context) error {
	// is it an attempt to PUT => archive?
	{
		a := archput{}
		if err := a.parse(c); err == nil {
			// Yes, it is
			msg := fmt.Sprintf("expecting %s\n(hint: use 'ais archive put' command, %s for details)",
				c.Command.ArgsUsage, qflprn(cli.HelpFlag))
			return errors.New(msg)
		}
	}

	// parse
	var a archbck
	a.apndIfExist = flagIsSet(c, archAppendOrPutFlag)
	if err := a.parse(c); err != nil {
		return err
	}
	// control msg
	msg := cmn.ArchiveBckMsg{ToBck: a.dst.bck}
	{
		msg.ArchName = a.dst.oname
		msg.InclSrcBname = flagIsSet(c, inclSrcBucketNameFlag)
		msg.ContinueOnError = flagIsSet(c, continueOnErrorFlag)
		msg.AppendIfExists = a.apndIfExist
		msg.ListRange = a.rsrc.lr
	}
	// dry-run
	if flagIsSet(c, dryRunFlag) {
		dryRunCptn(c)
		what := msg.ListRange.Template
		if msg.ListRange.IsList() {
			what = strings.Join(msg.ListRange.ObjNames, ", ")
		}
		fmt.Fprintf(c.App.Writer, "archive %s/{%s} as %q\n", a.rsrc.bck, what, a.dest())
		return nil
	}
	if !flagIsSet(c, dontHeadSrcDstBucketsFlag) {
		if _, err := headBucket(a.rsrc.bck, false /* don't add */); err != nil {
			return err
		}
		if _, err := headBucket(a.dst.bck, false /* don't add */); err != nil {
			return err
		}
	}
	// do
	_, err := api.ArchiveMultiObj(apiBP, a.rsrc.bck, &msg)
	if err != nil {
		return V(err)
	}
	// check (NOTE: not waiting through idle-ness, not looking at multiple returned xids)
	var (
		total time.Duration
		sleep = time.Second / 2
		maxw  = 2 * time.Second
	)
	if flagIsSet(c, waitFlag) {
		maxw = 8 * time.Second
	}
	for total < maxw {
		hargs := api.HeadArgs{FltPresence: apc.FltPresentNoProps, Silent: true}
		if _, errV := api.HeadObject(apiBP, a.dst.bck, a.dst.oname, hargs); errV == nil {
			goto ex
		}
		time.Sleep(sleep)
		total += sleep
	}
ex:
	actionDone(c, "Archived "+a.dest())
	return nil
}

func putApndArchHandler(c *cli.Context) (err error) {
	{
		src, dst := c.Args().Get(0), c.Args().Get(1)
		if _, _, err := parseBckObjURI(c, src, true); err == nil {
			if _, _, err := parseBckObjURI(c, dst, true); err == nil {
				return fmt.Errorf("expecting %s\n(hint: use 'ais archive bucket' command, see %s for details)",
					c.Command.ArgsUsage, qflprn(cli.HelpFlag))
			}
		}
	}
	a := archput{}
	if err = a.parse(c); err != nil {
		return
	}
	if flagIsSet(c, dryRunFlag) {
		dryRunCptn(c)
	}
	if a.srcIsRegular() {
		// [convention]: naming default when '--archpath' is omitted
		if a.archpath == "" {
			a.archpath = filepath.Base(a.src.abspath)
		}
		if err = a2aRegular(c, &a); err != nil {
			return
		}
		msg := fmt.Sprintf("%s %s to %s", a.verb(), a.src.arg, a.dst.bck.Cname(a.dst.oname))
		if a.archpath != a.src.arg {
			msg += " as \"" + a.archpath + "\""
		}
		actionDone(c, msg+"\n")
		return
	}

	//
	// multi-file cases
	//
	if _, err := headBucket(a.dst.bck, false /*don't add*/); err != nil {
		if _, ok := err.(*errDoesNotExist); ok {
			return fmt.Errorf("destination %v", err)
		}
		return V(err)
	}
	if !a.appendOnly && !a.appendOrPut {
		warn := fmt.Sprintf("multi-file 'archive put' operation requires either %s or %s option",
			qflprn(archAppendOnlyFlag), qflprn(archAppendOrPutFlag))
		actionWarn(c, warn)
		if flagIsSet(c, yesFlag) {
			fmt.Fprintf(c.App.ErrWriter, "Assuming %s - proceeding to execute...\n\n", qflprn(archAppendOrPutFlag))
		} else {
			if ok := confirm(c, fmt.Sprintf("Proceed to execute 'archive put %s'?", flprn(archAppendOrPutFlag))); !ok {
				return
			}
		}
		a.appendOrPut = true
	}

	// archpath
	if a.archpath != "" && !cos.IsLastB(a.archpath, '/') {
		if !flagIsSet(c, yesFlag) {
			warn := fmt.Sprintf("no trailing filepath separator in: '%s=%s'", qflprn(archpathFlag), a.archpath)
			actionWarn(c, warn)
			if ok := confirm(c, "Proceed anyway?"); !ok {
				return
			}
		}
	}

	incl := flagIsSet(c, inclSrcDirNameFlag)
	switch {
	case len(a.src.fdnames) > 0:
		// a) csv of files and/or directories (names) from the first arg, e.g. "f1[,f2...]" dst-bucket[/prefix]
		// b) csv from '--list' flag
		return verbList(c, &a, a.src.fdnames, a.dst.bck, a.archpath /*append pref*/, incl)
	case a.pt != nil && len(a.pt.Ranges) > 0:
		// a) range from the first arg, e.g. "/tmp/www/test{0..2}{0..2}.txt" dst-bucket/www.zip
		// b) from '--template'
		var trimPrefix string
		if !incl {
			trimPrefix = rangeTrimPrefix(a.pt)
		}
		return verbRange(c, &a, a.pt, a.dst.bck, trimPrefix, a.archpath, incl)
	default: // one directory
		var (
			ndir    int
			srcpath = a.src.arg
		)
		if a.pt != nil {
			debug.Assert(srcpath == "", srcpath)
			srcpath = a.pt.Prefix
		}
		fobjs, err := lsFobj(c, srcpath, "" /*trim pref*/, a.archpath /*append pref*/, &ndir, a.src.recurs, incl)
		if err != nil {
			return err
		}
		debug.Assert(ndir == 1)
		return verbFobjs(c, &a, fobjs, a.dst.bck, ndir, a.src.recurs)
	}
}

func a2aRegular(c *cli.Context, a *archput) error {
	var (
		reader   cos.ReadOpenCloser
		progress *mpb.Progress
	)
	if flagIsSet(c, dryRunFlag) {
		// resulting message printed upon return
		return nil
	}
	fh, err := cos.NewFileHandle(a.src.abspath)
	if err != nil {
		return err
	}
	reader = fh
	if flagIsSet(c, progressFlag) {
		fi, err := fh.Stat()
		if err != nil {
			return err
		}
		// setup progress bar
		var (
			bars []*mpb.Bar
			args = barArgs{barType: sizeArg, barText: a.dst.oname, total: fi.Size()}
		)
		progress, bars = simpleBar(args)
		cb := func(n int, _ error) { bars[0].IncrBy(n) }
		reader = cos.NewCallbackReadOpenCloser(fh, cb)
	}
	putArgs := api.PutArgs{
		BaseParams: apiBP,
		Bck:        a.dst.bck,
		ObjName:    a.dst.oname,
		Reader:     reader,
		Cksum:      nil,
		Size:       uint64(a.src.finfo.Size()),
		SkipVC:     flagIsSet(c, skipVerCksumFlag),
	}
	putApndArchArgs := api.PutApndArchArgs{
		PutArgs:  putArgs,
		ArchPath: a.archpath,
	}
	if a.appendOnly {
		putApndArchArgs.Flags = apc.ArchAppend
	}
	if a.appendOrPut {
		debug.Assert(!a.appendOnly)
		putApndArchArgs.Flags = apc.ArchAppendIfExist
	}
	err = api.PutApndArch(&putApndArchArgs)
	if progress != nil {
		progress.Wait()
	}
	return err
}

func getArchHandler(c *cli.Context) error {
	return getHandler(c)
}

func listArchHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	bck, objName, err := parseBckObjURI(c, c.Args().Get(0), true /*empty ok*/)
	if err != nil {
		return err
	}
	prefix := parseStrFlag(c, listObjPrefixFlag)
	if objName != "" && prefix != "" && !strings.HasPrefix(prefix, objName) {
		return fmt.Errorf("cannot handle object name ('%s') and prefix ('%s') simultaneously - "+NIY,
			objName, prefix)
	}
	if prefix == "" {
		prefix = objName
	}
	return listObjects(c, bck, prefix, true /*list arch*/)
}

//
// generate shards
//

func genShardsHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return incorrectUsageMsg(c, "missing destination bucket and BASH brace extension template")
	}
	if c.NArg() > 1 {
		return incorrectUsageMsg(c, "too many arguments (make sure to use quotation marks to prevent BASH brace expansion)")
	}

	// Expecting: "ais://bucket/shard-{00..99}.tar"
	bck, objname, err := parseBckObjURI(c, c.Args().Get(0), false)
	if err != nil {
		return err
	}

	mime, err := archive.Strict("", objname)
	if err != nil {
		return err
	}

	fileCnt := parseIntFlag(c, fcountFlag)

	fileSize, err := parseSizeFlag(c, fsizeFlag)
	if err != nil {
		return err
	}

	fileExts := []string{dfltFext}
	if flagIsSet(c, fextsFlag) {
		s := parseStrFlag(c, fextsFlag)
		fileExts = splitCsv(s)

		// file extension must start with "."
		for i := range fileExts {
			if fileExts[i][0] != '.' {
				fileExts[i] = "." + fileExts[i]
			}
		}
	}

	mm, err := memsys.NewMMSA("cli-gen-shards", true /*silent*/)
	if err != nil {
		debug.AssertNoErr(err) // unlikely
		return err
	}

	ext := mime
	template := strings.TrimSuffix(objname, ext)
	pt, err := cos.ParseBashTemplate(template)
	if err != nil {
		return err
	}

	if err := setupBucket(c, bck); err != nil {
		return err
	}

	var (
		shardNum      int
		progress      = mpb.New(mpb.WithWidth(barWidth))
		concLimit     = parseIntFlag(c, concurrencyFlag)
		concSemaphore = make(chan struct{}, concLimit)
		group, ctx    = errgroup.WithContext(context.Background())
		text          = "Shards created: "
		options       = make([]mpb.BarOption, 0, 6)
	)
	// progress bar
	options = append(options, mpb.PrependDecorators(
		decor.Name(text, decor.WC{W: len(text) + 2, C: decor.DSyncWidthR}),
		decor.CountersNoUnit("%d/%d", decor.WCSyncWidth),
	))
	options = appendDefaultDecorators(options)
	bar := progress.AddBar(pt.Count(), options...)

	pt.InitIter()

loop:
	for shardName, hasNext := pt.Next(); hasNext; shardName, hasNext = pt.Next() {
		select {
		case concSemaphore <- struct{}{}:
		case <-ctx.Done():
			break loop
		}
		group.Go(func(i int, name string) func() error {
			return func() error {
				defer func() {
					bar.Increment()
					<-concSemaphore
				}()

				name := fmt.Sprintf("%s%s", name, ext)
				sgl := mm.NewSGL(fileSize * int64(fileCnt))
				defer sgl.Free()

				if err := genOne(sgl, ext, i*fileCnt, (i+1)*fileCnt, fileCnt, int(fileSize), fileExts); err != nil {
					return err
				}
				putArgs := api.PutArgs{
					BaseParams: apiBP,
					Bck:        bck,
					ObjName:    name,
					Reader:     sgl,
					SkipVC:     true,
				}
				_, err := api.PutObject(&putArgs)
				return V(err)
			}
		}(shardNum, shardName))
		shardNum++
	}
	if err := group.Wait(); err != nil {
		bar.Abort(true)
		return err
	}
	progress.Wait()
	return nil
}

func genOne(w io.Writer, shardExt string, start, end, fileCnt, fileSize int, fileExts []string) (err error) {
	var (
		prefix = make([]byte, 10)
		width  = len(strconv.Itoa(fileCnt))
		oah    = cos.SimpleOAH{Size: int64(fileSize), Atime: time.Now().UnixNano()}
		opts   = archive.Opts{CB: archive.SetTarHeader, Serialize: false}
		writer = archive.NewWriter(shardExt, w, nil /*cksum*/, &opts)
	)
	for idx := start; idx < end && err == nil; idx++ {
		cryptorand.Read(prefix)

		for _, fext := range fileExts {
			name := fmt.Sprintf("%s-%0*d"+fext, hex.EncodeToString(prefix), width, idx)
			err = writer.Write(name, oah, io.LimitReader(cryptorand.Reader, int64(fileSize)))
		}
	}
	writer.Fini()
	return
}
