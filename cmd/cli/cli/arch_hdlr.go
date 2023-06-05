// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles CLI commands that pertain to AIS objects.
/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"context"
	"encoding/hex"
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

// TODO: destination naming: consider adding (explicit) '--trimprefix', here and elsewhere

var (
	archCmdsFlags = map[string][]cli.Flag{
		commandBucket: {
			templateFlag,
			listFlag,
			dryRunFlag,
			inclSrcBucketNameFlag,
			archAppendIfExistFlag,
			continueOnErrorFlag,
			waitFlag,
		},
		commandPut: append(
			listrangeFileFlags,
			archAppendIfExistFlag,
			archAppendFlag,
			archpathFlag,
			concurrencyFlag,
			dryRunFlag,
			recursFlag,
			verboseFlag,
			yesFlag,
			unitsFlag,
			inclSrcDirNameFlag,
			skipVerCksumFlag,
			continueOnErrorFlag, // TODO -- FIXME
		),
		cmdList: {
			objPropsFlag,
			allPropsFlag,
		},
		cmdGenShards: {
			cleanupFlag,
			concurrencyFlag,
			dsortFsizeFlag,
			dsortFcountFlag,
		},
	}

	archCmd = cli.Command{
		Name:  commandArch,
		Usage: "Archive multiple objects from a given bucket; archive local files and directories; list archived content",
		Subcommands: []cli.Command{
			{
				Name:         commandBucket,
				Usage:        "archive multiple objects from " + bucketSrcArgument + " as " + archExts + "-formatted shard",
				ArgsUsage:    bucketSrcArgument + " " + bucketDstArgument + "/SHARD_NAME",
				Flags:        archCmdsFlags[commandBucket],
				Action:       archMultiObjHandler,
				BashComplete: putPromApndCompletions,
			},
			{
				Name: commandPut,
				Usage: "archive a file, a directory, or multiple files and/or directories \n" +
					indent1 + "as a " + archExts + "-formatted object (\"shard\").\n" +
					indent1 + "Both APPEND (to an existing shard) and PUT (new version) variants are supported.\n" +
					indent1 + "Examples:\n" +
					indent1 + "- 'local-filename bucket/shard-00123.tar.lz4 --archpath name-in-archive' - append a file to a given shard and name it as specified;\n" +
					indent1 + "- 'src-dir bucket/shard-99999.zip -put' - one directory; iff the destination .zip doesn't exist create a new one;\n" +
					indent1 + "- '\"sys, docs\" ais://dst/CCC.tar --dry-run -y -r --archpath ggg/' - dry-run to recursively archive two directories.\n" +
					indent1 + "Tips:\n" +
					indent1 + "- use '--dry-run' option if in doubt;\n" +
					indent1 + "- to archive objects from a local or remote bucket, run 'ais archive bucket', see --help for details.",
				ArgsUsage:    putApndArchArgument,
				Flags:        archCmdsFlags[commandPut],
				Action:       putApndArchHandler,
				BashComplete: putPromApndCompletions,
			},
			{
				Name:         cmdList,
				Usage:        "list archived content (supported formats: " + archFormats + ")",
				ArgsUsage:    objectArgument,
				Flags:        archCmdsFlags[cmdList],
				Action:       listArchHandler,
				BashComplete: bucketCompletions(bcmplop{}),
			},
			{
				Name: cmdGenShards,
				Usage: "generate random " + archExts + "-formatted objects (\"shards\"), e.g.:\n" +
					indent4 + "\t- gen-shards 'ais://bucket1/shard-{001..999}.tar' - write 999 random shards (default sizes) to ais://bucket1\n" +
					indent4 + "\t- gen-shards \"gs://bucket2/shard-{01..20..2}.tgz\" - 10 random gzipped tarfiles to Cloud bucket\n" +
					indent4 + "\t(notice quotation marks in both cases)",
				ArgsUsage: `"BUCKET/TEMPLATE.EXT"`,
				Flags:     archCmdsFlags[cmdGenShards],
				Action:    genShardsHandler,
			},
		},
	}
)

func archMultiObjHandler(c *cli.Context) error {
	// parse
	var a archbck
	a.apndIfExist = flagIsSet(c, archAppendIfExistFlag)
	if err := a.parse(c); err != nil {
		return err
	}
	// api msg
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
	// do
	_, err := api.ArchiveMultiObj(apiBP, a.rsrc.bck, msg)
	if err != nil {
		return err
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
		if _, errV := api.HeadObject(apiBP, a.dst.bck, a.dst.oname, apc.FltPresentNoProps); errV == nil {
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
	a := archput{
		archpath:   parseStrFlag(c, archpathFlag),
		appendOnly: flagIsSet(c, archAppendFlag),
		appendIf:   flagIsSet(c, archAppendIfExistFlag),
	}
	if a.appendOnly && a.appendIf {
		return incorrectUsageMsg(c, errFmtExclusive, qflprn(archAppendFlag), qflprn(archAppendIfExistFlag))
	}
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

	// archpath
	if a.archpath != "" && !strings.HasSuffix(a.archpath, "/") {
		if !flagIsSet(c, yesFlag) {
			warn := fmt.Sprintf("no traling filepath separator in: '%s=%s'", qflprn(archpathFlag), a.archpath)
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
	case a.pt != nil:
		// a) range from the first arg, e.g. "/tmp/www/test{0..2}{0..2}.txt" dst-bucket/www.zip
		// b) from '--template'
		var trimPrefix string
		if !incl {
			trimPrefix = rangeTrimPrefix(a.pt)
		}
		return verbRange(c, &a, a.pt, a.dst.bck, trimPrefix, a.archpath, incl)
	default: // one directory
		var ndir int

		fobjs, err := lsFobj(c, a.src.arg, "" /*trim pref*/, a.archpath /*append pref*/, &ndir, a.src.recurs, incl)
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
		bars     []*mpb.Bar
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
		args := barArgs{barType: sizeArg, barText: a.dst.oname, total: fi.Size()}
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
	if a.appendIf {
		debug.Assert(!a.appendOnly)
		putApndArchArgs.Flags = apc.ArchAppendIfExist
	}
	err = api.PutApndArch(putApndArchArgs)
	if progress != nil {
		progress.Wait()
	}
	return err
}

func listArchHandler(c *cli.Context) error {
	bck, objName, err := parseBckObjURI(c, c.Args().Get(0), true)
	if err != nil {
		return err
	}
	return listObjects(c, bck, objName, true /*list arch*/)
}

//
// generate shards
//

func genShardsHandler(c *cli.Context) error {
	var (
		fileCnt   = parseIntFlag(c, dsortFcountFlag)
		concLimit = parseIntFlag(c, concurrencyFlag)
	)
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
	var (
		ext      = mime
		template = strings.TrimSuffix(objname, ext)
	)

	fileSize, err := parseSizeFlag(c, dsortFsizeFlag)
	if err != nil {
		return err
	}

	mm, err := memsys.NewMMSA("cli-gen-shards")
	if err != nil {
		debug.AssertNoErr(err) // unlikely
		return err
	}

	pt, err := cos.ParseBashTemplate(template)
	if err != nil {
		return err
	}

	if err := setupBucket(c, bck); err != nil {
		return err
	}

	var (
		// Progress bar
		text     = "Shards created: "
		progress = mpb.New(mpb.WithWidth(barWidth))
		bar      = progress.AddBar(
			pt.Count(),
			mpb.PrependDecorators(
				decor.Name(text, decor.WC{W: len(text) + 2, C: decor.DSyncWidthR}),
				decor.CountersNoUnit("%d/%d", decor.WCSyncWidth),
			),
			mpb.AppendDecorators(decor.Percentage(decor.WCSyncWidth)),
		)

		concSemaphore = make(chan struct{}, concLimit)
		group, ctx    = errgroup.WithContext(context.Background())
		shardNum      = 0
	)
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

				if err := genOne(sgl, ext, i*fileCnt, (i+1)*fileCnt, fileCnt, fileSize); err != nil {
					return err
				}
				putArgs := api.PutArgs{
					BaseParams: apiBP,
					Bck:        bck,
					ObjName:    name,
					Reader:     sgl,
					SkipVC:     true,
				}
				_, err := api.PutObject(putArgs)
				return err
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

func genOne(w io.Writer, ext string, start, end, fileCnt int, fileSize int64) (err error) {
	var (
		random = cos.NowRand()
		prefix = make([]byte, 10)
		width  = len(strconv.Itoa(fileCnt))
		oah    = cos.SimpleOAH{Size: fileSize, Atime: time.Now().UnixNano()}
		opts   = archive.Opts{CB: archive.SetTarHeader, Serialize: false}
		writer = archive.NewWriter(ext, w, nil /*cksum*/, &opts)
	)
	for idx := start; idx < end && err == nil; idx++ {
		random.Read(prefix)
		name := fmt.Sprintf("%s-%0*d.test", hex.EncodeToString(prefix), width, idx)
		err = writer.Write(name, oah, io.LimitReader(random, fileSize))
	}
	writer.Fini()
	return
}
