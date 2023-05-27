// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles CLI commands that pertain to AIS objects.
/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb/v4"
)

var (
	archCmdsFlags = map[string][]cli.Flag{
		commandPut: {
			templateFlag,
			listFlag,
			includeSrcBucketNameFlag,
			apndArchIf1Flag,
			continueOnErrorFlag,
		},
		cmdAppend: {
			archpathRequiredFlag,
			putArchIfNotExistFlag,
		},
		cmdList: {
			objPropsFlag,
			allPropsFlag,
		},
	}

	archCmd = cli.Command{
		Name:  commandArch,
		Usage: "Put multi-object archive; append files and directories to an existing archive; list archived content",
		Subcommands: []cli.Command{
			{
				Name:         commandPut,
				Usage:        "put multi-object " + archExts + " archive",
				ArgsUsage:    bucketSrcArgument + " " + bucketDstArgument + "/OBJECT_NAME",
				Flags:        archCmdsFlags[commandPut],
				Action:       archMultiObjHandler,
				BashComplete: putPromApndCompletions,
			},
			{
				Name: cmdAppend,
				Usage: "append file, directory, or multiple files and/or directories to\n" +
					indent4 + "\t" + archExts + "-formatted object (\"shard\")\n" +
					indent4 + "\t" + "e.g.:\n" +
					indent4 + "\t" + "- 'filename bucket/shard-00123.tar.lz4 --archpath name-in-archive' # append a single file\n" +
					indent4 + "\t" + "- 'src-dir bucket/shard-99999.zip -put'  # append a directory, put a new .zip iff doesn't exist",
				ArgsUsage:    appendToArchArgument,
				Flags:        archCmdsFlags[cmdAppend],
				Action:       appendArchHandler,
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
		},
	}
)

func archMultiObjHandler(c *cli.Context) (err error) {
	var a archargs

	a.apndIfExist = flagIsSet(c, apndArchIf1Flag) || flagIsSet(c, apndArchIf2Flag)
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

func appendArchHandler(c *cli.Context) (err error) {
	if c.NArg() < 2 {
		// TODO -- FIXME
		return errors.New("not implemented yet (currently, expecting local file and bucket/shard)")
	}
	a := a2args{
		archpath:      parseStrFlag(c, archpathRequiredFlag),
		putIfNotExist: flagIsSet(c, putArchIfNotExistFlag),
	}
	if err = a.parse(c); err != nil {
		return
	}
	if err = a2a(c, &a); err == nil {
		msg := fmt.Sprintf("APPEND %s to %s", a.src.arg, a.dst.bck.Cname(a.dst.oname))
		if a.archpath != "" && a.archpath != a.src.arg {
			msg += " as \"" + a.archpath + "\""
		}
		actionDone(c, msg+"\n")
	}
	return
}

func a2a(c *cli.Context, a *a2args) error {
	var (
		reader   cos.ReadOpenCloser
		progress *mpb.Progress
		bars     []*mpb.Bar
		cksum    *cos.Cksum
	)
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
		Cksum:      cksum,
		Size:       uint64(a.src.finfo.Size()),
		SkipVC:     flagIsSet(c, skipVerCksumFlag),
	}
	appendArchArgs := api.AppendToArchArgs{
		PutArgs:       putArgs,
		ArchPath:      a.archpath,
		PutIfNotExist: a.putIfNotExist,
	}
	err = api.AppendToArch(appendArchArgs)
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
