// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles object operations.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/urfave/cli"
)

func catHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	// source
	uri := c.Args().Get(0)
	bck, objName, err := parseBckObjectURI(c, uri)
	if err != nil {
		return err
	}
	return getObject(c, bck, objName, fileStdIO, true /*silent*/)
}

func getHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	// source
	uri := c.Args().Get(0)
	bck, objName, err := parseBckObjectURI(c, uri, flagIsSet(c, prefixFlag) /*optObjName*/)
	if err != nil {
		return err
	}
	// destination (empty "" implies using source `basename`)
	outFile := c.Args().Get(1)

	// GET multiple
	if flagIsSet(c, prefixFlag) {
		if objName != "" {
			return fmt.Errorf("object name in %q and %s cannot be used together (hint: use directory as destination)",
				uri, qflprn(prefixFlag))
		}
		return getMultiObj(c, bck, outFile)
	}

	// GET
	if !bck.IsHTTP() {
		if _, err = headBucket(bck, false /* don't add */); err != nil {
			return err
		}
	}
	return getObject(c, bck, objName, outFile, false /*silent*/)
}

func getMultiObj(c *cli.Context, bck cmn.Bck, outFile string) error {
	var (
		prefix   = parseStrFlag(c, prefixFlag)
		msg      = &apc.LsoMsg{Prefix: prefix, Props: apc.GetPropsName}
		listArch = flagIsSet(c, listArchFlag) // archived content
	)
	// setup list-objects msg and call
	msg.SetFlag(apc.LsNameOnly)
	if listArch {
		msg.SetFlag(apc.LsArchDir)
	}
	if flagIsSet(c, checkObjCachedFlag) {
		msg.SetFlag(apc.LsObjCached)
	}
	pageSize, limit, err := _setPage(c, bck)
	if err != nil {
		return err
	}
	msg.PageSize = uint(pageSize)

	// list-objects
	objList, err := api.ListObjectsWithOpts(apiBP, bck, msg, uint(limit), nil /*progress ctx*/)
	if err != nil {
		return err
	}
	// many to one
	l := len(objList.Entries)
	if l > 1 {
		if outFile != "" && outFile != fileStdIO && outFile != discardIO {
			finfo, errEx := os.Stat(outFile)
			// destination directory must exist
			if errEx != nil || !finfo.IsDir() {
				return fmt.Errorf("cannot write %d prefix-matching objects to a single file %q", l, outFile)
			}
		}
	}

	// announce, confirm
	var (
		silent = !flagIsSet(c, verboseFlag)
		cptn   = fmt.Sprintf("GET %d object%s from %s to %s", l, cos.Plural(l), bck.DisplayName(), outFile)
	)
	if flagIsSet(c, yesFlag) && (l > 1 || silent) {
		fmt.Fprintln(c.App.Writer, cptn)
	} else if ok := confirm(c, cptn); !ok {
		return nil
	}

	u := &uctx{
		showProgress: false, // TODO -- FIXME: progress bar
		wg:           cos.NewLimitedWaitGroup(4, 0),
	}
	for _, entry := range objList.Entries {
		u.wg.Add(1)
		go u.get(c, bck, entry.Name, outFile, silent)
	}
	u.wg.Wait()

	if u.showProgress {
		u.progress.Wait()
	}
	if numFailed := u.errCount.Load(); numFailed > 0 {
		return fmt.Errorf("failed to GET %d object%s", numFailed, cos.Plural(int(numFailed)))
	}
	return nil
}

//////////
// uctx (extension)
//////////

func (u *uctx) get(c *cli.Context, bck cmn.Bck, objName, outFile string, silent bool) {
	defer u.wg.Done()
	err := getObject(c, bck, objName, outFile, silent)
	if err != nil {
		actionWarn(c, err.Error())
		u.errCount.Inc()
	}
}

func getObject(c *cli.Context, bck cmn.Bck, objName, outFile string, silent bool) (err error) {
	var (
		getArgs api.GetArgs
		oah     api.ObjAttrs
		units   string
	)
	// just check if a remote object is present (do not GET)
	// TODO: archived files
	if flagIsSet(c, checkObjCachedFlag) {
		return isObjPresent(c, bck, objName)
	}

	var offset, length int64
	units, err = parseUnitsFlag(c, unitsFlag)
	if err != nil {
		return err
	}
	if flagIsSet(c, lengthFlag) != flagIsSet(c, offsetFlag) {
		return incorrectUsageMsg(c, "%q and %q flags both need to be set", lengthFlag.Name, offsetFlag.Name)
	}
	if offset, err = parseSizeFlag(c, offsetFlag); err != nil {
		return
	}
	if length, err = parseSizeFlag(c, lengthFlag); err != nil {
		return
	}

	// where to
	archPath := parseStrFlag(c, archpathOptionalFlag)
	if outFile == "" {
		// archive
		if archPath != "" {
			outFile = filepath.Base(archPath)
		} else {
			outFile = filepath.Base(objName)
		}
	} else if outFile != fileStdIO && outFile != discardIO {
		finfo, errEx := os.Stat(outFile)
		if errEx == nil {
			// destination is: directory | file (confirm overwrite)
			if finfo.IsDir() {
				// archive
				if archPath != "" {
					outFile = filepath.Join(outFile, filepath.Base(archPath))
				} else {
					outFile = filepath.Join(outFile, filepath.Base(objName))
				}
			} else if finfo.Mode().IsRegular() && !flagIsSet(c, yesFlag) { // `/dev/null` is fine
				warn := fmt.Sprintf("overwrite existing %q", outFile)
				if ok := confirm(c, warn); !ok {
					return nil
				}
			}
		}
	}

	hdr := cmn.MakeRangeHdr(offset, length)
	if outFile == fileStdIO {
		getArgs = api.GetArgs{Writer: os.Stdout, Header: hdr}
		silent = true
	} else {
		var file *os.File
		if file, err = os.Create(outFile); err != nil {
			return
		}
		defer func() {
			file.Close()
			if err != nil {
				os.Remove(outFile)
			}
		}()
		getArgs = api.GetArgs{Writer: file, Header: hdr}
	}

	if bck.IsHTTP() {
		uri := c.Args().Get(0)
		getArgs.Query = make(url.Values, 2)
		getArgs.Query.Set(apc.QparamOrigURL, uri)
	}
	// TODO: validate
	if archPath != "" {
		if getArgs.Query == nil {
			getArgs.Query = make(url.Values, 1)
		}
		getArgs.Query.Set(apc.QparamArchpath, archPath)
	}

	if flagIsSet(c, cksumFlag) {
		oah, err = api.GetObjectWithValidation(apiBP, bck, objName, &getArgs)
	} else {
		oah, err = api.GetObject(apiBP, bck, objName, &getArgs)
	}
	if err != nil {
		if cmn.IsStatusNotFound(err) && archPath == "" {
			err = fmt.Errorf("object \"%s/%s\" does not exist", bck, objName)
		}
		return
	}
	objLen := oah.Size()

	// print result (variations)
	sz := teb.FmtSize(objLen, units, 2)
	if flagIsSet(c, lengthFlag) && outFile != fileStdIO {
		fmt.Fprintf(c.App.ErrWriter, "Read range len=%s (%dB) as %q\n", sz, objLen, outFile)
		return
	}
	if silent || outFile == fileStdIO {
		return
	}
	bn := bck.DisplayName()
	if outFile == discardIO {
		if archPath != "" {
			fmt.Fprintf(c.App.Writer, "GET and discard: %q from archive \"%s/%s\" (size %s)\n",
				archPath, bn, objName, sz)
		} else {
			fmt.Fprintf(c.App.Writer, "GET and discard: %q from %s (size %s)\n", objName, bn, sz)
		}
		return
	}
	if archPath != "" {
		fmt.Fprintf(c.App.Writer, "GET %q from archive \"%s/%s\" as %q (size %s)\n",
			archPath, bn, objName, outFile, sz)
	} else {
		fmt.Fprintf(c.App.Writer, "GET %q from %s as %q (size %s)\n", objName, bn, outFile, sz)
	}
	return
}
