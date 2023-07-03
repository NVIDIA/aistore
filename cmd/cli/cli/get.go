// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles object operations.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb/v4"
)

func catHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	// source
	uri := c.Args().Get(0)
	bck, objName, err := parseBckObjURI(c, uri, false)
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
	bck, objName, err := parseBckObjURI(c, uri, flagIsSet(c, getObjPrefixFlag))
	if err != nil {
		return err
	}
	// destination (empty "" implies using source `basename`)
	outFile := c.Args().Get(1)

	//
	// TODO -- FIXME: `ais archive get` must have its own flags
	//
	if flagIsSet(c, archpathGetFlag) && flagIsSet(c, extractFlag) {
		return incorrectUsageMsg(c, errFmtExclusive, qflprn(archpathGetFlag), qflprn(extractFlag))
	}
	if flagIsSet(c, checkObjCachedFlag) && flagIsSet(c, extractFlag) {
		return incorrectUsageMsg(c, errFmtExclusive, qflprn(checkObjCachedFlag), qflprn(archpathGetFlag))
	}
	if flagIsSet(c, offsetFlag) && flagIsSet(c, extractFlag) {
		return incorrectUsageMsg(c, errFmtExclusive, qflprn(offsetFlag), qflprn(archpathGetFlag))
	}
	if flagIsSet(c, archpathGetFlag) && flagIsSet(c, checkObjCachedFlag) {
		return fmt.Errorf("checking presence (%s) of archived files (%s) is not implemented yet",
			qflprn(checkObjCachedFlag), qflprn(archpathGetFlag))
	}
	if flagIsSet(c, getObjPrefixFlag) && (flagIsSet(c, archpathGetFlag) || flagIsSet(c, extractFlag)) {
		return fmt.Errorf("extracting content (%s, %s) from multiple objects (%s) is not implemented yet",
			qflprn(archpathGetFlag), qflprn(extractFlag), qflprn(getObjPrefixFlag))
	}

	// GET multiple
	if flagIsSet(c, getObjPrefixFlag) {
		if objName != "" {
			return fmt.Errorf("object name in %q and %s cannot be used together (hint: use directory as destination)",
				uri, qflprn(getObjPrefixFlag))
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
		prefix   = parseStrFlag(c, getObjPrefixFlag)
		msg      = &apc.LsoMsg{Prefix: prefix}
		listArch = flagIsSet(c, listArchFlag) // archived content
	)
	// setup list-objects msg and call
	msg.AddProps(apc.GetPropsMinimal...)
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
	objList, err := api.ListObjects(apiBP, bck, msg, api.ListArgs{Num: uint(limit)})
	if err != nil {
		return V(err)
	}
	// can't do many to one
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
	// total size
	var totalSize int64
	for _, entry := range objList.Entries {
		totalSize += entry.Size
	}
	// announce, confirm
	var (
		silent      = !flagIsSet(c, verboseFlag)
		units, errU = parseUnitsFlag(c, unitsFlag)
	)
	if errU != nil {
		return err
	}
	cptn := fmt.Sprintf("GET %d object%s from %s to %s (total size %s)",
		l, cos.Plural(l), bck.Cname(""), outFile, teb.FmtSize(totalSize, units, 2))
	if flagIsSet(c, yesFlag) && (l > 1 || silent) {
		fmt.Fprintln(c.App.Writer, cptn)
	} else if ok := confirm(c, cptn); !ok {
		return nil
	}
	// context to get in parallel
	u := &uctx{
		showProgress: flagIsSet(c, progressFlag),
		wg:           cos.NewLimitedWaitGroup(4, 0),
	}
	if u.showProgress {
		var (
			filesBarArg = barArgs{ // bar[0]
				total:   int64(len(objList.Entries)),
				barText: "Objects:    ",
				barType: unitsArg,
			}
			sizeBarArg = barArgs{ // bar[1]
				total:   totalSize,
				barText: "Total size: ",
				barType: sizeArg,
			}
			totalBars []*mpb.Bar
		)
		u.progress, totalBars = simpleBar(filesBarArg, sizeBarArg)
		u.barObjs = totalBars[0]
		u.barSize = totalBars[1]
	}
	for _, entry := range objList.Entries {
		u.wg.Add(1)
		go u.get(c, bck, entry.Name, outFile, entry.Size, silent)
	}
	u.wg.Wait()

	if u.showProgress {
		u.progress.Wait()
		fmt.Fprint(c.App.Writer, u.errSb.String())
	}
	if numFailed := u.errCount.Load(); numFailed > 0 {
		return fmt.Errorf("failed to GET %d object%s", numFailed, cos.Plural(int(numFailed)))
	}
	return nil
}

//////////
// uctx - "get" extension
//////////

func (u *uctx) get(c *cli.Context, bck cmn.Bck, objName, outFile string, size int64, silent bool) {
	defer u.wg.Done()
	err := getObject(c, bck, objName, outFile, silent)
	if err != nil {
		u.errCount.Inc()
	}
	if u.showProgress {
		u.barObjs.IncrInt64(1)
		u.barSize.IncrInt64(size)
		if err != nil {
			u.errSb.WriteString(err.Error() + "\n")
		}
	} else if err != nil {
		actionWarn(c, err.Error())
	}
}

func getObject(c *cli.Context, bck cmn.Bck, objName, outFile string, silent bool) (err error) {
	var (
		getArgs api.GetArgs
		oah     api.ObjAttrs
		units   string
	)
	if outFile == fileStdIO && flagIsSet(c, extractFlag) {
		return fmt.Errorf("cannot extract archived files (%s) to standard output - not implemented yet",
			qflprn(extractFlag))
	}
	if outFile == discardIO && flagIsSet(c, extractFlag) {
		return fmt.Errorf("cannot extract (%s) and discard archived files - not implemented yet",
			qflprn(extractFlag))
	}

	// just check if a remote object is present (do not GET)
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
	if offset, err = parseSizeFlag(c, offsetFlag, units); err != nil {
		return
	}
	if length, err = parseSizeFlag(c, lengthFlag, units); err != nil {
		return
	}

	// where to
	archpath := parseStrFlag(c, archpathGetFlag)
	if outFile == "" {
		// archive
		if archpath != "" {
			outFile = filepath.Base(archpath)
		} else {
			outFile = filepath.Base(objName)
		}
	} else if outFile != fileStdIO && outFile != discardIO {
		finfo, errEx := os.Stat(outFile)
		if errEx == nil {
			if !finfo.IsDir() && flagIsSet(c, extractFlag) {
				return fmt.Errorf("cannot extract (%s) archived files - destination %q exists and is not a directory",
					qflprn(extractFlag), outFile)
			}
			// destination is: directory | file (confirm overwrite)
			if finfo.IsDir() {
				// archive
				if archpath != "" {
					outFile = filepath.Join(outFile, filepath.Base(archpath))
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
	if archpath != "" {
		if getArgs.Query == nil {
			getArgs.Query = make(url.Values, 1)
		}
		getArgs.Query.Set(apc.QparamArchpath, archpath)
	}

	if flagIsSet(c, cksumFlag) {
		oah, err = api.GetObjectWithValidation(apiBP, bck, objName, &getArgs)
	} else {
		oah, err = api.GetObject(apiBP, bck, objName, &getArgs)
	}
	if err != nil {
		if cmn.IsStatusNotFound(err) && archpath == "" {
			err = fmt.Errorf("%q does not exist", bck.Cname(objName))
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
	bn := bck.Cname("")
	if outFile == discardIO {
		if archpath != "" {
			fmt.Fprintf(c.App.Writer, "GET (and immediately discard) %s from %s (%s)\n",
				archpath, bck.Cname(objName), sz)
		} else {
			fmt.Fprintf(c.App.Writer, "GET (and immediately discard) %s from %s (%s)\n", objName, bn, sz)
		}
		return
	}

	if flagIsSet(c, extractFlag) {
		goto extract
	}
	if archpath != "" {
		fmt.Fprintf(c.App.Writer, "GET %s from %s as %q (%s)\n", archpath, bck.Cname(objName), outFile, sz)
	} else {
		fmt.Fprintf(c.App.Writer, "GET %s from %s as %q (%s)\n", objName, bn, outFile, sz)
	}
	return

extract:
	var (
		mime string
		rfh  *os.File
		ar   archive.Reader
	)
	if mime, err = archive.MimeFile(rfh, nil, "", objName); err != nil {
		return nil // skip silently: consider non-extractable and Ok
	}
	if rfh, err = os.Open(outFile); err != nil {
		return
	}
	if ar, err = archive.NewReader(mime, rfh, oah.Size()); err != nil {
		return
	}
	x := &extractor{outFile}
	fmt.Fprintf(c.App.Writer, "GET %s from %s as %q (%s) and extract to %s/\n",
		objName, bn, outFile, sz, cos.Basename(outFile))
	_, err = ar.Range("", x.do)
	rfh.Close()
	return err
}

type extractor struct {
	shardName string
}

func (x *extractor) do(filename string, size int64, reader io.ReadCloser, _ any) (bool /*stop*/, error) {
	fqn := filepath.Join(cos.Basename(x.shardName), filename)

	wfh, err := cos.CreateFile(fqn)
	if err != nil {
		reader.Close()
		return true, err
	}
	stop, err := x._write(filename, size, wfh, reader)
	reader.Close()
	wfh.Close()
	if err != nil {
		os.Remove(fqn)
	}
	return stop, err
}

func (x *extractor) _write(filename string, size int64, wfh *os.File, reader io.ReadCloser) (bool /*stop*/, error) {
	n, err := io.Copy(wfh, reader)
	if err != nil {
		return true, err
	}
	if n != size {
		return true, fmt.Errorf("failed to extract %s from %s: wrong size (%d vs %d)", filename, x.shardName, n, size)
	}
	return false, nil
}
