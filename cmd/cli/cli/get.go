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
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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
	archpath := parseStrFlag(c, archpathGetFlag)
	return getObject(c, bck, objName, archpath, fileStdIO, true /*silent*/)
}

func getHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	if flagIsSet(c, lengthFlag) != flagIsSet(c, offsetFlag) {
		return fmt.Errorf("%s and %s must be both present (or not)", qflprn(lengthFlag), qflprn(offsetFlag))
	}

	// source
	uri := c.Args().Get(0)
	bck, objName, err := parseBckObjURI(c, uri, flagIsSet(c, getObjPrefixFlag))
	if err != nil {
		return err
	}
	// destination (empty "" implies using source `basename`)
	outFile := c.Args().Get(1)

	// extraction (from archives) has current limitations
	var archpath string
	if flagIsSet(c, archpathGetFlag) {
		archpath = parseStrFlag(c, archpathGetFlag)
		if archpath != "" {
			if flagIsSet(c, extractFlag) {
				return fmt.Errorf(errFmtExclusive, qflprn(archpathGetFlag), qflprn(extractFlag))
			}
			if flagIsSet(c, getObjPrefixFlag) {
				return fmt.Errorf(errFmtExclusive, qflprn(getObjPrefixFlag), qflprn(archpathGetFlag))
			}
			if flagIsSet(c, checkObjCachedFlag) {
				return fmt.Errorf("checking presence (%s) of archived files (%s) is not implemented yet",
					qflprn(checkObjCachedFlag), qflprn(archpathGetFlag))
			}
			if flagIsSet(c, lengthFlag) {
				return fmt.Errorf("read range (%s, %s) of archived files (%s) is not implemented yet",
					qflprn(lengthFlag), qflprn(offsetFlag), qflprn(archpathGetFlag))
			}
		}
	}
	if flagIsSet(c, extractFlag) {
		if flagIsSet(c, checkObjCachedFlag) {
			return fmt.Errorf(errFmtExclusive, qflprn(checkObjCachedFlag), qflprn(extractFlag))
		}
		if flagIsSet(c, lengthFlag) {
			return fmt.Errorf("read range (%s, %s) of archived files (%s) is not implemented yet",
				qflprn(lengthFlag), qflprn(offsetFlag), qflprn(extractFlag))
		}
	}

	// GET multiple -- currently, only prefix (TODO: list/range)
	if flagIsSet(c, getObjPrefixFlag) {
		if objName != "" {
			if _, err := archive.Mime("", objName); err != nil {
				// not an archive
				return fmt.Errorf("object name in %q and %s cannot be used together (hint: use directory as destination)",
					uri, qflprn(getObjPrefixFlag))
			}
		}
		return getMultiObj(c, bck, outFile) // calls `getObject` with progress bar and bells and whistles..
	}

	// GET
	if !bck.IsHTTP() {
		if _, err = headBucket(bck, false /* don't add */); err != nil {
			return err
		}
	}
	return getObject(c, bck, objName, archpath, outFile, false /*silent*/)
}

// GET multiple -- currently, only prefix (TODO: list/range)
func getMultiObj(c *cli.Context, bck cmn.Bck, outFile string) error {
	var (
		prefix     = parseStrFlag(c, getObjPrefixFlag)
		origPrefix = prefix
		lstFilter  = &lstFilter{}
		listArch   = flagIsSet(c, listArchFlag) ||
			flagIsSet(c, archpathGetFlag) || flagIsSet(c, extractFlag) // when getting from arch is implied
	)
	if listArch && prefix != "" {
		// when prefix crosses shard boundary
		if external, internal := splitPrefix(prefix); len(internal) > 0 {
			prefix = external
			debug.Assert(prefix != origPrefix)

			if flagIsSet(c, extractFlag) {
				return fmt.Errorf("when getting multiple archived files flag %s is redundant (implied)", qflprn(extractFlag))
			}
			lstFilter._add(func(obj *cmn.LsoEntry) bool { return obj.Name == external || strings.HasPrefix(obj.Name, origPrefix) })
		}
	}

	// setup list-objects control msg and api call
	msg := &apc.LsoMsg{Prefix: prefix}
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
	if lstFilter._len() > 0 {
		objList.Entries, _ = lstFilter.apply(objList.Entries)
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
		discard, out string
		verb         = "GET"
		silent       = !flagIsSet(c, verboseFlag) // silent is non-verbose default
		units, errU  = parseUnitsFlag(c, unitsFlag)
	)
	if errU != nil {
		return err
	}

	if outFile == discardIO {
		discard = " (and discard)"
	} else if outFile == fileStdIO {
		out = " to standard output"
	} else {
		out = outFile
		if out != "" && out[len(out)-1] == filepath.Separator {
			out = out[:len(out)-1]
		}
	}
	if flagIsSet(c, lengthFlag) {
		verb = "Read range"
	}

	cptn := fmt.Sprintf("%s%s %d object%s from %s%s (total size %s)",
		verb, discard, l, cos.Plural(l), bck.Cname(""), out, teb.FmtSize(totalSize, units, 2))

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
		var shardName string
		if entry.IsInsideArch() {
			if origPrefix != msg.Prefix {
				if !strings.HasPrefix(entry.Name, origPrefix) {
					// skip
					if u.showProgress {
						u.barObjs.IncrInt64(1)
						u.barSize.IncrInt64(entry.Size)
					}
					continue
				}
			}
			for _, shardEntry := range objList.Entries {
				if shardEntry.IsListedArch() && strings.HasPrefix(entry.Name, shardEntry.Name+"/") {
					shardName = shardEntry.Name
					break
				}
			}
			if shardName == "" {
				// should not be happening
				warn := fmt.Sprintf("archived file %q: cannot find parent shard in the listed results", entry.Name)
				actionWarn(c, warn)
				continue
			}
		}
		u.wg.Add(1)
		go u.get(c, bck, entry, shardName, outFile, silent)
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

func (u *uctx) get(c *cli.Context, bck cmn.Bck, entry *cmn.LsoEntry, shardName, outFile string, silent bool) {
	var (
		objName  = entry.Name
		archpath string
	)
	if shardName != "" {
		objName = shardName
		archpath = strings.TrimPrefix(entry.Name, shardName+"/")
		if outFile != fileStdIO && outFile != discardIO {
			// when getting multiple retain the full archpath
			// (compare w/ filepath.Base usage otherwise)
			outFile = filepath.Join(outFile, archpath)
			if err := cos.CreateDir(filepath.Dir(outFile)); err != nil {
				actionWarn(c, err.Error())
			}
		}
	}
	err := getObject(c, bck, objName, archpath, outFile, silent)
	if err != nil {
		u.errCount.Inc()
	}
	if u.showProgress {
		u.barObjs.IncrInt64(1)
		u.barSize.IncrInt64(entry.Size)
		if err != nil {
			u.errSb.WriteString(err.Error() + "\n")
		}
	} else if err != nil {
		actionWarn(c, err.Error())
	}

	u.wg.Done()
}

// get one (main function)
func getObject(c *cli.Context, bck cmn.Bck, objName, archpath, outFile string, silent bool) (err error) {
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
	if offset, err = parseSizeFlag(c, offsetFlag, units); err != nil {
		return
	}
	if length, err = parseSizeFlag(c, lengthFlag, units); err != nil {
		return
	}

	// where to
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

	if flagIsSet(c, extractFlag) {
		err = extract(objName, outFile, objLen)
		if err != nil {
			return fmt.Errorf("failed to extract %s: %v", bck.Cname(objName), err)
		}
	}

	if silent {
		return
	}

	//
	// print result - variations
	//

	var (
		discard, out string
		sz           = teb.FmtSize(objLen, units, 2)
		bn           = bck.Cname("")
	)
	if outFile == discardIO {
		discard = " (and discard)"
	} else if outFile == fileStdIO {
		out = " to standard output"
	} else {
		out = " as " + cos.Basename(outFile)
		if out[len(out)-1] == filepath.Separator {
			out = out[:len(out)-1]
		}
	}
	switch {
	case flagIsSet(c, lengthFlag):
		fmt.Fprintf(c.App.Writer, "Read%s range (length %s (%dB) at offset %d)%s\n", discard, sz, objLen, offset, out)
	case archpath != "":
		fmt.Fprintf(c.App.Writer, "GET%s %s from %s%s (%s)\n", discard, archpath, bck.Cname(objName), out, sz)
	case flagIsSet(c, extractFlag):
		fmt.Fprintf(c.App.Writer, "GET %s from %s as %q (%s) and extract%s\n", objName, bn, outFile, sz, out)
	default:
		fmt.Fprintf(c.App.Writer, "GET%s %s from %s%s (%s)\n", discard, objName, bn, out, sz)
	}
	return
}

//
// post-GET extraction
//

type extractor struct {
	shardName string
}

func extract(objName, outFile string, objLen int64) (err error) {
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
	if ar, err = archive.NewReader(mime, rfh, objLen); err != nil {
		return
	}

	ex := &extractor{outFile}

	_, err = ar.Range("", ex.do)

	rfh.Close()
	return err
}

func (ex *extractor) do(filename string, reader cos.ReadCloseSizer, _ any) (bool /*stop*/, error) {
	fqn := filepath.Join(cos.Basename(ex.shardName), filename)

	wfh, err := cos.CreateFile(fqn)
	if err != nil {
		reader.Close()
		return true, err
	}
	stop, err := ex._write(filename, reader.Size(), wfh, reader)
	reader.Close()
	wfh.Close()
	if err != nil {
		os.Remove(fqn)
	}
	return stop, err
}

func (ex *extractor) _write(filename string, size int64, wfh *os.File, reader io.ReadCloser) (bool /*stop*/, error) {
	n, err := io.Copy(wfh, reader)
	if err != nil {
		return true, err
	}
	if n != size {
		return true, fmt.Errorf("failed to extract %s from %s: wrong size (%d vs %d)",
			filename, ex.shardName, n, size)
	}
	return false, nil
}
