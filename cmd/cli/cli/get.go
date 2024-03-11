// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles object operations.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
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

const (
	fileStdIO = "-" // STDIN (for `ais put`), STDOUT (for `ais put`)
)

const extractVia = "--extract(*)"

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
	return getObject(c, bck, objName, archpath, fileStdIO, true /*quiet*/, false /*extract*/)
}

func getHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	if flagIsSet(c, lengthFlag) != flagIsSet(c, offsetFlag) {
		return fmt.Errorf("%s and %s must be both present (or not)", qflprn(lengthFlag), qflprn(offsetFlag))
	}
	if flagIsSet(c, latestVerFlag) {
		if flagIsSet(c, headObjPresentFlag) {
			return fmt.Errorf(errFmtExclusive, qflprn(latestVerFlag), qflprn(headObjPresentFlag))
		}
		if flagIsSet(c, getObjCachedFlag) {
			return fmt.Errorf(errFmtExclusive, qflprn(latestVerFlag), qflprn(getObjCachedFlag))
		}
	}

	// source
	uri := c.Args().Get(0)
	bck, objName, err := parseBckObjURI(c, uri, flagIsSet(c, getObjPrefixFlag))
	if err != nil {
		return err
	}
	if !bck.IsHTTP() {
		if bck.Props, err = headBucket(bck, false /* don't add */); err != nil {
			return err
		}
	}
	if flagIsSet(c, latestVerFlag) && !bck.HasVersioningMD() {
		return fmt.Errorf("option %s is incompatible with the specified bucket %s\n"+
			"(tip: can only GET latest object's version from a bucket with Cloud or remote AIS backend)",
			qflprn(latestVerFlag), bck.String())
	}

	if flagIsSet(c, blobDownloadFlag) {
		if flagIsSet(c, lengthFlag) {
			return fmt.Errorf(errFmtExclusive, qflprn(lengthFlag), qflprn(blobDownloadFlag))
		}
		if flagIsSet(c, getObjCachedFlag) {
			return fmt.Errorf(errFmtExclusive, qflprn(getObjCachedFlag), qflprn(blobDownloadFlag))
		}
		if flagIsSet(c, archpathGetFlag) {
			return errors.New("cannot use blob downloader to read archived files - not implemented yet")
		}
		if !bck.IsRemote() {
			return fmt.Errorf("blob downloader: expecting remote bucket (have %s)", bck.Cname(""))
		}
		if flagIsSet(c, progressFlag) {
			// TODO: niy
			return fmt.Errorf("cannot show progress when running GET via blob-downloader (tip: try 'ais %s %s --progress')",
				cmdBlobDownload, uri)
		}
	}

	// destination (empty "" implies using source `basename`)
	outFile := c.Args().Get(1)

	// extract all or (archpath) selected
	var (
		archpath string
		extract  bool
	)
	if flagIsSet(c, archpathGetFlag) {
		archpath = parseStrFlag(c, archpathGetFlag)
	}
	if actionIsHandler(c.Command.Action, getArchHandler) {
		extract = true // extractFlag is implied unless:

		if archpath != "" {
			extract = false
		} else if oname, fname := splitObjnameShardBoundary(objName); fname != "" {
			objName = oname
			archpath = fname
			extract = false
		}
	} else {
		extract = flagIsSet(c, extractFlag)
	}

	// validate extract and archpath
	if extract {
		if flagIsSet(c, headObjPresentFlag) {
			return fmt.Errorf(errFmtExclusive, extractVia, qflprn(headObjPresentFlag))
		}
		if flagIsSet(c, lengthFlag) {
			return fmt.Errorf("read range (%s, %s) of archived files (%s) is not implemented yet",
				qflprn(lengthFlag), qflprn(offsetFlag), extractVia)
		}
	}
	if archpath != "" {
		if flagIsSet(c, getObjPrefixFlag) {
			return fmt.Errorf(errFmtExclusive, qflprn(getObjPrefixFlag), qflprn(archpathGetFlag))
		}
		if flagIsSet(c, headObjPresentFlag) {
			return fmt.Errorf("checking presence (%s) of archived files (%s) is not implemented yet",
				qflprn(headObjPresentFlag), qflprn(archpathGetFlag))
		}
		if flagIsSet(c, lengthFlag) {
			return fmt.Errorf("read range (%s, %s) of archived files (%s) is not implemented yet",
				qflprn(lengthFlag), qflprn(offsetFlag), qflprn(archpathGetFlag))
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
		// calls `getObject` with progress bar and bells and whistles
		return getMultiObj(c, bck, archpath, outFile, extract)
	}

	// GET
	return getObject(c, bck, objName, archpath, outFile, false /*quiet*/, extract)
}

// GET multiple -- currently, only prefix (TODO: list/range)
func getMultiObj(c *cli.Context, bck cmn.Bck, archpath, outFile string, extract bool) error {
	var (
		prefix     = parseStrFlag(c, getObjPrefixFlag)
		origPrefix = prefix
		lstFilter  = &lstFilter{}
	)
	if flagIsSet(c, listArchFlag) && prefix != "" {
		// when prefix crosses shard boundary
		if external, internal := splitPrefixShardBoundary(prefix); internal != "" {
			prefix = external
			debug.Assert(prefix != origPrefix)
			lstFilter._add(func(obj *cmn.LsoEntry) bool {
				return obj.Name == external || strings.HasPrefix(obj.Name, origPrefix)
			})
		}
	}

	// setup lsmsg
	msg := &apc.LsoMsg{Prefix: prefix}
	msg.AddProps(apc.GetPropsMinimal...)
	if flagIsSet(c, listArchFlag) || extract || archpath != "" {
		msg.SetFlag(apc.LsArchDir)
	}
	if flagIsSet(c, getObjCachedFlag) {
		msg.SetFlag(apc.LsObjCached)
	}
	pageSize, limit, err := _setPage(c, bck)
	if err != nil {
		return err
	}
	msg.PageSize = pageSize

	// setup lsargs
	lsargs := api.ListArgs{Limit: limit}
	if flagIsSet(c, useInventoryFlag) {
		lsargs.Header = http.Header{
			apc.HdrInventory: []string{"true"},
			apc.HdrInvName:   []string{parseStrFlag(c, invNameFlag)},
			apc.HdrInvID:     []string{parseStrFlag(c, invIDFlag)},
		}
	}

	// list-objects
	objList, err := api.ListObjects(apiBP, bck, msg, lsargs)
	if err != nil {
		return V(err)
	}
	if lstFilter._len() > 0 {
		objList.Entries, _ = lstFilter.apply(objList.Entries)
	}

	// can't do many to one
	l := len(objList.Entries)
	if l > 1 {
		if outFile != "" && outFile != fileStdIO && !discardOutput(outFile) {
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
		quiet        = !flagIsSet(c, verboseFlag) // quiet is non-verbose default
		units, errU  = parseUnitsFlag(c, unitsFlag)
	)
	if errU != nil {
		return err
	}

	if discardOutput(outFile) {
		discard = " (and discard)"
	} else if outFile == fileStdIO {
		out = " to standard output"
	} else {
		out = outFile
		if out != "" {
			out = cos.TrimLastB(out, filepath.Separator)
		}
	}
	if flagIsSet(c, lengthFlag) {
		verb = "Read range"
	}

	cptn := fmt.Sprintf("%s%s %d object%s from %s%s (total size %s)",
		verb, discard, l, cos.Plural(l), bck.Cname(""), out, teb.FmtSize(totalSize, units, 2))

	if flagIsSet(c, yesFlag) && (l > 1 || quiet) {
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

		// NOTE: s3.ListObjectsV2 _may_ return a directory - filtering out
		if err := cmn.ValidateObjName(entry.Name); err != nil {
			warn := fmt.Sprintf("%v in the list-objects results (ignored)", err)
			actionNote(c, warn)
			continue
		}

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
		go u.get(c, bck, entry, shardName, outFile, quiet, extract)
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

func (u *uctx) get(c *cli.Context, bck cmn.Bck, entry *cmn.LsoEntry, shardName, outFile string, quiet, extract bool) {
	var (
		objName  = entry.Name
		archpath string
	)
	if shardName != "" {
		objName = shardName
		archpath = strings.TrimPrefix(entry.Name, shardName+"/")
		if outFile != fileStdIO && !discardOutput(outFile) {
			// when getting multiple files retain the full archpath
			// (compare w/ filepath.Base usage otherwise)
			outFile = filepath.Join(outFile, archpath)
			if err := cos.CreateDir(filepath.Dir(outFile)); err != nil {
				actionWarn(c, err.Error())
			}
		}
	}
	err := getObject(c, bck, objName, archpath, outFile, quiet, extract)
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
func getObject(c *cli.Context, bck cmn.Bck, objName, archpath, outFile string, quiet, extract bool) (err error) {
	var (
		getArgs api.GetArgs
		oah     api.ObjAttrs
		units   string
	)
	if outFile == fileStdIO && extract {
		return errors.New("cannot extract archived files to standard output - not implemented yet")
	}
	if discardOutput(outFile) && extract {
		return errors.New("cannot extract and discard archived files - not implemented yet")
	}
	if flagIsSet(c, listArchFlag) && archpath == "" {
		if external, internal := splitPrefixShardBoundary(objName); internal != "" {
			objName, archpath = external, internal
		}
	}

	// just check if a remote object is present (do not GET)
	if flagIsSet(c, headObjPresentFlag) {
		return isObjPresent(c, bck, objName)
	}

	units, err = parseUnitsFlag(c, unitsFlag)
	if err != nil {
		return err
	}

	var offset, length int64
	if offset, err = parseSizeFlag(c, offsetFlag, units); err != nil {
		return err
	}
	if length, err = parseSizeFlag(c, lengthFlag, units); err != nil {
		return err
	}

	// where to
	if outFile == "" {
		// archive
		if archpath != "" {
			outFile = filepath.Base(archpath)
		} else {
			outFile = filepath.Base(objName)
		}
	} else if outFile != fileStdIO && !discardOutput(outFile) {
		finfo, errEx := os.Stat(outFile)
		if errEx == nil {
			if !finfo.IsDir() && extract {
				return fmt.Errorf("cannot extract archived files - destination %q exists and is not a directory", outFile)
			}
			// destination is: directory | file (confirm overwrite)
			if finfo.IsDir() {
				// archive
				if archpath != "" {
					outFile = filepath.Join(outFile, filepath.Base(archpath))
				} else {
					outFile = filepath.Join(outFile, filepath.Base(objName))
				}
				// TODO: strictly speaking: fstat again and confirm if exists
			} else if finfo.Mode().IsRegular() && !flagIsSet(c, yesFlag) { // `/dev/null` is fine
				warn := fmt.Sprintf("overwrite existing %q", outFile)
				if ok := confirm(c, warn); !ok {
					return nil
				}
			}
		}
	}

	var hdr http.Header
	if length > 0 {
		rng := cmn.MakeRangeHdr(offset, length)
		hdr = http.Header{cos.HdrRange: []string{rng}}
	}
	if flagIsSet(c, blobDownloadFlag) {
		debug.Assert(length == 0) // checked above
		hdr = http.Header{apc.HdrBlobDownload: []string{"true"}}
		if flagIsSet(c, chunkSizeFlag) {
			if _, err := parseSizeFlag(c, chunkSizeFlag); err != nil {
				return err
			}
			hdr.Set(apc.HdrBlobChunk, parseStrFlag(c, chunkSizeFlag))
		}
		if flagIsSet(c, numWorkersFlag) {
			nw := parseIntFlag(c, numWorkersFlag)
			if nw <= 0 || nw > 128 {
				return fmt.Errorf("invalid %s=%d: expecting (1..128) range", flprn(numWorkersFlag), nw)
			}
			hdr.Set(apc.HdrBlobWorkers, strconv.Itoa(nw))
		}
	} else if flagIsSet(c, chunkSizeFlag) || flagIsSet(c, numWorkersFlag) {
		return fmt.Errorf("command line options (%s, %s) can be used only together with %s",
			qflprn(chunkSizeFlag), qflprn(numWorkersFlag), qflprn(blobDownloadFlag))
	}

	if outFile == fileStdIO {
		getArgs = api.GetArgs{Writer: os.Stdout, Header: hdr}
		quiet = true
	} else if discardOutput(outFile) {
		getArgs = api.GetArgs{Writer: io.Discard, Header: hdr}
	} else {
		var file *os.File
		if file, err = os.Create(outFile); err != nil {
			return err
		}
		defer func() {
			file.Close()
			if err != nil {
				os.Remove(outFile)
			}
		}()
		getArgs = api.GetArgs{Writer: file, Header: hdr}
	}

	// finally, http query
	if bck.IsHTTP() || archpath != "" || flagIsSet(c, silentFlag) || flagIsSet(c, latestVerFlag) {
		getArgs.Query = _getQparams(c, &bck, archpath)
	}

	// do
	if flagIsSet(c, cksumFlag) {
		oah, err = api.GetObjectWithValidation(apiBP, bck, objName, &getArgs)
	} else {
		oah, err = api.GetObject(apiBP, bck, objName, &getArgs)
	}
	if err != nil {
		if cmn.IsStatusNotFound(err) && archpath == "" {
			err = &errDoesNotExist{what: "object", name: bck.Cname(objName)}
		}
		return err
	}

	var (
		mime   string
		objLen = oah.Size()
	)
	if extract {
		mime, err = doExtract(objName, outFile, objLen)
		if err != nil {
			if cliConfVerbose() {
				return fmt.Errorf("failed to extract %s (from local %q): %v", bck.Cname(objName), outFile, err)
			}
			return fmt.Errorf("failed to extract %s: %v", bck.Cname(objName), err)
		}
	}

	if quiet {
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
	switch {
	case discardOutput(outFile):
		discard = " (and discard)"
	case outFile == fileStdIO:
		out = " to standard output"
	case extract:
		out = " to " + outFile
		out = cos.TrimLastB(out, filepath.Separator)
		out = strings.TrimSuffix(out, mime) + "/"
	default:
		out = " as " + outFile
		out = cos.TrimLastB(out, filepath.Separator)
	}
	switch {
	case flagIsSet(c, lengthFlag):
		fmt.Fprintf(c.App.Writer, "Read%s range (length %s (%dB) at offset %d)%s\n", discard, sz, objLen, offset, out)
	case archpath != "":
		fmt.Fprintf(c.App.Writer, "GET%s %s from %s%s (%s)\n", discard, archpath, bck.Cname(objName), out, sz)
	case extract:
		fmt.Fprintf(c.App.Writer, "GET %s from %s as %q (%s) and extract%s\n", objName, bn, outFile, sz, out)
	default:
		fmt.Fprintf(c.App.Writer, "GET%s %s from %s%s (%s)\n", discard, objName, bn, out, sz)
	}
	return
}

func _getQparams(c *cli.Context, bck *cmn.Bck, archpath string) (q url.Values) {
	q = make(url.Values, 2)
	if bck.IsHTTP() {
		uri := c.Args().Get(0)
		q.Set(apc.QparamOrigURL, uri)
	}
	if archpath != "" {
		q.Set(apc.QparamArchpath, archpath)
	}
	if flagIsSet(c, silentFlag) {
		q.Set(apc.QparamSilent, "true")
	}
	if flagIsSet(c, latestVerFlag) {
		q.Set(apc.QparamLatestVer, "true")
	}
	return q
}

//
// post-GET extraction
//

type extractor struct {
	shardName string
	mime      string
}

func doExtract(objName, outFile string, objLen int64) (mime string, err error) {
	var (
		rfh *os.File
		ar  archive.Reader
	)
	if mime, err = archive.Mime("", objName); err != nil {
		return "", nil // skip silently: consider non-extractable and Ok
	}
	if rfh, err = os.Open(outFile); err != nil {
		return
	}
	if ar, err = archive.NewReader(mime, rfh, objLen); err != nil {
		return
	}

	ex := &extractor{outFile, mime}
	_, err = ar.Range("", ex.do)
	rfh.Close()
	return
}

func (ex *extractor) do(filename string, reader cos.ReadCloseSizer, _ any) (bool /*stop*/, error) {
	fqn := filepath.Join(strings.TrimSuffix(ex.shardName, ex.mime), filename)

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

// discard
func discardOutput(outf string) bool {
	return outf == "/dev/null" || outf == "dev/null" || outf == "dev/nil"
}
