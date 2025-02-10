// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles object operations.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb/v4"
)

const (
	fileStdIO = "-" // STDIN (for `ais put`), STDOUT (for `ais put`)
)

const extractVia = "--extract(*)"

type qparamArch struct {
	archpath string // apc.QparamArchpath
	archmime string // apc.QparamArchmime
	archregx string // apc.QparamArchregx
	archmode string // apc.QparamArchmode
}

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
	a := qparamArch{archpath: parseStrFlag(c, archpathGetFlag)}
	return getObject(c, bck, objName, fileStdIO, a, true /*quiet*/, false /*extract*/)
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
	if shouldHeadRemote(c, bck) {
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
			return errors.New("cannot use blob downloader to read archived files - " + NIY)
		}
		if !bck.IsRemote() {
			return fmt.Errorf("blob downloader: expecting remote bucket (have %s)", bck.Cname(""))
		}
		if flagIsSet(c, progressFlag) {
			tip := fmt.Sprintf("Tip: try 'ais %s %s --progress')", cmdBlobDownload, uri)
			return fmt.Errorf("cannot show progress when running GET via blob-downloader - "+NIY+"\n(%s)", tip)
		}
	}

	// destination (empty "" implies using source `basename`)
	outFile := c.Args().Get(1)

	// archive
	var (
		a       qparamArch
		extract bool
	)
	if err := a.init(c); err != nil {
		return err
	}

	extract = flagIsSet(c, extractFlag)

	// NOTE: unlike 'ais get' 'ais archive get' further implies:
	// - full extraction of the source shard when neither single- nor multi-selection specified;
	// - implicit archpath given one of the supported archival extensions in the source name

	if actionIsHandler(c.Command.Action, getArchHandler) {
		if a.archpath == "" {
			if oname, fname := splitObjnameShardBoundary(objName); fname != "" {
				objName = oname
				a.archpath = fname
				// revalidate
				if _, err := archive.ValidateMatchMode(a.archmode); err != nil {
					return err
				}
			}
		}
		if a.archpath == "" && a.archmode == "" {
			extract = true
		}
	}

	// validate '--extract' and '--archpath' vs other command line
	if extract {
		if flagIsSet(c, headObjPresentFlag) {
			return fmt.Errorf(errFmtExclusive, extractVia, qflprn(headObjPresentFlag))
		}
		if flagIsSet(c, lengthFlag) {
			return errRangeReadArch(extractVia)
		}
	}
	if err := a.validate(c); err != nil {
		return err
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

		// `getMultiObj` is a fusion of 'ais ls' and GET, with progress bar and a
		// very limited archival support (that boils down to listing archived files)
		// TODO -- FIXME: implement '--archregx' and the rest of the `qparamArch`
		return getMultiObj(c, bck, outFile, a.enabled(), extract)
	}

	// GET
	return getObject(c, bck, objName, outFile, a, false /*quiet*/, extract)
}

// GET multiple -- currently, only prefix (TODO: list/range)
func getMultiObj(c *cli.Context, bck cmn.Bck, outFile string, lsarch, extract bool) error {
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
			lstFilter._add(func(obj *cmn.LsoEnt) bool {
				return obj.Name == external || strings.HasPrefix(obj.Name, origPrefix)
			})
		}
	}

	// setup lsmsg
	msg := &apc.LsoMsg{Prefix: prefix}
	msg.AddProps(apc.GetPropsMinimal...)
	if flagIsSet(c, listArchFlag) || extract || lsarch {
		msg.SetFlag(apc.LsArchDir)
	}
	if flagIsSet(c, getObjCachedFlag) {
		msg.SetFlag(apc.LsCached)
	}
	pageSize, _, limit, err := _setPage(c, bck)
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
	lst, err := api.ListObjects(apiBP, bck, msg, lsargs)
	if err != nil {
		return V(err)
	}
	if lstFilter._len() > 0 {
		lst.Entries, _ = lstFilter.apply(lst.Entries)
	}

	// can't do many to one
	l := len(lst.Entries)
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
	for _, entry := range lst.Entries {
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
		return errU
	}

	switch {
	case discardOutput(outFile):
		discard = " (and discard)"
	case outFile == fileStdIO:
		out = " to standard output"
	default:
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
	} else if !confirm(c, cptn) {
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
				total:   int64(len(lst.Entries)),
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
	for _, en := range lst.Entries {
		var shardName string

		// NOTE: s3.ListObjectsV2 _may_ return a directory - filtering out
		if cos.IsLastB(en.Name, filepath.Separator) {
			actionNote(c, "virtual directory '"+en.Name+"' in 'list-objects' results (skipping)")
			continue
		}
		if err := cmn.ValidOname(en.Name); err != nil {
			continue
		}

		if en.IsAnyFlagSet(apc.EntryInArch) {
			if origPrefix != msg.Prefix {
				if !strings.HasPrefix(en.Name, origPrefix) {
					// skip
					if u.showProgress {
						u.barObjs.IncrInt64(1)
						u.barSize.IncrInt64(en.Size)
					}
					continue
				}
			}
			for _, shardEntry := range lst.Entries {
				if shardEntry.IsAnyFlagSet(apc.EntryIsArchive) && strings.HasPrefix(en.Name, shardEntry.Name+"/") {
					shardName = shardEntry.Name
					break
				}
			}
			if shardName == "" {
				// should not be happening
				warn := fmt.Sprintf("archived file %q: cannot find parent shard in the listed results", en.Name)
				actionWarn(c, warn)
				continue
			}
		}
		u.wg.Add(1)
		go u.get(c, bck, en, shardName, outFile, quiet, extract)
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

func (u *uctx) get(c *cli.Context, bck cmn.Bck, entry *cmn.LsoEnt, shardName, outFile string, quiet, extract bool) {
	var (
		a       qparamArch // effectively, ignore user-specified command line and redefine to GET a given shardName
		objName = entry.Name
	)
	if shardName != "" {
		objName = shardName
		a.archpath = strings.TrimPrefix(entry.Name, shardName+"/")
		if outFile != fileStdIO && !discardOutput(outFile) {
			// when getting multiple files retain the full archpath
			// (compare w/ filepath.Base usage otherwise)
			outFile = filepath.Join(outFile, a.archpath)
			if err := cos.CreateDir(filepath.Dir(outFile)); err != nil {
				actionWarn(c, err.Error())
			}
		}
	}
	err := getObject(c, bck, objName, outFile, a, quiet, extract)
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
func getObject(c *cli.Context, bck cmn.Bck, objName, outFile string, a qparamArch, quiet, extract bool) error {
	if outFile == fileStdIO && extract {
		return errors.New("cannot extract archived files to standard output - " + NIY)
	}
	if discardOutput(outFile) && extract {
		return errors.New("cannot extract and discard archived files - " + NIY)
	}
	if flagIsSet(c, listArchFlag) && a.archpath == "" {
		if external, internal := splitPrefixShardBoundary(objName); internal != "" {
			objName, a.archpath = external, internal
		}
	}

	// just check if a remote object is present (do not GET)
	if flagIsSet(c, headObjPresentFlag) {
		return isObjPresent(c, bck, objName)
	}

	units, err := parseUnitsFlag(c, unitsFlag)
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
		if a.archpath != "" {
			outFile = filepath.Base(a.archpath)
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
				if a.archpath != "" {
					outFile = filepath.Join(outFile, filepath.Base(a.archpath))
				} else {
					outFile = filepath.Join(outFile, filepath.Base(objName))
				}
				// TODO: strictly speaking: fstat again and confirm if exists
			} else if finfo.Mode().IsRegular() && !flagIsSet(c, yesFlag) { // `/dev/null` is fine
				warn := fmt.Sprintf("overwrite existing %q", outFile)
				if !confirm(c, warn) {
					return nil
				}
			}
		}
	}

	var (
		hdr http.Header
		now int64
	)
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
		if flagIsSet(c, numBlobWorkersFlag) {
			nw := parseIntFlag(c, numBlobWorkersFlag)
			if nw <= 0 || nw > 128 {
				return fmt.Errorf("invalid %s=%d: expecting (1..128) range", flprn(numBlobWorkersFlag), nw)
			}
			hdr.Set(apc.HdrBlobWorkers, strconv.Itoa(nw))
		}

		if !quiet {
			now = mono.NanoTime()
		}
	} else if flagIsSet(c, chunkSizeFlag) || flagIsSet(c, numBlobWorkersFlag) {
		return fmt.Errorf("command line options (%s, %s) can be used only together with %s",
			qflprn(chunkSizeFlag), qflprn(numBlobWorkersFlag), qflprn(blobDownloadFlag))
	}

	var getArgs api.GetArgs
	switch {
	case outFile == fileStdIO:
		getArgs = api.GetArgs{Writer: os.Stdout, Header: hdr}
		quiet = true
	case discardOutput(outFile):
		getArgs = api.GetArgs{Writer: io.Discard, Header: hdr}
	default:
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

	// finally: http query and API call
	getArgs.Query = a.getQuery(c, &bck)

	var oah api.ObjAttrs
	if flagIsSet(c, cksumFlag) {
		oah, err = api.GetObjectWithValidation(apiBP, bck, objName, &getArgs)
	} else {
		oah, err = api.GetObject(apiBP, bck, objName, &getArgs)
	}
	if err != nil {
		if cmn.IsStatusNotFound(err) && !a.enabled() {
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
		return nil
	}

	//
	// print result - variations
	//

	var (
		discard, out string
		elapsed      string
		sz           string
		bn           = bck.Cname("")
	)
	switch {
	case discardOutput(outFile):
		discard = " and discard"
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
	if now > 0 {
		elapsed = " in " + teb.FormatDuration(mono.Since(now))
	}
	if objLen > 0 {
		sz = " (" + teb.FmtSize(objLen, units, 2) + ")"
	}
	switch {
	case flagIsSet(c, lengthFlag):
		fmt.Fprintf(c.App.Writer, "Read%s range (length %d at offset %d)%s%s\n", discard, objLen, offset, out, elapsed)
	case a.archpath != "":
		fmt.Fprintf(c.App.Writer, "GET%s %s from %s%s%s%s\n", discard, a.archpath, bck.Cname(objName), out, sz, elapsed)
	case extract:
		fmt.Fprintf(c.App.Writer, "GET %s from %s as %q%s and extract%s%s\n", objName, bn, outFile, sz, out, elapsed)
	default:
		fmt.Fprintf(c.App.Writer, "GET%s %s from %s%s%s%s\n", discard, objName, bn, out, sz, elapsed)
	}
	return nil
}

//
// qparamArch
//

func (a *qparamArch) init(c *cli.Context) error {
	if flagIsSet(c, archpathGetFlag) {
		a.archpath = parseStrFlag(c, archpathGetFlag)
	}
	if flagIsSet(c, archmimeFlag) {
		a.archmime = parseStrFlag(c, archmimeFlag)
	}
	if flagIsSet(c, archregxFlag) {
		a.archregx = parseStrFlag(c, archregxFlag)
	}
	if flagIsSet(c, archmodeFlag) {
		a.archmode = parseStrFlag(c, archmodeFlag)
		mmode, err := archive.ValidateMatchMode(a.archmode)
		if err != nil {
			return err
		}
		a.archmode = mmode
	}
	if a.archpath != "" && a.archregx != "" {
		return fmt.Errorf(errFmtExclusive, qflprn(archpathGetFlag), qflprn(archregxFlag))
	}
	return nil
}

func (a *qparamArch) validate(c *cli.Context) error {
	if a.archpath == "" {
		return nil
	}
	if flagIsSet(c, getObjPrefixFlag) {
		return fmt.Errorf(errFmtExclusive, qflprn(getObjPrefixFlag), qflprn(archpathGetFlag))
	}
	if flagIsSet(c, headObjPresentFlag) {
		return fmt.Errorf("cannot check presence (%s) of archived file(s) (%s) - "+NIY,
			qflprn(headObjPresentFlag), qflprn(archpathGetFlag))
	}
	if flagIsSet(c, lengthFlag) {
		return errRangeReadArch(qflprn(archpathGetFlag))
	}
	return nil
}

func (a *qparamArch) enabled() bool { return a.archpath != "" || a.archregx != "" }

func (a *qparamArch) getQuery(c *cli.Context, bck *cmn.Bck) (q url.Values) {
	f := func() {
		if q == nil {
			q = make(url.Values, 4)
		}
	}
	if bck.IsHT() {
		f()
		uri := c.Args().Get(0)
		q.Set(apc.QparamOrigURL, uri)
	}
	if a.archpath != "" {
		f()
		q.Set(apc.QparamArchpath, a.archpath)
	}
	if a.archmime != "" {
		f()
		q.Set(apc.QparamArchmime, a.archmime)
	}
	if a.archregx != "" {
		f()
		q.Set(apc.QparamArchregx, a.archregx)
	}
	if a.archmode != "" {
		f()
		q.Set(apc.QparamArchmode, a.archmode)
	}
	if flagIsSet(c, silentFlag) {
		f()
		q.Set(apc.QparamSilent, "true")
	}
	if flagIsSet(c, latestVerFlag) {
		f()
		q.Set(apc.QparamLatestVer, "true")
	}
	return q
}

//
// post-GET local extraction
//

var _ archive.ArchRCB = (*extractor)(nil)

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
	err = ar.ReadUntil(ex, cos.EmptyMatchAll, "")
	rfh.Close()
	return
}

func (ex *extractor) Call(filename string, reader cos.ReadCloseSizer, _ any) (bool /*stop*/, error) {
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
