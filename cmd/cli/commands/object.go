// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles object operations.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmd/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	jsoniter "github.com/json-iterator/go"
	"github.com/urfave/cli"
	"github.com/vbauerster/mpb/v4"
	"github.com/vbauerster/mpb/v4/decor"
)

type uploadParams struct {
	bck       cmn.Bck
	files     []fileToObj
	workerCnt int
	refresh   time.Duration
	totalSize int64
}

const (
	dryRunExamplesCnt = 10
	dryRunHeader      = "[DRY RUN]"
	dryRunExplanation = "No modifications on the cluster"
)

func getObject(c *cli.Context, outFile string, silent bool) (err error) {
	var (
		objArgs                api.GetObjectInput
		bck                    cmn.Bck
		objName, archpath      string
		objLen, offset, length int64
	)

	if c.NArg() < 1 {
		return missingArgumentsError(c, "object name in the form bucket/object", "output file")
	}

	uri := c.Args().Get(0)
	if bck, objName, err = parseBckObjectURI(c, uri); err != nil {
		return
	}

	if !bck.IsHTTP() || !flagIsSet(c, forceFlag) {
		if _, err = headBucket(bck); err != nil {
			return
		}
	}

	if outFile == "" {
		outFile = filepath.Base(objName)
	}

	// just check if remote object is present (do not execute GET)
	// TODO: archive
	if flagIsSet(c, isCachedFlag) {
		return objectCheckExists(c, bck, objName)
	}

	if flagIsSet(c, lengthFlag) != flagIsSet(c, offsetFlag) {
		return incorrectUsageMsg(c, "%q and %q flags both need to be set", lengthFlag.Name, offsetFlag.Name)
	}
	if offset, err = parseByteFlagToInt(c, offsetFlag); err != nil {
		return
	}
	if length, err = parseByteFlagToInt(c, lengthFlag); err != nil {
		return
	}

	hdr := cmn.RangeHdr(offset, length)
	if outFile == fileStdIO {
		objArgs = api.GetObjectInput{Writer: os.Stdout, Header: hdr}
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
		objArgs = api.GetObjectInput{Writer: file, Header: hdr}
	}

	if bck.IsHTTP() {
		objArgs.Query = make(url.Values, 2)
		objArgs.Query.Set(cmn.URLParamOrigURL, uri)
	}
	// TODO: validate
	if archpath = parseStrFlag(c, archpathFlag); archpath != "" {
		if objArgs.Query == nil {
			objArgs.Query = make(url.Values, 1)
		}
		objArgs.Query.Set(cmn.URLParamArchpath, archpath)
	}

	if flagIsSet(c, checksumFlag) {
		objLen, err = api.GetObjectWithValidation(defaultAPIParams, bck, objName, objArgs)
	} else {
		objLen, err = api.GetObject(defaultAPIParams, bck, objName, objArgs)
	}
	if err != nil {
		if cmn.IsStatusNotFound(err) && archpath == "" {
			err = fmt.Errorf("object \"%s/%s\" does not exist", bck, objName)
		}
		return
	}

	if flagIsSet(c, lengthFlag) && outFile != fileStdIO {
		fmt.Fprintf(c.App.ErrWriter, "Read range len=%s(%dB) as %q\n", cos.B2S(objLen, 2), objLen, outFile)
		return
	}
	if !silent && outFile != fileStdIO {
		fmt.Fprintf(c.App.Writer, "GET %q from bucket %q as %q [%s]\n", objName, bck, outFile, cos.B2S(objLen, 2))
	}
	return
}

// Promote AIS-colocated files and directories to objects.

func promoteFileOrDir(c *cli.Context, bck cmn.Bck, objName, fqn string) (err error) {
	target := parseStrFlag(c, targetFlag)

	promoteArgs := &api.PromoteArgs{
		BaseParams: defaultAPIParams,
		Bck:        bck,
		Object:     objName,
		Target:     target,
		FQN:        fqn,
		Recursive:  flagIsSet(c, recursiveFlag),
		Overwrite:  c.Bool(overwriteFlag.GetName()),
		KeepOrig:   c.Bool(keepOrigFlag.GetName()),
	}
	if err = api.PromoteFileOrDir(promoteArgs); err != nil {
		return
	}
	fmt.Fprintf(c.App.Writer, "promoted %q => bucket %q\n", fqn, bck)
	return
}

func setCustomProps(c *cli.Context, bck cmn.Bck, objName string) (err error) {
	props := make(cos.SimpleKVs)
	propArgs := c.Args().Tail()

	if len(propArgs) == 1 && isJSON(propArgs[0]) {
		if err = jsoniter.Unmarshal([]byte(propArgs[0]), &props); err != nil {
			return
		}
	} else {
		if len(propArgs) == 0 {
			err = missingArgumentsError(c, "property key-value pairs")
			return
		}
		for _, pair := range propArgs {
			nv := strings.Split(pair, "=")
			if len(nv) != 2 {
				return fmt.Errorf("invalid custom property %q (Hint: use syntax key1=value1 key2=value2 ...)", nv)
			}
			nv[0] = strings.TrimSpace(nv[0])
			nv[1] = strings.TrimSpace(nv[1])
			props[nv[0]] = nv[1]
		}
	}
	if err = api.SetObjectCustomProps(defaultAPIParams, bck, objName, props); err != nil {
		return
	}
	fmt.Fprintf(c.App.Writer,
		"Custom props successfully updated (use `ais show object %s/%s --props=all` to show the updates).\n",
		bck, objName)
	return nil
}

// PUT methods.

func putSingleObject(c *cli.Context, bck cmn.Bck, objName, path string) (err error) {
	var (
		reader   cos.ReadOpenCloser
		progress *mpb.Progress
		bars     []*mpb.Bar
		cksum    *cos.Cksum
	)
	if flagIsSet(c, computeCksumFlag) {
		bckProps, err := api.HeadBucket(defaultAPIParams, bck)
		if err != nil {
			return err
		}
		cksum = cos.NewCksum(bckProps.Cksum.Type, "")
	} else {
		cksums := parseChecksumFlags(c)
		if len(cksums) > 1 {
			return fmt.Errorf("at most one checksum flags can be set (multi-checksum is not supported yet)")
		}
		if len(cksums) == 1 {
			cksum = cksums[0]
		}
	}
	fh, err := cos.NewFileHandle(path)
	if err != nil {
		return err
	}

	reader = fh
	if flagIsSet(c, progressBarFlag) {
		fi, err := fh.Stat()
		if err != nil {
			return err
		}

		progress, bars = simpleProgressBar(progressBarArgs{barType: sizeArg, barText: objName, total: fi.Size()})
		readCallback := func(n int, _ error) { bars[0].IncrBy(n) }
		reader = cos.NewCallbackReadOpenCloser(fh, readCallback)
	}

	putArgs := api.PutObjectArgs{
		BaseParams: defaultAPIParams,
		Bck:        bck,
		Object:     objName,
		Reader:     reader,
		Cksum:      cksum,
	}

	archPath := parseStrFlag(c, archpathFlag)
	if archPath != "" {
		fi, err := fh.Stat()
		if err != nil {
			return err
		}
		putArgs.Size = uint64(fi.Size())
		appendArcArgs := api.AppendObjectArchArgs{
			PutObjectArgs: putArgs,
			ArchPath:      archPath,
		}
		err = api.AppendObjectArch(appendArcArgs)
		if flagIsSet(c, progressBarFlag) {
			progress.Wait()
		}
		return err
	}

	err = api.PutObject(putArgs)

	if flagIsSet(c, progressBarFlag) {
		progress.Wait()
	}

	return err
}

func putSingleObjectChunked(c *cli.Context, bck cmn.Bck, objName string, r io.Reader, cksumType string) (err error) {
	var (
		handle string
		cksum  = cos.NewCksumHash(cksumType)
		pi     = newProgIndicator(objName)
	)
	chunkSize, err := parseByteFlagToInt(c, chunkSizeFlag)
	if err != nil {
		return err
	}

	if flagIsSet(c, progressBarFlag) {
		pi.start()
	}
	for {
		var (
			// TODO: use MMSA
			b      = bytes.NewBuffer(nil)
			n      int64
			err    error
			reader cos.ReadOpenCloser
		)
		if cksumType != cos.ChecksumNone {
			n, err = io.CopyN(cos.NewWriterMulti(cksum.H, b), r, chunkSize)
		} else {
			n, err = io.CopyN(b, r, chunkSize)
		}
		if err != nil && err != io.EOF {
			return err
		}
		if n == 0 {
			break
		}
		reader = cos.NewByteHandle(b.Bytes())
		if flagIsSet(c, progressBarFlag) {
			actualChunkOffset := atomic.NewInt64(0)
			reader = cos.NewCallbackReadOpenCloser(reader, func(n int, _ error) {
				if n == 0 {
					return
				}
				newChunkOffset := actualChunkOffset.Add(int64(n))
				// `actualChunkOffset` is needed to not count the bytes read more than
				// once upon redirection
				if newChunkOffset > chunkSize {
					// This part of the file was already read, so don't read it again
					pi.printProgress(chunkSize - newChunkOffset + int64(n))
					return
				}
				pi.printProgress(int64(n))
			})
		}
		handle, err = api.AppendObject(api.AppendArgs{
			BaseParams: defaultAPIParams,
			Bck:        bck,
			Object:     objName,
			Handle:     handle,
			Reader:     reader,
			Size:       n,
		})
		if err != nil {
			return err
		}
	}

	if flagIsSet(c, progressBarFlag) {
		pi.stop()
	}
	if cksumType != cos.ChecksumNone {
		cksum.Finalize()
	}

	return api.FlushObject(api.FlushArgs{
		BaseParams: defaultAPIParams,
		Bck:        bck,
		Object:     objName,
		Handle:     handle,
		Cksum:      cksum.Clone(),
	})
}

func putRangeObjects(c *cli.Context, pt cos.ParsedTemplate, bck cmn.Bck, trimPrefix, subdirName string) (err error) {
	getNext := pt.Iter()
	allFiles := make([]fileToObj, 0, pt.Count())
	for file, hasNext := getNext(); hasNext; file, hasNext = getNext() {
		files, err := generateFileList(file, trimPrefix, subdirName, flagIsSet(c, recursiveFlag))
		if err != nil {
			return err
		}
		allFiles = append(allFiles, files...)
	}

	return putMultipleObjects(c, allFiles, bck)
}

func putMultipleObjects(c *cli.Context, files []fileToObj, bck cmn.Bck) (err error) {
	if len(files) == 0 {
		return fmt.Errorf("no files found")
	}

	// calculate total size, group by extension
	totalSize, extSizes := groupByExt(files)
	totalCount := int64(len(files))

	if flagIsSet(c, dryRunFlag) {
		i := 0
		for ; i < dryRunExamplesCnt; i++ {
			fmt.Fprintf(c.App.Writer, "PUT %q => \"%s/%s\"\n", files[i].path, bck, files[i].name)
		}
		if i < len(files) {
			fmt.Fprintf(c.App.Writer, "(and %d more)\n", len(files)-i)
		}
		return nil
	}

	tmpl := templates.ExtensionTmpl + strconv.FormatInt(totalCount, 10) + "\t" + cos.B2S(totalSize, 2) + "\n"
	if err = templates.DisplayOutput(extSizes, c.App.Writer, tmpl); err != nil {
		return
	}

	// ask a user for confirmation
	if !flagIsSet(c, yesFlag) {
		if ok := confirm(c, fmt.Sprintf("Proceed uploading to bucket %q?", bck)); !ok {
			return errors.New("operation canceled")
		}
	}

	refresh := calcPutRefresh(c)
	numWorkers := parseIntFlag(c, concurrencyFlag)
	params := uploadParams{
		bck:       bck,
		files:     files,
		workerCnt: numWorkers,
		refresh:   refresh,
		totalSize: totalSize,
	}
	return uploadFiles(c, params)
}

// base + fileName = path
// if fileName is already absolute path, base is empty
func getPathFromFileName(fileName string) (path string, err error) {
	path = cmn.ExpandPath(fileName)
	if path, err = filepath.Abs(path); err != nil {
		return "", err
	}

	return path, err
}

// Returns longest common prefix ending with '/' (exclusive) for objects in the template
// /path/to/dir/test{0..10}/dir/another{0..10} => /path/to/dir
// /path/to/prefix-@00001-gap-@100-suffix => /path/to
func rangeTrimPrefix(pt cos.ParsedTemplate) string {
	sepaIndex := strings.LastIndex(pt.Prefix, string(os.PathSeparator))
	cos.Assert(sepaIndex >= 0)
	return pt.Prefix[:sepaIndex+1]
}

func putObject(c *cli.Context, bck cmn.Bck, objName, fileName, cksumType string) (err error) {
	printDryRunHeader(c)

	if fileName == "-" {
		if objName == "" {
			return fmt.Errorf("object name is required when reading from stdin")
		}

		if flagIsSet(c, dryRunFlag) {
			fmt.Fprintf(c.App.Writer, "PUT (stdin) => \"%s/%s\"\n", bck.Name, objName)
			return nil
		}

		if err := putSingleObjectChunked(c, bck, objName, os.Stdin, cksumType); err != nil {
			return err
		}
		fmt.Fprintf(c.App.Writer, "PUT %q into bucket %q\n", objName, bck)

		return nil
	}

	path, err := getPathFromFileName(fileName)
	if err != nil {
		return err
	}

	var pt cos.ParsedTemplate
	if pt, err = cos.ParseBashTemplate(path); err == nil {
		return putRangeObjects(c, pt, bck, rangeTrimPrefix(pt), objName)
	}
	// if parse failed continue to other options

	// Upload single file
	if fh, err := os.Stat(path); err == nil && !fh.IsDir() {
		if objName == "" {
			// objName was not provided, only bucket name, use just file name as object name
			objName = filepath.Base(path)
		}

		archPath := parseStrFlag(c, archpathFlag)
		if archPath != "" {
			archPath = "/" + archPath
		}
		if flagIsSet(c, dryRunFlag) {
			fmt.Fprintf(c.App.Writer, "PUT %q => \"%s/%s/%s\"\n", path, bck.Name, objName, archPath)
			return nil
		}
		if err := putSingleObject(c, bck, objName, path); err != nil {
			return err
		}
		if archPath == "" {
			fmt.Fprintf(c.App.Writer, "PUT %q into bucket %q\n", objName, bck)
		} else {
			fmt.Fprintf(c.App.Writer, "APPEND %q to object \"%s/%s[%s]\"\n", path, bck, objName, archPath)
		}
		return nil
	}

	files, err := generateFileList(path, "", objName, flagIsSet(c, recursiveFlag))
	if err != nil {
		return
	}

	return putMultipleObjects(c, files, bck)
}

func concatObject(c *cli.Context, bck cmn.Bck, objName string, fileNames []string) (err error) {
	var (
		bar        *mpb.Bar
		p          *mpb.Progress
		barText    = fmt.Sprintf("Composing %d files into object \"%s/%s\"", len(fileNames), bck.Name, objName)
		filesToObj = make([]FileToObjSlice, len(fileNames))
		sizes      = make(map[string]int64, len(fileNames))
		totalSize  = int64(0)
	)

	for i, fileName := range fileNames {
		filesToObj[i], err = generateFileList(fileName, "", "", flagIsSet(c, recursiveFlag))
		if err != nil {
			return err
		}

		sort.Sort(filesToObj[i])
		for _, f := range filesToObj[i] {
			totalSize += f.size
		}
	}

	if flagIsSet(c, progressBarFlag) {
		var bars []*mpb.Bar
		p, bars = simpleProgressBar(progressBarArgs{barType: sizeArg, barText: barText, total: totalSize})
		bar = bars[0]
	}

	var handle string
	for _, filesSlice := range filesToObj {
		for _, f := range filesSlice {
			fh, err := cos.NewFileHandle(f.path)
			if err != nil {
				return err
			}

			appendArgs := api.AppendArgs{
				BaseParams: defaultAPIParams,
				Bck:        bck,
				Object:     objName,
				Reader:     fh,
				Handle:     handle,
			}
			handle, err = api.AppendObject(appendArgs)
			if err != nil {
				return fmt.Errorf("%v. Object not created", err)
			}

			if bar != nil {
				bar.IncrInt64(sizes[fh.Name()])
			}
		}
	}

	if p != nil {
		p.Wait()
	}

	err = api.FlushObject(api.FlushArgs{
		BaseParams: defaultAPIParams,
		Bck:        bck,
		Object:     objName,
		Handle:     handle,
	})

	if err != nil {
		return fmt.Errorf("%v. Object not created", err)
	}

	_, _ = fmt.Fprintf(c.App.Writer, "COMPOSE %q [%s] into bucket %q\n", objName, cos.B2S(totalSize, 2), bck)

	return nil
}

func objectCheckExists(c *cli.Context, bck cmn.Bck, object string) error {
	_, err := api.HeadObject(defaultAPIParams, bck, object, true)
	if err != nil {
		if cmn.IsStatusNotFound(err) {
			fmt.Fprintf(c.App.Writer, "Cached: %v\n", false)
			return nil
		}
		return err
	}

	fmt.Fprintf(c.App.Writer, "Cached: %v\n", true)
	return nil
}

func uploadFiles(c *cli.Context, p uploadParams) error {
	var (
		errCount      atomic.Int32 // uploads failed so far
		processedCnt  atomic.Int32 // files processed so far
		processedSize atomic.Int64 // size of already processed files
		mx            sync.Mutex

		totalBars []*mpb.Bar
		progress  *mpb.Progress

		verbose      = flagIsSet(c, verboseFlag)
		showProgress = flagIsSet(c, progressBarFlag)

		errSb strings.Builder

		wg          = cos.NewLimitedWaitGroup(p.workerCnt)
		lastReport  = time.Now()
		reportEvery = p.refresh
	)

	if showProgress {
		var (
			filesBarArg = progressBarArgs{total: int64(len(p.files)), barText: "Uploaded files progress", barType: unitsArg}
			sizeBarArg  = progressBarArgs{total: p.totalSize, barText: "Uploaded sizes progress", barType: sizeArg}
		)
		progress, totalBars = simpleProgressBar(filesBarArg, sizeBarArg)
	}

	finalizePut := func(f fileToObj) {
		total := int(processedCnt.Inc())
		size := processedSize.Add(f.size)

		if showProgress {
			totalBars[0].Increment()
			// size bar sie updated in putOneFile
		}

		wg.Done()
		if reportEvery == 0 {
			return
		}

		// lock after releasing semaphore, so the next file can start
		// uploading even if we are stuck on mutex for a while
		mx.Lock()
		if !showProgress && time.Since(lastReport) > reportEvery {
			fmt.Fprintf(
				c.App.Writer, "Uploaded %d(%d%%) objects, %s (%d%%).\n",
				total, 100*total/len(p.files), cos.B2S(size, 1), 100*size/p.totalSize,
			)
			lastReport = time.Now()
		}
		mx.Unlock()
	}

	putOneFile := func(f fileToObj) {
		var bar *mpb.Bar

		defer finalizePut(f)

		reader, err := cos.NewFileHandle(f.path)
		if err != nil {
			str := fmt.Sprintf("Failed to open file %q: %v\n", f.path, err)
			if showProgress {
				errSb.WriteString(str)
			} else {
				_, _ = fmt.Fprint(c.App.Writer, str)
			}
			errCount.Inc()
			return
		}

		if showProgress && verbose {
			bar = progress.AddBar(
				f.size,
				mpb.BarRemoveOnComplete(),
				mpb.PrependDecorators(
					decor.Name(f.name+" ", decor.WC{W: len(f.name) + 1, C: decor.DSyncWidthR}),
					decor.Counters(decor.UnitKiB, "%.1f/%.1f", decor.WCSyncWidth),
				),
				mpb.AppendDecorators(decor.Percentage(decor.WCSyncWidth)),
			)
		}

		// keep track of single file upload bar, by tracking Read() call on file
		updateBar := func(n int, err error) {
			if showProgress {
				totalBars[1].IncrBy(n)
				if verbose {
					bar.IncrBy(n)
				}
			}
		}
		countReader := cos.NewCallbackReadOpenCloser(reader, updateBar)

		putArgs := api.PutObjectArgs{BaseParams: defaultAPIParams, Bck: p.bck, Object: f.name, Reader: countReader}
		if err := api.PutObject(putArgs); err != nil {
			str := fmt.Sprintf("Failed to put object %q: %v\n", f.name, err)
			if showProgress {
				errSb.WriteString(str)
			} else {
				_, _ = fmt.Fprint(c.App.Writer, str)
			}
			errCount.Inc()
		} else if verbose && !showProgress {
			_, _ = fmt.Fprintf(c.App.Writer, "%s -> %s\n", f.path, f.name)
		}
	}

	for _, f := range p.files {
		wg.Add(1)
		go putOneFile(f)
	}
	wg.Wait()

	if showProgress {
		progress.Wait()
		_, _ = fmt.Fprint(c.App.Writer, errSb.String())
	}

	if failed := errCount.Load(); failed != 0 {
		return fmt.Errorf("failed to upload: %d object(s)", failed)
	}

	_, _ = fmt.Fprintf(c.App.Writer, "%d objects put into %q bucket\n", len(p.files), p.bck)
	return nil
}

func calcPutRefresh(c *cli.Context) time.Duration {
	refresh := 5 * time.Second
	if flagIsSet(c, verboseFlag) && !flagIsSet(c, refreshFlag) {
		return 0
	}
	if flagIsSet(c, refreshFlag) {
		refresh = calcRefreshRate(c)
	}
	return refresh
}

// Displays object properties
func objectStats(c *cli.Context, bck cmn.Bck, object string) error {
	var (
		propsFlag     []string
		selectedProps []string
	)

	objProps, err := api.HeadObject(defaultAPIParams, bck, object)
	if err != nil {
		return handleObjHeadError(err, bck, object)
	}

	if flagIsSet(c, jsonFlag) {
		return templates.DisplayOutput(objProps, c.App.Writer, templates.PropsSimpleTmpl, true)
	}

	if flagIsSet(c, objPropsFlag) {
		propsFlag = strings.Split(parseStrFlag(c, objPropsFlag), ",")
	}

	if len(propsFlag) == 0 {
		selectedProps = cmn.GetPropsDefault
	} else if cos.StringInSlice("all", propsFlag) {
		selectedProps = cmn.GetPropsAll
	} else {
		selectedProps = propsFlag
	}

	props := objectPropList(objProps, selectedProps)
	return templates.DisplayOutput(props, c.App.Writer, templates.PropsSimpleTmpl)
}

// This function is needed to print a nice error message for the user
func handleObjHeadError(err error, bck cmn.Bck, object string) error {
	if cmn.IsStatusNotFound(err) {
		return fmt.Errorf("no such object %q in bucket %q", object, bck)
	}

	return err
}

func listOrRangeOp(c *cli.Context, command string, bck cmn.Bck) (err error) {
	if flagIsSet(c, listFlag) && flagIsSet(c, templateFlag) {
		return incorrectUsageMsg(c, "flags %q and %q cannot be both set", listFlag.Name, templateFlag.Name)
	}

	if flagIsSet(c, listFlag) {
		return listOp(c, command, bck)
	}
	if flagIsSet(c, templateFlag) {
		return rangeOp(c, command, bck)
	}
	return
}

// List handler
func listOp(c *cli.Context, command string, bck cmn.Bck) (err error) {
	var (
		fileList = makeList(parseStrFlag(c, listFlag))
		xactID   string
	)

	if flagIsSet(c, dryRunFlag) {
		limitedLineWriter(c.App.Writer, dryRunExamplesCnt, strings.ToUpper(command)+" "+bck.Name+"/%s\n", fileList)
		return nil
	}

	switch command {
	case commandRemove:
		xactID, err = api.DeleteList(defaultAPIParams, bck, fileList)
		command = "removed"
	case commandPrefetch:
		if err = ensureHasProvider(bck, command); err != nil {
			return
		}
		xactID, err = api.PrefetchList(defaultAPIParams, bck, fileList)
		command += "ed"
	case commandEvict:
		if err = ensureHasProvider(bck, command); err != nil {
			return
		}
		xactID, err = api.EvictList(defaultAPIParams, bck, fileList)
		command += "ed"
	default:
		err = fmt.Errorf(invalidCmdMsg, command)
		return
	}
	if err != nil {
		return
	}
	basemsg := fmt.Sprintf("%s %s from %q bucket", fileList, command, bck)
	if xactID != "" {
		basemsg += ", " + xactProgressMsg(xactID)
	}
	fmt.Fprintln(c.App.Writer, basemsg)
	return
}

// Range handler
func rangeOp(c *cli.Context, command string, bck cmn.Bck) (err error) {
	var (
		rangeStr = parseStrFlag(c, templateFlag)
		pt       cos.ParsedTemplate
		xactID   string
	)

	if flagIsSet(c, dryRunFlag) {
		pt, err = cos.ParseBashTemplate(rangeStr)
		if err != nil {
			fmt.Fprintf(c.App.Writer, "couldn't parse template %q locally; %s", rangeStr, err.Error())
			return nil
		}
		objs := pt.ToSlice(dryRunExamplesCnt)
		limitedLineWriter(c.App.Writer, dryRunExamplesCnt, strings.ToUpper(command)+" "+bck.String()+"/%s", objs)
		if pt.Count() > dryRunExamplesCnt {
			fmt.Fprintf(c.App.Writer, "(and %d more)", pt.Count()-dryRunExamplesCnt)
		}
		return
	}

	switch command {
	case commandRemove:
		xactID, err = api.DeleteRange(defaultAPIParams, bck, rangeStr)
		command = "removed"
	case commandPrefetch:
		if err = ensureHasProvider(bck, command); err != nil {
			return
		}
		xactID, err = api.PrefetchRange(defaultAPIParams, bck, rangeStr)
		command += "ed"
	case commandEvict:
		if err = ensureHasProvider(bck, command); err != nil {
			return
		}
		xactID, err = api.EvictRange(defaultAPIParams, bck, rangeStr)
		command += "ed"
	default:
		return fmt.Errorf(invalidCmdMsg, command)
	}
	if err != nil {
		return
	}

	baseMsg := fmt.Sprintf("%s from %s objects in the range %q", command, bck, rangeStr)

	if xactID != "" {
		baseMsg += ", " + xactProgressMsg(xactID)
	}
	fmt.Fprintln(c.App.Writer, baseMsg)
	return
}

// Multiple object arguments handler
func multiObjOp(c *cli.Context, command string) error {
	// stops iterating if it encounters an error
	for _, uri := range c.Args() {
		bck, objectName, err := parseBckObjectURI(c, uri)
		if err != nil {
			return err
		}
		if _, err = headBucket(bck); err != nil {
			return err
		}

		switch command {
		case commandRemove:
			if err := api.DeleteObject(defaultAPIParams, bck, objectName); err != nil {
				return err
			}
			fmt.Fprintf(c.App.Writer, "%q deleted from %q bucket\n", objectName, bck)
		case commandEvict:
			if bck.IsAIS() {
				return fmt.Errorf("evicting objects from AIS bucket is not allowed")
			}
			if flagIsSet(c, dryRunFlag) {
				fmt.Fprintf(c.App.Writer, "EVICT: %s/%s\n", bck, objectName)
				continue
			}
			if err := api.EvictObject(defaultAPIParams, bck, objectName); err != nil {
				return err
			}
			fmt.Fprintf(c.App.Writer, "%q evicted from %q bucket\n", objectName, bck)
		}
	}
	return nil
}
