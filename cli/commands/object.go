// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles object operations.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"errors"
	"fmt"
	"net/http"
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
	"github.com/NVIDIA/aistore/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
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

func getObject(c *cli.Context, bck cmn.Bck, object, outFile string) (err error) {
	// just check if object is cached, don't get object
	if flagIsSet(c, isCachedFlag) {
		return objectCheckExists(c, bck, object)
	}

	var (
		query   = url.Values{}
		objArgs api.GetObjectInput
		objLen  int64
		offset  string
		length  string
	)

	if flagIsSet(c, lengthFlag) != flagIsSet(c, offsetFlag) {
		return incorrectUsageMsg(c, "%q and %q flags both need to be set", lengthFlag.Name, offsetFlag.Name)
	}
	if offset, err = getByteFlagValue(c, offsetFlag); err != nil {
		return
	}
	if length, err = getByteFlagValue(c, lengthFlag); err != nil {
		return
	}

	query = cmn.AddBckToQuery(query, bck)
	query.Add(cmn.URLParamOffset, offset)
	query.Add(cmn.URLParamLength, length)

	if outFile == fileStdIO {
		objArgs = api.GetObjectInput{Writer: os.Stdout, Query: query}
	} else {
		var file *os.File
		if file, err = os.Create(outFile); err != nil {
			return
		}
		defer file.Close()
		objArgs = api.GetObjectInput{Writer: file, Query: query}
	}

	if flagIsSet(c, checksumFlag) {
		objLen, err = api.GetObjectWithValidation(defaultAPIParams, bck, object, objArgs)
	} else {
		objLen, err = api.GetObject(defaultAPIParams, bck, object, objArgs)
	}
	if err != nil {
		if httpErr, ok := err.(*cmn.HTTPError); ok {
			if httpErr.Status == http.StatusNotFound {
				return fmt.Errorf("object \"%s/%s\" does not exist", bck, object)
			}
		}
		return
	}

	if flagIsSet(c, lengthFlag) {
		fmt.Fprintf(c.App.ErrWriter, "Read %s (%d B)\n", cmn.B2S(objLen, 2), objLen)
		return
	}

	fmt.Fprintf(c.App.ErrWriter, "%q has the size %s (%d B)\n", object, cmn.B2S(objLen, 2), objLen)
	return
}

//////
// Promote AIS-colocated files and directories to objects (NOTE: advanced usage only)
//////
func promoteFileOrDir(c *cli.Context, bck cmn.Bck, objName, fqn string) (err error) {
	target := parseStrFlag(c, targetFlag)

	promoteArgs := &api.PromoteArgs{
		BaseParams: defaultAPIParams,
		Bck:        bck,
		Object:     objName,
		Target:     target,
		FQN:        fqn,
		Recurs:     flagIsSet(c, recursiveFlag),
		Overwrite:  flagIsSet(c, overwriteFlag),
		Verbose:    flagIsSet(c, verboseFlag),
	}
	if err = api.PromoteFileOrDir(promoteArgs); err != nil {
		return
	}
	fmt.Fprintf(c.App.Writer, "promoted %q => bucket %q\n", fqn, bck)
	return
}

// PUT methods

func putSingleObject(bck cmn.Bck, objName, path string) (err error) {
	fh, err := cmn.NewFileHandle(path)
	if err != nil {
		return err
	}

	putArgs := api.PutObjectArgs{
		BaseParams: defaultAPIParams,
		Bck:        bck,
		Object:     objName,
		Reader:     fh,
	}

	return api.PutObject(putArgs)
}

func putRangeObjects(c *cli.Context, pt cmn.ParsedTemplate, bck cmn.Bck, trimPrefix, subdirName string) (err error) {
	if flagIsSet(c, verboseFlag) {
		fmt.Fprintln(c.App.Writer, "Enumerating files")
	}

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

	tmpl := templates.ExtensionTmpl + strconv.FormatInt(totalCount, 10) + "\t" + cmn.B2S(totalSize, 2) + "\n"
	if err = templates.DisplayOutput(extSizes, c.App.Writer, tmpl); err != nil {
		return
	}

	// ask a user for confirmation
	if !flagIsSet(c, yesFlag) {
		var input string
		fmt.Fprintf(c.App.Writer, "Proceed? [y/n]: ")
		fmt.Scanln(&input)
		if ok, _ := cmn.ParseBool(input); !ok {
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
func rangeTrimPrefix(pt cmn.ParsedTemplate) string {
	sepaIndex := strings.LastIndex(pt.Prefix, string(os.PathSeparator))
	cmn.Assert(sepaIndex >= 0)
	return pt.Prefix[:sepaIndex+1]
}

func putObject(c *cli.Context, bck cmn.Bck, objName, fileName string) (err error) {
	printDryRunHeader(c)

	path, err := getPathFromFileName(fileName)
	if err != nil {
		return err
	}

	var pt cmn.ParsedTemplate
	if pt, err = cmn.ParseBashTemplate(path); err == nil {
		return putRangeObjects(c, pt, bck, rangeTrimPrefix(pt), objName)
	}
	// if parse failed continue to other options

	// Upload single file
	if fh, err := os.Stat(path); err == nil && !fh.IsDir() {
		if objName == "" {
			// objName was not provided, only bucket name, use just file name as object name
			objName = filepath.Base(path)
		}

		if flagIsSet(c, dryRunFlag) {
			fmt.Fprintf(c.App.Writer, "PUT %q => \"%s/%s\"\n", path, bck.Name, objName)
			return nil
		}
		if err = putSingleObject(bck, objName, path); err == nil {
			fmt.Fprintf(c.App.Writer, "PUT %q into bucket %q\n", objName, bck)
		}
		return err
	}

	if flagIsSet(c, verboseFlag) {
		fmt.Fprintf(c.App.Writer, "Enumerating files\n")
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
		barText    = fmt.Sprintf("Composing %d files into object \"%s/%s\"", len(fileNames), bck.Name, objName)
		filesToObj = make([]FileToObjSlice, len(fileNames))
		p          *mpb.Progress
		sizes      = make(map[string]int64, len(fileNames))
		totalSize  = int64(0)
		verbose    = flagIsSet(c, verboseFlag)
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

	if verbose {
		p = mpb.New(mpb.WithWidth(progressBarWidth))
		bar = p.AddBar(
			totalSize,
			mpb.PrependDecorators(
				decor.Name(barText, decor.WC{W: len(barText) + 1, C: decor.DidentRight}),
				decor.CountersKibiByte("% .2f / % .2f", decor.WCSyncWidth),
			),
			mpb.AppendDecorators(decor.Percentage(decor.WCSyncWidth)))
	}

	var handle string
	for _, filesSlice := range filesToObj {
		for _, f := range filesSlice {
			fh, err := cmn.NewFileHandle(f.path)
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

	err = api.FlushObject(api.AppendArgs{
		BaseParams: defaultAPIParams,
		Bck:        bck,
		Object:     objName,
		Handle:     handle,
	})

	if err != nil {
		return fmt.Errorf("%v. Object not created", err)
	}

	_, _ = fmt.Fprintf(c.App.Writer, "COMPOSE %q of size %d into bucket %q\n", objName, totalSize, bck)

	return nil
}

func objectCheckExists(c *cli.Context, bck cmn.Bck, object string) error {
	_, err := api.HeadObject(defaultAPIParams, bck, object, true)
	if err != nil {
		if err.(*cmn.HTTPError).Status == http.StatusNotFound {
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
	)

	wg := &sync.WaitGroup{}
	lastReport := time.Now()
	reportEvery := p.refresh
	sema := cmn.NewDynSemaphore(p.workerCnt)
	verbose := flagIsSet(c, verboseFlag)

	finalizePut := func(f fileToObj) {
		wg.Done()
		total := int(processedCnt.Inc())
		size := processedSize.Add(f.size)
		sema.Release()
		if reportEvery == 0 {
			return
		}

		// lock after releasing semaphore, so the next file can start
		// uploading even if we are stuck on mutex for a while
		mx.Lock()
		if time.Since(lastReport) > reportEvery {
			fmt.Fprintf(c.App.Writer, "Uploaded %d(%d%%) objects, %s (%d%%).\n",
				total, 100*total/len(p.files),
				cmn.B2S(size, 1), 100*size/p.totalSize)
			lastReport = time.Now()
		}
		mx.Unlock()
	}

	putOneFile := func(f fileToObj) {
		defer finalizePut(f)

		reader, err := cmn.NewFileHandle(f.path)
		if err != nil {
			_, _ = fmt.Fprintf(c.App.Writer, "Failed to open file %q: %v\n", f.path, err)
			errCount.Inc()
			return
		}

		putArgs := api.PutObjectArgs{BaseParams: defaultAPIParams, Bck: p.bck, Object: f.name, Reader: reader}
		if err := api.PutObject(putArgs); err != nil {
			_, _ = fmt.Fprintf(c.App.Writer, "Failed to put object %q: %v\n", f.name, err)
			errCount.Inc()
		} else if verbose {
			_, _ = fmt.Fprintf(c.App.Writer, "%s -> %s\n", f.path, f.name)
		}
	}

	for _, f := range p.files {
		sema.Acquire()
		wg.Add(1)
		putOneFile(f)
	}
	wg.Wait()

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

func buildObjStatTemplate(props string, showHeaders bool) string {
	var (
		headSb strings.Builder
		bodySb strings.Builder

		propsList = makeList(props, ",")
	)
	for _, field := range propsList {
		if _, ok := templates.ObjStatMap[field]; !ok {
			// just skip invalid props for now - here a prop string
			// from `bucket objects` can be used and how HEAD and LIST
			// request have different set of properties
			continue
		}
		headSb.WriteString(strings.Title(field) + "\t")
		bodySb.WriteString(templates.ObjStatMap[field])
	}
	headSb.WriteString("\n")
	bodySb.WriteString("\n")

	if showHeaders {
		return headSb.String() + bodySb.String()
	}

	return bodySb.String()
}

// Displays object properties
func objectStats(c *cli.Context, bck cmn.Bck, object string) error {
	props, propsFlag := "local,", ""
	if flagIsSet(c, objPropsFlag) {
		propsFlag = parseStrFlag(c, objPropsFlag)
	}
	if propsFlag == "all" || propsFlag == "" {
		// do not include `ec` into `GetPropsAll` - `GetPropsAll` is used
		// by object list operation, and getting EC info is heavy operation.
		// So, it is local to `show object` command for now.
		props += strings.Join(cmn.GetPropsAll, ",") + ",ec"
	} else {
		props += parseStrFlag(c, objPropsFlag)
	}

	tmpl := buildObjStatTemplate(props, !flagIsSet(c, noHeaderFlag))
	objProps, err := api.HeadObject(defaultAPIParams, bck, object)
	if err != nil {
		return handleObjHeadError(err, bck, object)
	}

	return templates.DisplayOutput(objProps, c.App.Writer, tmpl, flagIsSet(c, jsonFlag))
}

// This function is needed to print a nice error message for the user
func handleObjHeadError(err error, bck cmn.Bck, object string) error {
	httpErr, ok := err.(*cmn.HTTPError)
	if !ok {
		return err
	}
	if httpErr.Status == http.StatusNotFound {
		return fmt.Errorf("no such object %q in bucket %q", object, bck)
	}

	return err
}

func listOrRangeOp(c *cli.Context, command string, bck cmn.Bck) (err error) {
	if err = canReachBucket(bck); err != nil {
		return
	}
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
		fileList = makeList(parseStrFlag(c, listFlag), ",")
	)

	if flagIsSet(c, dryRunFlag) {
		limitedLineWriter(c.App.Writer, dryRunExamplesCnt, strings.ToUpper(command)+" "+bck.Name+"/%s\n", fileList)
		return nil
	}

	switch command {
	case commandRemove:
		err = api.DeleteList(defaultAPIParams, bck, fileList)
		command = "removed"
	case commandPrefetch:
		bck.Provider = cmn.Cloud
		err = api.PrefetchList(defaultAPIParams, bck, fileList)
		command += "ed"
	case commandEvict:
		bck.Provider = cmn.Cloud
		err = api.EvictList(defaultAPIParams, bck, fileList)
		command += "ed"
	default:
		return fmt.Errorf(invalidCmdMsg, command)
	}
	if err != nil {
		return
	}
	fmt.Fprintf(c.App.Writer, "%s %s from %q bucket\n", fileList, command, bck)
	return
}

// Range handler
func rangeOp(c *cli.Context, command string, bck cmn.Bck) (err error) {
	var (
		rangeStr = parseStrFlag(c, templateFlag)
		pt       cmn.ParsedTemplate
	)

	if flagIsSet(c, dryRunFlag) {
		pt, err = cmn.ParseBashTemplate(rangeStr)
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
		err = api.DeleteRange(defaultAPIParams, bck, rangeStr)
		command = "removed"
	case commandPrefetch:
		bck.Provider = cmn.Cloud
		err = api.PrefetchRange(defaultAPIParams, bck, rangeStr)
		command += "ed"
	case commandEvict:
		bck.Provider = cmn.Cloud
		err = api.EvictRange(defaultAPIParams, bck, rangeStr)
		command += "ed"
	default:
		return fmt.Errorf(invalidCmdMsg, command)
	}
	if err != nil {
		return
	}
	fmt.Fprintf(c.App.Writer, "%s files in the range %q from %q bucket\n",
		command, rangeStr, bck)
	return
}

// Multiple object arguments handler
func multiObjOp(c *cli.Context, command string) error {
	// stops iterating if it encounters an error
	for _, fullObjName := range c.Args() {
		var (
			bck, objectName, err = parseBckObjectURI(fullObjName)
		)
		if err != nil {
			return err
		}
		if bck, err = validateBucket(c, bck, fullObjName, false); err != nil {
			return err
		}
		if objectName == "" {
			return incorrectUsageMsg(c, "%q: missing object name", fullObjName)
		}

		switch command {
		case commandRemove:
			if err := api.DeleteObject(defaultAPIParams, bck, objectName); err != nil {
				return err
			}
			fmt.Fprintf(c.App.Writer, "%q deleted from %q bucket\n", objectName, bck)
		case commandEvict:
			if cmn.IsProviderAIS(bck) {
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
