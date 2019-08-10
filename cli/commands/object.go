// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that interact with objects in the cluster
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
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cli/templates"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

const (
	outFileStdout = "-"
)

type uploadParams struct {
	bucket    string
	provider  string
	files     []fileToObj
	workerCnt int
	refresh   time.Duration
	totalSize int64
}

var (
	baseObjectFlags = []cli.Flag{
		bucketFlag,
		nameFlag,
		bckProviderFlag,
	}

	baseLstRngFlags = []cli.Flag{
		listFlag,
		rangeFlag,
		prefixFlag,
		regexFlag,
		waitFlag,
		deadlineFlag,
	}

	objectFlags = map[string][]cli.Flag{
		objPut: append(
			baseObjectFlags,
			fileFlag,
			recursiveFlag,
			baseFlag,
			concurrencyFlag,
			refreshFlag,
			verboseFlag,
			yesFlag,
		),
		objGet: append(
			baseObjectFlags,
			outFileFlag,
			offsetFlag,
			lengthFlag,
			checksumFlag,
			propsFlag,
			cachedFlag,
		),
		subcommandRename: {
			nameFlag,
			newNameFlag,
			bucketFlag,
		},
		objDel: append(
			baseObjectFlags,
			baseLstRngFlags...,
		),
		objStat: append(
			baseObjectFlags,
			objPropsFlag,
			noHeaderFlag,
			jsonFlag,
		),
		objPrefetch: append(
			baseLstRngFlags,
			bucketFlag,
		),
		objEvict: append(
			baseLstRngFlags,
			bucketFlag,
			nameFlag,
		),
	}

	objectBasicUsageText = "%s object %s --bucket <value> --name <value>"
	objectGetUsage       = fmt.Sprintf(objectBasicUsageText, cliName, objGet)
	objectDelUsage       = fmt.Sprintf(objectBasicUsageText, cliName, objDel)
	objectStatUsage      = fmt.Sprintf(objectBasicUsageText, cliName, objStat)
	objectPutUsage       = fmt.Sprintf("%s object %s --bucket <value> --name <value> --file <value>", cliName, objPut)
	objectRenameUsage    = fmt.Sprintf("%s object %s --bucket <value> --name <value> --new-name <value> ", cliName, subcommandRename)
	objectPrefetchUsage  = fmt.Sprintf("%s object %s [--list <value>] [--range <value> --prefix <value> --regex <value>]", cliName, objPrefetch)
	objectEvictUsage     = fmt.Sprintf("%s object %s [--list <value>] [--range <value> --prefix <value> --regex <value>]", cliName, objEvict)

	objectCmds = []cli.Command{
		{
			Name:  commandObject,
			Usage: "interact with objects",
			Subcommands: []cli.Command{
				{
					Name:         objGet,
					Usage:        "gets the object from the specified bucket",
					UsageText:    objectGetUsage,
					Flags:        objectFlags[objGet],
					Action:       objectHandler,
					BashComplete: flagList,
				},
				{
					Name:         objPut,
					Usage:        "puts the object to the specified bucket",
					UsageText:    objectPutUsage,
					Flags:        objectFlags[objPut],
					Action:       objectHandler,
					BashComplete: flagList,
				},
				{
					Name:         objDel,
					Usage:        "deletes the object from the specified bucket",
					UsageText:    objectDelUsage,
					Flags:        objectFlags[objDel],
					Action:       objectHandler,
					BashComplete: flagList,
				},
				{
					Name:         objStat,
					Usage:        "displays basic information about the object",
					UsageText:    objectStatUsage,
					Flags:        objectFlags[objStat],
					Action:       objectHandler,
					BashComplete: flagList,
				},
				{
					Name:         subcommandRename,
					Usage:        "renames the local object",
					UsageText:    objectRenameUsage,
					Flags:        objectFlags[subcommandRename],
					Action:       objectHandler,
					BashComplete: flagList,
				},
				{
					Name:         objPrefetch,
					Usage:        "prefetches the object from the specified bucket",
					UsageText:    objectPrefetchUsage,
					Flags:        objectFlags[objPrefetch],
					Action:       objectHandler,
					BashComplete: flagList,
				},
				{
					Name:         objEvict,
					Usage:        "evicts the object from the specified bucket",
					UsageText:    objectEvictUsage,
					Flags:        objectFlags[objEvict],
					Action:       objectHandler,
					BashComplete: flagList,
				},
			},
		},
	}
)

func objectHandler(c *cli.Context) (err error) {
	if err = checkFlags(c, []cli.Flag{bucketFlag}); err != nil {
		return
	}

	baseParams := cliAPIParams(ClusterURL)
	bucket := parseStrFlag(c, bucketFlag)
	bckProvider, err := cmn.ProviderFromStr(parseStrFlag(c, bckProviderFlag))
	if err != nil {
		return
	}
	if err = canReachBucket(baseParams, bucket, bckProvider); err != nil {
		return
	}

	commandName := c.Command.Name
	switch commandName {
	case objGet:
		err = objectRetrieve(c, baseParams, bucket, bckProvider)
	case objPut:
		err = objectPut(c, baseParams, bucket, bckProvider)
	case objDel:
		err = objectDelete(c, baseParams, bucket, bckProvider)
	case objStat:
		err = objectStat(c, baseParams, bucket, bckProvider)
	case subcommandRename:
		err = objectRename(c, baseParams, bucket)
	case objPrefetch:
		err = objectPrefetch(c, baseParams, bucket, bckProvider)
	case objEvict:
		err = objectEvict(c, baseParams, bucket, bckProvider)
	default:
		return fmt.Errorf(invalidCmdMsg, commandName)
	}
	return err
}

// Get object from bucket
func objectRetrieve(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) error {
	if flagIsSet(c, cachedFlag) {
		return objectCheckCached(c, baseParams, bucket, bckProvider)
	}

	if err := checkFlags(c, []cli.Flag{nameFlag, outFileFlag}); err != nil {
		return err
	}
	if flagIsSet(c, lengthFlag) != flagIsSet(c, offsetFlag) {
		return fmt.Errorf("%s and %s flags both need to be set", lengthFlag.Name, offsetFlag.Name)
	}

	offset, err := getByteFlagValue(c, offsetFlag)
	if err != nil {
		return err
	}
	length, err := getByteFlagValue(c, lengthFlag)
	if err != nil {
		return err
	}
	obj := parseStrFlag(c, nameFlag)
	outFile := parseStrFlag(c, outFileFlag)

	query := url.Values{}
	query.Add(cmn.URLParamBckProvider, bckProvider)
	query.Add(cmn.URLParamOffset, offset)
	query.Add(cmn.URLParamLength, length)

	var objArgs api.GetObjectInput

	if outFile == outFileStdout {
		objArgs = api.GetObjectInput{Writer: os.Stdout, Query: query}
	} else {
		// Output to user location
		f, err := os.Create(outFile)
		if err != nil {
			return err
		}
		defer f.Close()
		objArgs = api.GetObjectInput{Writer: f, Query: query}
	}

	var objLen int64
	if flagIsSet(c, checksumFlag) {
		objLen, err = api.GetObjectWithValidation(baseParams, bucket, obj, objArgs)
	} else {
		objLen, err = api.GetObject(baseParams, bucket, obj, objArgs)
	}
	if err != nil {
		return err
	}

	if flagIsSet(c, lengthFlag) {
		_, _ = fmt.Fprintf(c.App.ErrWriter, "Read %s (%d B)\n", cmn.B2S(objLen, 2), objLen)
		return err
	}
	_, _ = fmt.Fprintf(c.App.ErrWriter, "%s has size %s (%d B)\n", obj, cmn.B2S(objLen, 2), objLen)
	return err
}

func objectCheckCached(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) error {
	object := parseStrFlag(c, nameFlag)

	_, err := api.HeadObject(baseParams, bucket, bckProvider, object, true)
	if err != nil {
		if err.(*cmn.HTTPError).Status == http.StatusNotFound {
			_, _ = fmt.Fprintf(c.App.Writer, "Cached: %v\n", false)
			return nil
		}
		return err
	}

	_, _ = fmt.Fprintf(c.App.Writer, "Cached: %v\n", true)
	return nil
}

func uploadFiles(c *cli.Context, baseParams *api.BaseParams, p uploadParams) error {
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
			fmt.Fprintf(c.App.Writer, "Uploaded %d(%d%%) objects, %s (%d%%)\n",
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
			_, _ = fmt.Fprintf(c.App.Writer, "Failed to open file %s: %v\n", f.path, err)
			errCount.Inc()
			return
		}

		putArgs := api.PutObjectArgs{BaseParams: baseParams, Bucket: p.bucket, BucketProvider: p.provider, Object: f.name, Reader: reader}
		if err := api.PutObject(putArgs); err != nil {
			_, _ = fmt.Fprintf(c.App.Writer, "Failed to put object %s: %v\n", f.name, err)
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
		return fmt.Errorf("Failed to upload: %d object(s)", failed)
	}

	_, _ = fmt.Fprintf(c.App.Writer, "%d objects put into %q bucket\n", len(p.files), p.bucket)
	return nil
}

func calcPutRefresh(c *cli.Context) (time.Duration, error) {
	refresh := 5 * time.Second
	if flagIsSet(c, verboseFlag) && !flagIsSet(c, refreshFlag) {
		return 0, nil
	}

	if flagIsSet(c, refreshFlag) {
		r, err := calcRefreshRate(c)
		if err != nil {
			return 0, err
		}
		refresh = r
	}
	return refresh, nil
}

// Put object into bucket
func objectPut(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) error {
	if err := checkFlags(c, []cli.Flag{fileFlag}); err != nil {
		return err
	}

	source := cmn.ExpandPath(parseStrFlag(c, fileFlag))
	path, err := filepath.Abs(source)
	if err != nil {
		return err
	}

	customName := parseStrFlag(c, nameFlag)
	objName := customName
	if objName == "" {
		// If name was not provided use the last element of the object's path as object's name
		objName = filepath.Base(source)
	}

	base := parseStrFlag(c, baseFlag)
	if base != "" {
		base = cmn.ExpandPath(base)
		base, err = filepath.Abs(base)
		if err != nil {
			return err
		}
	}

	// corner case: user sets custom object name, force one file to upload.
	// No wildcard support in this case
	if customName != "" {
		reader, err := cmn.NewFileHandle(path)
		if err != nil {
			return err
		}

		putArgs := api.PutObjectArgs{BaseParams: baseParams, Bucket: bucket, BucketProvider: bckProvider, Object: objName, Reader: reader}
		if err := api.PutObject(putArgs); err != nil {
			return err
		}

		_, _ = fmt.Fprintf(c.App.Writer, "%s put into %s bucket\n", objName, bucket)
		return nil
	}

	// here starts the mass upload.
	// first, get all files that are to be uploaded. Since it may take
	// some, show a message to a user
	_, _ = fmt.Fprintf(c.App.Writer, "Enumerating files\n")
	files, err := generateFileList(path, base, flagIsSet(c, recursiveFlag))
	if err != nil {
		return err
	}
	if len(files) == 0 {
		return fmt.Errorf("No files found")
	}

	// second,check for bucket is not empty: request only first object
	msg := cmn.SelectMsg{PageSize: 1}
	bckList, err := api.ListBucket(baseParams, bucket, &msg, 1)
	if err != nil {
		return err
	}

	// third, calculate total size and group by extensions
	totalSize, extSizes := groupByExt(files)
	totalCount := int64(len(files))
	tmpl := templates.ExtensionTmpl + strconv.FormatInt(totalCount, 10) + "\t" + cmn.B2S(totalSize, 2) + "\n"
	if err := templates.DisplayOutput(extSizes, c.App.Writer, tmpl); err != nil {
		return err
	}

	if len(bckList.Entries) != 0 {
		fmt.Fprintf(c.App.Writer, "\nWARNING: destination bucket %q is not empty\n\n", bucket)
	}

	// ask a user for confirmation if '-y' is not set in command line
	if !flagIsSet(c, yesFlag) {
		var input string
		fmt.Fprintf(c.App.Writer, "Proceed? [y/n]: ")
		fmt.Scanln(&input)
		if ok, _ := cmn.ParseBool(input); !ok {
			return fmt.Errorf("Operation canceled")
		}
	}
	refresh, err := calcPutRefresh(c)
	if err != nil {
		return err
	}
	workers := parseIntFlag(c, concurrencyFlag)
	params := uploadParams{
		bucket:    bucket,
		provider:  bckProvider,
		files:     files,
		workerCnt: workers,
		refresh:   refresh,
		totalSize: totalSize,
	}
	return uploadFiles(c, baseParams, params)
}

// Deletes object from bucket
func objectDelete(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) (err error) {
	if flagIsSet(c, listFlag) && flagIsSet(c, rangeFlag) {
		return fmt.Errorf("cannot use both %s and %s", listFlag.Name, rangeFlag.Name)
	}

	// Normal usage
	if flagIsSet(c, nameFlag) {
		obj := parseStrFlag(c, nameFlag)
		if err = api.DeleteObject(baseParams, bucket, obj, bckProvider); err != nil {
			return
		}
		_, _ = fmt.Fprintf(c.App.Writer, "%s deleted from %s bucket\n", obj, bucket)
		return
	} else if flagIsSet(c, listFlag) {
		// List Delete
		return listOp(c, baseParams, objDel, bucket, bckProvider)
	} else if flagIsSet(c, rangeFlag) {
		// Range Delete
		return rangeOp(c, baseParams, objDel, bucket, bckProvider)
	}

	return errors.New(c.Command.UsageText)
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
		return headSb.String() + bodySb.String() + "\n"
	}

	return bodySb.String()
}

// Displays object properties
func objectStat(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) error {
	if err := checkFlags(c, []cli.Flag{nameFlag}); err != nil {
		return err
	}

	props, propsFlag := "local,", ""
	if flagIsSet(c, objPropsFlag) {
		propsFlag = parseStrFlag(c, objPropsFlag)
	}
	if propsFlag == "all" || propsFlag == "" {
		props += strings.Join(cmn.GetPropsAll, ",")
	} else {
		props += parseStrFlag(c, objPropsFlag)
	}

	tmpl := buildObjStatTemplate(props, !flagIsSet(c, noHeaderFlag))
	name := parseStrFlag(c, nameFlag)
	objProps, err := api.HeadObject(baseParams, bucket, bckProvider, name)
	if err != nil {
		return handleObjHeadError(err, bucket, name)
	}

	return templates.DisplayOutput(objProps, c.App.Writer, tmpl, flagIsSet(c, jsonFlag))
}

// Prefetch operations
func objectPrefetch(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) (err error) {
	if flagIsSet(c, listFlag) {
		// List prefetch
		return listOp(c, baseParams, objPrefetch, bucket, bckProvider)
	} else if flagIsSet(c, rangeFlag) {
		// Range prefetch
		return rangeOp(c, baseParams, objPrefetch, bucket, bckProvider)
	}

	return errors.New(c.Command.UsageText)
}

// Evict operations
func objectEvict(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider string) (err error) {
	if flagIsSet(c, nameFlag) {
		// Name evict
		name := parseStrFlag(c, nameFlag)
		if err := api.EvictObject(baseParams, bucket, name); err != nil {
			return err
		}
		_, _ = fmt.Fprintf(c.App.Writer, "%s evicted from %s bucket\n", name, bucket)
		return
	} else if flagIsSet(c, listFlag) {
		// List evict
		return listOp(c, baseParams, objEvict, bucket, bckProvider)
	} else if flagIsSet(c, rangeFlag) {
		// Range evict
		return rangeOp(c, baseParams, objEvict, bucket, bckProvider)
	}

	return errors.New(c.Command.UsageText)
}

// Renames object
func objectRename(c *cli.Context, baseParams *api.BaseParams, bucket string) (err error) {
	if err = checkFlags(c, []cli.Flag{nameFlag, newNameFlag}); err != nil {
		return
	}
	obj := parseStrFlag(c, nameFlag)
	newName := parseStrFlag(c, newNameFlag)
	if err = api.RenameObject(baseParams, bucket, obj, newName); err != nil {
		return
	}

	_, _ = fmt.Fprintf(c.App.Writer, "%s renamed to %s\n", obj, newName)
	return
}

// =======================HELPERS=========================
// List handler
func listOp(c *cli.Context, baseParams *api.BaseParams, command, bucket, bckProvider string) (err error) {
	fileList := makeList(parseStrFlag(c, listFlag), ",")
	wait := flagIsSet(c, waitFlag)
	deadline, err := time.ParseDuration(parseStrFlag(c, deadlineFlag))
	if err != nil {
		return
	}

	switch command {
	case objDel:
		err = api.DeleteList(baseParams, bucket, bckProvider, fileList, wait, deadline)
		command += "d"
	case objPrefetch:
		err = api.PrefetchList(baseParams, bucket, cmn.CloudBs, fileList, wait, deadline)
		command += "ed"
	case objEvict:
		err = api.EvictList(baseParams, bucket, cmn.CloudBs, fileList, wait, deadline)
		command += "ed"
	default:
		return fmt.Errorf(invalidCmdMsg, command)
	}
	if err != nil {
		return
	}
	_, _ = fmt.Fprintf(c.App.Writer, "%s %s from %s bucket\n", fileList, command, bucket)
	return
}

// Range handler
func rangeOp(c *cli.Context, baseParams *api.BaseParams, command, bucket, bckProvider string) (err error) {
	var (
		wait     = flagIsSet(c, waitFlag)
		prefix   = parseStrFlag(c, prefixFlag)
		regex    = parseStrFlag(c, regexFlag)
		rangeStr = parseStrFlag(c, rangeFlag)
	)

	deadline, err := time.ParseDuration(parseStrFlag(c, deadlineFlag))
	if err != nil {
		return
	}

	switch command {
	case objDel:
		err = api.DeleteRange(baseParams, bucket, bckProvider, prefix, regex, rangeStr, wait, deadline)
		command += "d"
	case objPrefetch:
		err = api.PrefetchRange(baseParams, bucket, cmn.CloudBs, prefix, regex, rangeStr, wait, deadline)
		command += "ed"
	case objEvict:
		err = api.EvictRange(baseParams, bucket, cmn.CloudBs, prefix, regex, rangeStr, wait, deadline)
		command += "ed"
	default:
		return fmt.Errorf(invalidCmdMsg, command)
	}
	if err != nil {
		return
	}
	_, _ = fmt.Fprintf(c.App.Writer, "%s files with prefix '%s' matching '%s' in the range '%s' from %s bucket\n",
		command, prefix, regex, rangeStr, bucket)
	return
}

// This function is needed to print a nice error message for the user
func handleObjHeadError(err error, bucket, object string) error {
	httpErr, ok := err.(*cmn.HTTPError)
	if !ok {
		return err
	}
	if httpErr.Status == http.StatusNotFound {
		return fmt.Errorf("no such object %q in bucket %q", object, bucket)
	}

	return err
}
