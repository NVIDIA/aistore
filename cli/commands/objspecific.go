// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands related to specific (not supported for other entities) object actions.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	objectSpecificCmdsFlags = map[string][]cli.Flag{
		commandPrefetch: append(
			baseLstRngFlags,
			bckProviderFlag,
		),
		commandEvict: append(
			baseLstRngFlags,
			bckProviderFlag,
		),
		commandGet: {
			bckProviderFlag,
			offsetFlag,
			lengthFlag,
			checksumFlag,
			isCachedFlag,
		},
		commandPut: {
			bckProviderFlag,
			recursiveFlag,
			baseFlag,
			concurrencyFlag,
			refreshFlag,
			verboseFlag,
			yesFlag,
		},
	}

	objectSpecificCmds = []cli.Command{
		{
			Name:         commandPrefetch,
			Usage:        "prefetches objects from cloud buckets",
			ArgsUsage:    objectPrefetchBucketArgumentText,
			Flags:        objectSpecificCmdsFlags[commandPrefetch],
			Action:       prefetchHandler,
			BashComplete: bucketList([]cli.BashCompleteFunc{}, true /* multiple */, true /* separator */, cmn.Cloud),
		},
		{
			Name:         commandEvict,
			Usage:        "evicts objects from the cache",
			ArgsUsage:    objectsOptionalArgumentText,
			Flags:        objectSpecificCmdsFlags[commandEvict],
			Action:       evictHandler,
			BashComplete: bucketList([]cli.BashCompleteFunc{}, true /* multiple */, true /* separator */, cmn.Cloud),
		},
		{
			Name:         commandGet,
			Usage:        "gets the object from the specified bucket",
			ArgsUsage:    objectGetArgumentText,
			Flags:        objectSpecificCmdsFlags[commandGet],
			Action:       getHandler,
			BashComplete: bucketList([]cli.BashCompleteFunc{}, false /* multiple */, true /* separator */),
		},
		{
			Name:         commandPut,
			Usage:        "puts objects to the specified bucket",
			ArgsUsage:    objectPutArgumentText,
			Flags:        objectSpecificCmdsFlags[commandPut],
			Action:       putHandler,
			BashComplete: flagList, // FIXME
		},
	}
)

func prefetchHandler(c *cli.Context) (err error) {
	var (
		baseParams  = cliAPIParams(ClusterURL)
		bucket      string
		bckProvider string
	)

	if bckProvider, err = bucketProvider(c); err != nil {
		return
	}

	// bucket name given by the user
	if c.NArg() == 1 {
		bucket = strings.TrimSuffix(c.Args().Get(0), "/")
	}

	// default, if no bucket was given
	if bucket == "" {
		bucket, _ = os.LookupEnv(aisBucketEnvVar)
		if bucket == "" {
			return missingArgumentsError(c, "bucket name")
		}
	}

	if flagIsSet(c, listFlag) || flagIsSet(c, rangeFlag) {
		return listOrRangeOp(c, baseParams, commandPrefetch, bucket, bckProvider)
	}

	return missingArgumentsError(c, "object list or range")
}

func evictHandler(c *cli.Context) (err error) {
	var (
		baseParams  = cliAPIParams(ClusterURL)
		bucket      string
		bckProvider string
	)

	if bckProvider, err = bucketProvider(c); err != nil {
		return
	}

	// default bucket or bucket argument given by the user
	if c.NArg() == 0 || (c.NArg() == 1 && strings.HasSuffix(c.Args().Get(0), "/")) {
		if c.NArg() == 1 {
			bucket = strings.TrimSuffix(c.Args().Get(0), "/")
		}
		if bucket == "" {
			bucket, _ = os.LookupEnv(aisBucketEnvVar)
			if bucket == "" {
				return missingArgumentsError(c, "bucket or object name")
			}
		}
		if flagIsSet(c, listFlag) || flagIsSet(c, rangeFlag) {
			// list or range operation on a given bucket
			return listOrRangeOp(c, baseParams, commandEvict, bucket, bckProvider)
		}

		// operation on a given bucket
		return evictBucket(c, baseParams, bucket)
	}

	// list and range flags are invalid with object argument(s)
	if flagIsSet(c, listFlag) || flagIsSet(c, rangeFlag) {
		err = fmt.Errorf(invalidFlagsMsgFmt, strings.Join([]string{listFlag.Name, rangeFlag.Name}, ","))
		return incorrectUsageError(c, err)
	}

	// object argument(s) given by the user; operation on given object(s)
	return multiObjOp(c, baseParams, commandEvict, bckProvider)
}

func getHandler(c *cli.Context) (err error) {
	var (
		baseParams  = cliAPIParams(ClusterURL)
		fullObjName = c.Args().Get(0) // empty string if arg not given
		outFile     = c.Args().Get(1) // empty string if arg not given
		bckProvider string
		bucket      string
		object      string
	)

	if c.NArg() < 1 {
		return missingArgumentsError(c, "object name in format bucket/object", "output file")
	}
	if c.NArg() < 2 && !flagIsSet(c, isCachedFlag) {
		return missingArgumentsError(c, "output file")
	}
	if bckProvider, err = bucketProvider(c); err != nil {
		return
	}
	bucket, object = splitBucketObject(fullObjName)
	if bucket == "" {
		bucket, _ = os.LookupEnv(aisBucketEnvVar) // try env bucket var
	}
	if object == "" {
		return incorrectUsageError(c, fmt.Errorf("no object specified in '%s'", fullObjName))
	}
	if bucket == "" {
		return incorrectUsageError(c, fmt.Errorf("no bucket specified for object '%s'", object))
	}
	if err = canReachBucket(baseParams, bucket, bckProvider); err != nil {
		return
	}

	return getObject(c, baseParams, bucket, bckProvider, object, outFile)
}

func putHandler(c *cli.Context) (err error) {
	var (
		baseParams  = cliAPIParams(ClusterURL)
		fileName    = c.Args().Get(0)
		fullObjName = c.Args().Get(1)
		bckProvider string
		bucket      string
		objName     string
	)

	if c.NArg() < 1 {
		return missingArgumentsError(c, "file to upload", "object name in format bucket/[object]")
	}
	if c.NArg() < 2 {
		return missingArgumentsError(c, "object name in format bucket/[object]")
	}
	if bckProvider, err = bucketProvider(c); err != nil {
		return
	}
	bucket, objName = splitBucketObject(fullObjName)
	if bucket == "" {
		bucket, _ = os.LookupEnv(aisBucketEnvVar) // try env bucket var
		if bucket == "" {
			return incorrectUsageError(c, fmt.Errorf("no bucket specified in '%s'", fullObjName))
		}
	}
	if err = canReachBucket(baseParams, bucket, bckProvider); err != nil {
		return
	}

	return putObject(c, baseParams, bucket, bckProvider, objName, fileName)
}

func getObject(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider, object, outFile string) (err error) {
	// just check if object is cached, don't get object
	if flagIsSet(c, isCachedFlag) {
		return objectCheckCached(c, baseParams, bucket, bckProvider, object)
	}

	var (
		query   = url.Values{}
		objArgs api.GetObjectInput
		objLen  int64
		offset  string
		length  string
	)

	if flagIsSet(c, lengthFlag) != flagIsSet(c, offsetFlag) {
		err = fmt.Errorf("%s and %s flags both need to be set", lengthFlag.Name, offsetFlag.Name)
		return incorrectUsageError(c, err)
	}
	if offset, err = getByteFlagValue(c, offsetFlag); err != nil {
		return
	}
	if length, err = getByteFlagValue(c, lengthFlag); err != nil {
		return
	}

	query.Add(cmn.URLParamBckProvider, bckProvider)
	query.Add(cmn.URLParamOffset, offset)
	query.Add(cmn.URLParamLength, length)

	if outFile == outFileStdout {
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
		objLen, err = api.GetObjectWithValidation(baseParams, bucket, object, objArgs)
	} else {
		objLen, err = api.GetObject(baseParams, bucket, object, objArgs)
	}
	if err != nil {
		return
	}

	if flagIsSet(c, lengthFlag) {
		fmt.Fprintf(c.App.ErrWriter, "Read %s (%d B)\n", cmn.B2S(objLen, 2), objLen)
		return
	}

	fmt.Fprintf(c.App.ErrWriter, "%s has the size %s (%d B)\n", object, cmn.B2S(objLen, 2), objLen)
	return
}

func putObject(c *cli.Context, baseParams *api.BaseParams, bucket, bckProvider, objName, fileName string) (err error) {
	path := cmn.ExpandPath(fileName)
	if path, err = filepath.Abs(path); err != nil {
		return
	}

	commonBase := parseStrFlag(c, baseFlag)
	if commonBase != "" {
		commonBase = cmn.ExpandPath(commonBase)
		if commonBase, err = filepath.Abs(commonBase); err != nil {
			return
		}
	}

	if objName != "" {
		// corner case - user gave the object name, upload one file
		fh, err := cmn.NewFileHandle(path)
		if err != nil {
			return err
		}

		putArgs := api.PutObjectArgs{
			BaseParams:     baseParams,
			Bucket:         bucket,
			BucketProvider: bckProvider,
			Object:         objName,
			Reader:         fh,
		}
		err = api.PutObject(putArgs)
		if err != nil {
			return err
		}

		fmt.Fprintf(c.App.Writer, "%s put into %s bucket\n", objName, bucket)
		return nil
	}

	// enumerate files
	fmt.Fprintf(c.App.Writer, "Enumerating files\n")
	files, err := generateFileList(path, commonBase, flagIsSet(c, recursiveFlag))
	if err != nil {
		return
	}
	if len(files) == 0 {
		return fmt.Errorf("no files found")
	}

	// check if the bucket is empty
	msg := cmn.SelectMsg{PageSize: 1}
	bckList, err := api.ListBucket(baseParams, bucket, &msg, 1)
	if err != nil {
		return
	}
	if len(bckList.Entries) != 0 {
		fmt.Fprintf(c.App.Writer, "\nWARNING: destination bucket %q is not empty\n\n", bucket)
	}

	// calculate total size, group by extension
	totalSize, extSizes := groupByExt(files)
	totalCount := int64(len(files))
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
			return fmt.Errorf("Operation canceled")
		}
	}

	refresh, err := calcPutRefresh(c)
	if err != nil {
		return
	}

	numWorkers := parseIntFlag(c, concurrencyFlag)
	params := uploadParams{
		bucket:    bucket,
		provider:  bckProvider,
		files:     files,
		workerCnt: numWorkers,
		refresh:   refresh,
		totalSize: totalSize,
	}
	return uploadFiles(c, baseParams, params)
}
