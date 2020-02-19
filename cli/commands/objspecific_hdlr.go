// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands related to specific (not supported for other entities) object actions.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	objectSpecificCmdsFlags = map[string][]cli.Flag{
		commandPrefetch: append(
			baseLstRngFlags,
			providerFlag,
		),
		commandEvict: append(
			baseLstRngFlags,
			providerFlag,
		),
		commandGet: {
			providerFlag,
			offsetFlag,
			lengthFlag,
			checksumFlag,
			isCachedFlag,
		},
		commandPut: {
			providerFlag,
			recursiveFlag,
			baseFlag,
			concurrencyFlag,
			refreshFlag,
			verboseFlag,
			yesFlag,
		},
		commandPromote: {
			providerFlag,
			recursiveFlag,
			overwriteFlag,
			baseFlag,
			targetFlag,
			verboseFlag,
		},
		commandCompose: {
			providerFlag,
			verboseFlag,
			recursiveFlag,
		},
	}

	objectSpecificCmds = []cli.Command{
		{
			Name:         commandPrefetch,
			Usage:        "prefetches objects from cloud buckets",
			ArgsUsage:    bucketArgument,
			Flags:        objectSpecificCmdsFlags[commandPrefetch],
			Action:       prefetchHandler,
			BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, true /* multiple */, false /* separator */, cmn.Cloud),
		},
		{
			Name:         commandEvict,
			Usage:        "evicts objects from the cache",
			ArgsUsage:    optionalObjectsArgument,
			Flags:        objectSpecificCmdsFlags[commandEvict],
			Action:       evictHandler,
			BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, true /* multiple */, true /* separator */, cmn.Cloud),
		},
		{
			Name:         commandGet,
			Usage:        "gets the object from the specified bucket",
			ArgsUsage:    getObjectArgument,
			Flags:        objectSpecificCmdsFlags[commandGet],
			Action:       getHandler,
			BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, false /* multiple */, true /* separator */),
		},
		{
			Name:         commandPut,
			Usage:        "puts objects to the specified bucket",
			ArgsUsage:    putPromoteObjectArgument,
			Flags:        objectSpecificCmdsFlags[commandPut],
			Action:       putHandler,
			BashComplete: putPromoteObjectCompletions,
		},
		{
			Name:         commandPromote,
			Usage:        "promotes AIStore-local files and directories to objects",
			ArgsUsage:    putPromoteObjectArgument,
			Flags:        objectSpecificCmdsFlags[commandPromote],
			Action:       promoteHandler,
			BashComplete: putPromoteObjectCompletions,
		},
		{
			Name:      commandCompose,
			Usage:     "composes multiple files one by one into new, single object to the specified bucket",
			ArgsUsage: composeObjectArgument,
			Flags:     objectSpecificCmdsFlags[commandCompose],
			Action:    composeHandler,
		},
	}
)

func prefetchHandler(c *cli.Context) (err error) {
	var (
		bck    cmn.Bck
		bucket string
	)

	if c.NArg() > 0 {
		bucket = strings.TrimSuffix(c.Args().Get(0), "/")
	}
	if bck, err = validateBucket(c, bucket, "", false /* optional */); err != nil {
		return
	}
	if flagIsSet(c, listFlag) || flagIsSet(c, rangeFlag) {
		return listOrRangeOp(c, commandPrefetch, bck)
	}

	return missingArgumentsError(c, "object list or range")
}

func evictHandler(c *cli.Context) (err error) {
	var (
		bck    cmn.Bck
		bucket string
	)

	// default bucket or bucket argument given by the user
	if c.NArg() == 0 || (c.NArg() == 1 && strings.HasSuffix(c.Args().Get(0), "/")) {
		if c.NArg() == 1 {
			bucket = strings.TrimSuffix(c.Args().Get(0), "/")
		}
		if bck, err = validateBucket(c, bucket, "", false /* optional */); err != nil {
			return
		}
		if flagIsSet(c, listFlag) || flagIsSet(c, rangeFlag) {
			// list or range operation on a given bucket
			return listOrRangeOp(c, commandEvict, bck)
		}

		// operation on a given bucket
		return evictBucket(c, bck)
	}

	// list and range flags are invalid with object argument(s)
	if flagIsSet(c, listFlag) || flagIsSet(c, rangeFlag) {
		err = fmt.Errorf(invalidFlagsMsgFmt, strings.Join([]string{listFlag.Name, rangeFlag.Name}, ","))
		return incorrectUsageError(c, err)
	}

	// object argument(s) given by the user; operation on given object(s)
	return multiObjOp(c, commandEvict)
}

func getHandler(c *cli.Context) (err error) {
	var (
		bck             cmn.Bck
		bucket, objName string
		fullObjName     = c.Args().Get(0) // empty string if arg not given
		outFile         = c.Args().Get(1) // empty string if arg not given
	)
	if c.NArg() < 1 {
		return missingArgumentsError(c, "object name in the form bucket/object", "output file")
	}
	if c.NArg() < 2 && !flagIsSet(c, isCachedFlag) {
		return missingArgumentsError(c, "output file")
	}
	bucket, objName = splitBucketObject(fullObjName)
	if bck, err = validateBucket(c, bucket, fullObjName, false /* optional */); err != nil {
		return
	}
	if objName == "" {
		return incorrectUsageError(c, fmt.Errorf("'%s': missing object name", fullObjName))
	}
	return getObject(c, bck, objName, outFile)
}

func putHandler(c *cli.Context) (err error) {
	var (
		bck             cmn.Bck
		bucket, objName string
		fileName        = c.Args().Get(0)
		fullObjName     = c.Args().Get(1)
	)
	if c.NArg() < 1 {
		return missingArgumentsError(c, "file to upload", "object name in the form bucket/[object]")
	}
	if c.NArg() < 2 {
		return missingArgumentsError(c, "object name in the form bucket/[object]")
	}
	bucket, objName = splitBucketObject(fullObjName)
	if bck, err = validateBucket(c, bucket, fullObjName, false /* optional */); err != nil {
		return
	}
	return putObject(c, bck, objName, fileName)
}

func composeHandler(c *cli.Context) (err error) {
	var (
		bck             cmn.Bck
		bucket, objName string
	)
	if c.NArg() < 1 {
		return missingArgumentsError(c, "at least one file to upload", "object name in the form bucket/[object]")
	}
	if c.NArg() < 2 {
		return missingArgumentsError(c, "object name in the form bucket/object")
	}

	fullObjName := c.Args().Get(len(c.Args()) - 1)
	fileNames := make([]string, len(c.Args())-1)
	for i := 0; i < len(c.Args())-1; i++ {
		fileNames[i] = c.Args().Get(i)
	}

	bucket, objName = splitBucketObject(fullObjName)
	if objName == "" {
		return fmt.Errorf("object name is required")
	}
	if bck, err = validateBucket(c, bucket, fullObjName, false /* optional */); err != nil {
		return
	}

	return composeObject(c, bck, objName, fileNames)
}

func promoteHandler(c *cli.Context) (err error) {
	var (
		bck             cmn.Bck
		bucket, objName string
		fqn             = c.Args().Get(0)
		fullObjName     = c.Args().Get(1)
	)
	if c.NArg() < 1 {
		return missingArgumentsError(c, "file|directory to promote")
	}
	if c.NArg() < 2 {
		return missingArgumentsError(c, "object name in the form bucket/[object]")
	}
	bucket, objName = splitBucketObject(fullObjName)
	if bck, err = validateBucket(c, bucket, fullObjName, false /* optional */); err != nil {
		return
	}
	return promoteFileOrDir(c, bck, objName, fqn)
}
