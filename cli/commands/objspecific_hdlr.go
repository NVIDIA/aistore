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
			baseFlag,
			targetFlag,
		},
	}

	objectSpecificCmds = []cli.Command{
		{
			Name:         commandPrefetch,
			Usage:        "prefetches objects from cloud buckets",
			ArgsUsage:    prefetchObjectBucketArgument,
			Flags:        objectSpecificCmdsFlags[commandPrefetch],
			Action:       prefetchHandler,
			BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, true /* multiple */, true /* separator */, cmn.Cloud),
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
			BashComplete: flagCompletions, // FIXME
		},
		{
			Name:         commandPromote,
			Usage:        "promotes AIStore-resident files and directories to objects",
			ArgsUsage:    putPromoteObjectArgument,
			Flags:        objectSpecificCmdsFlags[commandPromote],
			Action:       promoteHandler,
			BashComplete: flagCompletions, // FIXME
		},
	}
)

func prefetchHandler(c *cli.Context) (err error) {
	var (
		bucket, provider string
		baseParams       = cliAPIParams(ClusterURL)
	)
	if c.NArg() > 0 {
		bucket = strings.TrimSuffix(c.Args().Get(0), "/")
	}
	if bucket, provider, err = validateBucket(c, baseParams, bucket, ""); err != nil {
		return
	}
	if flagIsSet(c, listFlag) || flagIsSet(c, rangeFlag) {
		return listOrRangeOp(c, baseParams, commandPrefetch, bucket, provider)
	}

	return missingArgumentsError(c, "object list or range")
}

func evictHandler(c *cli.Context) (err error) {
	var (
		bucket, provider string
		baseParams       = cliAPIParams(ClusterURL)
	)
	if provider, err = bucketProvider(c); err != nil {
		return
	}

	// default bucket or bucket argument given by the user
	if c.NArg() == 0 || (c.NArg() == 1 && strings.HasSuffix(c.Args().Get(0), "/")) {
		if c.NArg() == 1 {
			bucket = strings.TrimSuffix(c.Args().Get(0), "/")
		}
		if bucket, _, err = validateBucket(c, baseParams, bucket, ""); err != nil {
			return
		}
		if flagIsSet(c, listFlag) || flagIsSet(c, rangeFlag) {
			// list or range operation on a given bucket
			return listOrRangeOp(c, baseParams, commandEvict, bucket, provider)
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
	return multiObjOp(c, baseParams, commandEvict, provider)
}

func getHandler(c *cli.Context) (err error) {
	var (
		provider, bucket, objName string
		baseParams                = cliAPIParams(ClusterURL)
		fullObjName               = c.Args().Get(0) // empty string if arg not given
		outFile                   = c.Args().Get(1) // empty string if arg not given
	)
	if c.NArg() < 1 {
		return missingArgumentsError(c, "object name in the form bucket/object", "output file")
	}
	if c.NArg() < 2 && !flagIsSet(c, isCachedFlag) {
		return missingArgumentsError(c, "output file")
	}
	bucket, objName = splitBucketObject(fullObjName)
	if bucket, provider, err = validateBucket(c, baseParams, bucket, fullObjName); err != nil {
		return
	}
	if objName == "" {
		return incorrectUsageError(c, fmt.Errorf("'%s': missing object name", fullObjName))
	}
	return getObject(c, baseParams, bucket, provider, objName, outFile)
}

func putHandler(c *cli.Context) (err error) {
	var (
		provider, bucket, objName string
		baseParams                = cliAPIParams(ClusterURL)
		fileName                  = c.Args().Get(0)
		fullObjName               = c.Args().Get(1)
	)
	if c.NArg() < 1 {
		return missingArgumentsError(c, "file to upload", "object name in the form bucket/[object]")
	}
	if c.NArg() < 2 {
		return missingArgumentsError(c, "object name in the form bucket/[object]")
	}
	bucket, objName = splitBucketObject(fullObjName)
	if bucket, provider, err = validateBucket(c, baseParams, bucket, fullObjName); err != nil {
		return
	}
	return putObject(c, baseParams, bucket, provider, objName, fileName)
}

func promoteHandler(c *cli.Context) (err error) {
	var (
		provider, bucket, objName string
		baseParams                = cliAPIParams(ClusterURL)
		fileName                  = c.Args().Get(0)
		fullObjName               = c.Args().Get(1)
	)
	if c.NArg() < 1 {
		return missingArgumentsError(c, "file|directory to promote")
	}
	if c.NArg() < 2 {
		return missingArgumentsError(c, "object name in the form bucket/[object]")
	}
	bucket, objName = splitBucketObject(fullObjName)
	if bucket, provider, err = validateBucket(c, baseParams, bucket, fullObjName); err != nil {
		return
	}
	return promoteFile(c, baseParams, bucket, provider, objName, fileName)
}
