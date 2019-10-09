// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands related to specific (not supported for other entities) object actions.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"os"
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
			targetFlag,
			promoteFlag,
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
			ArgsUsage:    putObjectArgument,
			Flags:        objectSpecificCmdsFlags[commandPut],
			Action:       putHandler,
			BashComplete: flagCompletions, // FIXME
		},
	}
)

func prefetchHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		bucket     string
		provider   string
	)

	if provider, err = bucketProvider(c); err != nil {
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
		return listOrRangeOp(c, baseParams, commandPrefetch, bucket, provider)
	}

	return missingArgumentsError(c, "object list or range")
}

func evictHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		bucket     string
		provider   string
	)

	if provider, err = bucketProvider(c); err != nil {
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
		baseParams  = cliAPIParams(ClusterURL)
		fullObjName = c.Args().Get(0) // empty string if arg not given
		outFile     = c.Args().Get(1) // empty string if arg not given
		provider    string
		bucket      string
		object      string
	)

	if c.NArg() < 1 {
		return missingArgumentsError(c, "object name in format bucket/object", "output file")
	}
	if c.NArg() < 2 && !flagIsSet(c, isCachedFlag) {
		return missingArgumentsError(c, "output file")
	}
	if provider, err = bucketProvider(c); err != nil {
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
	if err = canReachBucket(baseParams, bucket, provider); err != nil {
		return
	}

	return getObject(c, baseParams, bucket, provider, object, outFile)
}

func putHandler(c *cli.Context) (err error) {
	var (
		baseParams  = cliAPIParams(ClusterURL)
		fileName    = c.Args().Get(0)
		fullObjName = c.Args().Get(1)
		provider    string
		bucket      string
		objName     string
	)

	if c.NArg() < 1 {
		return missingArgumentsError(c, "file to upload", "object name in format bucket/[object]")
	}
	if c.NArg() < 2 {
		return missingArgumentsError(c, "object name in the form bucket/[object]")
	}
	if provider, err = bucketProvider(c); err != nil {
		return
	}
	bucket, objName = splitBucketObject(fullObjName)
	if bucket == "" {
		bucket, _ = os.LookupEnv(aisBucketEnvVar) // try env bucket var
		if bucket == "" {
			return incorrectUsageError(c, fmt.Errorf("no bucket specified in '%s'", fullObjName))
		}
	}
	if err = canReachBucket(baseParams, bucket, provider); err != nil {
		return
	}

	if flagIsSet(c, promoteFlag) {
		return promoteFile(c, baseParams, bucket, provider, objName, fileName)
	}

	return putObject(c, baseParams, bucket, provider, objName, fileName)
}
