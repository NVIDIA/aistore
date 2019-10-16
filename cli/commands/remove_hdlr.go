// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that remove entities from the cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	removeCmdsFlags = map[string][]cli.Flag{
		subcmdRemoveBucket: {},
		subcmdRemoveObject: append(
			baseLstRngFlags,
			providerFlag,
		),
		subcmdRemoveNode:     {},
		subcmdRemoveDownload: {},
		subcmdRemoveDsort:    {},
	}

	removeCmds = []cli.Command{
		{
			Name:  commandRemove,
			Usage: "removes entities from the cluster",
			Subcommands: []cli.Command{
				{
					Name:         subcmdRemoveBucket,
					Usage:        "removes ais buckets",
					ArgsUsage:    bucketsArgument,
					Flags:        removeCmdsFlags[subcmdRemoveBucket],
					Action:       removeBucketHandler,
					BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, true /* multiple */, false /* separator */, cmn.AIS),
				},
				{
					Name:         subcmdRemoveObject,
					Usage:        "removes an object from the bucket",
					ArgsUsage:    optionalObjectsArgument,
					Flags:        removeCmdsFlags[subcmdRemoveObject],
					Action:       removeObjectHandler,
					BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, true /* multiple */, true /* separator */),
				},
				{
					Name:         subcmdRemoveNode,
					Usage:        "removes node from the cluster",
					ArgsUsage:    daemonIDArgument,
					Flags:        removeCmdsFlags[subcmdRemoveNode],
					Action:       removeNodeHandler,
					BashComplete: daemonCompletions(false /* optional */, false /* omit proxies */),
				},
				{
					Name:         subcmdRemoveDownload,
					Usage:        "removes finished download job with given id from the list",
					ArgsUsage:    jobIDArgument,
					Flags:        removeCmdsFlags[subcmdRemoveDownload],
					Action:       removeDownloadHandler,
					BashComplete: downloadIDFinishedCompletions,
				},
				{
					Name:         subcmdRemoveDsort,
					Usage:        fmt.Sprintf("remove finished %s job with given id from the list", cmn.DSortName),
					ArgsUsage:    jobIDArgument,
					Flags:        removeCmdsFlags[subcmdRemoveDsort],
					Action:       removeDsortHandler,
					BashComplete: dsortIDFinishedCompletions,
				},
			},
		},
	}
)

func removeBucketHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		buckets    []string
	)

	if buckets, err = bucketsFromArgsOrEnv(c); err != nil {
		return
	}

	return destroyBuckets(c, baseParams, buckets)
}

func removeObjectHandler(c *cli.Context) (err error) {
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
		if bucket, _, err = validateBucket(c, baseParams, bucket, "", false /* optional */); err != nil {
			return
		}
		if flagIsSet(c, listFlag) || flagIsSet(c, rangeFlag) {
			// list or range operation on a given bucket
			return listOrRangeOp(c, baseParams, commandRemove, bucket, provider)
		}

		err = fmt.Errorf("%s or %s flag not set with a single bucket argument", listFlag.Name, rangeFlag.Name)
		return incorrectUsageError(c, err)
	}

	if c.NArg() > 0 && (flagIsSet(c, rangeFlag) || flagIsSet(c, listFlag)) {
		err = fmt.Errorf(invalidFlagsMsgFmt, strings.Join([]string{listFlag.Name, rangeFlag.Name}, ","))
		return incorrectUsageError(c, err)
	}

	// list and range flags are invalid with object argument(s)
	if flagIsSet(c, listFlag) || flagIsSet(c, rangeFlag) {
		err = fmt.Errorf(invalidFlagsMsgFmt, strings.Join([]string{listFlag.Name, rangeFlag.Name}, ","))
		return incorrectUsageError(c, err)
	}

	// object argument(s) given by the user; operation on given object(s)
	return multiObjOp(c, baseParams, commandRemove, provider)
}

func removeNodeHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		daemonID   = c.Args().First()
	)

	return clusterRemoveNode(c, baseParams, daemonID)
}

func removeDownloadHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		id         = c.Args().First()
	)

	if c.NArg() < 1 {
		return missingArgumentsError(c, "download job ID")
	}

	if err = api.DownloadRemove(baseParams, id); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "download job with id %s successfully removed\n", id)
	return
}

func removeDsortHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		id         = c.Args().First()
	)

	if c.NArg() < 1 {
		return missingArgumentsError(c, cmn.DSortName+" job ID")
	}

	if err = api.RemoveDSort(baseParams, id); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "%s job with id %s successfully removed\n", cmn.DSortName, id)
	return
}
