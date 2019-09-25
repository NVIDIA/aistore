// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that list cluster metadata information.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"errors"
	"strings"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	listObjectFlags = []cli.Flag{
		regexFlag,
		templateFlag,
		prefixFlag,
		pageSizeFlag,
		objPropsFlag,
		objLimitFlag,
		showUnmatchedFlag,
		allItemsFlag,
		fastFlag,
		noHeaderFlag,
		pagedFlag,
		maxPagesFlag,
		markerFlag,
	}

	listCmdsFlags = map[string][]cli.Flag{
		commandList:     listObjectFlags,
		subcmdListAIS:   listObjectFlags,
		subcmdListCloud: listObjectFlags,
		subcmdListBckProps: {
			providerFlag,
			jsonFlag,
		},
		subcmdListConfig: {
			jsonFlag,
		},
		subcmdListSmap: {
			jsonFlag,
		},
	}

	listCmds = []cli.Command{
		{
			Name:      commandList,
			Usage:     "lists cluster metadata information",
			Action:    defaultListHandler,
			ArgsUsage: listCommandArgument,
			Flags:     listCmdsFlags[commandList],
			Subcommands: []cli.Command{
				{
					Name:         subcmdListAIS,
					Usage:        "lists ais buckets",
					ArgsUsage:    optionalBucketWithSeparatorArgument,
					Flags:        listCmdsFlags[subcmdListAIS],
					Action:       listAISBucketsHandler,
					BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, false /* multiple */, true /* separator */, cmn.AIS),
				},
				{
					Name:         subcmdListCloud,
					Usage:        "lists cloud buckets",
					ArgsUsage:    optionalBucketWithSeparatorArgument,
					Flags:        listCmdsFlags[subcmdListCloud],
					Action:       listCloudBucketsHandler,
					BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, false /* multiple */, true /* separator */, cmn.Cloud),
				},
				{
					Name:         subcmdListBckProps,
					Usage:        "lists bucket properties",
					ArgsUsage:    bucketArgument,
					Flags:        listCmdsFlags[subcmdListBckProps],
					Action:       listBckPropsHandler,
					BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, false /* multiple */, false /* separator */),
				},
				{
					Name:         subcmdListConfig,
					Usage:        "lists daemon configuration",
					ArgsUsage:    listConfigArgument,
					Flags:        listCmdsFlags[subcmdListConfig],
					Action:       listConfigHandler,
					BashComplete: daemonConfigSectionCompletions(false /* daemon optional */, true /* config optional */),
				},
				{
					Name:         subcmdListSmap,
					Usage:        "displays an smap copy of a node",
					ArgsUsage:    optionalDaemonIDArgument,
					Flags:        listCmdsFlags[subcmdListSmap],
					Action:       listSmapHandler,
					BashComplete: daemonCompletions(true /* optional */, false /* omit proxies */),
				},
			},
		},
	}
)

// Note: This handler ignores aisBucketEnvVar and aisBucketProviderEnvVar
// because the intention is to list all buckets or auto-detect bucket provider
// for a given bucket.
func defaultListHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		bucket     = c.Args().First()
	)

	if bucket == "" {
		return listBucketNames(c, baseParams, "" /* any provider */)
	}

	if strings.HasSuffix(bucket, "/") {
		bucket = strings.TrimSuffix(bucket, "/")
		if err = canReachBucket(baseParams, bucket, "" /* auto-detect provider */); err != nil {
			return
		}
		return listBucketObj(c, baseParams, bucket)
	}

	return commandNotFoundError(c, c.Args().First())
}

// Note: This handler ignores aisBucketEnvVar because the intention
// is to list ais bucket names if bucket name isn't given.
func listAISBucketsHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		bucket     = c.Args().First()
	)

	if bucket == "" {
		return listBucketNames(c, baseParams, cmn.AIS)
	}

	if strings.HasSuffix(bucket, "/") {
		bucket = strings.TrimSuffix(bucket, "/")
		if err = canReachBucket(baseParams, bucket, cmn.AIS); err != nil {
			return
		}
		return listBucketObj(c, baseParams, bucket)
	}

	return incorrectUsageError(c, errors.New("bucket name should end with '/'"))
}

// Note: This handler ignores aisBucketEnvVar because the intention
// is to list cloud bucket names if bucket name isn't given.
func listCloudBucketsHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		bucket     = c.Args().First()
	)

	if bucket == "" {
		return listBucketNames(c, baseParams, cmn.Cloud)
	}

	if strings.HasSuffix(bucket, "/") {
		bucket = strings.TrimSuffix(bucket, "/")
		if err = canReachBucket(baseParams, bucket, cmn.Cloud); err != nil {
			return
		}
		return listBucketObj(c, baseParams, bucket)
	}

	return incorrectUsageError(c, errors.New("bucket name should end with '/'"))
}

func listBckPropsHandler(c *cli.Context) (err error) {
	baseParams := cliAPIParams(ClusterURL)
	return listBucketProps(c, baseParams)
}

func listConfigHandler(c *cli.Context) (err error) {
	if _, err = fillMap(ClusterURL); err != nil {
		return
	}
	return getConfig(c, cliAPIParams(ClusterURL))
}

func listSmapHandler(c *cli.Context) (err error) {
	var (
		baseParams  = cliAPIParams(ClusterURL)
		daemonID    = c.Args().First()
		primarySmap *cluster.Smap
	)

	if primarySmap, err = fillMap(ClusterURL); err != nil {
		return
	}

	return clusterSmap(c, baseParams, primarySmap, daemonID, flagIsSet(c, jsonFlag))
}
