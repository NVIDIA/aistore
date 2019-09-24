// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that list cluster metadata information.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
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
		subcmdListAIS: {
			regexFlag,
			noHeaderFlag,
		},
		subcmdListCloud: {
			regexFlag,
			noHeaderFlag,
		},
		subcmdListBucket: append(
			listObjectFlags,
			providerFlag,
		),
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
			Name:  commandList,
			Usage: "lists cluster metadata information",
			Subcommands: []cli.Command{
				{
					Name:         subcmdListBucket,
					Usage:        "lists bucket names",
					ArgsUsage:    bucketArgument,
					Flags:        listCmdsFlags[subcmdListBucket],
					Action:       listBucketsHandler,
					BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, false /* multiple */, false /* separator */),
				},
				{
					Name:         subcmdListAIS,
					Usage:        "lists ais buckets",
					ArgsUsage:    noArguments,
					Flags:        listCmdsFlags[subcmdListAIS],
					Action:       listAISBucketsHandler,
					BashComplete: flagCompletions,
				},
				{
					Name:         subcmdListCloud,
					Usage:        "lists cloud buckets",
					ArgsUsage:    noArguments,
					Flags:        listCmdsFlags[subcmdListCloud],
					Action:       listCloudBucketsHandler,
					BashComplete: flagCompletions,
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

func listAISBucketsHandler(c *cli.Context) (err error) {
	baseParams := cliAPIParams(ClusterURL)
	return listBucketNames(c, baseParams, cmn.AIS)
}

func listCloudBucketsHandler(c *cli.Context) (err error) {
	baseParams := cliAPIParams(ClusterURL)
	return listBucketNames(c, baseParams, cmn.Cloud)
}

func listBucketsHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		provider   string
		bucket     string
	)

	if provider, err = bucketProvider(c); err != nil {
		return
	}
	if bucket, err = bucketFromArgsOrEnv(c); err != nil {
		return
	}
	if err = canReachBucket(baseParams, bucket, provider); err != nil {
		return
	}

	return listBucketObj(c, baseParams, bucket)
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
