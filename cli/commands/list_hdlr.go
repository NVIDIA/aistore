// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that list information about entities in the cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"github.com/NVIDIA/aistore/cluster"
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
		subcmdListBucket: {
			regexFlag,
			noHeaderFlag,
		},
		subcmdListBckProps: {
			providerFlag,
			jsonFlag,
		},
		subcmdListObject: append(
			listObjectFlags,
			providerFlag,
		),
		subcmdListDownload: {
			regexFlag,
			progressBarFlag,
			refreshFlag,
			verboseFlag,
		},
		subcmdListDsort: {
			regexFlag,
			refreshFlag,
			verboseFlag,
			logFlag,
		},
		subcmdListConfig: {
			jsonFlag,
		},
		subcmdListDisk: append(
			longRunFlags,
			jsonFlag,
			noHeaderFlag,
		),
		subcmdListSmap: {
			jsonFlag,
		},
	}

	listCmds = []cli.Command{
		{
			Name:  commandList,
			Usage: "lists information about entities in the cluster",
			Subcommands: []cli.Command{
				{
					Name:         subcmdListBucket,
					Usage:        "lists bucket names",
					ArgsUsage:    optionalProviderArgument,
					Flags:        listCmdsFlags[subcmdListBucket],
					Action:       listBucketsHandler,
					BashComplete: providerCompletions(true /* optional */),
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
					Name:         subcmdListObject,
					Usage:        "lists bucket objects",
					ArgsUsage:    bucketArgument,
					Flags:        listCmdsFlags[subcmdListObject],
					Action:       listObjectsHandler,
					BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, false /* multiple */, false /* separator */),
				},
				{
					Name:         subcmdListDownload,
					Usage:        "lists download jobs",
					ArgsUsage:    optionalJobIDArgument,
					Flags:        listCmdsFlags[subcmdListDownload],
					Action:       listDownloadsHandler,
					BashComplete: downloadIDAllCompletions,
				},
				{
					Name:         subcmdListDsort,
					Usage:        "lists dSort jobs",
					ArgsUsage:    optionalJobIDArgument,
					Flags:        listCmdsFlags[subcmdListDsort],
					Action:       listDsortHandler,
					BashComplete: dsortIDAllCompletions,
				},
				{
					Name:         subcmdListConfig,
					Usage:        "lists daemon configuration",
					ArgsUsage:    daemonIDArgument,
					Flags:        listCmdsFlags[subcmdListConfig],
					Action:       listConfigHandler,
					BashComplete: daemonConfigSectionCompletions(false /* daemon optional */, true /* config optional */),
				},
				{
					Name:         subcmdListDisk,
					Usage:        "lists disk stats for targets",
					ArgsUsage:    targetIDArgument,
					Flags:        listCmdsFlags[subcmdListDisk],
					Action:       listDisksHandler,
					BashComplete: daemonCompletions(true /* optional */, true /* omit proxies */),
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

func listBucketsHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		provider   string
	)

	if provider, err = providerFromArgsOrEnv(c); err != nil {
		return
	}

	return listBucketNames(c, baseParams, provider)
}

func listBckPropsHandler(c *cli.Context) (err error) {
	baseParams := cliAPIParams(ClusterURL)
	return listBucketProps(c, baseParams)
}

func listObjectsHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		provider   string
		bucket     string
	)

	if bucket, err = bucketFromArgsOrEnv(c); err != nil {
		return
	}
	if provider, err = bucketProvider(c); err != nil {
		return
	}
	if err = canReachBucket(baseParams, bucket, provider); err != nil {
		return
	}

	return listBucketObj(c, baseParams, bucket)
}

func listDownloadsHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		id         = c.Args().First()
	)

	if c.NArg() < 1 { // list all download jobs
		return downloadJobsList(c, baseParams, parseStrFlag(c, regexFlag))
	}

	// display status of a download job with given id
	return downloadJobStatus(c, baseParams, id)
}

func listDsortHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		id         = c.Args().First()
	)

	if c.NArg() < 1 { // list all dsort jobs
		return dsortJobsList(c, baseParams, parseStrFlag(c, regexFlag))
	}

	// display status of a dsort job with given id
	return dsortJobStatus(c, baseParams, id)
}

func listConfigHandler(c *cli.Context) (err error) {
	if _, err = fillMap(ClusterURL); err != nil {
		return
	}
	return getConfig(c, cliAPIParams(ClusterURL))
}

func listDisksHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		daemonID   = c.Args().First()
	)

	if _, err = fillMap(ClusterURL); err != nil {
		return
	}

	if err = updateLongRunParams(c); err != nil {
		return
	}

	return daemonDiskStats(c, baseParams, daemonID, flagIsSet(c, jsonFlag), flagIsSet(c, noHeaderFlag))
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
