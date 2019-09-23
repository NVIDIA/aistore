// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that list cluster metadata information.
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
