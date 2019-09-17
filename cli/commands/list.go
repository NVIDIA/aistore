// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that list information about entities in the cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"sort"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cli/templates"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/urfave/cli"
)

var (
	listCmdsFlags = map[string][]cli.Flag{
		subcmdListBuckets: {
			regexFlag,
			noHeaderFlag,
		},
		subcmdListBckProps: append(
			baseBucketFlags,
			jsonFlag,
		),
		subcmdListObjects: append(
			listObjectFlags,
			bckProviderFlag,
		),
		subcmdListDownloads: {
			regexFlag,
		},
		subcmdListDsort: {
			regexFlag,
		},
		subcmdListConfig: {
			jsonFlag,
		},
		subcmdListDisks: append(
			append(daecluBaseFlags, longRunFlags...),
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
					Name:         subcmdListBuckets,
					Usage:        "lists bucket names",
					ArgsUsage:    providerOptionalArgumentText,
					Flags:        listCmdsFlags[subcmdListBuckets],
					Action:       listBucketsHandler,
					BashComplete: providerList(true /* optional */),
				},
				{
					Name:         subcmdListBckProps,
					Usage:        "lists bucket properties",
					ArgsUsage:    bucketArgumentText,
					Flags:        listCmdsFlags[subcmdListBckProps],
					Action:       listBckPropsHandler,
					BashComplete: bucketList([]cli.BashCompleteFunc{}, false /* multiple */, false /* separator */),
				},
				{
					Name:         subcmdListObjects,
					Usage:        "lists bucket objects",
					ArgsUsage:    bucketArgumentText,
					Flags:        listCmdsFlags[subcmdListObjects],
					Action:       listObjectsHandler,
					BashComplete: bucketList([]cli.BashCompleteFunc{}, false /* multiple */, false /* separator */),
				},
				{
					Name:         subcmdListDownloads,
					Usage:        "lists all download jobs",
					ArgsUsage:    noArgumentsText,
					Flags:        listCmdsFlags[subcmdListDownloads],
					Action:       listDownloadsHandler,
					BashComplete: flagList,
				},
				{
					Name:         subcmdListDsort,
					Usage:        "lists all dSort jobs",
					ArgsUsage:    noArgumentsText,
					Flags:        listCmdsFlags[subcmdListDsort],
					Action:       listDsortHandler,
					BashComplete: flagList,
				},
				{
					Name:         subcmdListConfig,
					Usage:        "lists daemon configuration",
					ArgsUsage:    daemonIDArgumentText,
					Flags:        listCmdsFlags[subcmdListConfig],
					Action:       listConfigHandler,
					BashComplete: daemonConfigSectionSuggestions(false /* daemon optional */, true /* config optional */),
				},
				{
					Name:         subcmdListDisks,
					Usage:        "list disk stats for targets",
					ArgsUsage:    targetIDArgumentText,
					Flags:        listCmdsFlags[subcmdListDisks],
					Action:       listDisksHandler,
					BashComplete: daemonSuggestions(true /* optional */, true /* omit proxies */),
				},
				{
					Name:         subcmdListSmap,
					Usage:        "display smap copy of a node",
					ArgsUsage:    optionalDaemonIDArgumentText,
					Flags:        listCmdsFlags[subcmdListSmap],
					Action:       listSmapHandler,
					BashComplete: daemonSuggestions(true /* optional */, false /* omit proxies */),
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

	return listBucketNamesForProvider(c, baseParams, provider)
}

func listBckPropsHandler(c *cli.Context) (err error) {
	baseParams := cliAPIParams(ClusterURL)
	return listBucketProps(c, baseParams)
}

func listObjectsHandler(c *cli.Context) (err error) {
	var (
		baseParams  = cliAPIParams(ClusterURL)
		bckProvider string
		bucket      string
	)

	if bucket, err = bucketFromArgsOrEnv(c); err != nil {
		return
	}
	if bckProvider, err = bucketProvider(c); err != nil {
		return
	}
	if err = canReachBucket(baseParams, bucket, bckProvider); err != nil {
		return
	}

	return listBucketObj(c, baseParams, bucket)
}

func listDownloadsHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		regex      = parseStrFlag(c, regexFlag)
		list       map[string]cmn.DlJobInfo
	)

	if list, err = api.DownloadGetList(baseParams, regex); err != nil {
		return
	}

	return templates.DisplayOutput(list, c.App.Writer, templates.DownloadListTmpl)
}

func listDsortHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		regex      = parseStrFlag(c, regexFlag)
		list       []*dsort.JobInfo
	)

	if list, err = api.ListDSort(baseParams, regex); err != nil {
		return err
	}

	sort.Slice(list, func(i int, j int) bool {
		if list[i].IsRunning() && !list[j].IsRunning() {
			return true
		}
		if !list[i].IsRunning() && list[j].IsRunning() {
			return false
		}
		if !list[i].Aborted && list[j].Aborted {
			return true
		}
		if list[i].Aborted && !list[j].Aborted {
			return false
		}

		return list[i].StartedTime.Before(list[j].StartedTime)
	})

	return templates.DisplayOutput(list, c.App.Writer, templates.DSortListTmpl)
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
