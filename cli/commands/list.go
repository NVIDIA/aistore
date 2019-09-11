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
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/urfave/cli"
)

var (
	listCmdsFlags = map[string][]cli.Flag{
		listBuckets: {
			regexFlag,
			noHeaderFlag,
		},
		listBckProps: append(
			baseBucketFlags,
			jsonFlag,
		),
		listObjects: append(
			listObjectFlags,
			bckProviderFlag,
		),
		listDownloads: {
			regexFlag,
		},
		listDsort: {
			regexFlag,
		},
	}

	listCmds = []cli.Command{
		{
			Name:  commandList,
			Usage: "lists information about entities in the cluster",
			Subcommands: []cli.Command{
				{
					Name:         listBuckets,
					Usage:        "lists bucket names",
					ArgsUsage:    providerOptionalArgumentText,
					Flags:        listCmdsFlags[listBuckets],
					Action:       listBucketsHandler,
					BashComplete: providerList(true /* optional */),
				},
				{
					Name:         listBckProps,
					Usage:        "lists bucket properties",
					ArgsUsage:    bucketArgumentText,
					Flags:        listCmdsFlags[listBckProps],
					Action:       listBckPropsHandler,
					BashComplete: bucketList([]cli.BashCompleteFunc{}, false /* multiple */),
				},
				{
					Name:         listObjects,
					Usage:        "lists bucket objects",
					ArgsUsage:    bucketArgumentText,
					Flags:        listCmdsFlags[listObjects],
					Action:       listObjectsHandler,
					BashComplete: bucketList([]cli.BashCompleteFunc{}, false /* multiple */),
				},
				{
					Name:         listDownloads,
					Usage:        "lists all download jobs",
					ArgsUsage:    noArgumentsText,
					Flags:        listCmdsFlags[listDownloads],
					Action:       listDownloadsHandler,
					BashComplete: flagList,
				},
				{
					Name:         listDsort,
					Usage:        "lists all dSort jobs",
					ArgsUsage:    noArgumentsText,
					Flags:        listCmdsFlags[listDsort],
					Action:       listDsortHandler,
					BashComplete: flagList,
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
	var (
		baseParams = cliAPIParams(ClusterURL)
	)
	return listBucketProps(c, baseParams)
}

func listObjectsHandler(c *cli.Context) (err error) {
	var (
		baseParams     = cliAPIParams(ClusterURL)
		bucket         string
		bucketProvider string
	)

	if bucket, err = bucketFromArgsOrEnv(c); err != nil {
		return
	}
	if bucketProvider, err = cmn.ProviderFromStr(parseStrFlag(c, bckProviderFlag)); err != nil {
		return
	}
	if err = canReachBucket(baseParams, bucket, bucketProvider); err != nil {
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
