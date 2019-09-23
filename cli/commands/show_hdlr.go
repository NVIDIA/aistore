// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that show control information.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/cmn"

	"github.com/urfave/cli"
)

var (
	showCmdsFlags = map[string][]cli.Flag{
		subcmdShowBucket: {
			providerFlag,
		},
		subcmdShowDisk: append(
			longRunFlags,
			jsonFlag,
			noHeaderFlag,
		),
		subcmdShowDownload: {
			regexFlag,
			progressBarFlag,
			refreshFlag,
			verboseFlag,
		},
		subcmdShowDsort: {
			regexFlag,
			refreshFlag,
			verboseFlag,
			logFlag,
		},
	}

	showCmds = []cli.Command{
		{
			Name:  commandShow,
			Usage: "shows control info about entities in the cluster",
			Subcommands: []cli.Command{
				{
					Name:         subcmdShowBucket,
					Usage:        "shows details about a bucket",
					ArgsUsage:    optionalBucketArgument,
					Flags:        showCmdsFlags[subcmdShowBucket],
					Action:       showBucketHandler,
					BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, false /* multiple */, false /* separator */),
				},
				{
					Name:         subcmdShowDisk,
					Usage:        "shows disk stats for targets",
					ArgsUsage:    optionalTargetIDArgument,
					Flags:        showCmdsFlags[subcmdShowDisk],
					Action:       showDisksHandler,
					BashComplete: daemonCompletions(true /* optional */, true /* omit proxies */),
				},
				{
					Name:         subcmdShowDownload,
					Usage:        "shows information about download jobs",
					ArgsUsage:    optionalJobIDArgument,
					Flags:        showCmdsFlags[subcmdShowDownload],
					Action:       showDownloadsHandler,
					BashComplete: downloadIDAllCompletions,
				},
				{
					Name:         subcmdShowDsort,
					Usage:        fmt.Sprintf("shows information about %s jobs", cmn.DSortName),
					ArgsUsage:    optionalJobIDArgument,
					Flags:        showCmdsFlags[subcmdShowDsort],
					Action:       showDsortHandler,
					BashComplete: dsortIDAllCompletions,
				},
			},
		},
	}
)

func showBucketHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		provider   string
		bucket     string
	)

	if provider, err = bucketProvider(c); err != nil {
		return
	}

	bucket = c.Args().First()
	if bucket == "" {
		bucket, _ = os.LookupEnv(aisBucketEnvVar)
	}

	return bucketDetails(c, baseParams, bucket, provider)
}

func showDisksHandler(c *cli.Context) (err error) {
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

func showDownloadsHandler(c *cli.Context) (err error) {
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

func showDsortHandler(c *cli.Context) (err error) {
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
