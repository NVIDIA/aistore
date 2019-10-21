// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that show details about entities in the cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"

	"github.com/urfave/cli"
)

var (
	showCmdsFlags = map[string][]cli.Flag{
		subcmdShowBucket: {
			providerFlag,
			fastDetailsFlag,
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
		subcmdShowObject: {
			providerFlag,
			objPropsFlag,
			noHeaderFlag,
			jsonFlag,
		},
		subcmdShowNode: append(
			longRunFlags,
			jsonFlag,
		),
		subcmdShowXaction: {
			jsonFlag,
			allItemsFlag,
			activeFlag,
		},
		subcmdShowRebalance: {
			refreshFlag,
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
				{
					Name:         subcmdShowObject,
					Usage:        "shows details about an object",
					ArgsUsage:    objectArgument,
					Flags:        showCmdsFlags[subcmdShowObject],
					Action:       showObjectHandler,
					BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, false /* multiple */, true /* separator */),
				},
				{
					Name:         subcmdShowNode,
					Usage:        "shows details about a node",
					ArgsUsage:    optionalDaemonIDArgument,
					Flags:        showCmdsFlags[subcmdShowNode],
					Action:       showNodeHandler,
					BashComplete: daemonCompletions(true /* optional */, false /* omit proxies */),
				},
				{
					Name:         subcmdShowXaction,
					Usage:        "shows details about an xaction",
					ArgsUsage:    optionalXactionWithOptionalBucketArgument,
					Description:  xactKindsMsg,
					Flags:        showCmdsFlags[subcmdShowXaction],
					Action:       showXactionHandler,
					BashComplete: xactionCompletions,
				},
				{
					Name:         subcmdShowRebalance,
					Usage:        "shows details about global rebalance",
					ArgsUsage:    noArguments,
					Flags:        showCmdsFlags[subcmdShowRebalance],
					Action:       showRebalanceHandler,
					BashComplete: flagCompletions,
				},
			},
		},
	}
)

func showBucketHandler(c *cli.Context) (err error) {
	var (
		bucket, provider string
		baseParams       = cliAPIParams(clusterURL)
	)

	bucket = c.Args().First()
	if bucket, provider, err = validateBucket(c, baseParams, bucket, "", true /* optional */); err != nil {
		return
	}
	return bucketDetails(c, baseParams, bucket, provider, flagIsSet(c, fastDetailsFlag))
}

func showDisksHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(clusterURL)
		daemonID   = c.Args().First()
	)

	if _, err = fillMap(clusterURL); err != nil {
		return
	}

	if err = updateLongRunParams(c); err != nil {
		return
	}

	return daemonDiskStats(c, baseParams, daemonID, flagIsSet(c, jsonFlag), flagIsSet(c, noHeaderFlag))
}

func showDownloadsHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(clusterURL)
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
		baseParams = cliAPIParams(clusterURL)
		id         = c.Args().First()
	)

	if c.NArg() < 1 { // list all dsort jobs
		return dsortJobsList(c, baseParams, parseStrFlag(c, regexFlag))
	}

	// display status of a dsort job with given id
	return dsortJobStatus(c, baseParams, id)
}

func showNodeHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(clusterURL)
		daemonID   = c.Args().First()
	)

	if _, err = fillMap(clusterURL); err != nil {
		return
	}

	if err = updateLongRunParams(c); err != nil {
		return
	}

	return daemonStats(c, baseParams, daemonID, flagIsSet(c, jsonFlag))
}

func showXactionHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(clusterURL)
		xaction    = c.Args().Get(0) // empty string if no arg given
		bucket     = c.Args().Get(1) // empty string if no arg given
	)
	if xaction != "" {
		if _, ok := cmn.ValidXact(xaction); !ok {
			return fmt.Errorf("%q is not a valid xaction", xaction)
		}

		// valid xaction
		if bucketXactions.Contains(xaction) {
			if bucket == "" {
				return missingArgumentsError(c, fmt.Sprintf("bucket name for xaction '%s'", xaction))
			}
		} else if c.NArg() > 1 {
			fmt.Fprintf(c.App.ErrWriter, "Warning: %s is a global xaction, ignoring bucket name\n", xaction)
		}
	}

	xactStatsMap, err := api.MakeXactGetRequest(baseParams, xaction, commandStats, bucket, flagIsSet(c, allItemsFlag))
	if err != nil {
		return
	}

	if flagIsSet(c, activeFlag) {
		for daemonID, daemonStats := range xactStatsMap {
			if len(daemonStats) == 0 {
				continue
			}
			runningStats := make([]*stats.BaseXactStatsExt, 0, len(daemonStats))
			for _, xact := range daemonStats {
				if xact.Running() {
					runningStats = append(runningStats, xact)
				}
			}
			xactStatsMap[daemonID] = runningStats
		}
	}
	for daemonID, daemonStats := range xactStatsMap {
		if len(daemonStats) == 0 {
			delete(xactStatsMap, daemonID)
		}
	}

	return templates.DisplayOutput(xactStatsMap, c.App.Writer, templates.XactStatsTmpl, flagIsSet(c, jsonFlag))
}

func showObjectHandler(c *cli.Context) (err error) {
	var (
		provider, bucket, object string
		baseParams               = cliAPIParams(clusterURL)
		fullObjName              = c.Args().Get(0) // empty string if no arg given
	)

	if c.NArg() < 1 {
		return missingArgumentsError(c, "object name in format bucket/object")
	}
	bucket, object = splitBucketObject(fullObjName)
	if bucket, provider, err = validateBucket(c, baseParams, bucket, fullObjName, false /* optional */); err != nil {
		return
	}
	if object == "" {
		return incorrectUsageError(c, fmt.Errorf("no object specified in '%s'", fullObjName))
	}
	return objectStats(c, baseParams, bucket, provider, object)
}

func showRebalanceHandler(c *cli.Context) (err error) {
	var (
		baseParams  = cliAPIParams(clusterURL)
		refreshRate time.Duration
	)

	if refreshRate, err = calcRefreshRate(c); err != nil {
		return err
	}

	return showGlobalRebalance(c, baseParams, flagIsSet(c, refreshFlag), refreshRate)
}
