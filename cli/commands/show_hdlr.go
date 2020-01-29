// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file contains implementation of the top-level `show` command.
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
			cachedFlag,
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
					Usage:        "shows bucket details",
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
					Usage:        "shows object details",
					ArgsUsage:    objectArgument,
					Flags:        showCmdsFlags[subcmdShowObject],
					Action:       showObjectHandler,
					BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, false /* multiple */, true /* separator */),
				},
				{
					Name:         subcmdShowNode,
					Usage:        "shows node details",
					ArgsUsage:    optionalDaemonIDArgument,
					Flags:        showCmdsFlags[subcmdShowNode],
					Action:       showNodeHandler,
					BashComplete: daemonCompletions(true /* optional */, false /* omit proxies */),
				},
				{
					Name:         subcmdShowXaction,
					Usage:        "shows xaction details",
					ArgsUsage:    optionalXactionWithOptionalBucketArgument,
					Description:  xactKindsMsg,
					Flags:        showCmdsFlags[subcmdShowXaction],
					Action:       showXactionHandler,
					BashComplete: xactionCompletions,
				},
				{
					Name:         subcmdShowRebalance,
					Usage:        "shows rebalance details",
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
		bck    cmn.Bck
		bucket string
	)

	bucket = c.Args().First()
	if bck, err = validateBucket(c, bucket, "", true /* optional */); err != nil {
		return
	}
	return bucketDetails(c, bck)
}

func showDisksHandler(c *cli.Context) (err error) {
	daemonID := c.Args().First()
	if _, err = fillMap(); err != nil {
		return
	}

	if err = updateLongRunParams(c); err != nil {
		return
	}

	return daemonDiskStats(c, daemonID, flagIsSet(c, jsonFlag), flagIsSet(c, noHeaderFlag))
}

func showDownloadsHandler(c *cli.Context) (err error) {
	id := c.Args().First()

	if c.NArg() < 1 { // list all download jobs
		return downloadJobsList(c, parseStrFlag(c, regexFlag))
	}

	// display status of a download job with given id
	return downloadJobStatus(c, id)
}

func showDsortHandler(c *cli.Context) (err error) {
	id := c.Args().First()

	if c.NArg() < 1 { // list all dsort jobs
		return dsortJobsList(c, parseStrFlag(c, regexFlag))
	}

	// display status of a dsort job with given id
	return dsortJobStatus(c, id)
}

func showNodeHandler(c *cli.Context) (err error) {
	daemonID := c.Args().First()
	if _, err = fillMap(); err != nil {
		return
	}

	if err = updateLongRunParams(c); err != nil {
		return
	}

	return daemonStats(c, daemonID, flagIsSet(c, jsonFlag))
}

func showXactionHandler(c *cli.Context) (err error) {
	var (
		bck     cmn.Bck
		xaction = c.Args().Get(0) // empty string if no arg given
		bucket  = c.Args().Get(1) // empty string if no arg given
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

	if bucket != "" {
		if bck, err = validateBucket(c, bucket, "", false /* optional */); err != nil {
			return
		}
	}

	xactStatsMap, err := api.MakeXactGetRequest(defaultAPIParams, bck, xaction, commandStats, flagIsSet(c, allItemsFlag))
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
		bck            cmn.Bck
		bucket, object string
		fullObjName    = c.Args().Get(0) // empty string if no arg given
	)

	if c.NArg() < 1 {
		return missingArgumentsError(c, "object name in format bucket/object")
	}
	bucket, object = splitBucketObject(fullObjName)
	if bck, err = validateBucket(c, bucket, fullObjName, false /* optional */); err != nil {
		return
	}
	if object == "" {
		return incorrectUsageError(c, fmt.Errorf("no object specified in '%s'", fullObjName))
	}
	return objectStats(c, bck, object)
}

func showRebalanceHandler(c *cli.Context) (err error) {
	var refreshRate time.Duration

	if refreshRate, err = calcRefreshRate(c); err != nil {
		return err
	}

	return showGlobalRebalance(c, flagIsSet(c, refreshFlag), refreshRate)
}
