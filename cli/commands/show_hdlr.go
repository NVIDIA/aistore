// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that show details about entities in the cluster.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"os"
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

func showNodeHandler(c *cli.Context) (err error) {
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

	return daemonStats(c, baseParams, daemonID, flagIsSet(c, jsonFlag))
}

func showXactionHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		xaction    = c.Args().Get(0) // empty string if no arg given
		bucket     = c.Args().Get(1) // empty string if no arg given
	)

	if bucket == "" {
		bucket, _ = os.LookupEnv(aisBucketEnvVar)
	}

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
		baseParams  = cliAPIParams(ClusterURL)
		fullObjName = c.Args().Get(0) // empty string if no arg given
		provider    string
		bucket      string
		object      string
	)

	if c.NArg() < 1 {
		return missingArgumentsError(c, "object name in format bucket/object")
	}
	if provider, err = bucketProvider(c); err != nil {
		return
	}
	bucket, object = splitBucketObject(fullObjName)
	if bucket == "" {
		bucket, _ = os.LookupEnv(aisBucketEnvVar) // try env bucket var
	}
	if object == "" {
		return incorrectUsageError(c, fmt.Errorf("no object specified in '%s'", fullObjName))
	}
	if bucket == "" {
		return incorrectUsageError(c, fmt.Errorf("no bucket specified for object '%s'", object))
	}
	if err = canReachBucket(baseParams, bucket, provider); err != nil {
		return
	}

	return objectStats(c, baseParams, bucket, provider, object)
}

func showRebalanceHandler(c *cli.Context) (err error) {
	var (
		baseParams  = cliAPIParams(ClusterURL)
		refreshRate time.Duration
	)

	if refreshRate, err = calcRefreshRate(c); err != nil {
		return err
	}

	return monitorGlobalRebalance(c, baseParams, refreshRate)
}
