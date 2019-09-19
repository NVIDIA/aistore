// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that show various stats.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"os"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
	"github.com/urfave/cli"
)

var (
	statsCmdsFlags = map[string][]cli.Flag{
		subcmdStatsNode: append(
			longRunFlags,
			jsonFlag,
		),
		subcmdStatsXaction: {
			jsonFlag,
			allItemsFlag,
			activeFlag,
		},
		subcmdStatsObject: {
			providerFlag,
			objPropsFlag,
			noHeaderFlag,
			jsonFlag,
		},
	}

	statsCmds = []cli.Command{
		{
			Name:  commandStats,
			Usage: "displays stats of entities in the cluster",
			Subcommands: []cli.Command{
				{
					Name:         subcmdStatsNode,
					Usage:        "displays stats of a node",
					ArgsUsage:    daemonStatsArgument,
					Flags:        statsCmdsFlags[subcmdStatsNode],
					Action:       statsNodeHandler,
					BashComplete: daemonCompletions(false /* optional */, false /* omit proxies */),
				},
				{
					Name:         subcmdStatsXaction,
					Usage:        "displays stats of an xaction",
					ArgsUsage:    stopStatsCommandXactionArgument,
					Description:  xactKindsMsg,
					Flags:        statsCmdsFlags[subcmdStatsXaction],
					Action:       statsXactionHandler,
					BashComplete: xactionCompletions,
				},
				{
					Name:         subcmdStatsObject,
					Usage:        "displays stats of an object",
					ArgsUsage:    objectArgument,
					Flags:        statsCmdsFlags[subcmdStatsObject],
					Action:       statsObjectHandler,
					BashComplete: bucketCompletions([]cli.BashCompleteFunc{}, false /* multiple */, true /* separator */),
				},
			},
		},
	}
)

func statsNodeHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		daemonID   = c.Args().First()
	)

	if c.NArg() == 0 {
		return missingArgumentsError(c, fmt.Sprintf("daemon ID or '%s'", allArgument))
	}

	if daemonID == allArgument {
		daemonID = ""
	}

	if _, err = fillMap(ClusterURL); err != nil {
		return
	}

	if err = updateLongRunParams(c); err != nil {
		return
	}

	return daemonStats(c, baseParams, daemonID, flagIsSet(c, jsonFlag))
}

func statsXactionHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		xaction    = c.Args().First() // empty string if no args given
		bucket     string
	)

	if c.NArg() == 0 {
		return missingArgumentsError(c, fmt.Sprintf("xaction name or '%s'", allArgument))
	}

	if xaction == allArgument {
		xaction = ""
		bucket = c.Args().Get(1)
		if bucket == "" {
			bucket, _ = os.LookupEnv(aisBucketEnvVar)
		}
	} else if _, ok := cmn.ValidXact(xaction); !ok {
		return fmt.Errorf("%q is not a valid xaction", xaction)
	} else { // valid xaction
		if bucketXactions.Contains(xaction) {
			bucket = c.Args().Get(1)
			if bucket == "" {
				bucket, _ = os.LookupEnv(aisBucketEnvVar)
				if bucket == "" {
					return missingArgumentsError(c, "bucket name")
				}
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

func statsObjectHandler(c *cli.Context) (err error) {
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
