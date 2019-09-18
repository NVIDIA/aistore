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
			daecluBaseFlags,
			longRunFlags...,
		),
		subcmdStatsXaction: {
			jsonFlag,
			allItemsFlag,
			activeFlag,
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
					ArgsUsage:    daemonStatsArgumentText,
					Flags:        statsCmdsFlags[subcmdStatsNode],
					Action:       statsNodeHandler,
					BashComplete: daemonSuggestions(false /* optional */, false /* omit proxies */),
				},
				{
					Name:         subcmdStatsXaction,
					Usage:        "displays stats of an xaction",
					ArgsUsage:    xactionStopStatsCommandArgumentText,
					Description:  xactKindsMsg,
					Flags:        statsCmdsFlags[subcmdStatsXaction],
					Action:       statsXactionHandler,
					BashComplete: xactStopStatsCompletions,
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
		return missingArgumentsError(c, fmt.Sprintf("daemon ID or '%s'", allArgumentText))
	}

	if daemonID == allArgumentText {
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
		return missingArgumentsError(c, fmt.Sprintf("xaction name or '%s'", allArgumentText))
	}

	if xaction == allArgumentText {
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
