// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that control running jobs in the cluster.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/urfave/cli"
)

var (
	stopCmdsFlags = map[string][]cli.Flag{
		subcmdStopXaction:  {},
		subcmdStopDownload: {},
		subcmdStopDsort:    {},
	}

	jobStopSubcmds = cli.Command{
		Name:  commandStop,
		Usage: "stop a running (batch) job",
		Subcommands: []cli.Command{
			{
				Name:         subcmdStopXaction,
				Usage:        "stop xaction",
				ArgsUsage:    "XACTION_ID|XACTION_KIND [BUCKET]",
				Description:  xactionDesc(false),
				Flags:        stopCmdsFlags[subcmdStopXaction],
				Action:       stopXactionHandler,
				BashComplete: xactionCompletions(apc.ActXactStop),
			},
			{
				Name:         subcmdStopDownload,
				Usage:        "stop download",
				ArgsUsage:    jobIDArgument,
				Flags:        stopCmdsFlags[subcmdStopDownload],
				Action:       stopDownloadHandler,
				BashComplete: downloadIDRunningCompletions,
			},
			{
				Name:         subcmdStopDsort,
				Usage:        "stop " + dsort.DSortName + " job",
				ArgsUsage:    jobIDArgument,
				Action:       stopDsortHandler,
				BashComplete: dsortIDRunningCompletions,
			},
			makeAlias(stopCmdETL, "", true, commandETL),
		},
	}
)

func stopXactionHandler(c *cli.Context) (err error) {
	var sid string
	if c.NArg() == 0 {
		return missingArgumentsError(c, "xaction name or id")
	}

	_, xactID, xactKind, bck, err := parseXactionFromArgs(c)
	if err != nil {
		return err
	}

	xactArgs := api.XactReqArgs{ID: xactID, Kind: xactKind, Bck: bck}
	if err = api.AbortXaction(apiBP, xactArgs); err != nil {
		return
	}

	if xactKind != "" && xactID != "" {
		sid = fmt.Sprintf("xaction kind=%s, ID=%s", xactKind, xactID)
	} else if xactKind != "" {
		sid = fmt.Sprintf("xaction kind=%s", xactKind)
	} else {
		sid = fmt.Sprintf("xaction ID=%s", xactID)
	}
	if bck.IsEmpty() {
		fmt.Fprintf(c.App.Writer, "Stopped %s\n", sid)
	} else {
		fmt.Fprintf(c.App.Writer, "Stopped %s, bucket=%s\n", sid, bck.DisplayName())
	}
	return
}

func stopDownloadHandler(c *cli.Context) (err error) {
	id := c.Args().First()

	if c.NArg() == 0 {
		return missingArgumentsError(c, "download job ID")
	}

	if err = api.AbortDownload(apiBP, id); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "Stopped download job %s\n", id)
	return
}

func stopDsortHandler(c *cli.Context) (err error) {
	id := c.Args().First()

	if c.NArg() == 0 {
		return missingArgumentsError(c, dsort.DSortName+" job ID")
	}

	if err = api.AbortDSort(apiBP, id); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "Stopped %s job %s\n", dsort.DSortName, id)
	return
}
