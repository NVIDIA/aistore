// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles commands that control running jobs in the cluster.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
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
		Usage: "stop jobs running in the cluster",
		Subcommands: []cli.Command{
			{
				Name:         subcmdStopXaction,
				Usage:        "stop an xaction",
				ArgsUsage:    "XACTION_ID|XACTION_NAME [BUCKET]",
				Description:  xactionDesc(false),
				Flags:        stopCmdsFlags[subcmdStopXaction],
				Action:       stopXactionHandler,
				BashComplete: xactionCompletions(cmn.ActXactStop),
			},
			{
				Name:         subcmdStopDownload,
				Usage:        "stop a download job with given ID",
				ArgsUsage:    jobIDArgument,
				Flags:        stopCmdsFlags[subcmdStopDownload],
				Action:       stopDownloadHandler,
				BashComplete: downloadIDRunningCompletions,
			},
			{
				Name:         subcmdStopDsort,
				Usage:        fmt.Sprintf("stop a %s job with given ID", cmn.DSortName),
				ArgsUsage:    jobIDArgument,
				Action:       stopDsortHandler,
				BashComplete: dsortIDRunningCompletions,
			},
		},
	}
)

func stopXactionHandler(c *cli.Context) (err error) {
	var sid string
	if c.NArg() == 0 {
		return missingArgumentsError(c, "xaction name or id")
	}

	xactID, xactKind, bck, err := parseXactionFromArgs(c)
	if err != nil {
		return err
	}

	xactArgs := api.XactReqArgs{ID: xactID, Kind: xactKind, Bck: bck}
	if err = api.AbortXaction(defaultAPIParams, xactArgs); err != nil {
		return
	}

	if xactKind != "" && xactID != "" {
		sid = fmt.Sprintf("%s, ID=%q", xactKind, xactID)
	} else if xactKind != "" {
		sid = xactKind
	} else {
		sid = fmt.Sprintf("xaction ID=%q", xactID)
	}
	if bck.IsEmpty() {
		fmt.Fprintf(c.App.Writer, "Stopped %s\n", sid)
	} else {
		fmt.Fprintf(c.App.Writer, "Stopped %s, bucket=%s\n", sid, bck)
	}
	return
}

func stopDownloadHandler(c *cli.Context) (err error) {
	id := c.Args().First()

	if c.NArg() == 0 {
		return missingArgumentsError(c, "download job ID")
	}

	if err = api.AbortDownload(defaultAPIParams, id); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "download job %q successfully stopped\n", id)
	return
}

func stopDsortHandler(c *cli.Context) (err error) {
	id := c.Args().First()

	if c.NArg() == 0 {
		return missingArgumentsError(c, cmn.DSortName+" job ID")
	}

	if err = api.AbortDSort(defaultAPIParams, id); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "%s job %q successfully stopped\n", cmn.DSortName, id)
	return
}
