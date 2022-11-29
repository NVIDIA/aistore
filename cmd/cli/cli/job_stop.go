// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that control running jobs in the cluster.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/dsort"
	"github.com/urfave/cli"
)

var (
	stopCmdsFlags = map[string][]cli.Flag{
		subcmdStopXaction:  {},
		subcmdStopDownload: {},
		subcmdStopDsort:    {},
	}

	jobStopSub = cli.Command{
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
				BashComplete: xactCompletions,
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
			{
				Name:   commandRebalance,
				Usage:  "stop rebalancing ais cluster",
				Flags:  clusterCmdsFlags[commandStop],
				Action: stopClusterRebalanceHandler,
			},
		},
	}
)

func stopXactionHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	// parse, validate
	_, xactID, xactKind, bck, err := parseXactionFromArgs(c)
	if err != nil {
		return err
	}
	msg := formatStoppedMsg(xactID, xactKind, bck)

	// query
	xactArgs := api.XactReqArgs{ID: xactID, Kind: xactKind}
	snap, err := getXactSnap(xactArgs)
	if err != nil {
		return fmt.Errorf("Cannot stop %s: %v", formatStoppedMsg(xactID, xactKind, bck), err)
	}
	if snap == nil {
		fmt.Fprintf(c.App.Writer, "%s not found, nothing to do\n", msg)
		return nil
	}

	// reformat
	if xactID == "" {
		xactID = snap.ID
	}
	debug.Assertf(xactID == snap.ID, "%q, %q", xactID, snap.ID)
	msg = formatStoppedMsg(xactID, xactKind, bck)

	var s string
	if snap.IsAborted() {
		s = " (aborted)"
	}
	if snap.IsAborted() || snap.Finished() {
		fmt.Fprintf(c.App.Writer, "%s is already finished%s, nothing to do\n", msg, s)
		return nil
	}

	// abort
	args := api.XactReqArgs{ID: xactID, Kind: xactKind, Bck: bck}
	if err := api.AbortXaction(apiBP, args); err != nil {
		return err
	}
	fmt.Fprintf(c.App.Writer, "Stopped %s\n", msg)
	return nil
}

func formatStoppedMsg(xactID, xactKind string, bck cmn.Bck) string {
	var sb string
	if !bck.IsEmpty() {
		sb = fmt.Sprintf(", %s", bck.DisplayName())
	}
	switch {
	case xactKind != "" && xactID != "":
		return fmt.Sprintf("%s[%s%s]", xactKind, xactID, sb)
	case xactKind != "" && sb != "":
		return fmt.Sprintf("%s[%s]", xactKind, sb)
	case xactKind != "":
		return xactKind
	default:
		return fmt.Sprintf("xaction[%s%s]", xactID, sb)
	}
}

func stopDownloadHandler(c *cli.Context) (err error) {
	id := c.Args().First()

	if c.NArg() == 0 {
		return missingArgumentsError(c, c.Command.ArgsUsage)
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
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}

	if err = api.AbortDSort(apiBP, id); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "Stopped %s job %s\n", dsort.DSortName, id)
	return
}
