// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that wait for specific task.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/urfave/cli"
)

var (
	waitCmdsFlags = map[string][]cli.Flag{
		subcmdWaitXaction: {
			timeoutFlag,
		},
		subcmdWaitDownload: {
			refreshFlag,
			progressBarFlag,
		},
		subcmdWaitDSort: {
			refreshFlag,
			progressBarFlag,
		},
	}

	jobWaitSubcmds = cli.Command{
		Name:  commandWait,
		Usage: "wait for a specific task to finish",
		Subcommands: []cli.Command{
			{
				Name:         subcmdWaitXaction,
				Usage:        "wait for an xaction to finish",
				ArgsUsage:    "XACTION_ID|XACTION_NAME [BUCKET]",
				Flags:        waitCmdsFlags[subcmdWaitXaction],
				Action:       waitXactionHandler,
				BashComplete: xactionCompletions(""),
			},
			{
				Name:         subcmdWaitDownload,
				Usage:        "wait for a download to finish",
				ArgsUsage:    jobIDArgument,
				Flags:        waitCmdsFlags[subcmdWaitDownload],
				Action:       waitDownloadHandler,
				BashComplete: downloadIDRunningCompletions,
			},
			{
				Name:         subcmdWaitDSort,
				Usage:        fmt.Sprintf("wait for %s to finish", dsort.DSortName),
				ArgsUsage:    jobIDArgument,
				Flags:        waitCmdsFlags[subcmdWaitDSort],
				Action:       waitDSortHandler,
				BashComplete: dsortIDRunningCompletions,
			},
		},
	}
)

func waitXactionHandler(c *cli.Context) error {
	if c.NArg() == 0 {
		return missingArgumentsError(c, "xaction name")
	}

	_, xactID, xactKind, bck, err := parseXactionFromArgs(c)
	if err != nil {
		return err
	}

	xactArgs := api.XactReqArgs{ID: xactID, Kind: xactKind, Bck: bck, Timeout: parseDurationFlag(c, timeoutFlag)}
	status, err := api.WaitForXactionIC(apiBP, xactArgs)
	if err != nil {
		return err
	}
	if status.Aborted() {
		if xactID != "" {
			return fmt.Errorf("xaction %q was aborted", xactID)
		}
		if bck.IsEmpty() {
			return fmt.Errorf("xaction %q was aborted", xactKind)
		}
		return fmt.Errorf("xaction %q (bucket %q) was aborted", xactKind, bck)
	}
	return nil
}

func waitDownloadHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, "job id")
	}

	var (
		aborted     bool
		refreshRate = calcRefreshRate(c)
		id          = c.Args()[0]
	)

	if flagIsSet(c, progressBarFlag) {
		downloadingResult, err := newDownloaderPB(apiBP, id, refreshRate).run()
		if err != nil {
			return err
		}

		fmt.Fprintln(c.App.Writer, downloadingResult)
		return nil
	}

	for {
		resp, err := api.DownloadStatus(apiBP, id, true)
		if err != nil {
			return err
		}

		aborted = resp.Aborted
		if aborted || resp.JobFinished() {
			break
		}
		time.Sleep(refreshRate)
	}

	if aborted {
		return fmt.Errorf("download job with id %q was aborted", id)
	}
	return nil
}

func waitDSortHandler(c *cli.Context) (err error) {
	if c.NArg() == 0 {
		return missingArgumentsError(c, "job id")
	}

	var (
		aborted     bool
		refreshRate = calcRefreshRate(c)
		id          = c.Args()[0]
	)

	if flagIsSet(c, progressBarFlag) {
		dsortResult, err := newDSortPB(apiBP, id, refreshRate).run()
		if err != nil {
			return err
		}
		fmt.Fprintln(c.App.Writer, dsortResult)
		return nil
	}

	for {
		resp, err := api.MetricsDSort(apiBP, id)
		if err != nil {
			return err
		}

		finished := true
		for _, targetMetrics := range resp {
			aborted = aborted || targetMetrics.Aborted.Load()
			finished = finished && targetMetrics.Creation.Finished
		}
		if aborted || finished {
			break
		}
		time.Sleep(refreshRate)
	}

	if aborted {
		return fmt.Errorf("dsort job with id %q was aborted", id)
	}
	return nil
}
