// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file provides commands that remove various entities from the cluster.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/downloader"
	"github.com/urfave/cli"
)

var (
	removeCmdsFlags = map[string][]cli.Flag{
		subcmdRemoveDownload: {
			allJobsFlag,
		},
		subcmdRemoveDsort: {},
	}

	jobRemoveSubcmds = cli.Command{
		Name:  commandRemove,
		Usage: "remove finished jobs",
		Subcommands: []cli.Command{
			{
				Name:         subcmdRemoveDownload,
				Usage:        "remove finished download job(s) identified by job's ID or regular expression",
				ArgsUsage:    jobIDArgument,
				Flags:        removeCmdsFlags[subcmdRemoveDownload],
				Action:       removeDownloadHandler,
				BashComplete: downloadIDFinishedCompletions,
			},
			{
				Name:         subcmdRemoveDsort,
				Usage:        fmt.Sprintf("remove finished %s job identified by the job's ID", apc.DSortName),
				ArgsUsage:    jobIDArgument,
				Flags:        removeCmdsFlags[subcmdRemoveDsort],
				Action:       removeDsortHandler,
				BashComplete: dsortIDFinishedCompletions,
			},
		},
	}
)

func removeDownloadHandler(c *cli.Context) (err error) {
	id := c.Args().First()
	if flagIsSet(c, allJobsFlag) {
		return removeDownloadRegex(c)
	}
	if c.NArg() < 1 {
		return missingArgumentsError(c, "download job ID")
	}
	if err = api.RemoveDownload(defaultAPIParams, id); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "removed download job %q\n", id)
	return
}

func removeDownloadRegex(c *cli.Context) (err error) {
	var (
		dlList downloader.DlJobInfos
		regex  = ".*"
		cnt    int
		failed bool
	)
	dlList, err = api.DownloadGetList(defaultAPIParams, regex)
	if err != nil {
		return err
	}
	for _, dl := range dlList {
		if !dl.JobFinished() {
			continue
		}
		if err = api.RemoveDownload(defaultAPIParams, dl.ID); err == nil {
			fmt.Fprintf(c.App.Writer, "removed download job %q\n", dl.ID)
			cnt++
		} else {
			fmt.Fprintf(c.App.Writer, "failed to remove download job %q, err: %v\n", dl.ID, err)
			failed = true
		}
	}
	if cnt == 0 && !failed {
		fmt.Fprintf(c.App.Writer, "no finished download jobs, nothing to do\n")
	}
	return
}

func removeDsortHandler(c *cli.Context) (err error) {
	id := c.Args().First()

	if c.NArg() < 1 {
		return missingArgumentsError(c, apc.DSortName+" job ID")
	}

	if err = api.RemoveDSort(defaultAPIParams, id); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "removed %s job %q\n", apc.DSortName, id)
	return
}
