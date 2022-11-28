// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file provides commands that remove various entities from the cluster.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/dloader"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/urfave/cli"
)

var (
	removeCmdsFlags = map[string][]cli.Flag{
		subcmdRemoveDownload: {
			allJobsFlag,
		},
		subcmdRemoveDsort: {},
	}

	jobRemoveSub = cli.Command{
		Name:  commandRemove,
		Usage: "remove finished jobs",
		Subcommands: []cli.Command{
			{
				Name:         subcmdRemoveDownload,
				Usage:        "remove finished download job(s)",
				ArgsUsage:    jobIDArgument,
				Flags:        removeCmdsFlags[subcmdRemoveDownload],
				Action:       removeDownloadHandler,
				BashComplete: downloadIDFinishedCompletions,
			},
			{
				Name:         subcmdRemoveDsort,
				Usage:        "remove finished " + dsort.DSortName + " job",
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
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}
	if err = api.RemoveDownload(apiBP, id); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "removed download job %q\n", id)
	return
}

func removeDownloadRegex(c *cli.Context) (err error) {
	var (
		dlList dloader.JobInfos
		regex  = ".*"
		cnt    int
		failed bool
	)
	dlList, err = api.DownloadGetList(apiBP, regex, false /*onlyActive*/)
	if err != nil {
		return err
	}
	for _, dl := range dlList {
		if !dl.JobFinished() {
			continue
		}
		if err = api.RemoveDownload(apiBP, dl.ID); err == nil {
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
		return missingArgumentsError(c, c.Command.ArgsUsage)
	}

	if err = api.RemoveDSort(apiBP, id); err != nil {
		return
	}

	fmt.Fprintf(c.App.Writer, "removed %s job %q\n", dsort.DSortName, id)
	return
}
