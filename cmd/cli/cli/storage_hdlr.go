// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/xact"
	"github.com/urfave/cli"
)

var (
	storageCmdFlags = map[string][]cli.Flag{
		cmdStgCleanup: {
			waitFlag,
			waitTimeoutFlag,
		},
	}

	storageCmd = cli.Command{
		Name:  commandStorage,
		Usage: "monitor and manage clustered storage",
		Subcommands: []cli.Command{
			makeAlias(showCmdStorage, "", true, commandShow), // alias for `ais show`
			showCmdStgSummary,
			{
				Name:         cmdStgValidate,
				Usage:        "check buckets for misplaced objects and objects that have insufficient numbers of copies or EC slices",
				ArgsUsage:    listAnyCommandArgument,
				Flags:        storageCmdFlags[cmdStgValidate],
				Action:       showMisplacedAndMore,
				BashComplete: bucketCompletions(bcmplop{}),
			},
			mpathCmd,
			showCmdDisk,
			{
				Name:         cmdStgCleanup,
				Usage:        "perform storage cleanup: remove deleted objects and old/obsolete workfiles",
				ArgsUsage:    listAnyCommandArgument,
				Flags:        storageCmdFlags[cmdStgCleanup],
				Action:       cleanupStorageHandler,
				BashComplete: bucketCompletions(bcmplop{}),
			},
		},
	}
)

func cleanupStorageHandler(c *cli.Context) (err error) {
	var (
		bck cmn.Bck
		id  string
	)
	if c.NArg() != 0 {
		bck, err = parseBckURI(c, c.Args().First(), true /*require provider*/)
		if err != nil {
			return
		}
		if _, err = headBucket(bck, true /* don't add */); err != nil {
			return
		}
	}
	xargs := xact.ArgsMsg{Kind: apc.ActStoreCleanup, Bck: bck}
	if id, err = api.StartXaction(apiBP, xargs); err != nil {
		return
	}

	if !flagIsSet(c, waitFlag) {
		if id != "" {
			fmt.Fprintf(c.App.Writer, "Started storage cleanup %q. %s\n", id, toMonitorMsg(c, id, ""))
		} else {
			fmt.Fprintf(c.App.Writer, "Started storage cleanup\n")
		}
		return
	}

	fmt.Fprintf(c.App.Writer, "Started storage cleanup %s...\n", id)
	wargs := xact.ArgsMsg{ID: id, Kind: apc.ActStoreCleanup}
	if flagIsSet(c, waitTimeoutFlag) {
		wargs.Timeout = parseDurationFlag(c, waitTimeoutFlag)
	}
	if err := api.WaitForXactionIdle(apiBP, wargs); err != nil {
		return err
	}
	fmt.Fprint(c.App.Writer, fmtXactSucceeded)
	return
}
