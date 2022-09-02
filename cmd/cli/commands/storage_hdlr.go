// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

var (
	storageCmdFlags = map[string][]cli.Flag{
		subcmdStgSummary: append(
			longRunFlags,
			listCachedFlag,
			fastFlag,
			verboseFlag,
		),
		subcmdStgValidate:  {},
		subcmdStgMountpath: {},
		subcmdStgCleanup: {
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
				Name:         subcmdStgValidate,
				Usage:        "check buckets for misplaced objects and objects that have insufficient numbers of copies or EC slices",
				ArgsUsage:    listCommandArgument,
				Flags:        storageCmdFlags[subcmdStgValidate],
				Action:       showMisplacedAndMore,
				BashComplete: bucketCompletions(),
			},
			mpathCmd,
			showCmdDisk,
			{
				Name:         subcmdStgCleanup,
				Usage:        "perform storage cleanup: remove deleted objects and old/obsolete workfiles",
				ArgsUsage:    listCommandArgument,
				Flags:        storageCmdFlags[subcmdStgCleanup],
				Action:       cleanupStorageHandler,
				BashComplete: bucketCompletions(),
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
		bck, err = parseBckURI(c, c.Args().First())
		if err != nil {
			return
		}
		if _, err = headBucket(bck); err != nil {
			return
		}
	}
	xactArgs := api.XactReqArgs{Kind: apc.ActStoreCleanup, Bck: bck}
	if id, err = api.StartXaction(defaultAPIParams, xactArgs); err != nil {
		return
	}

	if !flagIsSet(c, waitFlag) {
		if id != "" {
			fmt.Fprintf(c.App.Writer, "Started storage cleanup %q, %s\n", id, xactProgressMsg(id))
		} else {
			fmt.Fprintf(c.App.Writer, "Started storage cleanup\n")
		}
		return
	}

	fmt.Fprintf(c.App.Writer, "Started storage cleanup %s...\n", id)
	wargs := api.XactReqArgs{ID: id, Kind: apc.ActStoreCleanup}
	if flagIsSet(c, waitTimeoutFlag) {
		wargs.Timeout = parseDurationFlag(c, waitTimeoutFlag)
	}
	if err := api.WaitForXactionIdle(defaultAPIParams, wargs); err != nil {
		return err
	}
	fmt.Fprint(c.App.Writer, fmtXactSucceeded)
	return
}
