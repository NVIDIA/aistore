// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that control running jobs in the cluster.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/xact"
	"github.com/urfave/cli"
)

var (
	jobCmd = cli.Command{
		Name:        commandJob,
		Usage:       "query and manage jobs (aka extended actions or xactions)",
		Subcommands: jobSubcmds,
	}

	jobSubcmds = []cli.Command{
		jobStartSubcmds,
		jobStopSubcmds,
		jobWaitSubcmds,
		jobRemoveSubcmds,
		makeAlias(showCmdJob, "", true, commandShow), // alias for `ais show`
	}
)

func initJobSubcmds() {
	// add to `ais job start`
	jobStartSubcmds := jobSubcmds[0].Subcommands
	jobStartSubcmds = append(jobStartSubcmds, storageSvcCmds...)
	jobStartSubcmds = append(jobStartSubcmds, xactionCmds()...)

	jobSubcmds[0].Subcommands = jobStartSubcmds
}

func xactionCmds() cli.Commands {
	cmds := make(cli.Commands, 0, 16)

	splCmdKinds := make(cos.StrSet)
	// Add any xaction which requires a separate handler here.
	splCmdKinds.Add(
		apc.ActPrefetchObjects,
		apc.ActECEncode,
		apc.ActMakeNCopies,
		apc.ActLoadLomCache,
		apc.ActLRU,
		apc.ActStoreCleanup,
		apc.ActResilver,
	)

	startable := listXactions(true)
	for _, xctn := range startable {
		if splCmdKinds.Contains(xctn) {
			continue
		}
		cmd := cli.Command{
			Name:   xctn,
			Usage:  fmt.Sprintf("start %s", xctn),
			Flags:  startCmdsFlags[subcmdStartXaction],
			Action: startXactionHandler,
		}
		if xact.IsBckScope(xctn) {
			cmd.ArgsUsage = bucketArgument
			cmd.BashComplete = bucketCompletions()
		}
		cmds = append(cmds, cmd)
	}
	return cmds
}
