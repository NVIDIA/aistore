// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This file handles commands that control running jobs in the cluster.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/xaction"
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

// dynamically add some commands to `ais job start`
func init() {
	jobSubcmds[0].Subcommands = append(jobSubcmds[0].Subcommands, bucketSpecificCmds...)
	jobSubcmds[0].Subcommands = append(jobSubcmds[0].Subcommands, xactionCmds()...)
}

func xactionCmds() cli.Commands {
	cmds := make(cli.Commands, 0)

	splCmdKinds := make(cos.StringSet)
	// Add any xaction which requires a separate handler here.
	splCmdKinds.Add(
		cmn.ActPrefetchObjects,
		cmn.ActECEncode,
		cmn.ActMakeNCopies,
		cmn.ActLoadLomCache,
		cmn.ActLRU,
		cmn.ActStoreCleanup,
		cmn.ActResilver,
	)

	startable := listXactions(true)
	for _, xact := range startable {
		if splCmdKinds.Contains(xact) {
			continue
		}
		cmd := cli.Command{
			Name:   xact,
			Usage:  fmt.Sprintf("start %s", xact),
			Flags:  startCmdsFlags[subcmdStartXaction],
			Action: startXactionHandler,
		}
		if xaction.IsBckScope(xact) {
			cmd.ArgsUsage = bucketArgument
			cmd.BashComplete = bucketCompletions()
		}
		cmds = append(cmds, cmd)
	}
	return cmds
}
