// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that control running jobs in the cluster.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/xact"
	"github.com/urfave/cli"
)

var (
	jobCmd = cli.Command{
		Name:        commandJob,
		Usage:       "monitor, query, and start/stop jobs and eXtended actions (xactions)",
		Subcommands: jobSub,
	}
	jobSub = []cli.Command{
		jobStartSub,
		jobStopSub,
		jobWaitSub,
		jobRemoveSub,
		makeAlias(showCmdJob, "", true, commandShow), // alias for `ais show`
	}
)

func initJobSub() {
	// add to `ais job start`
	jobStartSub := jobSub[0].Subcommands
	jobStartSub = append(jobStartSub, storageSvcCmds...)
	jobStartSub = append(jobStartSub, startableXactions(jobStartSub)...)

	jobSub[0].Subcommands = jobStartSub
}

func startableXactions(explDefinedCmds cli.Commands) cli.Commands {
	var (
		cmds      = make(cli.Commands, 0, 8)
		startable = xact.ListDisplayNames(true /*onlyStartable*/)
	)
outer:
	for _, xname := range startable {
		for i := range explDefinedCmds {
			cmd := &explDefinedCmds[i]
			kind, _ := xact.GetKindName(xname)
			// CLI is allowed to make it even shorter..
			if strings.HasPrefix(xname, cmd.Name) || strings.HasPrefix(kind, cmd.Name) {
				continue outer
			}
		}
		cmd := cli.Command{
			Name:   xname,
			Usage:  fmt.Sprintf("start %s", xname),
			Flags:  startCmdsFlags[subcmdStartXaction],
			Action: startXactionHandler,
		}
		if xact.IsSameScope(xname, xact.ScopeB) {
			cmd.ArgsUsage = bucketArgument
			cmd.BashComplete = bucketCompletions(bcmplop{})
		} else if xact.IsSameScope(xname, xact.ScopeGB) {
			cmd.ArgsUsage = optionalBucketArgument
			cmd.BashComplete = bucketCompletions(bcmplop{})
		}
		cmds = append(cmds, cmd)
	}
	return cmds
}
