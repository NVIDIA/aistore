// Package commands provides the set of CLI commands used to communicate with the AIS cluster.
// This specific file handles the CLI commands that interact with cluster xactions
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package commands

import (
	"fmt"
	"strings"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cli/templates"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/urfave/cli"
)

const (
	xactStart = cmn.ActXactStart
	xactStop  = cmn.ActXactStop
	xactStats = cmn.ActXactStats

	xactionNameArgumentText         = "XACTION_NAME"
	xactionNameOptionalArgumentText = "[XACTION_NAME]"
)

var (
	allFlagXact   = cli.BoolFlag{Name: "all", Usage: "show all (even old) xactions"}
	baseXactFlags = []cli.Flag{bucketFlag, allFlagXact}

	xactFlags = map[string][]cli.Flag{
		xactStart: baseXactFlags,
		xactStop:  baseXactFlags,
		xactStats: append(baseXactFlags, jsonFlag),
	}

	xactKindsMsg = buildXactKindsMsg()

	xactCmds = []cli.Command{
		{
			Name:  cmn.GetWhatXaction,
			Usage: "interacts with extended actions (xactions)",
			Subcommands: []cli.Command{
				{
					Name:         xactStart,
					Usage:        "starts the extended action",
					ArgsUsage:    xactionNameArgumentText,
					Description:  xactKindsMsg,
					Flags:        xactFlags[xactStart],
					Action:       xactHandler,
					BashComplete: xactList,
				},
				{
					Name:         xactStop,
					Usage:        "stops the extended action",
					ArgsUsage:    xactionNameOptionalArgumentText,
					Description:  xactKindsMsg,
					Flags:        xactFlags[xactStop],
					Action:       xactHandler,
					BashComplete: xactList,
				},
				{
					Name:         xactStats,
					Usage:        "returns the stats of the extended action",
					ArgsUsage:    xactionNameOptionalArgumentText,
					Description:  xactKindsMsg,
					Flags:        xactFlags[xactStats],
					Action:       xactHandler,
					BashComplete: xactList,
				},
			},
		},
	}
)

func xactHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		command    = c.Command.Name
		bucket     = parseStrFlag(c, bucketFlag)
		xaction    = c.Args().First()
	)

	_, ok := cmn.ValidXact(xaction)

	if !ok && xaction != "" {
		return fmt.Errorf("%q is not a valid xaction", xaction)
	}

	xactStatsMap, err := api.GetXactStatusStats(baseParams, xaction, command, bucket, flagIsSet(c, allFlagXact))
	if err != nil {
		return err
	}

	switch command {
	case xactStart:
		_, _ = fmt.Fprintf(c.App.Writer, "started %q xaction\n", xaction)
	case xactStop:
		if xaction == "" {
			_, _ = fmt.Fprintln(c.App.Writer, "stopped all xactions")
		} else {
			_, _ = fmt.Fprintf(c.App.Writer, "stopped %q xaction\n", xaction)
		}
	case xactStats:
		for key, val := range xactStatsMap {
			if len(val) == 0 {
				delete(xactStatsMap, key)
			}
		}
		err = templates.DisplayOutput(xactStatsMap, c.App.Writer, templates.XactStatsTmpl, flagIsSet(c, jsonFlag))
	default:
		return fmt.Errorf(invalidCmdMsg, command)
	}
	return err
}

func buildXactKindsMsg() string {
	xactKinds := make([]string, 0, len(cmn.XactKind))

	for kind := range cmn.XactKind {
		xactKinds = append(xactKinds, kind)
	}

	return fmt.Sprintf("%s can be one of: %s", xactionNameArgumentText, strings.Join(xactKinds, ", "))
}
