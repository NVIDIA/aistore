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
	"github.com/NVIDIA/aistore/stats"
	"github.com/urfave/cli"
)

var (
	baseXactFlags = []cli.Flag{bucketFlag}

	xactFlags = map[string][]cli.Flag{
		xactStart: baseXactFlags,
		xactStop:  baseXactFlags,
		xactStats: append(baseXactFlags, jsonFlag, allFlag, activeFlag),
	}

	bucketXactions = bucketXactionNames()
	xactKindsMsg   = buildXactKindsMsg()

	xactCmds = []cli.Command{
		{
			Name:  commandXaction,
			Usage: "interacts with extended actions (xactions)",
			Subcommands: []cli.Command{
				{
					Name:         xactStart,
					Usage:        "starts the extended action",
					ArgsUsage:    xactionNameArgumentText,
					Description:  xactKindsMsg,
					Flags:        xactFlags[xactStart],
					Action:       xactHandler,
					BashComplete: xactStartCompletions,
				},
				{
					Name:         xactStop,
					Usage:        "stops the extended action",
					ArgsUsage:    xactionNameOptionalArgumentText,
					Description:  xactKindsMsg,
					Flags:        xactFlags[xactStop],
					Action:       xactHandler,
					BashComplete: xactStopStatsCompletions,
				},
				{
					Name:         xactStats,
					Usage:        "returns the stats of the extended action",
					ArgsUsage:    xactionNameOptionalArgumentText,
					Description:  xactKindsMsg,
					Flags:        xactFlags[xactStats],
					Action:       xactHandler,
					BashComplete: xactStopStatsCompletions,
				},
			},
		},
	}
)

func xactHandler(c *cli.Context) (err error) {
	var (
		baseParams = cliAPIParams(ClusterURL)
		command    = c.Command.Name              // aka action
		bucket     = parseStrFlag(c, bucketFlag) // bucket-specific (non-global) xactions
		xaction    = c.Args().First()            // aka xaction kind
	)
	if command == xactStart {
		if c.NArg() == 0 {
			return missingArgumentsError(c, "xaction name")
		}
		if !bucketXactions.Contains(xaction) && flagIsSet(c, bucketFlag) {
			_, _ = fmt.Fprintf(c.App.ErrWriter, "Warning: %s is a global xaction, ignoring bucket name %s\n", xaction, bucket)
			bucket = ""
		}
	}
	if c.NArg() > 0 && bucketXactions.Contains(xaction) {
		if err = checkFlags(c, []cli.Flag{bucketFlag}, fmt.Sprintf("%s is xaction that operates on buckets", xaction)); err != nil {
			return
		}
	}
	if _, ok := cmn.ValidXact(xaction); !ok && xaction != "" {
		return fmt.Errorf("%q is not a valid xaction", xaction)
	}
	switch command {
	case xactStart:
		if err = api.ExecXaction(baseParams, xaction, command, bucket); err != nil {
			return
		}
		_, _ = fmt.Fprintf(c.App.Writer, "started %q xaction\n", xaction)
	case xactStop:
		if err = api.ExecXaction(baseParams, xaction, command, bucket); err != nil {
			return
		}
		if xaction == "" {
			_, _ = fmt.Fprintln(c.App.Writer, "stopped all xactions")
		} else {
			_, _ = fmt.Fprintf(c.App.Writer, "stopped %q xaction\n", xaction)
		}
	case xactStats:
		var xactStatsMap map[string][]*stats.BaseXactStatsExt
		xactStatsMap, err = api.MakeXactGetRequest(baseParams, xaction, command, bucket, flagIsSet(c, allFlag))
		if err != nil {
			return
		}
		if flagIsSet(c, activeFlag) {
			for key, val := range xactStatsMap {
				if len(val) == 0 {
					continue
				}
				newVal := make([]*stats.BaseXactStatsExt, 0, len(val))
				for _, xact := range val {
					if xact.Running() {
						newVal = append(newVal, xact)
					}
				}
				xactStatsMap[key] = newVal
			}
		}
		for key, val := range xactStatsMap {
			if len(val) == 0 {
				delete(xactStatsMap, key)
			}
		}
		err = templates.DisplayOutput(xactStatsMap, c.App.Writer, templates.XactStatsTmpl, flagIsSet(c, jsonFlag))
	default:
		err = fmt.Errorf(invalidCmdMsg, command)
	}
	return
}

func buildXactKindsMsg() string {
	xactKinds := make([]string, 0, len(cmn.XactKind))

	for kind := range cmn.XactKind {
		xactKinds = append(xactKinds, kind)
	}

	return fmt.Sprintf("%s can be one of: %s", xactionNameArgumentText, strings.Join(xactKinds, ", "))
}

func bucketXactionNames() cmn.StringSet {
	result := make(cmn.StringSet)

	for name, meta := range cmn.XactKind {
		if !meta.IsGlobal {
			result[name] = struct{}{}
		}
	}

	return result
}
