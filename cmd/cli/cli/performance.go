// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains util functions and types.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"regexp"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/urfave/cli"
)

var perfCmd = cli.Command{
	Name:  commandPerf,
	Usage: showPerfArgument,
	Subcommands: []cli.Command{
		makeAlias(showCmdPeformance, "", true, commandShow),
	},
}

var (
	showPerfFlags = append(
		longRunFlags,
		allColumnsFlag,
		regexColsFlag,
	)
	showCmdPeformance = cli.Command{
		Name:      commandPerf,
		Usage:     showPerfArgument,
		ArgsUsage: optionalTargetIDArgument,
		Flags:     showPerfFlags,
		Action:    showPerfHandler,
		Subcommands: []cli.Command{
			showCounters,
			showThroughput,
			showLatency,
			showGET,
			showSysCap,
		},
	}
	showCounters = cli.Command{
		Name: cmdShowCounters,
		Usage: "show (GET, PUT, DELETE, RENAME, EVICT, APPEND) object counters;\n" +
			argsUsageIndent + "show (GET, PUT, etc.) cumulative sizes;\n" +
			argsUsageIndent + "show the number of LIST-objects requests;\n" +
			argsUsageIndent + "show error counters, if any, and more...",
		ArgsUsage:    optionalTargetIDArgument,
		Flags:        showPerfFlags,
		Action:       showCountersHandler,
		BashComplete: suggestTargetNodes,
	}
	showThroughput = cli.Command{
		Name:         cmdShowThroughput,
		Usage:        "TODO",
		ArgsUsage:    optionalTargetIDArgument,
		Flags:        showPerfFlags,
		Action:       showThroughputHandler,
		BashComplete: suggestTargetNodes,
	}
	showLatency = cli.Command{
		Name:         cmdShowLatency,
		Usage:        "TODO",
		ArgsUsage:    optionalTargetIDArgument,
		Flags:        showPerfFlags,
		Action:       showLatencyHandler,
		BashComplete: suggestTargetNodes,
	}
	showGET = cli.Command{
		Name:         cmdShowGET,
		Usage:        "TODO",
		ArgsUsage:    optionalTargetIDArgument,
		Flags:        showPerfFlags,
		Action:       showGETHandler,
		BashComplete: suggestTargetNodes,
	}
	showSysCap = cli.Command{
		Name:         cmdShowSysCap,
		Usage:        "TODO",
		ArgsUsage:    optionalTargetIDArgument,
		Flags:        showPerfFlags,
		Action:       showSysCapHandler,
		BashComplete: suggestTargetNodes,
	}
)

func showCountersHandler(c *cli.Context) (err error) {
	var (
		sid     string
		smap    *cluster.Smap
		metrics cos.StrKVs
		regex   *regexp.Regexp

		regexStr   = parseStrFlag(c, regexColsFlag)
		hideHeader = flagIsSet(c, noHeaderFlag)
		allCols    = flagIsSet(c, allColumnsFlag)
	)
	if sid, _, err = argNode(c); err != nil {
		return err
	}
	debug.Assert(sid == "" || getNodeType(c, sid) == apc.Target)
	if regexStr != "" {
		regex, err = regexp.Compile(regexStr)
		if err != nil {
			return err
		}
	}

	if smap, err = fillNodeStatusMap(c, apc.Target); err != nil {
		return err
	}
	if smap.CountActiveTs() == 0 {
		return cmn.NewErrNoNodes(apc.Target, smap.CountTargets())
	}
	if metrics, err = getMetricNames(c); err != nil {
		return err
	}

	setLongRunParams(c, 72)

	table, err := tmpls.NewCountersTab(curTgtStatus, smap, sid, metrics, regex, allCols)
	if err != nil {
		return err
	}
	out := table.Template(hideHeader)
	return tmpls.Print(curTgtStatus, c.App.Writer, out, nil, false /*usejs*/)
}

// TODO -- FIXME: work in progress from here on ---------------

func showPerfHandler(c *cli.Context) error {
	_, _, err := argNode(c, 0)
	return err
}

func showThroughputHandler(c *cli.Context) error {
	_, _, err := argNode(c)
	return err
}

func showLatencyHandler(c *cli.Context) error {
	_, _, err := argNode(c)
	return err
}

func showGETHandler(c *cli.Context) error {
	_, _, err := argNode(c)
	return err
}

func showSysCapHandler(c *cli.Context) error {
	_, _, err := argNode(c)
	return err
}
