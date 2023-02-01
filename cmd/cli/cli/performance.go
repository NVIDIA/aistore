// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains util functions and types.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
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
		jsonFlag,
	)
	showCmdPeformance = cli.Command{
		Name:      commandPerf,
		Usage:     showPerfArgument,
		ArgsUsage: optionalTargetIDArgument,
		Flags:     append(showPerfFlags, allPerformanceTables),
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
		Name:         cmdShowCounters,
		Usage:        "show GET, PUT, and list-objects counters; show error counters, if any",
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

// TODO -- FIXME: work in progress

func showPerfHandler(c *cli.Context) error {
	_, _, err := argNode(c, 0)
	return err
}

func showCountersHandler(c *cli.Context) error {
	var (
		usejs      = flagIsSet(c, jsonFlag)
		hideHeader = flagIsSet(c, noHeaderFlag)
	)
	smap, err := fillNodeStatusMap(c, apc.Target)
	if err != nil {
		return err
	}
	out := tmpls.NewCountersTab(curTgtStatus, smap).Template(hideHeader)
	return tmpls.Print(curTgtStatus, c.App.Writer, out, nil, usejs)
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
