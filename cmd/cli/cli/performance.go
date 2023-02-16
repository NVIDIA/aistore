// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains util functions and types.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"regexp"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/stats"
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
		noHeaderFlag,
		regexColsFlag,
		unitsFlag,
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

// show all non-zero counters _and_ sizes (unless allColumnsFlag)
func showCountersHandler(c *cli.Context) error {
	metrics, err := getMetricNames(c)
	if err != nil {
		return err
	}

	for name := range metrics {
		if metrics[name] == stats.KindCounter || metrics[name] == stats.KindSize {
			continue
		}
		delete(metrics, name)
	}
	return showPerformanceTab(c, metrics, false)
}

// TODO -- FIXME: compute and add SUM(disk)/disks per target (w/ maybe min, max)
func showThroughputHandler(c *cli.Context) error {
	metrics, err := getMetricNames(c)
	if err != nil {
		return err
	}
	for name := range metrics {
		if metrics[name] == stats.KindThroughput {
			continue
		}
		// but also take assorted error counters:
		if name == stats.ErrPrefix+stats.GetCount ||
			name == stats.ErrPrefix+stats.PutCount ||
			name == stats.ErrPrefix+stats.ListCount ||
			name == stats.ErrPutMirrorCount ||
			name == stats.ErrHTTPWriteCount {
			continue
		}
		// otherwise
		delete(metrics, name)
	}
	return showPerformanceTab(c, metrics, true /* with units-per-time averaging*/)
}

// (generic)
func showPerformanceTab(c *cli.Context, metrics cos.StrKVs, averaging bool) error {
	var (
		regex       *regexp.Regexp
		regexStr    = parseStrFlag(c, regexColsFlag)
		hideHeader  = flagIsSet(c, noHeaderFlag)
		allCols     = flagIsSet(c, allColumnsFlag)
		units, errU = parseUnitsFlag(c, unitsFlag)
	)
	if errU != nil {
		return errU
	}
	sid, _, err := argNode(c)
	if err != nil {
		return err
	}
	debug.Assert(sid == "" || getNodeType(c, sid) == apc.Target)
	if regexStr != "" {
		regex, err = regexp.Compile(regexStr)
		if err != nil {
			return err
		}
	}

	smap, tstatusMap, _, err := fillNodeStatusMap(c, apc.Target)
	if err != nil {
		return err
	}
	if smap.CountActiveTs() == 0 {
		return cmn.NewErrNoNodes(apc.Target, smap.CountTargets())
	}

	if !averaging {
		setLongRunParams(c, 72)

		table, err := teb.NewPerformanceTab(tstatusMap, smap, sid, metrics, regex, units, allCols)
		if err != nil {
			return err
		}

		out := table.Template(hideHeader)
		return teb.Print(tstatusMap, out)
	}

	var (
		refresh            = flagIsSet(c, refreshFlag)
		sleep, averageOver = _refreshAvgRate(c)
		mapBegin           teb.DaemonStatusMap
	)
	// forever until Ctrl-C
	for {
		mapBeginUpdated, mapEnd, err := _cluStatusMapPs(c, mapBegin, metrics, averageOver)
		if err != nil || !refresh {
			return err
		}

		time.Sleep(sleep)

		table, err := teb.NewPerformanceTab(mapBeginUpdated, smap, sid, metrics, regex, units, allCols)
		if err != nil {
			return err
		}

		out := table.Template(hideHeader)
		err = teb.Print(mapBeginUpdated, out)
		if err != nil {
			return err
		}
		printLongRunFooter(c.App.Writer, 36)

		mapBegin = mapEnd
	}
}

// TODO -- FIXME: work in progress from here on ---------------

func showPerfHandler(c *cli.Context) error {
	_, _, err := argNode(c, 0)
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
