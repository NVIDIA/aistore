// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains util functions and types.
/*
 * Copyright (c) 2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"regexp"
	"runtime"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/stats"
	"github.com/urfave/cli"
)

type perfcb func(c *cli.Context, metrics cos.StrKVs, mapBegin, mapEnd teb.StStMap, elapsed time.Duration)

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
		averageSizeFlag,
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
		Usage: "show (GET, PUT, DELETE, RENAME, EVICT, APPEND) object counts;\n" +
			argsUsageIndent + "numbers of list-objects requests;\n" +
			argsUsageIndent + "(GET, PUT, etc.) cumulative and average sizes;\n" +
			argsUsageIndent + "associated error counters, if any, and more.",
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
	selected := make(cos.StrKVs, len(metrics))

	for name, kind := range metrics {
		if metrics[name] == stats.KindCounter || metrics[name] == stats.KindSize {
			selected[name] = kind
		}
	}
	return showPerfTab(c, selected, nil)
}

// TODO -- FIXME: compute and add SUM(disk)/disks per target (w/ maybe min, max)
func showThroughputHandler(c *cli.Context) error {
	metrics, err := getMetricNames(c)
	if err != nil {
		return err
	}
	selected := make(cos.StrKVs, len(metrics))
	for name, kind := range metrics {
		switch {
		case kind == stats.KindThroughput:
			selected[name] = kind
		case stats.IsErrMetric(name): // but also take assorted error counters:
			if strings.Contains(name, "get") || strings.Contains(name, "put") ||
				strings.Contains(name, "read") || strings.Contains(name, "write") {
				selected[name] = kind
			}
		}
	}
	return showPerfTab(c, selected, _throughput /* callback*/)
}

// update mapBegin <= (size/s)
func _throughput(c *cli.Context, metrics cos.StrKVs, mapBegin, mapEnd teb.StStMap, elapsed time.Duration) {
	seconds := cos.MaxI64(int64(elapsed.Seconds()), 1) // averaging per second
	for tid, begin := range mapBegin {
		end := mapEnd[tid]
		if end == nil {
			warn := fmt.Sprintf("missing %s in the get-stats-and-status results\n", cluster.Tname(tid))
			actionWarn(c, warn)
			continue
		}
		for name, v := range begin.Tracker {
			if kind, ok := metrics[name]; !ok || kind != stats.KindThroughput {
				continue
			}
			vend := end.Tracker[name]
			v.Value = (vend.Value - v.Value) / seconds
			begin.Tracker[name] = v
		}
	}
}

// NOTE: two built-in assumptions: one cosmetic, another major
// - ".ns" => ".n" correspondence is the cosmetic one
// - the naive way to recompute latency using the total elapsed, not the actual, time to execute so many requests...
func showLatencyHandler(c *cli.Context) error {
	metrics, err := getMetricNames(c)
	if err != nil {
		return err
	}

	warn := "using rather naive and inexact way to recompute latency based on the total elapsed time (to execute so many requests)"
	actionWarn(c, warn)

	selected := make(cos.StrKVs, len(metrics)>>1)
	for name, kind := range metrics {
		switch {
		case kind == stats.KindLatency:
			ncounter := name[:len(name)-1] // ".ns" => ".n"
			// take the pair iff exists
			if k, ok := metrics[ncounter]; ok && k == stats.KindCounter {
				selected[name] = kind
				selected[ncounter] = k
			}
		case stats.IsErrMetric(name): // ditto
			if strings.Contains(name, "get") || strings.Contains(name, "put") ||
				strings.Contains(name, "read") || strings.Contains(name, "write") {
				selected[name] = kind
			}
		}
	}
	return showPerfTab(c, selected, _latency)
}

// update mapBegin <= (elapsed/num-samples)
func _latency(c *cli.Context, metrics cos.StrKVs, mapBegin, mapEnd teb.StStMap, elapsed time.Duration) {
	for tid, begin := range mapBegin {
		end := mapEnd[tid]
		if end == nil {
			warn := fmt.Sprintf("missing %s in the get-stats-and-status results\n", cluster.Tname(tid))
			actionWarn(c, warn)
			continue
		}
		for name, v := range begin.Tracker {
			if kind, ok := metrics[name]; !ok || kind != stats.KindLatency {
				continue
			}
			ncounter := name[:len(name)-1] // ".ns" => ".n"
			if cntBegin, ok1 := begin.Tracker[ncounter]; ok1 {
				if cntEnd, ok2 := end.Tracker[ncounter]; ok2 && cntEnd.Value > cntBegin.Value {
					v.Value = int64(elapsed) / (cntEnd.Value - cntBegin.Value)
					begin.Tracker[name] = v
					continue
				}
				v.Value = 0
				begin.Tracker[name] = v
			}
		}
	}
}

// (common use)
func showPerfTab(c *cli.Context, metrics cos.StrKVs, cb perfcb) error {
	var (
		regex       *regexp.Regexp
		regexStr    = parseStrFlag(c, regexColsFlag)
		hideHeader  = flagIsSet(c, noHeaderFlag)
		allCols     = flagIsSet(c, allColumnsFlag)
		avgSize     = flagIsSet(c, averageSizeFlag)
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

	if cb == nil {
		setLongRunParams(c, 72)

		ctx := teb.PerfTabCtx{Smap: smap, Sid: sid, Metrics: metrics, Regex: regex, Units: units, AllCols: allCols, AvgSize: avgSize}
		table, err := teb.NewPerformanceTab(tstatusMap, &ctx)
		if err != nil {
			return err
		}

		out := table.Template(hideHeader)
		return teb.Print(tstatusMap, out)
	}

	var (
		refresh = flagIsSet(c, refreshFlag)
		sleep   = _refreshRate(c)
		ini     teb.StStMap
	)
	if sleep < time.Second || sleep > time.Minute {
		return fmt.Errorf("invalid %s value, got %v, expecting [1s - 1m]", qflprn(refreshFlag), sleep)
	}

	// until Ctrl-C
	for {
		mapBegin, mapEnd, err := _cluStatusBeginEnd(c, ini, sleep)
		if err != nil {
			return err
		}

		cb(c, metrics, mapBegin, mapEnd, sleep) // call back to recompute

		runtime.Gosched()

		ctx := teb.PerfTabCtx{Smap: smap, Sid: sid, Metrics: metrics, Regex: regex, Units: units, AllCols: allCols, AvgSize: avgSize}
		table, err := teb.NewPerformanceTab(mapBegin, &ctx)
		if err != nil {
			return err
		}

		out := table.Template(hideHeader)
		err = teb.Print(mapBegin, out)
		if err != nil || !refresh {
			return err
		}
		printLongRunFooter(c.App.Writer, 36)

		ini = mapEnd
	}
}

// TODO -- FIXME: work in progress from here on ---------------

func showPerfHandler(c *cli.Context) error {
	_, _, err := argNode(c, 0)
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
