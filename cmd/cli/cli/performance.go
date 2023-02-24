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

type (
	perfcb func(c *cli.Context,
		metrics cos.StrKVs, mapBegin, mapEnd teb.StstMap, elapsed time.Duration) bool
)

// statically defined for `latency` tab (compare with counter and throughput tabs)
var latabNames = []string{
	stats.GetLatency, stats.GetSize, stats.GetCount, stats.GetColdCount, stats.GetColdSize,
	stats.PutLatency, stats.PutSize, stats.PutCount,
	stats.AppendLatency, stats.AppendCount}

// true when called by top-level handler
var allPerfTabs bool

var (
	showPerfFlags = append(
		longRunFlags,
		allColumnsFlag,
		noHeaderFlag,
		regexColsFlag,
		unitsFlag,
		averageSizeFlag,
	)

	// alias
	perfCmd = cli.Command{
		Name:  commandPerf,
		Usage: showPerfArgument,
		Subcommands: []cli.Command{
			makeAlias(showCmdPeformance, "", true, commandShow),
		},
	}
	// `show performance` command
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
			showSysCap,
		},
	}
	showCounters = cli.Command{
		Name: cmdShowCounters,
		Usage: "show (GET, PUT, DELETE, RENAME, EVICT, APPEND) object counts;\n" +
			"\t\t" + argsUsageIndent + "numbers of list-objects requests;\n" +
			"\t\t" + argsUsageIndent + "(GET, PUT, etc.) cumulative and average sizes;\n" +
			"\t\t" + argsUsageIndent + "associated error counters, if any, and more.",
		ArgsUsage:    optionalTargetIDArgument,
		Flags:        showPerfFlags,
		Action:       showCountersHandler,
		BashComplete: suggestTargetNodes,
	}
	showThroughput = cli.Command{
		Name:         cmdShowThroughput,
		Usage:        "show GET and PUT throughput, associated (cumulative, average) sizes and counters",
		ArgsUsage:    optionalTargetIDArgument,
		Flags:        showPerfFlags,
		Action:       showThroughputHandler,
		BashComplete: suggestTargetNodes,
	}
	showLatency = cli.Command{
		Name:         cmdShowLatency,
		Usage:        "show GET, PUT, and APPEND latencies and average sizes",
		ArgsUsage:    optionalTargetIDArgument,
		Flags:        showPerfFlags,
		Action:       showLatencyHandler,
		BashComplete: suggestTargetNodes,
	}
	showSysCap = cli.Command{
		Name:         cmdCapacity,
		Usage:        "TBD",
		ArgsUsage:    optionalTargetIDArgument,
		Flags:        showPerfFlags,
		Action:       showSysCapHandler,
		BashComplete: suggestTargetNodes,
	}
)

func showPerfHandler(c *cli.Context) error {
	allPerfTabs = true // global (TODO: consider passing as param)

	if c.NArg() > 1 && strings.HasPrefix(c.Args().Get(1), "-") {
		return fmt.Errorf("misplaced flags in %v (hint: change the order of arguments or %s specific view)",
			c.Args(), tabtab)
	}

	if err := showCountersHandler(c); err != nil {
		return err
	}
	fmt.Fprintln(c.App.Writer)

	if err := showThroughputHandler(c); err != nil {
		return err
	}
	fmt.Fprintln(c.App.Writer)

	if err := showLatencyHandler(c); err != nil {
		return err
	}
	fmt.Fprintln(c.App.Writer)

	return nil
}

func perfCptn(c *cli.Context, tab string) {
	if allPerfTabs {
		stamp := cos.FormatNowStamp()
		repeat := 40 - len(stamp) - len(tab)
		actionCptn(c, tab, " "+strings.Repeat("-", repeat)+" "+stamp)
	}
}

// show all non-zero counters _and_ sizes (unless `allColumnsFlag`)
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
	return showPerfTab(c, selected, nil, cmdShowCounters, nil, false)
}

func showThroughputHandler(c *cli.Context) error {
	var (
		totals       = make(map[string]int64, 4) // throughput metrics ("columns") to tally up
		metrics, err = getMetricNames(c)
	)
	if err != nil {
		return err
	}
	selected := make(cos.StrKVs, len(metrics))
	for name, kind := range metrics {
		switch {
		case kind == stats.KindThroughput:
			// 1. all throughput
			selected[name] = kind
			totals[name] = 0
		case name == stats.GetSize || name == stats.GetColdSize || name == stats.PutSize ||
			name == stats.GetCount || name == stats.GetColdCount || name == stats.PutCount:
			// 2. to show average get/put sizes
			selected[name] = kind
		case stats.IsErrMetric(name):
			// 3. errors
			if strings.Contains(name, "get") || strings.Contains(name, "put") ||
				strings.Contains(name, "read") || strings.Contains(name, "write") {
				selected[name] = kind
			}
		}
	}
	// `true` to show average get/put sizes
	return showPerfTab(c, selected, _throughput /*cb*/, cmdShowThroughput, totals, true)
}

// update mapBegin <= (size/s)
func _throughput(c *cli.Context, metrics cos.StrKVs, mapBegin, mapEnd teb.StstMap, elapsed time.Duration) (idle bool) {
	var (
		seconds = cos.MaxI64(int64(elapsed.Seconds()), 1) // averaging per second
		num     int
	)
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
			if v.Value != 0 {
				num++
			}
		}
	}
	idle = num == 0
	return
}

// NOTE: two built-in assumptions: one cosmetic, another major
// - ".ns" => ".n" correspondence is the cosmetic one
// - the naive way to recompute latency using the total elapsed, not the actual, time to execute so many requests...
func showLatencyHandler(c *cli.Context) error {
	metrics, err := getMetricNames(c)
	if err != nil {
		return err
	}
	selected := make(cos.StrKVs, len(latabNames))
	for name, kind := range metrics {
		if cos.StringInSlice(name, latabNames) {
			selected[name] = kind
		} else if stats.IsErrMetric(name) {
			if strings.Contains(name, "get") || strings.Contains(name, "put") || strings.Contains(name, "append") {
				selected[name] = kind
			}
		}
	}
	// `true` to show (and put request latency numbers in perspective)
	return showPerfTab(c, selected, _latency, cmdShowLatency, nil, true)
}

// update mapBegin <= (elapsed/num-samples)
func _latency(c *cli.Context, metrics cos.StrKVs, mapBegin, mapEnd teb.StstMap, _ time.Duration) (idle bool) {
	var num int // num computed latencies
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
			vend := end.Tracker[name]
			ncounter := name[:len(name)-1] // ".ns" => ".n"
			switch name {
			case stats.GetLatency:
				ncounter = stats.GetCount
			case stats.PutLatency:
				ncounter = stats.PutCount
			case stats.AppendLatency:
				ncounter = stats.AppendCount
			}
			if cntBegin, ok1 := begin.Tracker[ncounter]; ok1 {
				if cntEnd, ok2 := end.Tracker[ncounter]; ok2 && cntEnd.Value > cntBegin.Value {
					// (cumulative-end-time - cumulative-begin-time) / num-requests
					v.Value = (vend.Value - v.Value) / (cntEnd.Value - cntBegin.Value)
					begin.Tracker[name] = v
					num++
					continue
				}
			}
			v.Value = 0
			begin.Tracker[name] = v
		}
	}
	idle = num == 0
	return
}

// (main method)
func showPerfTab(c *cli.Context, metrics cos.StrKVs, cb perfcb, tag string, totals map[string]int64, inclAvgSize bool) error {
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
	avgSize := flagIsSet(c, averageSizeFlag)
	if inclAvgSize {
		avgSize = true // caller override
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

	// (1) no recompute, no totals; "long-run" (if spec-ed) via app.go
	if cb == nil {
		lfooter := 72
		if allPerfTabs {
			lfooter = 0
		}
		setLongRunParams(c, lfooter)

		ctx := teb.PerfTabCtx{Smap: smap, Sid: sid, Metrics: metrics, Regex: regex, Units: units,
			Totals:  totals,
			AllCols: allCols, AvgSize: avgSize}
		table, num, err := teb.NewPerformanceTab(tstatusMap, &ctx)
		if err != nil {
			return err
		}

		perfCptn(c, tag)
		if num == 0 && tag == cmdShowCounters {
			if regex == nil {
				actionNote(c, "the cluster is completely idle: all collected counters have zero values\n")
			} else {
				actionNote(c, fmt.Sprintf("%q matching counters have zero values\n", regexStr))
			}
		}

		out := table.Template(hideHeader)
		return teb.Print(tstatusMap, out)
	}

	// (2) locally controlled "long-run" with `cb` recompute at each cycle
	var (
		refresh = flagIsSet(c, refreshFlag)
		sleep   = _refreshRate(c)
		longRun = &longRun{}
		ini     teb.StstMap
	)
	if sleep < time.Second || sleep > time.Minute {
		return fmt.Errorf("invalid %s value, got %v, expecting [1s - 1m]", qflprn(refreshFlag), sleep)
	}

	longRun.init(c, true /*run once unless*/)
	for countdown := longRun.count; countdown > 0 || longRun.isForever(); countdown-- {
		for name := range totals { // reset
			totals[name] = 0
		}

		mapBegin, mapEnd, err := _cluStatusBeginEnd(c, ini, sleep)
		if err != nil {
			return err
		}

		idle := cb(c, metrics, mapBegin, mapEnd, sleep) // call back to recompute
		if !idle {
			perfCptn(c, tag)
		} else {
			s := "cluster"
			if sid != "" {
				s = "target"
			}
			switch tag {
			case cmdShowThroughput:
				if regex == nil {
					actionNote(c, "the "+s+" is idle throughput-wise (all throughput metrics have zero values)\n")
				} else {
					actionNote(c, fmt.Sprintf("%q matching throughput metrics have zero values\n", regexStr))
				}
			case cmdShowLatency:
				if regex == nil {
					actionNote(c, "the "+s+" is idle latency-wise (all GET, PUT, APPEND latency metrics have zero values)\n")
				} else {
					actionNote(c, fmt.Sprintf("%q matching latency metrics have zero values\n", regexStr))
				}
			}
		}

		runtime.Gosched()

		// tally up recomputed
		if totals != nil {
			for _, begin := range mapBegin {
				for name, v := range begin.Tracker {
					if _, ok := totals[name]; ok {
						totals[name] += v.Value
					}
				}
			}
		}

		ctx := teb.PerfTabCtx{Smap: smap, Sid: sid, Metrics: metrics, Regex: regex, Units: units,
			Totals:  totals,
			AllCols: allCols, AvgSize: avgSize}
		table, _, err := teb.NewPerformanceTab(mapBegin, &ctx)
		if err != nil {
			return err
		}

		out := table.Template(hideHeader)
		err = teb.Print(mapBegin, out)
		if err != nil || !refresh || allPerfTabs {
			return err
		}
		printLongRunFooter(c.App.Writer, 36)

		ini = mapEnd
	}
	return nil
}

// TODO -- FIXME: work in progress from here on ---------------

func showSysCapHandler(c *cli.Context) error {
	_, _, err := argNode(c)
	return err
}
