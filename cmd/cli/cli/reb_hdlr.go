// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
/*
 * Copyright (c) 2021-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"net/http"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/xact"

	"github.com/urfave/cli"
)

const (
	// migration mode: TX/RX split
	showRebHdr = "REB ID\t NODE\t OBJECTS RECV\t SIZE RECV\t OBJECTS SENT\t SIZE SENT\t START\t END\t STATE"

	// cleanup mode: removed = Objs/Bytes (see reb/cleanup.go)
	showRebCleanupHdr = "REB ID\t NODE\t REMOVED OBJECTS\t REMOVED SIZE\t START\t END\t STATE"

	rebColCount        = 9 // showRebHdr column count (\t-separated)
	rebCleanupColCount = 7 // showRebCleanupHdr column count
)

var showRebUsage = "Show global rebalance status and per-target stats.\n" +
	indent1 + "e.g.:\n" +
	indent1 + "\t- show rebalance\t- show the most recent rebalance (running or finished);\n" +
	indent1 + "\t- show rebalance g21\t- show a specific rebalance by ID;\n" +
	indent1 + "\t- show rebalance --refresh 10\t- continuous monitoring with 10s refresh (until Ctrl-C);\n" +
	indent1 + "\t- show rebalance --refresh 10 --count 5\t- same as above, limited to 5 iterations;\n" +
	indent1 + "\t- show rebalance --all\t- include all rebalances: running, finished, and aborted\n" +
	indent1 + "\t\t  (note: 'rebalance --cleanup' generations render with a cleanup-mode template).\n" +
	indent1 + "for alternative views, see also:\n" +
	indent1 + "\t- 'ais show job'\t- all running jobs (possibly including rebalance) across the cluster;\n" +
	indent1 + "\t- 'ais performance intra-data'\t- live peer-to-peer RX/TX counters and sizes (rebalance generates intra-cluster traffic)."

type targetRebSnap struct {
	tid  string
	snap *core.Snap
}

var (
	showRebFlags = append(longRunFlags, allJobsFlag, noHeaderFlag, unitsFlag, dateTimeFlag)

	showCmdRebalance = cli.Command{
		Name:      cmdRebalance,
		Usage:     showRebUsage,
		ArgsUsage: jobShowRebalanceArgument,
		Flags:     sortFlags(showRebFlags),
		Action:    showRebalanceHandler,
	}
)

// TODO -- FIXME:
// add Flags field to core.Snap (requires msgpack regen)
// and replace with `snap.Flags & xact.FlagRemoveMisplaced`
func _isRebCleanup(snap *core.Snap) bool {
	return snap.Kind == apc.ActRebalance && strings.Contains(snap.CtlMsg, ":cleanup")
}

func isRebCleanupGroup(group []*targetRebSnap) bool {
	for _, sts := range group {
		if _isRebCleanup(sts.snap) {
			return true
		}
	}
	return false
}

// (implemented over Go text/tabwriter directly w/ no templates)
func showRebalanceHandler(c *cli.Context) error {
	var (
		latestAborted, latestFinished bool

		tw             = &tabwriter.Writer{}
		keepMonitoring = flagIsSet(c, refreshFlag)
		refreshRate    = _refreshRate(c)
		hideHeader     = flagIsSet(c, noHeaderFlag)
		xargs          = xact.ArgsMsg{Kind: apc.ActRebalance}
		datedTime      = flagIsSet(c, dateTimeFlag)
		units, errU    = parseUnitsFlag(c, unitsFlag)
	)
	if errU != nil {
		units = ""
	}
	tw.Init(c.App.Writer, 0, 8, 2, ' ', 0)

	// [REB_ID] [NODE_ID]
	if c.NArg() > 0 {
		arg := c.Args().Get(0)
		if xact.IsValidRebID(arg) {
			xargs.ID = arg
		} else if node, _, err := getNode(c, arg); err == nil {
			xargs.DaemonID = node.ID()
		}
		if c.NArg() > 1 {
			arg = c.Args().Get(1)
			if xact.IsValidRebID(arg) {
				xargs.ID = arg
			} else if node, _, err := getNode(c, arg); err == nil {
				xargs.DaemonID = node.ID()
			}
		}
	}
	// show running unless --all
	if !flagIsSet(c, allJobsFlag) {
		xargs.OnlyRunning = true
	}

	// run until rebalance completes
	var (
		longRun = &longRun{}
		printed bool
	)
	longRun.init(c, true /*run once unless*/)
	for countdown := longRun.count; countdown > 0 || longRun.isForever(); countdown-- {
		rebSnaps, err := api.QueryXactionSnaps(apiBP, &xargs)
		if err != nil {
			if herr, ok := err.(*cmn.ErrHTTP); ok {
				if herr.Status == http.StatusNotFound {
					fmt.Fprintln(c.App.Writer, "Rebalance is not running or hasn't started yet.")
					return nil
				}
			}
			return V(err)
		}

		allSnaps := make([]*targetRebSnap, 0, 32)
		for daemonID, daemonStats := range rebSnaps {
			if xargs.DaemonID != "" && xargs.DaemonID != daemonID {
				continue
			}
			for _, sts := range daemonStats {
				allSnaps = append(allSnaps, &targetRebSnap{
					tid:  daemonID,
					snap: sts,
				})
			}
		}

		var lastID string
		if len(allSnaps) > 0 {
			sort.Slice(allSnaps, func(i, j int) bool {
				if allSnaps[i].snap.ID != allSnaps[j].snap.ID {
					return allSnaps[i].snap.ID < allSnaps[j].snap.ID
				}
				return allSnaps[i].tid < allSnaps[j].tid
			})

			if printed {
				fmt.Fprintln(tw)
			}

			latestAborted, latestFinished, printed, lastID = renderSnaps(c, tw, allSnaps, units, datedTime, hideHeader)
		}

		if !flagIsSet(c, allJobsFlag) {
			id := fcyan(lastID)
			if latestFinished && latestAborted {
				fmt.Fprintf(c.App.Writer, "\nRebalance %s aborted.\n", id)
				break
			} else if latestFinished {
				fmt.Fprintf(c.App.Writer, "\nRebalance %s completed.\n", id)
				break
			}
		}

		if !keepMonitoring {
			break
		}
		printLongRunFooter(c.App.Writer, 72)
		time.Sleep(refreshRate)
	}

	if !printed && !flagIsSet(c, allJobsFlag) {
		n, h := qflprn(allJobsFlag), qflprn(cli.HelpFlag)
		fmt.Fprintf(c.App.Writer, "Global rebalance is not running. Use %s to show history, %s for details.\n", n, h)
	}

	return nil
}

// walk sorted snaps, grouping by rebalance ID. Each ID-group renders
// with its own header (migration vs cleanup mode), body, and per-group totals
// footer; return aborted/finished flags and ID for the selected latest generation
// when not showing all; with --all these return values are ignored by caller.
func renderSnaps(c *cli.Context, tw *tabwriter.Writer, allSnaps []*targetRebSnap,
	units string, datedTime, hideHeader bool) (latestAborted, latestFinished, printed bool, lastID string) {
	showAll := flagIsSet(c, allJobsFlag)

	// allSnaps is sorted newest-ID first above; groupByRebID preserves that order.
	groups := groupByRebID(allSnaps)
	if !showAll && len(groups) > 0 {
		groups = groups[len(groups)-1:] // only the most-recent generation; preserve old first-ID behavior
	}

	for i, group := range groups {
		cleanup := isRebCleanupGroup(group)
		renderGroup(c, tw, group, units, datedTime, hideHeader, cleanup)

		// generation transition separator (between groups only)
		if showAll && i < len(groups)-1 {
			fmt.Fprintln(tw, strings.Repeat("\t ", colCountFor(cleanup)))
		}

		latestAborted, latestFinished = false, false
		for _, sts := range group {
			latestAborted = latestAborted || sts.snap.AbortedX
			latestFinished = latestFinished || !sts.snap.EndTime.IsZero()
		}
		lastID = group[0].snap.ID
	}

	tw.Flush()
	return latestAborted, latestFinished, len(groups) > 0, lastID
}

// groupByRebID partitions a slice of snaps (sorted by ID then tid) into per-ID
// groups, preserving order. Returns nil for empty input.
func groupByRebID(allSnaps []*targetRebSnap) [][]*targetRebSnap {
	if len(allSnaps) == 0 {
		return nil
	}
	groups := make([][]*targetRebSnap, 0, 4)
	start := 0
	for i := 1; i <= len(allSnaps); i++ {
		if i == len(allSnaps) || allSnaps[i].snap.ID != allSnaps[start].snap.ID {
			groups = append(groups, allSnaps[start:i])
			start = i
		}
	}
	return groups
}

// print header + body + per-group totals footer for one rebalance generation
func renderGroup(c *cli.Context, tw *tabwriter.Writer, snaps []*targetRebSnap, units string, datedTime, hideHeader, cleanup bool) {
	// caption: per-target ctlMsg lines (deduped) above the table
	tw.Flush()
	printGroupCtlMsgs(c, snaps)

	if !hideHeader {
		if cleanup {
			fmt.Fprintln(tw, showRebCleanupHdr)
		} else {
			fmt.Fprintln(tw, showRebHdr)
		}
	}

	var totalObjs, totalBytes int64
	for _, sts := range snaps {
		if cleanup {
			displayCleanupStats(tw, sts, units, datedTime)
			// cleanup: locally-processed counters (ObjsAdd => Stats.Objs/Bytes)
			totalObjs += sts.snap.Stats.Objs
			totalBytes += sts.snap.Stats.Bytes
		} else {
			displayMigrationStats(tw, sts, units, datedTime)
			totalObjs += sts.snap.Stats.OutObjs
			totalBytes += sts.snap.Stats.OutBytes
		}
	}

	tw.Flush()
	if totalObjs == 0 || len(snaps) == 0 {
		return
	}
	id := fcyan(snaps[0].snap.ID)
	verb := "migrated"
	if cleanup {
		verb = "removed"
	}
	fmt.Fprintf(c.App.Writer, "%s: %d objects %s (total size %s)\n",
		id, totalObjs, verb, teb.FmtSize(totalBytes, units, 1))
}

// printGroupCtlMsgs prints deduped, deterministically-ordered ctlMsg lines
// above the per-generation table. Mirrors the caption block in `ais show job`
// (see job_show_hdlr.go: seenCtl/ctlmsgs collection, jobCptn rendering).
//
// For regular rebalance (migration) mode, ctlMsg carries traversal phase timings:
// <fin> trav:6s post-trav:2s fin:28s fin-streams:12s
// For cleanup mode, it carries visits/loads/removed counters:
// t[PAYt8083]:cleanup done visits=257259 loads=86170 removed=86170
func printGroupCtlMsgs(c *cli.Context, snaps []*targetRebSnap) {
	if len(snaps) == 0 {
		return
	}
	var (
		seen = make(map[string]struct{}, len(snaps))
		msgs = make([]string, 0, len(snaps))
	)
	for _, sts := range snaps {
		m := sts.snap.CtlMsg
		if m == "" {
			continue
		}
		if _, ok := seen[m]; ok {
			continue
		}
		seen[m] = struct{}{}
		msgs = append(msgs, m)
	}
	if len(msgs) == 0 {
		return
	}
	sort.Strings(msgs) // deterministic regardless of map iteration order
	id := fcyan(snaps[0].snap.ID)
	fmt.Fprintf(c.App.Writer, "%s ctl:\n", id)
	for _, m := range msgs {
		fmt.Fprintf(c.App.Writer, "  %s\n", m)
	}
}

func colCountFor(cleanup bool) int {
	if cleanup {
		return rebCleanupColCount
	}
	return rebColCount
}

func displayMigrationStats(tw *tabwriter.Writer, st *targetRebSnap, units string, datedTime bool) {
	startTime, endTime := fmtRebTimes(st.snap, datedTime)
	fmt.Fprintf(tw,
		"%s\t %s\t %d\t %s\t %d\t %s\t %s\t %s\t %s\n",
		st.snap.ID, st.tid,
		st.snap.Stats.InObjs, teb.FmtSize(st.snap.Stats.InBytes, units, 2),
		st.snap.Stats.OutObjs, teb.FmtSize(st.snap.Stats.OutBytes, units, 2),
		startTime, endTime, teb.FmtXactRunFinAbrt(st.snap),
	)
}

func displayCleanupStats(tw *tabwriter.Writer, st *targetRebSnap, units string, datedTime bool) {
	startTime, endTime := fmtRebTimes(st.snap, datedTime)
	fmt.Fprintf(tw,
		"%s\t %s\t %d\t %s\t %s\t %s\t %s\n",
		st.snap.ID, st.tid,
		st.snap.Stats.Objs, teb.FmtSize(st.snap.Stats.Bytes, units, 2),
		startTime, endTime, teb.FmtXactRunFinAbrt(st.snap),
	)
}

func fmtRebTimes(snap *core.Snap, datedTime bool) (startTime, endTime string) {
	if datedTime {
		startTime = teb.FmtDateTime(snap.StartTime)
		endTime = teb.FmtDateTime(snap.EndTime)
	} else {
		startTime = teb.FmtTime(snap.StartTime)
		endTime = teb.FmtTime(snap.EndTime)
	}
	return
}
