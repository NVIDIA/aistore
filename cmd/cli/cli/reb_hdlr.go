// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021-2024, NVIDIA CORPORATION. All rights reserved.
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
	showRebHdr = "REB ID\t NODE\t OBJECTS RECV\t SIZE RECV\t OBJECTS SENT\t SIZE SENT\t START\t END\t STATE"
)

type targetRebSnap struct {
	tid  string
	snap *core.Snap
}

var (
	showRebFlags = append(longRunFlags, allJobsFlag, noHeaderFlag, unitsFlag, dateTimeFlag)

	showCmdRebalance = cli.Command{
		Name:      cmdRebalance,
		Usage:     "show rebalance status and stats",
		ArgsUsage: jobShowRebalanceArgument,
		Flags:     showRebFlags,
		Action:    showRebalanceHandler,
	}
)

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

		var (
			numMigratedObjs   int64 // acknowledged migrations
			sizeMigratedBytes int64
			prevID            string
			allSnaps          = make([]*targetRebSnap, 0, 32)
		)
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

		if len(allSnaps) > 0 {
			sort.Slice(allSnaps, func(i, j int) bool {
				if allSnaps[i].snap.ID != allSnaps[j].snap.ID {
					return allSnaps[i].snap.ID > allSnaps[j].snap.ID
				}
				return allSnaps[i].tid < allSnaps[j].tid
			})

			if printed {
				fmt.Fprintln(tw)
			}
			if !hideHeader {
				// NOTE: when/if changing header update `colCount` & `displayRebStats` as well
				fmt.Fprintln(tw, showRebHdr)
			}
			for _, sts := range allSnaps {
				if flagIsSet(c, allJobsFlag) {
					if prevID != "" && sts.snap.ID != prevID {
						fmt.Fprintln(tw, strings.Repeat("\t ", 9 /*colCount*/))
						numMigratedObjs, sizeMigratedBytes = 0, 0
					}
					displayRebStats(tw, sts, units, datedTime)
				} else {
					if prevID != "" && sts.snap.ID != prevID {
						break
					}
					latestAborted = latestAborted || sts.snap.AbortedX
					latestFinished = latestFinished || !sts.snap.EndTime.IsZero()
					displayRebStats(tw, sts, units, datedTime)
				}
				numMigratedObjs += sts.snap.Stats.Objs
				sizeMigratedBytes += sts.snap.Stats.Bytes
				prevID = sts.snap.ID
			}
			tw.Flush()
			printed = true
		}

		id := fcyan(prevID)
		if numMigratedObjs > 0 {
			fmt.Fprintf(c.App.Writer, "%s: %d objects migrated (total size %s)\n",
				id, numMigratedObjs, teb.FmtSize(sizeMigratedBytes, units, 1))
		}
		if !flagIsSet(c, allJobsFlag) {
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

func displayRebStats(tw *tabwriter.Writer, st *targetRebSnap, units string, datedTime bool) {
	var startTime, endTime string
	if datedTime {
		startTime = teb.FmtDateTime(st.snap.StartTime)
		endTime = teb.FmtDateTime(st.snap.EndTime)
	} else {
		startTime = teb.FmtTime(st.snap.StartTime)
		endTime = teb.FmtTime(st.snap.EndTime)
	}
	fmt.Fprintf(tw,
		"%s\t %s\t %d\t %s\t %d\t %s\t %s\t %s\t %s\n",
		st.snap.ID, st.tid,
		st.snap.Stats.InObjs, teb.FmtSize(st.snap.Stats.InBytes, units, 2),
		st.snap.Stats.OutObjs, teb.FmtSize(st.snap.Stats.OutBytes, units, 2),
		startTime, endTime, teb.FmtXactRunFinAbrt(st.snap),
	)
}
