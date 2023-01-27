// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file handles commands that interact with the cluster.
/*
 * Copyright (c) 2021-2023, NVIDIA CORPORATION. All rights reserved.
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
	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/xact"
	"github.com/urfave/cli"
)

const (
	showRebHdr = "REB ID\t NODE\t OBJECTS RECV\t SIZE RECV\t OBJECTS SENT\t SIZE SENT\t START\t END\t STATE"
)

var (
	showRebFlags = append(longRunFlags, allJobsFlag, noHeaderFlag)

	showCmdRebalance = cli.Command{
		Name:      subcmdRebalance,
		Usage:     "show rebalance status and stats",
		ArgsUsage: jobShowRebalanceArgument,
		Flags:     showRebFlags,
		Action:    showRebalanceHandler,
	}
)

func showRebalanceHandler(c *cli.Context) error {
	var (
		tw             = &tabwriter.Writer{}
		keepMonitoring = flagIsSet(c, refreshFlag)
		refreshRate    = calcRefreshRate(c)
		hideHeader     = flagIsSet(c, noHeaderFlag)
		xargs          = xact.ArgsMsg{Kind: apc.ActRebalance}

		latestAborted, latestFinished bool
	)
	tw.Init(c.App.Writer, 0, 8, 2, ' ', 0)

	// [REB_ID] [NODE_ID]
	if c.NArg() > 0 {
		arg := c.Args().Get(0)
		if xact.IsValidRebID(arg) {
			xargs.ID = arg
		} else if sid, _, err := getNodeIDName(c, arg); err == nil {
			xargs.DaemonID = sid
		}
		if c.NArg() > 1 {
			arg = c.Args().Get(1)
			if xact.IsValidRebID(arg) {
				xargs.ID = arg
			} else if sid, _, err := getNodeIDName(c, arg); err == nil {
				xargs.DaemonID = sid
			}
		}
	}
	// show running unless --all
	if !flagIsSet(c, allJobsFlag) {
		xargs.OnlyRunning = true
	}

	// run until rebalance is completed
	var printed bool
	for {
		rebSnaps, err := api.QueryXactionSnaps(apiBP, xargs)
		if err != nil {
			if herr, ok := err.(*cmn.ErrHTTP); ok {
				if herr.Status == http.StatusNotFound {
					fmt.Fprintln(c.App.Writer, "Rebalance is not running or hasn't started yet.")
					return nil
				}
			}
			return err
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
					displayRebStats(tw, sts)
				} else {
					if prevID != "" && sts.snap.ID != prevID {
						break
					}
					latestAborted = latestAborted || sts.snap.AbortedX
					latestFinished = latestFinished || !sts.snap.EndTime.IsZero()
					displayRebStats(tw, sts)
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
				id, numMigratedObjs, cos.B2S(sizeMigratedBytes, 1))
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
		n := qflprn(allJobsFlag)
		fmt.Fprintf(c.App.Writer, "Global rebalance is not running. Use %s to show the history, help for details.\n", n)
	}

	return nil
}

func displayRebStats(tw *tabwriter.Writer, st *targetRebSnap) {
	startTime, endTime := tmpls.FmtStartEnd(st.snap.StartTime, st.snap.EndTime)
	fmt.Fprintf(tw,
		"%s\t %s\t %d\t %s\t %d\t %s\t %s\t %s\t %s\n",
		st.snap.ID, st.tid,
		st.snap.Stats.InObjs, cos.B2S(st.snap.Stats.InBytes, 2),
		st.snap.Stats.OutObjs, cos.B2S(st.snap.Stats.OutBytes, 2),
		startTime, endTime, tmpls.FmtXactStatus(st.snap),
	)
}
