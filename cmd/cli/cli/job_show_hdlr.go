// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains implementation of the top-level `show` command.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact"

	"github.com/urfave/cli"
)

var showJobUsage = "Show running and/or finished jobs:\n" +
	indent1 + "\t" + formatJobNames() +
	indent1 + "(use any of these names with 'ais show job' command, or try shortcuts: \"evict\", \"prefetch\", \"copy\", \"delete\", \"ec\")\n" +
	indent1 + "e.g.:\n" +
	indent1 + "\t- show job prefetch-listrange\t- show all running prefetch jobs;\n" +
	indent1 + "\t- show job prefetch\t- same as above;\n" +
	indent1 + "\t- show job prefetch --top 5\t- show 5 most recent prefetch jobs;\n" +
	indent1 + "\t- show job tco-cysbohAGL\t- show a given (multi-object copy/transform) job identified by its unique ID;\n" +
	indent1 + "\t- show job copy-listrange\t- show all running multi-object copies;\n" +
	indent1 + "\t- show job copy-objects\t- same as above (using display name);\n" +
	indent1 + "\t- show job copy\t- show all copying jobs including both bucket-to-bucket and multi-object;\n" +
	indent1 + "\t- show job copy-objects --all\t- show both running and already finished (or stopped) multi-object copies;\n" +
	indent1 + "\t- show job copy-objects --all --top 10\t- show 10 most recent multi-object copy jobs;\n" +
	indent1 + "\t- show job ec\t- show all erasure-coding;\n" +
	indent1 + "\t- show job list\t- show all running list-objects jobs;\n" +
	indent1 + "\t- show job ls\t- same as above;\n" +
	indent1 + "\t- show job ls --refresh 10\t- same as above with periodic _refreshing_ every 10 seconds;\n" +
	indent1 + "\t- show job ls --refresh 10 --count 4\t- same as above but only for the first four 10-seconds intervals;\n" +
	indent1 + "\t- show job prefetch --refresh 1m\t- show all running prefetch jobs at 1 minute intervals (until Ctrl-C);\n" +
	indent1 + "\t- show job evict\t- all running bucket and/or data evicting jobs;\n" +
	indent1 + "\t- show job --all\t- show absolutely all jobs, running and finished."

var showJobFlags = append(
	longRunFlags,
	jsonFlag,
	allJobsFlag,
	regexJobsFlag,
	noHeaderFlag,
	verboseJobFlag,
	unitsFlag,
	dateTimeFlag,
	topFlag,
	// download and dsort only
	progressFlag,
	dsortLogFlag,
)

var showCmdJob = cli.Command{
	Name:         commandJob,
	Usage:        showJobUsage,
	ArgsUsage:    jobAnyArg,
	Flags:        showJobFlags,
	Action:       showJobsHandler,
	BashComplete: runningJobCompletions,
}

// args [NAME] [JOB_ID] [NODE_ID] [BUCKET] may:
// - be omitted, in part or in total, and may
// - come in arbitrary order
func showJobsHandler(c *cli.Context) error {
	var (
		multimatch                    bool
		l                             int
		name, xid, daemonID, bck, err = jobArgs(c, 0, false /*ignore daemonID*/)
	)
	if err != nil {
		return err
	}
	if name == cmdRebalance {
		return showRebalanceHandler(c)
	}

	// [usability]
	// keywords: multi-match, multiple selection, shortcut, job name prefix
	if name == "" && xid != "" {
		var prefix string
		name, prefix, multimatch = xid2Name(xid)
		if multimatch && prefix != "" {
			xid = prefix // see below
		}
	}

	setLongRunParams(c, 72)

	if multimatch {
		var (
			prefix = xid
			cnt    int
		)
		allnames := xact.ListDisplayNames(false /*only-startable*/)
		for _, xname := range allnames {
			if !strings.HasPrefix(xname, prefix) { // filter
				continue
			}

			ll, errV := _showJobs(c, xname, "" /*xid*/, daemonID, bck, true)
			if errV != nil {
				actionWarn(c, errV.Error())
				err = errV
				cnt++
				if cnt > 1 {
					break
				}
			}
			l += ll
		}
	} else {
		l, err = showJobsDo(c, name, xid, daemonID, bck)
	}

	if err == nil && l == 0 && !flagIsSet(c, allJobsFlag) {
		var (
			what string
			n, h = qflprn(allJobsFlag), qflprn(cli.HelpFlag)
		)
		if name != "" {
			what = " '" + name + "'"
		}
		fmt.Fprintf(c.App.Writer, "No running%s jobs. "+
			"Use %s to show all, %s <TAB-TAB> to select, %s for details.\n", what, n, n, h)
	}
	return err
}

func showJobsDo(c *cli.Context, name, xid, daemonID string, bck cmn.Bck) (int, error) {
	if name != "" || xid != "" {
		return _showJobs(c, name, xid, daemonID, bck, true /*caption*/)
	}

	// special (best-effort)
	if xid != "" {
		switch name {
		case cmdDownload:
			return _showJobs(c, cmdDownload, xid, daemonID, bck, false /*caption, here and elsewhere*/)
		case cmdDsort:
			return _showJobs(c, cmdDsort, xid, daemonID, bck, false)
		case commandETL:
			_, otherID, _ := xid2Name(xid)
			return _showJobs(c, commandETL, otherID /*etl name*/, daemonID, bck, false)
		}
	}

	var (
		ll  int
		cnt int
		err error
	)
	names := xact.ListDisplayNames(false /*only-startable*/)
	for _, name = range names {
		l, errV := _showJobs(c, name, "" /*xid*/, daemonID, bck, true)
		if errV != nil {
			actionWarn(c, errV.Error())
			err = errV
			cnt++
			if cnt > 1 {
				break
			}
		}
		ll += l
	}
	return ll, err
}

func _jname(xname, xid string) string { return xname + "[" + xid + "]" }

func jobCptn(c *cli.Context, name, xid, ctlmsg string, onlyActive, byTarget bool) {
	var (
		s, tip string
	)
	if !flagIsSet(c, verboseJobFlag) {
		if _, dtor, err := xact.GetDescriptor(name); err == nil && dtor.ExtendedStats {
			tip = fmt.Sprintf("(tip: use %s to include extended stats)", qflprn(verboseJobFlag))
		}
	}
	if xid != "" {
		jname := _jname(name, xid)
		if ctlmsg != "" {
			actionCptn(c, jname, "(run options:", fcyan(ctlmsg)+")", tip)
		} else {
			actionCptn(c, jname, tip)
		}
		return
	}

	if byTarget {
		s = "by target"
	}
	if onlyActive {
		actionCptn(c, name, "jobs", s, tip)
	} else {
		actionCptn(c, name, "jobs", s, "including finished", tip)
	}
}

func _showJobs(c *cli.Context, name, xid, daemonID string, bck cmn.Bck, caption bool) (int, error) {
	switch name {
	case cmdDownload:
		return showDownloads(c, xid, caption)
	case commandETL:
		return showETLs(c, xid, caption)
	case cmdDsort:
		return showDsorts(c, xid, caption)
	default:
		var (
			// finished or not, always try to show when xid provided
			all         = flagIsSet(c, allJobsFlag) || xact.IsValidUUID(xid)
			onlyActive  = !all
			xactKind, _ = xact.GetKindName(name)
			regexStr    = parseStrFlag(c, regexJobsFlag)
			xargs       = xact.ArgsMsg{
				ID:          xid,
				Kind:        xactKind,
				DaemonID:    daemonID,
				Bck:         bck,
				OnlyRunning: onlyActive,
			}
		)
		if regexStr != "" {
			regex, err := regexp.Compile(regexStr)
			if err != nil {
				actionWarn(c, err.Error())
				regex = nil
			}
			if regex != nil && !regex.MatchString(name) && !regex.MatchString(xactKind) {
				return 0, nil
			}
		}
		return xactList(c, &xargs, caption)
	}
}

func showDownloads(c *cli.Context, id string, caption bool) (int, error) {
	if id == "" { // list all download jobs
		return downloadJobsList(c, parseStrFlag(c, regexJobsFlag), caption)
	}
	// display status of a download job identified by its JOB_ID
	return 1, downloadJobStatus(c, id)
}

func showDsorts(c *cli.Context, id string, caption bool) (int, error) {
	var (
		usejs      = flagIsSet(c, jsonFlag)
		onlyActive = !flagIsSet(c, allJobsFlag)
	)
	if id == "" {
		list, err := api.ListDsort(apiBP, parseStrFlag(c, regexJobsFlag), onlyActive)
		l := len(list)
		if err != nil || l == 0 {
			return l, V(err)
		}
		if caption {
			jobCptn(c, cmdDsort, id, "" /*ctlmsg*/, onlyActive, false)
		}
		return l, dsortJobsList(c, list, usejs)
	}

	return 1, dsortJobStatus(c, id)
}

//
// common
//

func xactList(c *cli.Context, xargs *xact.ArgsMsg, caption bool) (int, error) {
	// override the caller's choice if explicitly identified
	if xargs.ID != "" {
		if !xact.IsValidUUID(xargs.ID) {
			return 0, fmt.Errorf("UUID %q is invalid (typo?)", xargs.ID)
		}
		xargs.OnlyRunning = false
	}

	xs, _, err := queryXactions(xargs, false /*summarize*/)
	if err != nil {
		return 0, err
	}
	var numSnaps int
	for _, snaps := range xs {
		numSnaps += len(snaps)
	}
	if numSnaps == 0 {
		return 0, nil
	}

	var (
		ll           int
		allXactKinds = extractXactKinds(xs)
		topN         = c.Int(topFlag.Name)
	)

	// Validate --top flag if explicitly set
	if flagIsSet(c, topFlag) && topN <= 0 {
		return 0, fmt.Errorf("invalid value for %s: %d (must be positive)", qflprn(topFlag), topN)
	}

	for _, xactKind := range allXactKinds {
		xactIDs := extractXactIDsForKind(xs, xactKind)

		// Apply --top filtering if specified
		if topN > 0 && len(xactIDs) > 0 {
			xactIDs = xactListTopN(xs, xactKind, xactIDs, topN)
		}

		for _, xid := range xactIDs {
			xargs.Kind, xargs.ID = xactKind, xid
			l, err := xlistByKindID(c, xargs, caption, xs)
			if err != nil {
				actionWarn(c, err.Error())
			}
			ll += l
		}
	}
	return ll, nil
}

// xactListTopN filters xaction IDs to return only the top N most recent ones
// based on their start times. Jobs that haven't started (zero time) are excluded.
func xactListTopN(xs xact.MultiSnap, xactKind string, xactIDs []string, topN int) []string {
	type xidWithTime struct {
		xid       string
		startTime time.Time
	}

	xidsWithTime := make([]xidWithTime, 0, len(xactIDs))

	for _, xid := range xactIDs {
		// Find any start time for this xaction ID
		var (
			startTime time.Time
			found     bool
		)

		// Look for the first valid start time and use it
		for _, snaps := range xs {
			for _, snap := range snaps {
				if snap.ID == xid && snap.Kind == xactKind {
					// Skip zero times (job hasn't started)
					if !snap.StartTime.IsZero() {
						startTime = snap.StartTime
						found = true
						break
					}
				}
			}
			if found {
				break
			}
		}

		// Only include jobs that have actually started
		if found {
			xidsWithTime = append(xidsWithTime, xidWithTime{xid: xid, startTime: startTime})
		}
	}

	// Sort by start time (most recent first)
	sort.Slice(xidsWithTime, func(i, j int) bool {
		return xidsWithTime[i].startTime.After(xidsWithTime[j].startTime)
	})

	// Take only top N
	if len(xidsWithTime) > topN {
		xidsWithTime = xidsWithTime[:topN]
	}

	// Extract just the xaction IDs
	xactIDs = make([]string, len(xidsWithTime))
	for i, xwt := range xidsWithTime {
		xactIDs[i] = xwt.xid
	}

	return xactIDs
}

func xlistByKindID(c *cli.Context, xargs *xact.ArgsMsg, caption bool, xs xact.MultiSnap) (int, error) {
	type (
		nodeSnaps struct {
			DaemonID  string
			XactSnaps []*core.Snap
		}
	)

	// first, extract snaps for: xargs.ID, Kind
	filteredXs := make(xact.MultiSnap, 8)

	for tid, snaps := range xs {
		for _, snap := range snaps {
			if snap.ID != xargs.ID {
				continue
			}
			debug.Assert(snap.Kind == xargs.Kind)
			if _, ok := filteredXs[tid]; !ok {
				filteredXs[tid] = make([]*core.Snap, 0, 8)
			}
			filteredXs[tid] = append(filteredXs[tid], snap)
		}
	}

	// second, filteredXs => dts templates
	var (
		dts    = make([]nodeSnaps, 0, len(filteredXs))
		ctlmsg string

		totals core.Stats

		// (xsnap.Packed => [joggers. workers, channel-full count])
		// instead of min/max - in verbose mode add extra column to show per target info
		jwfmin = [3]int{math.MaxInt, math.MaxInt, math.MaxInt}
		jwfmax = [3]int{}

		fromToBck, haveBck bool
	)
	for tid, snaps := range filteredXs {
		if len(snaps) == 0 {
			continue
		}
		if xargs.DaemonID != "" && xargs.DaemonID != tid {
			continue
		}
		if !snaps[0].SrcBck.IsEmpty() {
			debug.Assert(!snaps[0].DstBck.IsEmpty())
			fromToBck = true
		} else if !snaps[0].Bck.IsEmpty() {
			haveBck = true
		}

		// [parallelism]
		j, w, f := snaps[0].Unpack()
		jwfmax[0] = max(jwfmax[0], j)
		jwfmax[1] = max(jwfmax[1], w)
		jwfmax[2] = max(jwfmax[2], f)
		jwfmin[0] = min(jwfmin[0], j)
		jwfmin[1] = min(jwfmin[1], w)
		jwfmin[2] = min(jwfmin[2], f)

		// a.k.a "run options"
		// try to show more but not too much
		nmsg := snaps[0].CtlMsg
		switch {
		case nmsg == "":
			// do nothing
		case ctlmsg == "":
			ctlmsg = nmsg
		case strings.HasSuffix(ctlmsg, "..."):
			// do nothing
		case strings.Contains(ctlmsg, nmsg):
			// do nothing
		default:
			ctlmsg += "; " + nmsg
		}

		dts = append(dts, nodeSnaps{DaemonID: tid, XactSnaps: snaps}) // <--- this gets ultimately displayed via static template

		// totals
		for _, xsnap := range snaps {
			totals.Objs += xsnap.Stats.Objs
			totals.Bytes += xsnap.Stats.Bytes
			totals.OutObjs += xsnap.Stats.OutObjs
			totals.OutBytes += xsnap.Stats.OutBytes
			totals.InObjs += xsnap.Stats.InObjs
			totals.InBytes += xsnap.Stats.InBytes
		}
	}

	sort.Slice(dts, func(i, j int) bool {
		return dts[i].DaemonID < dts[j].DaemonID // ascending by node id/name
	})

	_, xname := xact.GetKindName(xargs.Kind)
	if caption {
		s := _parallelism(jwfmin, jwfmax)
		if s != "" {
			ctlmsg = strings.TrimSuffix(ctlmsg, " ")
			switch ctlmsg {
			case "":
				ctlmsg = s
			default:
				ctlmsg += ", " + s
			}
		}
		jobCptn(c, xname, xargs.ID, ctlmsg, xargs.OnlyRunning, xargs.DaemonID != "")
	}

	// multiple target nodes: append totals as a single special `nodeSnap`
	if len(filteredXs) > 1 && totals.Objs > 0 {
		dts = append(dts, nodeSnaps{
			DaemonID: teb.XactColTotals,
			XactSnaps: []*core.Snap{
				{
					Stats: totals,
				},
			},
		})
	}

	l := len(dts)
	var (
		err error

		usejs       = flagIsSet(c, jsonFlag)
		hideHeader  = flagIsSet(c, noHeaderFlag)
		units, errU = parseUnitsFlag(c, unitsFlag)
		datedTime   = flagIsSet(c, dateTimeFlag)
	)
	if errU != nil {
		actionWarn(c, errU.Error())
		units = ""
	}
	opts := teb.Opts{AltMap: teb.FuncMapUnits(units, datedTime), UseJSON: usejs}
	switch xargs.Kind {
	case apc.ActECGet:
		if hideHeader {
			err = teb.Print(dts, teb.XactECGetNoHdrTmpl, opts)
		} else {
			err = teb.Print(dts, teb.XactECGetTmpl, opts)
		}
	case apc.ActECPut:
		if hideHeader {
			err = teb.Print(dts, teb.XactECPutNoHdrTmpl, opts)
		} else {
			err = teb.Print(dts, teb.XactECPutTmpl, opts)
		}
	default:
		switch {
		case fromToBck && hideHeader:
			err = teb.Print(dts, teb.XactNoHdrFromToTmpl, opts)
		case fromToBck:
			err = teb.Print(dts, teb.XactFromToTmpl, opts)
		case haveBck && hideHeader:
			err = teb.Print(dts, teb.XactNoHdrBucketTmpl, opts)
		case haveBck:
			err = teb.Print(dts, teb.XactBucketTmpl, opts)
		default:
			if hideHeader {
				err = teb.Print(dts, teb.XactNoHdrNoBucketTmpl, opts)
			} else {
				err = teb.Print(dts, teb.XactNoBucketTmpl, opts)
			}
		}
	}
	if err != nil {
		return l, err
	}
	if !flagIsSet(c, verboseJobFlag) {
		return l, nil
	}

	// show in/out stats when verbose
	for _, di := range dts {
		if len(dts[0].XactSnaps) == 0 {
			continue
		}
		var (
			props   = flattenXactStats(di.XactSnaps[0], units)
			_, name = xact.GetKindName(di.XactSnaps[0].Kind)
			err     error
		)
		debug.Assert(name != "", di.XactSnaps[0].Kind)
		actionCptn(c, meta.Tname(di.DaemonID)+":", fmt.Sprintf("%s[%s] stats", name, di.XactSnaps[0].ID))

		if hideHeader {
			err = teb.Print(props, teb.PropValTmplNoHdr, teb.Jopts(usejs))
		} else {
			err = teb.Print(props, teb.PropValTmpl, teb.Jopts(usejs))
		}
		if err != nil {
			return l, err
		}
	}
	return l, nil
}

// ctlmsg += _parallelism()
func _parallelism(jwfmin, jwfmax [3]int) (s string) {
	if jwfmax[0] == 0 && jwfmax[1] == 0 && jwfmax[2] == 0 {
		return ""
	}
	s = "parallelism:"
	if jwfmax[0] != 0 {
		if jwfmax[0] == jwfmin[0] {
			s += " j[" + strconv.Itoa(jwfmax[0]) + "]"
		} else {
			s += " j[" + strconv.Itoa(jwfmin[0]) + "," + strconv.Itoa(jwfmax[0]) + "]"
		}
	}
	if jwfmax[1] != 0 {
		if jwfmax[1] == jwfmin[1] {
			s += " w[" + strconv.Itoa(jwfmax[1]) + "]"
		} else {
			s += " w[" + strconv.Itoa(jwfmin[1]) + "," + strconv.Itoa(jwfmax[1]) + "]"
		}
	}
	if jwfmax[2] != 0 {
		if jwfmax[2] == jwfmin[2] {
			s += " chan-full[" + strconv.Itoa(jwfmax[2]) + "]"
		} else {
			s += " chan-full[" + strconv.Itoa(jwfmin[2]) + "," + strconv.Itoa(jwfmax[2]) + "]"
		}
	}
	return s
}
