// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains util functions and types.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/xact"

	"github.com/urfave/cli"
)

const (
	fmtXactFailed    = "Failed to %s (%q => %q)\n"
	fmtXactSucceeded = "Done.\n"

	maxDisplayJobErrs = 3 // checkXactErrs: max per-target errors to show inline (with `ais show job` for the rest)
)

type (
	// queryXactions return
	commonMultiSnap struct {
		xid     string
		bck     cmn.Bck
		aborted bool
		running bool
	}
)

// TODO -- FIXME: consolidate job-starting cases into actionX (which already encapsulates
// "Started X[xid]. To monitor..."). Would also enable --non-verbose to work uniformly
// across xstart paths, and is a natural place to add a script-friendly short-output flag.

func actionX(c *cli.Context, xargs *xact.ArgsMsg, s string) {
	if flagIsSet(c, nonverboseFlag) {
		fmt.Fprintln(c.App.Writer, xargs.ID)
		return
	}
	msg := fmt.Sprintf("Started %s%s. %s", xact.Cname(xargs.Kind, xargs.ID), s, toMonitorMsg(c, xargs.ID, ""))
	actionDone(c, msg)
}

func toMonitorMsg(c *cli.Context, xjid, suffix string) (out string) {
	out = toShowMsg(c, xjid, "To monitor the progress", false)
	if suffix != "" && out != "" {
		out = strings.TrimRight(out, "'") + " " + suffix
	}
	return
}

func toShowMsg(c *cli.Context, xjid, prompt string, verbose bool) string {
	// use command search
	cmds := findCmdMultiKeyAlt(commandShow, c.Command.Name)
	if len(cmds) == 0 || xjid != "" {
		// generic
		cmds = findCmdMultiKeyAlt(commandShow, commandJob)
	}
	for _, cmd := range cmds {
		if strings.HasPrefix(cmd, cliName+" "+commandShow+" ") {
			var sid, sv string
			if verbose {
				sv = " -v"
			}
			if xjid != "" {
				sid = " " + xjid
			}
			return fmt.Sprintf("%s, run '%s%s%s'", prompt, cmd, sid, sv)
		}
	}
	return ""
}

// Wait for the caller's started xaction to run until finished _or_ idle;
// having waited, report job errors and abort, if any (see checkXactErrs).
// Waiting logic, by kind:
//   - blob-download: no notification listener (see ais/prxclu) - poll target snaps
//   - kinds that IdlesBeforeFinishing: wait for idle via target snaps
//   - all other kinds: wait via IC (api.WaitForXactionIC)
//
// TODO:
//   - simplify: replace the latter two calls with a single api.WaitForXaction; note:
//     the latter discards IC status - kind-only (no job ID) waits lose abort reporting
//   - not covered wrt finishing-with-errors: download and dsort (own wait handlers),
//     blob-download (ditto), and the `--progress` (cpr) flows
func waitXact(args *xact.ArgsMsg) error {
	debug.Assert(args.ID == "" || xact.IsValidUUID(args.ID))

	// NOTE: relying on the Kind to decide between waiting APIs
	debug.Assert(args.Kind != "")
	kind, xname := xact.GetKindName(args.Kind)
	debug.Assert(kind != "")

	// normalize: args.Kind may be display-name
	// (for usability, CLI must support kind and display-name interchangeably)
	args.Kind = kind

	if kind == apc.ActBlobDl {
		return waitXactBlob(args)
	}

	if xact.IdlesBeforeFinishing(kind) {
		err := api.WaitForSnapsIdle(apiBP, args)
		return checkXactErrs(args, err)
	}
	// otherwise, IC
	status, err := api.WaitForXactionIC(apiBP, args)
	if err == nil && status.IsAborted() {
		return fmt.Errorf("%s aborted", xact.Cname(xname, status.UUID))
	}
	err = V(err)
	return checkXactErrs(args, err)
}

// report the way the job ended: per-target errors (first few), abort, or nothing;
// `waitErr` == not-found is ok (job finished, no longer tracked) - proceed to check snaps;
// return any other `waitErr` (e.g., timeout) as is
func checkXactErrs(xargs *xact.ArgsMsg, waitErr error) error {
	if waitErr != nil && !cmn.IsErrHTTPNotFound(waitErr) {
		return waitErr
	}
	if xargs.ID == "" {
		// cannot reliably attribute errors without the job ID
		return nil
	}
	xs, err := api.QueryXactionSnaps(apiBP, xargs)
	if err != nil {
		return V(err)
	}

	// 1. collect per-target errors
	var errs []string // nil on the common (no errors) path
	for tid, snaps := range xs {
		for _, snap := range snaps {
			if snap.ID != xargs.ID || snap.Err == "" {
				continue
			}
			e := snap.Err
			if tname := meta.Tname(tid); !strings.HasPrefix(e, tname) {
				e = tname + ": " + e
			}
			errs = append(errs, e)
		}
	}

	// 2. check for job abort or unknown job
	if len(errs) == 0 {
		aborted, _, notstarted := xs.AggregateState(xargs.ID)
		switch {
		case aborted:
			_, xname := xact.GetKindName(xargs.Kind)
			return fmt.Errorf("%s aborted", xact.Cname(xname, xargs.ID))
		case notstarted && waitErr != nil:
			return waitErr // job unknown (or not tracked anymore)
		}
		return nil
	}
	sort.Strings(errs) // (deterministic order)

	// 3. report the errors
	_, xname := xact.GetKindName(xargs.Kind)
	var sb strings.Builder
	sb.WriteString(xact.Cname(xname, xargs.ID) + " finished with errors:\n")
	num := min(len(errs), maxDisplayJobErrs)
	for i := range num {
		sb.WriteString("    " + errs[i] + "\n")
	}
	if len(errs) > num {
		sb.WriteString("    ...\n")
	}
	fmt.Fprintf(&sb, "Use '%s %s %s %s' to view all errors.", cliName, commandShow, commandJob, xargs.ID)
	return errors.New(sb.String())
}

// (x-blob doesn't do nofif listener - see ais/prxclu xstart)
func waitXactBlob(xargs *xact.ArgsMsg) error {
	var sleep = xact.MinPollTime
	for {
		time.Sleep(sleep)
		_, snap, errN := getAnyXactSnap(xargs)
		if errN != nil {
			return errN
		}
		if snap.IsAborted() {
			return errors.New(snap.AbortErr)
		}
		debug.Assert(snap.ID == xargs.ID || xargs.ID == "")
		if snap.IsFinished() {
			return nil
		}
		sleep = min(sleep+sleep/2, xact.MaxPollTime)
	}
}

func getKindNameForID(xid string, otherKind ...string) (kind, xname string, rerr error) {
	xargs := xact.ArgsMsg{ID: xid}
	status, err := api.GetOneXactionStatus(apiBP, &xargs) // via IC
	if err == nil {
		kind, xname = xact.GetKindName(status.Kind)
		return
	}
	if herr, ok := err.(*cmn.ErrHTTP); ok && herr.Status == http.StatusNotFound {
		// 2nd attempt assuming xaction in question `IdlesBeforeFinishing`
		briefPause(1)
		xs, _, err := queryXactions(&xargs, false /*summarize*/)
		if err != nil {
			rerr = err
			return
		}
		for _, snaps := range xs {
			if len(snaps) > 0 {
				debug.Assert(snaps[0].ID == xid)
				kind = snaps[0].Kind
				_, xname = xact.GetKindName(kind)
				return
			}
		}
	}
	if len(otherKind) > 0 {
		rerr = fmt.Errorf("x-%s?[%s] not found", otherKind[0], xid)
	} else {
		rerr = fmt.Errorf("x-???[%s] not found", xid)
	}
	return
}

func flattenXactStats(snap *core.Snap, units string) nvpairList {
	props := make(nvpairList, 0, 16)
	if snap == nil {
		return props
	}
	fmtTime := func(t time.Time) string {
		if t.IsZero() {
			return teb.NotSetVal
		}
		return t.Format("01-02 15:04:05")
	}
	_, xname := xact.GetKindName(snap.Kind)
	if xname != snap.Kind {
		props = append(props, nvpair{Name: ".display-name", Value: xname})
	}
	props = append(props,
		// Start xaction properties with a dot to make them first alphabetically
		nvpair{Name: ".id", Value: snap.ID},
		nvpair{Name: ".kind", Value: snap.Kind},
		nvpair{Name: ".bck", Value: snap.Bck.String()},
		nvpair{Name: ".start", Value: fmtTime(snap.StartTime)},
		nvpair{Name: ".end", Value: fmtTime(snap.EndTime)},
		nvpair{Name: ".aborted", Value: strconv.FormatBool(snap.AbortedX)},
		nvpair{Name: ".state", Value: teb.FmtXactRunFinAbrt(snap)},
	)
	if snap.Stats.Objs != 0 || snap.Stats.Bytes != 0 {
		printtedVal := teb.FmtSize(snap.Stats.Bytes, units, 2)
		props = append(props,
			nvpair{Name: "loc.obj.n", Value: strconv.FormatInt(snap.Stats.Objs, 10)},
			nvpair{Name: "loc.obj.size", Value: printtedVal},
		)
	}
	if snap.Stats.InObjs != 0 || snap.Stats.InBytes != 0 {
		printtedVal := teb.FmtSize(snap.Stats.InBytes, units, 2)
		props = append(props,
			nvpair{Name: "in.obj.n", Value: strconv.FormatInt(snap.Stats.InObjs, 10)},
			nvpair{Name: "in.obj.size", Value: printtedVal},
		)
	}
	if snap.Stats.Objs != 0 || snap.Stats.Bytes != 0 {
		printtedVal := teb.FmtSize(snap.Stats.OutBytes, units, 2)
		props = append(props,
			nvpair{Name: "out.obj.n", Value: strconv.FormatInt(snap.Stats.OutObjs, 10)},
			nvpair{Name: "out.obj.size", Value: printtedVal},
		)
	}
	// NOTE: extended stats
	if extStats, ok := snap.Ext.(map[string]any); ok {
		for k, v := range extStats {
			var value string
			if strings.HasSuffix(k, ".size") {
				val := v.(string)
				if i, err := strconv.ParseInt(val, 10, 64); err == nil {
					value = cos.IEC(i, 2)
				}
			}
			if value == "" { // not ".size"
				if mapVal, ok := v.(map[string]any); ok {
					vv, err := jsonMarshalIndent(mapVal)
					debug.AssertNoErr(err)
					value = string(vv)
				} else {
					value = fmt.Sprintf("%v", v)
				}
			}
			props = append(props, nvpair{Name: k, Value: value})
		}
	}
	sort.Slice(props, func(i, j int) bool {
		return props[i].Name < props[j].Name
	})
	return props
}

func getAnyXactSnap(xargs *xact.ArgsMsg) (string, *core.Snap, error) {
	xs, _, err := queryXactions(xargs, false)
	if err != nil {
		return "", nil, err
	}
	for tid, snaps := range xs {
		for _, snap := range snaps {
			return tid, snap, nil
		}
	}
	return "", nil, nil
}

func queryXactions(xargs *xact.ArgsMsg, summarize bool) (xs xact.MultiSnap, cms commonMultiSnap, err error) {
	orig := apiBP.Client.Timeout
	if !xargs.OnlyRunning {
		apiBP.Client.Timeout = min(orig, longClientTimeout)
		defer func(t time.Duration) {
			apiBP.Client.Timeout = t
		}(orig)
	}
	xs, err = api.QueryXactionSnaps(apiBP, xargs)
	if err != nil {
		return xs, cms, V(err)
	}
	// filter
	if xargs.DaemonID != "" {
		for tid := range xs {
			if tid != xargs.DaemonID {
				delete(xs, tid)
			}
		}
	}

	if !summarize || len(xs) == 0 {
		return xs, cms, nil
	}

	// summarize
	// in part, check whether all x-snaps share the same xid and/or bucket
	var (
		first  = true
		notID  bool // (no ID, multiple IDs)
		notBck bool // (no bucket | multiple buckets)
	)
	for _, snaps := range xs {
		for _, snap := range snaps {
			if first {
				cms.xid, cms.bck = snap.ID, snap.Bck
				cms.aborted, cms.running = snap.IsAborted(), snap.IsRunning()
				if cms.bck.IsEmpty() {
					notBck = true
					debug.Assert(xargs.Bck.IsEmpty())
				}
				if cms.xid == "" {
					notID = true
				}
				first = false
				continue
			}

			if !notID && cms.xid != snap.ID {
				cms.xid = ""
				notID = true
			}
			if !notBck && !snap.Bck.Equal(&cms.bck) {
				cms.bck = cmn.Bck{}
				notBck = true
			}
			cms.aborted = cms.aborted && snap.IsAborted()
			cms.running = cms.running || snap.IsRunning() // NOTE: also true when idle (as in: snap.IsIdle())
		}
	}

	// unlikely (see core/xaction); added for readability
	if cms.aborted && cms.running {
		debug.Assert(false)
		cms.running = false
	}

	return xs, cms, nil
}

// isRebRunning reports whether any global rebalance is currently running, cluster-wide.
// Returns the xaction ID when known; xid may be empty if multiple rebalances with
// different IDs are running concurrently (unusual, but the summarize path handles it).
// Best-effort: on query error, returns (false, "") and lets the caller proceed —
// the authoritative check belongs on the primary.
//
// Callers: 'ais start rebalance --cleanup' preflight, and (TODO) any operation that
// places new content or otherwise conflicts with a rebalance in flight.
func isRebRunning() (bool, string) {
	qargs := xact.ArgsMsg{Kind: apc.ActRebalance, OnlyRunning: true}
	_, cms, err := queryXactions(&qargs, true /*summarize*/)
	if err != nil {
		return false, ""
	}
	return cms.running, cms.xid
}

//
// xact.MultiSnap regrouping helpers
//

func extractXactKinds(xs xact.MultiSnap) []string {
	var out = make(cos.StrSet, 8)
	for _, snaps := range xs {
		for _, snap := range snaps {
			out[snap.Kind] = struct{}{}
		}
	}
	x := out.ToSlice()
	sort.Strings(x)
	return x
}

// sorted by start time
func extractXactIDsForKind(xs xact.MultiSnap, xactKind string) (xactIDs []string) {
	// prep. temp timedIDs
	timedIDs := make(map[string]time.Time, 8)
	for _, snaps := range xs {
		for _, snap := range snaps {
			if snap.Kind != xactKind {
				continue
			}
			if _, ok := timedIDs[snap.ID]; !ok {
				timedIDs[snap.ID] = snap.StartTime
				continue
			}
			// take the earliest
			if timedIDs[snap.ID].After(snap.StartTime) {
				timedIDs[snap.ID] = snap.StartTime
			}
		}
	}
	// fill and sort
	xactIDs = make([]string, 0, len(timedIDs))
	for xid := range timedIDs {
		xactIDs = append(xactIDs, xid)
	}
	if len(xactIDs) <= 1 {
		return xactIDs
	}
	sort.Slice(xactIDs, func(i, j int) bool {
		xi, xj := xactIDs[i], xactIDs[j]
		return timedIDs[xi].Before(timedIDs[xj])
	})
	return xactIDs
}

func xstart(xargs *xact.ArgsMsg, extra string) (xid string, err error) {
	return api.StartXaction(apiBP, xargs, extra)
}

func xstop(xargs *xact.ArgsMsg) (err error) {
	if xargs.Flags != 0 {
		err = errors.New("invalid 'ais stop' command - expecting zero flags")
		debug.AssertNoErr(err)
		return err
	}
	if err = api.AbortXaction(apiBP, xargs); err != nil {
		return V(err)
	}
	return nil
}
