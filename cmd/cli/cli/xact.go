// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains util functions and types.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cmd/cli/teb"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/xact"
	"github.com/urfave/cli"
)

const (
	fmtXactFailed    = "Failed to %s (%q => %q)\n"
	fmtXactSucceeded = "Done.\n"
)

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
	if len(cmds) == 0 {
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

// Wait for the caller's started xaction to run until finished _or_ idle (NOTE),
// warn if aborted
func waitXact(apiBP api.BaseParams, args *xact.ArgsMsg) error {
	debug.Assert(args.ID == "" || xact.IsValidUUID(args.ID))
	kind, xname := xact.GetKindName(args.Kind)
	debug.Assert(kind != "") // relying on it to decide between APIs
	if xact.IdlesBeforeFinishing(kind) {
		return api.WaitForXactionIdle(apiBP, args)
	}
	// otherwise, IC
	status, err := api.WaitForXactionIC(apiBP, args)
	if err != nil {
		return V(err)
	}
	if status.Aborted() {
		return fmt.Errorf("%s[%s] aborted", xname, status.UUID)
	}
	return nil
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
		xs, err := queryXactions(&xargs)
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
		nvpair{Name: ".state", Value: teb.FmtXactStatus(snap)},
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
					value = cos.ToSizeIEC(i, 2)
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

func getXactSnap(xargs *xact.ArgsMsg) (*core.Snap, error) {
	xs, err := api.QueryXactionSnaps(apiBP, xargs)
	if err != nil {
		return nil, V(err)
	}
	for _, snaps := range xs {
		for _, snap := range snaps {
			return snap, nil
		}
	}
	return nil, nil
}

func queryXactions(xargs *xact.ArgsMsg) (xs xact.MultiSnap, err error) {
	orig := apiBP.Client.Timeout
	if !xargs.OnlyRunning {
		apiBP.Client.Timeout = min(orig, longClientTimeout)
		defer func(t time.Duration) {
			apiBP.Client.Timeout = t
		}(orig)
	}
	xs, err = api.QueryXactionSnaps(apiBP, xargs)
	if err != nil {
		return
	}
	if xargs.OnlyRunning {
		for tid, snaps := range xs {
			if len(snaps) == 0 {
				continue
			}
			runningStats := xs[tid][:0]
			for _, xctn := range snaps {
				if xctn.Running() {
					runningStats = append(runningStats, xctn)
				}
			}
			xs[tid] = runningStats
		}
	}

	if xargs.DaemonID != "" {
		var found bool
		for tid := range xs {
			if tid == xargs.DaemonID {
				found = true
				break
			}
		}
		if !found {
			return
		}
		// remove all other targets
		for tid := range xs {
			if tid != xargs.DaemonID {
				delete(xs, tid)
			}
		}
	}
	return
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
		return
	}
	sort.Slice(xactIDs, func(i, j int) bool {
		xi, xj := xactIDs[i], xactIDs[j]
		return timedIDs[xi].Before(timedIDs[xj])
	})
	return
}
