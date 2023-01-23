// Package cli provides easy-to-use commands to manage, monitor, and utilize AIS clusters.
// This file contains util functions and types.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cli

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmd/cli/tmpls"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/xact"
	"github.com/urfave/cli"
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

// Wait for xaction to run for completion, warn if aborted
func waitForXactionCompletion(apiBP api.BaseParams, args api.XactReqArgs) (err error) {
	if args.Timeout == 0 {
		args.Timeout = time.Minute // TODO: make it a flag and an argument with configurable default
	}
	status, err := api.WaitForXactionIC(apiBP, args)
	if err != nil {
		return err
	}
	if status.Aborted() {
		return fmt.Errorf("xaction %q appears to be aborted", status.UUID)
	}
	return nil
}

func flattenXactStats(snap *cluster.Snap) nvpairList {
	props := make(nvpairList, 0, 16)
	if snap == nil {
		return props
	}
	fmtTime := func(t time.Time) string {
		if t.IsZero() {
			return tmpls.NotSetVal
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
		nvpair{Name: ".aborted", Value: fmt.Sprintf("%t", snap.AbortedX)},
	)
	if snap.Stats.Objs != 0 || snap.Stats.Bytes != 0 {
		props = append(props,
			nvpair{Name: "loc.obj.n", Value: fmt.Sprintf("%d", snap.Stats.Objs)},
			nvpair{Name: "loc.obj.size", Value: formatStatHuman(".size", snap.Stats.Bytes)},
		)
	}
	if snap.Stats.InObjs != 0 || snap.Stats.InBytes != 0 {
		props = append(props,
			nvpair{Name: "in.obj.n", Value: fmt.Sprintf("%d", snap.Stats.InObjs)},
			nvpair{Name: "in.obj.size", Value: formatStatHuman(".size", snap.Stats.InBytes)},
		)
	}
	if snap.Stats.Objs != 0 || snap.Stats.Bytes != 0 {
		props = append(props,
			nvpair{Name: "out.obj.n", Value: fmt.Sprintf("%d", snap.Stats.OutObjs)},
			nvpair{Name: "out.obj.size", Value: formatStatHuman(".size", snap.Stats.OutBytes)},
		)
	}
	if extStats, ok := snap.Ext.(map[string]any); ok {
		for k, v := range extStats {
			var value string
			if strings.HasSuffix(k, ".size") {
				val := v.(string)
				if i, err := strconv.ParseInt(val, 10, 64); err == nil {
					value = cos.B2S(i, 2)
				}
			}
			if value == "" {
				value = fmt.Sprintf("%v", v)
			}
			props = append(props, nvpair{Name: k, Value: value})
		}
	}
	sort.Slice(props, func(i, j int) bool {
		return props[i].Name < props[j].Name
	})
	return props
}

func getXactSnap(xactArgs api.XactReqArgs) (*cluster.Snap, error) {
	xs, err := api.QueryXactionSnaps(apiBP, xactArgs)
	if err != nil {
		return nil, err
	}
	for _, snaps := range xs {
		for _, snap := range snaps {
			return snap, nil
		}
	}
	return nil, nil
}

func queryXactions(xactArgs api.XactReqArgs) (xs api.XactMultiSnap, err error) {
	xs, err = api.QueryXactionSnaps(apiBP, xactArgs)
	if err != nil {
		return
	}
	if xactArgs.OnlyRunning {
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

	if xactArgs.DaemonID != "" {
		var found bool
		for tid := range xs {
			if tid == xactArgs.DaemonID {
				found = true
				break
			}
		}
		if !found {
			return
		}
		// remove all other targets
		for tid := range xs {
			if tid != xactArgs.DaemonID {
				delete(xs, tid)
			}
		}
	}
	return
}

//
// api.XactMultiSnap regrouping helpers
//

func extractXactKinds(xs api.XactMultiSnap) []string {
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
func extractXactIDsForKind(xs api.XactMultiSnap, xactKind string) (xactIDs []string) {
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
