// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
)

// low-level formatting routines and misc.

func fmtObjStatus(obj *cmn.LsoEntry) string {
	switch obj.Status() {
	case apc.LocOK:
		return "ok"
	case apc.LocMisplacedNode:
		return "misplaced(cluster)"
	case apc.LocMisplacedMountpath:
		return "misplaced(mountpath)"
	case apc.LocIsCopy:
		return "replica"
	case apc.LocIsCopyMissingObj:
		return "replica(object-is-missing)"
	default:
		debug.Assertf(false, "%#v", obj)
		return "invalid"
	}
}

func fmtObjIsCached(obj *cmn.LsoEntry) string {
	return FmtBool(obj.CheckExists())
}

// FmtBool returns "yes" if true, else "no"
func FmtBool(t bool) string {
	if t {
		return "yes"
	}
	return "no"
}

func fmtObjCustom(custom string) string {
	if custom != "" {
		return custom
	}
	return NotSetVal
}

// FmtCopies formats an int to a string, where 0 becomes "-"
func FmtCopies(copies int) string {
	if copies == 0 {
		return unknownVal
	}
	return fmt.Sprint(copies)
}

// FmtEC formats EC data (DataSlices, ParitySlices, IsECCopy) into a
// readable string for CLI, e.g. "1:2[encoded]"
func FmtEC(gen int64, data, parity int, isCopy bool) string {
	if data == 0 {
		return unknownVal
	}
	info := fmt.Sprintf("%d:%d (gen %d)", data, parity, gen)
	if isCopy {
		info += "[replicated]"
	} else {
		info += "[encoded]"
	}
	return info
}

func fmtDaemonID(id string, smap *cluster.Smap, daeStatus ...string) (snamePlus string) {
	si := smap.GetNode(id)
	snamePlus = si.StringEx()

	if len(daeStatus) > 0 {
		if daeStatus[0] != NodeOnline {
			snamePlus += specialStatusSuffix
			return
		}
	}
	if id == smap.Primary.ID() {
		snamePlus += primarySuffix
		return
	}
	if smap.NonElectable(si) {
		debug.Assert(si.IsProxy())
		snamePlus += nonElectableSuffix
		return
	}
	if si.InMaintOrDecomm() {
		snamePlus += specialStatusSuffix
	}
	return
}

func fmtSmapVer(v int64) string { return fmt.Sprintf("v%d", v) }

func fmtStringList(lst []string) string {
	if len(lst) == 0 {
		return unknownVal
	}
	return fmtStringListGeneric(lst, ",")
}

func fmtStringListGeneric(lst []string, sep string) string {
	var s strings.Builder
	for idx, url := range lst {
		if idx != 0 {
			fmt.Fprint(&s, sep)
		}
		fmt.Fprint(&s, url)
	}
	return s.String()
}

// internal helper for the methods above
func toString(lst []string) string {
	switch len(lst) {
	case 0:
		return NotSetVal
	case 1:
		return lst[0]
	default:
		return "[" + strings.Join(lst, ", ") + "]"
	}
}

func fmtACL(acl apc.AccessAttrs) string {
	if acl == 0 {
		return unknownVal
	}
	return acl.Describe()
}

func fmtNameArch(val string, flags uint16) string {
	if flags&apc.EntryInArch == 0 {
		return val
	}
	return "    " + val
}

//
// cluster.Snap helpers
//

func fmtRebStatus(rebSnap *cluster.Snap) string {
	if rebSnap == nil {
		return unknownVal
	}
	if rebSnap.IsAborted() {
		return fmt.Sprintf("aborted(%s)", rebSnap.ID)
	}
	if rebSnap.EndTime.IsZero() {
		return fmt.Sprintf("running(%s)", rebSnap.ID)
	}
	if time.Since(rebSnap.EndTime) < rebalanceExpirationTime {
		return fmt.Sprintf("finished(%s)", rebSnap.ID)
	}
	return unknownVal
}

func FmtXactStatus(snap *cluster.Snap) string {
	if snap.AbortedX {
		return xactStateAborted
	}
	if !snap.EndTime.IsZero() {
		return xactStateFinished
	}
	if snap.IsIdle() {
		return xactStateIdle
	}
	return xactStateRunning
}

func extECGetStats(base *cluster.Snap) *ec.ExtECGetStats {
	ecGet := &ec.ExtECGetStats{}
	if err := cos.MorphMarshal(base.Ext, ecGet); err != nil {
		return &ec.ExtECGetStats{}
	}
	return ecGet
}

func extECPutStats(base *cluster.Snap) *ec.ExtECPutStats {
	ecPut := &ec.ExtECPutStats{}
	if err := cos.MorphMarshal(base.Ext, ecPut); err != nil {
		return &ec.ExtECPutStats{}
	}
	return ecPut
}

//
// time and duration
//

// see also: cli.isUnsetTime and cli.fmtBucketCreatedTime
func isUnsetTime(t time.Time) bool {
	return t.IsZero()
}

func FmtStartEnd(start, end time.Time) (startS, endS string) {
	startS, endS = NotSetVal, NotSetVal
	if start.IsZero() {
		return
	}
	y1, m1, d1 := start.Date()
	f := cos.StampSec // hh:mm:ss
	if !end.IsZero() {
		y2, m2, d2 := end.Date()
		if y1 != y2 || m1 != m2 || d1 != d2 {
			f = time.Stamp // with date
		}
		endS = cos.FormatTime(end, f)
	}
	startS = cos.FormatTime(start, f)
	return
}

//
// stats.DaemonStatusMap
//

func statusMap2SortedNodes(daeMap stats.DaemonStatusMap) (ids []string) {
	ids = make([]string, 0, len(daeMap))
	for sid := range daeMap {
		ids = append(ids, sid)
	}
	sort.Strings(ids)
	return
}

func fmtCDF(cdfs map[string]*fs.CDF) string {
	fses := make([]string, 0, len(cdfs))
	for _, cdf := range cdfs {
		fses = append(fses, cdf.FS)
	}
	return fmt.Sprintf("%v", fses)
}
