// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"fmt"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ec"
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

func fmtDaemonID(id string, smap *meta.Smap, daeStatus string) (snamePlus string) {
	snamePlus, _ = fmtStatusSID(id, smap, daeStatus)
	return
}

func fmtStatusSID(id string, smap *meta.Smap, daeStatus string) (snamePlus, status string) {
	si := smap.GetNode(id)
	snamePlus, status = si.StringEx(), daeStatus

	if status == "" {
		status = FmtNodeStatus(si)
	}
	if status != NodeOnline {
		goto offline
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
offline:
	if si.InMaintOrDecomm() {
		snamePlus += offlineStatusSuffix
		if si.IsProxy() || (si.IsTarget() && si.InMaintPostReb()) {
			status = fcyan(status)
		} else {
			status = fred(status) // (please do not disconnect!)
		}
	}
	return
}

func FmtNodeStatus(node *meta.Snode) (status string) {
	status = NodeOnline
	switch {
	case node.Flags.IsSet(meta.SnodeMaint):
		status = apc.NodeMaintenance
	case node.Flags.IsSet(meta.SnodeDecomm):
		status = apc.NodeDecommission
	}
	return
}

func fmtProxiesSumm(smap *meta.Smap) string {
	cnt, act := len(smap.Pmap), smap.CountActivePs()
	if cnt != act {
		return fmt.Sprintf("%d (%d inactive)", cnt, cnt-act)
	}
	une := smap.CountNonElectable()
	if une == 0 {
		if cnt == 1 {
			return fmt.Sprintf("%d", cnt)
		}
		return fmt.Sprintf("%d (all electable)", cnt)
	}
	return fmt.Sprintf("%d (%d unelectable)", cnt, smap.CountNonElectable())
}

func fmtTargetsSumm(smap *meta.Smap) string {
	cnt, act := len(smap.Tmap), smap.CountActiveTs()
	if cnt != act {
		return fmt.Sprintf("%d (%d inactive)", cnt, cnt-act)
	}
	return fmt.Sprintf("%d", cnt)
}

func fmtSmap(smap *meta.Smap) string {
	return fmt.Sprintf("version %d, UUID %s, primary %s", smap.Version, smap.UUID, smap.Primary.StringEx())
}

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
	if time.Since(rebSnap.EndTime) < rebalanceForgetTime {
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
