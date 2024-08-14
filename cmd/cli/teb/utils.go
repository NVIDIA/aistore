// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/ext/dsort"
	"github.com/NVIDIA/aistore/fs"
)

// this file: low-level formatting routines and misc.

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
	return strconv.Itoa(copies)
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
			return strconv.Itoa(cnt)
		}
		return fmt.Sprintf("%d (all electable)", cnt)
	}
	return fmt.Sprintf("%d (%d unelectable)", cnt, smap.CountNonElectable())
}

func fmtTargetsSumm(smap *meta.Smap, numDisks int) string {
	var (
		cnt, act = len(smap.Tmap), smap.CountActiveTs()
		s        string
	)
	if numDisks > 0 {
		switch {
		case act > 1 && numDisks > 1:
			s = fmt.Sprintf(" (total disks: %d)", numDisks)
		case numDisks > 1:
			s = fmt.Sprintf(" (num disks: %d)", numDisks)
		default:
			s = " (one disk)"
		}
	}
	if cnt != act {
		return fmt.Sprintf("%d (%d inactive)%s", cnt, cnt-act, s)
	}
	return fmt.Sprintf("%d%s", cnt, s)
}

func fmtCapPctMAM(tcdf *fs.Tcdf, list bool) string {
	var (
		a, b, c string
		skipMin = " -    " // len(sepa) + len("min%,")
		sepa    = "  "
	)
	// list vs table
	if list {
		a, b, c, sepa = "min=", "avg=", "max=", ","
		skipMin = ""
	}

	// [backward compatibility]: PctMin was added in v3.21
	// TODO: remove
	if tcdf.PctAvg > 0 && tcdf.PctMin == 0 {
		return fmt.Sprintf("%s%s%2d%%%s %s%2d%%", skipMin, b, tcdf.PctAvg, sepa, c, tcdf.PctMax)
	}

	return fmt.Sprintf("%s%2d%%%s %s%2d%%%s %s%2d%%", a, tcdf.PctMin, sepa, b, tcdf.PctAvg, sepa, c, tcdf.PctMax)
}

func fmtCDFDisks(cdf *fs.CDF) string {
	alert, _ := fs.HasAlert(cdf.Disks)
	if alert == "" {
		return cdf.FS.String() // fs.Fs + "(" + fs.FsType + ")"
	}
	return cdf.FS.Fs + fred(alert)
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

// "FormatBckName"
func fmtBckName(bck cmn.Bck) string {
	if bck.IsQuery() {
		if bck.IsEmpty() {
			return NotSetVal
		}
		return fmt.Sprintf("match[%s]:", bck.Cname(""))
	}
	return bck.Cname("")
}

func fmtACL(acl apc.AccessAttrs) string {
	if acl == 0 {
		return unknownVal
	}
	return acl.Describe(true /*incl. all*/)
}

func fmtNameDirArch(val string, flags uint16) string {
	if flags&apc.EntryInArch == 0 {
		if flags&apc.EntryIsDir != 0 {
			if !cos.IsLastB(val, '/') {
				val += "/"
			}
			return fgreen(val)
		}
		return val
	}
	return "    " + val
}

func dsortJobInfoStatus(j *dsort.JobInfo) string {
	switch {
	case j.Aborted:
		return "Aborted"
	case j.Archived:
		return "Archived"
	case j.FinishTime.IsZero():
		return "Running"
	default:
		return "Finished"
	}
}

//
// core.Snap helpers
//

const rebalanceForgetTime = 5 * time.Minute

func fmtRebStatus(snap *core.Snap) string {
	if snap == nil {
		return unknownVal
	}
	if snap.IsAborted() {
		if snap.AbortErr == cmn.ErrXactUserAbort.Error() {
			return fmt.Sprintf("user-abort(%s)", snap.ID)
		}
		return fmt.Sprintf("%s(%s): %q", strings.ToLower(xaborted), snap.ID, snap.AbortErr)
	}
	if snap.EndTime.IsZero() {
		if snap.Err == "" {
			return fmt.Sprintf("%s(%s)", strings.ToLower(xrunning), snap.ID)
		}
		return fmt.Sprintf("%s(%s) with errors: %q", strings.ToLower(xrunning), snap.ID, snap.Err)
	}
	if time.Since(snap.EndTime) < rebalanceForgetTime {
		if snap.Err == "" {
			return fmt.Sprintf("%s(%s)", strings.ToLower(xfinished), snap.ID)
		}
		return fmt.Sprintf("%s(%s): %q", strings.ToLower(xfinishedErrs), snap.ID, snap.Err)
	}
	return unknownVal
}

func FmtXactStatus(snap *core.Snap) (s string) {
	switch {
	case snap.AbortedX:
		if snap.AbortErr == cmn.ErrXactUserAbort.Error() {
			return xaborted + " by user"
		}
		return fmt.Sprintf("%s: %q", xaborted, snap.AbortErr)
	case !snap.EndTime.IsZero():
		if snap.Err == "" {
			return xfinished
		}
		return fmt.Sprintf("%s: %q", xfinishedErrs, snap.Err)
	case snap.IsIdle():
		s = xidle
	default:
		s = xrunning
	}
	if snap.Err != "" {
		s += " with errors: \"" + snap.Err + "\""
	}
	return
}

func extECGetStats(base *core.Snap) *ec.ExtECGetStats {
	ecGet := &ec.ExtECGetStats{}
	if err := cos.MorphMarshal(base.Ext, ecGet); err != nil {
		return &ec.ExtECGetStats{}
	}
	return ecGet
}

func extECPutStats(base *core.Snap) *ec.ExtECPutStats {
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

func FmtTime(t time.Time) (s string) {
	s = NotSetVal
	if t.IsZero() {
		return
	}
	return cos.FormatTime(t, cos.StampSec)
}

func FmtDateTime(t time.Time) (s string) {
	s = NotSetVal
	if t.IsZero() {
		return
	}
	return cos.FormatTime(t, time.Stamp)
}
