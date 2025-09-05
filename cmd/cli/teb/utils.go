// Package teb contains templates and (templated) tables to format CLI output.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package teb

import (
	"fmt"
	"strconv"
	"strings"
	"text/template"
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
	"github.com/NVIDIA/aistore/stats"
)

//
// low-level formatting routines and misc. helpers
//

type (
	// Used to return specific fields/objects for marshaling (MarshalIdent).
	forMarshaler interface {
		forMarshal() any
	}
	DiskStatsHelper struct {
		TargetID string
		DiskName string
		Stat     cos.DiskStats
		Tcdf     *fs.Tcdf
	}
	SmapHelper struct {
		Smap         *meta.Smap
		ExtendedURLs bool
	}
	StatsAndStatusHelper struct {
		Pmap StstMap
		Tmap StstMap
	}
	StatusHelper struct {
		Smap      *meta.Smap
		CluConfig *cmn.ClusterConfig
		Stst      StatsAndStatusHelper
		Capacity  string
		Version   string // when all equal
		BuildTime string // ditto
		NumDisks  int
		Backend   string // configured backend providers
		Endpoint  string // cluster endpoint URL
	}
	ListBucketsHelper struct {
		XactID string
		Bck    cmn.Bck
		Props  *cmn.Bprops
		Info   *cmn.BsummResult
	}
)

var (
	// for extensions and override, see also:
	// - FuncMapUnits
	// - HelpTemplateFuncMap
	// - `altMap template.FuncMap` below
	funcMap = template.FuncMap{
		// formatting
		"FormatBytesSig":       func(size int64, digits int) string { return FmtSize(size, cos.UnitsIEC, digits) },
		"FormatBytesSig2":      fmtSize2,
		"FormatBytesUns":       func(size uint64, digits int) string { return FmtSize(int64(size), cos.UnitsIEC, digits) },
		"FormatMAM":            func(u int64) string { return fmt.Sprintf("%-10s", FmtSize(u, cos.UnitsIEC, 2)) },
		"FormatMilli":          func(dur cos.Duration) string { return fmtMilli(dur, cos.UnitsIEC) },
		"FormatDuration":       FormatDuration,
		"FormatStart":          FmtTime,
		"FormatEnd":            FmtTime,
		"FormatDsortStatus":    dsortJobInfoStatus,
		"FormatLsObjStatus":    fmtLsObjStatus,
		"FormatLsObjIsCached":  fmtLsObjIsCached,
		"FormatObjCustom":      fmtObjCustom,
		"FormatDaemonID":       fmtDaemonID,
		"FormatSmap":           fmtSmap,
		"FormatCluSoft":        fmtCluSoft,
		"FormatRebalance":      fmtRebalance,
		"FormatProxiesSumm":    fmtProxiesSumm,
		"FormatTargetsSumm":    fmtTargetsSumm,
		"FormatCapPctMAM":      fmtCapPctMAM,
		"FormatCDFDisks":       fmtCDFDisks,
		"FormatFloat":          func(f float64) string { return fmt.Sprintf("%.2f", f) },
		"FormatBool":           FmtBool,
		"FormatBckName":        fmtBckName,
		"FormatACL":            fmtACL,
		"FormatEntryNameDAC":   fmtEntryNameDAC,
		"FormatIsChunked":      fmtIsChunked,
		"FormatXactRunFinAbrt": FmtXactRunFinAbrt,
		//  misc. helpers
		"IsUnsetTime":      isUnsetTime,
		"IsEqS":            func(a, b string) bool { return a == b },
		"IsTotals":         func(a string) bool { return a == XactColTotals },
		"FancyTotalsCheck": func() string { return fblue(" ✓") },
		"IsFalse":          func(v bool) bool { return !v },
		"JoinList":         fmtStringList,
		"JoinListNL":       func(lst []string) string { return fmtStringListGeneric(lst, "\n") },
		"ExtECGetStats":    extECGetStats,
		"ExtECPutStats":    extECPutStats,
		// StatsAndStatusHelper:
		// select specific field and make a slice, and then a string out of it
		"OnlineStatus": func(h StatsAndStatusHelper) string { return toString(h.onlineStatus()) },
		"Deployments":  func(h StatsAndStatusHelper) string { return toString(h.deployments()) },
		"Versions":     func(h StatsAndStatusHelper) string { return toString(h.versions()) },
		"BuildTimes":   func(h StatsAndStatusHelper) string { return toString(h.buildTimes()) },
	}

	AliasTemplate = "ALIAS\tCOMMAND\n" +
		"=====\t=======\n" +
		"{{range $alias := .}}" +
		"{{ $alias.Name }}\t{{ $alias.Value }}\n" +
		"{{end}}"
)

////////////////
// SmapHelper //
////////////////

var _ forMarshaler = SmapHelper{}

func (sth SmapHelper) forMarshal() any {
	return sth.Smap
}

//
// stats.NodeStatus
//

func calcCap(ds *stats.NodeStatus) (total uint64) {
	for _, cdf := range ds.Tcdf.Mountpaths {
		total += cdf.Capacity.Avail
		// TODO: a simplifying local-playground assumption and shortcut - won't work with loop devices, etc.
		// (ref: 152408)
		if ds.DeploymentType == apc.DeploymentDev {
			break
		}
	}
	return total
}

////////////////////////
// StatsAndStatusHelper //
////////////////////////

// for all stats.NodeStatus structs: select specific field and append to the returned slice
// (using the corresponding jtags here for no particular reason)
func (h *StatsAndStatusHelper) onlineStatus() []string { return h.toSlice("status") }
func (h *StatsAndStatusHelper) deployments() []string  { return h.toSlice("deployment") }
func (h *StatsAndStatusHelper) versions() []string     { return h.toSlice("ais_version") }
func (h *StatsAndStatusHelper) buildTimes() []string   { return h.toSlice("build_time") }
func (h *StatsAndStatusHelper) rebalance() []string    { return h.toSlice("rebalance_snap") }
func (h *StatsAndStatusHelper) pods() []string         { return h.toSlice("k8s_pod_name") }

// internal helper for the methods above
func (h *StatsAndStatusHelper) toSlice(jtag string) []string {
	if jtag == "status" {
		counts := make(map[string]int, 2)
		for _, m := range []StstMap{h.Pmap, h.Tmap} {
			for _, s := range m {
				status := s.Status
				if status == "" {
					status = UnknownStatusVal
				}
				if _, ok := counts[status]; !ok {
					counts[status] = 0
				}
				counts[status]++
			}
		}
		res := make([]string, 0, len(counts))
		for status, count := range counts {
			res = append(res, fmt.Sprintf("%d %s", count, status))
		}
		return res
	}

	// all other tags
	set := cos.NewStrSet()
	for _, m := range []StstMap{h.Pmap, h.Tmap} {
		for _, s := range m {
			switch jtag {
			case "deployment":
				if s.DeploymentType != "" { // (node offline)
					set.Add(s.DeploymentType)
				}
			case "ais_version":
				if s.Version != "" { // ditto
					set.Add(s.Version)
				}
			case "build_time":
				if s.BuildTime != "" { // ditto
					set.Add(s.BuildTime)
				}
			case "k8s_pod_name":
				set.Add(s.K8sPodName)
			case "rebalance_snap":
				if s.RebSnap != nil {
					set.Add(fmtRebStatus(s.RebSnap))
				}
			default:
				debug.Assert(false, jtag)
			}
		}
	}
	res := set.ToSlice()
	if len(res) == 0 {
		res = []string{UnknownStatusVal}
	}
	return res
}

func (m StstMap) allStateFlagsOK() bool {
	for _, ds := range m {
		if !ds.Cluster.Flags.IsOK() {
			return false
		}
	}
	return true
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

func fmtAlerts(flags cos.NodeStateFlags) (s string) {
	if flags.IsOK() {
		return "ok"
	}

	s = flags.String()
	if flags.IsRed() {
		return fred(s)
	}
	if flags.IsWarn() {
		return fcyan(s)
	}

	return s
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

func fmtCluSoft(version, build string) string {
	if version == "" {
		return unknownVal
	}
	if build == "" {
		return version + " (build: " + unknownVal + ")"
	}
	return version + " (build: " + build + ")"
}

func fmtRebalance(h StatsAndStatusHelper, config *cmn.ClusterConfig) (out string) {
	out = toString(h.rebalance())
	if config.Rebalance.Enabled {
		return out
	}
	disabled := fred("disabled")
	if out == NotSetVal || out == UnknownStatusVal {
		return disabled
	}
	return out + " (" + disabled + ")"
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

// DAC for:
// - virtual Directory
// - Archived file (ie, a file inside a shard object)
// - Chunked object
// NOTE: all 3 are mutually exclusive
//
// in re: virtual directories, see further:
// - `apc.LsNoDirs` and `apc.LsNoRecursion` and
// - https://github.com/NVIDIA/aistore/blob/main/docs/howto_virt_dirs.md
func fmtEntryNameDAC(val string, flags uint16) string {
	switch {
	case flags&apc.EntryInArch == apc.EntryInArch:
		debug.Assert(flags&(apc.EntryIsChunked|apc.EntryIsDir) == 0)
		return "    " + val
	case flags&apc.EntryIsChunked == apc.EntryIsChunked:
		debug.Assert(flags&(apc.EntryInArch|apc.EntryIsDir) == 0)

		// TODO: ideally, am able to color this entry but... indentation
		// see related fmtIsChunked() below
		return val
	case flags&apc.EntryIsDir == apc.EntryIsDir:
		debug.Assert(flags&(apc.EntryInArch|apc.EntryIsChunked) == 0)
		if !cos.IsLastB(val, '/') {
			val += "/"
		}
		return fgreen(val)
	default:
		return val
	}
}

func fmtIsChunked(flags uint16) string {
	if flags&apc.EntryIsChunked != 0 {
		debug.Assert(flags&apc.EntryIsDir == 0)
		debug.Assert(flags&apc.EntryInArch == 0)
		return FmtBool(true) // (compare with CACHED column)
	}
	return ""
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

func FmtXactRunFinAbrt(snap *core.Snap) (s string) {
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
