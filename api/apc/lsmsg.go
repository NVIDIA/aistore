// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"net/http"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
)

const (
	LocationPropSepa = ":"
	LsPropsSepa      = ","
)

// LsoMsg flags
const (
	// Applies to objects from the buckets with remote backends (e.g., to optimize-out listing remotes)
	// See related Flt* enum
	LsObjCached = 1 << iota

	LsMissing // include missing main obj (with copy existing)

	LsDeleted // include obj-s marked for deletion (TODO: not implemented yet)

	LsArchDir // expand archives as directories

	LsNameOnly // return only object names and, spearately, statuses
	LsNameSize // same as above and size (minor speedup)

	// Background: ============================================================
	// as far as AIS is concerned, adding a (confirmed to exist)
	// remote bucket (and its properties) to the cluster metadata is equivalent
	// to creating the bucket on the fly. =====================================

	// same as fltPresence == apc.Present (see query.go)
	LsBckPresent

	// LsDontHeadRemote is introduced primarily to support GCP buckets with
	// ACL policies that allow public _anonymous_ access.
	//
	// It appears that sometimes those policies do honor HEAD(bucket),
	// while other times they don't, failing the request with 401 or 403 status.
	// See also:
	// * https://cloud.google.com/storage/docs/access-control/making-data-public
	// * cmd/cli/cli/const.go for `dontHeadRemoteFlag`
	// * `QparamDontHeadRemote` (this package)
	LsDontHeadRemote

	// To list remote buckets without adding them to aistore
	// See also:
	// * cmd/cli/cli/const.go for `dontAddRemoteFlag`
	// * `QparamDontAddRemote` (this package)
	LsDontAddRemote

	// cache list-objects results and use this cache to speed-up
	UseListObjsCache

	// For remote buckets - list only remote props (aka `wantOnlyRemote`). When false,
	// the default that's being used is: `WantOnlyRemoteProps` - see below.
	// When true, the request gets executed in a pass-through fashion whereby a single ais target
	// simply forwards it to the associated remote backend and delivers the results as is to the
	// requesting proxy and, subsequently, to client.
	LsWantOnlyRemoteProps

	// List objects without recursion (POSIX-wise).
	// See related feature flag: feat.DontOptimizeVirtualDir
	LsNoRecursion

	// For remote metadata-capable buckets (ie., bck.HasVersioningMD() == true):
	// - check whether remote version exists,
	// and if it does:
	// - check whether remote version differs from its in-cluster copy
	LsVerChanged

	// Do not return virtual subdirectories - do not include them as `cmn.LsoEnt` entries
	LsNoDirs
)

// max page sizes
// see also:  bprops Extra.AWS.MaxPageSize
const (
	MaxPageSizeAIS   = 10000
	MaxPageSizeAWS   = 1000
	MaxPageSizeGCP   = 1000
	MaxPageSizeAzure = 5000
	MaxPageSizeOCI   = 1000
)

const (
	// Status
	LocOK = iota
	LocMisplacedNode
	LocMisplacedMountpath
	LocIsCopy
	LocIsCopyMissingObj

	// LsoEntry Flags
	EntryIsCached   = 1 << (EntryStatusBits + 1)
	EntryInArch     = 1 << (EntryStatusBits + 2)
	EntryIsDir      = 1 << (EntryStatusBits + 3)
	EntryIsArchive  = 1 << (EntryStatusBits + 4)
	EntryVerChanged = 1 << (EntryStatusBits + 5) // see also: QparamLatestVer, et al.
	EntryVerRemoved = 1 << (EntryStatusBits + 6) // ditto
)

// ObjEntry.Flags field
const (
	EntryStatusBits = 5                          // N bits
	EntryStatusMask = (1 << EntryStatusBits) - 1 // mask for N low bits
)

// LsoMsg and HEAD(object) enum (NOTE: compare with `cmn.ObjectProps`)
const (
	GetPropsName     = "name"
	GetPropsSize     = "size"
	GetPropsVersion  = "version"
	GetPropsChecksum = "checksum"
	GetPropsAtime    = "atime"
	GetPropsCached   = "cached"
	GetPropsStatus   = "status"
	GetPropsCopies   = "copies"
	GetPropsEC       = "ec"
	GetPropsCustom   = "custom"
	GetPropsLocation = "location" // advanced usage
)

const GetPropsNameSize = GetPropsName + LsPropsSepa + GetPropsSize

// NOTE: update when changing any of the above :NOTE
var (
	GetPropsMinimal      = []string{GetPropsName, GetPropsSize, GetPropsCached}
	GetPropsDefaultCloud = []string{GetPropsName, GetPropsSize, GetPropsCached,
		GetPropsChecksum, GetPropsVersion, GetPropsCustom}

	GetPropsDefaultAIS = []string{GetPropsName, GetPropsSize, GetPropsChecksum, GetPropsAtime}
	GetPropsAll        = []string{GetPropsName, GetPropsSize, GetPropsChecksum, GetPropsAtime,
		GetPropsVersion, GetPropsCached, GetPropsStatus, GetPropsCopies, GetPropsEC, GetPropsCustom, GetPropsLocation}
)

type LsoMsg struct {
	Header            http.Header `json:"hdr,omitempty"`         // (for pointers, see `ListArgs` in api/ls.go)
	UUID              string      `json:"uuid"`                  // ID to identify a single multi-page request
	Props             string      `json:"props"`                 // comma-delimited, e.g. "checksum,size,custom" (see GetProps* enum)
	TimeFormat        string      `json:"time_format,omitempty"` // RFC822 is the default
	Prefix            string      `json:"prefix"`                // return obj names starting with prefix (TODO: e.g. "A.tar/tutorials/")
	StartAfter        string      `json:"start_after,omitempty"` // start listing after (AIS buckets only)
	ContinuationToken string      `json:"continuation_token"`    // => LsoResult.ContinuationToken => LsoMsg.ContinuationToken
	SID               string      `json:"target"`                // selected target to solely execute backend.list-objects
	Flags             uint64      `json:"flags,string"`          // enum {LsObjCached, ...} - "LsoMsg flags" above
	PageSize          int64       `json:"pagesize"`              // max entries returned by list objects call
}

////////////
// LsoMsg //
////////////

func (lsmsg *LsoMsg) WantOnlyRemoteProps() bool {
	// set by user
	if lsmsg.IsFlagSet(LsWantOnlyRemoteProps) {
		return true
	}
	// set by user or proxy
	if lsmsg.IsFlagSet(LsNameOnly) || lsmsg.IsFlagSet(LsNameSize) {
		return true
	}
	// return false if there's anything outside GetPropsDefaultCloud subset
	for _, wn := range GetPropsAll {
		if lsmsg.WantProp(wn) {
			for _, n := range GetPropsDefaultCloud {
				if wn != n {
					return false
				}
			}
		}
	}
	return true
}

// WantProp returns true if msg request requires to return propName property.
func (lsmsg *LsoMsg) WantProp(propName string) bool {
	return strings.Contains(lsmsg.Props, propName)
}

func (lsmsg *LsoMsg) AddProps(propNames ...string) {
	for _, propName := range propNames {
		if lsmsg.WantProp(propName) {
			continue
		}
		if lsmsg.Props != "" {
			lsmsg.Props += LsPropsSepa
		}
		lsmsg.Props += propName
	}
}

func (lsmsg *LsoMsg) PropsSet() (s cos.StrSet) {
	props := strings.Split(lsmsg.Props, LsPropsSepa)
	s = make(cos.StrSet, len(props))
	for _, p := range props {
		s.Set(p)
	}
	return s
}

func (lsmsg *LsoMsg) Str(cname string) string {
	var sb strings.Builder
	sb.Grow(80)

	sb.WriteString(cname)
	if lsmsg.Props != "" {
		sb.WriteString(", props:")
		sb.WriteString(lsmsg.Props)
	}
	if lsmsg.Flags == 0 {
		return sb.String()
	}

	sb.WriteString(", flags:")
	if lsmsg.IsFlagSet(LsObjCached) {
		sb.WriteString("cached,")
	}
	if lsmsg.IsFlagSet(LsMissing) {
		sb.WriteString("missing,")
	}
	if lsmsg.IsFlagSet(LsArchDir) {
		sb.WriteString("arch,")
	}
	if lsmsg.IsFlagSet(LsBckPresent) {
		sb.WriteString("bck-present,")
	}
	if lsmsg.IsFlagSet(LsDontAddRemote) {
		sb.WriteString("skip-lookup,")
	}
	if lsmsg.IsFlagSet(LsNoRecursion) {
		sb.WriteString("no-recurs,")
	}
	if lsmsg.IsFlagSet(LsVerChanged) {
		sb.WriteString("version-changed,")
	}
	s := sb.String()
	return s[:len(s)-1]
}

// LsoMsg flags enum: LsObjCached, ...
func (lsmsg *LsoMsg) SetFlag(flag uint64)         { lsmsg.Flags |= flag }
func (lsmsg *LsoMsg) ClearFlag(flag uint64)       { lsmsg.Flags &= ^flag }
func (lsmsg *LsoMsg) IsFlagSet(flags uint64) bool { return lsmsg.Flags&flags == flags }

func (lsmsg *LsoMsg) Clone() *LsoMsg {
	c := &LsoMsg{}
	cos.CopyStruct(c, lsmsg)
	return c
}
