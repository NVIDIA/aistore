// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
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
	// only list in-cluster objects, i.e., those from the respective remote bucket that are present (\"cached\")
	// see also: flt* enum and `LsNotCached` below
	LsCached = 1 << iota

	// include missing main obj (with copy existing)
	LsMissing

	// include obj-s marked for deletion (TODO: not implemented yet)
	LsDeleted

	// expand archives as directories
	LsArchDir

	// return only object names and, separately, statuses
	LsNameOnly

	// same as above and size (minor speedup)
	LsNameSize

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

	// list remote buckets without adding them to aistore
	// See also:
	// * cmd/cli/cli/const.go for `dontAddRemoteFlag`
	// * `QparamDontAddRemote` (this package)
	LsDontAddRemote

	// strict opposite of the `LsCached`
	LsNotCached

	// NOTE: optimization-only and not used yet -------------------------------------------------
	// for remote buckets - list only remote props (aka `wantOnlyRemote`). When false,
	// the default that's being used is: `WantOnlyRemoteProps` - see below.
	// When true, the request gets executed in a pass-through fashion whereby a single ais target
	// simply forwards it to the associated remote backend and delivers the results as is to the
	// requesting proxy and, subsequently, to client. -------------------------------------------
	lsWantOnlyRemoteProps

	// list objects without recursion (POSIX-wise).
	// see related feature flag: feat.DontOptimizeVirtualDir
	LsNoRecursion

	// bidirectional (remote <-> in-cluster) diff requires remote metadata-capable (`HasVersioningMD`) buckets;
	// it entails:
	// - checking whether remote version exists,
	//   and if it does,
	// - checking whether it differs from its in-cluster copy.
	// see related `cmn.LsoEnt` flags: `EntryVerChanged` and `EntryVerRemoved`, respectively.
	LsDiff

	// do not return virtual subdirectories - do not include them as `cmn.LsoEnt` entries
	LsNoDirs

	// the caller is s3 compatibility API
	LsIsS3

	// instead of (remote) bucket: list native bucket inventory (NBI) snapshot
	// NOTE on pagination:
	// in the context of inventory listing, lsmsg.PageSize (if non-zero) is best-effort and approximate:
	// each target delivers an approximate share of the requested page size,
	// subject to local chunking, minimum bounds, and slight overfetch
	LsNBI
)

// max page sizes
// see also:  bprops Extra.AWS.MaxPageSize
const (
	MaxPageSizeAIS   = 10000
	MaxPageSizeAWS   = 1000 // note: SwiftStack/AWS = 10_000
	MaxPageSizeGCP   = 1000
	MaxPageSizeAzure = 5000
	MaxPageSizeOCI   = 1000

	MaxPageSizeGlobal = MaxPageSizeAIS // NOTE: maximum across all providers
)

// cmn/objlist_utils
const (
	statusBits    = 5
	LsoStatusMask = (1 << statusBits) - 1
)

// NOTE: approaching uint16 limit - bits 9,14,15 remaining
const (
	// location _status_
	LocOK = iota
	LocMisplacedNode
	LocMisplacedMountpath
	LocIsCopy
	LocIsCopyMissingObj // missing "main replica"

	// LsoEntry Flags
	EntryIsCached   = 1 << (statusBits + 1)
	EntryInArch     = 1 << (statusBits + 2)
	EntryIsDir      = 1 << (statusBits + 3)
	EntryIsArchive  = 1 << (statusBits + 4)
	EntryVerChanged = 1 << (statusBits + 5) // see also: QparamLatestVer, et al.
	EntryVerRemoved = 1 << (statusBits + 6) // ditto
	EntryHeadFail   = 1 << (statusBits + 7)
	// added v4.0
	EntryIsChunked = 1 << (statusBits + 8) // see NOTE above
)

// LsoMsg and HEAD(object) enum
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

	// 4.0
	GetPropsChunked = "chunked"
	// 4.2
	GetPropsLastModified = "last-modified"
	GetPropsETag         = "etag"
)

const GetPropsNameSize = GetPropsName + LsPropsSepa + GetPropsSize

// NOTE: update when changing any of the above :NOTE
var (
	// TODO [v4.5]: remove V1 props and HeadObject() API and impl. - superseded by V2
	GetPropsMinimal      = []string{GetPropsName, GetPropsSize, GetPropsCached}
	GetPropsDefaultCloud = []string{GetPropsName, GetPropsSize, GetPropsCached,
		GetPropsChecksum, GetPropsVersion, GetPropsCustom}

	GetPropsDefaultAIS = []string{GetPropsName, GetPropsSize, GetPropsChecksum, GetPropsAtime}
	GetPropsAll        = []string{GetPropsName, GetPropsSize, GetPropsChecksum, GetPropsAtime,
		GetPropsVersion, GetPropsCached, GetPropsStatus, GetPropsCopies, GetPropsEC, GetPropsCustom, GetPropsLocation}

	// GetPropsAllV2 extends GetPropsAll with fields exclusive to ObjectPropsV2.
	// Note: GetPropsCached ("cached") and GetPropsStatus ("status") are intentionally
	// omitted — the V2 HEAD handler rejects them; `present` is always returned separately.
	GetPropsAllV2 = []string{GetPropsName, GetPropsSize, GetPropsChecksum, GetPropsAtime,
		GetPropsVersion, GetPropsLastModified, GetPropsETag,
		GetPropsCopies, GetPropsEC, GetPropsCustom, GetPropsLocation, GetPropsChunked}

	GetPropsMinimalV2      = []string{GetPropsName, GetPropsSize}
	GetPropsDefaultCloudV2 = []string{GetPropsName, GetPropsSize, GetPropsChecksum, GetPropsVersion, GetPropsCustom, GetPropsLastModified, GetPropsETag}
)

type (
	// LsoMsg is the list-objects control message. It carries paging
	// state (`uuid`, `continuation_token`) plus filters and
	// presentation options that select which objects to return and
	// which properties to include. For multi-page listings, the server
	// echoes an opaque `continuation_token` that the client passes back
	// on the next request; `uuid` ties all pages of the same listing
	// together.
	LsoMsg struct {
		// Request headers forwarded to the backend (remote buckets only).
		Header http.Header `json:"hdr,omitempty"` // +gen:optional
		// Stable ID that ties all pages of one listing together.
		// Server-assigned on the first response; echoed by the client
		// on each subsequent page.
		UUID string `json:"uuid"` // +gen:optional
		// Comma-separated list of object properties to include per
		// entry (e.g. `"checksum,size,custom"`). See the `GetProps*`
		// constants in this package for the full set. Empty selects
		// server-side defaults.
		Props string `json:"props"` // +gen:optional
		// Go time format used to render time-valued properties.
		// Defaults to RFC822.
		TimeFormat string `json:"time_format,omitempty"` // +gen:optional
		// Return only entries whose name starts with this prefix. For
		// archive objects, the prefix also matches paths inside the
		// archive (e.g. `"A.tar/tutorials/"`).
		Prefix string `json:"prefix"` // +gen:optional
		// Start listing strictly after this name (exclusive). AIS
		// buckets only.
		StartAfter string `json:"start_after,omitempty"` // +gen:optional
		// Opaque token returned by the previous page; pass it back to
		// fetch the next page. Empty on the first request; empty again
		// once the listing is exhausted.
		ContinuationToken string `json:"continuation_token"` // +gen:optional
		// Pin the backend list-objects call to a single target by
		// daemon ID. Advanced; typically left empty.
		SID string `json:"target"` // +gen:optional
		// Bit flags selecting presentation and filters (see the `Ls*`
		// constants, e.g. `LsCached`, `LsNotCached`, `LsDiff`,
		// `LsNameOnly`, `LsNameSize`, `LsNoRecursion`).
		Flags uint64 `json:"flags,string"` // +gen:optional
		// Maximum entries returned in a single page. `0` selects the
		// server-side default.
		PageSize int64 `json:"pagesize"` // +gen:optional
	}
)

////////////
// LsoMsg //
////////////

func (lsmsg *LsoMsg) WantOnlyRemoteProps() bool {
	if lsmsg.IsFlagSet(LsDiff) || lsmsg.IsFlagSet(LsNotCached) || lsmsg.IsFlagSet(LsCached) {
		return false
	}
	if lsmsg.IsFlagSet(lsWantOnlyRemoteProps) {
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

// default props & flags => user-provided message
func (lsmsg *LsoMsg) NormalizeNameSizeDflt() {
	switch lsmsg.Props {
	case "":
		if lsmsg.IsFlagSet(LsCached) {
			lsmsg.AddProps(GetPropsDefaultAIS...)
		} else {
			lsmsg.AddProps(GetPropsMinimal...)
			lsmsg.SetFlag(LsNameSize)
		}
	case GetPropsName:
		lsmsg.SetFlag(LsNameOnly)
	case GetPropsNameSize:
		lsmsg.SetFlag(LsNameSize)
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

func (lsmsg *LsoMsg) Str(cname string, sb *cos.SB) {
	sb.WriteString(cname)
	if lsmsg.Props != "" {
		sb.WriteString(", props:")
		sb.WriteString(lsmsg.Props)
	}
	if fl := lsmsg.Flags; fl != 0 {
		sb.WriteString(", flags:")
		lsmsg.appendFlags(sb)
	}
}

func (lsmsg *LsoMsg) appendFlags(sb *cos.SB) {
	flags := make([]string, 0, 8)

	if lsmsg.IsFlagSet(LsCached) {
		flags = append(flags, "cached")
	}
	if lsmsg.IsFlagSet(LsMissing) {
		flags = append(flags, "missing")
	}
	if lsmsg.IsFlagSet(LsDeleted) {
		flags = append(flags, "deleted")
	}
	if lsmsg.IsFlagSet(LsArchDir) {
		flags = append(flags, "arch-dir")
	}
	if lsmsg.IsFlagSet(LsNameOnly) {
		flags = append(flags, "name-only")
	}
	if lsmsg.IsFlagSet(LsNameSize) {
		flags = append(flags, "name-size")
	}
	if lsmsg.IsFlagSet(LsBckPresent) {
		flags = append(flags, "bck-present")
	}
	if lsmsg.IsFlagSet(LsDontHeadRemote) {
		flags = append(flags, "no-head-remote")
	}
	if lsmsg.IsFlagSet(LsDontAddRemote) {
		flags = append(flags, "no-add-remote")
	}
	if lsmsg.IsFlagSet(LsNotCached) {
		flags = append(flags, "not-cached")
	}
	if lsmsg.IsFlagSet(lsWantOnlyRemoteProps) {
		flags = append(flags, "only-remote-props")
	}
	if lsmsg.IsFlagSet(LsNoRecursion) {
		flags = append(flags, "no-recursion")
	}
	if lsmsg.IsFlagSet(LsDiff) {
		flags = append(flags, "diff")
	}
	if lsmsg.IsFlagSet(LsNoDirs) {
		flags = append(flags, "no-dirs")
	}
	if lsmsg.IsFlagSet(LsIsS3) {
		flags = append(flags, "s3")
	}

	if len(flags) > 0 {
		sb.WriteString(strings.Join(flags, ","))
	}
}

// LsoMsg flags enum: LsCached, ...
func (lsmsg *LsoMsg) SetFlag(flag uint64)         { lsmsg.Flags |= flag }
func (lsmsg *LsoMsg) ClearFlag(flag uint64)       { lsmsg.Flags &= ^flag }
func (lsmsg *LsoMsg) IsFlagSet(flags uint64) bool { return lsmsg.Flags&flags == flags }

func (lsmsg *LsoMsg) Clone() *LsoMsg {
	c := &LsoMsg{}
	cos.CopyStruct(c, lsmsg)
	return c
}
