// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"errors"
	"fmt"
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
)

// max page sizes
// see also:  bprops Extra.AWS.MaxPageSize
const (
	MaxPageSizeAIS   = 10000
	MaxPageSizeAWS   = 1000 // note: SwiftStack/AWS = 10_000
	MaxPageSizeGCP   = 1000
	MaxPageSizeAzure = 5000
	MaxPageSizeOCI   = 1000
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
	GetPropsMinimal      = []string{GetPropsName, GetPropsSize, GetPropsCached}
	GetPropsDefaultCloud = []string{GetPropsName, GetPropsSize, GetPropsCached,
		GetPropsChecksum, GetPropsVersion, GetPropsCustom}

	GetPropsDefaultAIS = []string{GetPropsName, GetPropsSize, GetPropsChecksum, GetPropsAtime}
	GetPropsAll        = []string{GetPropsName, GetPropsSize, GetPropsChecksum, GetPropsAtime,
		GetPropsVersion, GetPropsCached, GetPropsStatus, GetPropsCopies, GetPropsEC, GetPropsCustom, GetPropsLocation}
)

type (
	// swagger:model
	LsoMsg struct {
		Header            http.Header `json:"hdr,omitempty"`         // (for pointers, see `ListArgs` in api/ls.go)
		UUID              string      `json:"uuid"`                  // ID to identify a single multi-page request
		Props             string      `json:"props"`                 // comma-delimited, e.g. "checksum,size,custom" (see GetProps* enum)
		TimeFormat        string      `json:"time_format,omitempty"` // RFC822 is the default
		Prefix            string      `json:"prefix"`                // return names starting with prefix (including arch. files (e.g. "A.tar/tutorials/"))
		StartAfter        string      `json:"start_after,omitempty"` // start listing after (AIS buckets only)
		ContinuationToken string      `json:"continuation_token"`    // => LsoResult.ContinuationToken => LsoMsg.ContinuationToken
		SID               string      `json:"target"`                // selected target to solely execute backend.list-objects
		Flags             uint64      `json:"flags,string"`          // enum {LsCached, ...} - "LsoMsg flags" above
		PageSize          int64       `json:"pagesize"`              // max entries returned by list objects call
	}

	CreateInvMsg struct {
		Name string `json:"name,omitempty"` // inventory name (optional; must be unique for a given bucket)
		LsoMsg

		// PagesPerChunk overrides the default number of pages to pack into a single inventory chunk.
		// If zero, the default is used.
		PagesPerChunk int64 `json:"pages_per_chunk,omitempty"`

		// MaxEntriesPerChunk puts a hard cap on the number of entries in a single inventory chunk.
		// If zero, the cap is disabled.
		MaxEntriesPerChunk int64 `json:"max_entries_per_chunk,omitempty"`
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

//////////////////
// CreateInvMsg //
//////////////////

const (
	DefaultInvPagesPerChunk = 50
	MaxInvPagesPerChunk     = 256
)

// validate; set defaults
func (m *CreateInvMsg) SetValidate() error {
	const etag = "invalid control message for " + ActCreateInventory

	// 1) disallow
	if m.ContinuationToken != "" {
		return errors.New(etag + ": continuation_token must be empty")
	}
	if m.StartAfter != "" {
		return errors.New(etag + ": start_after is not supported")
	}
	// flags that don't make sense for inventory generation
	const badFlags = LsCached | LsNotCached | LsMissing | LsDeleted | LsArchDir |
		LsBckPresent | LsDontHeadRemote | LsDontAddRemote |
		lsWantOnlyRemoteProps | LsNoRecursion | LsDiff | LsIsS3
	if m.Flags&badFlags != 0 {
		var sb cos.SB
		sb.Grow(64)
		sb.WriteString("flags:")
		m.appendFlags(&sb)
		return fmt.Errorf("%s: %s", etag, sb.String())
	}

	// 2) advanced tunables
	switch {
	case m.PagesPerChunk == 0:
		m.PagesPerChunk = DefaultInvPagesPerChunk
	case m.PagesPerChunk < 0:
		return fmt.Errorf("%s: pages_per_chunk=%d", etag, m.PagesPerChunk)
	case m.PagesPerChunk > MaxInvPagesPerChunk:
		return fmt.Errorf("%s: pages_per_chunk too large: %d", etag, m.PagesPerChunk)
	}
	if m.MaxEntriesPerChunk < 0 {
		return fmt.Errorf("%s: max_entries_per_chunk=%d", etag, m.MaxEntriesPerChunk)
	}

	// 3) NOTE: othwerwise, backend _may_ append extra (virt-dir) entries (in re: pre-allocation+reuse)
	m.SetFlag(LsNoDirs)

	// 4) absolute minimum
	if m.IsFlagSet(LsNameOnly) {
		m.Props = GetPropsName
		return nil
	}
	if m.IsFlagSet(LsNameSize) {
		if m.Props == "" {
			m.Props = GetPropsNameSize
		} else {
			m.AddProps(GetPropsName, GetPropsSize)
		}
		return nil
	}

	// 5) default props
	if m.Props == "" {
		m.AddProps(GetPropsName, GetPropsSize, GetPropsCached)
	} else {
		m.AddProps(GetPropsCached)
	}

	return nil
}
