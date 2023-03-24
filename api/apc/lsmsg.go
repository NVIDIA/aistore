// Package apc: API messages and constants
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
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

	LsAll      // include misplaced objects and replicas
	LsDeleted  // include obj-s marked for deletion (TODO)
	LsArchDir  // expand archives as directories
	LsNameOnly // return only object names and statuses (for faster listing)
	LsNameSize // same as above plus size

	// The following two flags have to do with listing objects in those remote
	// buckets that we don't yet have in the cluster's BMD. As far as AIS is concerned,
	// adding a (confirmed to exist) remote bucket (and its properties) to the metadata
	// is equivalent to creating the bucket *on the fly*.
	//
	// For this, we need or, more exactly, we would like to execute HEAD request
	// against the remote backend in question, in order to:
	//    1) confirm the bucket's existence, and
	//    2) obtain its properties (e.g., versioning - for Cloud backends)
	//
	// Note: this is done only once.
	//
	// There are scenarios and cases, however, when HEAD(remote bucket) when
	// would rather be avoided or, alternatively, when an error it returns
	// (if it returns one) can be disregarded.

	// LsDontHeadRemote tells AIS _not_ to execute HEAD request on the remote bucket.
	// The reasons may include cleanup/eviction of any kind, prior knowledge that the bucket
	// must simply exist in AIS, and more.
	// See also:
	// * `cmn/feat/feat.go` source, and the (configurable) capability
	//    to disable on-the-fly creation of remote buckets altogether.
	LsDontHeadRemote

	// LsTryHeadRemote is introduced primarily to support GCP buckets with
	// ACL policies that allow public _anonymous_ access.
	//
	// It appears that sometimes those policies do honor HEAD(bucket),
	// while other times they don't, failing the request with 401 or 403 status.
	// See also:
	// * at https://cloud.google.com/storage/docs/access-control/making-data-public
	LsTryHeadRemote

	// To list remote buckets that, if not be present in AIS, shall not be added to AIS
	// (TODO: reserved for future use)
	LsDontAddRemote

	// cache list-objects results and use this cache to speed-up
	UseListObjsCache

	// For remote buckets - list only remote props (aka `wantOnlyRemote`). When false,
	// the default that's being used is: `WantOnlyRemoteProps` - see below.
	// When true, the request gets executed in a pass-through fashion whereby a single ais target
	// simply forwards it to the associated remote backend and delivers the results as is to the
	// requesting proxy and, subsequently, to client.
	LsWantOnlyRemoteProps

	// List entries without recursion, the result includes matching directories
	// TODO: works only with AIS bucket. Need to fix:
	// - update AIS CLI and allow a user to list a bucket non-recursively
	// - check the flag with 'Cached' flag
	// - forbid combinations(if any exists) with other flags that do not work
	// - pass correct value to the backend in case of a Cloud bucket
	LsNoRecursion
)

// List objects default page size
const (
	DefaultPageSizeAIS   = 10000
	DefaultPageSizeCloud = 1000
)

const (
	// Status
	LocOK = iota
	LocMisplacedNode
	LocMisplacedMountpath
	LocIsCopy
	LocIsCopyMissingObj

	// Flags
	EntryIsCached = 1 << (EntryStatusBits + 1)
	EntryInArch   = 1 << (EntryStatusBits + 2)
	EntryIsDir    = 1 << (EntryStatusBits + 3)
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
	UUID              string `json:"uuid"`               // ID to identify a single multi-page request
	Props             string `json:"props"`              // comma-delimited, e.g. "checksum,size,custom" (see GetProps* enum)
	TimeFormat        string `json:"time_format"`        // RFC822 is the default
	Prefix            string `json:"prefix"`             // objname filter: return names starting with prefix
	StartAfter        string `json:"start_after"`        // start listing after (AIS buckets only)
	ContinuationToken string `json:"continuation_token"` // BucketList.ContinuationToken
	SID               string `json:"target"`             // selected target to solely execute backend.list-objects
	Flags             uint64 `json:"flags,string"`       // enum {LsObjCached, ...} - see above
	PageSize          uint   `json:"pagesize"`           // max entries returned by list objects call
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

// NOTE: internal usage
func (lsmsg *LsoMsg) WantOnlyName() bool {
	if lsmsg.IsFlagSet(LsNameOnly) || lsmsg.Props == GetPropsName {
		return true
	}
	return strings.IndexByte(lsmsg.Props, LsPropsSepa[0]) < 0 && strings.Contains(lsmsg.Props, GetPropsName)
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

func (lsmsg *LsoMsg) SetFlag(flag uint64)         { lsmsg.Flags |= flag }
func (lsmsg *LsoMsg) IsFlagSet(flags uint64) bool { return lsmsg.Flags&flags == flags }

func (lsmsg *LsoMsg) Clone() *LsoMsg {
	c := &LsoMsg{}
	cos.CopyStruct(c, lsmsg)
	return c
}
