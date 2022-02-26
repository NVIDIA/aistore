// Package apc: API constants
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// ListObjsMsg flags
const (
	LsPresent   = 1 << iota // applies to buckets with remote backends (to _optimize-out_ listing the latter)
	LsMisplaced             // include misplaced obj-s
	LsDeleted               // include obj-s marked for deletion (TODO)
	LsArchDir               // expand archives as directories
	LsNameOnly              // return only object names and statuses (for faster listing)

	// cache list-objects results and use this cache to speed-up
	UseListObjsCache
)

// ListObjsMsg and HEAD(object) enum
// NOTE: compare with `ObjectProps` below and popular lists of selected props (below as well)
const (
	GetPropsName     = "name"
	GetPropsSize     = "size"
	GetPropsVersion  = "version"
	GetPropsChecksum = "checksum"
	GetPropsAtime    = "atime"
	GetPropsCached   = "cached"
	GetTargetURL     = "target_url"
	GetPropsStatus   = "status"
	GetPropsCopies   = "copies"
	GetPropsEC       = "ec"
	GetPropsCustom   = "custom"
)

type (
	ListObjsMsg struct {
		UUID              string `json:"uuid"`               // ID to identify a single multi-page request
		Props             string `json:"props"`              // e.g. "checksum,size"
		TimeFormat        string `json:"time_format"`        // "RFC822" default - see the enum above
		Prefix            string `json:"prefix"`             // objname filter: return names starting with prefix
		StartAfter        string `json:"start_after"`        // start listing after (AIS buckets only)
		ContinuationToken string `json:"continuation_token"` // `BucketList.ContinuationToken`
		Flags             uint64 `json:"flags,string"`       // enum {LsPresent, ...} - see above
		PageSize          uint   `json:"pagesize"`           // max entries returned by list objects call
	}
)

// popular lists of selected props (NOTE: update when/if `ObjectProps` type changes)
var (
	// minimal
	GetPropsMinimal = []string{GetPropsName, GetPropsSize}
	// compact
	GetPropsDefault = []string{GetPropsName, GetPropsSize, GetPropsChecksum, GetPropsAtime}
	// all
	GetPropsAll = append(GetPropsDefault,
		GetPropsVersion, GetPropsCached, GetTargetURL, GetPropsStatus, GetPropsCopies, GetPropsEC, GetPropsCustom,
	)
)

/////////////////
// ListObjsMsg //
/////////////////

// NeedLocalMD indicates that ListObjects for a remote bucket needs
// to include AIS-maintained metadata: access time, etc.
func (lsmsg *ListObjsMsg) NeedLocalMD() bool {
	return lsmsg.WantProp(GetPropsAtime) ||
		lsmsg.WantProp(GetPropsStatus) ||
		lsmsg.WantProp(GetPropsCopies) ||
		lsmsg.WantProp(GetPropsCached)
}

// WantProp returns true if msg request requires to return propName property.
func (lsmsg *ListObjsMsg) WantProp(propName string) bool {
	debug.Assert(!strings.ContainsRune(propName, ','))
	return strings.Contains(lsmsg.Props, propName)
}

func (lsmsg *ListObjsMsg) AddProps(propNames ...string) {
	for _, propName := range propNames {
		if lsmsg.WantProp(propName) {
			continue
		}
		if lsmsg.Props != "" {
			lsmsg.Props += ","
		}
		lsmsg.Props += propName
	}
}

func (lsmsg *ListObjsMsg) PropsSet() (s cos.StringSet) {
	props := strings.Split(lsmsg.Props, ",")
	s = make(cos.StringSet, len(props))
	for _, p := range props {
		s.Add(p)
	}
	return s
}

func (lsmsg *ListObjsMsg) SetFlag(flag uint64)         { lsmsg.Flags |= flag }
func (lsmsg *ListObjsMsg) IsFlagSet(flags uint64) bool { return lsmsg.Flags&flags == flags }

func (lsmsg *ListObjsMsg) Clone() *ListObjsMsg {
	c := &ListObjsMsg{}
	cos.CopyStruct(c, lsmsg)
	return c
}
