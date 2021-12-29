// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"fmt"
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
	LsOnlyNames             // return only object names and statuses (for faster listing)
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
		UseCache          bool   `json:"use_cache"`          // use proxy cache to speed up listing objects
	}
)

/////////////////
// ListObjsMsg //
/////////////////

// NeedLocalMD indicates that ListObjects for a remote bucket needs
// to include AIS-maintained metadata: access time, etc.
func (msg *ListObjsMsg) NeedLocalMD() bool {
	return msg.WantProp(GetPropsAtime) ||
		msg.WantProp(GetPropsStatus) ||
		msg.WantProp(GetPropsCopies) ||
		msg.WantProp(GetPropsCached)
}

// WantProp returns true if msg request requires to return propName property.
func (msg *ListObjsMsg) WantProp(propName string) bool {
	debug.Assert(!strings.ContainsRune(propName, ','))
	return strings.Contains(msg.Props, propName)
}

func (msg *ListObjsMsg) AddProps(propNames ...string) {
	for _, propName := range propNames {
		if msg.WantProp(propName) {
			continue
		}
		if msg.Props != "" {
			msg.Props += ","
		}
		msg.Props += propName
	}
}

func (msg *ListObjsMsg) PropsSet() (s cos.StringSet) {
	props := strings.Split(msg.Props, ",")
	s = make(cos.StringSet, len(props))
	for _, p := range props {
		s.Add(p)
	}
	return s
}

func (msg *ListObjsMsg) SetFlag(flag uint64)         { msg.Flags |= flag }
func (msg *ListObjsMsg) IsFlagSet(flags uint64) bool { return msg.Flags&flags == flags }

func (msg *ListObjsMsg) ListObjectsCacheID(bck Bck) string {
	return fmt.Sprintf("%s/%s", bck.String(), msg.Prefix)
}

func (msg *ListObjsMsg) Clone() *ListObjsMsg {
	c := &ListObjsMsg{}
	cos.CopyStruct(c, msg)
	return c
}
