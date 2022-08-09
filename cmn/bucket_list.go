// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
)

//
// NOTE: changes in this source MAY require re-running `msgp` code generation - see docs/msgp.md for details.
//

// BucketEntry corresponds to a single entry in the BucketList and
// contains file and directory metadata as per the ListObjsMsg
// Flags is a bit field:
// 0-2: objects status, all statuses are mutually exclusive, so it can hold up
//
//	to 8 different statuses. Now only OK=0, Moved=1, Deleted=2 are supported
//
// 3:   CheckExists (for remote bucket it shows if the object in present in AIS)
type BucketEntry struct {
	Name      string `json:"name" msg:"n"`                            // name of the object (NOTE: does not include the bucket name)
	Checksum  string `json:"checksum,omitempty" msg:"cs,omitempty"`   // object's checksum
	Atime     string `json:"atime,omitempty" msg:"a,omitempty"`       // last access time; formatted as per ListObjsMsg.TimeFormat
	Version   string `json:"version,omitempty" msg:"v,omitempty"`     // GCP int64 generation, AWS version (string), etc.
	TargetURL string `json:"target_url,omitempty" msg:"t,omitempty"`  // (advanced usage)
	Size      int64  `json:"size,string,omitempty" msg:"s,omitempty"` // size in bytes
	Copies    int16  `json:"copies,omitempty" msg:"c,omitempty"`      // ## copies (NOTE: non-replicated = 1)
	Flags     uint16 `json:"flags,omitempty" msg:"f,omitempty"`
}

func (be *BucketEntry) CheckExists() bool  { return be.Flags&apc.EntryIsCached != 0 }
func (be *BucketEntry) SetExists()         { be.Flags |= apc.EntryIsCached }
func (be *BucketEntry) IsStatusOK() bool   { return be.Status() == 0 }
func (be *BucketEntry) Status() uint16     { return be.Flags & apc.EntryStatusMask }
func (be *BucketEntry) IsInsideArch() bool { return be.Flags&apc.EntryInArch != 0 }
func (be *BucketEntry) String() string     { return "{" + be.Name + "}" }

func (be *BucketEntry) CopyWithProps(propsSet cos.StringSet) (ne *BucketEntry) {
	ne = &BucketEntry{Name: be.Name}
	if propsSet.Contains(apc.GetPropsSize) {
		ne.Size = be.Size
	}
	if propsSet.Contains(apc.GetPropsChecksum) {
		ne.Checksum = be.Checksum
	}
	if propsSet.Contains(apc.GetPropsAtime) {
		ne.Atime = be.Atime
	}
	if propsSet.Contains(apc.GetPropsVersion) {
		ne.Version = be.Version
	}
	if propsSet.Contains(apc.GetTargetURL) {
		ne.TargetURL = be.TargetURL
	}
	if propsSet.Contains(apc.GetPropsCopies) {
		ne.Copies = be.Copies
	}
	return
}

const (
	BckListFlagRebalance = (1 << iota) // List generated while rebalance was running
)

// BucketList represents the contents of a given bucket - somewhat analogous to the 'ls <bucket-name>'
type BucketList struct {
	UUID              string         `json:"uuid"`
	ContinuationToken string         `json:"continuation_token"`
	Entries           []*BucketEntry `json:"entries"`
	Flags             uint32         `json:"flags"`
}
