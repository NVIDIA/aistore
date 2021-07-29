// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import "github.com/NVIDIA/aistore/cmn/cos"

// BEWARE: change in this source MAY require re-running go generate ..

//go:generate msgp -tests=false -marshal=false

// BucketEntry corresponds to a single entry in the BucketList and
// contains file and directory metadata as per the SelectMsg
// Flags is a bit field:
// 0-2: objects status, all statuses are mutually exclusive, so it can hold up
//      to 8 different statuses. Now only OK=0, Moved=1, Deleted=2 are supported
// 3:   CheckExists (for remote bucket it shows if the object in present in AIS)
type BucketEntry struct {
	Name      string `json:"name" msg:"n"`                            // name of the object - NOTE: Does not include the bucket name.
	Size      int64  `json:"size,string,omitempty" msg:"s,omitempty"` // size in bytes
	Checksum  string `json:"checksum,omitempty" msg:"cs,omitempty"`   // checksum
	Atime     string `json:"atime,omitempty" msg:"a,omitempty"`       // formatted as per SelectMsg.TimeFormat
	Version   string `json:"version,omitempty" msg:"v,omitempty"`     // version/generation ID. In GCP it is int64, in AWS it is a string
	TargetURL string `json:"target_url,omitempty" msg:"t,omitempty"`  // URL of target which has the entry
	Copies    int16  `json:"copies,omitempty" msg:"c,omitempty"`      // ## copies (non-replicated = 1)
	Flags     uint16 `json:"flags,omitempty" msg:"f,omitempty"`       // object flags, like CheckExists, IsMoved etc
}

func (be *BucketEntry) CheckExists() bool  { return be.Flags&EntryIsCached != 0 }
func (be *BucketEntry) SetExists()         { be.Flags |= EntryIsCached }
func (be *BucketEntry) IsStatusOK() bool   { return be.Flags&EntryStatusMask == 0 }
func (be *BucketEntry) IsInsideArch() bool { return be.Flags&EntryInArch == 0 }
func (be *BucketEntry) String() string     { return "{" + be.Name + "}" }

func (be *BucketEntry) CopyWithProps(propsSet cos.StringSet) (ne *BucketEntry) {
	ne = &BucketEntry{Name: be.Name}
	if propsSet.Contains(GetPropsSize) {
		ne.Size = be.Size
	}
	if propsSet.Contains(GetPropsChecksum) {
		ne.Checksum = be.Checksum
	}
	if propsSet.Contains(GetPropsAtime) {
		ne.Atime = be.Atime
	}
	if propsSet.Contains(GetPropsVersion) {
		ne.Version = be.Version
	}
	if propsSet.Contains(GetTargetURL) {
		ne.TargetURL = be.TargetURL
	}
	if propsSet.Contains(GetPropsCopies) {
		ne.Copies = be.Copies
	}
	return
}

const (
	BckListFlagRebalance = (1 << iota) // List generated while rebalance was running
)

// BucketList represents the contents of a given bucket - somewhat analogous to the 'ls <bucket-name>'
type BucketList struct {
	UUID    string         `json:"uuid"`
	Entries []*BucketEntry `json:"entries"`
	// TODO: merge `UUID` into `ContinuationToken`
	ContinuationToken string `json:"continuation_token"`
	Flags             uint32 `json:"flags"` // extra information for a client, see BckListFlag*
}
