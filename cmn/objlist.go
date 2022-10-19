// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
)

//
// NOTE: changes in this source MAY require re-running `msgp` code generation - see docs/msgp.md for details.
// NOTE: all json tags except `Flags` must belong to the (apc.GetPropsName, apc.GetPropsSize, etc.) enumeration
//

// LsoEntry corresponds to a single entry in the LsoResult and
// contains file and directory metadata as per the ListObjsMsg
// `Flags` is a bit field where ibits 0-2 are reserved for object status
// (all statuses are mutually exclusive)
type (
	LsoEntry struct {
		Name     string `json:"name" msg:"n"`                            // object name
		Checksum string `json:"checksum,omitempty" msg:"cs,omitempty"`   // checksum
		Atime    string `json:"atime,omitempty" msg:"a,omitempty"`       // last access time; formatted as ListObjsMsg.TimeFormat
		Version  string `json:"version,omitempty" msg:"v,omitempty"`     // e.g., GCP int64 generation, AWS version (string), etc.
		Location string `json:"location,omitempty" msg:"t,omitempty"`    // [tnode:mountpath]
		Custom   string `json:"custom-md,omitempty" msg:"m,omitempty"`   // custom metadata: ETag, MD5, CRC, user-defined ...
		Size     int64  `json:"size,string,omitempty" msg:"s,omitempty"` // size in bytes
		Copies   int16  `json:"copies,omitempty" msg:"c,omitempty"`      // ## copies (NOTE: for non-replicated object copies == 1)
		Flags    uint16 `json:"flags,omitempty" msg:"f,omitempty"`
	}

	// LsoResult carries the results of `api.ListObjects`, `BackendProvider.ListObjects`, and friends
	LsoResult struct {
		UUID              string      `json:"uuid"`
		ContinuationToken string      `json:"continuation_token"`
		Entries           []*LsoEntry `json:"entries"`
		Flags             uint32      `json:"flags"`
	}
)

////////////////
// LsoEntry //
////////////////

func (be *LsoEntry) CheckExists() bool  { return be.Flags&apc.EntryIsCached != 0 } // NOTE: "cached" and "present" are interchangeable
func (be *LsoEntry) SetPresent()        { be.Flags |= apc.EntryIsCached }
func (be *LsoEntry) IsStatusOK() bool   { return be.Status() == 0 }
func (be *LsoEntry) Status() uint16     { return be.Flags & apc.EntryStatusMask }
func (be *LsoEntry) IsInsideArch() bool { return be.Flags&apc.EntryInArch != 0 }
func (be *LsoEntry) String() string     { return "{" + be.Name + "}" }

func (be *LsoEntry) CopyWithProps(propsSet cos.StrSet) (ne *LsoEntry) {
	ne = &LsoEntry{Name: be.Name}
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
	if propsSet.Contains(apc.GetPropsLocation) {
		ne.Location = be.Location
	}
	if propsSet.Contains(apc.GetPropsCustom) {
		ne.Custom = be.Custom
	}
	if propsSet.Contains(apc.GetPropsCopies) {
		ne.Copies = be.Copies
	}
	return
}
