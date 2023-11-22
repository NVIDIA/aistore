// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"github.com/NVIDIA/aistore/cmn/cos"
)

const MsgpLsoBufSize = 32 * cos.KiB

//
// NOTE: changes in this source MAY require re-running `msgp` code generation - see docs/msgp.md for details.
// NOTE: all json tags except `Flags` must belong to the (apc.GetPropsName, apc.GetPropsSize, etc.) enumeration
//

// LsoEntry is a single entry in LsoResult.Entries slice (below) containing list-objects result
// for the corresponding (listed) object or an archived file;
// `Flags` is a bit field where `EntryStatusBits` bits [0-4] are reserved for object status
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
		Flags    uint16 `json:"flags,omitempty" msg:"f,omitempty"`       // enum { EntryIsCached, EntryIsDir, EntryInArch, ...}
	}

	LsoEntries []*LsoEntry

	// LsoResult carries the results of `api.ListObjects`, `BackendProvider.ListObjects`, and friends
	LsoResult struct {
		UUID              string     `json:"uuid"`
		ContinuationToken string     `json:"continuation_token"`
		Entries           LsoEntries `json:"entries"`
		Flags             uint32     `json:"flags"`
	}
)
