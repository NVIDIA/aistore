// Package apc: API control messages and constants
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"encoding/json"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// Definitions
// ===========
// - GetBatch() supports reading (and returning in a single TAR) - multiple objects, multiple archived (sharded) files,
//   or any mix of thereof
//
// - TAR is not the only supported output format - compression is supported in a variety of ways (see `OutputFormat`)
//    - here and elsewhere, "TAR" is used strictly for reading convenience
//
// - When GetBatch() returns an error all the other returned data and/or metadata (if any) can be ignored and dropped
//
// - When GetBatch() succeeds (err == nil):
//    - user should expect the same exact number of entries in the MossResp and the same number of files in the resulting TAR
//    - the order: entries in the response (MossResp, TAR) are ordered in **precisely** the same order as in the MossReq
//
// - Naming convention:
//    - by default, files in the resulting TAR are named <Bucket>/<ObjName>
//    - `OnlyObjName` further controls the naming:
//       - when set:            archived (sharded) files are named as <ObjName>/<archpath> in the resulting TAR
//       - otherwise (default): archived files are named as <Bucket>/<ObjName>/<archpath>
//    The third variant (just <archpath>) is not currently supported to avoid ambiguity.
//
// - When (and only if) `ContinueOnErr` is set true:
//   - missing files will not result in GetBatch() failure
//   - missing files will be present in the resulting TAR
//     - with "__404__/" prefix and zero size
//     - if you extract resulting TAR you'll find them all in one place: under __404__/<Bucket>/<ObjName>
//
// - Colocation hint:
//   - 0 (default): no optimization - suitable for uniformly distributed data lakes where objects are spread
//     evenly across all targets
//   - 1: target-aware - indicates that objects in this batch are collocated on few targets;
//     proxy will compute HRW distribution and select the optimal distributed target (DT) to minimize
//     cross-cluster data movement
//   - 2: target and shard-aware - implies level 1, plus indicates that archpaths are collocated in few shards;
//     enables additional optimization for archive handle reuse
//   E.g., use level 1 or 2 when input TARs were constructed to requested batches.
//
// - Returned size:
//   - when the requested file is found the corresponding MossOut.Size will be equal (the number of bytes in the respective TAR-ed payload)
//     - but when read range is defined: MossOut.Size = (length of this range)

const (
	MossMissingDir = "__404__"
)

const (
	MossMetaPart = "metadata"
	MossDataPart = "archive"
)

type ColocLevel uint8

const (
	ColocNone ColocLevel = iota // no optimization
	ColocOne                    // target-aware: when routing request, select DT (leader) to maximize number of entries per target
	ColocTwo                    // shard-aware: ColocOne + cache hot shards in memory
)

type (
	// swagger:model
	MossIn struct {
		ObjName string `json:"objname"`
		// optional fields
		Bucket   string `json:"bucket,omitempty"`   // if present, overrides cmn.Bck from the GetBatch request
		Provider string `json:"provider,omitempty"` // e.g. "s3", "ais", etc.
		Uname    string `json:"uname,omitempty"`    // per-object, fully qualified - defines the entire (bucket, provider, objname) triplet, and more
		ArchPath string `json:"archpath,omitempty"` // extract the specified file from an object ("shard") formatted as: .tar, .tgz or .tar.gz, .zip, .tar.lz4;
		Opaque   []byte `json:"opaque,omitempty"`   // user-provided identifier - e.g., to maintain one-to-many
		Start    int64  `json:"start,omitempty"`
		Length   int64  `json:"length,omitempty"`
	}
	// swagger:model
	MossReq struct {
		OutputFormat  string     `json:"mime,omitempty"`  // enum { archive.ExtTar, archive.ExtTGZ, ... } from "cmn/archive/mime.go"; empty string defaults to TAR
		In            []MossIn   `json:"in"`              // of arbitrary size >= 1
		ContinueOnErr bool       `json:"coer,omitempty"`  // primary usage: ignore missing files and/or objects - include them under "__404__/" prefix and keep going
		OnlyObjName   bool       `json:"onob"`            // name-in-archive: default naming convention is <Bucket>/<ObjName>; set this flag to have <ObjName> only
		StreamingGet  bool       `json:"strm"`            // stream resulting archive prior to finalizing it in memory
		Colocation    ColocLevel `json:"coloc,omitempty"` // enum { 0=ColocNone, 1=ColocOne, 2=ColocTwo }
	}
	// swagger:model
	MossOut struct {
		ObjName  string `json:"objname"`            // same as the corresponding MossIn.ObjName
		ArchPath string `json:"archpath,omitempty"` // ditto
		Bucket   string `json:"bucket"`             // ditto
		Provider string `json:"provider"`           // ditto
		ErrMsg   string `json:"err_msg,omitempty"`  // e.g., when missing
		Opaque   []byte `json:"opaque,omitempty"`   // from the corresponding MossIn; multi-objname logic on the client side
		Size     int64  `json:"size"`
	}
	// swagger:model
	MossResp struct {
		UUID string    `json:"uuid"`
		Out  []MossOut `json:"out"`
	}
)

// in-archive naming [convention]:
// bucket/object skipping provider, uuid, and/or namespace
func (in *MossIn) NameInRespArch(bucket string, onlyObjName bool) string {
	switch {
	case onlyObjName:
		return in.ObjName
	case in.Uname != "":
		debug.Assert(false, "Uname must be handled by the caller to produce cmn.Bck (and then call this method with it)")
		return ""
	case in.Bucket == "":
		return bucket + "/" + in.ObjName
	default:
		return in.Bucket + "/" + in.ObjName
	}
}

// validate ArchPath and ObjName
func (in *MossIn) UnmarshalJSON(data []byte) error {
	type alias MossIn
	var tmp = (*alias)(in)
	if err := json.Unmarshal(data, tmp); err != nil {
		return err
	}

	if err := cos.ValidateOname(in.ObjName); err != nil {
		return err
	}
	if in.ArchPath != "" {
		if err := cos.ValidateArchpath(in.ArchPath); err != nil {
			return err
		}
	}
	return nil
}
