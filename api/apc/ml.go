// Package apc: API control messages and constants
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
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

type (
	// @Description Input specification for GetBatch operation, defining object location, range, and archive extraction parameters
	MossIn struct {
		ObjName string `json:"objname"`
		// optional fields
		Opaque   []byte `json:"opaque,omitempty"`   // user-provided identifier - e.g., to maintain one-to-many
		Bucket   string `json:"bucket,omitempty"`   // if present, overrides cmn.Bck from the GetBatch request
		Provider string `json:"provider,omitempty"` // e.g. "s3", "ais", etc.
		Uname    string `json:"uname,omitempty"`    // per-object, fully qualified - defines the entire (bucket, provider, objname) triplet, and more
		ArchPath string `json:"archpath,omitempty"` // extract the specified file from an object ("shard") formatted as: .tar, .tgz or .tar.gz, .zip, .tar.lz4;
		Start    int64  `json:"start,omitempty"`
		Length   int64  `json:"length,omitempty"`
	}
	// @Description Request message for GetBatch operation, specifying multiple objects to retrieve and output format options
	MossReq struct {
		In            []MossIn `json:"in"`             // of arbitrary size >= 1
		OutputFormat  string   `json:"mime,omitempty"` // enum { archive.ExtTar, archive.ExtTGZ, ... } from "cmn/archive/mime.go"; empty string defaults to TAR
		ContinueOnErr bool     `json:"coer,omitempty"` // primary usage: ignore missing files and/or objects - include them under "__404__/" prefix and keep going
		OnlyObjName   bool     `json:"onob"`           // name-in-archive: default naming convention is <Bucket>/<ObjName>; set this flag to have <ObjName> only
		StreamingGet  bool     `json:"strm"`           // stream resulting archive prior to finalizing it in memory
	}
	// @Description Output information for a single object in GetBatch response, including metadata and error details
	MossOut struct {
		ObjName  string `json:"objname"`            // same as the corresponding MossIn.ObjName
		ArchPath string `json:"archpath,omitempty"` // ditto
		Bucket   string `json:"bucket"`             // ditto
		Provider string `json:"provider"`           // ditto
		Opaque   []byte `json:"opaque,omitempty"`   // from the corresponding MossIn; multi-objname logic on the client side
		ErrMsg   string `json:"err_msg,omitempty"`
		Size     int64  `json:"size"`
	}
	// @Description Response message for GetBatch operation containing results for all requested objects
	MossResp struct {
		Out  []MossOut `json:"out"`
		UUID string    `json:"uuid"`
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
