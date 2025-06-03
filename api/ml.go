// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"io"
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// Definitions
// ===========
// - when GetBatch() returns an error all the other returned data and/or metadata (if any) can be ignored and dropped
// - when GetBatch() succeeds (err == nil):
//    - user should expect the same exact number of entries in the MossResp and the same number of files in the resulting TAR
//    - the order: entries in the response (MossResp, TAR) are ordered in **precisely** the same order as in the MossReq
// - TAR is not the only supported output format - compression is supported in a variety of ways (see `OutputFormat`)
//    - here and elsewhere, "TAR" is used for convenience (to keep this text readable)
// - File Naming in the resulting TAR:
//    - existing files' names: Bucket/ObjName
//    - when ContinueOnErr == true: missing files will not result in the GetBatch() failure
//      - all missing files will carry "__404__/" prefix
//        - and yes, if you extract entire TAR you will find them all in one place
//      - each missing file will have zero length and the name: "__404__/Bucket/ObjName",
// - Returned Size:
//   - when the requested file is found the corresponding MossOut.Size = (the number of bytes in the respective TAR-ed payload)
//     - with read range defined: MossOut.Size = (length of this range)

const (
	MissingFilesDirectory = "__404__"
)

const (
	MossMultipartPrefix = "multipart/" // must match response Content-Type prefix
	MossBoundaryParam   = "boundary"   // Content-Type param expected by multipart.Reader
)

type (
	MossIn struct {
		ObjName string `json:"objname"`
		// optional fields
		Opaque   []byte `json:"opaque,omitempty"`   // user-provided identifier - e.g., to maintain one-to-many
		Bucket   string `json:"bucket,omitempty"`   // if present, overrides cmn.Bck from the GetBatch request
		Provider string `json:"provider,omitempty"` // e.g. "s3", "ais", etc.
		Uname    string `json:"uname,omitempty"`    // per-object, fully qualified - defines the entire (bucket, provider, objname) triplet, and more
		Start    int64  `json:"start,omitempty"`
		Length   int64  `json:"length,omitempty"`
	}
	MossReq struct {
		In            []MossIn `json:"in"`             // of arbitrary size >= 1
		OutputFormat  string   `json:"mime,omitempty"` // enum { archive.ExtTar, archive.ExtTGZ, ... } from "cmn/archive/mime.go"; empty string defaults to TAR
		ContinueOnErr bool     `json:"coer,omitempty"` // e.g. usage: ignore missing files - include them under __404__/ and keep going
		// and maybe more TBD tunables ...
	}
	MossOut struct {
		ObjName  string `json:"objname"`          // same as the corresponding MossIn.ObjName
		Bucket   string `json:"bucket"`           // ditto
		Provider string `json:"provider"`         // ditto
		Opaque   string `json:"opaque,omitempty"` // from the corresponding MossIn; multi-objname logic on the client side
		ErrMsg   string `json:"err_msg,omitempty"`
		Size     int64  `json:"size"`
	}
	MossResp struct {
		Out  []MossOut `json:"out"`
		UUID string    `json:"uuid"`
	}
)

func GetBatch(bp BaseParams, bck cmn.Bck, req *MossReq, w io.Writer) (resp MossResp, err error) {
	bp.Method = http.MethodGet
	q := qalloc()
	bck.SetQuery(q)
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = apc.URLPathML.Join(apc.Moss, bck.Name)
		reqParams.Body = cos.MustMarshal(req)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	_, err = reqParams.readMultipart(&resp, w)
	FreeRp(reqParams)
	qfree(q)
	return resp, err
}
