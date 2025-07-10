// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
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

func GetBatch(bp BaseParams, bck cmn.Bck, req *apc.MossReq, w io.Writer) (resp apc.MossResp, err error) {
	bp.Method = http.MethodGet

	q, path := _optionalBucket(&bck)
	reqParams := AllocRp()
	{
		reqParams.BaseParams = bp
		reqParams.Path = path
		reqParams.Body = cos.MustMarshal(req)
		reqParams.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
		reqParams.Query = q
	}
	if req.StreamingGet {
		var wresp *wrappedResp
		wresp, err = reqParams.doWriter(w)
		if err == nil {
			ct := wresp.Header.Get(cos.HdrContentType)
			if !strings.HasPrefix(ct, "application/") {
				err = fmt.Errorf("unexpected Content-Type %q", ct)
			}
		}
	} else {
		_, err = reqParams.readMultipart(&resp, w)
	}
	FreeRp(reqParams)
	if q != nil {
		qfree(q)
	}
	return resp, err
}

func _optionalBucket(bck *cmn.Bck) (q url.Values, path string) {
	if bck.IsEmpty() {
		path = apc.URLPathML.Join(apc.Moss)
		return
	}
	q = qalloc()
	bck.SetQuery(q)
	path = apc.URLPathML.Join(apc.Moss, bck.Name)
	return
}
