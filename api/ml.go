// Package api provides native Go-based API/SDK over HTTP(S).
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// -----------------------------------------------------------------------
// For background and usage, please refer to:
// 1. GetBatch: Multi-Object Retrieval API, at https://github.com/NVIDIA/aistore/blob/main/docs/get_batch.md
// 2. Monitoring GetBatch Performance,      at https://github.com/NVIDIA/aistore/blob/main/docs/monitoring-get-batch.md
// -----------------------------------------------------------------------

// Definitions
// ===========
// - GetBatch() supports reading (and returning in a single TAR) - multiple objects, multiple archived (sharded) files,
//   or any mix of thereof
// - TAR is not the only supported output format - compression is supported in a variety of ways (see `OutputFormat`)
//    - here and elsewhere, "TAR" is used strictly for reading convenience
// - When GetBatch() returns an error all the other returned data and/or metadata (if any) can be ignored and dropped
// - When GetBatch() succeeds (err == nil):
//    - user should expect the same exact number of entries in the MossResp and the same number of files in the resulting TAR
//    - the order: entries in the response (MossResp, TAR) are ordered in **precisely** the same order as in the MossReq
// - Naming convention:
//    - by default, files in the resulting TAR are named <Bucket>/<ObjName>
//    - `OnlyObjName` further controls the naming:
//       - when set:            archived (sharded) files are named as <ObjName>/<archpath> in the resulting TAR
//       - otherwise (default): archived files are named as <Bucket>/<ObjName>/<archpath>
//    The third variant (just <archpath>) is not currently supported to avoid ambiguity.
// - When (and only if) `ContinueOnErr` is set true:
//   - missing files will not result in GetBatch() failure
//   - missing files will be present in the resulting TAR
//     - with "__404__/" prefix and zero size
//     - if you extract resulting TAR you'll find them all in one place: under __404__/<Bucket>/<ObjName>
// - Colocation hint:
//   - 0 (default): no optimization - suitable for uniformly distributed data lakes where objects are spread
//     evenly across all targets
//   - 1: target-aware - indicates that objects in this batch are collocated on few targets;
//     proxy will compute HRW distribution and select the optimal distributed target (DT) to minimize
//     cross-cluster data movement
//   - 2: target and shard-aware - implies level 1, plus indicates that archpaths are collocated in few shards;
//     enables additional optimization for archive handle reuse
//   E.g., use level 1 or 2 when input TARs were constructed to requested batches.
// - Returned size:
//   - when the requested file is found the corresponding MossOut.Size will be equal (the number of bytes in the respective TAR-ed payload)
//     - but when read range is defined: MossOut.Size = (length of this range)

func GetBatch(bp BaseParams, bck cmn.Bck, req *apc.MossReq, w io.Writer) (resp apc.MossResp, err error) {
	rp, q, e := _makeMossReq(bp, bck, req)
	if e != nil {
		return resp, e
	}
	if req.StreamingGet {
		var wresp *wrappedResp
		wresp, err = rp.doWriter(w)
		if err == nil {
			err = _checkMossResp(wresp.Header)
		}
	} else {
		_, err = rp.readMultipart(&resp, w)
	}
	FreeRp(rp)
	if q != nil {
		qfree(q)
	}
	return resp, err
}

// GetBatchStream starts a streaming GetBatch and returns the response body _as is_
// and response headers:
// - the returned body is forward-only (non-seekable)
// - supported streaming formats: .tar/.tgz/.tar.lz4; zip is excepted as non-streamable
// - it is the caller's responsibility to close the body
// - compare with GetBatch() above

const zipext = ".zip"

func GetBatchStream(bp BaseParams, bck cmn.Bck, req *apc.MossReq) (io.ReadCloser, http.Header, error) {
	if !req.StreamingGet {
		return nil, nil, errors.New("GetBatchStream: expecting req.StreamingGet to be set")
	}
	if req.OutputFormat != "" {
		of := strings.ToLower(req.OutputFormat)
		if of == zipext || strings.Contains(of, zipext[1:]) {
			return nil, nil, fmt.Errorf("GetBatchStream: output format %q is not streamable", req.OutputFormat)
		}
	}
	rp, q, e := _makeMossReq(bp, bck, req)
	if e != nil {
		return nil, nil, e
	}

	wresp, body, err := rp.doStream()
	FreeRp(rp)
	if q != nil {
		qfree(q)
	}
	if err != nil {
		return nil, nil, err
	}
	if err := _checkMossResp(wresp.Header); err != nil {
		body.Close()
		return nil, nil, err
	}
	return body, wresp.Header, nil
}

//
// misc. helpers
//

// not a very strict check for: application/x-tar | application/gzip (tgz) | application/x-gtar | ...
func _checkMossResp(hdr http.Header) error {
	ct := hdr.Get(cos.HdrContentType)
	if !strings.HasPrefix(ct, "application/") {
		return fmt.Errorf("unexpected Content-Type %q", ct)
	}
	return nil
}

func _makeMossReq(bp BaseParams, bck cmn.Bck, req *apc.MossReq) (*ReqParams, url.Values, error) {
	bp.Method = http.MethodGet

	// - set path with the specified default `bck` unless empty
	// - make sure every entry in the request has bucket defined explicitly
	// - see related  _bucket() in x-moss
	q, path, err := _bucket(&bck, req)
	if err != nil {
		return nil, nil, err
	}

	rp := AllocRp()
	rp.BaseParams = bp
	rp.Path = path
	rp.Body = cos.MustMarshal(req)
	rp.Header = http.Header{cos.HdrContentType: []string{cos.ContentJSON}}
	rp.Query = q
	if req.Colocation > apc.ColocNone {
		q.Set(apc.QparamColoc, strconv.Itoa(int(req.Colocation)))
	}

	return rp, q, nil
}

func _bucket(bck *cmn.Bck, req *apc.MossReq) (q url.Values, path string, err error) {
	nodflt := bck.IsEmpty()

	// _may_ mutate user's apc.MossReq
	for i := range req.In {
		in := &req.In[i]
		if in.Uname == "" && in.Bucket == "" {
			if nodflt {
				return nil, "", fmt.Errorf("missing bucket specification for (name:%s idx:%d)", in.ObjName, i)
			}
			in.Bucket = bck.Name
			in.Provider = bck.Provider
		}
	}

	if nodflt {
		path = apc.URLPathML.Join(apc.Moss)
		return nil, path, nil
	}
	q = qalloc()
	bck.SetQuery(q)
	path = apc.URLPathML.Join(apc.Moss, bck.Name)
	return q, path, nil
}
