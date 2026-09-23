// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"net/http"
	"strconv"

	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
)

// rsphdr: the one place where object attributes get serialized into a _response_ header
// (GET, PUT, and S3 HEAD; native HEAD: see _opToHeader and _objHeadV2)
//
// two parts:
// - standard (common): Content-Type, Content-Length, ETag (always quoted)
// - s3 vs native API:
//   - native: "ais-" prefixed attributes via cmn.ToHeader; ETag: stored (custom) only - no syscalls on the native data path
//   - S3:     Last-Modified, ETag (lom.ETag), x-amz-version-id, x-amz-meta-* via s3.SetS3Headers
//
// callers set exactly one rsphdr per response

type rsphdr struct {
	hdr   http.Header
	lom   *core.LOM
	oa    *cmn.ObjAttrs // nil: lom; otherwise (blob-dl) cold attrs - stored only, no syscalls
	cksum *cos.Cksum    // native only: range or archived-file checksum; nil: oa.Checksum()
	size  int64         // Content-Length; negative: omit
	s3    bool          // S3 dialect
	ctype bool          // Content-Type: responses that carry (or describe) object content
}

func (a *rsphdr) set() {
	var (
		oa         = a.oa
		src s3.OAH = a.oa
	)
	if oa == nil {
		oa, src = a.lom.ObjAttrs(), a.lom
	}

	// 1. standard
	if a.ctype {
		oa.ContentTypeToHeader(a.hdr) // stored or cos.ContentBinary
	}
	if a.size >= 0 {
		a.hdr.Set(cos.HdrContentLength, strconv.FormatInt(a.size, 10))
	}

	// 2. dialect
	if a.s3 {
		s3.SetS3Headers(a.hdr, src)
		return
	}
	cksum := a.cksum
	if cksum == nil {
		cksum = oa.Checksum()
	}
	cmn.ToHeader(oa, a.hdr, cksum)
	cmn.ETagToHeader(oa, a.hdr)
}
