// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"context"
	"io"
	"net/http"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core/meta"
)

type (
	GetReaderResult struct {
		R        io.ReadCloser // see cmn.CloseRemote and usage
		Err      error
		ExpCksum *cos.Cksum
		Pin      ObjPin // full GET: response revision matching the returned metadata; zero when unavailable
		Size     int64
		ErrCode  int
	}

	// remote object version to match when reading a range
	// - each backend selects which field to condition on
	// - both empty: backend has nothing to pin against
	ObjPin struct {
		Version string // S3 VersionId, GCP generation
		ETag    string // unquoted
	}

	Backend interface {
		Provider() string
		MetricName(string) string
		CondWrite(bck *meta.Bck) bool // forward If-Match/If-None-Match to the remote store and fail on mismatch

		CreateBucket(bck *meta.Bck) (ecode int, err error)
		ListBuckets(qbck cmn.QueryBcks) (bcks cmn.Bcks, ecode int, err error)

		// list-objects: Cloud implementation
		// - set and return lst.ContinuationToken on every successful call
		// - return empty lst.ContinuationToken when the listing is exhausted
		// - callers may reuse lst
		// - callers do not need to worry about setting lst.ContinuationToken to ""
		//
		// (backend/ais satisfies this contract by copying the complete remote result)
		ListObjects(ctx context.Context, bck *meta.Bck, msg *apc.LsoMsg, lst *cmn.LsoRes) (ecode int, err error)

		PutObj(ctx context.Context, r io.ReadCloser, lom *LOM, origReq *http.Request) (ecode int, err error)
		DeleteObj(ctx context.Context, lom *LOM) (ecode int, err error)

		// head
		HeadBucket(ctx context.Context, bck *meta.Bck) (bckProps cos.StrKVs, ecode int, err error)
		HeadObj(ctx context.Context, lom *LOM, origReq *http.Request) (objAttrs *cmn.ObjAttrs, ecode int, err error)

		// get (exclusively via GetCold; calls GetObjReader)
		GetObj(ctx context.Context, lom *LOM, owt cmn.OWT, origReq *http.Request) (ecode int, err error)
		// get (jobs; REST)
		// - offset, length: range read when length > 0, the entire object otherwise
		// - pin: required for range reads (exactly one; see RangePin), not used otherwise
		GetObjReader(ctx context.Context, lom *LOM, offset, length int64, pin ...*ObjPin) GetReaderResult

		// Multipart operations accept context independently of optional request headers.
		// A nil context defaults to context.Background().
		StartMpt(ctx context.Context, lom *LOM, r *http.Request) (uploadID string, ecode int, err error)
		PutMptPart(ctx context.Context, lom *LOM, reader cos.ReadOpenCloser, r *http.Request, uploadID string, size int64, partNum int32) (etag string, ecode int, err error)
		CompleteMpt(ctx context.Context, lom *LOM, r *http.Request, uploadID string, body []byte, parts apc.MptCompletedParts) (version, etag string, ecode int, err error)
		AbortMpt(ctx context.Context, lom *LOM, r *http.Request, uploadID string) (ecode int, err error)
	}
)

// build pin from remote object attributes; return nil if there's nothing to pin against
// - version: provider-specific custom key; remote AIS: oa.Ver
func NewObjPin(provider string, oa *cmn.ObjAttrs) *ObjPin {
	var p ObjPin
	p.Init(provider, oa)
	if p.Version == "" && p.ETag == "" {
		return nil
	}
	return &p
}

// snapshot version metadata without allocating
func (p *ObjPin) Init(provider string, oa *cmn.ObjAttrs) {
	p.Version, _ = oa.GetCustomKey(cmn.VersionObjMD)
	p.ETag, _ = oa.GetCustomKey(cmn.ETag)
	if provider == apc.AIS && oa.Version() != "" {
		p.Version = oa.Version() // native revision takes precedence over custom metadata
	}
}

// return the pin of a range read
// - range reads must pass exactly one pin (nil when there's nothing to pin against)
func RangePin(pin []*ObjPin) *ObjPin {
	debug.Assert(len(pin) == 1, "range read requires exactly one pin, have ", len(pin))
	if len(pin) == 0 {
		return nil
	}
	return pin[0]
}

func (p *ObjPin) String() string {
	switch {
	case p.Version != "" && p.ETag != "":
		return "[version=" + p.Version + " etag=" + p.ETag + "]"
	case p.Version != "":
		return "[version=" + p.Version + "]"
	default:
		return "[etag=" + p.ETag + "]"
	}
}
