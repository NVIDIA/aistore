// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
)

type base struct {
	provider string
}

func (b *base) Provider() string { return b.provider }

func (b *base) CreateBucket(_ *meta.Bck) (int, error) {
	return http.StatusNotImplemented, cmn.NewErrNotImpl("create", b.provider+" bucket")
}

func (b *base) ListObjectsInv(*meta.Bck, *apc.LsoMsg, *cmn.LsoRes, *core.LsoInvCtx) (int, error) {
	return 0, cmn.NewErrNotImpl("list "+b.provider+" backend objects via", "bucket inventory")
}

//
// common helpers and misc
//

func fmtTime(t time.Time) string { return t.Format(time.RFC3339) }

func calcPageSize(pageSize, maxPageSize int64) int64 {
	debug.Assert(pageSize >= 0, pageSize)
	if pageSize == 0 {
		return maxPageSize
	}
	return min(pageSize, maxPageSize)
}

//nolint:deadcode,unused // used by dummy backends
func newErrInitBackend(provider string) error { return &cmn.ErrInitBackend{Provider: provider} }

func allocPutParams(res core.GetReaderResult, owt cmn.OWT) *core.PutParams {
	params := core.AllocPutParams()
	{
		params.WorkTag = fs.WorkfileColdget
		params.Reader = res.R
		params.OWT = owt
		params.Cksum = res.ExpCksum
		params.Size = res.Size
		params.Atime = time.Now()
		params.ColdGET = true
	}
	return params
}
