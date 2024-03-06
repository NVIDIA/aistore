// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/fs"
)

type ctx = context.Context // used when omitted for shortness sake

func fmtTime(t time.Time) string { return t.Format(time.RFC3339) }

func calcPageSize(pageSize, maxPageSize uint) uint {
	debug.Assert(int(pageSize) >= 0, pageSize)
	if pageSize == 0 {
		return maxPageSize
	}
	return min(pageSize, maxPageSize)
}

func newErrInventory(provider string) error {
	return cmn.NewErrNotImpl("list "+provider+" backend objects via", "bucket inventory")
}

//nolint:deadcode,unused // It is used but in `*_mock.go` files.
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
