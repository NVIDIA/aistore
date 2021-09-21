// Package backend contains implementation of various backend providers.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
)

var verbose bool

func Init() {
	verbose = bool(glog.FastV(4, glog.SmoduleBackend))
}

func creatingBucketNotSupportedErr(provider string) (errCode int, err error) {
	return http.StatusBadRequest, fmt.Errorf(cmn.FmtErrUnsupported, provider, "creating bucket")
}

func wrapReader(ctx context.Context, r io.ReadCloser) io.ReadCloser {
	if v := ctx.Value(cmn.CtxReadWrapper); v != nil {
		return v.(cmn.ReadWrapperFunc)(r)
	}
	return r
}

func setSize(ctx context.Context, size int64) {
	if v := ctx.Value(cmn.CtxSetSize); v != nil {
		v.(cmn.SetSizeFunc)(size)
	}
}

func calcPageSize(pageSize, maxPageSize uint) uint {
	if pageSize == 0 {
		return maxPageSize
	}
	return pageSize
}

// nolint:deadcode,unused // It is used but in `*_mock.go` files.
func newErrInitBackend(provider string) error { return &cmn.ErrInitBackend{Provider: provider} }
