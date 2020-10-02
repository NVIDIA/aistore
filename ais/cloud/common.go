// Package cloud contains implementation of various cloud providers.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cloud

import (
	"context"
	"fmt"
	"io"

	"github.com/NVIDIA/aistore/cmn"
)

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
func newInitCloudErr(provider string) error {
	return fmt.Errorf(
		"tried to initialize %q cloud (specified in config) but the binary was not built with %q build tag",
		provider, provider,
	)
}
