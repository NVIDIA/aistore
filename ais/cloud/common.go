// Package cloud contains implementation of various cloud providers.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cloud

import (
	"context"
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
