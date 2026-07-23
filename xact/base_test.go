// Package xact_test tests the xaction base lifecycle.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xact_test

import (
	"context"
	"errors"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
)

func TestBaseContextAbort(t *testing.T) {
	var base xact.Base
	base.InitBase(cos.GenUUID(), apc.ActSummaryBck, nil)

	abortErr := errors.New("test abort")
	tassert.Fatalf(t, base.Abort(abortErr), "failed to abort xaction")
	tassert.Fatalf(t, errors.Is(base.Context().Err(), context.Canceled),
		"expected canceled context, got %v", base.Context().Err())
	tassert.Fatalf(t, errors.Is(base.AbortErr(), abortErr),
		"expected abort error %v, got %v", abortErr, base.AbortErr())
}

func TestBaseContextFinish(t *testing.T) {
	xreg.Init()
	var base xact.Base
	base.InitBase(cos.GenUUID(), apc.ActSummaryBck, nil)

	tassert.Fatalf(t, base.Context().Err() == nil,
		"expected live context, got %v", base.Context().Err())
	base.Finish()
	tassert.Fatalf(t, errors.Is(base.Context().Err(), context.Canceled),
		"expected canceled context, got %v", base.Context().Err())
	tassert.Fatalf(t, base.AbortErr() == nil,
		"expected no abort error, got %v", base.AbortErr())
}
