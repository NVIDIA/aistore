// Package ais: internal unit tests
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"errors"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/xact/xs"
)

func TestBlobdlTermErr(t *testing.T) {
	var (
		runtimeErr = errors.New("runtime error")
		abortErr   = errors.New("abort error")
	)
	tests := []struct {
		name                 string
		runtimeErr, abortErr error
		want                 error
	}{
		{name: "success"},
		{name: "runtime", runtimeErr: runtimeErr, want: runtimeErr},
		{name: "abort", abortErr: abortErr, want: abortErr},
		{name: "abort-precedence", runtimeErr: runtimeErr, abortErr: abortErr, want: abortErr},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			xblob := &xs.XactBlobDl{}
			xblob.InitBase(context.Background(), cos.GenUUID(), apc.ActBlobDl, nil)
			if tc.runtimeErr != nil {
				xblob.AddErr(tc.runtimeErr)
			}
			if tc.abortErr != nil {
				tassert.Fatalf(t, xblob.Abort(tc.abortErr), "failed to abort blob download")
			}
			xblob.Finish()

			got := blobdlTermErr(xblob)
			if tc.want == nil {
				tassert.Fatalf(t, got == nil, "expected no error, got %v", got)
				return
			}
			tassert.Fatalf(t, errors.Is(got, tc.want), "expected %v, got %v", tc.want, got)
			if tc.runtimeErr != nil && tc.abortErr != nil {
				tassert.Fatalf(t, !errors.Is(got, tc.runtimeErr), "runtime error must not override abort")
			}
		})
	}
}
