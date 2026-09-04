// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestBlobDlChunkReadTimeout(t *testing.T) {
	tests := []struct {
		name                   string
		requested, limit, want time.Duration
	}{
		{"default", 0, 5 * time.Minute, 5 * time.Minute},
		{"explicit", 30 * time.Second, 5 * time.Minute, 30 * time.Second},
		{"explicit-capped", 10 * time.Minute, 5 * time.Minute, 5 * time.Minute},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			config := &cmn.Config{}
			config.Timeout.SendFile = cos.Duration(tc.limit)
			r := XactBlobDl{
				args:   &core.BlobParams{Msg: &apc.BlobMsg{ChunkReadTimeout: cos.Duration(tc.requested)}},
				config: config,
			}
			r.setChunkReadTimeout()
			tassert.Fatalf(t, r.timeout == tc.want, "expected %v, got %v", tc.want, r.timeout)
		})
	}
}
