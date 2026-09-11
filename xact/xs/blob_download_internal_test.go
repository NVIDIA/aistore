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
	"github.com/NVIDIA/aistore/cmn/load"
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

// NOTE: unit tests do not LoadConfig, so cmn.Rom.testingEnv is left false - which is
// what keeps the `load.High && streaming` rejection reachable here (and only here).
func TestBlobDlMemErr(t *testing.T) {
	tassert.Fatalf(t, !cmn.Rom.TestingEnv(), "expecting unset cmn.Rom: this test covers the non-testing-env branch")

	const (
		sglCost = 4 * cos.MiB
		bufCost = 32 * cos.KiB
	)
	tests := []struct {
		name      string
		memLoad   load.Load
		streaming bool
		wantErr   bool
	}{
		{"critical-streaming", load.Critical, true, true},
		{"critical-background", load.Critical, false, true},
		{"high-streaming", load.High, true, true},
		{"high-background", load.High, false, false},
		{"moderate-streaming", load.Moderate, true, false},
		{"low-streaming", load.Low, true, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := XactBlobDl{cname: "blob-dl[" + tc.name + "]"}
			err := r.memErr(tc.memLoad, tc.streaming, sglCost, bufCost)
			tassert.Fatalf(t, (err != nil) == tc.wantErr, "expected err=%t, got %v", tc.wantErr, err)
			if err != nil {
				tassert.Fatalf(t, IsErrBlobDlAdmission(err), "expected admission error, got %v", err)
			}
		})
	}
}
