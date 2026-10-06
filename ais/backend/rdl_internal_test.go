// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"context"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
)

func TestRdlInit(t *testing.T) {
	defer cmn.Rom.Set(&cmn.GCO.Get().ClusterConfig)

	tests := []struct {
		name     string
		sendfile time.Duration
		features feat.Flags
		enabled  bool
	}{
		{"enabled", 5 * time.Minute, 0, true},
		{"opt-out", 5 * time.Minute, feat.DisableGetDeadline, false},
		{"zero-timeout", 0, 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := cmn.GCO.Get().ClusterConfig
			cfg.Timeout.SendFile = cos.Duration(tc.sendfile)
			cfg.Features = tc.features
			cmn.Rom.Set(&cfg)

			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			d, ctx := newRdl(parent)
			if d != nil {
				defer d.timer.Stop()
				defer d.cancel(nil)
			}
			if enabled := d != nil; enabled != tc.enabled {
				t.Fatalf("enabled=%t, want %t", enabled, tc.enabled)
			}
			if !tc.enabled {
				if ctx != parent {
					t.Fatal("disabled deadline must preserve the original context")
				}
				return
			}
			if ctx != d.ctx || ctx == parent || d.tout != tc.sendfile || d.csize != cmn.XferChunkSize(tc.sendfile) {
				t.Fatal("enabled deadline must derive a context and use the configured timeout and quantum")
			}
		})
	}
}
