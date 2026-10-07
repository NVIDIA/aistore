// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"bytes"
	"context"
	"io"
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
			if ctx != d.ctx || ctx == parent || d.tout != tc.sendfile || d.renewSize != cmn.XferRenewSize(tc.sendfile) {
				t.Fatal("enabled deadline must derive a context and use the configured window and renewal size")
			}
		})
	}
}

// test rdl w/ explicit window and renewal size (config-validated send_file_time is >= 1m)
func newTestRdl(tout time.Duration, renewSize int64) *rdl {
	d := &rdl{tout: tout, renewSize: renewSize}
	d.ctx, d.cancel = context.WithCancelCause(context.Background())
	d.timer = time.AfterFunc(tout, d.expire)
	return d
}

// non-divisible reads: overshoot carries over (renewal k at cumulative k*renewSize)
func TestRdlQuantum(t *testing.T) {
	const renewSize = 64 * cos.KiB
	tests := []struct {
		name  string
		reads []int
		want  int64 // residual
	}{
		{"divisible", []int{renewSize, renewSize}, 0},
		{"three-quarter-steps", []int{48 * cos.KiB, 48 * cos.KiB, 48 * cos.KiB, 48 * cos.KiB}, 0}, // 192KiB = 3 renewal sizes
		{"overshoot", []int{48 * cos.KiB, 48 * cos.KiB}, 32 * cos.KiB},                            // 96KiB: 32KiB carried
		{"multi-quantum", []int{renewSize*2 + renewSize/2}, renewSize / 2},                        // single read spanning 2.5 renewal sizes
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := newTestRdl(time.Hour, renewSize)
			defer d.Close()
			var total int
			for _, n := range tc.reads {
				total += n
			}
			d.r = io.NopCloser(bytes.NewReader(make([]byte, total)))
			for _, n := range tc.reads {
				if got, err := io.ReadFull(d, make([]byte, n)); got != n || err != nil {
					t.Fatalf("read %d: got %d, err %v", n, got, err)
				}
			}
			if d.n != tc.want {
				t.Fatalf("residual %d, want %d", d.n, tc.want)
			}
		})
	}
}

// crossing a renewal boundary after expiry must not re-arm the timer
func TestRdlNoRenewAfterExpiry(t *testing.T) {
	const renewSize = 4 * cos.KiB
	d := newTestRdl(10*time.Millisecond, renewSize)
	defer d.Close()
	<-d.ctx.Done()
	time.Sleep(10 * time.Millisecond)                            // (expire has returned)
	d.r = io.NopCloser(bytes.NewReader(make([]byte, renewSize))) // buffered data, delivered despite cancel
	if _, err := io.ReadFull(d, make([]byte, renewSize)); err != nil {
		t.Fatal(err)
	}
	if d.timer.Stop() {
		t.Fatal("timer re-armed after expiry")
	}
	if d.expired() == nil {
		t.Fatal("expected errReadTimeout cause")
	}
}
