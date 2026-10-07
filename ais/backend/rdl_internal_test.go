// Package backend contains core/backend interface implementations for supported backend providers.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package backend

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/stats"
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
			d, ctx := newRdl(parent, nil, nil)
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

type rdlStats struct {
	stats.Tracker
	name  string
	vlabs map[string]string
	calls int
}

func (s *rdlStats) IncWith(name string, vlabs map[string]string) {
	s.calls++
	s.name, s.vlabs = name, vlabs
}

func trackTestRdl(d *rdl) *rdlStats {
	s := &rdlStats{}
	d.b = &base{tstats: s, metrics: cos.StrKVs{stats.GetTimeoutCount: "err.aws.get.timeout.n"}}
	d.bck = meta.NewBck("rdl-test", apc.AWS, cmn.NsGlobal)
	return s
}

func (s *rdlStats) check(t *testing.T, want int) {
	t.Helper()
	if s.calls != want {
		t.Fatalf("metric increments %d, want %d", s.calls, want)
	}
	if want > 0 && (s.name != "err.aws.get.timeout.n" || len(s.vlabs) != 1 || s.vlabs[stats.VlabBucket] != "s3://rdl-test") {
		t.Fatalf("unexpected metric %q, labels %v", s.name, s.vlabs)
	}
}

// non-divisible reads: overshoot carries over (renewal k at cumulative k*renewSize)
func TestRdlRenewal(t *testing.T) {
	const renewSize = 64 * cos.KiB
	tests := []struct {
		name  string
		reads []int
		want  int64 // residual
	}{
		{"divisible", []int{renewSize, renewSize}, 0},
		{"three-quarter-steps", []int{48 * cos.KiB, 48 * cos.KiB, 48 * cos.KiB, 48 * cos.KiB}, 0}, // 192KiB = 3 renewal sizes
		{"overshoot", []int{48 * cos.KiB, 48 * cos.KiB}, 32 * cos.KiB},                            // 96KiB: 32KiB carried
		{"multi-renewal", []int{renewSize*2 + renewSize/2}, renewSize / 2},                        // single read spanning 2.5 renewal sizes
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
	s := trackTestRdl(d)
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
		t.Fatal("expected cmn.ErrRemoteGetTimeout cause")
	}
	s.check(t, 0) // expiration alone is not a failed read
}

// expired before the body: typed error, 504, counted once
func TestRdlTimeoutBeforeBody(t *testing.T) {
	d := newTestRdl(10*time.Millisecond, 4*cos.KiB)
	s := trackTestRdl(d)
	<-d.ctx.Done()
	res := core.GetReaderResult{Err: context.Canceled, ErrCode: http.StatusInternalServerError}
	d.fini(&res)
	if !cmn.IsErrRemoteGetTimeout(res.Err) {
		t.Fatalf("expected cmn.ErrRemoteGetTimeout, got %v", res.Err)
	}
	if cos.IsErrClientTimeout(res.Err) {
		t.Fatalf("remote timeout misclassified as a client timeout: %v", res.Err)
	}
	if res.ErrCode != http.StatusGatewayTimeout {
		t.Fatalf("status %d, want %d", res.ErrCode, http.StatusGatewayTimeout)
	}
	if herr := cmn.NewErrHTTP(nil, res.Err, res.ErrCode); herr.TypeCode != "ErrRemoteGetTimeout" {
		t.Fatalf("wire type %q, want ErrRemoteGetTimeout", herr.TypeCode)
	}
	s.check(t, 1)
}

// expired mid-body: the reader's error is substituted (every time), counted once
func TestRdlTimeoutMidBody(t *testing.T) {
	d := newTestRdl(10*time.Millisecond, 4*cos.KiB)
	s := trackTestRdl(d)
	defer d.Close()
	<-d.ctx.Done()
	d.r = io.NopCloser(&errReader{context.Canceled})
	for range 2 {
		if _, err := d.Read(make([]byte, cos.KiB)); !cmn.IsErrRemoteGetTimeout(err) {
			t.Fatalf("expected cmn.ErrRemoteGetTimeout, got %v", err)
		}
	}
	s.check(t, 1)
}

// parent cancellation or timeout wins over later expiration: no 504, not counted
func TestRdlParentCancel(t *testing.T) {
	for _, cause := range []error{context.Canceled, context.DeadlineExceeded} {
		t.Run(cause.Error(), func(t *testing.T) {
			parent, cancel := context.WithCancelCause(context.Background())
			d := &rdl{tout: time.Hour, renewSize: 4 * cos.KiB}
			s := trackTestRdl(d)
			d.ctx, d.cancel = context.WithCancelCause(parent)
			d.timer = time.AfterFunc(d.tout, d.expire)
			cancel(cause)
			d.expire() // parent cause must still win

			res := core.GetReaderResult{Err: cause, ErrCode: http.StatusInternalServerError}
			d.fini(&res)
			if res.Err != cause || !errors.Is(context.Cause(d.ctx), cause) {
				t.Fatalf("expected %v pass-through, got %v (cause %v)", cause, res.Err, context.Cause(d.ctx))
			}
			if res.ErrCode != http.StatusInternalServerError || d.counted {
				t.Fatalf("status %d, counted %t: want unchanged", res.ErrCode, d.counted)
			}
			s.check(t, 0)
		})
	}
}

type errReader struct{ err error }

func (r *errReader) Read([]byte) (int, error) { return 0, r.err }
