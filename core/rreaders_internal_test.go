// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"errors"
	"net/http"
	"testing"

	"github.com/NVIDIA/aistore/cmn"
)

func TestRangeReadersReserve(t *testing.T) {
	var s rreaders
	if n := s.Reserve(3, 4); n != 3 {
		t.Fatalf("reserve: got %d, want 3", n)
	}
	if n := s.Reserve(3, 4); n != 1 {
		t.Fatalf("reserve near the cap: got %d, want 1", n)
	}
	if n := s.Reserve(3, 4); n != 0 {
		t.Fatalf("reserve at the cap: got %d, want 0", n)
	}
	if got := s.Inflight(); got != 4 {
		t.Fatalf("in-flight: got %d, want 4", got)
	}
	if n := s.Reserve(1, 2); n != 0 {
		t.Fatalf("reserve against a lower cap: got %d, want 0", n)
	}
	s.Release(4)
	if got := s.Inflight(); got != 0 {
		t.Fatalf("in-flight after release: got %d, want 0", got)
	}
}

func TestRangeReadersThrottle(t *testing.T) {
	var (
		s       rreaders
		config  = &cmn.Config{}
		plain   = &cmn.Bprops{}
		limited = &cmn.Bprops{}
		errx    = errors.New("remote error")
	)
	limited.RateLimit.Backend.Enabled = true

	if s.Throttle(plain, config, http.StatusInternalServerError, errx) || s.BackingOff(plain) {
		t.Fatal("non-throttling status must not start backoff")
	}
	if s.Throttle(limited, config, http.StatusTooManyRequests, errx) || s.BackingOff(plain) {
		t.Fatal("bucket with rate_limit.backend must not start backoff")
	}
	if !s.Throttle(plain, config, http.StatusServiceUnavailable, errx) {
		t.Fatal("503 must start backoff")
	}
	if !s.BackingOff(plain) {
		t.Fatal("expected backoff")
	}
	if s.BackingOff(limited) {
		t.Fatal("bucket with rate_limit.backend is not subject to backoff")
	}
	if !s.Throttle(plain, config, 0, cmn.NewErrTooManyRequests(errx, http.StatusTooManyRequests)) {
		t.Fatal("ErrTooManyRequests must start backoff")
	}
}
