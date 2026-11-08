// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"net/http"
	"sync/atomic"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/fs"
)

// target-wide remote range readers: parallel cold GET (ais) and blob download (xact/xs)
// - cap on extra readers: available mountpaths times a per-media multiplier (see get_tunables)
// - backoff after remote 429/503; buckets with rate_limit.backend use the configured limiter
// - TODO: consider per-bucket granularity
// - TODO: consider in-flight protection under scheduler, CPU, memory, and disk pressure

type rreaders struct {
	backoffUntil atomic.Int64 // mono time: no extra range readers until then
	inflight     atomic.Int32 // extra range readers, target-wide
}

var RR rreaders

// cap on extra range readers
func (*rreaders) Limit() int32 {
	mult := int32(RRMultNVMe)
	if fs.IsRotational() || fs.IsSlow() {
		mult = RRMultHDD
	}
	return int32(fs.NumAvail()) * mult
}

// record remote 429/503 and return true
func (s *rreaders) Throttle(bprops *cmn.Bprops, config *cmn.Config, code int, err error) bool {
	if bprops.RateLimit.Backend.Enabled {
		return false
	}
	if code != http.StatusTooManyRequests && code != http.StatusServiceUnavailable && !cmn.IsErrTooManyRequests(err) {
		return false
	}
	until := mono.NanoTime() + int64(config.Timeout.ColdRangeBackoff())
	for prev := s.backoffUntil.Load(); until > prev; prev = s.backoffUntil.Load() {
		if s.backoffUntil.CompareAndSwap(prev, until) {
			break
		}
	}
	return true
}

func (s *rreaders) BackingOff(bprops *cmn.Bprops) bool {
	until := s.backoffUntil.Load()
	return until != 0 && !bprops.RateLimit.Backend.Enabled && mono.NanoTime() < until
}

// reserve up to `want` extra readers within `limit`; zero means none
func (s *rreaders) Reserve(want int, limit int32) int {
	for {
		cur := s.inflight.Load()
		n := min(int32(want), limit-cur)
		if n <= 0 {
			return 0
		}
		if s.inflight.CompareAndSwap(cur, cur+n) {
			return int(n)
		}
	}
}

func (s *rreaders) Release(n int) { s.inflight.Add(-int32(n)) }

func (s *rreaders) Inflight() int32 { return s.inflight.Load() }
