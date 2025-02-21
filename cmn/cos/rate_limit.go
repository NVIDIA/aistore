// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn/mono"
)

const (
	dfltMinBtwn = 10 * time.Millisecond
	dfltMinIval = time.Second
	dfltMaxIval = 10 * time.Minute
	// adaptive
	dfltRetries = 5
)

const (
	rltag  = "rate-limiter"
	arltag = "adaptive-rate-limiter"
)

const (
	erateLow    = 0.1
	erateMedium = 0.3
	erateHigh   = 0.5
)

type (
	RateLim struct {
		tokens    float64       // current tokens
		maxTokens float64       // max tokens
		tokenIval float64       // duration in nanoseconds as in: maxTokens per tokenIval
		minBtwn   time.Duration // min duration since the previous granted; >= 1ms (above)
		// 'last' timestamps
		tsb struct {
			granted int64
			refill  int64
		}
		mu sync.Mutex
	}
	AdaptRateLim struct {
		RateLim
		origTokens int
		retries    int
		stats      struct {
			nerr, n, perr int
		}
	}
)

/////////////
// RateLim //
/////////////

func NewRateLim(maxTokens int, tokenIval time.Duration) (*RateLim, error) {
	rl := &RateLim{}
	return rl, rl.init(rltag, maxTokens, tokenIval)
}

func (rl *RateLim) init(tag string, maxTokens int, tokenIval time.Duration) error {
	if tokenIval < dfltMinIval || tokenIval > dfltMaxIval {
		return fmt.Errorf("%s: invalid token interval %v (min=%v, max=%v)", tag, tokenIval, dfltMinIval, dfltMaxIval)
	}
	if maxTokens <= 0 || maxTokens >= math.MaxInt32 {
		return fmt.Errorf("%s: invalid number of tokens %d per (token) interval", tag, maxTokens)
	}
	{
		rl.maxTokens = float64(maxTokens)
		rl.tokenIval = float64(tokenIval)
		rl.minBtwn = rl.recompute()
	}
	return nil
}

// recompute minBtwn
func (rl *RateLim) recompute() time.Duration {
	return max(time.Duration(rl.tokenIval/rl.maxTokens), dfltMinBtwn)
}

func (rl *RateLim) TryAcquire() (ok bool) {
	rl.mu.Lock()
	ok = rl.acquire()
	rl.mu.Unlock()
	return ok
}

func (rl *RateLim) acquire() bool {
	now := mono.NanoTime()
	elapsed := time.Duration(now - rl.tsb.granted)
	if elapsed < rl.minBtwn>>1 {
		return false
	}

	// replenish
	elapsed = time.Duration(now - rl.tsb.refill)
	if pct := float64(elapsed) / rl.tokenIval; pct > 0 {
		rl.tsb.refill = now
		if pct >= 1 {
			rl.tokens = rl.maxTokens
		} else {
			rl.tokens = min(rl.tokens+rl.maxTokens*pct, rl.maxTokens)
		}
	}

	if rl.tokens < 1 {
		return false
	}
	rl.tokens--
	rl.tsb.granted = now
	return true
}

//////////////////
// AdaptRateLim //
//////////////////

func NewAdaptRateLim(maxTokens, retries int, tokenIval time.Duration) (*AdaptRateLim, error) {
	if retries < 0 || retries > 10 {
		return nil, fmt.Errorf("%s: invalid number of retries %d", arltag, retries)
	}
	arl := &AdaptRateLim{
		origTokens: maxTokens,
		retries:    NonZero(retries, dfltRetries),
	}
	return arl, arl.RateLim.init(arltag, maxTokens, tokenIval)
}

func (arl *AdaptRateLim) Acquire() error {
	var sleep time.Duration
	for i := 0; ; i++ {
		arl.mu.Lock()
		ok := arl.acquire()
		if ok {
			arl.stats.n++
			if arl.stats.n >= arl.origTokens {
				arl.recompute()
			}
			arl.mu.Unlock()
			return nil
		}
		if i >= arl.retries {
			arl.mu.Unlock()
			return fmt.Errorf("%s: failed to acquire (%d, %v)", arltag, i, sleep)
		}
		sleep = min(max(sleep+sleep>>1, arl.minBtwn), arl.minBtwn<<2)
		arl.mu.Unlock()
		time.Sleep(sleep)
	}
}

// backoff before retrying 429
func (arl *AdaptRateLim) OnErr() {
	var (
		sleep time.Duration
		erate float64
	)
	arl.mu.Lock()
	arl.stats.nerr++
	erate = float64(arl.stats.nerr) / float64(arl.stats.n)
	switch {
	case erate > erateHigh:
		sleep = arl.minBtwn << 3
	case erate > erateMedium:
		sleep = arl.minBtwn << 2
	default:
		sleep = arl.minBtwn << 1
	}
	arl.mu.Unlock()
	time.Sleep(sleep)
}

// recompute maxTokens for the next interval, and
// reset counters
func (arl *AdaptRateLim) recompute() {
	var (
		erate = float64(arl.stats.nerr) / float64(arl.stats.n)
		delta = max(math.Floor(arl.maxTokens/10), 1)
	)
	switch {
	case arl.stats.nerr == 0 && arl.stats.perr == 0:
		if v := math.Ceil(arl.maxTokens); v > arl.maxTokens {
			arl.maxTokens = v
		} else {
			arl.maxTokens = min(arl.maxTokens+delta, float64(arl.origTokens))
		}
	case erate > erateHigh:
		arl.maxTokens = min(arl.maxTokens-delta*4, 1)
	case erate > erateMedium:
		arl.maxTokens = max(arl.maxTokens-delta*2, 1)
	case erate > erateLow:
		if v := math.Floor(arl.maxTokens); v < arl.maxTokens {
			arl.maxTokens = v
		} else {
			arl.maxTokens = max(arl.maxTokens-delta, 1)
		}
	case arl.stats.nerr > 0 && arl.stats.perr > 0:
		arl.maxTokens = max(arl.maxTokens-delta/2, 1)
	}

	arl.minBtwn = arl.RateLim.recompute()

	// reset counters for the next interval, keep previous error count
	arl.stats.perr, arl.stats.nerr = arl.stats.nerr, 0
	arl.stats.n = 0
}
