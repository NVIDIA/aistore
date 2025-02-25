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

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
)

// tunables
const (
	dfltRateMinBtwn = 10 * time.Millisecond

	DfltRateMinIval = time.Second
	DfltRateMaxIval = time.Hour

	dfltRateMaxWait = time.Minute

	DfltRateMaxRetries  = 10
	DfltRateMaxBurstPct = 50
)

// more tunables
const (
	erateLow    = 0.1
	erateMedium = 0.3
	erateHigh   = 0.5

	erateSample = 3
)

// names
const (
	rltag  = "rate-limiter"
	arltag = "adaptive-rate-limiter"
	brltag = "bursty-rate-limiter"
)

// reason
const (
	acquireOK = iota
	acquireNoTokens
	acquireTooSoon
)

type (
	RateLim struct {
		tokens    float64       // current tokens
		maxTokens float64       // max tokens
		tokenIval float64       // duration in nanoseconds as in: maxTokens per tokenIval
		minBtwn   time.Duration // min duration since the previous granted; >= 1ms (above)
		// runtime: "last" timestamps
		tsb struct {
			granted int64
			refill  int64
		}
		mu sync.Mutex
	}
	// usage: adapt to the rate limit from s3, gcp, et al. ("rate shaper")
	AdaptRateLim struct {
		RateLim
		origTokens int
		retries    int
		// runtime
		stats struct {
			// respectively, (errors, granted, prev. errors, prev. granted) counters
			// TODO: consider prev-granted timestamp, to discard previous numbers when they become stale
			nerr, n, perr, pn int
		}
	}
	// usage: rate limit user GET, PUT, and DELETE requests
	BurstRateLim struct {
		RateLim
		origTokens int
		burstSize  int // max allowed burst; cannot exceed maxTokens/2
		// runtime
		stats struct {
			burstLeft int // remaining burst capacity for the current interval
			n         int // number of granted requests since last reset
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
	if tokenIval < DfltRateMinIval || tokenIval > DfltRateMaxIval {
		return fmt.Errorf("%s: invalid token interval %v (min=%v, max=%v)", tag, tokenIval, DfltRateMinIval, DfltRateMaxIval)
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
	return max(time.Duration(rl.tokenIval/rl.maxTokens), dfltRateMinBtwn)
}

func (rl *RateLim) TryAcquire() bool {
	var reason int
	rl.mu.Lock()
	reason = rl.acquire()
	rl.mu.Unlock()
	return reason == acquireOK
}

// is called under lock
func (rl *RateLim) acquire() int {
	now := mono.NanoTime()
	elapsed := time.Duration(now - rl.tsb.granted)
	if elapsed < rl.minBtwn>>1 {
		return acquireTooSoon
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
		return acquireNoTokens
	}
	rl.tokens--
	rl.tsb.granted = now
	return acquireOK
}

//////////////////
// AdaptRateLim //
//////////////////

func NewAdaptRateLim(maxTokens, retries int, tokenIval time.Duration) (*AdaptRateLim, error) {
	if retries < 0 || retries > DfltRateMaxRetries {
		return nil, fmt.Errorf("%s: invalid number of retries %d", arltag, retries)
	}
	arl := &AdaptRateLim{
		origTokens: maxTokens,
		retries:    retries,
	}
	return arl, arl.RateLim.init(arltag, maxTokens, tokenIval)
}

func (arl *AdaptRateLim) Acquire() error {
	var sleep time.Duration
	for i := 0; ; i++ {
		arl.mu.Lock()
		reason := arl.acquire()
		if reason == acquireOK {
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

// backoff before retrying (429, 503)
func (arl *AdaptRateLim) OnErr() {
	var sleep time.Duration
	arl.mu.Lock()
	arl.stats.nerr++

	erate := arl._erate()
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

func (arl *AdaptRateLim) _erate() (erate float64) {
	if n := arl.stats.pn + arl.stats.n; n >= erateSample {
		erate = float64(arl.stats.nerr+arl.stats.perr) / float64(n)
	}
	return erate
}

// - recompute maxTokens for the next interval
// - swap/reset counters
func (arl *AdaptRateLim) recompute() {
	var (
		erate = arl._erate()
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
		arl.maxTokens = max(arl.maxTokens-delta*4, 1)
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

	// swap/reset counters for the next interval
	arl.stats.perr, arl.stats.nerr = arl.stats.nerr, 0
	arl.stats.pn, arl.stats.n = arl.stats.n, 0
}

func (arl *AdaptRateLim) SleepMore(sleep time.Duration) error {
	var minBtwn time.Duration
	arl.mu.Lock()
	minBtwn = arl.minBtwn
	arl.mu.Unlock()
	debug.Assert(minBtwn > 0)
	if minBtwn <= sleep {
		return fmt.Errorf("failed to replenish after %v: %s", sleep, arl._str())
	}
	time.Sleep(minBtwn - sleep)
	return nil
}

func (arl *AdaptRateLim) RetryAcquire(sleep time.Duration) {
	for ; sleep < dfltRateMaxWait; sleep += sleep >> 1 {
		if arl.Acquire() == nil {
			return
		}
		time.Sleep(sleep)
	}
}

func (arl *AdaptRateLim) _str() string {
	return fmt.Sprintf("%s[tokens=(%d,%f),retries=%d,minBtwn=%v]", arltag,
		arl.origTokens, arl.maxTokens, arl.retries, arl.minBtwn)
}

//////////////////
// BurstRateLim //
//////////////////

func NewBurstRateLim(maxTokens, burstSize int, tokenIval time.Duration) (*BurstRateLim, error) {
	if burstSize <= 0 || burstSize > maxTokens*DfltRateMaxBurstPct/100 {
		return nil, fmt.Errorf("%s: invalid burst size %d (expecting positive integer <= (%d%% of maxTokens %d)",
			brltag, burstSize, DfltRateMaxBurstPct, maxTokens)
	}
	brl := &BurstRateLim{
		origTokens: maxTokens,
		burstSize:  burstSize,
	}
	brl.stats.burstLeft = burstSize
	return brl, brl.RateLim.init(brltag, maxTokens, tokenIval)
}

func (brl *BurstRateLim) TryAcquire() bool {
	brl.mu.Lock()
	defer brl.mu.Unlock()

	reason := brl.RateLim.acquire()
	switch reason {
	case acquireOK:
	case acquireNoTokens:
		return false
	case acquireTooSoon:
		if brl.stats.burstLeft > 0 {
			// adjust for burstiness and try again
			brl.recompute(1 - float64(brl.stats.burstLeft)/brl.maxTokens)
			reason = brl.RateLim.acquire()
			if reason != acquireOK {
				return false
			}
			brl.stats.burstLeft--
		}
	}

	brl.stats.n++
	if brl.stats.n >= brl.origTokens {
		debug.Assert(brl.stats.n >= int(brl.maxTokens)) // same
		brl.stats.burstLeft = brl.burstSize
		brl.stats.n = 0
	}
	return true
}

func (brl *BurstRateLim) recompute(factor float64) {
	brl.minBtwn = time.Duration(float64(brl.minBtwn) * factor)
}

// with exponential backoff
func (brl *BurstRateLim) RetryAcquire(sleep time.Duration) {
	for ; sleep < dfltRateMaxWait; sleep += sleep >> 1 {
		if brl.TryAcquire() {
			return
		}
		time.Sleep(sleep)
	}
}
