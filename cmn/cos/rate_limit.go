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
	dfltMinSince = time.Millisecond
	dfltMaxIval  = 10 * time.Minute
)

type RateLimiter struct {
	tokens    float64       // current tokens
	maxTokens float64       // max tokens
	tokenIval float64       // duration in nanoseconds as in: maxTokens per tokenIval
	minSince  time.Duration // min duration since the previous granted; >= 1ms (above)
	// runtime
	lastReq int64
	lastAdd int64
	mu      sync.Mutex
}

func NewRateLimiter(maxTokens int, tokenIval, minSince time.Duration) (*RateLimiter, error) {
	const (
		tag = "rate-limiter"
	)
	minSince = NonZero(minSince, dfltMinSince)
	if tokenIval < minSince || tokenIval > dfltMaxIval {
		return nil, fmt.Errorf("%s: invalid token interval %v (min=%v, max=%v)", tag, tokenIval, minSince, dfltMaxIval)
	}
	if maxTokens <= 0 || maxTokens >= math.MaxInt32 {
		return nil, fmt.Errorf("%s: invalid number of tokens %d per (token) interval", tag, maxTokens)
	}
	rl := &RateLimiter{
		tokens:    0,
		maxTokens: float64(maxTokens),
		tokenIval: float64(tokenIval),
		minSince:  minSince,
	}
	return rl, nil
}

func (rl *RateLimiter) TryAcquire() bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	now := mono.NanoTime()
	elapsed := time.Duration(now - rl.lastReq)
	if elapsed < rl.minSince {
		return false
	}

	// replenish
	elapsed = time.Duration(now - rl.lastAdd)
	if pct := float64(elapsed) / rl.tokenIval; pct > 0 {
		rl.lastAdd = now
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
	rl.lastReq = now
	return true
}
