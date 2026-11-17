// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package tests_test

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func TestRateLim(t *testing.T) {
	var (
		tests = []struct {
			maxTokens int
			tokenIval time.Duration
		}{
			{maxTokens: 10, tokenIval: time.Second},
			{maxTokens: 100, tokenIval: 5 * time.Second},
			{maxTokens: 1000, tokenIval: 7 * time.Second},
		}
	)
	if testing.Short() {
		tests = tests[:1]
	}
	for _, test := range tests {
		sleep := test.tokenIval / time.Duration(test.maxTokens)
		tname := fmt.Sprintf("%d:%v", test.maxTokens, test.tokenIval)
		t.Run(tname, func(t *testing.T) {
			rl, err := cos.NewRateLim(test.maxTokens, test.tokenIval)
			if err != nil {
				t.Fatal(err)
			}
			for i := range test.maxTokens {
				time.Sleep(sleep)
				if !rl.TryAcquire() {
					t.Errorf("Failed to acquire token %d", i+1)
				}
			}
			if rl.TryAcquire() {
				t.Error("Acquired token when tokens should've been exhausted")
			}
			time.Sleep(sleep)
			if !rl.TryAcquire() {
				t.Error("Failed to acquire token after replenishment")
			}
		})
	}
}

func TestAdaptRateLim(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true}) // sleep-bound
	var (
		tests = []struct {
			maxTokens int
			retries   int
			tokenIval time.Duration
		}{
			{maxTokens: 10, retries: 0, tokenIval: time.Second},
			{maxTokens: 100, retries: 2, tokenIval: 3 * time.Second},
			{maxTokens: 1000, retries: 3, tokenIval: 5 * time.Second},
		}
	)
	for _, test := range tests {
		tname := fmt.Sprintf("tokens_%d:retries_%d:%v", test.maxTokens, test.retries, test.tokenIval)
		t.Run(tname, func(t *testing.T) {
			arl, err := cos.NewAdaptRateLim(test.maxTokens, test.retries, test.tokenIval)
			if err != nil {
				t.Fatal(err)
			}
			sleep := test.tokenIval / time.Duration(test.maxTokens)
			for i := range test.maxTokens * 2 {
				randSleep := time.Duration(rand.Int64N(int64(sleep)))
				time.Sleep(randSleep)
				if err := arl.Acquire(); err != nil {
					if test.retries > 0 {
						t.Errorf("expecting to acquire when retries (%d) is positive: iter %d: %v\n", test.retries, i, err)
						continue
					}
					// simulate 429 retry
					arl.OnErr(0)
					if err := arl.Acquire(); err != nil {
						t.Errorf("expecting to acquire after OnErr: iter %d: %v\n", i, err)
						continue
					}
				}
			}
			time.Sleep(sleep)
			if !arl.TryAcquire() {
				if err := arl.SleepMore(sleep); err != nil {
					t.Error(err)
				} else if !arl.TryAcquire() {
					t.Error("failed to replenish after sleeping for the computed duration")
				}
			}
		})
	}
}

func TestJitter(t *testing.T) {
	for _, tc := range []struct {
		name     string
		duration time.Duration
		want     [8]time.Duration
	}{
		{name: "zero"},
		{name: "20ms", duration: 20 * time.Millisecond,
			want: [8]time.Duration{19375000, 19531250, 19687500, 19843750, 20000000, 20156250, 20312500, 20468750}},
		{name: "6s", duration: 6 * time.Second,
			want: [8]time.Duration{5812500000, 5859375000, 5906250000, 5953125000, 6000000000, 6046875000, 6093750000, 6140625000}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for slot, want := range tc.want {
				now := int64(1024 + slot)
				got := cos.Jitter(tc.duration, now)
				tassert.Fatalf(t, got == want, "choice %d: got %v, want %v", slot, got, want)
			}
		})
	}
}

func TestAdaptRateLimBackoff(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	for _, tc := range []struct {
		name     string
		tokens   int
		errors   int
		previous time.Duration
		want     time.Duration
	}{
		{name: "initial", tokens: 100, want: 20 * time.Millisecond},
		{name: "medium-error-rate", tokens: 100, errors: 3, want: 40 * time.Millisecond},
		{name: "high-error-rate", tokens: 100, errors: 5, want: 80 * time.Millisecond},
		{name: "grow-previous-wait", tokens: 100, errors: 5, previous: 20 * time.Millisecond,
			want: 160 * time.Millisecond},
		{name: "configured-slower-rate", tokens: 10, previous: time.Millisecond,
			want: 200 * time.Millisecond},
		{name: "sleep-ceiling", tokens: 100, errors: 5, previous: time.Second,
			want: cos.DfltRateMaxWait / cos.DfltRateMaxRetries},
	} {
		t.Run(tc.name, func(t *testing.T) {
			arl, err := cos.NewAdaptRateLim(tc.tokens, cos.DfltRateMaxRetries, time.Second)
			tassert.CheckFatal(t, err)
			for attempt := range 10 {
				tassert.CheckFatal(t, arl.Acquire())
				if attempt < tc.errors {
					arl.OnErr(0)
				}
			}
			started := time.Now()
			sleep := arl.OnErr(tc.previous)
			lower := tc.want - tc.want/32
			upper := tc.want + tc.want/32
			tassert.Fatalf(t, sleep >= lower && sleep <= upper && time.Since(started) >= sleep,
				"backoff = %v, want [%v, %v]; elapsed %v", sleep, lower, upper, time.Since(started))
		})
	}
}

func TestAdaptRateLimRetrySequence(t *testing.T) {
	tools.CheckSkip(t, &tools.SkipTestArgs{Long: true})
	arl, err := cos.NewAdaptRateLim(int(cos.DfltRateMinIval/cos.DfltRateMinBtwn), cos.DfltRateMaxRetries, cos.DfltRateMinIval)
	tassert.CheckFatal(t, err)
	tassert.CheckFatal(t, arl.Acquire())
	var sleep time.Duration
	for range cos.DfltRateMaxRetries {
		base := min(max(sleep, cos.DfltRateMinBtwn)<<1, cos.DfltRateMaxWait/cos.DfltRateMaxRetries)
		sleep = arl.OnErr(sleep)
		lower := base - base/32
		upper := base + base/32
		tassert.Fatalf(t, sleep >= lower && sleep <= upper, "backoff = %v, want [%v, %v]", sleep, lower, upper)
	}
	tassert.CheckFatal(t, arl.Acquire())
	sleep = arl.OnErr(0)
	base := cos.DfltRateMinBtwn << 1
	tassert.Fatalf(t, sleep >= base-base/32 && sleep <= base+base/32,
		"new operation reused the previous operation's wait: %v", sleep)
}

func TestBurstRateLim(t *testing.T) {
	var (
		tests = []struct {
			maxTokens int
			burstSize int
			tokenIval time.Duration
		}{
			{maxTokens: 10, burstSize: 4, tokenIval: time.Second},
			{maxTokens: 100, burstSize: 30, tokenIval: 3 * time.Second},
			{maxTokens: 1000, burstSize: 200, tokenIval: 5 * time.Second},
		}
	)
	if testing.Short() {
		tests = tests[:1]
	}
	for _, test := range tests {
		tname := fmt.Sprintf("tokens_%d:burst_%d:%v", test.maxTokens, test.burstSize, test.tokenIval)
		t.Run(tname, func(t *testing.T) {
			brl, err := cos.NewBurstRateLim("no-bucket", test.maxTokens, test.burstSize, test.tokenIval)
			if err != nil {
				t.Fatal(err)
			}

			sleep := test.tokenIval / time.Duration(test.maxTokens)
			// compare with BurstRateLim.recompute()
			burstSleep := time.Duration(float64(sleep) * (1 - float64(test.burstSize)/float64(test.maxTokens)))

			// 1st burst
			for i := range test.burstSize {
				time.Sleep(burstSleep)
				if !brl.TryAcquire() {
					t.Errorf("failed to acquire during first burst phase: iter %d, sleep %v", i+1, burstSleep)
					break
				}
			}

			// normal rate
			for i := range test.maxTokens {
				randSleep := sleep - time.Duration(rand.Int64N(int64(sleep/3)))
				time.Sleep(randSleep)
				if !brl.TryAcquire() {
					t.Errorf("failed to acquire during normal phase: iter %d, sleep %v", i+1, randSleep)
					break
				}
			}

			// replenish for half interval
			time.Sleep(test.tokenIval / 2)
			if !brl.TryAcquire() {
				t.Errorf("failed to replenish after half token interval %v", test.tokenIval)
			}

			// 2nd burst
			for i := range test.burstSize {
				time.Sleep(burstSleep)
				if !brl.TryAcquire() {
					t.Errorf("failed to acquire during first burst phase: iter %d, sleep %v", i+1, burstSleep)
					break
				}
			}
		})
	}
}
