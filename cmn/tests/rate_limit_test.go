// Package test provides tests for common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2025, NVIDIA CORPORATION. All rights reserved.
 */
package tests_test

import (
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
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
					arl.OnErr()
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
