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
				t.Errorf("Acquired token when tokens should've been exhausted")
			}
			time.Sleep(sleep)
			if !rl.TryAcquire() {
				t.Errorf("Failed to acquire token after replenishment")
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
			{maxTokens: 100, retries: 2, tokenIval: 5 * time.Second},
			{maxTokens: 1000, retries: 3, tokenIval: 7 * time.Second},
		}
	)
	for _, test := range tests {
		tname := fmt.Sprintf("tokens(%d):retries(%d):interval(%v)", test.maxTokens, test.retries, test.tokenIval)
		t.Run(tname, func(t *testing.T) {
			rl, err := cos.NewAdaptRateLim(test.maxTokens, test.retries, test.tokenIval)
			if err != nil {
				t.Fatal(err)
			}
			sleep := test.tokenIval / time.Duration(test.maxTokens)
			for i := range test.maxTokens {
				randSleep := time.Duration(rand.Int64N(int64(sleep)))
				time.Sleep(randSleep)
				if err := rl.Acquire(); err != nil {
					t.Errorf("%d: %v\n", i, err)
				}
			}
			if rl.TryAcquire() {
				t.Errorf("Acquired token when tokens should've been exhausted")
			}
			time.Sleep(sleep)
			if !rl.TryAcquire() {
				t.Errorf("Failed to acquire token after replenishment")
			}
		})
	}
}
