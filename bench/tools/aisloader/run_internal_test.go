// Package aisloader
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package aisloader

import (
	"reflect"
	"testing"

	"github.com/NVIDIA/aistore/bench/tools/aisloader/namegetter"
)

// newNameGetter: verifies workload settings select the expected name-getter strategy.
func TestNewNameGetter(t *testing.T) {
	tests := []struct {
		name        string
		nameCount   int
		putPct      int
		epochs      uint
		threshold   uint
		expected    any
		isPermBased bool
	}{
		{
			name:      "mixed workload",
			putPct:    10,
			threshold: 100,
			expected:  &namegetter.Random{},
		},
		{
			name:      "mixed epoch workload",
			putPct:    10,
			epochs:    1,
			threshold: 100,
			expected:  &namegetter.RandomUnique{},
		},
		{
			name:        "read-only with count at or below permShuffleMax",
			threshold:   100,
			expected:    &namegetter.PermShuffle{},
			isPermBased: true,
		},
		{
			name:        "read-only with count above permShuffleMax",
			threshold:   namegetter.AffineMinN - 1,
			expected:    &namegetter.PermAffinePrime{},
			isPermBased: true,
		},
		{
			name:        "read-only with count above permShuffleMax but not above AffineMinN",
			nameCount:   namegetter.AffineMinN,
			expected:    &namegetter.PermShuffle{},
			isPermBased: true,
		},
	}

	oldRunParams := runParams
	t.Cleanup(func() { runParams = oldRunParams })

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			nameCount := test.nameCount
			if nameCount == 0 {
				nameCount = namegetter.AffineMinN + 1
			}
			runParams = &params{
				workloadParams: workloadParams{
					putPct:         test.putPct,
					numEpochs:      test.epochs,
					permShuffleMax: test.threshold,
				},
			}

			actual, isPermBased := newNameGetter(make([]string, nameCount))
			if reflect.TypeOf(actual) != reflect.TypeOf(test.expected) {
				t.Fatalf("newNameGetter() type = %T, expected %T", actual, test.expected)
			}
			if isPermBased != test.isPermBased {
				t.Fatalf("newNameGetter() isPermBased = %t, expected %t", isPermBased, test.isPermBased)
			}
		})
	}
}
