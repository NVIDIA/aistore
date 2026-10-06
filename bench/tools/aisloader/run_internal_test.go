// Package aisloader
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package aisloader

import (
	"bytes"
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

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

// TestMultipartStatsJSON verifies successes and failures survive JSON serialization
// independently of regular PUT statistics.
func TestMultipartStatsJSON(t *testing.T) {
	s := newStats(time.Now())
	s.put.Add(128, time.Millisecond)
	s.putMPU.Add(1024, time.Millisecond)
	s.putMPU.Add(2048, time.Millisecond)
	s.putMPU.AddErr()
	var output bytes.Buffer
	writeStatsJSON(&output, &s, false)
	var report map[string]jsonStats
	if err := json.Unmarshal(output.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	mpu, ok := report["put_multipart"]
	if !ok || mpu.Cnt != 2 || mpu.Bytes != 3072 || mpu.Errs != 1 {
		t.Fatalf("multipart statistics missing or incorrect: %s", output.Bytes())
	}
	put := report["put"]
	if put.Cnt != 1 || put.Bytes != 128 || put.Errs != 0 {
		t.Fatalf("regular PUT statistics changed: %s", output.Bytes())
	}
}

func TestObjNamesFromList(t *testing.T) {
	// covers blank lines, surrounding whitespace, names with spaces/tabs, and preserved ordering
	input := "\n  data/first.bin  \r\n \t \ninvalid name\n\tdata/second.tar\t\ninvalid\tname\ndata/first.bin"
	want := []string{"data/first.bin", "data/second.tar", "data/first.bin"}
	names, err := objNamesFromList(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(names, want) {
		t.Fatalf("objNamesFromList() = %q, expected %q", names, want)
	}
}
