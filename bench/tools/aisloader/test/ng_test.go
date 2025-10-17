// Package test provides tests of aisloader package
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package test_test

import (
	"flag"
	"fmt"
	"os"
	"reflect"
	"testing"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/bench/tools/aisloader/namegetter"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
)

const (
	objNamesSize    = 200000
	smallSampleSize = 1000
	batchSize       = 512
)

var objNames []string

func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		fmt.Println("skipping bench/tools/aisloader/namegetter in short mode")
		os.Exit(0)
	}
	objNames = make([]string, objNamesSize)
	for i := range objNamesSize {
		objNames[i] = fmt.Sprintf("test-%d", i)
	}
	m.Run()
}

//
// Benchmarks
//

func BenchmarkRandomUnique(b *testing.B) {
	ng := &namegetter.RandomUnique{}
	ng.Init(objNames, cos.NowRand())
	for b.Loop() {
		_ = ng.Pick()
	}
}

func BenchmarkPermShuffle(b *testing.B) {
	ng := &namegetter.PermShuffle{}
	ng.Init(objNames, cos.NowRand())
	for b.Loop() {
		_ = ng.Pick()
	}
}

func BenchmarkPermAffinePrime(b *testing.B) {
	ng := &namegetter.PermAffinePrime{}
	ng.Init(objNames, cos.NowRand())
	for b.Loop() {
		_ = ng.Pick()
	}
}

//
// Tests
//

func TestRandomUnique(t *testing.T) {
	ng := &namegetter.RandomUnique{}
	checkGetsAllOncePerEpoch(t, ng, "RandomUnique")
	checkSmallSampleRandomness(t, ng, "RandomUnique")
	checkPickBatchUniqueness(t, ng, "RandomUnique")
}

func TestPermShuffle(t *testing.T) {
	ng := &namegetter.PermShuffle{}
	checkGetsAllOncePerEpoch(t, ng, "PermShuffle")
	checkSmallSampleRandomness(t, ng, "PermShuffle")
	checkPickBatchUniqueness(t, ng, "PermShuffle")
}

func TestPermAffinePrime(t *testing.T) {
	ng := &namegetter.PermAffinePrime{}
	checkGetsAllOncePerEpoch(t, ng, "PermAffinePrime")
	checkSmallSampleRandomness(t, ng, "PermAffinePrime")
	checkPickBatchUniqueness(t, ng, "PermAffinePrime")
}

//
// Helpers
//

// verify the getter visits every element exactly once
func checkGetsAllOncePerEpoch(t *testing.T, getter namegetter.Basic, name string) {
	t.Helper()

	getter.Init(objNames, cos.NowRand())

	seen := make(map[string]struct{}, objNamesSize)
	for range objNamesSize {
		seen[getter.Pick()] = struct{}{}
	}
	tassert.Fatalf(t, len(seen) == objNamesSize, "%s did not visit all elements; got=%d want=%d", name, len(seen), objNamesSize)

	// new epoch (same instance): should again cover all elements exactly once
	seen = make(map[string]struct{}, objNamesSize)
	for range objNamesSize {
		seen[getter.Pick()] = struct{}{}
	}
	tassert.Fatalf(t, len(seen) == objNamesSize, "%s failed on second epoch; got=%d want=%d", name, len(seen), objNamesSize)
}

// basic “is it random enough” sanity
func checkSmallSampleRandomness(t *testing.T, getter namegetter.Basic, name string) {
	t.Helper()

	rnd := cos.NowRand()

	s1 := make([]string, smallSampleSize)
	getter.Init(objNames, rnd)
	for i := range smallSampleSize {
		s1[i] = getter.Pick()
	}

	s2 := make([]string, smallSampleSize)
	getter.Init(objNames, rnd)
	for i := range smallSampleSize {
		s2[i] = getter.Pick()
	}

	tassert.Fatal(t, !reflect.DeepEqual(s1, s2), name+" sequence appears deterministic with same RNG instance")
}

// ensures PickBatch:
// - returns <= requested
// - tolerates epoch rollover mid-batch and across batches
// - guarantees per-epoch uniqueness (we track epochs via seen-set resets)
func checkPickBatchUniqueness(t *testing.T, getter namegetter.Basic, name string) {
	t.Helper()

	rnd := cos.NowRand()
	getter.Init(objNames, rnd)

	seen := make(map[string]struct{}, objNamesSize)
	epochs := 0

	buf := make([]apc.MossIn, batchSize)
	maxBatches := (objNamesSize/batchSize + 8)
	for b := 0; epochs < 1 && b < maxBatches; b++ {
		out := getter.PickBatch(buf)

		if len(out) > batchSize {
			t.Fatalf("%s PickBatch len=%d exceeds requested=%d", name, len(out), batchSize)
		}
		for i := range out {
			obj := out[i].ObjName

			if _, dup := seen[obj]; dup {
				// Count a completed epoch if we actually covered all N; then reset.
				if len(seen) == objNamesSize {
					epochs++
				}
				seen = make(map[string]struct{}, objNamesSize)
			}

			seen[obj] = struct{}{}

			if len(seen) == objNamesSize {
				epochs++
				seen = make(map[string]struct{}, objNamesSize)
			}
		}
	}
	if epochs < 1 {
		t.Fatalf("%s PickBatch did not complete a full epoch (epochs=%d)", name, epochs)
	}
}
