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

	"github.com/NVIDIA/aistore/bench/tools/aisloader/namegetter"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools/tassert"
)

// Running these benchmarks with different objNamesSize returns different results
// Almost for every number of objects, permutation strategies outcompete random

const (
	objNamesSize    = 200000
	smallSampleSize = 1000
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

func BenchmarkRandomUniqueNameGetter(b *testing.B) {
	ng := &namegetter.RandomUnique{}
	ng.Init(objNames, cos.NowRand())
	for b.Loop() {
		ng.Pick()
	}
}

func BenchmarkPermutationImprovedUniqueNameGetter(b *testing.B) {
	ng := &namegetter.PermShuffle{}
	ng.Init(objNames, cos.NowRand())
	for b.Loop() {
		ng.Pick()
	}
}

func TestRandomUniqueNameGetter(t *testing.T) {
	ng := &namegetter.RandomUnique{}

	checkGetsAllObjNames(t, ng, "RandomUnique")
	checkSmallSampleRandomness(t, ng, "RandomUnique")
}

func TestPermutationUniqueImprovedNameGetter(t *testing.T) {
	ng := &namegetter.PermShuffle{}

	checkGetsAllObjNames(t, ng, "PermShuffle")
	checkSmallSampleRandomness(t, ng, "PermShuffle")
}

func checkGetsAllObjNames(t *testing.T, getter namegetter.Basic, name string) {
	getter.Init(objNames, cos.NowRand())
	m := make(map[string]struct{})

	// Should visit every objectName once
	for range objNamesSize {
		m[getter.Pick()] = struct{}{}
	}

	tassert.Fatalf(t, len(m) == objNamesSize,
		"%s has not visited every element; got %d, expected %d", name, len(m), objNamesSize)

	// Check that starting operation for the beginning still works as expected
	m = make(map[string]struct{})
	for range objNamesSize {
		m[getter.Pick()] = struct{}{}
	}
	tassert.Fatalf(t, len(m) == objNamesSize,
		"%s has not visited every element for second time; got %d, expected %d", name, len(m), objNamesSize)
}

func checkSmallSampleRandomness(t *testing.T, getter namegetter.Basic, name string) {
	s1 := make([]string, smallSampleSize)
	s2 := make([]string, smallSampleSize)

	rnd := cos.NowRand()

	getter.Init(objNames, rnd)
	for i := range smallSampleSize {
		s1[i] = getter.Pick()
	}
	getter.Init(objNames, rnd)
	for i := range smallSampleSize {
		s2[i] = getter.Pick()
	}

	tassert.Fatal(t, !reflect.DeepEqual(s1, s2), name+" is not random!")
}
