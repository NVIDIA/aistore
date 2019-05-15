/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/bench/aisloader/namegetter"
)

// Running these benchmarks with different objnamesSize returns different results
// Roughly if it's > 800k, permutation strategies work better

const objnamesSize = 100000

var objnames []string

func init() {
	objnames = make([]string, objnamesSize)
	for i := 0; i < objnamesSize; i++ {
		objnames[i] = string(i + 1000000)
	}
}

func BenchmarkRandomUniqueNameGetter(b *testing.B) {
	ng := &namegetter.RandomUniqueNameGetter{}
	ng.Init(objnames, rand.New(rand.NewSource(time.Now().UnixNano())))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		ng.ObjName()
	}
}

func BenchmarkRandomUniqueIterNameGetter(b *testing.B) {
	ng := &namegetter.RandomUniqueIterNameGetter{}
	ng.Init(objnames, rand.New(rand.NewSource(time.Now().UnixNano())))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		ng.ObjName()
	}
}

func BenchmarkPermutationUniqueNameGetter(b *testing.B) {
	ng := &namegetter.PermutationUniqueNameGetter{}
	ng.Init(objnames, rand.New(rand.NewSource(time.Now().UnixNano())))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		ng.ObjName()
	}
}

func BenchmarkPermutationImprovedUniqueNameGetter(b *testing.B) {
	ng := &namegetter.PermutationUniqueImprovedNameGetter{}
	ng.Init(objnames, rand.New(rand.NewSource(time.Now().UnixNano())))
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		ng.ObjName()
	}
}
