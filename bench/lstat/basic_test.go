// Package lstat compares access(), stat() and lstat() (syscall) latencies
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */

// how to run:
// go test -bench=. -benchtime=20s ./lstat_test.go

package lstat_test

import (
	"testing"
)

const (
	one = "basic_test.go"
	two = "parallel_test.go"
)

func BenchmarkAccess(b *testing.B) {
	for i := 0; i < b.N; i++ {
		access(b, one)
		access(b, two)
	}
}

func BenchmarkStat(b *testing.B) {
	for i := 0; i < b.N; i++ {
		stat(b, one)
		stat(b, two)
	}
}

func BenchmarkLstat(b *testing.B) {
	for i := 0; i < b.N; i++ {
		lstat(b, one)
		lstat(b, two)
	}
}

func BenchmarkOpen(b *testing.B) {
	for i := 0; i < b.N; i++ {
		open(b, one)
		open(b, two)
	}
}

func BenchmarkSyscallStat(b *testing.B) {
	for i := 0; i < b.N; i++ {
		syscallStat(b, one)
		syscallStat(b, two)
	}
}
