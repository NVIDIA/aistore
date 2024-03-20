// Package lstat compares access(), stat() and lstat() (syscall) latencies
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
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
	for range b.N {
		access(b, one)
		access(b, two)
	}
}

func BenchmarkStat(b *testing.B) {
	for range b.N {
		stat(b, one)
		stat(b, two)
	}
}

func BenchmarkLstat(b *testing.B) {
	for range b.N {
		lstat(b, one)
		lstat(b, two)
	}
}

func BenchmarkOpen(b *testing.B) {
	for range b.N {
		open(b, one)
		open(b, two)
	}
}

func BenchmarkSyscallStat(b *testing.B) {
	for range b.N {
		syscallStat(b, one)
		syscallStat(b, two)
	}
}
