// Package lstat compares access(), stat() and lstat() (syscall) latencies
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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
	for b.Loop() {
		access(b, one)
		access(b, two)
	}
}

func BenchmarkStat(b *testing.B) {
	for b.Loop() {
		stat(b, one)
		stat(b, two)
	}
}

func BenchmarkLstat(b *testing.B) {
	for b.Loop() {
		lstat(b, one)
		lstat(b, two)
	}
}

func BenchmarkOpen(b *testing.B) {
	for b.Loop() {
		open(b, one)
		open(b, two)
	}
}

func BenchmarkSyscallStat(b *testing.B) {
	for b.Loop() {
		syscallStat(b, one)
		syscallStat(b, two)
	}
}
