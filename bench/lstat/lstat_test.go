// Package lstat compares access(), stat() and lstat() (syscall) latencies
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */

// how to run:
// go test -bench=. -benchtime=20s ./lstat_test.go

package lstat_test

import (
	"os"
	"testing"

	"github.com/NVIDIA/aistore/fs"
)

const (
	filename = "lstat_test.go"
)

func BenchmarkAccess(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if err := fs.Access(filename); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStat(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if _, err := os.Stat(filename); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkLstat(b *testing.B) {
	for i := 0; i < b.N; i++ {
		if _, err := os.Lstat(filename); err != nil {
			b.Fatal(err)
		}
	}
}
