// Package lstat compares access(), stat() and lstat() (syscall) latencies
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */

// # Generate files prior to running this one, e.g.:
// $ mkdir /tmp/w; for f in {10000..99999}; do echo "$RANDOM -- $RANDOM" > /tmp/w/$f.txt; done
//
// # Run:
// go test -bench=. -benchtime=20s -benchmem

package lstat_test

import (
	"fmt"
	"os"
	"strconv"
	"syscall"
	"testing"
)

const dir = "/tmp/w/"

func init() {
	if _, err := os.Stat("/tmp/w/12345.txt"); err != nil {
		fmt.Println("Reminder to generate random files as follows (e.g.):")
		fmt.Println(`
$ mkdir /tmp/w; for f in {10000..99999}; do echo "$RANDOM -- $RANDOM" > /tmp/w/$f.txt; done`)
		os.Exit(0)
	}
}

func syscallAccess(path string) error { return syscall.Access(path, syscall.F_OK) }

func access(b *testing.B, path string) {
	if err := syscallAccess(path); err != nil {
		b.Fatalf("%s: %v", path, err)
	}
}

func stat(b *testing.B, path string) {
	if _, err := os.Stat(path); err != nil {
		b.Fatalf("%s: %v", path, err)
	}
}

func lstat(b *testing.B, path string) {
	if _, err := os.Lstat(path); err != nil {
		b.Fatalf("%s: %v", path, err)
	}
}

func open(b *testing.B, path string) {
	if file, err := os.Open(path); err != nil {
		b.Fatalf("%s: %v", path, err)
	} else {
		file.Close()
	}
}

func syscallStat(b *testing.B, path string) {
	var sys syscall.Stat_t
	if err := syscall.Stat(path, &sys); err != nil {
		b.Fatalf("%s: %v", path, err)
	}
}

func BenchmarkParallel(b *testing.B) {
	tests := []struct {
		tag string
		f   func(b *testing.B, path string)
	}{
		{"access", access},
		{"stat", stat},
		{"lstat", lstat},
		{"open", open},
		{"syscall-stat", syscallStat},
	}
	for _, test := range tests {
		b.Run(test.tag, func(b *testing.B) { all(b, test.f) })
	}
}

func all(b *testing.B, f func(b *testing.B, path string)) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			for i := 10000; i < 100000; i++ {
				f(b, dir+strconv.Itoa(i)+".txt")
			}
		}
	})
}
