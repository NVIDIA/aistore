// Package walkspeed compares godirwalk and filepath.WalkDir walking performance
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package walkspeed

import (
	iofs "io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/karrick/godirwalk"
)

// Examples:
//
// go test -bench=. -benchtime=10s -benchmem

const (
	numFilesDflt = 500_000 // NOTE: 50K files is way too small
	scratchDftl  = 64 * 1024
	depthDflt    = 16
)

// setupLargeDir creates a flat directory with numFiles files.
func setupLargeDir(b *testing.B, numFiles int) string {
	dir := "large_dir"
	if err := os.RemoveAll(dir); err != nil {
		b.Fatalf("Failed to remove large_dir: %v", err)
	}
	if err := os.Mkdir(dir, 0o755); err != nil {
		b.Fatalf("Failed to create large_dir: %v", err)
	}
	for i := range numFiles {
		fname := filepath.Join(dir, "file_"+strconv.Itoa(i))
		if err := os.WriteFile(fname, []byte{}, 0o600); err != nil {
			b.Fatalf("Failed to create file %s: %v", fname, err)
		}
	}
	return dir
}

// setupDeepTree creates a nested directory with numFiles files across depth levels.
func setupDeepTree(b *testing.B, numFiles, depth int) string {
	dir := "deep_tree"
	if err := os.RemoveAll(dir); err != nil {
		b.Fatalf("Failed to remove deep_tree: %v", err)
	}
	if err := os.Mkdir(dir, 0o755); err != nil {
		b.Fatalf("Failed to create deep_tree: %v", err)
	}

	filesPerLevel := numFiles / depth
	remainder := numFiles % depth
	fileCount := 0
	currentDir := dir

	for d := 0; d < depth && fileCount < numFiles; d++ {
		levelFiles := filesPerLevel
		if d < remainder {
			levelFiles++
		}

		for f := 0; f < levelFiles && fileCount < numFiles; f++ {
			fname := filepath.Join(currentDir, "file_"+strconv.Itoa(fileCount))
			if err := os.WriteFile(fname, []byte{}, 0o600); err != nil {
				b.Fatalf("Failed to create file %s: %v", fname, err)
			}
			fileCount++
		}

		if fileCount < numFiles {
			currentDir = filepath.Join(currentDir, "subdir_"+strconv.Itoa(d))
			if err := os.Mkdir(currentDir, 0o755); err != nil {
				b.Fatalf("Failed to create directory %s: %v", currentDir, err)
			}
		}
	}
	return dir
}

// BenchmarkGodirwalkLargeDir benchmarks godirwalk on a flat directory.
func BenchmarkGodirwalkLargeDir(b *testing.B) {
	dir := setupLargeDir(b, numFilesDflt)
	defer os.RemoveAll(dir)

	b.ResetTimer()
	var count int64
	b.RunParallel(func(pb *testing.PB) {
		scratch := make([]byte, scratchDftl)
		for pb.Next() {
			err := godirwalk.Walk(dir, &godirwalk.Options{
				Callback: func(_ string, de *godirwalk.Dirent) error {
					if de.IsRegular() {
						atomic.AddInt64(&count, 1)
					}
					return nil
				},
				Unsorted:      false,
				ScratchBuffer: scratch,
			})
			if err != nil {
				b.Fatalf("godirwalk error: %v", err)
			}
		}
	})
}

// BenchmarkWalkDirLargeDir benchmarks filepath.WalkDir on a flat directory.
func BenchmarkWalkDirLargeDir(b *testing.B) {
	dir := setupLargeDir(b, numFilesDflt)
	defer os.RemoveAll(dir)

	b.ResetTimer()
	var count int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := filepath.WalkDir(dir, func(_ string, d iofs.DirEntry, _ error) error {
				if d.Type().IsRegular() {
					atomic.AddInt64(&count, 1)
				}
				return nil
			})
			if err != nil {
				b.Fatalf("WalkDir error: %v", err)
			}
		}
	})
}

// BenchmarkGodirwalkDeepTree benchmarks godirwalk on a nested directory.
func BenchmarkGodirwalkDeepTree(b *testing.B) {
	dir := setupDeepTree(b, numFilesDflt, depthDflt)
	defer os.RemoveAll(dir)

	b.ResetTimer()
	var count int64
	b.RunParallel(func(pb *testing.PB) {
		scratch := make([]byte, scratchDftl)
		for pb.Next() {
			err := godirwalk.Walk(dir, &godirwalk.Options{
				Callback: func(_ string, de *godirwalk.Dirent) error {
					if de.IsRegular() {
						atomic.AddInt64(&count, 1)
					}
					return nil
				},
				Unsorted:      false,
				ScratchBuffer: scratch,
			})
			if err != nil {
				b.Fatalf("godirwalk error: %v", err)
			}
		}
	})
}

// BenchmarkWalkDirDeepTree benchmarks filepath.WalkDir on a nested directory.
func BenchmarkWalkDirDeepTree(b *testing.B) {
	dir := setupDeepTree(b, numFilesDflt, depthDflt)
	defer os.RemoveAll(dir)

	b.ResetTimer()
	var count int64
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			err := filepath.WalkDir(dir, func(_ string, d iofs.DirEntry, _ error) error {
				if d.Type().IsRegular() {
					atomic.AddInt64(&count, 1)
				}
				return nil
			})
			if err != nil {
				b.Fatalf("WalkDir error: %v", err)
			}
		}
	})
}
