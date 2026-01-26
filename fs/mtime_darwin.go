//go:build darwin

// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"os"
	"time"
)

// MtimeUTC Get mtime in UTC using standard library
func MtimeUTC(path string) (time.Time, error) {
	info, err := os.Stat(path)
	if err != nil {
		return time.Time{}, err
	}
	return info.ModTime().UTC(), nil
}

// Chtimes Set atime and mtime
func Chtimes(path string, atime, mtime time.Time) error {
	return os.Chtimes(path, atime, mtime)
}

// ChtimeOnly Set only atime
func ChtimeOnly(path string, atime time.Time) error {
	info, err := os.Stat(path)
	if err != nil {
		return err
	}
	return os.Chtimes(path, atime, info.ModTime())
}
