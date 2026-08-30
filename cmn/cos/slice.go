// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"strings"
)

// return the same slice if its capacity is within maxCap; otherwise,
// copy at most maxCap elements so that the oversized one can be reclaimed
func ResetSliceCap[T any](s []T, maxCap int) []T {
	if maxCap <= 0 {
		return nil
	}
	if cap(s) <= maxCap {
		return s
	}

	l := min(len(s), maxCap)
	clipped := make([]T, l, maxCap)
	copy(clipped, s[:l])
	return clipped
}

func AnyHasPrefixInSlice(prefix string, arr []string) bool {
	for _, el := range arr {
		if strings.HasPrefix(el, prefix) {
			return true
		}
	}
	return false
}
