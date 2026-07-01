// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"strings"
)

func ResetSliceCap[T any](s []T, maxCap int) []T {
	var (
		l = len(s)
		c = cap(s)
	)
	switch {
	case maxCap <= 0:
		return s[:0:0]
	case c <= maxCap:
		return s
	default:
		l = min(l, maxCap)
		return s[:l:maxCap]
	}
}

func AnyHasPrefixInSlice(prefix string, arr []string) bool {
	for _, el := range arr {
		if strings.HasPrefix(el, prefix) {
			return true
		}
	}
	return false
}
