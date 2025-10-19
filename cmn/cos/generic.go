// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2024-2025, NVIDIA CORPORATION. All rights reserved.
 */
package cos

// left if non-zero; otherwise right
// (generics)

func Ternary[T any](cond bool, a, b T) T {
	if cond {
		return a
	}
	return b
}

func NonZero[T comparable](left, right T) T {
	var zero T
	return Ternary(left != zero, left, right)
}
