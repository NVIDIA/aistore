// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos

const SparseFirst = 10

// Sparse returns true for sparse/progressive event reporting.
// It expects a positive, 1-based event counter: first SparseFirst events,
// then every 100th up to 1000, then powers of two.
func Sparse(cnt int64) bool {
	return cnt <= SparseFirst || (cnt <= 1000 && cnt%100 == 0) || cnt&(cnt-1) == 0
}
