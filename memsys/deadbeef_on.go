//go:build deadbeef

// Package memsys provides memory management and slab/SGL allocation with io.Reader and io.Writer interfaces
// on top of scatter-gather lists of reusable buffers.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

const (
	deadBEEF = "DEADBEEF"
)

func deadbeef(b []byte) {
	l := len(b)
	for i := 0; i < l; i += len(deadBEEF) {
		copy(b[i:], deadBEEF)
	}
}
