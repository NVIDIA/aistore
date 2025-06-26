//go:build deadbeef

// Package memsys provides memory management and slab/SGL allocation with io.Reader and io.Writer interfaces
// on top of scatter-gather lists of reusable buffers.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package debug

const (
	dbsmall = "DEADBEEF"
	dblarge = "DEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEFDEADBEEF"
)

func DeadBeefSmall(b []byte) {
	l := len(b)
	for i := 0; i < l; i += len(dbsmall) {
		copy(b[i:], dbsmall)
	}
}

func DeadBeefLarge(b []byte) {
	l := len(b)
	for i := 0; i < l; i += len(dblarge) {
		copy(b[i:], dblarge)
	}
}
