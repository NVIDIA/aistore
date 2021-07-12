// +build !deadbeef

// Package memsys provides memory management and slab/SGL allocation with io.Reader and io.Writer interfaces
// on top of scatter-gather lists of reusable buffers.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package memsys

func deadbeef([]byte) {}
