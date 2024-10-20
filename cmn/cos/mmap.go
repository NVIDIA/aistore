// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"sync"

	"github.com/NVIDIA/aistore/cmn/debug"
)

const (
	MultiHashMapCount = 0x40 // m.b. a power of two
	MultiHashMapMask  = MultiHashMapCount - 1
)

type MultiHashMap struct {
	m [MultiHashMapCount]sync.Map
}

func (mhm *MultiHashMap) Get(idx int) *sync.Map {
	debug.Assert(idx >= 0 && idx < MultiHashMapCount, idx)
	return &mhm.m[idx]
}
