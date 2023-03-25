// Package atomic provides simple wrappers around numerics to enforce atomic
// access.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package atomic

import (
	"sync/atomic"
	"unsafe"
)

// Structure which will detect copies of atomic
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// Pointer is an atomic wrapper around unsafe.Pointer
// https://godoc.org/unsafe#Pointer
type Pointer struct {
	noCopy noCopy
	p      unsafe.Pointer
}

func (p *Pointer) Load() unsafe.Pointer {
	return atomic.LoadPointer(&p.p)
}

func (p *Pointer) Store(v unsafe.Pointer) {
	atomic.StorePointer(&p.p, v)
}
