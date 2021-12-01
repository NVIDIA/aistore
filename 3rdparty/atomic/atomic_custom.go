// Package atomic provides simple wrappers around numerics to enforce atomic
// access.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package atomic

import (
	"math"
	"sync/atomic"
	"unsafe"
)

// Structure which will detect copies of atomic
type noCopy struct{}

func (*noCopy) Lock()   {}
func (*noCopy) Unlock() {}

// Float32 is an atomic wrapper around float32.
type Float32 struct {
	_ noCopy
	v uint32
}

// NewFloat32 creates a Float32.
func NewFloat32(f float32) *Float32 {
	return &Float32{v: math.Float32bits(f)}
}

// Load atomically loads the wrapped value.
func (f *Float32) Load() float32 {
	return math.Float32frombits(atomic.LoadUint32(&f.v))
}

// Store atomically stores the passed value.
func (f *Float32) Store(s float32) {
	atomic.StoreUint32(&f.v, math.Float32bits(s))
}

// CAS is an atomic compare-and-swap.
func (f *Float32) CAS(old, new float32) bool {
	return atomic.CompareAndSwapUint32(&f.v, math.Float32bits(old), math.Float32bits(new))
}

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
