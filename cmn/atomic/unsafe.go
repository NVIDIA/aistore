// Package atomic provides simple wrappers around numerics to enforce atomic
// access.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package atomic

import (
	"sync/atomic"
	"unsafe"
)

type noCopy struct{}

//
// pointer
//

type Pointer struct {
	_ noCopy
	v unsafe.Pointer
}

func (p *Pointer) Load() unsafe.Pointer   { return atomic.LoadPointer(&p.v) }
func (p *Pointer) Store(v unsafe.Pointer) { atomic.StorePointer(&p.v, v) }
