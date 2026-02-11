// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import "sync/atomic"

const MSB64 = uint64(1) << 63

type BitFlags uint64

func (f BitFlags) Set(flags BitFlags) BitFlags {
	return f | flags
}

func (f BitFlags) Clear(flags BitFlags) BitFlags {
	return f &^ flags
}

func (f BitFlags) IsSet(flags BitFlags) bool {
	return f&flags == flags
}

func (f BitFlags) IsAnySet(flags BitFlags) bool {
	return f&flags != 0
}

//
// atomic
//

func SetFlag(f *uint64, flags uint64) {
	for {
		o := atomic.LoadUint64(f)
		n := o | flags
		if atomic.CompareAndSwapUint64(f, o, n) || o == n {
			return
		}
	}
}

func ClrFlag(f *uint64, flags uint64) {
	for {
		o := atomic.LoadUint64(f)
		n := o &^ flags
		if atomic.CompareAndSwapUint64(f, o, n) || o == n {
			return
		}
	}
}

func IsAnySetFlag(f *uint64, flags uint64) bool {
	return atomic.LoadUint64(f)&flags != 0
}
