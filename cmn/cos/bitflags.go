// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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

// atomic

// "set" as in: "add"
func SetfAtomic(f *uint64, flags uint64) (ok bool) {
	return atomic.CompareAndSwapUint64(f, *f, *f|flags)
}

func ClearfAtomic(f *uint64, flags uint64) (ok bool) {
	return atomic.CompareAndSwapUint64(f, *f, *f&^flags)
}

func IsSetfAtomic(f *uint64, flags uint64) (yes bool) {
	return atomic.LoadUint64(f)&flags == flags
}

func IsAnySetfAtomic(f *uint64, flags uint64) (yes bool) {
	return atomic.LoadUint64(f)&flags != 0
}
