// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"unsafe"
)

// assorted common constants
const (
	SizeofI64 = int(unsafe.Sizeof(uint64(0)))
	SizeofI32 = int(unsafe.Sizeof(uint32(0)))
	SizeofI16 = int(unsafe.Sizeof(uint16(0)))
)

// Unsafe cast (string => []byte) and ([]byte => string)
// ******* CAUTION! the result must never change *******

// cast bytes to an immutable string
func UnsafeS(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}

// cast string to immutable bytes
func UnsafeB(s string) []byte {
	return unsafe.Slice(unsafe.StringData(s), len(s))
}
