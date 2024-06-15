// Package cos provides common low-level types and utilities for all aistore projects.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"reflect"
	"unsafe"

	"github.com/NVIDIA/aistore/cmn/debug"
)

const MLCG32 = 1103515245 // xxhash seed

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

// bytes to *string
func UnsafeSptr(b []byte) *string {
	return (*string)(unsafe.Pointer(&b))
}

// *string to *[]bytes
func UnsafeBptr(s *string) *[]byte {
	return (*[]byte)(unsafe.Pointer(s))
}

// shallow copy
func CopyStruct(dst, src any) {
	x := reflect.ValueOf(src)
	debug.Assert(x.Kind() == reflect.Ptr)
	starX := x.Elem()
	y := reflect.New(starX.Type())
	starY := y.Elem()
	starY.Set(starX)
	reflect.ValueOf(dst).Elem().Set(y.Elem())
}
