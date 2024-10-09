// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2024, NVIDIA CORPORATION. All rights reserved.
 */
package cos

import (
	"fmt"
	"time"

	"github.com/NVIDIA/aistore/cmn/debug"
)

// left if non-zero; otherwise right
// (generics)
func NonZero[T any](left, right T) (value T) {
	v := any(left)
	value = left
	switch v.(type) {
	case int:
		if v == int(0) {
			value = right
		}
	case int64:
		if v == int64(0) {
			value = right
		}
	case time.Duration:
		if v == time.Duration(0) {
			value = right
		}
	default:
		err := fmt.Errorf("unexpected type %T", v)
		debug.AssertNoErr(err)
	}
	return value
}
