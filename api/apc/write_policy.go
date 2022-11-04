// Package apc: API messages and constants
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import "fmt"

// write policy (enum and accessors)
// applies to both AIS metadata and data; bucket-configurable with global defaults via cluster config
type WritePolicy string

const (
	WriteImmediate = WritePolicy("immediate") // immediate write (default)
	WriteDelayed   = WritePolicy("delayed")   // cache and flush when not accessed for a while (lom_cache_hk.go)
	WriteNever     = WritePolicy("never")     // transient - in-memory only

	WriteDefault = WritePolicy("") // same as `WriteImmediate` - see IsImmediate() below
)

var SupportedWritePolicy = []string{string(WriteImmediate), string(WriteDelayed), string(WriteNever)}

func (wp WritePolicy) IsImmediate() bool { return wp == WriteDefault || wp == WriteImmediate }

func (wp WritePolicy) Validate() (err error) {
	if wp.IsImmediate() || wp == WriteDelayed || wp == WriteNever {
		return
	}
	return fmt.Errorf("invalid write policy %q (expecting one of %v)", wp, SupportedWritePolicy)
}
