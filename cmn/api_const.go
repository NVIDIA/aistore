// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import "github.com/NVIDIA/aistore/api/apc"

// metadata write policy
type MDWritePolicy string

func (mdw MDWritePolicy) IsImmediate() bool { return mdw == WriteDefault || mdw == WriteImmediate }

const (
	WriteImmediate = MDWritePolicy("immediate") // immediate write (default)
	WriteDelayed   = MDWritePolicy("delayed")   // cache and flush when not accessed for a while (lom_cache_hk.go)
	WriteNever     = MDWritePolicy("never")     // transient - in-memory only

	WriteDefault = MDWritePolicy("") // equivalent to immediate writing (WriteImmediate)
)

var (
	SupportedWritePolicy = []string{string(WriteImmediate), string(WriteDelayed), string(WriteNever)}
	SupportedCompression = []string{apc.CompressNever, apc.CompressAlways}
)
