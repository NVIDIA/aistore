// Package apc: API constants
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import "fmt"

// metadata write policy
type MDWritePolicy string

const (
	WriteImmediate = MDWritePolicy("immediate") // immediate write (default)
	WriteDelayed   = MDWritePolicy("delayed")   // cache and flush when not accessed for a while (lom_cache_hk.go)
	WriteNever     = MDWritePolicy("never")     // transient - in-memory only

	WriteDefault = MDWritePolicy("") // equivalent to immediate writing (WriteImmediate)
)

var (
	SupportedWritePolicy = []string{string(WriteImmediate), string(WriteDelayed), string(WriteNever)}
	SupportedCompression = []string{CompressNever, CompressAlways}
)

///////////////////////////////////////////
// MDWritePolicy (part of ClusterConfig) //
///////////////////////////////////////////

func (mdw MDWritePolicy) IsImmediate() bool { return mdw == WriteDefault || mdw == WriteImmediate }

func (mdw MDWritePolicy) Validate() (err error) {
	if mdw.IsImmediate() || mdw == WriteDelayed || mdw == WriteNever {
		return
	}
	return fmt.Errorf("invalid md_write policy %q", mdw)
}

func (mdw MDWritePolicy) ValidateAsProps(...interface{}) (err error) { return mdw.Validate() }
