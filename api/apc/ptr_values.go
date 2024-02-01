// Package apc: API control messages and constants
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package apc

import (
	"time"
)

func String(v string) *string                 { return &v }
func Bool(v bool) *bool                       { return &v }
func Int(v int) *int                          { return &v }
func Int64(v int64) *int64                    { return &v }
func Duration(v time.Duration) *time.Duration { return &v }
func AccAttrs(v AccessAttrs) *AccessAttrs     { return &v }
func WPolicy(v WritePolicy) *WritePolicy      { return &v }
