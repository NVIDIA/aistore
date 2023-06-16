// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"time"

	"github.com/NVIDIA/aistore/api/apc"
)

// usage: update config and bucket props

func String(v string) *string                        { return &v }
func Bool(v bool) *bool                              { return &v }
func Int(v int) *int                                 { return &v }
func Int64(v int64) *int64                           { return &v }
func Duration(v time.Duration) *time.Duration        { return &v }
func AccessAttrs(v apc.AccessAttrs) *apc.AccessAttrs { return &v }
func WritePolicy(v apc.WritePolicy) *apc.WritePolicy { return &v }
