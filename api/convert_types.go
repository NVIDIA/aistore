// Package api provides AIStore API over HTTP(S)
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package api

import (
	"time"

	"github.com/NVIDIA/aistore/cmn"
)

// String returns a pointer to the string value passed in.
func String(v string) *string {
	return &v
}

// Bool returns a pointer to the bool value passed in.
func Bool(v bool) *bool {
	return &v
}

// Int returns a pointer to the int value passed in.
func Int(v int) *int {
	return &v
}

// Int64 returns a pointer to the int64 value passed in.
func Int64(v int64) *int64 {
	return &v
}

// AccessAttrs returns a pointer to the AccessAttr value passed in.
func AccessAttrs(v cmn.AccessAttrs) *cmn.AccessAttrs {
	return &v
}

func MDWritePolicy(v cmn.MDWritePolicy) *cmn.MDWritePolicy {
	return &v
}

// Duration returns a pointer to the time duration value passed in.
func Duration(v time.Duration) *time.Duration {
	return &v
}
