// Package api provides RESTful API to AIS object storage
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package api

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

// Uint64 returns a pointer to the int64 value passed in.
func Uint64(v uint64) *uint64 {
	return &v
}
