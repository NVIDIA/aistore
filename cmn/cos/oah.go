// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cos

// OAH: object attrs holder
type OAH interface {
	SizeBytes(special ...bool) int64
	Version(special ...bool) string
	Checksum() *Cksum
	AtimeUnix() int64
	GetCustomMD() StrKVs
	GetCustomKey(key string) (val string, exists bool)
	SetCustomKey(k, v string)
	String() string
}
