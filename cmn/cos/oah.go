// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cos

// OAH: object attrs holder

type (
	OAH interface {
		SizeBytes(special ...bool) int64
		Version(special ...bool) string
		Checksum() *Cksum
		AtimeUnix() int64
		GetCustomMD() StrKVs
		GetCustomKey(key string) (val string, exists bool)
		SetCustomKey(k, v string)
		String() string
	}
	// convenience/shortcut
	SimpleOAH struct {
		Size  int64
		Atime int64
	}
)

func (s SimpleOAH) SizeBytes(...bool) int64 { return s.Size }
func (s SimpleOAH) AtimeUnix() int64        { return s.Atime }

func (SimpleOAH) Version(...bool) string             { return "" }
func (SimpleOAH) Checksum() *Cksum                   { return nil }
func (SimpleOAH) GetCustomMD() StrKVs                { return nil }
func (SimpleOAH) GetCustomKey(string) (string, bool) { return "", false }
func (SimpleOAH) SetCustomKey(_, _ string)           {}
func (SimpleOAH) String() string                     { return "" }
