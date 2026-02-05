// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos

// OAH: object attrs holder

type (
	OAH interface {
		Lsize(special ...bool) int64
		Version(special ...bool) string
		VersionPtr() *string
		Checksum() *Cksum
		EqCksum(cksum *Cksum) bool
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

// interface guard
var _ OAH = (*SimpleOAH)(nil)

func (s SimpleOAH) Lsize(...bool) int64 { return s.Size }
func (s SimpleOAH) AtimeUnix() int64    { return s.Atime }

func (SimpleOAH) Version(...bool) string             { return "" }
func (SimpleOAH) VersionPtr() *string                { return nil }
func (SimpleOAH) Checksum() *Cksum                   { return nil }
func (SimpleOAH) EqCksum(*Cksum) bool                { return false }
func (SimpleOAH) GetCustomMD() StrKVs                { return nil }
func (SimpleOAH) GetCustomKey(string) (string, bool) { return "", false }
func (SimpleOAH) SetCustomKey(_, _ string)           {}
func (SimpleOAH) String() string                     { return "" }
