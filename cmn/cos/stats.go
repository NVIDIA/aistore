// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cos

// TODO: try resolve transport -> stats cyclic dep and move => stats/api.go

type (
	StatsUpdater interface {
		Inc(name string)
		Add(name string, val int64)
		Get(name string) int64
		AddMany(namedVal64 ...NamedVal64)
	}
	NamedVal64 struct {
		Name       string
		NameSuffix string // forces immediate send when non-empty (see NOTE below)
		Value      int64
	}
)
