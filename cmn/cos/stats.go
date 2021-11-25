// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cos

type (
	StatsTracker interface {
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
