// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package cos

// motivated by transport <-> stats cyclic dep (via core interfaces)

// intra-cluster transmit & receive (cumulative counters)
const (
	StreamsOutObjCount = "stream.out.n"
	StreamsOutObjSize  = "stream.out.size"
	StreamsInObjCount  = "stream.in.n"
	StreamsInObjSize   = "stream.in.size"
)

type (
	StatsUpdater interface {
		Inc(name string)
		Add(name string, val int64)
		Observe(name string, val float64)
		SetFlag(name string, set NodeStateFlags)
		ClrFlag(name string, clr NodeStateFlags)
		SetClrFlag(name string, set, clr NodeStateFlags)
		Get(name string) int64
		AddWith(namedVal64 ...NamedVal64)
		IncWith(name string, VarLabs map[string]string)
	}
	NamedVal64 struct {
		VarLabs map[string]string
		Name    string
		Value   int64
	}
)
