// Package cos provides common low-level types and utilities for all aistore projects
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
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
		SetFlag(name string, set NodeStateFlags)
		ClrFlag(name string, clr NodeStateFlags)
		SetClrFlag(name string, set, clr NodeStateFlags)
		Get(name string) int64
		AddWith(namedVal64 ...NamedVal64)
	}
	NamedVal64 struct {
		Name    string
		Value   int64
		VarLabs map[string]string // (Prometheus only)
	}
)
