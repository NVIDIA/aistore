// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import "time"

// shard-index cache: synchronous eviction passes
func SidxEvictIdle(idle time.Duration) { g.sidx.evictIdle(idle) }
func SidxClearAll()                    { g.sidx.clearAll() }

// shard-index cache: bytes loaded since the last memory sample
func SidxNbytes() int64 { return g.sidx.nbytes.Load() }

type SidxCounters struct{ Miss, Load, Wait, Deny, Evict, Clear int64 }

func SidxStats() SidxCounters {
	s := &g.sidx.stats
	return SidxCounters{s.miss.Load(), s.load.Load(), s.wait.Load(), s.deny.Load(), s.evict.Load(), s.clear.Load()}
}
