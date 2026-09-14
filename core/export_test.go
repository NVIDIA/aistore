// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import "time"

// shard-index cache: synchronous eviction passes
func SidxEvictIdle(idle time.Duration) { g.sidx.evictIdle(idle) }
func SidxClearAll()                    { g.sidx.clearAll() }

type SidxCounters struct{ Miss, Load, Retry, Deny, Evict, Clear int64 }

func SidxStats() SidxCounters {
	s := &g.sidx.stats
	return SidxCounters{s.miss.Load(), s.load.Load(), s.retry.Load(), s.deny.Load(), s.evict.Load(), s.clear.Load()}
}
