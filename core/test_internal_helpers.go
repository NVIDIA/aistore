// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import "time"

// shard-index cache: synchronous eviction passes
func SidxEvictIdleTestOnly(idle time.Duration) { g.sidx.evictIdle(idle) }
func SidxClearAllTestOnly()                    { g.sidx.clearAll() }

// shard-index cache: bytes loaded since the last memory sample
func SidxNbytesTestOnly() int64 { return g.sidx.nbytes.Load() }

type SidxCountersTestOnly struct{ Miss, Load, Fail, Wait, Deny, Evict, Clear int64 }

func SidxStatsTestOnly() SidxCountersTestOnly {
	s := &g.sidx.stats
	return SidxCountersTestOnly{s.miss.Load(), s.load.Load(), s.fail.Load(), s.wait.Load(), s.deny.Load(), s.evict.Load(), s.clear.Load()}
}
