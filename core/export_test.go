// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package core

import "time"

// shard-index cache: synchronous eviction passes
func SidxEvictIdle(idle time.Duration) { g.sidx.evictIdle(idle) }
func SidxClearAll()                    { g.sidx.clearAll() }
