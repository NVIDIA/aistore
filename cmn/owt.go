// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

// Object Write Transaction (OWT) is used to control some of the aspects of creating
// new objects in the cluster.
// In particular, OwtGet* group below simultaneously specifies cold-GET variations
// (that all involve reading from a remote backend) and the associated locking
// (that will always reflect a tradeoff between consistency and parallelism)
type OWT int

const (
	OwtPut             OWT = iota // PUT
	OwtMigrate                    // used when migrating objects during global rebalance
	OwtFinalize                   // to finalize object archives
	OwtGetTryLock                 // if !try-lock(exclusive) { return error }; read from remote; ...
	OwtGetLock                    // lock(exclusive); read from remote; ...
	OwtGet                        // GET (with upgrading read-lock in the local-write path)
	OwtGetPrefetchLock            // used for maximum parallelism when prefetching
)
