// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import "github.com/NVIDIA/aistore/cmn/debug"

// Object Write Transaction (OWT) is used to control some of the aspects of creating
// new objects in the cluster.
// In particular, OwtGet* group below simultaneously specifies cold-GET variations
// (that all involve reading from a remote backend) and the associated locking
// (that will always reflect a tradeoff between consistency and parallelism)
type OWT int

const (
	OwtPut             OWT = iota // PUT
	OwtMigrate                    // migrate or replicate objects within cluster (e.g. global rebalance)
	OwtPromote                    // promote target-accessible files and directories
	OwtFinalize                   // finalize object archives
	OwtGetTryLock                 // if !try-lock(exclusive) { return error }; read from remote; ...
	OwtGetLock                    // lock(exclusive); read from remote; ...
	OwtGet                        // GET (with upgrading read-lock in the local-write path)
	OwtGetPrefetchLock            // (used for maximum parallelism when prefetching)
)

func (owt OWT) String() (s string) {
	switch owt {
	case OwtPut:
		s = "owt-put"
	case OwtMigrate:
		s = "owt-migrate"
	case OwtPromote:
		s = "owt-promote"
	case OwtFinalize:
		s = "owt-finalize"
	case OwtGetTryLock:
		s = "owt-get-try-lock"
	case OwtGetLock:
		s = "owt-get-lock"
	case OwtGet:
		s = "owt-get"
	case OwtGetPrefetchLock:
		s = "owt-prefetch-lock"
	default:
		debug.Assert(false)
	}
	return
}
