// Package cmn provides common constants, types, and utilities for AIS clients
// and AIStore.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package cmn

import (
	"strconv"

	"github.com/NVIDIA/aistore/cmn/debug"
)

// Object Write Transaction (OWT) controls certain aspects of creating new objects in the cluster.
// In particular, OwtGet* group below simultaneously specifies cold-GET variations
// (that all involve reading from a remote backend) and the associated locking
// (that will always reflect a tradeoff between consistency and parallelism)
// NOTE: enum entries' order is important
type OWT int

const (
	// PUT and PUT-like transactions
	OwtPut       OWT = iota // PUT
	OwtPromote              // promote target-accessible files and directories
	OwtFinalize             // finalize object archives and S3 multipart
	OwtTransform            // ETL
	OwtCopy                 // copy and move objects within cluster
	OwtRebalance            // NOTE: must be the last in PUT* group
	//
	// GET and friends
	//
	OwtGetTryLock      // if !try-lock(exclusive) { return error }; read from remote; ...
	OwtGetLock         // lock(exclusive); read from remote; ...
	OwtGet             // GET (with upgrading read-lock in the local-write path)
	OwtGetPrefetchLock // (used for maximum parallelism when prefetching)
)

func (owt *OWT) FromS(s string) {
	n, err := strconv.Atoi(s)
	debug.AssertNoErr(err)
	*owt = OWT(n)
}

func (owt OWT) ToS() (s string) { return strconv.Itoa(int(owt)) }

func (owt OWT) String() (s string) {
	switch owt {
	case OwtPut:
		s = "owt-put"
	case OwtPromote:
		s = "owt-promote"
	case OwtFinalize:
		s = "owt-finalize"
	case OwtTransform:
		s = "owt-transform"
	case OwtCopy:
		s = "owt-copy"
	case OwtRebalance:
		s = "owt-rebalance"
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
