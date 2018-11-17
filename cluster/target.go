/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
// Package cluster provides local access to cluster-level metadata
package cluster

// For implementations, please refer to dfc/target.go

type Target interface {
	IsRebalancing() bool
	RunLRU()
	PrefetchQueueLen() int
	Prefetch()
}
