// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

// For implementations, please refer to dfc/target.go

type Target interface {
	IsRebalancing() bool
	RunLRU()
	PrefetchQueueLen() int
	Prefetch()
}
