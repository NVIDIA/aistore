/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package cluster provides local access to cluster-level metadata
package cluster

type NameLocker interface {
	// TryLock(name string, exclusive bool, info *pendinginfo) bool
}
