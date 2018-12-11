// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

// NameLocker interface locks and unlocks (and try-locks, etc.)
// arbitrary strings.
// NameLocker is currently utilized to lock objects stored in the cluster
// when there's a pending GET, PUT, etc. transaction and we don't want
// the object in question to get updated or evicted concurrently.
// Objects are locked by their unique (string) names aka unames.
// The lock can be exclusive (write) or shared (read).
//
// For implementation, please refer to dfc/rtnames.go

type NameLocker interface {
	TryLock(uname string, exclusive bool) bool
	Lock(uname string, exclusive bool)
	DowngradeLock(uname string)
	Unlock(uname string, exclusive bool)
}
