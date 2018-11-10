/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package cluster provides local access to cluster-level metadata
package cluster

// For implementation, please refer to dfc/throttle.go
type Throttler interface {
	Throttle()
}
