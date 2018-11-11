/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
// Package cluster provides local access to cluster-level metadata
package cluster

// For implementation, please refer to dfc/target.go

type Rebalancer interface {
	IsRebalancing() bool
}
