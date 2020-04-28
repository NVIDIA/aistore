// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

type Proxy interface {
	Snode() *Snode
	Started() bool
}
