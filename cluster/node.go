// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import "github.com/NVIDIA/aistore/cluster/meta"

const (
	Targets = iota // 0 (cluster.Targets) used as default value for NewStreamBundle
	Proxies
	AllNodes
)

type (
	// cluster node
	Node interface {
		SID() string
		String() string
		Snode() *meta.Snode

		Bowner() meta.Bowner
		Sowner() meta.Sowner

		ClusterStarted() bool
		NodeStarted() bool
	}
)
