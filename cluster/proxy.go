// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

// NOTE: For implementations, please refer to ais/prxifimpl.go and ais/httpcommon.go
type Proxy interface {
	node
}
