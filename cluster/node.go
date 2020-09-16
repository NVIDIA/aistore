// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"net/http"
	"time"
)

type Node interface {
	Snode() *Snode
	Bowner() Bowner
	Sowner() Sowner

	ClusterStarted() bool
	NodeStarted() bool
	NodeStartedTime() time.Time

	Client() *http.Client
}
