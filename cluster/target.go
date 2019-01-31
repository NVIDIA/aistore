// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"io"

	"github.com/NVIDIA/aistore/atime"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

// For implementations, please refer to ais/target.go

type Target interface {
	OOS(oos ...bool) bool
	IsRebalancing() bool
	RunLRU()
	PrefetchQueueLen() int
	Prefetch()
	GetBowner() Bowner
	FSHC(err error, path string)
	GetAtimeRunner() *atime.Runner
	GetMem2() *memsys.Mem2
	Receive(workFQN string, reader io.ReadCloser, lom *LOM) error
	GetFSPRG() fs.PathRunGroup
}
