// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"context"
	"io"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

type RecvType int

const (
	ColdGet RecvType = iota
	WarmGet
)

// For implementations, please refer to ais/target.go
type Target interface {
	AvgCapUsed(config *cmn.Config, used ...int32) (int32, bool)
	IsRebalancing() bool
	RunLRU()
	PrefetchQueueLen() int
	Prefetch()
	GetBowner() Bowner
	FSHC(err error, path string)
	GetMem2() *memsys.Mem2
	GetCold(ctx context.Context, lom *LOM, prefetch bool) (string, int)
	Receive(workFQN string, reader io.ReadCloser, lom *LOM, recvType RecvType, cksum cmn.Cksummer) error
	GetFSPRG() fs.PathRunGroup
}
