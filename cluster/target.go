// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"context"
	"io"

	"github.com/NVIDIA/aistore/atime"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

// For implementations, please refer to ais/target.go

type Target interface {
	IsRebalancing() bool
	RunLRU()
	PrefetchQueueLen() int
	Prefetch()
	GetBowner() Bowner
	FSHC(err error, path string)
	GetAtimeRunner() *atime.Runner
	GetMem2() *memsys.Mem2
	Receive(workfqn string, lom *LOM, omd5 string, reader io.Reader) (sgl *memsys.SGL /* NIY */, nhobj cmn.CksumValue, written int64, errstr string)
	Commit(ct context.Context, lom *LOM, tempfqn string, rebalance bool) (errstr string, errcode int)
	RegPathRunner(r fs.PathRunner)
	UnregPathRunner(r fs.PathRunner)
}
