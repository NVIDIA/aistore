// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"context"
	"io"
	"time"

	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"

	"github.com/NVIDIA/aistore/cmn"
)

type RecvType int

const (
	ColdGet RecvType = iota
	WarmGet
)

type CloudIf interface {
	ListBucket(ctx context.Context, bucket string, msg *cmn.SelectMsg) (bckList *cmn.BucketList, err error, errcode int)
}

// For implementations, please refer to ais/target.go
type Target interface {
	AvgCapUsed(config *cmn.Config, used ...int32) (int32, bool)
	Snode() *Snode
	FSHC(err error, path string)
	GetBowner() Bowner
	GetFSPRG() fs.PathRunGroup
	GetMem2() *memsys.Mem2
	HRWTarget(bucket, objname string) (si *Snode, errstr string)
	IsRebalancing() bool
	Prefetch()
	PrefetchQueueLen() int
	PutObject(workFQN string, reader io.ReadCloser, lom *LOM, recvType RecvType, cksum cmn.Cksummer, started time.Time) error
	GetObject(w io.Writer, lom *LOM, started time.Time) error
	GetCold(ctx context.Context, lom *LOM, prefetch bool) (string, int)
	RunLRU()
	CloudIntf() CloudIf
}
