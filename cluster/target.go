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

type CloudProvider interface {
	ListBucket(ctx context.Context, bucket string, msg *cmn.SelectMsg) (bckList *cmn.BucketList, err error, errCode int)
}

// NOTE: For implementations, please refer to ais/tgtifimpl.go
type Target interface {
	GetBowner() Bowner
	FSHC(err error, path string)
	GetMem2() *memsys.Mem2
	GetFSPRG() fs.PathRunGroup
	GetSmap() *Smap
	Snode() *Snode
	Cloud() CloudProvider
	PrefetchQueueLen() int
	RebalanceInfo() RebalanceInfo
	AvgCapUsed(config *cmn.Config, used ...int32) (int32, bool)
	RunLRU()
	Prefetch()

	GetObject(w io.Writer, lom *LOM, started time.Time) error
	PutObject(workFQN string, reader io.ReadCloser, lom *LOM, recvType RecvType, cksum *cmn.Cksum, started time.Time) error
	CopyObject(lom *LOM, bckTo *Bck, buf []byte, uncache bool) error
	GetCold(ctx context.Context, lom *LOM, prefetch bool) (error, int)
}

type RebalanceInfo struct {
	IsRebalancing bool
	GlobalRebID   int64
}
