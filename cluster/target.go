// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"context"
	"io"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

type RecvType int

const (
	ColdGet RecvType = iota
	WarmGet
)

type CloudProvider interface {
	ListBucket(ctx context.Context, bucket string, msg *cmn.SelectMsg) (bckList *cmn.BucketList, err error, errCode int)
}

// a callback called by EC PUT jogger after the object is processed and
// all its slices/replicas are sent to other targets
type OnFinishObj = func(lom *LOM, err error)
type ECManager interface {
	EncodeObject(lom *LOM, cb ...OnFinishObj) error
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
	ECM() ECManager
	PrefetchQueueLen() int
	RebalanceInfo() RebalanceInfo
	AvgCapUsed(config *cmn.Config, used ...int32) (capInfo cmn.CapacityInfo)
	RunLRU()
	Prefetch()

	GetObject(w io.Writer, lom *LOM, started time.Time) error
	PutObject(workFQN string, reader io.ReadCloser, lom *LOM, recvType RecvType, cksum *cmn.Cksum, started time.Time) error
	CopyObject(lom *LOM, bckTo *Bck, buf []byte) error
	GetCold(ctx context.Context, lom *LOM, prefetch bool) (error, int)
	PromoteFile(srcFQN string, bck *Bck, objName string, overwrite, safe, verbose bool) (err error)
}

type RebalanceInfo struct {
	IsRebalancing bool
	GlobalRebID   int64
}

type RebManager interface {
	RunLocalReb(skipMisplaced bool, bucket ...string)
	RunGlobalReb(smap *Smap, rebID int64, bucket ...string)
	BMDVersionFixup(bucket string, sleep bool)
}
