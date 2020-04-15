// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
)

type RecvType int

const (
	ColdGet RecvType = iota
	WarmGet
	Migrated
)

type GFNType int

const (
	GFNGlobal GFNType = iota
	GFNLocal
)

type CloudProvider interface {
	GetObj(ctx context.Context, fqn string, lom *LOM) (err error, errCode int)
	PutObj(ctx context.Context, r io.Reader, lom *LOM) (version string, err error, errCode int)
	DeleteObj(ctx context.Context, lom *LOM) (error, int)
	HeadObj(ctx context.Context, lom *LOM) (objMeta cmn.SimpleKVs, err error, errCode int)

	HeadBucket(ctx context.Context, bck cmn.Bck) (bucketProps cmn.SimpleKVs, err error, errCode int)
	ListObjects(ctx context.Context, bck cmn.Bck, msg *cmn.SelectMsg) (bckList *cmn.BucketList, err error, errCode int)
	ListBuckets(ctx context.Context) (buckets cmn.BucketNames, err error, errCode int)
}

// a callback called by EC PUT jogger after the object is processed and
// all its slices/replicas are sent to other targets
type OnFinishObj = func(lom *LOM, err error)

type GFN interface {
	Activate() bool
	Deactivate()
}

type PutObjectParams struct {
	LOM          *LOM
	Reader       io.ReadCloser
	WorkFQN      string
	RecvType     RecvType
	Cksum        *cmn.Cksum // checksum to check
	Started      time.Time
	WithFinalize bool // determines if we should also finalize the object
}

// NOTE: For implementations, please refer to ais/tgtifimpl.go
type Target interface {
	GetBowner() Bowner
	GetSowner() Sowner
	FSHC(err error, path string)
	GetMMSA() *memsys.MMSA
	GetSmallMMSA() *memsys.MMSA
	GetFSPRG() fs.PathRunGroup
	Snode() *Snode
	Cloud(provider string) CloudProvider
	RebalanceInfo() RebalanceInfo
	AvgCapUsed(config *cmn.Config, used ...int32) (capInfo cmn.CapacityInfo)
	RunLRU(id string)

	GetObject(w io.Writer, lom *LOM, started time.Time) error
	PutObject(params PutObjectParams) error
	CopyObject(lom *LOM, bckTo *Bck, buf []byte, localOnly bool) (bool, error)
	GetCold(ctx context.Context, lom *LOM, prefetch bool) (error, int)
	PromoteFile(srcFQN string, bck *Bck, objName string, overwrite, safe, verbose bool) (err error)
	LookupRemoteSingle(lom *LOM, si *Snode) bool
	CheckCloudVersion(ctx context.Context, lom *LOM) (vchanged bool, err error, errCode int)

	GetGFN(gfnType GFNType) GFN
	Health(si *Snode, includeReb bool, timeout time.Duration) ([]byte, error)
	RebalanceNamespace(si *Snode) ([]byte, int, error)
	BMDVersionFixup(r *http.Request, bck cmn.Bck, sleep bool)
}

type RebalanceInfo struct {
	IsRebalancing bool
	RebID         int64
}

type RebManager interface {
	RunResilver(id string, skipMisplaced bool)
	RunRebalance(smap *Smap, rebID int64)
}
