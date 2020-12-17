// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dbdriver"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
)

type (
	RecvType    int
	GetColdType uint8

	GFNType int
	GFN     interface {
		Activate() bool
		Deactivate()
	}
)

const (
	RegularPut RecvType = iota
	ColdGet
	Migrated

	// Error if lock is not available to be acquired immediately. Otherwise acquire, create an object, release the lock.
	Prefetch GetColdType = iota
	// Wait until a lock is acquired, create an object, release the lock.
	PrefetchWait
	// Wait until a lock is acquired, create an object, downgrade the lock.
	GetCold

	GFNGlobal GFNType = iota
	GFNLocal
)

type CloudProvider interface {
	Provider() string
	MaxPageSize() uint
	// GetObj fetches and finalizes the object from the cloud.
	GetObj(ctx context.Context, lom *LOM) (errCode int, err error)
	GetObjReader(ctx context.Context, lom *LOM) (r io.ReadCloser, expectedCksum *cmn.Cksum, errCode int, err error)
	PutObj(ctx context.Context, r io.Reader, lom *LOM) (version string, errCode int, err error)
	DeleteObj(ctx context.Context, lom *LOM) (errCode int, err error)
	HeadObj(ctx context.Context, lom *LOM) (objMeta cmn.SimpleKVs, errCode int, err error)

	HeadBucket(ctx context.Context, bck *Bck) (bckProps cmn.SimpleKVs, errCode int, err error)
	ListObjects(ctx context.Context, bck *Bck, msg *cmn.SelectMsg) (bckList *cmn.BucketList, errCode int, err error)
	ListBuckets(ctx context.Context, query cmn.QueryBcks) (buckets cmn.BucketNames, errCode int, err error)
}

// Callback called by EC PUT jogger after the object is processed and
// all its slices/replicas are sent to other targets.
type OnFinishObj = func(lom *LOM, err error)

type (
	DataMover interface {
		RegRecv() error
		SetXact(xact Xact)
		Open()
		Close(err error)
		UnregRecv()
		Send(obj *transport.Obj, roc cmn.ReadOpenCloser, tsi *Snode) error
		ACK(hdr transport.ObjHdr, cb transport.ObjSentCB, tsi *Snode) error
		RecvType() RecvType
	}
	PutObjectParams struct {
		Tag        string // Used to distinguish between different PUT operation.
		Reader     io.ReadCloser
		RecvType   RecvType
		Cksum      *cmn.Cksum // Checksum to check.
		Started    time.Time
		SkipEncode bool // Do not run EC encode after finalizing.
	}
	CopyObjectParams struct {
		BckTo     *Bck
		ObjNameTo string
		Buf       []byte
		DM        DataMover
		DP        LomReaderProvider // optional
		DryRun    bool
	}
	SendToParams struct {
		Reader    cmn.ReadOpenCloser
		BckTo     *Bck
		ObjNameTo string
		Tsi       *Snode
		DM        DataMover
		Locked    bool
		HdrMeta   cmn.ObjHeaderMetaProvider
	}
	PromoteFileParams struct {
		SrcFQN    string
		Bck       *Bck
		ObjName   string
		Cksum     *cmn.Cksum
		Overwrite bool
		KeepOrig  bool
	}
)

// NOTE: For implementations, please refer to `ais/tgtifimpl.go` and `ais/httpcommon.go`.
type Target interface {
	Node

	// Memory related functions.
	MMSA() *memsys.MMSA
	SmallMMSA() *memsys.MMSA

	// Cloud related functions.
	Cloud(*Bck) CloudProvider
	CheckCloudVersion(ctx context.Context, lom *LOM) (vchanged bool, errCode int, err error)

	// Object related functions.
	GetObject(w io.Writer, lom *LOM, started time.Time) error
	PutObject(lom *LOM, params PutObjectParams) (err error)
	EvictObject(lom *LOM) error
	DeleteObject(ctx context.Context, lom *LOM, evict bool) (errCode int, err error)
	CopyObject(lom *LOM, params CopyObjectParams, localOnly bool) (bool, int64, error)
	GetCold(ctx context.Context, lom *LOM, getType GetColdType) (errCode int, err error)
	PromoteFile(params PromoteFileParams) (lom *LOM, err error)
	LookupRemoteSingle(lom *LOM, si *Snode) bool

	// File-system related functions.
	FSHC(err error, path string)
	RunLRU(id string, force bool, bcks ...cmn.Bck)

	// Getting other interfaces.
	DB() dbdriver.Driver
	GFN(gfnType GFNType) GFN

	// Other.
	BMDVersionFixup(r *http.Request, bck cmn.Bck, sleep bool)
	RebalanceNamespace(si *Snode) (body []byte, errCode int, err error)
	Health(si *Snode, timeout time.Duration, query url.Values) (body []byte, errCode int, err error)
}

type RebManager interface {
	RunResilver(id string, skipMisplaced bool)
	RunRebalance(smap *Smap, rebID int64)
}
