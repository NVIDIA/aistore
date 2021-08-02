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
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/dbdriver"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
)

type (
	GFN interface {
		Activate() bool
		Deactivate()
	}
)

// poi RecvType and enum
type RecvType int

const (
	RegularPut RecvType = iota
	ColdGet
	Migrated
	Finalize
)

// GetColdType and enum
type GetColdType uint8

const (
	// Prefetch errors if lock is not available to be acquired immediately.
	// Otherwise acquire, create an object, release the lock.
	Prefetch GetColdType = iota
	// PrefetchWait waits until a lock is acquired, create an object, release the lock.
	PrefetchWait
	// GetCold waits until a lock is acquired, create an object, downgrade the lock.
	GetCold
)

// GFNType and enum
type GFNType int

const (
	GFNGlobal GFNType = iota
	GFNLocal
)

//
// ais target's types and interfaces
//

type (
	BackendProvider interface {
		Provider() string
		MaxPageSize() uint

		CreateBucket(bck *Bck) (errCode int, err error)
		ListObjects(bck *Bck, msg *cmn.SelectMsg) (bckList *cmn.BucketList, errCode int, err error)
		ListBuckets(query cmn.QueryBcks) (bcks cmn.Bcks, errCode int, err error)
		PutObj(r io.ReadCloser, lom *LOM) (version string, errCode int, err error)
		DeleteObj(lom *LOM) (errCode int, err error)

		// with context
		HeadBucket(ctx context.Context, bck *Bck) (bckProps cos.SimpleKVs, errCode int, err error)
		HeadObj(ctx context.Context, lom *LOM) (objMeta cos.SimpleKVs, errCode int, err error)
		GetObj(ctx context.Context, lom *LOM) (errCode int, err error)
		GetObjReader(ctx context.Context, lom *LOM) (r io.ReadCloser, expectedCksum *cos.Cksum, errCode int, err error)
	}

	// Callback called by EC PUT jogger after the object is processed and
	// all its slices/replicas are sent to other targets.
	OnFinishObj = func(lom *LOM, err error)

	DataMover interface {
		RegRecv() error
		SetXact(xact Xact)
		Open()
		Close(err error)
		UnregRecv()
		Send(obj *transport.Obj, roc cos.ReadOpenCloser, tsi *Snode) error
		ACK(hdr transport.ObjHdr, cb transport.ObjSentCB, tsi *Snode) error
		RecvType() RecvType
	}
	PutObjectParams struct {
		Tag        string // Used to distinguish between different PUT operation.
		Reader     io.ReadCloser
		RecvType   RecvType
		Cksum      *cos.Cksum // Checksum to check.
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
		Reader    cos.ReadOpenCloser
		BckTo     *Bck
		ObjNameTo string
		Tsi       *Snode
		DM        DataMover
		ObjAttrs  cmn.ObjAttrsHolder
	}
	PromoteFileParams struct {
		SrcFQN    string
		Bck       *Bck
		ObjName   string
		Cksum     *cos.Cksum
		Overwrite bool
		KeepOrig  bool
	}
)

type Node interface {
	Snode() *Snode
	Bowner() Bowner
	Sowner() Sowner

	ClusterStarted() bool
	NodeStarted() bool

	DataClient() *http.Client
}

// For implementations, please refer to `ais/tgtifimpl.go` and `ais/httpcommon.go`.
type Target interface {
	Node
	// convenience methods (shortcuts)
	Sname() string
	SID() string

	// Memory related functions.
	MMSA() *memsys.MMSA
	SmallMMSA() *memsys.MMSA

	// Backend provider(s) related functions.
	Backend(*Bck) BackendProvider
	CheckRemoteVersion(ctx context.Context, lom *LOM) (vchanged bool, errCode int, err error)

	// Object related functions.
	PutObject(lom *LOM, params PutObjectParams) (err error)
	FinalizeObj(lom *LOM, workFQN string) (errCode int, err error)
	EvictObject(lom *LOM) (errCode int, err error)
	DeleteObject(lom *LOM, evict bool) (errCode int, err error)
	CopyObject(lom *LOM, params *CopyObjectParams, localOnly bool) (int64, error)
	GetCold(ctx context.Context, lom *LOM, getType GetColdType) (errCode int, err error)
	PromoteFile(params PromoteFileParams) (lom *LOM, err error)
	LookupRemoteSingle(lom *LOM, si *Snode) bool

	// File-system related functions.
	FSHC(err error, path string)
	RunLRU(id string, wg *sync.WaitGroup, force bool, bcks ...cmn.Bck)

	// Getting other interfaces.
	DB() dbdriver.Driver
	GFN(gfnType GFNType) GFN

	// Other.
	BMDVersionFixup(r *http.Request, bck ...cmn.Bck)
	RebalanceNamespace(si *Snode) (body []byte, errCode int, err error)
	Health(si *Snode, timeout time.Duration, query url.Values) (body []byte, errCode int, err error)
}
