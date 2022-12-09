// Package cluster provides local access to cluster-level metadata
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
)

//
// ais target: types and interfaces
//

type (
	Node interface {
		SID() string
		String() string
		Snode() *Snode

		Bowner() Bowner
		Sowner() Sowner

		ClusterStarted() bool
		NodeStarted() bool
	}

	NodeMemCap interface {
		Node

		// Memory allocators
		PageMM() *memsys.MMSA
		ByteMM() *memsys.MMSA

		// Space
		OOS(*fs.CapStatus) fs.CapStatus
	}

	// a node that can also write objects locally
	TargetPut interface {
		NodeMemCap

		PutObject(lom *LOM, params *PutObjectParams) (err error)
	}

	// For implementations, see `ais/tgtimpl.go` and `ais/htrun.go`.
	Target interface {
		TargetPut

		// (for intra-cluster data-net comm - no streams)
		DataClient() *http.Client

		// backend
		Backend(*Bck) BackendProvider
		CompareObjects(ctx context.Context, lom *LOM) (equal bool, errCode int, err error)

		// core object
		FinalizeObj(lom *LOM, workFQN string, xctn Xact) (errCode int, err error)
		EvictObject(lom *LOM) (errCode int, err error)
		DeleteObject(lom *LOM, evict bool) (errCode int, err error)
		CopyObject(lom *LOM, params *CopyObjectParams, dryRun bool) (int64, error)
		GetCold(ctx context.Context, lom *LOM, owt cmn.OWT) (errCode int, err error)
		Promote(params PromoteParams) (errCode int, err error)
		HeadObjT2T(lom *LOM, si *Snode) bool

		// FS health and Health
		FSHC(err error, path string)
		Health(si *Snode, timeout time.Duration, query url.Values) (body []byte, errCode int, err error)
	}

	TargetExt interface {
		Target

		// misc
		BMDVersionFixup(r *http.Request, bck ...cmn.Bck)
	}
)

// data path: control structures and types
type (
	OnFinishObj = func(lom *LOM, err error)

	DataMover interface {
		RegRecv() error
		GetXact() Xact
		Open()
		Close(err error)
		UnregRecv()
		Send(obj *transport.Obj, roc cos.ReadOpenCloser, tsi *Snode) error
		ACK(hdr transport.ObjHdr, cb transport.ObjSentCB, tsi *Snode) error
		OWT() cmn.OWT
	}
	PutObjectParams struct {
		Reader     io.ReadCloser
		Cksum      *cos.Cksum // checksum to check
		Atime      time.Time
		Xact       Xact
		WorkTag    string // (=> work fqn)
		OWT        cmn.OWT
		SkipEncode bool // don't run erasure-code when finalizing
	}
	CopyObjectParams struct {
		DM        DataMover
		DP        DP // Data Provider (optional; see Transform/Copy Bucket (TCB))
		Xact      Xact
		BckTo     *Bck
		ObjNameTo string
		Buf       []byte
	}
	// common part that's used in `api.PromoteArgs` and `PromoteParams`(server side), both
	PromoteArgs struct {
		DaemonID  string `json:"tid,omitempty"` // target ID
		SrcFQN    string `json:"src,omitempty"` // source file or directory (must be absolute pathname)
		ObjName   string `json:"obj,omitempty"` // destination object name
		Recursive bool   `json:"rcr,omitempty"` // recursively promote nested dirs
		// once successfully promoted:
		OverwriteDst bool `json:"ovw,omitempty"` // overwrite destination
		DeleteSrc    bool `json:"dls,omitempty"` // remove source when (and after) successfully promoting
		// explicit request _not_ to treat the source as a potential file share
		// and _not_ to try to auto-detect if it is;
		// (auto-detection takes time, etc.)
		SrcIsNotFshare bool `json:"notshr,omitempty"` // the source is not a file share equally accessible by all targets
	}
	PromoteParams struct {
		Bck         *Bck       // destination bucket
		Cksum       *cos.Cksum // checksum to validate
		Xact        Xact       // responsible xaction
		PromoteArgs            // all of the above
	}
)
