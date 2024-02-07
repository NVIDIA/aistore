// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
)

//
// ais target: types and interfaces
//

type (
	NodeMemCap interface {
		Node

		// Memory allocators
		PageMM() *memsys.MMSA
		ByteMM() *memsys.MMSA

		// Space
		OOS(*fs.CapStatus) fs.CapStatus

		// xactions (jobs) now
		GetAllRunning(inout *AllRunningInOut, periodic bool)
	}

	// a node that can also write objects
	TargetPut interface {
		NodeMemCap

		// local PUT
		PutObject(lom *LOM, params *PutParams) (err error)
	}

	// local target node
	TargetLoc interface {
		TargetPut

		// backend
		Backend(*meta.Bck) BackendProvider

		// FS health and Health
		FSHC(err error, path string)
		Health(si *meta.Snode, timeout time.Duration, query url.Values) (body []byte, errCode int, err error)
	}

	// all of the above; for implementations, see `ais/tgtimpl.go` and `ais/htrun.go`
	Target interface {
		TargetLoc

		// (for intra-cluster data-net comm - no streams)
		DataClient() *http.Client

		// core object (+ PutObject above)
		FinalizeObj(lom *LOM, workFQN string, xctn Xact, owt cmn.OWT) (errCode int, err error)
		EvictObject(lom *LOM) (errCode int, err error)
		DeleteObject(lom *LOM, evict bool) (errCode int, err error)
		GetCold(ctx context.Context, lom *LOM, owt cmn.OWT) (errCode int, err error)
		CopyObject(lom *LOM, dm DM, coi *CopyParams) (int64, error)
		Promote(params *PromoteParams) (errCode int, err error)
		HeadObjT2T(lom *LOM, si *meta.Snode) bool

		BMDVersionFixup(r *http.Request, bck ...cmn.Bck)
	}
)

// intra-cluster data path: control structures and types
type (
	OnFinishObj = func(lom *LOM, err error)

	DM interface {
		Send(obj *transport.Obj, roc cos.ReadOpenCloser, tsi *meta.Snode) error
	}

	PutParams struct {
		Reader  io.ReadCloser
		Cksum   *cos.Cksum // checksum to check
		Atime   time.Time
		Xact    Xact
		WorkTag string // (=> work fqn)
		Size    int64
		OWT     cmn.OWT
		SkipEC  bool // don't erasure-code when finalizing
		ColdGET bool // this PUT is in fact a cold-GET
	}
	PromoteParams struct {
		Bck             *meta.Bck   // destination bucket
		Cksum           *cos.Cksum  // checksum to validate
		Config          *cmn.Config // during xaction
		Xact            Xact        // responsible xaction
		apc.PromoteArgs             // all of the above
	}
	CopyParams struct {
		DP        DP // transform via: ext/etl/dp.go or core/ldp.go
		Xact      Xact
		Config    *cmn.Config
		BckTo     *meta.Bck
		ObjnameTo string
		Buf       []byte
		OWT       cmn.OWT
		Finalize  bool // copies and EC (as in poi.finalize())
		DryRun    bool
		LatestVer bool // can be used without changing bucket's 'versioning.validate_warm_get'; see also: QparamLatestVer
		Sync      bool // ditto -  bucket's 'versioning.synchronize'
	}
)
