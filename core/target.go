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
		DP        DP // copy or transform via data provider, see impl-s: (ext/etl/dp.go, core/ldp.go)
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

	// blob
	WriteSGL func(*memsys.SGL) error

	BlobParams struct {
		Lmfh     cos.LomWriter
		RspW     http.ResponseWriter // (GET)
		WriteSGL WriteSGL            // custom write
		Lom      *LOM
		Msg      *apc.BlobMsg
		Wfqn     string
	}
)

type (
	// a node that can also write objects
	TargetPut interface {
		Node

		// Space
		OOS(*fs.CapStatus, *cmn.Config, *fs.TargetCDF) fs.CapStatus

		// xactions (jobs) now
		GetAllRunning(inout *AllRunningInOut, periodic bool)

		// PUT params.Reader => lom
		PutObject(lom *LOM, params *PutParams) (err error)

		// utilize blob downloader to cold-GET => (lom | custom write callback)
		GetColdBlob(params *BlobParams, oa *cmn.ObjAttrs) (xctn Xact, err error)
	}

	// local target node
	TargetLoc interface {
		TargetPut

		// backend
		Backend(*meta.Bck) Backend

		// FS health
		FSHC(err error, mi *fs.Mountpath, fqn string)

		// Node health
		Health(si *meta.Snode, timeout time.Duration, query url.Values) (body []byte, ecode int, err error)
	}

	// all of the above; for implementations, see `ais/tgtimpl.go` and `ais/htrun.go`
	Target interface {
		TargetLoc

		// target <=> target & target => backend (no streams)
		DataClient() *http.Client

		// core object (+ PutObject above)
		FinalizeObj(lom *LOM, workFQN string, xctn Xact, owt cmn.OWT) (ecode int, err error)
		EvictObject(lom *LOM) (ecode int, err error)
		DeleteObject(lom *LOM, evict bool) (ecode int, err error)

		GetCold(ctx context.Context, lom *LOM, owt cmn.OWT) (ecode int, err error)

		CopyObject(lom *LOM, dm DM, coi *CopyParams) (int64, error)
		Promote(params *PromoteParams) (ecode int, err error)
		HeadObjT2T(lom *LOM, si *meta.Snode) bool

		BMDVersionFixup(r *http.Request, bck ...cmn.Bck)
	}
)
