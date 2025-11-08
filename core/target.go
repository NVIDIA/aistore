// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
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
)

//
// ais target: types and interfaces
//

// intra-cluster data path: control structures and types
type (
	PutParams struct {
		Reader    io.ReadCloser
		Cksum     *cos.Cksum // checksum to check
		Atime     time.Time
		Xact      Xact
		WorkTag   string // (=> work fqn)
		Size      int64
		ChunkSize int64 // if set, the object will be chunked with this size regardless of the bucket's chunk properties
		OWT       cmn.OWT
		SkipEC    bool // don't erasure-code when finalizing
		ColdGET   bool // this PUT is in fact a cold-GET
		Locked    bool // true if the LOM is already locked by the caller
	}
	PromoteParams struct {
		Bck             *meta.Bck   // destination bucket
		Cksum           *cos.Cksum  // checksum to validate
		Config          *cmn.Config // during xaction
		Xact            Xact        // responsible xaction
		apc.PromoteArgs             // all of the above
	}

	BlobParams struct {
		Lom *LOM
		Msg *apc.BlobMsg

		// When `RespWriter` is set, `XactBlobDl` not only downloads chunks into the cluster,
		// but also stitches them together and sequentially writes to `RespWriter`.
		// This makes the blob downloading job synchronous and blocking until all chunks are written.
		// Only set this if you need to simultaneously download and write to the response writer (e.g., for streaming blob GET).
		RespWriter io.Writer
	}

	GfnParams struct {
		Lom      *LOM
		Tsi      *meta.Snode
		Config   *cmn.Config
		ArchPath string
		Size     int64
	}
)

type (
	// a node that can also write objects
	TargetPut interface {
		Node

		// Space
		OOS(*fs.CapStatus, *cmn.Config, *fs.Tcdf) fs.CapStatus

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

		fs.HC

		// backend
		Backend(*meta.Bck) Backend

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

		GetCold(ctx context.Context, lom *LOM, xkind string, owt cmn.OWT) (ecode int, err error)

		HeadCold(lom *LOM, origReq *http.Request) (objAttrs *cmn.ObjAttrs, ecode int, err error)

		Promote(params *PromoteParams) (ecode int, err error)
		HeadObjT2T(lom *LOM, si *meta.Snode) bool

		ECRestoreReq(ct *CT, si *meta.Snode, uuid string) error

		BMDVersionFixup(r *http.Request, bck ...cmn.Bck)

		GetFromNeighbor(params *GfnParams) (*http.Response, error)
	}
)
