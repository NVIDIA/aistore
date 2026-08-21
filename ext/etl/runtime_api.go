// Package etl provides utilities to initialize and use transformation pods.
/*
 * Copyright (c) 2026, NVIDIA CORPORATION. All rights reserved.
 */
package etl

// Runtime-neutral contracts are shared by ETL-enabled and disabled builds.
// Runtime implementations live exclusively in build-tagged files.

import (
	"io"
	"net/http"
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/xact"
)

const MaxObjErr = 128

// ErrNotBuilt indicates that ETL runtime support was excluded at build time.
// Keep it interface-typed to avoid build-specific nilness propagation into shared callers
// (and also avoid lint staticcheck SA4023).
var ErrNotBuilt error = cmn.NewErrUnsupp("use ETL", "in a build without ETL support")

var _ core.Xact = (*XactETL)(nil)

type (
	// XactETL is the inline ETL xaction shared by enabled and disabled builds.
	XactETL struct {
		msg            InitMsg
		Vlabs          map[string]string
		offlineObjErrs map[string]*cos.Errs // xid of TCB/TCB => errors encountered during offline transformation
		InlineObjErrs  cos.Errs
		xact.Base
		ctlmsg string
		m      sync.Mutex // protects offlineErrs
	}

	CommStats interface {
		ObjCount() int64
		InBytes() int64
		OutBytes() int64
	}

	// Communicator is responsible for managing communications with local ETL pod.
	Communicator interface {
		ETLName() string
		getInitMsg() InitMsg

		String() string

		setupConnection(schema, podAddr string) (ecode int, err error)
		setupXaction(xid string) error
		stop() error
		GetSecret() string
		Xact() *XactETL // underlying `apc.ActETLInline` xaction (see xact/xs/etl.go)
		CommStats       // only stats for `apc.ActETLInline` inline transform

		// InlineTransform uses one of the two ETL container endpoints:
		//  - Method "PUT", Path "/"
		//  - Method "GET", Path "/bucket/object"
		//  - Returns:
		//    - size: the size of transformed object
		//    - ecode: error code
		//    - err: error encountered during transformation
		InlineTransform(w http.ResponseWriter, r *http.Request, lom *core.LOM, args *InlineTransArgs) (size int64, ecode int, err error)

		// ProcessDownloadJob extracts objects from job and routes them to ETL pod
		ProcessDownloadJob(ctx *ETLObjDownloadCtx) (cos.ReadCloseSizer, int, error)
	}

	InlineTransArgs struct {
		TransformArgs string
		Pipeline      apc.ETLPipeline
		LatestVer     bool
		// TODO: add sync option
	}

	// Session represents a per-xaction communication context created by the statefulCommunicator.
	Session interface {
		// Finish cleans up the job's communication channel, and aborts the undergoing xaction (`TCB`/`TCO`) if errCause is provided
		Finish(errCause error) error
		OfflineWrite(lom *core.LOM, latestVer, sync bool, writer io.WriteCloser, args *core.ETLArgs) (written int64, ecode int, err error)

		transform(lom *core.LOM, latestVer, sync bool, writer io.WriteCloser, args *core.ETLArgs) (written int64, ecode int, err error)
	}
)

func (*XactETL) Run(*sync.WaitGroup) { debug.Assert(false) }

func (r *XactETL) CtlMsg() string {
	if r.ctlmsg != "" || r.msg == nil {
		return r.ctlmsg
	}
	r.ctlmsg = r.msg.String()
	return r.ctlmsg
}

func (r *XactETL) Snap() *core.Snap { return r.Base.NewSnap(r) }
