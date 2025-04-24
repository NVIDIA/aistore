// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"sync"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/transport/bundle"
)

type (
	CoiParams struct {
		Xact   core.Xact
		OAH    cos.OAH // object attributes after applying core.GetROC
		Config *cmn.Config
		BckTo  *meta.Bck
		core.GetROC
		ObjnameTo       string
		Buf             []byte
		OWT             cmn.OWT
		Finalize        bool // copies and EC (as in poi.finalize())
		DryRun          bool // no changes
		LatestVer       bool // can be used without changing bucket's 'versioning.validate_warm_get'; see also: QparamLatestVer
		Sync            bool // see core.GetROC at core/ldp.go
		ContinueOnError bool // when false, a failure to copy triggers abort
	}
	CoiRes struct {
		Err   error
		Lsize int64
		Ecode int
		RGET  bool // when reading source via backend.GetObjReader
	}

	COI interface {
		CopyObject(lom *core.LOM, dm *bundle.DM, coi *CoiParams) CoiRes
	}
)

// target i/f (ais/tgtimpl)
var (
	gcoi COI
)

// mem pool
var (
	coiPool sync.Pool
	coi0    CoiParams
)

//
// CoiParams pool
//

func AllocCOI() (a *CoiParams) {
	if v := coiPool.Get(); v != nil {
		a = v.(*CoiParams)
		return
	}
	return &CoiParams{}
}

func FreeCOI(a *CoiParams) {
	*a = coi0
	coiPool.Put(a)
}

//
// tcb/tcobjs common part (internal)
//

type (
	copier struct {
		r      core.Xact
		bp     core.Backend // backend(source bucket)
		getROC core.GetROC
		rate   tcrate
		vlabs  map[string]string
	}
)

func (tc *copier) prepare(lom *core.LOM, bckTo *meta.Bck, msg *apc.TCBMsg, config *cmn.Config, buf []byte, owt cmn.OWT) *CoiParams {
	toName := msg.ToName(lom.ObjName)
	if cmn.Rom.FastV(5, cos.SmoduleXs) {
		nlog.Infoln(tc.r.Name(), lom.Cname(), "=>", bckTo.Cname(toName))
	}

	// apply frontend rate-limit, if any
	tc.rate.acquire()

	a := AllocCOI()
	{
		a.GetROC = tc.getROC
		a.Xact = tc.r
		a.Config = config
		a.BckTo = bckTo
		a.ObjnameTo = toName
		a.Buf = buf
		a.OWT = owt
		a.DryRun = msg.DryRun
		a.LatestVer = msg.LatestVer
		a.Sync = msg.Sync
		a.Finalize = false
		a.ContinueOnError = msg.ContinueOnError
	}

	return a
}

func (tc *copier) do(a *CoiParams, lom *core.LOM, dm *bundle.DM) (err error) {
	var started int64
	if tc.bp != nil {
		started = mono.NanoTime()
	}

	res := gcoi.CopyObject(lom, dm, a)
	contOnErr := a.ContinueOnError
	FreeCOI(a)

	switch {
	case res.Err == nil:
		debug.Assert(res.Lsize != cos.ContentLengthUnknown)
		tc.r.ObjsAdd(1, res.Lsize)
		if res.RGET {
			// RGET stats (compare with ais/tgtimpl namesake)
			debug.Assert(tc.bp != nil)
			rgetstats(tc.bp /*from*/, tc.vlabs, res.Lsize, started)
		}
	case cos.IsNotExist(res.Err, 0):
		// do nothing
	case res.Err == cmn.ErrSkip:
		// do nothing as well
	case cos.IsErrOOS(res.Err):
		err = res.Err
		tc.r.Abort(err)
	default:
		if cmn.Rom.FastV(5, cos.SmoduleXs) {
			nlog.Warningln(tc.r.Name(), lom.Cname(), res.Err)
		}
		if contOnErr {
			tc.r.AddErr(res.Err, 5, cos.SmoduleXs)
		} else {
			err = res.Err
			tc.r.Abort(err)
		}
	}

	return err
}
