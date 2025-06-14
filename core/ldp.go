// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"context"
	"io"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
)

type (
	// ReadResp represents the status, attributes, and reader of an object from the source provider
	// - Ecode 200 (http.StatusOK): the object is successfully retrieved; content available in `R`.
	// - Ecode 204 (http.StatusNoContent): object has already been delivered directly to `daddr`, R is nil.
	// 	- ref: https://www.rfc-editor.org/rfc/rfc9110.html#name-204-no-content
	ReadResp struct {
		R      cos.ReadOpenCloser // Reader for the object content, may be nil if already delivered
		OAH    cos.OAH
		Err    error
		Ecode  int
		Remote bool
	}
	// GetROC defines a function that retrieves an object based on the given `lom` and flags.
	// If `daddr` are provided, the implementation may choose to deliver the object directly,
	// in which case the returned ReadResp will have R = nil and Ecode = 204.
	//
	// Implementations include:
	// - `core.DefaultGetROC`: fetches from local or remote backend
	// - `etl.HTTPCommunicator.OfflineTransform`: fetches transformed object from ETL pod through HTTP response
	// - `etl.statefulCommnicator.Transform`: fetches transformed object from ETL pod through WebSocket message
	GetROCArgs struct {
		Daddr         string // destination target's address for direct put
		TransformArgs string // optional and ETL-specific; can be used to indicate transformation on a per-object basis
		Local         bool   // true when the destination is local; used by communicator to handle direct put
	}
	GetROC func(lom *LOM, latestVer, sync bool, args *GetROCArgs) ReadResp

	PutWOC func(lom *LOM, latestVer, sync bool, woc io.WriteCloser, args *GetROCArgs) (written int64, ecode int, err error)

	// returned by lom.CheckRemoteMD
	CRMD struct {
		Err      error
		ObjAttrs *cmn.ObjAttrs
		ErrCode  int
		Eq       bool
	}
)

type (
	// compare with `deferROC` from cmn/cos/io.go
	deferROC struct {
		cos.ReadOpenCloser
		lif LIF
	}
)

func (r *deferROC) Close() (err error) {
	err = r.ReadOpenCloser.Close()
	r.lif.Unlock(false)
	return
}

// is called under rlock; unlocks on fail
// NOTE: compare w/ lom.Open() returning cos.LomReader
func (lom *LOM) NewDeferROC(loaded bool) (cos.ReadOpenCloser, error) {
	lh, err := lom.NewHandle(loaded)
	if err == nil {
		return &deferROC{lh, lom.LIF()}, nil
	}
	lom.Unlock(false)
	return nil, cmn.NewErrFailedTo(T, "open", lom.Cname(), err)
}

func (lom *LOM) GetROC(latestVer, sync bool) (resp ReadResp) {
	bck := lom.Bck()

	lom.Lock(false)
	lom.SetAtimeUnix(time.Now().UnixNano())
	loadErr := lom.Load(false /*cache it*/, true /*locked*/)

	// loaded: check ver
	if loadErr == nil {
		if latestVer || sync {
			debug.Assert(bck.IsRemote(), bck.String()) // caller's responsibility
			crmd := lom.CheckRemoteMD(true /* rlocked*/, sync, nil /*origReq*/)
			if crmd.Err != nil {
				lom.Unlock(false)
				resp.Err, resp.Ecode = crmd.Err, crmd.ErrCode
				if !cos.IsNotExist(crmd.Err, crmd.ErrCode) {
					resp.Err = cmn.NewErrFailedTo(T, "head-latest", lom.Cname(), crmd.Err)
				}
				return resp
			}
			if !crmd.Eq {
				// version changed
				lom.Unlock(false)
				goto remote // ---->
			}
		}

		resp.R, resp.Err = lom.NewDeferROC(true) // keeping lock, reading local
		resp.OAH = lom
		return resp
	}

	lom.Unlock(false)
	if !cos.IsNotExist(loadErr, 0) {
		resp.Err = cmn.NewErrFailedTo(T, "ldp-load", lom.Cname(), loadErr)
		return resp
	}
	if !bck.IsRemote() {
		resp.Err, resp.Ecode = cos.NewErrNotFound(T, lom.Cname()), http.StatusNotFound
		return resp
	}

remote:
	// GetObjReader and return remote (object) reader and oah for object metadata
	// (compare w/ T.GetCold)
	res := T.Backend(bck).GetObjReader(context.Background(), lom, 0, 0)

	if res.Err != nil {
		resp.Err, resp.Ecode = res.Err, res.ErrCode
		return resp
	}

	oah := &cmn.ObjAttrs{
		Cksum:    res.ExpCksum,
		CustomMD: lom.GetCustomMD(),
		Atime:    lom.AtimeUnix(),
		Size:     res.Size,
	}
	resp.OAH = oah
	resp.Remote = true

	// [NOTE] ref 6079834
	// non-trivial limitation: this reader cannot be transmitted to
	// multiple targets (where we actually rely on real re-opening);
	// current usage: tcb/tco
	resp.R = cos.NopOpener(res.R)
	return resp
}

func GetDefaultROC(lom *LOM, latestVer, sync bool, _ *GetROCArgs) ReadResp {
	return lom.GetROC(latestVer, sync)
}

// NOTE:
// - [PRECONDITION]: `versioning.validate_warm_get` || QparamLatestVer
// - [Sync] when Sync option is used (via bucket config and/or `sync` argument) caller MUST take wlock or rlock
// - [MAY] delete remotely-deleted (non-existing) object and increment associated stats counter
//
// also returns `NotFound` after removing local replica - the Sync option
func (lom *LOM) CheckRemoteMD(locked, sync bool, origReq *http.Request) (res CRMD) {
	bck := lom.Bck()
	if !bck.HasVersioningMD() {
		// nothing to do with: in-cluster ais:// bucket, or a remote one
		// that doesn't provide any versioning metadata
		return CRMD{Eq: true}
	}

	oa, ecode, err := T.HeadCold(lom, origReq)
	if err == nil {
		e := lom.CheckEq(oa)
		if !lom.IsFeatureSet(feat.DisableColdGET) || e == nil {
			debug.Assert(ecode == 0, ecode)
			return CRMD{ObjAttrs: oa, Eq: e == nil, ErrCode: ecode}
		}
		// Cold Get disabled and metadata doesn't match, so we must treat
		// it as if the object doesn't really exist.
		err = cmn.NewErrRemoteMetadataMismatch(e)
		ecode = http.StatusNotFound
	} else if ecode == http.StatusNotFound {
		err = cos.NewErrNotFound(T, lom.Cname())
	}

	if !locked {
		// return info (neq and, possibly, not-found), and be done
		return CRMD{ObjAttrs: oa, ErrCode: ecode, Err: err}
	}

	// rm remotely-deleted
	if cos.IsNotExist(err, ecode) && (lom.VersionConf().Sync || sync) {
		errDel := lom.RemoveObj(locked /*force through rlock*/)
		if errDel != nil {
			ecode, err = 0, errDel
		} else {
			// TODO -- FIXME: cannot import `stats.VlabBucket`; optimize or remove
			vlabs := map[string]string{"bucket": lom.Bck().Cname("")}
			T.StatsUpdater().IncWith(RemoteDeletedDelCount, vlabs)
		}
		debug.Assert(err != nil)
		return CRMD{ErrCode: ecode, Err: err}
	}

	lom.Uncache()
	return CRMD{ObjAttrs: oa, ErrCode: ecode, Err: err}
}

// NOTE: Sync is false (ie., not deleting)
func (lom *LOM) LoadLatest(latest bool) (oa *cmn.ObjAttrs, deleted bool, err error) {
	debug.Assert(lom.IsLocked() > apc.LockNone, "must be locked: ", lom.Cname())

	err = lom.Load(true /*cache it*/, true /*locked*/)
	if err != nil {
		if !cmn.IsErrObjNought(err) || !latest {
			return nil, false, err
		}
	}
	if latest {
		res := lom.CheckRemoteMD(true /*locked*/, false /*synchronize*/, nil /*origReq*/)
		if res.Eq {
			debug.AssertNoErr(res.Err)
			return nil, false, nil
		}
		if err == nil { // undo lom.Load
			lom.UncacheDel()
		}
		if res.Err != nil {
			deleted = cos.IsNotExist(res.Err, res.ErrCode)
			return nil, deleted, res.Err
		}
		oa = res.ObjAttrs
	}
	return oa, false, nil
}
