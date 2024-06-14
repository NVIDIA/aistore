// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package core

import (
	"context"
	"net/http"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
)

// NOTE: compare with ext/etl/dp.go

type (
	// data provider
	DP interface {
		Reader(lom *LOM, latestVer, sync bool) (reader cos.ReadOpenCloser, oah cos.OAH, err error)
	}

	LDP struct{}

	// compare with `deferROC` from cmn/cos/io.go
	deferROC struct {
		cos.ReadOpenCloser
		lif LIF
	}

	CRMD struct {
		Err      error
		ObjAttrs *cmn.ObjAttrs
		ErrCode  int
		Eq       bool
	}
)

// interface guard
var _ DP = (*LDP)(nil)

func (r *deferROC) Close() (err error) {
	err = r.ReadOpenCloser.Close()
	r.lif.Unlock(false)
	return
}

// is called under rlock; unlocks on fail
func (lom *LOM) NewDeferROC() (cos.ReadOpenCloser, error) {
	fh, err := cos.NewFileHandle(lom.FQN)
	if err == nil {
		return &deferROC{fh, lom.LIF()}, nil
	}
	lom.Unlock(false)
	return nil, cmn.NewErrFailedTo(T, "open", lom.Cname(), err)
}

// (compare with ext/etl/dp.go)
func (*LDP) Reader(lom *LOM, latestVer, sync bool) (cos.ReadOpenCloser, cos.OAH, error) {
	lom.Lock(false)
	loadErr := lom.Load(false /*cache it*/, true /*locked*/)
	if loadErr == nil {
		if latestVer || sync {
			debug.Assert(lom.Bck().IsRemote(), lom.Bck().String()) // caller's responsibility
			res := lom.CheckRemoteMD(true /* rlocked*/, sync, nil /*origReq*/)
			if res.Err != nil {
				lom.Unlock(false)
				if !cos.IsNotExist(res.Err, res.ErrCode) {
					res.Err = cmn.NewErrFailedTo(T, "head-latest", lom.Cname(), res.Err)
				}
				return nil, nil, res.Err
			}
			if !res.Eq {
				// version changed
				lom.Unlock(false)
				goto remote
			}
		}

		roc, err := lom.NewDeferROC() // keeping lock, reading local
		return roc, lom, err
	}

	lom.Unlock(false)
	if !cos.IsNotExist(loadErr, 0) {
		return nil, nil, cmn.NewErrFailedTo(T, "ldp-load", lom.Cname(), loadErr)
	}
	if !lom.Bck().IsRemote() {
		return nil, nil, cos.NewErrNotFound(T, lom.Cname())
	}

remote:
	// GetObjReader and return remote (object) reader and oah for object metadata
	// (compare w/ T.GetCold)
	lom.SetAtimeUnix(time.Now().UnixNano())
	oah := &cmn.ObjAttrs{
		Ver:   nil,           // TODO: differentiate between copying (same version) vs. transforming
		Cksum: cos.NoneCksum, // will likely reassign (below)
		Atime: lom.AtimeUnix(),
	}
	res := T.Backend(lom.Bck()).GetObjReader(context.Background(), lom, 0, 0)

	if lom.Checksum() != nil {
		oah.Cksum = lom.Checksum()
	} else if res.ExpCksum != nil {
		oah.Cksum = res.ExpCksum
	}
	oah.Size = res.Size
	return cos.NopOpener(res.R), oah, res.Err
}

// NOTE:
// - [PRECONDITION]: `versioning.validate_warm_get` || QparamLatestVer
// - [Sync] when Sync option is used (via bucket config and/or `sync` argument) caller MUST take wlock or rlock
// - [MAY] delete remotely-deleted (non-existing) object and increment associated stats counter
//
// Returns NotFound also after having removed local replica (the Sync option)
func (lom *LOM) CheckRemoteMD(locked, sync bool, origReq *http.Request) (res CRMD) {
	bck := lom.Bck()
	if !bck.HasVersioningMD() {
		// nothing to do with: in-cluster ais:// bucket, or a remote one
		// that doesn't provide any versioning metadata
		return CRMD{Eq: true}
	}

	oa, ecode, err := T.Backend(bck).HeadObj(context.Background(), lom, origReq)
	if err == nil {
		debug.Assert(ecode == 0, ecode)
		return CRMD{ObjAttrs: oa, Eq: lom.Equal(oa), ErrCode: ecode}
	}

	if ecode == http.StatusNotFound {
		err = cos.NewErrNotFound(T, lom.Cname())
	}
	if !locked {
		// return info (neq and, possibly, not-found), and be done
		return CRMD{ErrCode: ecode, Err: err}
	}

	// rm remotely-deleted
	if cos.IsNotExist(err, ecode) && (lom.VersionConf().Sync || sync) {
		errDel := lom.Remove(locked /*force through rlock*/)
		if errDel != nil {
			ecode, err = 0, errDel
		} else {
			g.tstats.Inc(RemoteDeletedDelCount)
		}
		debug.Assert(err != nil)
		return CRMD{ErrCode: ecode, Err: err}
	}

	lom.Uncache()
	return CRMD{ErrCode: ecode, Err: err}
}

// NOTE: must be locked; NOTE: Sync == false (ie., not deleting)
func (lom *LOM) LoadLatest(latest bool) (oa *cmn.ObjAttrs, deleted bool, err error) {
	debug.AssertFunc(func() bool {
		rc, exclusive := lom.IsLocked()
		return exclusive || rc > 0
	})
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
		if res.Err != nil {
			deleted = cos.IsNotExist(res.Err, res.ErrCode)
			return nil, deleted, res.Err
		}
		oa = res.ObjAttrs
	}
	return oa, false, nil
}
