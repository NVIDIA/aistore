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
	return nil, cmn.NewErrFailedTo(T, "open", lom.FQN, err)
}

// (compare with ext/etl/dp.go)
func (*LDP) Reader(lom *LOM, latestVer, sync bool) (cos.ReadOpenCloser, cos.OAH, error) {
	lom.Lock(false)
	loadErr := lom.Load(false /*cache it*/, true /*locked*/)
	if loadErr == nil {
		if latestVer || sync {
			debug.Assert(lom.Bck().IsRemote(), lom.Bck().String()) // caller's responsibility
			eq, errCode, err := lom.CheckRemoteMD(true /* rlocked*/, sync)
			if err != nil {
				lom.Unlock(false)
				if !cos.IsNotExist(err, errCode) {
					err = cmn.NewErrFailedTo(T, "head-latest", lom, err)
				}
				return nil, nil, err
			}
			if !eq {
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
		return nil, nil, cmn.NewErrFailedTo(T, "ldp-load", lom, loadErr)
	}
	if !lom.Bck().IsRemote() {
		return nil, nil, cos.NewErrNotFound(T, lom.Cname())
	}

remote:
	// GetObjReader and return remote (object) reader and oah for object metadata
	// (compare w/ T.GetCold)
	lom.SetAtimeUnix(time.Now().UnixNano())
	oah := &cmn.ObjAttrs{
		Ver:   "",            // TODO: differentiate between copying (same version) vs. transforming
		Cksum: cos.NoneCksum, // will likely reassign (below)
		Atime: lom.AtimeUnix(),
	}
	res := T.Backend(lom.Bck()).GetObjReader(context.Background(), lom)

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
// - [Sync] when Sync option is used (both, via bucket configuration and/or `sync` argument) caller MUST take wlock or rlock
// - [MAY] delete remotely-deleted (non-existing) object and increment associated stats counter
func (lom *LOM) CheckRemoteMD(locked, sync bool) (bool /*equal*/, int, error) {
	bck := lom.Bck()
	if !bck.HasVersioningMD() {
		// nothing to do with: in-cluster ais:// bucket, or a remote one
		// that doesn't provide any versioning metadata
		return true, 0, nil
	}

	oa, errCode, err := T.Backend(bck).HeadObj(context.Background(), lom)
	if err == nil {
		debug.Assert(errCode == 0, errCode)
		return lom.Equal(oa), errCode, nil
	}

	if errCode == http.StatusNotFound {
		err = cos.NewErrNotFound(T, lom.Cname())
	}
	if !locked {
		// return info (neq and, possibly, not-found), and be done
		return false, errCode, err
	}

	// rm remotely-deleted
	if cos.IsNotExist(err, errCode) && (lom.VersionConf().Sync || sync) {
		errDel := lom.Remove(locked /*force through rlock*/)
		if errDel != nil {
			errCode, err = 0, errDel
		} else {
			g.tstats.Inc(RemoteDeletedDelCount)
		}
		return false, errCode, err
	}

	lom.Uncache()
	return false, errCode, err
}
