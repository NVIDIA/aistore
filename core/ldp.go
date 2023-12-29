// Package core provides core metadata and in-cluster API
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
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

const ldpact = ".LDP.Reader"

type (
	// data provider
	DP interface {
		Reader(lom *LOM, latestVer bool) (reader cos.ReadOpenCloser, oah cos.OAH, err error)
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

// compare with ext/etl/dp.go
// returns ErrSkip if not found (to favor streaming callers)
func (*LDP) Reader(lom *LOM, latestVer bool) (cos.ReadOpenCloser, cos.OAH, error) {
	lom.Lock(false)
	loadErr := lom.Load(false /*cache it*/, true /*locked*/)
	if loadErr == nil {
		if latestVer {
			debug.Assert(lom.Bck().IsRemote(), lom.Bck().String())
			eq, errCode, err := lom.CompareRemoteMD()
			if err != nil {
				lom.Unlock(false)
				if errCode == http.StatusNotFound || cmn.IsObjNotExist(err) {
					err = cmn.ErrSkip
				} else {
					err = cmn.NewErrFailedTo(T.String()+ldpact, "head-latest", lom, err)
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
	if !cmn.IsObjNotExist(loadErr) {
		return nil, nil, cmn.NewErrFailedTo(T.String()+ldpact, "load", lom, loadErr)
	}
	if !lom.Bck().IsRemote() {
		return nil, nil, cmn.ErrSkip
	}

remote:
	// cold GetObjReader and return oah (holder) to represent non-existing object
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

func (lom *LOM) CompareRemoteMD() (bool, int, error) {
	bck := lom.Bck()
	if !bck.IsCloud() && !bck.IsRemoteAIS() {
		return true, 0, nil
	}
	oa, errCode, err := T.Backend(bck).HeadObj(context.Background(), lom)
	if err != nil {
		return false, errCode, err
	}
	debug.Assert(errCode == 0, errCode)
	return lom.Equal(oa), errCode, nil
}
