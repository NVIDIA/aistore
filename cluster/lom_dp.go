// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"context"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// NOTE: compare with ext/etl/dp.go

const ldpact = ".LDP.Reader"

type (
	// data provider
	DP interface {
		Reader(lom *LOM) (reader cos.ReadOpenCloser, oah cmn.ObjAttrsHolder, err error)
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

// compare with etl/dp.go
func (*LDP) Reader(lom *LOM) (cos.ReadOpenCloser, cmn.ObjAttrsHolder, error) {
	lom.Lock(false)
	loadErr := lom.Load(false /*cache it*/, true /*locked*/)
	if loadErr == nil {
		roc, err := lom.NewDeferROC()
		return roc, lom, err
	}

	defer lom.Unlock(false)
	if !cmn.IsObjNotExist(loadErr) {
		return nil, nil, cmn.NewErrFailedTo(T.String()+ldpact, "load", lom, loadErr)
	}
	if !lom.Bck().IsRemote() {
		return nil, nil, loadErr
	}

	// cold GetObjReader and return oah (holder) to represent non-existing object
	lom.SetAtimeUnix(time.Now().UnixNano())
	oah := &cmn.ObjAttrs{
		Ver:   "",            // TODO: differentiate between copying (same version) vs. transforming
		Cksum: cos.NoneCksum, // will likely reassign (below)
		Atime: lom.AtimeUnix(),
	}
	ctx := context.Background()
	ctx = context.WithValue(ctx, cos.CtxSetSize, cos.SetSizeFunc(oah.SetSize))
	reader, expCksum, _, err := T.Backend(lom.Bck()).GetObjReader(ctx, lom)

	if lom.Checksum() != nil {
		oah.Cksum = lom.Checksum()
	} else if expCksum != nil {
		oah.Cksum = expCksum
	}
	return cos.NopOpener(reader), oah, err
}
