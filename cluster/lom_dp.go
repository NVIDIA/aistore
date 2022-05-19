// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"context"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

// NOTE: compare with etl/dp.go

const ldpact = ".LDP.Reader"

type (
	// data provider
	DP interface {
		Reader(lom *LOM) (reader cos.ReadOpenCloser, objMeta cmn.ObjAttrsHolder, err error)
	}
	LDP struct{}
)

// interface guard
var _ DP = (*LDP)(nil)

func (*LDP) Reader(lom *LOM) (cos.ReadOpenCloser, cmn.ObjAttrsHolder, error) {
	var loadErr error

	lom.Lock(false)
	if loadErr = lom.Load(false /*cache it*/, true /*locked*/); loadErr == nil {
		file, err := cos.NewFileHandle(lom.FQN)
		if err == nil {
			// fast path
			return cos.NewDeferROC(file, func() { lom.Unlock(false) }), lom, nil
		}

		lom.Unlock(false)
		return nil, nil, cmn.NewErrFailedTo(T.String()+ldpact, "open", lom, err)
	}

	defer lom.Unlock(false)
	if !cmn.IsObjNotExist(loadErr) {
		return nil, nil, cmn.NewErrFailedTo(T.String()+ldpact, "load", lom, loadErr)
	}
	if !lom.Bck().IsRemote() {
		return nil, nil, loadErr
	}

	// Stream the object directly from remote backend
	reader, _, _, err := T.Backend(lom.Bck()).GetObjReader(context.Background(), lom)
	return cos.NopOpener(reader), lom, err
}
