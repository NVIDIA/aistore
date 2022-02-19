// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"context"
	"fmt"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
)

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
	var lomLoadErr, err error

	lom.Lock(false)
	if lomLoadErr = lom.Load(false /*cache it*/, true /*locked*/); lomLoadErr == nil {
		var file *cos.FileHandle
		if file, err = cos.NewFileHandle(lom.FQN); err != nil {
			lom.Unlock(false)
			return nil, nil, fmt.Errorf(cmn.FmtErrWrapFailed, "LOMReader", "open", lom.FQN, err)
		}
		return cos.NewDeferROC(file, func() { lom.Unlock(false) }), lom, nil
	}

	// LOM loading error has occurred
	defer lom.Unlock(false)

	if !cmn.IsObjNotExist(lomLoadErr) {
		return nil, nil, fmt.Errorf("%s: err: %v", lom, lomLoadErr)
	}

	if !lom.Bck().IsRemote() {
		return nil, nil, lomLoadErr
	}

	// Get object directly from a cloud, as it doesn't exist locally
	// TODO: revisit versus global rebalancing
	reader, _, _, err := T.Backend(lom.Bck()).GetObjReader(context.Background(), lom)
	return cos.NopOpener(reader), lom, err
}
