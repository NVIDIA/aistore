// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package cluster

import (
	"context"
	"fmt"

	"github.com/NVIDIA/aistore/cmn"
)

type (

	// Defines what to send to a target.
	LomReaderProvider interface {
		// Returned func() will be called after reading from reader is done.
		Reader(lom *LOM) (reader cmn.ReadOpenCloser, objMeta cmn.ObjHeaderMetaProvider, err error)
	}

	LomReader struct{}
)

// interface guard
var _ LomReaderProvider = (*LomReader)(nil)

func (r *LomReader) Reader(lom *LOM) (cmn.ReadOpenCloser, cmn.ObjHeaderMetaProvider, error) {
	var lomLoadErr, err error

	lom.Lock(false)
	if lomLoadErr = lom.Load(false /*cache it*/, true /*locked*/); lomLoadErr == nil {
		var file *cmn.FileHandle
		if file, err = cmn.NewFileHandle(lom.FQN); err != nil {
			lom.Unlock(false)
			return nil, nil, fmt.Errorf("failed to open %s, err: %v", lom.FQN, err)
		}
		return cmn.NewDeferROC(file, func() { lom.Unlock(false) }), lom, nil
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
	return cmn.NopOpener(reader), lom, err
}
