// Package fs provides mountpath and FQN abstractions and methods to resolve/map stored content
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package fs

import (
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/ios"
)

// Used ONLY by:
// - unit tests (across many packages)
// - test tools (namely, `tools/file.go`).
//
// TODO:
// - add `fstest` build tag
// - move these bits out of production code.

func NewTestMFS(iostater ios.IOS) {
	const num = 10
	mfs = &MFS{fsIDs: make(map[cos.FsID]string, num)}
	if iostater == nil {
		mfs.ios, _ = ios.New(num)
	} else {
		mfs.ios = iostater
	}
	PutMPI(make(MPI, num), make(MPI, num))

	// ditto
	_once.Do(initCSM)
}

func AddTestMpath(mpath, tid string) (mi *Mountpath, err error) {
	mi, err = NewMountpath(mpath, cos.TestMpathLabel)
	if err != nil {
		return nil, err
	}
	config := cmn.GCO.Get()
	mfs.mu.Lock()
	err = mi._cloneAddEnabled(tid, config)
	mfs.mu.Unlock()
	return mi, err
}
