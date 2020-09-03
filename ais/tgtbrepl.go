// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
)

type replicInfo struct {
	t         *targetrunner
	smap      *smapX
	bckTo     *cluster.Bck
	buf       []byte
	localOnly bool // copy locally with no HRW=>target
	uncache   bool // uncache the source
	finalize  bool // copies and EC (as in poi.finalize())
}

//
// replicInfo
//

func (ri *replicInfo) copyObject(lom *cluster.LOM, objNameTo string) (copied bool, err error) {
	si := ri.t.si
	if !ri.localOnly {
		cmn.Assert(ri.smap != nil)
		if si, err = cluster.HrwTarget(ri.bckTo.MakeUname(objNameTo), &ri.smap.Smap); err != nil {
			return
		}
	}

	lom.Lock(false)
	if err = lom.Load(false); err != nil {
		if !cmn.IsObjNotExist(err) {
			err = fmt.Errorf("%s: err: %v", lom, err)
		}
		lom.Unlock(false)
		return
	}
	if ri.uncache {
		defer lom.Uncache()
	}

	if si.ID() != ri.t.si.ID() {
		copied, err := ri.putRemote(lom, objNameTo, si)
		lom.Unlock(false)
		return copied, err
	}

	if !lom.TryUpgradeLock() {
		// We haven't managed to upgrade the lock so we must do it slow way...
		lom.Unlock(false)
		lom.Lock(true)
		if err = lom.Load(false); err != nil {
			lom.Unlock(true)
			return
		}
	}

	// At this point we must have an exclusive lock for the object.
	defer lom.Unlock(true)

	// local op
	dst := &cluster.LOM{T: ri.t, ObjName: objNameTo}
	err = dst.Init(ri.bckTo.Bck)
	if err != nil {
		return
	}

	// Lock destination for writing if the destination has a different uname.
	if lom.Uname() != dst.Uname() {
		dst.Lock(true)
		defer dst.Unlock(true)
	}

	// If before initializing the `dst` all mountpaths would be removed except
	// the one on which the `lom` is placed then both `lom` and `dst` will have
	// the same FQN in which case we should not copy.
	if lom.FQN == dst.FQN {
		return
	}

	if err = dst.Load(false); err == nil {
		if lom.Cksum().Equal(dst.Cksum()) {
			return
		}
	} else if cmn.IsErrBucketNought(err) {
		return
	}

	dst, err = lom.CopyObject(dst.FQN, ri.buf)
	if err == nil {
		copied = true
		dst.ReCache()
		if ri.finalize {
			ri.t.putMirror(dst)
		}
	}
	return
}

// TODO: reuse rebalancing code and streams
func (ri *replicInfo) putRemote(lom *cluster.LOM, objNameTo string, si *cluster.Snode) (copied bool, err error) {
	var file *cmn.FileHandle // Closed by `PutObjectToTarget()`
	if file, err = cmn.NewFileHandle(lom.FQN); err != nil {
		return false, fmt.Errorf("failed to open %s, err: %v", lom.FQN, err)
	}

	// PUT object into different target
	header := lom.PopulateHdr(nil)
	err = ri.t.PutObjectToTarget(si, file, ri.bckTo, objNameTo, header)
	if err != nil {
		return false, err
	}
	return true, nil
}
