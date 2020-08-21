// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
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
	var file *cmn.FileHandle // Closed by `.Do()`
	if file, err = cmn.NewFileHandle(lom.FQN); err != nil {
		err = fmt.Errorf("failed to open %s, err: %v", lom.FQN, err)
		return
	}

	// PUT object into different target
	var (
		query  = url.Values{}
		header = lom.PopulateHdr(nil)
	)
	header.Set(cmn.HeaderCallerID, ri.t.si.ID())
	query = cmn.AddBckToQuery(query, ri.bckTo.Bck)
	query.Add(cmn.URLParamTargetID, ri.t.si.ID())
	query.Add(cmn.URLParamRecvType, strconv.Itoa(int(cluster.Migrated)))
	reqArgs := cmn.ReqArgs{
		Method: http.MethodPut,
		Base:   si.URL(cmn.NetworkIntraData),
		Path:   cmn.URLPath(cmn.Version, cmn.Objects, ri.bckTo.Name, objNameTo),
		Query:  query,
		Header: header,
		BodyR:  file,
	}
	req, _, cancel, err := reqArgs.ReqWithTimeout(lom.Config().Timeout.SendFile)
	if err != nil {
		debug.AssertNoErr(file.Close())
		err = fmt.Errorf("unexpected failure to create request, err: %v", err)
		return
	}
	defer cancel()
	resp, err1 := ri.t.httpclientGetPut.Do(req)
	if err1 != nil {
		err = fmt.Errorf("failed to PUT to %s, err: %v", reqArgs.URL(), err1)
	} else {
		copied = true
	}
	debug.AssertNoErr(resp.Body.Close())
	return
}
