// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"

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

func (ri *replicInfo) copyObject(lom *cluster.LOM, objnameTo string) (copied bool, err error) {
	si := ri.t.si
	if !ri.localOnly {
		cmn.Assert(ri.smap != nil)
		if si, err = cluster.HrwTarget(ri.bckTo.MakeUname(objnameTo), &ri.smap.Smap); err != nil {
			return
		}
	}
	lom.Lock(false)
	defer lom.Unlock(false)

	if err = lom.Load(false); err != nil {
		if !cmn.IsNotObjExist(err) {
			err = fmt.Errorf("%s: err: %v", lom, err)
		}
		return
	}
	if ri.uncache {
		defer lom.Uncache()
	}

	if si.ID() != ri.t.si.ID() {
		return ri.putRemote(lom, objnameTo, si)
	}

	// local op
	dst := &cluster.LOM{T: ri.t, Objname: objnameTo}
	err = dst.Init(ri.bckTo.Bck)
	if err != nil {
		return
	}

	// lock for writing; check if already exists and is identical
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
		if dst.Size() == lom.Size() && cmn.EqCksum(lom.Cksum(), dst.Cksum()) {
			copied = true
			return
		}
	} else if cmn.IsErrBucketNought(err) {
		return
	}

	// do
	dst, err = lom.CopyObject(dst.FQN, ri.buf)
	if err == nil {
		copied = true
		dst.ReCache()

		if ri.finalize {
			//
			// TODO -- FIXME: reuse poi.finalize()
			//
			ri.t.putMirror(dst)
		}
	}
	return
}

//
// TODO: introduce namespace refs and then reuse rebalancing logic and streams instead of PUT
//
func (ri *replicInfo) putRemote(lom *cluster.LOM, objnameTo string, si *cluster.Snode) (copied bool, err error) {
	var (
		file                  *cmn.FileHandle
		cksumType, cksumValue string
	)
	if file, err = cmn.NewFileHandle(lom.FQN); err != nil {
		err = fmt.Errorf("failed to open %s, err: %v", lom.FQN, err)
		return
	}
	defer file.Close()
	if lom.Cksum() != nil {
		cksumType, cksumValue = lom.Cksum().Get()
	}

	// PUT object into different target
	query := url.Values{}
	query.Add(cmn.URLParamProvider, ri.bckTo.Provider)
	query.Add(cmn.URLParamProxyID, ri.smap.ProxySI.ID())
	reqArgs := cmn.ReqArgs{
		Method: http.MethodPut,
		Base:   si.URL(cmn.NetworkIntraData),
		Path:   cmn.URLPath(cmn.Version, cmn.Objects, ri.bckTo.Name, objnameTo),
		Query:  query,
		BodyR:  file,
	}
	req, _, cancel, err := reqArgs.ReqWithTimeout(lom.Config().Timeout.SendFile)
	if err != nil {
		err = fmt.Errorf("unexpected failure to create request, err: %v", err)
		return
	}
	defer cancel()
	req.Header.Set(cmn.HeaderObjCksumType, cksumType)
	req.Header.Set(cmn.HeaderObjCksumVal, cksumValue)
	req.Header.Set(cmn.HeaderObjVersion, lom.Version())
	timeInt := lom.Atime().UnixNano()
	if lom.Atime().IsZero() {
		timeInt = 0
	}
	req.Header.Set(cmn.HeaderObjAtime, strconv.FormatInt(timeInt, 10))

	_, err = ri.t.httpclientGetPut.Do(req)
	if err != nil {
		err = fmt.Errorf("failed to PUT to %s, err: %v", reqArgs.URL(), err)
	} else {
		copied = true
	}
	return
}
