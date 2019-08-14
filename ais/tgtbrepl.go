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
	"github.com/NVIDIA/aistore/fs"
)

type replicInfo struct {
	t         *targetrunner
	smap      *smapX
	bucketTo  string
	buf       []byte
	localCopy bool
	uncache   bool
}

//
// replicInfo
//

func (ri *replicInfo) copyObject(lom *cluster.LOM, objnameTo string) (copied bool, err error) {
	var (
		file                  *cmn.FileHandle
		cksumType, cksumValue string
		si                    = ri.t.si
	)
	if ri.smap != nil {
		cmn.Assert(!ri.localCopy)
		if si, err = cluster.HrwTarget(ri.bucketTo, objnameTo, &ri.smap.Smap); err != nil {
			return
		}
	} else {
		cmn.Assert(ri.localCopy)
	}
	cluster.ObjectLocker.Lock(lom.Uname(), false)
	defer cluster.ObjectLocker.Unlock(lom.Uname(), false)

	if err = lom.Load(false); err != nil {
		err = fmt.Errorf("%s: err: %v", lom, err)
		return
	}
	if !lom.Exists() || lom.IsCopy() {
		return
	}
	if ri.uncache {
		defer lom.Uncache()
	}

	// local op
	if si.DaemonID == ri.t.si.DaemonID {
		var dst *cluster.LOM
		dst, err = cluster.LOM{T: ri.t, Bucket: ri.bucketTo, Objname: objnameTo}.Init(cmn.ProviderFromLoc(true /*is local*/))
		if err != nil {
			return
		}
		workFQN := fs.CSM.GenContentParsedFQN(dst.ParsedFQN, fs.WorkfileType, fs.WorkfilePut)
		_, err = lom.CopyObject(dst.FQN, workFQN, ri.buf, false /*  object, not a mirrored copy */)
		if err == nil {
			copied = true
			ri.t.putMirror(dst)
		}
		// TODO: EC via ecmanager.EncodeObject
		return
	}

	// another target
	//
	// TODO: introduce namespace refs and then reuse rebalancing logic and streams instead of PUT
	//
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
	query.Add(cmn.URLParamBckProvider, cmn.ProviderFromLoc(lom.BckIsLocal))
	query.Add(cmn.URLParamProxyID, ri.smap.ProxySI.DaemonID)
	reqArgs := cmn.ReqArgs{
		Method: http.MethodPut,
		Base:   si.URL(cmn.NetworkIntraData),
		Path:   cmn.URLPath(cmn.Version, cmn.Objects, ri.bucketTo, objnameTo),
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

	_, err = ri.t.httpclientLongTimeout.Do(req)
	if err != nil {
		err = fmt.Errorf("failed to PUT to %s, err: %v", reqArgs.URL(), err)
	} else {
		copied = true
	}
	return
}
