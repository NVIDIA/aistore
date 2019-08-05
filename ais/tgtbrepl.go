// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
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

func (t *targetrunner) copyLB(bucketFrom, bucketTo string, rename bool) (err error) {
	var (
		wg                = &sync.WaitGroup{}
		smap              = t.smapowner.get()
		availablePaths, _ = fs.Mountpaths.Get()
		errCh             = make(chan error, len(fs.CSM.RegisteredContentTypes)*len(availablePaths))
	)
	//
	// NOTE: only objects; TODO for contentType := range fs.CSM.RegisteredContentTypes
	//
	for _, mpathInfo := range availablePaths {
		toDir := mpathInfo.MakePathBucket(fs.ObjectType, bucketTo, true /*bucket is local*/)
		if err := cmn.CreateDir(toDir); err != nil {
			errCh <- err
			continue
		}
		fromDir := mpathInfo.MakePathBucket(fs.ObjectType, bucketFrom, true /*bucket is local*/)
		wg.Add(1)
		go func(fromDir string) {
			buf, slab := nodeCtx.mm.AllocDefault()
			ri := &replicInfo{smap: smap, bucketTo: bucketTo, t: t, buf: buf, uncache: rename}
			errCh <- filepath.Walk(fromDir, ri.walkCopyLB)
			wg.Done()
			slab.Free(buf)
		}(fromDir)
	}
	wg.Wait()
	close(errCh)
	for err = range errCh {
		if err != nil {
			return
		}
	}
	return
}

//
// replicInfo methods
//

func (ri *replicInfo) walkCopyLB(fqn string, osfi os.FileInfo, err error) error {
	if err != nil {
		if err := cmn.PathWalkErr(err); err != nil {
			glog.Error(err)
			return err
		}
		return nil
	}
	if osfi.Mode().IsDir() {
		return nil
	}
	lom, err := cluster.LOM{T: ri.t, FQN: fqn}.Init(cmn.ProviderFromLoc(true /*is local*/))
	if err != nil {
		return nil
	}
	_, err = ri.copyObject(lom, lom.Objname)
	if err != nil {
		glog.Error(err)
	}
	return nil
}

func (ri *replicInfo) copyObject(lom *cluster.LOM, objnameTo string) (copied bool, err error) {
	var (
		file                  *cmn.FileHandle
		cksumType, cksumValue string
		si                    = ri.t.si
	)
	if ri.smap != nil {
		cmn.Assert(!ri.localCopy)
		if si, err = hrwTarget(ri.bucketTo, objnameTo, ri.smap); err != nil {
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
