/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/memsys"
)

// ================================================= Summary ===============================================
//
// This replication module implements a replication service in DFC - a long running task with the
// purpose of creating replicas of stored objects.
//
// The API exposed to the rest of the code includes the following operations:
//   * reqSendReplica    - to send a replica of a specified object to a specified URL
//   * reqReceiveReplica - to receive a replica of an object
// As a fsprunner, replicationRunner implements methods described by the fsprunner interface.
//
// All other operations are private to the replication module and used only internally!
//
// replicationRunner creates and runs an mpathReplicator for each found or added mountpath.
// mpathReplicators send and receive object replicas. For now, there is only one mpathReplicator
// per mountpath. Communication between the client code and replicationRunner as well between
// the replicationRunner and each mpathReplicator is through a channel of replRequest (replication request).
// replRequest also holds a result channel for synchronous replication calls.
//
// ================================================= Summary ===============================================

// TODO
// * Replication bucket-specific configuration
// * Add a check validating that sending and receiving targets are not inside the same cluster
// * On the receiving side do not calculate checksum of existing file if it's stored as xattr

const (
	replicationPolicyNone  = "none"
	replicationPolicySync  = "sync"
	replicationPolicyAsync = "async"

	replicationActSend    = "send"
	replicationActReceive = "receive"

	replicationAddMountpath    = "addmpath"
	replicationRemoveMountpath = "removempath"

	replicationRequestBufferSize      = 1024
	mpathReplicationRequestBufferSize = 1024
)

type mpathReq struct {
	action string
	mpath  string
}

type replRequest struct {
	action          string
	remoteDirectURL string
	fqn             string
	deleteObject    bool          // used only on send side
	httpReq         *http.Request // used only on receive side
	resultCh        chan error
}

type mpathReplicator struct {
	t         *targetrunner
	directURL string
	mpath     string
	replReqCh chan *replRequest
	once      *sync.Once
	stopCh    chan struct{}
}

type replicationRunner struct {
	cmn.Named
	t                *targetrunner
	replReqCh        chan *replRequest
	mpathReqCh       chan mpathReq
	mountpaths       *fs.MountedFS
	mpathReplicators map[string]*mpathReplicator // mpath -> replicator
	stopCh           chan struct{}
}

func (rr *replicationRunner) newMpathReplicator(mpath string) *mpathReplicator {
	return &mpathReplicator{
		t:         rr.t,
		directURL: rr.t.si.IntraDataNet.DirectURL,
		mpath:     mpath,
		replReqCh: make(chan *replRequest, mpathReplicationRequestBufferSize),
		once:      &sync.Once{},
		stopCh:    make(chan struct{}, 1),
	}
}

func (rr *replicationRunner) newSendReplRequest(dstDirectURL, fqn string, deleteObject, sync bool) *replRequest {
	req := &replRequest{
		action:          replicationActSend,
		remoteDirectURL: dstDirectURL,
		fqn:             fqn,
		deleteObject:    deleteObject,
		httpReq:         nil,
	}
	if sync {
		req.resultCh = make(chan error, 1)
	}
	return req
}

func (rr *replicationRunner) newReceiveReplRequest(srcDirectURL, fqn string, httpReq *http.Request, sync bool) *replRequest {
	req := &replRequest{
		action:          replicationActReceive,
		remoteDirectURL: srcDirectURL,
		fqn:             fqn,
		deleteObject:    false,
		httpReq:         httpReq,
	}
	if sync {
		req.resultCh = make(chan error, 1)
	}
	return req
}

func newReplicationRunner(t *targetrunner, mountpaths *fs.MountedFS) *replicationRunner {
	return &replicationRunner{
		t:                t,
		replReqCh:        make(chan *replRequest, replicationRequestBufferSize),
		mpathReqCh:       make(chan mpathReq),
		mountpaths:       mountpaths,
		mpathReplicators: make(map[string]*mpathReplicator),
		stopCh:           make(chan struct{}),
	}
}

func (r *mpathReplicator) Run() {
	glog.Infof("Started replicator for mountpath: %s", r.mpath)
	for {
		select {
		case req := <-r.replReqCh:
			r.replicate(req)
		case <-r.stopCh:
			return
		}
	}
}

func (r *mpathReplicator) Stop() {
	glog.Infof("Stopping replicator for mountpath: %s", r.mpath)
	r.stopCh <- struct{}{}
	close(r.stopCh)
}

func (r *mpathReplicator) replicate(req *replRequest) {
	var err error

	switch req.action {
	case replicationActSend:
		err = r.send(req)
	case replicationActReceive:
		err = r.receive(req)
	}

	if err != nil {
		var src, dst string
		switch req.action {
		case replicationActSend:
			src, dst = r.directURL, req.remoteDirectURL
		case replicationActReceive:
			src, dst = req.remoteDirectURL, r.directURL
		}
		glog.Errorf("Error occurred during object replication: "+
			"action: %s %s, source: %s, destination: %s, err: %v",
			req.action, req.fqn, src, dst, err)
	}

	if req.resultCh != nil {
		req.resultCh <- err
		close(req.resultCh)
	}
}

func (r *mpathReplicator) send(req *replRequest) error {
	var (
		errstr string
		err    error
	)

	bucket, object, err := r.t.fqn2bckobj(req.fqn)
	if err != nil {
		errstr = fmt.Sprintf("Failed to extract bucket and object name from %s, error: %v", req.fqn, err)
		return errors.New(errstr)
	}

	url := req.remoteDirectURL + cmn.URLPath(cmn.Version, cmn.Objects, bucket, object)

	uname := cluster.Uname(bucket, object)
	r.t.rtnamemap.Lock(uname, req.deleteObject)
	defer r.t.rtnamemap.Unlock(uname, req.deleteObject)

	file, err := os.Open(req.fqn)
	if err != nil {
		if os.IsPermission(err) {
			errstr = fmt.Sprintf("Permission denied: access forbidden to %s", req.fqn)
		} else {
			errstr = fmt.Sprintf("Failed to open local file %s, error: %v", req.fqn, err)
		}
		r.t.fshc(err, req.fqn)
		return errors.New(errstr)
	}
	defer file.Close()

	xxHashBinary, errstr := Getxattr(req.fqn, cmn.XattrXXHashVal)
	xxHashVal := ""
	if errstr != "" {
		buf, slab := gmem2.AllocFromSlab2(0)
		xxHashVal, errstr = cmn.ComputeXXHash(file, buf)
		slab.Free(buf)
		if errstr != "" {
			errstr = fmt.Sprintf("Failed to calculate checksum on %s, error: %s", req.fqn, errstr)
			return errors.New(errstr)
		}
		if _, err = file.Seek(0, os.SEEK_SET); err != nil {
			return fmt.Errorf("Unexpected fseek failure when replicating (sending) %q, err: %v", req.fqn, err)
		}
	} else {
		xxHashVal = string(xxHashBinary)
	}

	httpReq, err := http.NewRequest(http.MethodPut, url, file)
	if err != nil {
		errstr = fmt.Sprintf("Failed to create HTTP request, err: %v", err)
		return errors.New(errstr)
	}
	httpReq.GetBody = func() (io.ReadCloser, error) {
		return os.Open(req.fqn)
	}
	var accessTime time.Time
	okAccessTime := r.t.bucketLRUEnabled(bucket)
	if okAccessTime {
		// obtain the object's access time
		atimeResponse := <-getatimerunner().atime(req.fqn)
		accessTime, okAccessTime = atimeResponse.accessTime, atimeResponse.ok
	}

	// Read from storage if it doesn't exist
	if !okAccessTime {
		if fileInfo, err := os.Stat(req.fqn); err == nil {
			accessTime, _, _ = getAmTimes(fileInfo)
			okAccessTime = true
		} else {
			glog.Errorf("Failed to get %q access time upon replication", req.fqn)
		}
	}

	// obtain the version of the file
	if version, errstr := Getxattr(req.fqn, cmn.XattrObjVersion); errstr != "" {
		glog.Errorf("Failed to read %q xattr %s, err %s", req.fqn, cmn.XattrObjVersion, errstr)
	} else if len(version) != 0 {
		httpReq.Header.Add(cmn.HeaderDFCObjVersion, string(version))
	}

	// specify source direct URL in request header
	httpReq.Header.Add(cmn.HeaderDFCReplicationSrc, r.directURL)

	httpReq.Header.Add(cmn.HeaderDFCChecksumType, cmn.ChecksumXXHash)
	httpReq.Header.Add(cmn.HeaderDFCChecksumVal, xxHashVal)
	if okAccessTime {
		httpReq.Header.Add(cmn.HeaderDFCObjAtime, string(accessTime.Format(cmn.RFC822)))
	}

	resp, err := r.t.httpclientLongTimeout.Do(httpReq)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= http.StatusBadRequest {
		b, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			errstr = fmt.Sprintf(
				"HTTP status code: %d, unable to read response body (error: %v)",
				resp.StatusCode, err,
			)
		} else {
			errstr = fmt.Sprintf(
				"HTTP status code: %d, HTTP response body: %s, bucket/object: %s/%s, "+
					"destination URL: %s",
				resp.StatusCode, string(b), bucket, object, req.remoteDirectURL,
			)
		}
		return errors.New(errstr)
	}

	if req.deleteObject {
		if err := os.Remove(req.fqn); err != nil {
			errstr = fmt.Sprintf("failed to remove local file %s, error: %v", req.fqn, err)
			return errors.New(errstr)
		}
	}

	return nil
}

func (r *mpathReplicator) receive(req *replRequest) error {
	var (
		nhobj         cksumvalue
		nhtype, nhval string
		sgl           *memsys.SGL
		errstr        string
		accessTime    time.Time
	)
	httpr := req.httpReq
	putfqn := cluster.GenContentFQN(req.fqn, cluster.DefaultWorkfileType)
	bucket, object, err := r.t.fqn2bckobj(req.fqn)
	if err != nil {
		errstr = fmt.Sprintf("Failed to extract bucket and object name from %s, error: %v", req.fqn, err)
		return errors.New(errstr)
	}

	hdhobj := newcksumvalue(httpr.Header.Get(cmn.HeaderDFCChecksumType), httpr.Header.Get(cmn.HeaderDFCChecksumVal))
	if hdhobj == nil {
		errstr = fmt.Sprintf("Failed to extract checksum from replication PUT request for %s/%s", bucket, object)
		return errors.New(errstr)
	}

	if accessTimeStr := httpr.Header.Get(cmn.HeaderDFCObjAtime); accessTimeStr != "" {
		if parsedTime, err := time.Parse(time.RFC822, accessTimeStr); err == nil {
			accessTime = parsedTime
		}
	}

	hdhtype, hdhval := hdhobj.get()
	if hdhtype != cmn.ChecksumXXHash {
		errstr = fmt.Sprintf("Unsupported checksum type: %q", hdhtype)
		return errors.New(errstr)
	}

	// Avoid replication by checking if cheksums from header and existing file match
	// Attempt to access the checksum Xattr if it already exists
	if xxHashBinary, errstr := Getxattr(req.fqn, cmn.XattrXXHashVal); errstr != "" && xxHashBinary != nil && string(xxHashBinary) == hdhval {
		glog.Infof("Existing %s/%s is valid: replication PUT is a no-op", bucket, object)
		return nil
	}
	// Calculate the checksum when the Xattr does not exit
	if file, err := os.Open(req.fqn); err == nil {
		buf, slab := gmem2.AllocFromSlab2(0)
		xxHashVal, errstr := cmn.ComputeXXHash(file, buf)
		slab.Free(buf)
		if err = file.Close(); err != nil {
			glog.Warningf("Unexpected failure to close %s once xxhash has been computed, error: %v", req.fqn, err)
		}
		if errstr == "" && xxHashVal == hdhval {
			glog.Infof("Existing %s/%s is valid: replication PUT is a no-op", bucket, object)
			return nil
		}
	}

	// TODO
	// Method targetrunner.receive validates checksum based on cluster-level or bucket-level
	// checksum configuration. Replication service needs its own checksum configuration.
	sgl, nhobj, _, errstr = r.t.receive(putfqn, object, "", hdhobj, httpr.Body)
	httpr.Body.Close()
	if errstr != "" {
		return errors.New(errstr)
	}

	if nhobj != nil {
		nhtype, nhval = nhobj.get()
		cmn.Assert(hdhtype == nhtype)
	}
	if hdhval != "" && nhval != "" && hdhval != nhval {
		errstr = fmt.Sprintf("Bad checksum: %s/%s %s %s... != %s...", bucket, object, nhtype, hdhval[:8], nhval[:8])
		return errors.New(errstr)
	} else if hdhval == "" || nhval == "" {
		glog.Warningf("Failed to compare checksums during replication PUT for %s/%s", bucket, object)
	}

	if sgl != nil {
		return nil
	}

	props := &objectProps{nhobj: nhobj, version: httpr.Header.Get(cmn.HeaderDFCObjVersion)}
	if !accessTime.IsZero() {
		props.atime = accessTime
	}
	errstr, _ = r.t.putCommit(r.t.contextWithAuth(httpr), bucket, object, putfqn, req.fqn, props, false /* rebalance */)
	if errstr != "" {
		return errors.New(errstr)
	}
	return nil
}

func (rr *replicationRunner) init() {
	availablePaths, disabledPaths := rr.mountpaths.Mountpaths()
	for mpath := range availablePaths {
		rr.addMpath(mpath)
	}
	for mpath := range disabledPaths {
		rr.addMpath(mpath)
	}
}

func (rr *replicationRunner) Run() error {
	glog.Infof("Starting %s", rr.Getname())
	rr.init()

	for {
		select {
		case req := <-rr.replReqCh:
			rr.dispatchRequest(req)
		case mpathRequest := <-rr.mpathReqCh:
			switch mpathRequest.action {
			case replicationAddMountpath:
				rr.addMpath(mpathRequest.mpath)
			case replicationRemoveMountpath:
				rr.removeMpath(mpathRequest.mpath)
			}
		case <-rr.stopCh:
			return nil
		}
	}
}

func (rr *replicationRunner) Stop(err error) {
	rr.stopCh <- struct{}{}
	glog.Warningf("Replication runner stopped with error: %v", err)
	close(rr.stopCh)
}

func (rr *replicationRunner) dispatchRequest(req *replRequest) {
	mpathInfo, _ := path2mpathInfo(req.fqn)
	if mpathInfo == nil {
		errmsg := fmt.Sprintf("Failed to get mountpath for file %q", req.fqn)
		glog.Error(errmsg)
		if req.resultCh != nil {
			req.resultCh <- errors.New(errmsg)
		}
		return
	}
	mpath := mpathInfo.Path

	r, ok := rr.mpathReplicators[mpath]
	cmn.Assert(ok, "Invalid mountpath given in replication request")

	go r.once.Do(r.Run) // only run replicator if there is at least one replication request
	r.replReqCh <- req
}

func (rr *replicationRunner) reqSendReplica(dstDirectURL, fqn string, deleteObject bool, policy string) error {

	switch policy {
	case replicationPolicyAsync:
		rr.replReqCh <- rr.newSendReplRequest(dstDirectURL, fqn, deleteObject, false)

	case replicationPolicySync:
		req := rr.newSendReplRequest(dstDirectURL, fqn, deleteObject, true)
		rr.replReqCh <- req
		return <-req.resultCh // block until replication finishes

	case replicationPolicyNone:
		return nil // do nothing

	default:
		errstr := fmt.Sprintf("Invalid replication policy: %q. Expected: %q|%q|%q",
			policy, replicationPolicySync, replicationPolicyAsync, replicationPolicyNone)
		return errors.New(errstr)
	}

	return nil
}

func (rr *replicationRunner) reqReceiveReplica(srcDirectURL, fqn string, r *http.Request) error {
	req := rr.newReceiveReplRequest(srcDirectURL, fqn, r, true)
	rr.replReqCh <- req
	return <-req.resultCh // block until replication finishes
}

/*
 * fsprunner methods
 */

func (rr *replicationRunner) reqAddMountpath(mpath string) {
	rr.mpathReqCh <- mpathReq{action: replicationAddMountpath, mpath: mpath}
}

func (rr *replicationRunner) reqRemoveMountpath(mpath string) {
	rr.mpathReqCh <- mpathReq{action: replicationRemoveMountpath, mpath: mpath}
}

func (rr *replicationRunner) reqEnableMountpath(mpath string) {
	return
}

func (rr *replicationRunner) reqDisableMountpath(mpath string) {
	return
}

func (rr *replicationRunner) addMpath(mpath string) {
	replicator, ok := rr.mpathReplicators[mpath]
	if ok && replicator != nil {
		glog.Warningf("Attempted to add already existing mountpath: %s", mpath)
		return
	}
	rr.mpathReplicators[mpath] = rr.newMpathReplicator(mpath)
}

func (rr *replicationRunner) removeMpath(mpath string) {
	replicator, ok := rr.mpathReplicators[mpath]
	cmn.Assert(ok, "Mountpath unregister handler for replication called with invalid mountpath")
	replicator.Stop()
	delete(rr.mpathReplicators, mpath)
}
