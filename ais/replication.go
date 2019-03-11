// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/atime"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
)

// TODO: FIXME: must become a separate package; must initialize with cluster.Bowner, etc. interfaces

// ================================================= Summary ===============================================
//
// This replication module implements a replication service for AIStore.
// The API exposed to the rest of the code includes the following operations:
//   * reqSendReplica    - to send a replica of a specified object to a specified URL
//   * reqReceiveReplica - to receive a replica of an object
// As a fs.PathRunner, replicationRunner implements methods described by the fs.PathRunner interface.
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
// * On the receiving side do not compute checksum of existing file if it's stored as xattr

const (
	replicationPolicyNone  = "none"
	replicationPolicySync  = "sync"
	replicationPolicyAsync = "async"

	replicationActSend    = "send"
	replicationActReceive = "receive"

	replicationRequestBufferSize      = 1024
	mpathReplicationRequestBufferSize = 1024
)

type (
	replRequest struct {
		action          string
		remoteDirectURL string
		fqn             string
		deleteObject    bool          // used only on send side
		httpReq         *http.Request // used only on receive side
		resultCh        chan error
	}
	mpathReplicator struct {
		t           *targetrunner
		directURL   string
		mpath       string
		replReqCh   chan *replRequest
		once        *sync.Once
		stopCh      chan struct{}
		atimeRespCh chan *atime.Response
	}
	replicationRunner struct {
		cmn.NamedID
		t                *targetrunner // FIXME: package out
		replReqCh        chan *replRequest
		mpathReqCh       chan fs.ChangeReq
		mountpaths       *fs.MountedFS
		mpathReplicators map[string]*mpathReplicator // mpath -> replicator
		stopCh           chan struct{}
	}
)

//
// replicationRunner
//

func newReplicationRunner(t *targetrunner, mountpaths *fs.MountedFS) *replicationRunner {
	return &replicationRunner{
		t:                t,
		replReqCh:        make(chan *replRequest, replicationRequestBufferSize),
		mpathReqCh:       make(chan fs.ChangeReq), // FIXME: unbuffered
		mountpaths:       mountpaths,
		mpathReplicators: make(map[string]*mpathReplicator),
		stopCh:           make(chan struct{}),
	}
}

func (rr *replicationRunner) init() {
	availablePaths, disabledPaths := rr.mountpaths.Get()
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
			switch mpathRequest.Action {
			case fs.Add:
				rr.addMpath(mpathRequest.Path)
			case fs.Remove:
				rr.removeMpath(mpathRequest.Path)
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

func (rr *replicationRunner) newMpathReplicator(mpath string) *mpathReplicator {
	return &mpathReplicator{
		t:           rr.t,
		directURL:   rr.t.si.IntraDataNet.DirectURL,
		mpath:       mpath,
		replReqCh:   make(chan *replRequest, mpathReplicationRequestBufferSize),
		once:        &sync.Once{},
		stopCh:      make(chan struct{}, 1),
		atimeRespCh: make(chan *atime.Response, 1),
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

//lint:ignore U1000 unused
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

func (rr *replicationRunner) dispatchRequest(req *replRequest) {
	mpathInfo, _ := rr.mountpaths.Path2MpathInfo(req.fqn)
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
	cmn.AssertMsg(ok, "Invalid mountpath given in replication request")

	go r.once.Do(r.jog) // FIXME: (only run replicator if there is at least one replication request)
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

//lint:ignore U1000 unused
func (rr *replicationRunner) reqReceiveReplica(srcDirectURL, fqn string, r *http.Request) error {
	req := rr.newReceiveReplRequest(srcDirectURL, fqn, r, true)
	rr.replReqCh <- req
	return <-req.resultCh // block until replication finishes
}

/*
 * fs.PathRunner methods
 */

var _ fs.PathRunner = &replicationRunner{}

func (rr *replicationRunner) ReqAddMountpath(mpath string)     { rr.mpathReqCh <- fs.MountpathAdd(mpath) }
func (rr *replicationRunner) ReqRemoveMountpath(mpath string)  { rr.mpathReqCh <- fs.MountpathRem(mpath) }
func (rr *replicationRunner) ReqEnableMountpath(mpath string)  {}
func (rr *replicationRunner) ReqDisableMountpath(mpath string) {}

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
	cmn.AssertMsg(ok, "Mountpath unregister handler for replication called with invalid mountpath")
	replicator.Stop()
	delete(rr.mpathReplicators, mpath)
}

//
// mpathReplicator
//

func (r *mpathReplicator) jog() {
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
		glog.Errorf("Error occurred during object replication: "+"action: %s %s, source: %s, destination: %s, err: %v",
			req.action, req.fqn, src, dst, err)
	}

	if req.resultCh != nil {
		req.resultCh <- err
		close(req.resultCh)
	}
}

func (r *mpathReplicator) send(req *replRequest) error {
	var errstr string
	bckProvider := req.httpReq.URL.Query().Get(cmn.URLParamBckProvider)
	lom := &cluster.LOM{T: r.t, FQN: req.fqn}
	if errstr = lom.Fill(bckProvider, cluster.LomFstat); errstr != "" {
		return errors.New(errstr)
	}
	if !lom.Exists() {
		return fmt.Errorf("%s/%s %s", lom.Bucket, lom.Objname, cmn.DoesNotExist)
	}
	url := req.remoteDirectURL + cmn.URLPath(cmn.Version, cmn.Objects, lom.Bucket, lom.Objname)
	r.t.rtnamemap.Lock(lom.Uname, req.deleteObject)
	defer r.t.rtnamemap.Unlock(lom.Uname, req.deleteObject)

	if errstr = lom.Fill(bckProvider, cluster.LomCksum|cluster.LomCksumMissingRecomp); errstr != "" {
		return errors.New(errstr)
	}

	file, err := os.Open(req.fqn)
	if err != nil {
		return err
	}
	defer file.Close()

	httpReq, err := http.NewRequest(http.MethodPut, url, file)
	if err != nil {
		errstr = fmt.Sprintf("Failed to create HTTP request, err: %v", err)
		return errors.New(errstr)
	}
	httpReq.GetBody = func() (io.ReadCloser, error) {
		return os.Open(req.fqn)
	}

	_, atime, err := getatimerunner().FormatAtime(req.fqn, lom.ParsedFQN.MpathInfo.Path, r.atimeRespCh, lom.LRUEnabled())
	if err != nil {
		return fmt.Errorf("failed to get atime: %v", err)
	}

	// obtain the version of the file
	if version, errstr := fs.GetXattr(req.fqn, cmn.XattrVersion); errstr != "" {
		glog.Errorf("Failed to read %q xattr %s, err %s", req.fqn, cmn.XattrVersion, errstr)
	} else if len(version) != 0 {
		httpReq.Header.Add(cmn.HeaderObjVersion, string(version))
	}

	// specify source direct URL in request header
	httpReq.Header.Add(cmn.HeaderObjReplicSrc, r.directURL)
	if lom.Cksum != nil {
		cksumType, cksumValue := lom.Cksum.Get()
		httpReq.Header.Add(cmn.HeaderObjCksumType, cksumType)
		httpReq.Header.Add(cmn.HeaderObjCksumVal, cksumValue)
	}
	httpReq.Header.Add(cmn.HeaderObjAtime, strconv.FormatInt(atime.UnixNano(), 10))

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
				resp.StatusCode, string(b), lom.Bucket, lom.Objname, req.remoteDirectURL,
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

// FIXME: use atime
func (r *mpathReplicator) receive(req *replRequest) error {
	var (
		httpReq     = req.httpReq
		lom         = &cluster.LOM{T: r.t, FQN: req.fqn}
		bckProvider = httpReq.URL.Query().Get(cmn.URLParamBckProvider)
	)
	if errstr := lom.Fill(bckProvider, 0); errstr != "" {
		return errors.New(errstr)
	}
	if timeStr := httpReq.Header.Get(cmn.HeaderObjAtime); timeStr != "" {
		atimeInt, err := strconv.ParseInt(timeStr, 10, 64)
		if err != nil {
			errstr := fmt.Sprintf("Failed to parse atime string: %s to int, err: %v", timeStr, err)
			return errors.New(errstr)
		}
		lom.Atime = time.Unix(0, atimeInt) // FIXME: not used
	}
	if err, _ := r.t.doPut(httpReq, lom.Bucket, lom.Objname); err != nil {
		return err
	}
	return nil
}
