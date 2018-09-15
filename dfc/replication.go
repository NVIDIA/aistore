/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

import (
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/common"
	"github.com/NVIDIA/dfcpub/iosgl"
)

// TODO: Instead of using below listed constants, make some of these settings configurable
const (
	replicationPolicyNone  = "none"
	replicationPolicySync  = "sync"
	replicationPolicyAsync = "async"

	replicationActSend    = "send"
	replicationActReceive = "receive"

	replicationDispatchBufferSize = 1024
	replicationServicePoolSize    = 32
)

type replicationRunner struct {
	namedrunner
	t                 *targetrunner
	controlCh         chan struct{}
	serviceDispatchCh chan *replicationService
	serviceControlCh  chan struct{}
}

type replicationService struct {
	t            *targetrunner
	action       string
	srcDirectURL string
	dstDirectURL string
	fqn          string
	resultCh     chan error
	deleteObject bool          // used only on send side
	httpReq      *http.Request // used only on receive side
}

func (rr *replicationRunner) newSendService(dstDirectURL, fqn string, deleteObject, sync bool) *replicationService {
	rserv := &replicationService{
		t:            rr.t,
		action:       replicationActSend,
		srcDirectURL: rr.t.si.ReplNet.DirectURL,
		dstDirectURL: dstDirectURL,
		fqn:          fqn,
		deleteObject: deleteObject,
		httpReq:      nil,
	}
	if sync {
		rserv.resultCh = make(chan error, 1)
	}
	return rserv
}

func (rr *replicationRunner) newReceiveService(srcDirectURL, fqn string, httpReq *http.Request, sync bool) *replicationService {
	rserv := &replicationService{
		t:            rr.t,
		action:       replicationActReceive,
		srcDirectURL: srcDirectURL,
		dstDirectURL: rr.t.si.ReplNet.DirectURL,
		fqn:          fqn,
		deleteObject: false,
		httpReq:      httpReq,
	}
	if sync {
		rserv.resultCh = make(chan error, 1)
	}
	return rserv
}

func newReplicationRunner(t *targetrunner) *replicationRunner {
	return &replicationRunner{
		t:                 t,
		controlCh:         make(chan struct{}),
		serviceDispatchCh: make(chan *replicationService, replicationDispatchBufferSize),
		serviceControlCh:  make(chan struct{}, replicationServicePoolSize),
	}
}

func (rserv *replicationService) send() error {
	var (
		errstr string
		err    error
	)

	bucket, object, err := rserv.t.fqn2bckobj(rserv.fqn)
	if err != nil {
		errstr = fmt.Sprintf("Failed to extract bucket and object name from %s, error: %v", rserv.fqn, err)
		return errors.New(errstr)
	}

	url := rserv.dstDirectURL + api.URLPath(api.Version, api.Objects, bucket, object)

	uname := uniquename(bucket, object)
	rserv.t.rtnamemap.lockname(uname, rserv.deleteObject, &pendinginfo{Time: time.Now(), fqn: rserv.fqn}, time.Second)
	defer rserv.t.rtnamemap.unlockname(uname, rserv.deleteObject)

	file, err := os.Open(rserv.fqn)
	if err != nil {
		if os.IsPermission(err) {
			errstr = fmt.Sprintf("Permission denied: access forbidden to %s", rserv.fqn)
		} else {
			errstr = fmt.Sprintf("Failed to open local file %s, error: %v", rserv.fqn, err)
		}
		rserv.t.fshc(err, rserv.fqn)
		return errors.New(errstr)
	}
	defer file.Close()

	xxhashbinary, errstr := Getxattr(rserv.fqn, XattrXXHashVal)
	xxhashval := ""
	if errstr != "" {
		buf, slab := iosgl.AllocFromSlab(0)
		xxhashval, errstr = ComputeXXHash(file, buf)
		slab.Free(buf)
		if errstr != "" {
			errstr = fmt.Sprintf("Failed to calculate checksum on %s, error: %s", rserv.fqn, errstr)
			return errors.New(errstr)
		}
	} else {
		xxhashval = string(xxhashbinary)
	}

	req, err := http.NewRequest(http.MethodPut, url, file)
	if err != nil {
		errstr = fmt.Sprintf("Failed to create HTTP request, err: %v", err)
		return errors.New(errstr)
	}

	// specify source direct URL in request header
	req.Header.Add(api.HeaderDFCReplicationSrc, rserv.srcDirectURL)

	req.Header.Add(api.HeaderDFCChecksumType, ChecksumXXHash)
	req.Header.Add(api.HeaderDFCChecksumVal, xxhashval)

	resp, err := rserv.t.httpclientLongTimeout.Do(req)
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
				resp.StatusCode, string(b), bucket, object, rserv.dstDirectURL,
			)
		}
		return errors.New(errstr)
	}

	if rserv.deleteObject {
		if err := os.Remove(rserv.fqn); err != nil {
			errstr = fmt.Sprintf("failed to remove local file %s, error: %v", rserv.fqn, err)
			return errors.New(errstr)
		}
	}

	return nil
}

func (rserv *replicationService) receive() error {
	var (
		nhobj         cksumvalue
		nhtype, nhval string
		sgl           *iosgl.SGL
		errstr        string
	)
	r := rserv.httpReq
	putfqn := rserv.t.fqn2workfile(rserv.fqn)
	bucket, object, err := rserv.t.fqn2bckobj(rserv.fqn)
	if err != nil {
		errstr = fmt.Sprintf("Failed to extract bucket and object name from %s, error: %v", rserv.fqn, err)
		return errors.New(errstr)
	}

	hdhobj := newcksumvalue(r.Header.Get(api.HeaderDFCChecksumType), r.Header.Get(api.HeaderDFCChecksumVal))
	if hdhobj == nil {
		errstr = fmt.Sprintf("Failed to extract checksum from replication PUT request for %s/%s", bucket, object)
		return errors.New(errstr)
	}
	hdhtype, hdhval := hdhobj.get()
	if hdhtype != ChecksumXXHash {
		errstr = fmt.Sprintf("Unsupported checksum type: %s", hdhtype)
		return errors.New(errstr)
	}

	// optimize out if checksums from header and existing file match
	if file, err := os.Open(rserv.fqn); err == nil {
		buf, slab := iosgl.AllocFromSlab(0)
		xxhashval, errstr := ComputeXXHash(file, buf)
		slab.Free(buf)
		if err = file.Close(); err != nil {
			glog.Warningf("Unexpected failure to close %s once xxhash has been computed, error: %v", rserv.fqn, err)
		}
		if errstr == "" && xxhashval == hdhval {
			glog.Infof("Existing %s/%s is valid: replication PUT is a no-op", bucket, object)
			return nil
		}
	}

	// TODO:
	// targetrunner.receive validates checksum based on cluster-level or bucket-level
	// checksum configuration. Replication service needs its own checksum configuration
	sgl, nhobj, _, errstr = rserv.t.receive(putfqn, object, "", hdhobj, r.Body)
	r.Body.Close()
	if errstr != "" {
		return errors.New(errstr)
	}

	if nhobj != nil {
		nhtype, nhval = nhobj.get()
		common.Assert(hdhtype == nhtype)
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

	props := &objectProps{nhobj: nhobj}
	errstr, _ = rserv.t.putCommit(rserv.t.contextWithAuth(r), bucket, object, putfqn, rserv.fqn, props, false /* rebalance */)
	if errstr != "" {
		return errors.New(errstr)
	}
	return nil
}

func (rserv *replicationService) wait() error {
	common.Assert(rserv.resultCh != nil)
	return <-rserv.resultCh
}

func (rr *replicationRunner) run() error {
	for {
		select {
		case service := <-rr.serviceDispatchCh:
			go func() {
				var err error

				// allow at most replicationServicePoolSize services
				// to run concurrently
				rr.serviceControlCh <- struct{}{}

				if service.action == replicationActSend {
					err = service.send()
				} else { // replicationActReceive
					err = service.receive()
				}

				if err != nil {
					glog.Errorf("Error occurred during object replication: "+
						"action: %s %s, source: %s, destination: %s",
						service.action, service.fqn, service.srcDirectURL, service.dstDirectURL)
				}

				if service.resultCh != nil {
					service.resultCh <- err
					close(service.resultCh)
				}

				<-rr.serviceControlCh
			}()
		case <-rr.controlCh:
			return nil
		}
	}
}

func (rr *replicationRunner) stop(err error) {
	rr.controlCh <- struct{}{}
	glog.Warningf("Replication runner stopped with error: %v", err)
}

func (rr *replicationRunner) dispatchService(rserv *replicationService) {
	rr.serviceDispatchCh <- rserv
}

func (rr *replicationRunner) sendReplica(dstDirectURL, fqn string, deleteObject bool, policy string) error {

	switch policy {
	case replicationPolicyAsync:
		rr.dispatchService(rr.newSendService(dstDirectURL, fqn, deleteObject, false))

	case replicationPolicySync:
		service := rr.newSendService(dstDirectURL, fqn, deleteObject, true)
		rr.dispatchService(service)
		return service.wait()

	case replicationPolicyNone:
		return nil // do nothing

	default:
		errstr := fmt.Sprintf("Invalid replication policy: %q. Expected: %q|%q|%q",
			policy, replicationPolicySync, replicationPolicyAsync, replicationPolicyNone)
		return errors.New(errstr)
	}

	return nil
}

func (rr *replicationRunner) receiveReplica(srcDirectURL, fqn string, r *http.Request) error {
	service := rr.newReceiveService(srcDirectURL, fqn, r, true)
	rr.dispatchService(service)
	return service.wait()
}

/*
 *
 * fsprunner methods
 *
 */

func (rr *replicationRunner) reqAddMountpath(mpath string) {
	return
}

func (rr *replicationRunner) reqRemoveMountpath(mpath string) {
	return
}

func (rr *replicationRunner) reqEnableMountpath(mpath string) {
	return
}

func (rr *replicationRunner) reqDisableMountpath(mpath string) {
	return
}
