// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
//
// extended action aka xaction
//
package ais

import (
	"errors"
	"io"
	"net/http"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
)

type ecManager struct {
	sync.RWMutex
	t          *targetrunner
	xacts      map[string]*ec.XactEC // bckName -> xact map, only local buckets allowed, no naming collisions
	bowner     cluster.Bowner        // bucket manager
	netReq     string                // network used to send object request
	netResp    string                // network used to send/receive slices
	reqBundle  *transport.StreamBundle
	respBundle *transport.StreamBundle
}

var ECM *ecManager

func newECM(t *targetrunner) *ecManager {
	config := cmn.GCO.Get()
	netReq, netResp := cmn.NetworkIntraControl, cmn.NetworkIntraData
	if !config.Net.UseIntraControl {
		netReq = cmn.NetworkPublic
	}
	if !config.Net.UseIntraData {
		netResp = cmn.NetworkPublic
	}

	ECM = &ecManager{
		netReq:  netReq,
		netResp: netResp,
		t:       t,
		bowner:  t.bmdowner,
		xacts:   make(map[string]*ec.XactEC),
	}

	ECM.initECBundles()

	var err error
	if _, err = transport.Register(ECM.netReq, ec.ReqStreamName, ECM.makeRecvRequest()); err != nil {
		glog.Errorf("Failed to register recvRequest: %v", err)
		return nil
	}
	if _, err = transport.Register(ECM.netResp, ec.RespStreamName, ECM.makeRecvResponse()); err != nil {
		glog.Errorf("Failed to register respResponse: %v", err)
		return nil
	}

	return ECM
}

func (mgr *ecManager) initECBundles() {
	cmn.AssertMsg(mgr.reqBundle == nil && mgr.respBundle == nil, "EC Bundles have been already initialized")

	cbReq := func(hdr transport.Header, reader io.ReadCloser, err error) {
		if err != nil {
			glog.Errorf("Failed to request %s/%s: %v", hdr.Bucket, hdr.Objname, err)
		}
	}

	client := transport.NewDefaultClient()
	extraReq := transport.Extra{Callback: cbReq}

	reqSbArgs := transport.SBArgs{
		Multiplier: transport.IntraBundleMultiplier,
		Extra:      &extraReq,
		Network:    mgr.netReq,
		Trname:     ec.ReqStreamName,
	}

	respSbArgs := transport.SBArgs{
		Multiplier: transport.IntraBundleMultiplier,
		Trname:     ec.RespStreamName,
		Network:    mgr.netResp,
	}

	mgr.reqBundle = transport.NewStreamBundle(mgr.t.smapowner, mgr.t.si, client, reqSbArgs)
	mgr.respBundle = transport.NewStreamBundle(mgr.t.smapowner, mgr.t.si, client, respSbArgs)
}

func (mgr *ecManager) newXact(bucket string) *ec.XactEC {
	return ec.NewXact(mgr.t, mgr.t.bmdowner,
		mgr.t.smapowner, mgr.t.si, bucket, mgr.reqBundle, mgr.respBundle)
}

func (mgr *ecManager) getBckXact(bckName string) *ec.XactEC {
	mgr.RLock()
	defer mgr.RUnlock()

	return mgr.xacts[bckName]
}

func (mgr *ecManager) setBckXact(bckName string, xact *ec.XactEC) {
	mgr.Lock()
	mgr.xacts[bckName] = xact
	mgr.Unlock()
}

func (mgr *ecManager) restoreBckXact(bckName string) *ec.XactEC {
	xact := mgr.getBckXact(bckName)
	if xact == nil || xact.Finished() {
		newXact := mgr.t.xactions.renewEC(bckName)
		mgr.setBckXact(bckName, newXact)
		return newXact
	}

	return xact
}

// A function to process command requests from other targets
func (mgr *ecManager) makeRecvRequest() transport.Receive {
	return func(w http.ResponseWriter, hdr transport.Header, object io.Reader, err error) {
		if err != nil {
			glog.Errorf("Request failed: %v", err)
			return
		}
		// check if the header contains a valid request
		if len(hdr.Opaque) == 0 {
			glog.Error("Empty request")
			return
		}

		mgr.restoreBckXact(hdr.Bucket).DispatchReq(w, hdr, object)
	}
}

// A function to process big chunks of data (replica/slice/meta) sent from other targets
func (mgr *ecManager) makeRecvResponse() transport.Receive {
	return func(w http.ResponseWriter, hdr transport.Header, object io.Reader, err error) {
		if err != nil {
			glog.Errorf("Receive failed: %v", err)
			return
		}
		// check if the request is valid
		if len(hdr.Opaque) == 0 {
			glog.Error("Empty request")
			return
		}

		mgr.restoreBckXact(hdr.Bucket).DispatchResp(w, hdr, object)
	}
}

func (mgr *ecManager) EncodeObject(lom *cluster.LOM) error {
	if !lom.BckProps.EC.Enabled {
		return ec.ErrorECDisabled
	}
	cmn.Assert(lom.FQN != "")
	cmn.Assert(lom.ParsedFQN.MpathInfo != nil && lom.ParsedFQN.MpathInfo.Path != "")

	if lom.T.OOS() {
		return errors.New("OOS") // out of space
	}
	spec, _ := fs.CSM.FileSpec(lom.FQN)
	if spec != nil && !spec.PermToProcess() {
		return nil
	}

	req := &ec.Request{
		Action: ec.ActSplit,
		IsCopy: ec.IsECCopy(lom.Size, &lom.BckProps.EC),
		LOM:    lom,
	}

	xact := mgr.restoreBckXact(lom.Bucket)

	if errstr := lom.Fill("", cluster.LomAtime|cluster.LomVersion|cluster.LomCksum); errstr != "" {
		return errors.New(errstr)
	}
	xact.Encode(req)

	return nil
}

func (mgr *ecManager) CleanupObject(lom *cluster.LOM) {
	if !lom.BckProps.EC.Enabled {
		return
	}
	cmn.Assert(lom.FQN != "")
	cmn.Assert(lom.ParsedFQN.MpathInfo != nil && lom.ParsedFQN.MpathInfo.Path != "")
	req := &ec.Request{
		Action: ec.ActDelete,
		LOM:    lom,
	}

	mgr.restoreBckXact(lom.Bucket).Cleanup(req)
}

func (mgr *ecManager) RestoreObject(lom *cluster.LOM) error {
	if !lom.BckProps.EC.Enabled {
		return ec.ErrorECDisabled
	}

	cmn.Assert(lom.ParsedFQN.MpathInfo != nil && lom.ParsedFQN.MpathInfo.Path != "")
	req := &ec.Request{
		Action: ec.ActRestore,
		LOM:    lom,
		ErrCh:  make(chan error), // unbuffered
	}

	mgr.restoreBckXact(lom.Bucket).Decode(req)
	// wait for EC completes restoring the object
	return <-req.ErrCh
}
