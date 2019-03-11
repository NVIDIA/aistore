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

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
)

type ecManager struct {
	t        *targetrunner
	xact     *ec.XactEC
	bmd      cluster.Bowner // bucket manager
	netReq   string         // network used to send object request
	netResp  string         // network used to send/receive slices
	respPath string         // URL path for constructing response requests
	reqPath  string         // URL path for constructing request requests
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
		bmd:     t.bmdowner,
	}

	var err error
	if ECM.reqPath, err = transport.Register(ECM.netReq, ec.ReqStreamName, ECM.makeRecvRequest()); err != nil {
		glog.Errorf("Failed to register recvRequest: %v", err)
		return nil
	}
	if ECM.respPath, err = transport.Register(ECM.netResp, ec.RespStreamName, ECM.makeRecvResponse()); err != nil {
		glog.Errorf("Failed to register respResponse: %v", err)
		return nil
	}

	return ECM
}

func (mgr *ecManager) newXact() *ec.XactEC {
	return ec.NewXact(mgr.netReq, mgr.netResp, mgr.t, mgr.t.bmdowner,
		mgr.t.smapowner, mgr.t.si)
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
		if mgr.xact == nil || mgr.xact.Finished() {
			mgr.xact = mgr.t.xactions.renewEC()
		}
		mgr.xact.DispatchReq(w, hdr, object)
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
		if mgr.xact == nil || mgr.xact.Finished() {
			mgr.xact = mgr.t.xactions.renewEC()
		}
		mgr.xact.DispatchResp(w, hdr, object)
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
	if mgr.xact == nil || mgr.xact.Finished() {
		mgr.xact = mgr.t.xactions.renewEC()
	}
	if errstr := lom.Fill("", cluster.LomAtime|cluster.LomVersion|cluster.LomCksum); errstr != "" {
		return errors.New(errstr)
	}
	mgr.xact.Encode(req)

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
	if mgr.xact == nil || mgr.xact.Finished() {
		mgr.xact = mgr.t.xactions.renewEC()
	}
	mgr.xact.Cleanup(req)
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
	if mgr.xact == nil || mgr.xact.Finished() {
		mgr.xact = mgr.t.xactions.renewEC()
	}
	mgr.xact.Decode(req)
	// wait for EC completes restoring the object
	return <-req.ErrCh
}
