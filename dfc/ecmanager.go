// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
//
// extended action aka xaction
//
package dfc

import (
	"errors"
	"io"
	"net/http"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/ec"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/transport"
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

func NewECM(t *targetrunner) *ecManager {
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
		mgr.t.smapowner, mgr.t.si, mgr.t.rtnamemap)
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
	cmn.Assert(lom.Bprops != nil)
	cmn.Assert(lom.Fqn != "")
	cmn.Assert(lom.ParsedFQN.MpathInfo != nil && lom.ParsedFQN.MpathInfo.Path != "")

	if !lom.Bprops.ECEnabled {
		return nil
	}
	spec, _ := fs.CSM.FileSpec(lom.Fqn)
	if spec != nil && !spec.PermToProcess() {
		return nil
	}

	req := &ec.Request{
		Action: ec.ActSplit,
		IsCopy: ec.IsECCopy(lom.Size, lom.Bprops),
		LOM:    lom,
	}
	if mgr.xact == nil || mgr.xact.Finished() {
		mgr.xact = mgr.t.xactions.renewEC()
	}
	if errstr := lom.Fill(cluster.LomAtime | cluster.LomVersion | cluster.LomCksum); errstr != "" {
		return errors.New(errstr)
	}
	mgr.xact.Encode(req)

	return nil
}

func (mgr *ecManager) CleanupObject(lom *cluster.LOM) {
	// EC cleanup if EC is enabled
	cmn.Assert(lom.Bprops != nil)
	cmn.Assert(lom.Fqn != "")
	cmn.Assert(lom.ParsedFQN.MpathInfo != nil && lom.ParsedFQN.MpathInfo.Path != "")

	if !lom.Bprops.ECEnabled {
		return
	}
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
	if lom.Bprops == nil || !lom.Bprops.ECEnabled {
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
