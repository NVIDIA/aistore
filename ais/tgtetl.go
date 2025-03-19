// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/xact"
)

// [METHOD] /v1/etl
func (t *target) etlHandler(w http.ResponseWriter, r *http.Request) {
	if !k8s.IsK8s() {
		t.writeErr(w, r, k8s.ErrK8sRequired, 0, Silent)
		return
	}
	switch r.Method {
	case http.MethodPut:
		t.handleETLPut(w, r)
	case http.MethodPost:
		t.handleETLPost(w, r)
	case http.MethodDelete:
		t.handleETLDelete(w, r)
	case http.MethodGet:
		t.handleETLGet(w, r)
	case http.MethodHead:
		t.headObjectETL(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodGet, http.MethodHead, http.MethodPost)
	}
}

// PUT /v1/etl
// init ETL spec/code
func (t *target) handleETLPut(w http.ResponseWriter, r *http.Request) {
	// disallow to run when above high wm (let alone OOS)
	cs := fs.Cap()
	if err := cs.Err(); err != nil {
		t.writeErr(w, r, err, http.StatusInsufficientStorage)
		return
	}
	if _, err := t.parseURL(w, r, apc.URLPathETL.L, 0, false); err != nil {
		t.writeErr(w, r, err)
		return
	}
	if err := t.initETLFromMsg(r); err != nil {
		t.writeErr(w, r, err)
		return
	}
}

func (t *target) handleETLGet(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.parseURL(w, r, apc.URLPathETL.L, 0, true)
	if err != nil {
		return
	}

	// /v1/etl
	if len(apiItems) == 0 {
		t.writeJSON(w, r, etl.List(), "list-etl")
		return
	}

	// /v1/etl/_object/<secret>/<uname>
	if apiItems[0] == apc.ETLObject {
		t.getObjectETL(w, r)
		return
	}

	// /v1/etl/<etl-name>
	if len(apiItems) == 1 {
		t.writeErr(w, r, fmt.Errorf("GET(ETL[%s] info) not implemented yet", apiItems[0]), http.StatusNotImplemented)
		return
	}

	// /v1/etl/<etl-name>/logs or /v1/etl/<etl-name>/health or /v1/etl/<etl-name>/metrics
	switch apiItems[1] {
	case apc.ETLLogs:
		t.logsETL(w, r, apiItems[0])
	case apc.ETLHealth:
		t.healthETL(w, r, apiItems[0])
	case apc.ETLMetrics:
		k8s.InitMetricsClient()
		t.metricsETL(w, r, apiItems[0])
	default:
		t.writeErrURL(w, r)
	}
}

// POST /v1/etl/<etl-name>/stop (or) /v1/etl/<etl-name>/start
//
// Handles starting/stopping ETL pods
func (t *target) handleETLPost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.parseURL(w, r, apc.URLPathETL.L, 2, true)
	if err != nil {
		return
	}
	switch op := apiItems[1]; op {
	case apc.ETLStop:
		t.stopETL(w, r, apiItems[0])
	case apc.ETLStart:
		t.startETL(w, r)
	default:
		debug.Assert(false, "invalid operation: "+op)
		t.writeErrAct(w, r, "invalid operation: "+op)
	}
}

// DELETE /v1/etl/<etl-name>
//
// Handles deleting ETL pods
func (t *target) handleETLDelete(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.parseURL(w, r, apc.URLPathETL.L, 1, true)
	if err != nil {
		return
	}
	if err := etl.Delete(apiItems[0]); err != nil {
		t.writeErr(w, r, err)
	}
}

func (t *target) startETL(w http.ResponseWriter, r *http.Request) {
	if err := t.initETLFromMsg(r); err != nil {
		t.writeErr(w, r, err)
	}
}

func (t *target) stopETL(w http.ResponseWriter, r *http.Request, etlName string) {
	if err := etl.Stop(etlName, cmn.ErrXactUserAbort); err != nil {
		if cos.IsErrNotFound(err) {
			nlog.Infof("ETL %q doesn't exist on target\n", etlName)
			return
		}
		t.writeErr(w, r, err)
	}
}

func (t *target) getFromETL(w http.ResponseWriter, r *http.Request, dpq *dpq, lom *core.LOM) {
	var (
		name  = dpq.etl.name  // apc.QparamETLName
		targs = dpq.etl.targs // apc.QparamETLTransformArgs
	)
	comm, err := etl.GetCommunicator(name)
	if err != nil {
		if cos.IsErrNotFound(err) {
			smap := t.owner.smap.Get()
			errV := fmt.Errorf("%v - try starting new ETL with \"%s/v1/etl/init\" endpoint",
				err, smap.Primary.URL(cmn.NetPublic))
			t.writeErr(w, r, errV, http.StatusNotFound)
			return
		}
		t.writeErr(w, r, err)
		return
	}

	xetl := comm.Xact()
	ecode, err := comm.InlineTransform(w, r, lom, targs)
	if err != nil {
		// Wait for possible abort errors from the xaction (e.g., pod runtime errors)
		if abortErr := xetl.AbortedAfter(etl.DefaultReqTimeout); abortErr != nil {
			t.writeErr(w, r, abortErr, ecode)
			return
		}
		ectx := &cmn.ETLErrCtx{
			ETLName:          name,
			ETLTransformArgs: targs,
			PodName:          comm.PodName(),
			SvcName:          comm.SvcName(),
		}
		errV := cmn.NewErrETL(ectx, err.Error(), ecode)
		xetl.AddErr(errV)
		t.writeErr(w, r, errV, ecode)
		return
	}

	xetl.ObjsAdd(1, lom.Lsize(true)) // _special_ as the transformed size could be `cos.ContentLengthUnknown` at this point
}

func (t *target) logsETL(w http.ResponseWriter, r *http.Request, etlName string) {
	logs, err := etl.PodLogs(etlName)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	t.writeJSON(w, r, logs, "logs-etl")
}

func (t *target) healthETL(w http.ResponseWriter, r *http.Request, etlName string) {
	health, err := etl.PodHealth(etlName)
	if err != nil {
		if cos.IsErrNotFound(err) {
			t.writeErr(w, r, err, http.StatusNotFound, Silent)
		} else {
			t.writeErr(w, r, err)
		}
		return
	}
	writeXid(w, health)
}

func (t *target) metricsETL(w http.ResponseWriter, r *http.Request, etlName string) {
	metricMsg, err := etl.PodMetrics(etlName)
	if err != nil {
		if cos.IsErrNotFound(err) {
			t.writeErr(w, r, err, http.StatusNotFound, Silent)
		} else {
			t.writeErr(w, r, err)
		}
		return
	}
	t.writeJSON(w, r, metricMsg, "metrics-etl")
}

func etlParseObjectReq(_ http.ResponseWriter, r *http.Request) (secret string, bck *meta.Bck, objName string, err error) {
	var items []string
	items, err = cmn.ParseURL(r.URL.EscapedPath(), apc.URLPathETLObject.L, 2, false)
	if err != nil {
		return
	}
	secret = items[0]
	// Encoding is done in `transformerPath`.
	var uname string
	uname, err = url.PathUnescape(items[1])
	if err != nil {
		return
	}
	var b cmn.Bck
	b, objName = cmn.ParseUname(uname)
	if err = b.Validate(); err != nil {
		err = fmt.Errorf("%v, uname=%q", err, uname)
		return
	}
	if objName == "" {
		err = fmt.Errorf("object name is missing (bucket=%s, uname=%q)", b.String(), uname)
		return
	}
	bck = meta.CloneBck(&b)
	return
}

// GET /v1/etl/_object/<secret>/<uname>
// Handles GET requests from ETL containers (K8s Pods).
// Validates the secret that was injected into a Pod during its initialization
// (see boot.go `_setPodEnv`).
//
// NOTE: this is an internal URL with "_object" in its path intended to avoid
// conflicts with ETL name in `/v1/elts/<etl-name>/...`
func (t *target) getObjectETL(w http.ResponseWriter, r *http.Request) {
	secret, bck, objName, err := etlParseObjectReq(w, r)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	if err := etl.CheckSecret(secret); err != nil {
		t.writeErr(w, r, err)
		return
	}
	dpq := dpqAlloc()
	if err := dpq.parse(r.URL.RawQuery); err != nil {
		dpqFree(dpq)
		t.writeErr(w, r, err)
		return
	}
	lom := core.AllocLOM(objName)
	lom, err = t.getObject(w, r, dpq, bck, lom)
	core.FreeLOM(lom)

	if err != nil {
		t._erris(w, r, err, 0, dpq.silent)
	}
	dpqFree(dpq)
}

// HEAD /v1/etl/objects/<secret>/<uname>
//
// Handles HEAD requests from ETL containers (K8s Pods).
// Validates the secret that was injected into a Pod during its initialization.
func (t *target) headObjectETL(w http.ResponseWriter, r *http.Request) {
	secret, bck, objName, err := etlParseObjectReq(w, r)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	if err := etl.CheckSecret(secret); err != nil {
		t.writeErr(w, r, err)
		return
	}

	lom := core.AllocLOM(objName)
	ecode, err := t.objHead(r, w.Header(), r.URL.Query(), bck, lom)
	core.FreeLOM(lom)
	if err != nil {
		// always silent (compare w/ httpobjhead)
		t.writeErr(w, r, err, ecode, Silent)
	}
}

func (t *target) initETLFromMsg(r *http.Request) error {
	b, err := cos.ReadAll(r.Body)
	if err != nil {
		return err
	}
	r.Body.Close()

	initMsg, err := etl.UnmarshalInitMsg(b)
	if err != nil {
		return err
	}
	xid := r.URL.Query().Get(apc.QparamUUID)

	switch msg := initMsg.(type) {
	case *etl.InitSpecMsg:
		err = etl.InitSpec(msg, xid, etl.StartOpts{})
	case *etl.InitCodeMsg:
		err = etl.InitCode(msg, xid)
	default:
		debug.Assert(false, initMsg.String())
	}

	if err != nil {
		return err
	}

	// setup proxy notification on abort
	comm, err := etl.GetCommunicator(initMsg.Name())
	if err != nil {
		return err
	}

	xetl := comm.Xact()
	notif := &xact.NotifXact{
		Base: nl.Base{When: core.UponTerm, Dsts: []string{equalIC}, F: t.notifyTerm},
		Xact: xetl,
	}
	xetl.AddNotif(notif)

	return err
}
