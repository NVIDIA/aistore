// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/ext/etl"
)

// [METHOD] /v1/etl
func (t *target) etlHandler(w http.ResponseWriter, r *http.Request) {
	if err := k8s.Detect(); err != nil {
		t.writeErr(w, r, err, 0, Silent)
		return
	}
	switch {
	case r.Method == http.MethodPut:
		t.handleETLPut(w, r)
	case r.Method == http.MethodPost:
		t.handleETLPost(w, r)
	case r.Method == http.MethodGet:
		t.handleETLGet(w, r)
	case r.Method == http.MethodHead:
		t.headObjectETL(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodGet, http.MethodHead, http.MethodPost)
	}
}

// PUT /v1/etl
// start ETL spec/code
func (t *target) handleETLPut(w http.ResponseWriter, r *http.Request) {
	if _, err := t.apiItems(w, r, 0, false, apc.URLPathETL.L); err != nil {
		return
	}

	b, err := io.ReadAll(r.Body)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	r.Body.Close()

	initMsg, err := etl.UnmarshalInitMsg(b)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	xid := r.URL.Query().Get(apc.QparamUUID)

	switch msg := initMsg.(type) {
	case *etl.InitSpecMsg:
		err = etl.InitSpec(t, msg, xid, etl.StartOpts{})
	case *etl.InitCodeMsg:
		err = etl.InitCode(t, msg, xid)
	default:
		debug.Assert(false, initMsg.String())
	}
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	if cmn.FastV(4, cos.SmoduleETL) {
		glog.Infoln(t.String() + ": " + initMsg.String())
	}
}

func (t *target) handleETLGet(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.apiItems(w, r, 0, true, apc.URLPathETL.L)
	if err != nil {
		return
	}

	// /v1/etl
	if len(apiItems) == 0 {
		t.writeJSON(w, r, etl.List(), "list-etl")
		return
	}

	// /v1/etl/_objects/<secret>/<uname>
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
		t.metricsETL(w, r, apiItems[0])
	default:
		t.writeErrURL(w, r)
	}
}

// POST /v1/etl/<etl-name>/stop (or) TODO: /v1/etl/<etl-name>/start
//
// Handles starting/stopping ETL pods
func (t *target) handleETLPost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.apiItems(w, r, 2, true, apc.URLPathETL.L)
	if err != nil {
		return
	}
	if apiItems[1] == apc.ETLStop {
		t.stopETL(w, r, apiItems[0])
		return
	}
	// TODO: Implement ETLStart to start inactive ETLs
	t.writeErrURL(w, r)
}

func (t *target) stopETL(w http.ResponseWriter, r *http.Request, etlName string) {
	if err := etl.Stop(t, etlName, cmn.ErrXactUserAbort); err != nil {
		statusCode := http.StatusBadRequest
		if cos.IsErrNotFound(err) {
			statusCode = http.StatusNotFound
		}
		t.writeErr(w, r, err, statusCode)
		return
	}
}

func (t *target) doETL(w http.ResponseWriter, r *http.Request, etlName string, bck *meta.Bck, objName string) {
	var (
		comm etl.Communicator
		err  error
	)
	comm, err = etl.GetCommunicator(etlName, t.si)
	if err != nil {
		if cos.IsErrNotFound(err) {
			smap := t.owner.smap.Get()
			t.writeErrStatusf(w, r,
				http.StatusNotFound,
				"%v - try starting new ETL with \"%s/v1/etl/init\" endpoint",
				err.Error(), smap.Primary.URL(cmn.NetPublic))
			return
		}
		t.writeErr(w, r, err)
		return
	}
	if err := comm.OnlineTransform(w, r, bck, objName); err != nil {
		t.writeErr(w, r, cmn.NewErrETL(&cmn.ETLErrCtx{
			ETLName: etlName,
			PodName: comm.PodName(),
			SvcName: comm.SvcName(),
		}, err.Error()))
	}
}

func (t *target) logsETL(w http.ResponseWriter, r *http.Request, etlName string) {
	logs, err := etl.PodLogs(t, etlName)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	t.writeJSON(w, r, logs, "logs-etl")
}

func (t *target) healthETL(w http.ResponseWriter, r *http.Request, etlName string) {
	health, err := etl.PodHealth(t, etlName)
	if err != nil {
		if cos.IsErrNotFound(err) {
			t.writeErr(w, r, err, http.StatusNotFound, Silent)
		} else {
			t.writeErr(w, r, err)
		}
		return
	}
	w.Header().Set(cos.HdrContentLength, strconv.Itoa(len(health)))
	w.Write([]byte(health))
}

func (t *target) metricsETL(w http.ResponseWriter, r *http.Request, etlName string) {
	metricMsg, err := etl.PodMetrics(t, etlName)
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
	items, err = cmn.MatchItems(r.URL.EscapedPath(), 2, false, apc.URLPathETLObject.L)
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
		return
	}
	if objName == "" {
		err = fmt.Errorf("etl-parse-req: object name is missing (bucket %s)", b)
		return
	}
	bck = meta.CloneBck(&b)
	return
}

// GET /v1/etl/_objects/<secret>/<uname>
// Handles GET requests from ETL containers (K8s Pods).
// Validates the secret that was injected into a Pod during its initialization.
//
// NOTE: this is an internal URL with `_objects` in its path intended to avoid
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
	if err := dpq.fromRawQ(r.URL.RawQuery); err != nil {
		dpqFree(dpq)
		t.writeErr(w, r, err)
		return
	}
	lom := cluster.AllocLOM(objName)
	t.getObject(w, r, dpq, bck, lom)
	cluster.FreeLOM(lom)
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

	lom := cluster.AllocLOM(objName)
	errCode, err := t.objhead(w.Header(), r.URL.Query(), bck, lom)
	cluster.FreeLOM(lom)
	if err != nil {
		// always silent (compare w/ httpobjhead)
		t.writeErr(w, r, err, errCode, Silent)
	}
}
