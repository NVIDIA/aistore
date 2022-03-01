// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/etl"
)

// [METHOD] /v1/etl
func (t *target) etlHandler(w http.ResponseWriter, r *http.Request) {
	if err := k8s.Detect(); err != nil {
		t.writeErrSilent(w, r, err)
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
//
// handleETLPut is responsible validation and adding new ETL spec/code
// to etl metadata.
func (t *target) handleETLPut(w http.ResponseWriter, r *http.Request) {
	_, err := t.checkRESTItems(w, r, 0, false, apc.URLPathETL.L)
	if err != nil {
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

	switch msg := initMsg.(type) {
	case *etl.InitSpecMsg:
		err = etl.InitSpec(t, *msg, etl.StartOpts{})
	case *etl.InitCodeMsg:
		err = etl.InitCode(t, *msg)
	}
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
}

func (t *target) handleETLGet(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 0, true, apc.URLPathETL.L)
	if err != nil {
		return
	}

	// /v1/etl
	if len(apiItems) == 0 {
		t.writeJSON(w, r, etl.List(), "list-ETL")
		return
	}

	// /v1/etl/_objects/<secret>/<uname>
	if apiItems[0] == apc.ETLObject {
		t.getObjectETL(w, r)
		return
	}

	// /v1/etl/<uuid>
	if len(apiItems) == 1 {
		// TODO: should return info for given UUID
		t.writeErr(w, r, errors.New("not implemented yet"))
		return
	}

	// /v1/etl/<uuid>/logs or /v1/etl/<uuid>/health
	switch apiItems[1] {
	case apc.ETLLogs:
		t.logsETL(w, r, apiItems[0])
	case apc.ETLHealth:
		t.healthETL(w, r, apiItems[0])
	default:
		t.writeErrURL(w, r)
	}
}

// POST /v1/etl/<uuid>/stop (or) TODO: /v1/etl/<uuid>/start
//
// handleETLPost handles start/stop ETL pods
func (t *target) handleETLPost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 2, true, apc.URLPathETL.L)
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

func (t *target) stopETL(w http.ResponseWriter, r *http.Request, etlID string) {
	if err := etl.Stop(t, etlID, cmn.ErrXactUserAbort); err != nil {
		statusCode := http.StatusBadRequest
		if cmn.IsErrNotFound(err) {
			statusCode = http.StatusNotFound
		}
		t.writeErr(w, r, err, statusCode)
		return
	}
}

func (t *target) doETL(w http.ResponseWriter, r *http.Request, uuid string, bck *cluster.Bck, objName string) {
	var (
		comm etl.Communicator
		err  error
	)
	comm, err = etl.GetCommunicator(uuid, t.si)
	if err != nil {
		if cmn.IsErrNotFound(err) {
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
		t.writeErr(w, r, cmn.NewErrETL(&cmn.ETLErrorContext{
			UUID:    uuid,
			PodName: comm.PodName(),
			SvcName: comm.SvcName(),
		}, err.Error()))
	}
}

func (t *target) logsETL(w http.ResponseWriter, r *http.Request, etlID string) {
	logs, err := etl.PodLogs(t, etlID)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	t.writeJSON(w, r, logs, "logs-ETL")
}

func (t *target) healthETL(w http.ResponseWriter, r *http.Request, etlID string) {
	healthMsg, err := etl.PodHealth(t, etlID)
	if err != nil {
		if cmn.IsErrNotFound(err) {
			t.writeErrSilent(w, r, err, http.StatusNotFound)
		} else {
			t.writeErr(w, r, err)
		}
		return
	}
	t.writeJSON(w, r, healthMsg, "health-ETL")
}

func etlParseObjectReq(_ http.ResponseWriter, r *http.Request) (secret string, bck *cluster.Bck, objName string, err error) {
	items, err := cmn.MatchRESTItems(r.URL.EscapedPath(), 2, false, apc.URLPathETLObject.L)
	if err != nil {
		return secret, bck, objName, err
	}
	secret = items[0]
	// Encoding is done in `transformerPath`.
	uname, err := url.PathUnescape(items[1])
	if err != nil {
		return secret, bck, objName, err
	}
	var b cmn.Bck
	b, objName = cmn.ParseUname(uname)
	if err := b.Validate(); err != nil {
		return secret, bck, objName, err
	} else if objName == "" {
		return secret, bck, objName, fmt.Errorf("object name is missing")
	}
	bck = cluster.CloneBck(&b)
	return
}

// GET /v1/etl/_objects/<secret>/<uname>
// NOTE: this is an internal URL, `_objects` in the path is chosen to avoid
// conflicts with ETL UUID in URL paths of format `/v1/elts/<uuid>/...`
//
// getObjectETL handles GET requests from ETL containers (K8s Pods).
// getObjectETL validates the secret that was injected into a Pod during its initialization.
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
	if err := urlQuery(r.URL.RawQuery, dpq); err != nil {
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
// headObjectETL handles HEAD requests from ETL containers (K8s Pods).
// headObjectETL validates the secret that was injected into a Pod during its initialization.
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
	t.headObject(w, r, r.URL.Query(), bck, lom)
	cluster.FreeLOM(lom)
}
