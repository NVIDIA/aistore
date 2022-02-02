// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/etl"
)

// [METHOD] /v1/etls
func (t *target) etlHandler(w http.ResponseWriter, r *http.Request) {
	if err := k8s.Detect(); err != nil {
		t.writeErrSilent(w, r, err)
		return
	}
	switch {
	case r.Method == http.MethodPost:
		apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.URLPathETL.L)
		if err != nil {
			return
		}

		switch apiItems[0] {
		case cmn.ETLInitSpec:
			t.initSpecETL(w, r)
		case cmn.ETLInitCode:
			t.initCodeETL(w, r)
		default:
			t.writeErrURL(w, r)
		}
	case r.Method == http.MethodGet:
		t.handleETLGet(w, r)
	case r.Method == http.MethodHead:
		t.headObjectETL(w, r)
	case r.Method == http.MethodDelete:
		t.stopETL(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodHead, http.MethodPost)
	}
}

func (t *target) handleETLGet(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 0, true, cmn.URLPathETL.L)
	if err != nil {
		return
	}

	// /v1/etls
	if len(apiItems) == 0 {
		t.listETL(w, r)
		return
	}

	// /v1/etls/_objects/<secret>/<uname>
	if apiItems[0] == cmn.ETLObject {
		t.getObjectETL(w, r)
		return
	}

	// /v1/etls/<uuid>
	if len(apiItems) == 1 {
		// TODO: should return info for given UUID
		t.writeErr(w, r, errors.New("not implemented yet"))
		return
	}

	// /v1/etls/<uuid>/logs or /v1/etls/<uuid>/health
	switch apiItems[1] {
	case cmn.ETLLogs:
		t.logsETL(w, r, apiItems[0])
	case cmn.ETLHealth:
		t.healthETL(w, r, apiItems[0])
	default:
		t.writeErrURL(w, r)
	}
}

func (t *target) initSpecETL(w http.ResponseWriter, r *http.Request) {
	var msg etl.InitSpecMsg
	if _, err := t.checkRESTItems(w, r, 0, false, cmn.URLPathETLInitSpec.L); err != nil {
		return
	}
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}

	if err := etl.InitSpec(t, msg, etl.StartOpts{}); err != nil {
		t.writeErr(w, r, err)
		return
	}
}

func (t *target) initCodeETL(w http.ResponseWriter, r *http.Request) {
	var msg etl.InitCodeMsg
	if _, err := t.checkRESTItems(w, r, 0, false, cmn.URLPathETLInitCode.L); err != nil {
		return
	}
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}

	if err := etl.InitCode(t, msg); err != nil {
		t.writeErr(w, r, err)
		return
	}
}

func (t *target) stopETL(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.URLPathETLStop.L)
	if err != nil {
		return
	}
	uuid := apiItems[0]
	if err := etl.Stop(t, uuid, cmn.ErrXactUserAbort); err != nil {
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
				"%v - try starting new ETL with \"%s/v1/etls/init\" endpoint",
				err.Error(), smap.Primary.URL(cmn.NetworkPublic))
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

// GET /v1/etls
func (t *target) listETL(w http.ResponseWriter, r *http.Request) {
	t.writeJSON(w, r, etl.List(), "list-ETL")
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
	items, err := cmn.MatchRESTItems(r.URL.EscapedPath(), 2, false, cmn.URLPathETLObject.L)
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
	bck = cluster.NewBckEmbed(b)
	return
}

// GET /v1/etls/_objects/<secret>/<uname>
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

// HEAD /v1/etls/objects/<secret>/<uname>
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
