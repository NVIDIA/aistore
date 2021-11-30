// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"net/url"

	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/etl"
)

// [METHOD] /v1/etl
func (t *targetrunner) etlHandler(w http.ResponseWriter, r *http.Request) {
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
		apiItems, err := t.checkRESTItems(w, r, 1, true, cmn.URLPathETL.L)
		if err != nil {
			return
		}

		switch apiItems[0] {
		case cmn.ETLList:
			t.listETL(w, r)
		case cmn.ETLLogs:
			t.logsETL(w, r)
		case cmn.ETLObject:
			t.getObjectETL(w, r)
		case cmn.ETLHealth:
			t.healthETL(w, r)
		default:
			t.writeErrURL(w, r)
		}
	case r.Method == http.MethodHead:
		t.headObjectETL(w, r)
	case r.Method == http.MethodDelete:
		t.stopETL(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodHead, http.MethodPost)
	}
}

func (t *targetrunner) initSpecETL(w http.ResponseWriter, r *http.Request) {
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

func (t *targetrunner) initCodeETL(w http.ResponseWriter, r *http.Request) {
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

func (t *targetrunner) stopETL(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.URLPathETLStop.L)
	if err != nil {
		return
	}
	uuid := apiItems[0]
	if err := etl.Stop(t, uuid); err != nil {
		statusCode := http.StatusBadRequest
		if _, ok := err.(*cmn.ErrNotFound); ok {
			statusCode = http.StatusNotFound
		}
		t.writeErr(w, r, err, statusCode)
		return
	}
}

func (t *targetrunner) doETL(w http.ResponseWriter, r *http.Request, uuid string, bck *cluster.Bck, objName string) {
	var (
		comm etl.Communicator
		err  error
	)
	comm, err = etl.GetCommunicator(uuid, t.si)
	if err != nil {
		if _, ok := err.(*cmn.ErrNotFound); ok {
			smap := t.owner.smap.Get()
			t.writeErrStatusf(w, r,
				http.StatusNotFound,
				"%v - try starting new ETL with \"%s/v1/etl/init\" endpoint",
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

func (t *targetrunner) listETL(w http.ResponseWriter, r *http.Request) {
	if _, err := t.checkRESTItems(w, r, 0, false, cmn.URLPathETLList.L); err != nil {
		return
	}
	t.writeJSON(w, r, etl.List(), "list-ETL")
}

func (t *targetrunner) logsETL(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.URLPathETLLogs.L)
	if err != nil {
		return
	}

	uuid := apiItems[0]
	logs, err := etl.PodLogs(t, uuid)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	t.writeJSON(w, r, logs, "logs-ETL")
}

func (t *targetrunner) healthETL(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.URLPathETLHealth.L)
	if err != nil {
		return
	}

	healthMsg, err := etl.PodHealth(t, apiItems[0])
	if err != nil {
		if _, ok := err.(*cmn.ErrNotFound); ok {
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

// GET /v1/etl/objects/<secret>/<uname>
//
// getObjectETL handles GET requests from ETL containers (K8s Pods).
// getObjectETL validates the secret that was injected into a Pod during its initialization.
func (t *targetrunner) getObjectETL(w http.ResponseWriter, r *http.Request) {
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
	t.getObject(w, r, r.URL.Query(), bck, lom)
	cluster.FreeLOM(lom)
}

// HEAD /v1/etl/objects/<secret>/<uname>
//
// headObjectETL handles HEAD requests from ETL containers (K8s Pods).
// headObjectETL validates the secret that was injected into a Pod during its initialization.
func (t *targetrunner) headObjectETL(w http.ResponseWriter, r *http.Request) {
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
