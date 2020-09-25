// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"io/ioutil"
	"net/http"
	"sort"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/etl"
)

/////////////////
// ETL: target //
/////////////////

// [METHOD] /v1/etl
func (t *targetrunner) etlHandler(w http.ResponseWriter, r *http.Request) {
	if err := k8s.Detect(); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	switch {
	case r.Method == http.MethodPost:
		apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.ETL)
		if err != nil {
			return
		}

		switch apiItems[0] {
		case cmn.ETLInit:
			t.initETL(w, r)
		case cmn.ETLBuild:
			t.buildETL(w, r)
		default:
			t.invalmsghdlrf(w, r, "invalid POST path: %s", apiItems[0])
		}
	case r.Method == http.MethodGet:
		apiItems, err := t.checkRESTItems(w, r, 1, true, cmn.Version, cmn.ETL)
		if err != nil {
			return
		}

		switch apiItems[0] {
		case cmn.ETLList:
			t.listETL(w, r)
		case cmn.ETLLogs:
			t.logsETL(w, r)
		case cmn.ETLObject: // TODO: maybe it should be just a default
			t.getObjectETL(w, r)
		}
	case r.Method == http.MethodHead:
		t.headObjectETL(w, r)
	case r.Method == http.MethodDelete:
		t.stopETL(w, r)
	default:
		t.invalmsghdlrf(w, r, "Invalid HTTP Method: %v %s", r.Method, r.URL.Path)
	}
}

func (t *targetrunner) initETL(w http.ResponseWriter, r *http.Request) {
	var msg etl.InitMsg
	if _, err := t.checkRESTItems(w, r, 0, false, cmn.Version, cmn.ETL, cmn.ETLInit); err != nil {
		return
	}
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}
	if err := etl.Start(t, msg); err != nil {
		t.invalmsghdlr(w, r, err.Error())
	}
}

func (t *targetrunner) buildETL(w http.ResponseWriter, r *http.Request) {
	var msg etl.BuildMsg
	if _, err := t.checkRESTItems(w, r, 0, false, cmn.Version, cmn.ETL, cmn.ETLBuild); err != nil {
		return
	}
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}
	if err := etl.Build(t, msg); err != nil {
		t.invalmsghdlr(w, r, err.Error())
	}
}

func (t *targetrunner) stopETL(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.ETL, cmn.ETLStop)
	if err != nil {
		return
	}
	uuid := apiItems[0]
	if err := etl.Stop(t, uuid); err != nil {
		statusCode := http.StatusBadRequest
		if _, ok := err.(*cmn.NotFoundError); ok {
			statusCode = http.StatusNotFound
		}
		t.invalmsghdlr(w, r, err.Error(), statusCode)
	}
}

func (t *targetrunner) doETL(w http.ResponseWriter, r *http.Request, uuid string, bck *cluster.Bck, objName string) {
	var (
		comm etl.Communicator
		err  error
	)
	comm, err = etl.GetCommunicator(uuid)
	if err != nil {
		if _, ok := err.(*cmn.NotFoundError); ok {
			smap := t.owner.smap.Get()
			t.invalmsghdlrstatusf(w, r,
				http.StatusNotFound,
				"%v - try starting new ETL with \"%s/v1/etl/init\" endpoint",
				err.Error(), smap.Primary.URL(cmn.NetworkPublic))
			return
		}
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if err := comm.Do(w, r, bck, objName); err != nil {
		t.invalmsghdlr(w, r, cmn.NewETLError(&cmn.ETLErrorContext{
			UUID:    uuid,
			PodName: comm.PodName(),
			SvcName: comm.SvcName(),
		}, err.Error()).Error())
	}
}

func (t *targetrunner) listETL(w http.ResponseWriter, r *http.Request) {
	if _, err := t.checkRESTItems(w, r, 0, false, cmn.Version, cmn.ETL, cmn.ETLList); err != nil {
		return
	}
	t.writeJSON(w, r, etl.List(), "list-ETL")
}

func (t *targetrunner) logsETL(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.ETL, cmn.ETLLogs)
	if err != nil {
		return
	}

	uuid := apiItems[0]
	logs, err := etl.PodLogs(t, uuid)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	t.writeJSON(w, r, logs, "logs-ETL")
}

// GET /v1/etl/objects/<secret>/<bucket-name>/<object-name>
//
// getObjectETL is the handler for the requests that come from the K8s Pods.
// It validates the secret that was inject into the Pod during initialization
// to ensure that the origin is indeed correct.
func (t *targetrunner) getObjectETL(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 3, false, cmn.Version, cmn.ETL, cmn.ETLObject)
	if err != nil {
		return
	}

	var (
		secret  = apiItems[0]
		bucket  = apiItems[1]
		objName = apiItems[2]
	)

	if err := etl.CheckSecret(secret); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	t.getObject(w, r, r.URL.Query(), bucket, objName)
}

// HEAD /v1/etl/objects/<secret>/<bucket-name>/<object-name>
//
// headObjectETL is the handler for the requests that come from the K8s Pods.
// It validates the secret that was inject into the Pod during initialization
// to ensure that the origin is indeed correct.
func (t *targetrunner) headObjectETL(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 3, false, cmn.Version, cmn.ETL, cmn.ETLObject)
	if err != nil {
		return
	}

	var (
		secret  = apiItems[0]
		bucket  = apiItems[1]
		objName = apiItems[2]
	)

	if err := etl.CheckSecret(secret); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	t.headObject(w, r, r.URL.Query(), bucket, objName)
}

////////////////
// ETL: proxy //
////////////////

// [METHOD] /v1/etl
func (p *proxyrunner) etlHandler(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodPost:
		apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.ETL)
		if err != nil {
			return
		}
		switch apiItems[0] {
		case cmn.ETLInit:
			p.initETL(w, r)
		case cmn.ETLBuild:
			p.buildETL(w, r)
		default:
			p.invalmsghdlrf(w, r, "invalid POST path: %s", apiItems[0])
		}
	case r.Method == http.MethodGet:
		apiItems, err := p.checkRESTItems(w, r, 1, true, cmn.Version, cmn.ETL)
		if err != nil {
			return
		}

		switch apiItems[0] {
		case cmn.ETLList:
			p.listETL(w, r)
		case cmn.ETLLogs:
			p.logsETL(w, r)
		default:
			p.invalmsghdlrf(w, r, "invalid GET path: %s", apiItems[0])
		}
	case r.Method == http.MethodDelete:
		p.stopETL(w, r)
	default:
		p.invalmsghdlrf(w, r, "Invalid HTTP Method: %v %s", r.Method, r.URL.Path)
	}
}

// POST /v1/etl/init
//
// initETL creates new ETL following steps:
//  1. Validate user provided pod specification.
//  2. Generate UUID of an ETL.
//  3. Broadcast initETL message to all targets.
//  4. If any of targets failed starting an ETL, stop the ETL on all targets.
//  5. Return UUID to the user if all above steps succeeded.
func (p *proxyrunner) initETL(w http.ResponseWriter, r *http.Request) {
	_, err := p.checkRESTItems(w, r, 0, false, cmn.Version, cmn.ETL, cmn.ETLInit)
	if err != nil {
		return
	}

	spec, err := ioutil.ReadAll(r.Body)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	r.Body.Close()

	msg, err := etl.ValidateSpec(spec)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	msg.ID = cmn.GenUUID()

	results := p.bcastToGroup(bcastArgs{
		req:     cmn.ReqArgs{Method: http.MethodPost, Path: r.URL.Path, Body: cmn.MustMarshal(msg)},
		timeout: cmn.LongTimeout,
	})
	for res := range results {
		if res.err != nil {
			err = res.err
			glog.Error(err)
		}
	}
	if err == nil {
		// All init calls have succeeded, return UUID.
		w.Write([]byte(msg.ID))
		return
	}

	// At least one init call has failed. Terminate all started ETL pods.
	// Discard the stop results. Calls may succeed (for targets that successfully started a pod),
	// or fail (for targets that didn't start successfully a pod).
	p.bcastToGroup(bcastArgs{
		req: cmn.ReqArgs{
			Method: http.MethodDelete,
			Path:   cmn.JoinWords(cmn.Version, cmn.ETL, cmn.ETLStop, msg.ID),
		},
		timeout: cmn.LongTimeout,
	})
	p.invalmsghdlr(w, r, err.Error())
}

// POST /v1/etl/build
func (p *proxyrunner) buildETL(w http.ResponseWriter, r *http.Request) {
	_, err := p.checkRESTItems(w, r, 0, false, cmn.Version, cmn.ETL, cmn.ETLBuild)
	if err != nil {
		return
	}

	var msg etl.BuildMsg
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}

	msg.ID = cmn.GenUUID()
	if err := msg.Validate(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	results := p.bcastToGroup(bcastArgs{
		req:     cmn.ReqArgs{Method: http.MethodPost, Path: r.URL.Path, Body: cmn.MustMarshal(msg)},
		timeout: cmn.LongTimeout,
	})
	for res := range results {
		if res.err != nil {
			err = res.err
			glog.Error(err)
		}
	}
	if err == nil {
		// All init calls have succeeded, return UUID.
		w.Write([]byte(msg.ID))
		return
	}

	// At least one init call has failed. Terminate all started ETL pods.
	// Discard the stop results. Calls may succeed (for targets that successfully started a pod),
	// or fail (for targets that didn't start successfully a pod).
	p.bcastToGroup(bcastArgs{
		req: cmn.ReqArgs{
			Method: http.MethodDelete,
			Path:   cmn.JoinWords(cmn.Version, cmn.ETL, cmn.ETLStop, msg.ID),
		},
		timeout: cmn.LongTimeout,
	})
	p.invalmsghdlr(w, r, err.Error())
}

// GET /v1/etl/list
func (p *proxyrunner) listETL(w http.ResponseWriter, r *http.Request) {
	if _, err := p.checkRESTItems(w, r, 0, false, cmn.Version, cmn.ETL, cmn.ETLList); err != nil {
		return
	}
	si, err := p.Sowner().Get().GetRandTarget()
	if err != nil {
		p.invalmsghdlrf(w, r, "failed to pick random target, err: %v", err)
		return
	}
	redirectURL := p.redirectURL(r, si, time.Now(), cmn.NetworkIntraData)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

// GET /v1/etl/logs/<uuid>[/<target_id>]
func (p *proxyrunner) logsETL(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 1, true, cmn.Version, cmn.ETL, cmn.ETLLogs)
	if err != nil {
		return
	}
	uuid := apiItems[0]
	if uuid == "" {
		p.invalmsghdlr(w, r, "ETL ID cannot be empty")
		return
	}
	var (
		results chan callResult
	)
	if len(apiItems) > 1 {
		// Request specific target for the logs.
		var (
			tid = apiItems[1]
			si  = p.owner.smap.get().GetTarget(tid)
		)
		if si == nil {
			p.invalmsghdlrf(w, r, "unknown target %q", tid)
			return
		}
		results = make(chan callResult, 1)
		results <- p.call(callArgs{
			req:     cmn.ReqArgs{Method: http.MethodGet, Path: cmn.JoinWords(cmn.Version, cmn.ETL, cmn.ETLLogs, uuid)},
			si:      si,
			timeout: cmn.DefaultTimeout,
			v:       &etl.PodLogsMsg{},
		})
		close(results)
	} else {
		results = p.bcastToGroup(bcastArgs{
			req:     cmn.ReqArgs{Method: http.MethodGet, Path: r.URL.Path},
			timeout: cmn.DefaultTimeout,
			fv:      func() interface{} { return &etl.PodLogsMsg{} },
		})
	}
	logs := make(etl.PodsLogsMsg, 0, len(results))
	for res := range results {
		if res.err != nil {
			p.invalmsghdlr(w, r, res.err.Error())
			return
		}
		logs = append(logs, *res.v.(*etl.PodLogsMsg))
	}
	sort.Sort(logs)
	p.writeJSON(w, r, logs, "logs-ETL")
}

// DELETE /v1/etl/stop/<uuid>
func (p *proxyrunner) stopETL(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.ETL, cmn.ETLStop)
	if err != nil {
		return
	}
	uuid := apiItems[0]
	if uuid == "" {
		p.invalmsghdlr(w, r, "ETL ID cannot be empty")
		return
	}
	results := p.bcastToGroup(bcastArgs{
		req:     cmn.ReqArgs{Method: http.MethodDelete, Path: r.URL.Path},
		timeout: cmn.LongTimeout,
	})
	for res := range results {
		if res.err != nil {
			p.invalmsghdlr(w, r, res.err.Error())
			return
		}
	}
}
