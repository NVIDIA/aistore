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
		apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.URLPathETL.L)
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
			t.invalmsghdlrf(w, r, "invalid GET path: %s", apiItems[0])
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
	if _, err := t.checkRESTItems(w, r, 0, false, cmn.URLPathETLInit.L); err != nil {
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
	if _, err := t.checkRESTItems(w, r, 0, false, cmn.URLPathETLBuild.L); err != nil {
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
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.URLPathETLStop.L)
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
		t.invalmsghdlr(w, r, err.Error())
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
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	t.writeJSON(w, r, healthMsg, "health-ETL")
}

// GET /v1/etl/objects/<secret>/<bucket-name>/<object-name>
//
// getObjectETL handles GET requests from ETL containers (K8s Pods).
// getObjectETL validates the secret that was injected into a Pod during its initialization.
func (t *targetrunner) getObjectETL(w http.ResponseWriter, r *http.Request) {
	request := &apiRequest{after: 3, prefix: cmn.URLPathETLObject.L, bckIdx: 1}
	if err := t.parseAPIRequest(w, r, request); err != nil {
		return
	}
	if err := etl.CheckSecret(request.items[0]); err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}

	t.getObject(w, r, r.URL.Query(), request.bck, request.items[2])
}

// HEAD /v1/etl/objects/<secret>/<bucket-name>/<object-name>
//
// headObjectETL handles HEAD requests from ETL containers (K8s Pods).
// headObjectETL validates the secret that was injected into a Pod during its initialization.
func (t *targetrunner) headObjectETL(w http.ResponseWriter, r *http.Request) {
	request := &apiRequest{after: 3, prefix: cmn.URLPathETLObject.L, bckIdx: 1}
	if err := t.parseAPIRequest(w, r, request); err != nil {
		return
	}
	if err := etl.CheckSecret(request.items[0]); err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}

	t.headObject(w, r, r.URL.Query(), request.bck, request.items[2])
}

////////////////
// ETL: proxy //
////////////////

// [METHOD] /v1/etl
func (p *proxyrunner) etlHandler(w http.ResponseWriter, r *http.Request) {
	if !p.ClusterStartedWithRetry() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	switch {
	case r.Method == http.MethodPost:
		apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.URLPathETL.L)
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
		apiItems, err := p.checkRESTItems(w, r, 1, true, cmn.URLPathETL.L)
		if err != nil {
			return
		}

		switch apiItems[0] {
		case cmn.ETLList:
			p.listETL(w, r)
		case cmn.ETLLogs:
			p.logsETL(w, r)
		case cmn.ETLHealth:
			p.healthETL(w, r)
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
// initETL creates a new ETL (instance) as follows:
//  1. Validate user-provided pod specification.
//  2. Generate UUID.
//  3. Broadcast initETL message to all targets.
//  4. If any target fails to start ETL stop it on all (targets).
//  5. In the event of success return ETL's UUID to the user.
func (p *proxyrunner) initETL(w http.ResponseWriter, r *http.Request) {
	_, err := p.checkRESTItems(w, r, 0, false, cmn.URLPathETLInit.L)
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

	args := allocBcastArgs()
	args.req = cmn.ReqArgs{Method: http.MethodPost, Path: r.URL.Path, Body: cmn.MustMarshal(msg)}
	args.timeout = cmn.LongTimeout
	results := p.bcastGroup(args)
	freeBcastArgs(args)
	for res := range results {
		if res.err == nil {
			freeCallRes(res)
			continue
		}
		err = res.err
		glog.Error(err)
		freeCallRes(res)
	}
	if err == nil {
		// All init calls have succeeded, return UUID.
		w.Write([]byte(msg.ID))
		return
	}

	// At least one `init` call has failed. Terminate all started ETL pods.
	// (Termination calls may succeed for the targets that already succeeded in starting ETL,
	//  or fail otherwise - ignore the failures).
	argsTerm := allocBcastArgs()
	argsTerm.req = cmn.ReqArgs{Method: http.MethodDelete, Path: cmn.URLPathETLStop.Join(msg.ID)}
	argsTerm.timeout = cmn.LongTimeout
	p.bcastGroup(argsTerm)
	freeBcastArgs(argsTerm)
	p.invalmsghdlr(w, r, err.Error())
}

// POST /v1/etl/build
func (p *proxyrunner) buildETL(w http.ResponseWriter, r *http.Request) {
	_, err := p.checkRESTItems(w, r, 0, false, cmn.URLPathETLBuild.L)
	if err != nil {
		return
	}

	var msg etl.BuildMsg
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}

	if msg.ID == "" {
		msg.ID = cmn.GenUUID()
	} else if err = cmn.ValidateID(msg.ID); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	if err := msg.Validate(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	args := allocBcastArgs()
	args.req = cmn.ReqArgs{Method: http.MethodPost, Path: r.URL.Path, Body: cmn.MustMarshal(msg)}
	args.timeout = cmn.LongTimeout
	results := p.bcastGroup(args)
	freeBcastArgs(args)
	for res := range results {
		if res.err == nil {
			freeCallRes(res)
			continue
		}
		err = res.err
		glog.Error(err)
		freeCallRes(res)
	}
	if err == nil {
		// All init calls have succeeded, return UUID.
		w.Write([]byte(msg.ID))
		return
	}

	// At least one `build` call has failed. Terminate all `build`s.
	// (Termination calls may succeed for the targets that already succeeded in starting ETL,
	//  or fail otherwise - ignore the failures).
	argsTerm := allocBcastArgs()
	argsTerm.req = cmn.ReqArgs{Method: http.MethodDelete, Path: cmn.URLPathETLStop.Join(msg.ID)}
	argsTerm.timeout = cmn.LongTimeout
	p.bcastGroup(argsTerm)
	freeBcastArgs(argsTerm)
	p.invalmsghdlr(w, r, err.Error())
}

// GET /v1/etl/list
func (p *proxyrunner) listETL(w http.ResponseWriter, r *http.Request) {
	if _, err := p.checkRESTItems(w, r, 0, false, cmn.URLPathETLList.L); err != nil {
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
	apiItems, err := p.checkRESTItems(w, r, 1, true, cmn.URLPathETLLogs.L)
	if err != nil {
		return
	}
	uuid := apiItems[0]
	if uuid == "" {
		p.invalmsghdlr(w, r, "ETL ID cannot be empty")
		return
	}
	var (
		results chanResults
		args    *bcastArgs
	)
	if len(apiItems) > 1 {
		// specific target
		var (
			tid = apiItems[1]
			si  = p.owner.smap.get().GetTarget(tid)
		)
		if si == nil {
			p.invalmsghdlrf(w, r, "unknown target %q", tid)
			return
		}
		results = make(chanResults, 1)
		results <- p.call(callArgs{
			req: cmn.ReqArgs{
				Method: http.MethodGet,
				Path:   cmn.URLPathETLLogs.Join(uuid),
			},
			si:      si,
			timeout: cmn.DefaultTimeout,
			v:       &etl.PodLogsMsg{},
		})
		close(results)
	} else {
		// all targets
		args = allocBcastArgs()
		args.req = cmn.ReqArgs{Method: http.MethodGet, Path: r.URL.Path}
		args.timeout = cmn.DefaultTimeout
		args.fv = func() interface{} { return &etl.PodLogsMsg{} }
		results = p.bcastGroup(args)
		freeBcastArgs(args)
	}
	logs := make(etl.PodsLogsMsg, 0, len(results))
	for res := range results {
		if res.err != nil {
			p.invalmsghdlr(w, r, res.err.Error())
			drainCallResults(res, results)
			return
		}
		logs = append(logs, *res.v.(*etl.PodLogsMsg))
		freeCallRes(res)
	}
	sort.Sort(logs)
	p.writeJSON(w, r, logs, "logs-ETL")
}

// GET /v1/etl/health/<uuid>
func (p *proxyrunner) healthETL(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.URLPathETLHealth.L)
	if err != nil {
		return
	}
	uuid := apiItems[0]
	if uuid == "" {
		p.invalmsghdlr(w, r, "ETL ID cannot be empty")
		return
	}
	var (
		results chanResults
		args    *bcastArgs
	)

	args = allocBcastArgs()
	args.req = cmn.ReqArgs{Method: http.MethodGet, Path: r.URL.Path}
	args.timeout = cmn.DefaultTimeout
	args.fv = func() interface{} { return &etl.PodHealthMsg{} }
	results = p.bcastGroup(args)
	freeBcastArgs(args)

	healths := make(etl.PodsHealthMsg, 0, len(results))
	for res := range results {
		if res.err != nil {
			p.invalmsghdlr(w, r, res.err.Error())
			drainCallResults(res, results)
			return
		}
		healths = append(healths, res.v.(*etl.PodHealthMsg))
		freeCallRes(res)
	}
	sort.SliceStable(healths, func(i, j int) bool { return healths[i].TargetID < healths[j].TargetID })
	p.writeJSON(w, r, healths, "health-ETL")
}

// DELETE /v1/etl/stop/<uuid>
func (p *proxyrunner) stopETL(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.URLPathETLStop.L)
	if err != nil {
		return
	}
	uuid := apiItems[0]
	if uuid == "" {
		p.invalmsghdlr(w, r, "ETL ID cannot be empty")
		return
	}
	args := allocBcastArgs()
	args.req = cmn.ReqArgs{Method: http.MethodDelete, Path: r.URL.Path}
	args.timeout = cmn.LongTimeout
	results := p.bcastGroup(args)
	freeBcastArgs(args)
	for res := range results {
		if res.err == nil {
			freeCallRes(res)
			continue
		}
		p.invalmsghdlr(w, r, res.err.Error())
		drainCallResults(res, results)
		return
	}
}
