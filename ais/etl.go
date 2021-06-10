// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sort"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/etl"
)

var startETL = atomic.NewBool(false)

/////////////////
// ETL: target //
/////////////////

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
		case cmn.ETLInit:
			t.initETL(w, r)
		case cmn.ETLBuild:
			t.buildETL(w, r)
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

func (t *targetrunner) initETL(w http.ResponseWriter, r *http.Request) {
	var msg etl.InitMsg
	if _, err := t.checkRESTItems(w, r, 0, false, cmn.URLPathETLInit.L); err != nil {
		return
	}
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}
	if err := etl.Start(t, msg); err != nil {
		t.writeErr(w, r, err)
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
		t.writeErr(w, r, err)
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
	}
}

func (t *targetrunner) doETL(w http.ResponseWriter, r *http.Request, uuid string, bck *cluster.Bck, objName string) {
	var (
		comm etl.Communicator
		err  error
	)
	comm, err = etl.GetCommunicator(uuid)
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
	if err := comm.Do(w, r, bck, objName); err != nil {
		t.writeErr(w, r, cmn.NewETLError(&cmn.ETLErrorContext{
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

func (t *targetrunner) etlParseObjectReq(_ http.ResponseWriter, r *http.Request) (secret string, bck *cluster.Bck, objName string, err error) {
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
	secret, bck, objName, err := t.etlParseObjectReq(w, r)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	if err := etl.CheckSecret(secret); err != nil {
		t.writeErr(w, r, err)
		return
	}

	t.getObject(w, r, r.URL.Query(), bck, objName)
}

// HEAD /v1/etl/objects/<secret>/<uname>
//
// headObjectETL handles HEAD requests from ETL containers (K8s Pods).
// headObjectETL validates the secret that was injected into a Pod during its initialization.
func (t *targetrunner) headObjectETL(w http.ResponseWriter, r *http.Request) {
	secret, bck, objName, err := t.etlParseObjectReq(w, r)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	if err := etl.CheckSecret(secret); err != nil {
		t.writeErr(w, r, err)
		return
	}

	t.headObject(w, r, r.URL.Query(), bck, objName)
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
			p.writeErrURL(w, r)
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
			p.writeErrURL(w, r)
		}
	case r.Method == http.MethodDelete:
		p.stopETL(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodPost)
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

	spec, err := io.ReadAll(r.Body)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	r.Body.Close()

	msg, err := etl.ValidateSpec(spec)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}

	if p.forwardCP(w, r, nil, "initETL") {
		return
	}

	if err = p.startETL(w, r, cos.MustMarshal(msg), msg.ID); err != nil {
		p.writeErr(w, r, err)
	}
}

// POST /v1/etl/build
func (p *proxyrunner) buildETL(w http.ResponseWriter, r *http.Request) {
	var msg etl.BuildMsg
	_, err := p.checkRESTItems(w, r, 0, false, cmn.URLPathETLBuild.L)
	if err != nil {
		return
	}
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}
	if msg.ID == "" {
		msg.ID = cos.GenUUID()
	} else if err = cos.ValidateID(msg.ID); err != nil {
		p.writeErr(w, r, err)
		return
	}
	if err := msg.Validate(); err != nil {
		p.writeErr(w, r, err)
		return
	}
	if p.forwardCP(w, r, nil, "buildETL") {
		return
	}
	if err = p.startETL(w, r, cos.MustMarshal(msg), msg.ID); err != nil {
		p.writeErr(w, r, err)
	}
}

// startETL broadcasts a build or init ETL request and ensures only one ETL is running
func (p *proxyrunner) startETL(w http.ResponseWriter, r *http.Request, body []byte, msgID string) (err error) {
	if !startETL.CAS(false, true) {
		return cmn.ErrETLOnlyOne
	}
	defer startETL.CAS(true, false)
	if err := p.ensureNoETLs(); err != nil {
		return err
	}

	args := allocBcastArgs()
	args.req = cmn.ReqArgs{Method: http.MethodPost, Path: r.URL.Path, Body: body}
	args.timeout = cmn.LongTimeout
	results := p.bcastGroup(args)
	freeBcastArgs(args)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		err = res.error()
		glog.Error(err)
	}
	freeCallResults(results)
	if err == nil {
		// All init calls have succeeded, return UUID.
		w.Write([]byte(msgID))
		return
	}

	// At least one `build` call has failed. Terminate all `build`s.
	// (Termination calls may succeed for the targets that already succeeded in starting ETL,
	//  or fail otherwise - ignore the failures).
	argsTerm := allocBcastArgs()
	argsTerm.req = cmn.ReqArgs{Method: http.MethodDelete, Path: cmn.URLPathETLStop.Join(msgID)}
	argsTerm.timeout = cmn.LongTimeout
	p.bcastGroup(argsTerm)
	freeBcastArgs(argsTerm)
	return err
}

// ensureOneETL makes sure only no ETLs are running
func (p *proxyrunner) ensureNoETLs() error {
	etls, err := p.listETLs()
	if err != nil {
		return err
	}
	if len(etls) > 0 {
		return cmn.ErrETLOnlyOne
	}
	return nil
}

// GET /v1/etl/list
func (p *proxyrunner) listETL(w http.ResponseWriter, r *http.Request) {
	if _, err := p.checkRESTItems(w, r, 0, false, cmn.URLPathETLList.L); err != nil {
		return
	}
	etls, err := p.listETLs()
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	p.writeJSON(w, r, etls, "list-etl")
}

func (p *proxyrunner) listETLs() (infoList etl.InfoList, err error) {
	var (
		args = allocBcastArgs()
		etls *etl.InfoList
	)
	args.req = cmn.ReqArgs{Method: http.MethodGet, Path: cmn.URLPathETLList.S}
	args.timeout = cmn.DefaultTimeout
	args.fv = func() interface{} { return &etl.InfoList{} }
	results := p.bcastGroup(args)
	freeBcastArgs(args)

	for _, res := range results {
		if res.err != nil {
			err = res.error()
			freeCallResults(results)
			return nil, err
		}

		if etls == nil {
			etls = res.v.(*etl.InfoList)
			sort.Sort(etls)
		} else {
			another := res.v.(*etl.InfoList)
			sort.Sort(another)
			if !reflect.DeepEqual(etls, another) {
				// TODO: Should we return an error to a user?
				// Or stop mismatching ETLs and return internal server error?
				glog.Warningf("Targets returned different ETLs: %v vs %v", etls, another)
			}
		}
	}
	freeCallResults(results)

	if etls == nil {
		etls = &etl.InfoList{}
	}
	return *etls, err
}

// GET /v1/etl/logs/<uuid>[/<target_id>]
func (p *proxyrunner) logsETL(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 1, true, cmn.URLPathETLLogs.L)
	if err != nil {
		return
	}
	uuid := apiItems[0]
	if uuid == "" {
		p.writeErr(w, r, cmn.ErrETLMissingUUID)
		return
	}
	var (
		results sliceResults
		args    *bcastArgs
	)
	if len(apiItems) > 1 {
		// specific target
		var (
			tid = apiItems[1]
			si  = p.owner.smap.get().GetTarget(tid)
		)
		if si == nil {
			p.writeErrf(w, r, "unknown target %q", tid)
			return
		}
		results = make(sliceResults, 1)
		results[0] = p.call(callArgs{
			req: cmn.ReqArgs{
				Method: http.MethodGet,
				Path:   cmn.URLPathETLLogs.Join(uuid),
			},
			si:      si,
			timeout: cmn.DefaultTimeout,
			v:       &etl.PodLogsMsg{},
		})
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
	for _, res := range results {
		if res.err != nil {
			p.writeErr(w, r, res.error())
			freeCallResults(results)
			return
		}
		logs = append(logs, *res.v.(*etl.PodLogsMsg))
	}
	freeCallResults(results)
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
		p.writeErr(w, r, cmn.ErrETLMissingUUID)
		return
	}
	var (
		results sliceResults
		args    *bcastArgs
	)

	args = allocBcastArgs()
	args.req = cmn.ReqArgs{Method: http.MethodGet, Path: r.URL.Path}
	args.timeout = cmn.DefaultTimeout
	args.fv = func() interface{} { return &etl.PodHealthMsg{} }
	results = p.bcastGroup(args)
	defer freeCallResults(results)
	freeBcastArgs(args)

	healths := make(etl.PodsHealthMsg, 0, len(results))
	for _, res := range results {
		if res.err != nil {
			p.writeErr(w, r, res.error(), res.status)
			return
		}
		healths = append(healths, res.v.(*etl.PodHealthMsg))
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
		p.writeErr(w, r, cmn.ErrETLMissingUUID)
		return
	}
	args := allocBcastArgs()
	args.req = cmn.ReqArgs{Method: http.MethodDelete, Path: r.URL.Path}
	args.timeout = cmn.LongTimeout
	results := p.bcastGroup(args)
	freeBcastArgs(args)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		p.writeErr(w, r, res.error())
		break
	}
	freeCallResults(results)
}
