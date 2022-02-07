// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"io"
	"net/http"
	"reflect"
	"sort"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/etl"
)

// [METHOD] /v1/etls
func (p *proxy) etlHandler(w http.ResponseWriter, r *http.Request) {
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
		case cmn.ETLInitSpec:
			p.initSpecETL(w, r)
		case cmn.ETLInitCode:
			p.initCodeETL(w, r)
		default:
			p.writeErrURL(w, r)
		}
	case r.Method == http.MethodGet:
		p.handleETLGet(w, r)
	case r.Method == http.MethodDelete:
		p.stopETL(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodPost)
	}
}

// GET /v1/etls
func (p *proxy) handleETLGet(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 0, true, cmn.URLPathETL.L)
	if err != nil {
		return
	}

	if len(apiItems) == 0 {
		p.listETL(w, r)
		return
	}

	// /v1/etls/<uuid>
	if len(apiItems) == 1 {
		p.infoETL(w, r, apiItems[0])
		return
	}

	// /v1/etls/<uuid>/logs[/<target-id>] or /v1/etls/<uuid>/health
	switch apiItems[1] {
	case cmn.ETLLogs:
		p.logsETL(w, r, apiItems[0], apiItems[2:]...)
	case cmn.ETLHealth:
		p.healthETL(w, r)
	default:
		p.writeErrURL(w, r)
	}
}

// POST /v1/etls/init_spec
//
// initSpecETL creates a new ETL (instance) as follows:
//  1. Validate user-provided pod specification.
//  2. Generate UUID.
//  3. Broadcast initSpecETL message to all targets.
//  4. If any target fails to start ETL stop it on all (targets).
//  5. In the event of success return ETL's UUID to the user.
func (p *proxy) initSpecETL(w http.ResponseWriter, r *http.Request) {
	_, err := p.checkRESTItems(w, r, 0, false, cmn.URLPathETLInitSpec.L)
	if err != nil {
		return
	}

	if p.forwardCP(w, r, nil, "initSpecETL") {
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

	if err = p.startETL(w, r, &msg); err != nil {
		p.writeErr(w, r, err)
	}
}

// POST /v1/etls/init_code
func (p *proxy) initCodeETL(w http.ResponseWriter, r *http.Request) {
	_, err := p.checkRESTItems(w, r, 0, false, cmn.URLPathETLInitCode.L)
	if err != nil {
		return
	}

	if p.forwardCP(w, r, nil, "initCodeETL") {
		return
	}

	var msg etl.InitCodeMsg
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}
	// TODO: Make ID required
	if msg.IDX == "" {
		msg.IDX = cos.GenUUID()
	} else if err = cos.ValidateEtlID(msg.IDX); err != nil {
		p.writeErr(w, r, err)
		return
	}
	if err := msg.Validate(); err != nil {
		p.writeErr(w, r, err)
		return
	}
	if err = p.startETL(w, r, &msg); err != nil {
		p.writeErr(w, r, err)
	}
}

// startETL broadcasts a build or init ETL request and ensures only one ETL is running
func (p *proxy) startETL(w http.ResponseWriter, r *http.Request, msg etl.InitMsg) (err error) {
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodPost, Path: r.URL.Path, Body: cos.MustMarshal(msg)}
	args.timeout = cmn.LongTimeout
	results := p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		err = res.toErr()
		glog.Error(err)
	}
	freeBcastRes(results)
	if err == nil {
		ctx := &etlMDModifier{
			pre:   _addETLPre,
			final: p._syncEtlMDFinal,
			msg:   msg,
		}

		p.owner.etl.modify(ctx)

		// All init calls have succeeded, return UUID.
		w.Write([]byte(msg.ID()))
		return
	}

	// At least one `build` call has failed. Terminate all `build`s.
	// (Termination calls may succeed for the targets that already succeeded in starting ETL,
	//  or fail otherwise - ignore the failures).
	argsTerm := allocBcArgs()
	argsTerm.req = cmn.HreqArgs{Method: http.MethodDelete, Path: cmn.URLPathETLStop.Join(msg.ID())}
	argsTerm.timeout = cmn.LongTimeout
	p.bcastGroup(argsTerm)
	freeBcArgs(argsTerm)
	return err
}

func _addETLPre(ctx *etlMDModifier, clone *etlMD) (_ error) {
	debug.Assert(ctx.msg != nil)
	clone.add(ctx.msg)
	return
}

func (p *proxy) _syncEtlMDFinal(ctx *etlMDModifier, clone *etlMD) {
	wg := p.metasyncer.sync(revsPair{clone, p.newAmsgStr("etl-reg", nil)})
	if ctx.wait {
		wg.Wait()
	}
}

// GET /v1/etls/<uuid>
func (p *proxy) infoETL(w http.ResponseWriter, r *http.Request, etlID string) {
	if err := cos.ValidateEtlID(etlID); err != nil {
		p.writeErr(w, r, err)
		return
	}

	etlMD := p.owner.etl.get()
	initMsg := etlMD.get(etlID)
	if initMsg == nil {
		p.writeErr(w, r, cmn.NewErrNotFound("%s: etl UUID %s", p.si, etlID))
		return
	}
	p.writeJSON(w, r, initMsg, "info-etl")
}

// GET /v1/etls
func (p *proxy) listETL(w http.ResponseWriter, r *http.Request) {
	etls, err := p.listETLs()
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	p.writeJSON(w, r, etls, "list-etl")
}

func (p *proxy) listETLs() (infoList etl.InfoList, err error) {
	var (
		args = allocBcArgs()
		etls *etl.InfoList
	)
	args.req = cmn.HreqArgs{Method: http.MethodGet, Path: cmn.URLPathETL.S}
	args.timeout = cmn.DefaultTimeout
	args.fv = func() interface{} { return &etl.InfoList{} }
	results := p.bcastGroup(args)
	freeBcArgs(args)

	for _, res := range results {
		if res.err != nil {
			err = res.toErr()
			freeBcastRes(results)
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
	freeBcastRes(results)

	if etls == nil {
		etls = &etl.InfoList{}
	}
	return *etls, err
}

// GET /v1/etls/<uuid>/logs[/<target_id>]
func (p *proxy) logsETL(w http.ResponseWriter, r *http.Request, etlID string, apiItems ...string) {
	var (
		results sliceResults
		args    *bcastArgs
	)
	if len(apiItems) > 0 {
		// specific target
		var (
			tid = apiItems[0]
			si  = p.owner.smap.get().GetTarget(tid)
		)
		if si == nil {
			p.writeErrf(w, r, "unknown target %q", tid)
			return
		}
		results = make(sliceResults, 1)
		cargs := allocCargs()
		{
			cargs.req = cmn.HreqArgs{Method: http.MethodGet, Path: cmn.URLPathETL.Join(etlID, cmn.ETLLogs)}
			cargs.si = si
			cargs.timeout = cmn.DefaultTimeout
			cargs.v = &etl.PodLogsMsg{}
		}
		results[0] = p.call(cargs)
		freeCargs(cargs)
	} else {
		// all targets
		args = allocBcArgs()
		args.req = cmn.HreqArgs{Method: http.MethodGet, Path: r.URL.Path}
		args.timeout = cmn.DefaultTimeout
		args.fv = func() interface{} { return &etl.PodLogsMsg{} }
		results = p.bcastGroup(args)
		freeBcArgs(args)
	}
	logs := make(etl.PodsLogsMsg, 0, len(results))
	for _, res := range results {
		if res.err != nil {
			p.writeErr(w, r, res.toErr())
			freeBcastRes(results)
			return
		}
		logs = append(logs, *res.v.(*etl.PodLogsMsg))
	}
	freeBcastRes(results)
	sort.Sort(logs)
	p.writeJSON(w, r, logs, "logs-ETL")
}

// GET /v1/etls/<uuid>/health
func (p *proxy) healthETL(w http.ResponseWriter, r *http.Request) {
	var (
		results sliceResults
		args    *bcastArgs
	)

	args = allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodGet, Path: r.URL.Path}
	args.timeout = cmn.DefaultTimeout
	args.fv = func() interface{} { return &etl.PodHealthMsg{} }
	results = p.bcastGroup(args)
	defer freeBcastRes(results)
	freeBcArgs(args)

	healths := make(etl.PodsHealthMsg, 0, len(results))
	for _, res := range results {
		if res.err != nil {
			p.writeErr(w, r, res.toErr(), res.status)
			return
		}
		healths = append(healths, res.v.(*etl.PodHealthMsg))
	}
	sort.SliceStable(healths, func(i, j int) bool { return healths[i].TargetID < healths[j].TargetID })
	p.writeJSON(w, r, healths, "health-ETL")
}

// DELETE /v1/etls/stop/<uuid>
func (p *proxy) stopETL(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.URLPathETLStop.L)
	if err != nil {
		return
	}
	uuid := apiItems[0]
	if uuid == "" {
		p.writeErr(w, r, cmn.ErrETLMissingUUID)
		return
	}
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodDelete, Path: r.URL.Path}
	args.timeout = cmn.LongTimeout
	results := p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		p.writeErr(w, r, res.toErr())
		break
	}
	// TODO: implement using ETL modifier
	freeBcastRes(results)
}
