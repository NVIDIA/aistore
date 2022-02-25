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
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/etl"
)

// [METHOD] /v1/etl
func (p *proxy) etlHandler(w http.ResponseWriter, r *http.Request) {
	if !p.ClusterStartedWithRetry() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	switch {
	case r.Method == http.MethodPut:
		p.handleETLPut(w, r)
	case r.Method == http.MethodPost:
		p.handleETLPost(w, r)
	case r.Method == http.MethodGet:
		p.handleETLGet(w, r)
	case r.Method == http.MethodDelete:
		p.handleETLDelete(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodPost)
	}
}

// GET /v1/etl
func (p *proxy) handleETLGet(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 0, true, apc.URLPathETL.L)
	if err != nil {
		return
	}

	if len(apiItems) == 0 {
		p.listETL(w, r)
		return
	}

	// /v1/etl/<uuid>
	if len(apiItems) == 1 {
		p.infoETL(w, r, apiItems[0])
		return
	}

	// /v1/etl/<uuid>/logs[/<target-id>] or /v1/etl/<uuid>/health
	switch apiItems[1] {
	case apc.ETLLogs:
		p.logsETL(w, r, apiItems[0], apiItems[2:]...)
	case apc.ETLHealth:
		p.healthETL(w, r)
	default:
		p.writeErrURL(w, r)
	}
}

// PUT /v1/etl
//
// handleETLPut is responsible validation and adding new ETL spec/code
// to etl metadata.
func (p *proxy) handleETLPut(w http.ResponseWriter, r *http.Request) {
	_, err := p.checkRESTItems(w, r, 0, false, apc.URLPathETL.L)
	if err != nil {
		return
	}

	if p.forwardCP(w, r, nil, "init ETL") {
		return
	}

	b, err := io.ReadAll(r.Body)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	r.Body.Close()

	initMsg, err := etl.UnmarshalInitMsg(b)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}

	err = initMsg.Validate()
	if err != nil {
		p.writeErr(w, r, err)
		return
	}

	etlMD := p.owner.etl.get()
	if etlMD.get(initMsg.ID()) != nil {
		p.writeErrf(w, r, "ETL with ID %q exists", initMsg.ID())
		return
	}

	// creates a new ETL (instance) as follows:
	//  1. Validate user-provided code/pod specification.
	//  2. Broadcast init ETL message to all targets.
	//  3. If any target fails to start ETL stop it on all (targets).
	//  4. In the event of success return ETL's UUID to the user.
	if err = p.startETL(w, initMsg, true /*add to etlMD*/); err != nil {
		p.writeErr(w, r, err)
	}
}

// POST /v1/etl/<uuid>/stop (or) /v1/etl/<uuid>/start
//
// handleETLPost handles start/stop ETL pods
func (p *proxy) handleETLPost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 2, true, apc.URLPathETL.L)
	if err != nil {
		return
	}
	etlID := apiItems[0]
	if err := cos.ValidateEtlID(etlID); err != nil {
		p.writeErr(w, r, err)
		return
	}
	etlMD := p.owner.etl.get()
	etlMsg := etlMD.get(etlID)
	if etlMsg == nil {
		p.writeErr(w, r, cmn.NewErrNotFound("%s: etl UUID %s", p.si, etlID))
		return
	}
	if apiItems[1] == apc.ETLStop {
		p.stopETL(w, r)
		return
	}
	if apiItems[1] == apc.ETLStart {
		p.startETL(w, etlMsg, false /*add to etlMD*/)
		return
	}
	p.writeErrURL(w, r)
}

// DELETE /v1/etl/<uuid>
func (p *proxy) handleETLDelete(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 1, true, apc.URLPathETL.L)
	if err != nil {
		return
	}

	if p.forwardCP(w, r, nil, "delete ETL") {
		return
	}

	etlID := apiItems[0]
	if err := cos.ValidateEtlID(etlID); err != nil {
		p.writeErr(w, r, err)
		return
	}
	ctx := &etlMDModifier{
		pre:   p._deleteETLPre,
		final: p._syncEtlMDFinal,
		etlID: etlID,
	}
	_, err = p.owner.etl.modify(ctx)
	if err != nil {
		p.writeErr(w, r, err)
	}
}

func (p *proxy) _deleteETLPre(ctx *etlMDModifier, clone *etlMD) (err error) {
	debug.AssertNoErr(cos.ValidateEtlID(ctx.etlID))
	if exists := clone.delete(ctx.etlID); !exists {
		err = cmn.NewErrNotFound("%s: etl UUID %s", p.si, ctx.etlID)
	}
	return
}

// startETL broadcasts a init or start ETL request
// `addToMD` is set `true` for init requests to add a new ETL to etlMD
// `addToMD` is `false` for start requests, where ETL already exists in `etlMD`
func (p *proxy) startETL(w http.ResponseWriter, msg etl.InitMsg, addToMD bool) (err error) {
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodPut, Path: apc.URLPathETL.S, Body: cos.MustMarshal(msg)}
	args.timeout = apc.LongTimeout
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
	if err != nil {
		// At least one `build` call has failed. Terminate all `build`s.
		// (Termination calls may succeed for the targets that already succeeded in starting ETL,
		//  or fail otherwise - ignore the failures).
		argsTerm := allocBcArgs()
		argsTerm.req = cmn.HreqArgs{Method: http.MethodPost, Path: apc.URLPathETL.Join(msg.ID(), apc.ETLStop)}
		argsTerm.timeout = apc.LongTimeout
		p.bcastGroup(argsTerm)
		freeBcArgs(argsTerm)
		return err
	}

	if addToMD {
		ctx := &etlMDModifier{
			pre:   _addETLPre,
			final: p._syncEtlMDFinal,
			msg:   msg,
			wait:  true,
		}
		p.owner.etl.modify(ctx)
	}
	// All init calls have succeeded, return UUID.
	w.Write([]byte(msg.ID()))
	return
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

// GET /v1/etl/<uuid>
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

// GET /v1/etl
func (p *proxy) listETL(w http.ResponseWriter, r *http.Request) {
	var (
		args = allocBcArgs()
		etls *etl.InfoList
	)
	args.req = cmn.HreqArgs{Method: http.MethodGet, Path: apc.URLPathETL.S}
	args.timeout = apc.DefaultTimeout
	args.fv = func() interface{} { return &etl.InfoList{} }
	results := p.bcastGroup(args)
	freeBcArgs(args)

	for _, res := range results {
		if res.err != nil {
			p.writeErr(w, r, res.toErr())
			freeBcastRes(results)
			return
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
	p.writeJSON(w, r, *etls, "list-etl")
}

// GET /v1/etl/<uuid>/logs[/<target_id>]
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
			cargs.req = cmn.HreqArgs{Method: http.MethodGet, Path: apc.URLPathETL.Join(etlID, apc.ETLLogs)}
			cargs.si = si
			cargs.timeout = apc.DefaultTimeout
			cargs.v = &etl.PodLogsMsg{}
		}
		results[0] = p.call(cargs)
		freeCargs(cargs)
	} else {
		// all targets
		args = allocBcArgs()
		args.req = cmn.HreqArgs{Method: http.MethodGet, Path: r.URL.Path}
		args.timeout = apc.DefaultTimeout
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

// GET /v1/etl/<uuid>/health
func (p *proxy) healthETL(w http.ResponseWriter, r *http.Request) {
	var (
		results sliceResults
		args    *bcastArgs
	)

	args = allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodGet, Path: r.URL.Path}
	args.timeout = apc.DefaultTimeout
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

// POST /v1/etl/<uuid>/stop
func (p *proxy) stopETL(w http.ResponseWriter, r *http.Request) {
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodPost, Path: r.URL.Path}
	args.timeout = apc.LongTimeout
	results := p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		p.writeErr(w, r, res.toErr())
		break
	}
	freeBcastRes(results)
}
