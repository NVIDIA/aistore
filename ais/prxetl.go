// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"io"
	"net/http"
	"net/url"
	"reflect"
	"sort"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/ext/etl"
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
	apiItems, err := p.apiItems(w, r, 0, true, apc.URLPathETL.L)
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

	switch apiItems[1] {
	case apc.ETLLogs:
		// /v1/etl/<uuid>/logs[/<target-id>]
		p.logsETL(w, r, apiItems[0], apiItems[2:]...)
	case apc.ETLHealth:
		// /v1/etl/<uuid>/health
		p.healthETL(w, r)
	default:
		p.writeErrURL(w, r)
	}
}

// PUT /v1/etl
// Validate and start a new ETL instance:
//  1. validate user-provided code/pod specification.
//  2. broadcast `etl.InitMsg` to all targets.
//  3. if any target fails to start ETL stop it on all (targets).
//  4. otherwise:
//     - add the new ETL instance (represented by the user-specified `etl.InitMsg`) to cluster MD
//     - return ETL UUID to the user.
func (p *proxy) handleETLPut(w http.ResponseWriter, r *http.Request) {
	if _, err := p.apiItems(w, r, 0, false, apc.URLPathETL.L); err != nil {
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
	if err := initMsg.Validate(); err != nil {
		p.writeErr(w, r, err)
		return
	}

	// must be new
	etlMD := p.owner.etl.get()
	if etlMD.get(initMsg.Name()) != nil {
		p.writeErrf(w, r, "%s: etl[%s] already exists", p, initMsg.Name())
		return
	}

	// add to cluster MD and start running
	if err := p.startETL(w, initMsg, true /*add to etlMD*/); err != nil {
		p.writeErr(w, r, err)
	}
}

// POST /v1/etl/<uuid>/stop (or) /v1/etl/<uuid>/start
//
// start/stop ETL pods
func (p *proxy) handleETLPost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.apiItems(w, r, 2, true, apc.URLPathETL.L)
	if err != nil {
		return
	}
	etlName := apiItems[0]
	if err := k8s.ValidateEtlName(etlName); err != nil {
		p.writeErr(w, r, err)
		return
	}
	etlMD := p.owner.etl.get()
	etlMsg := etlMD.get(etlName)
	if etlMsg == nil {
		p.writeErr(w, r, cmn.NewErrNotFound("%s: etl[%s]", p, etlName))
		return
	}

	switch op := apiItems[1]; op {
	case apc.ETLStop:
		p.stopETL(w, r)
	case apc.ETLStart:
		p.startETL(w, etlMsg, false /*add to etlMD*/)
	default:
		debug.Assert(false, "invalid operation: "+op)
		p.writeErrURL(w, r)
	}
}

// DELETE /v1/etl/<uuid>
func (p *proxy) handleETLDelete(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.apiItems(w, r, 1, true, apc.URLPathETL.L)
	if err != nil {
		return
	}

	if p.forwardCP(w, r, nil, "delete ETL") {
		return
	}

	etlName := apiItems[0]
	if err := k8s.ValidateEtlName(etlName); err != nil {
		p.writeErr(w, r, err)
		return
	}
	ctx := &etlMDModifier{
		pre:     p._deleteETLPre,
		final:   p._syncEtlMDFinal,
		etlName: etlName,
	}
	if _, err := p.owner.etl.modify(ctx); err != nil {
		p.writeErr(w, r, err)
	}
}

func (p *proxy) _deleteETLPre(ctx *etlMDModifier, clone *etlMD) (err error) {
	debug.AssertNoErr(k8s.ValidateEtlName(ctx.etlName))
	if exists := clone.del(ctx.etlName); !exists {
		err = cmn.NewErrNotFound("%s: etl[%s]", p, ctx.etlName)
	}
	return
}

// broadcast start-ETL request to all targets
func (p *proxy) startETL(w http.ResponseWriter, msg etl.InitMsg, addToMD bool) error {
	var (
		err    error
		args   = allocBcArgs()
		xactID = etl.PrefixXactID + cos.GenUUID()
	)
	{
		args.req = cmn.HreqArgs{
			Method: http.MethodPut,
			Path:   apc.URLPathETL.S,
			Body:   cos.MustMarshal(msg),
			Query:  url.Values{apc.QparamUUID: []string{xactID}},
		}
		args.timeout = apc.LongTimeout
	}
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
		// At least one target failed. Terminate all.
		// (Termination calls may succeed for the targets that already succeeded in starting ETL,
		//  or fail otherwise - ignore the failures).
		argsTerm := allocBcArgs()
		argsTerm.req = cmn.HreqArgs{Method: http.MethodPost, Path: apc.URLPathETL.Join(msg.Name(), apc.ETLStop)}
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
	// All init calls succeeded - return user-specified ETL name.
	w.Write([]byte(msg.Name()))
	return nil
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
func (p *proxy) infoETL(w http.ResponseWriter, r *http.Request, etlName string) {
	if err := k8s.ValidateEtlName(etlName); err != nil {
		p.writeErr(w, r, err)
		return
	}

	etlMD := p.owner.etl.get()
	initMsg := etlMD.get(etlName)
	if initMsg == nil {
		p.writeErr(w, r, cmn.NewErrNotFound("%s: etl[%s]", p, etlName))
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
	args.cresv = cresEI{} // -> etl.InfoList
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
func (p *proxy) logsETL(w http.ResponseWriter, r *http.Request, etlName string, apiItems ...string) {
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
			cargs.req = cmn.HreqArgs{Method: http.MethodGet, Path: apc.URLPathETL.Join(etlName, apc.ETLLogs)}
			cargs.si = si
			cargs.timeout = apc.DefaultTimeout
			cargs.cresv = cresEL{} // -> etl.PodLogsMsg
		}
		results[0] = p.call(cargs)
		freeCargs(cargs)
	} else {
		// all targets
		args = allocBcArgs()
		args.req = cmn.HreqArgs{Method: http.MethodGet, Path: r.URL.Path}
		args.timeout = apc.DefaultTimeout
		args.cresv = cresEL{} // -> etl.PodLogsMsg
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
	p.writeJSON(w, r, logs, "logs-etl")
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
	args.cresv = cresEH{} // -> etl.PodHealthMsg
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
	p.writeJSON(w, r, healths, "health-etl")
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
