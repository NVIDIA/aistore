// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"sort"
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/ext/etl"
)

// TODO: support start/stop/list using `xid`

// [METHOD] /v1/etl
func (p *proxy) etlHandler(w http.ResponseWriter, r *http.Request) {
	if !p.cluStartedWithRetry() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	switch {
	case r.Method == http.MethodPut:
		// require Admin access (a no-op if AuthN is not used, here and elsewhere)
		if err := p.checkAccess(w, r, nil, apc.AceAdmin); err != nil {
			return
		}
		p.httpetlput(w, r)
	case r.Method == http.MethodPost:
		p.httpetlpost(w, r)
	case r.Method == http.MethodGet:
		p.httpetlget(w, r)
	case r.Method == http.MethodDelete:
		// ditto
		if err := p.checkAccess(w, r, nil, apc.AceAdmin); err != nil {
			return
		}
		p.httpetldel(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodPost)
	}
}

// GET /v1/etl
func (p *proxy) httpetlget(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.parseURL(w, r, apc.URLPathETL.L, 0, true)
	if err != nil {
		return
	}

	if len(apiItems) == 0 {
		p.listETL(w, r)
		return
	}

	// /v1/etl/<etl-name>
	if len(apiItems) == 1 {
		p.infoETL(w, r, apiItems[0])
		return
	}

	switch apiItems[1] {
	case apc.ETLLogs:
		// /v1/etl/<etl-name>/logs[/<target-id>]
		p.logsETL(w, r, apiItems[0], apiItems[2:]...)
	case apc.ETLHealth:
		// /v1/etl/<etl-name>/health
		p.healthETL(w, r)
	case apc.ETLMetrics:
		// /v1/etl/<etl-name>/metrics
		p.metricsETL(w, r)
	default:
		p.writeErrURL(w, r)
	}
}

// PUT /v1/etl
// Validate and start a new ETL instance:
//   - validate user-provided code/pod specification.
//   - broadcast `etl.InitMsg` to all targets.
//   - (as usual) if any target fails to start ETL stop it on all (targets).
//     otherwise:
//   - add the new ETL instance (represented by the user-specified `etl.InitMsg`) to cluster MD
//   - return ETL UUID to the user.
func (p *proxy) httpetlput(w http.ResponseWriter, r *http.Request) {
	if _, err := p.parseURL(w, r, apc.URLPathETL.L, 0, false); err != nil {
		return
	}
	if p.forwardCP(w, r, nil, "init ETL") {
		return
	}

	b, err := cos.ReadAll(r.Body)
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
	if msg, _ := etlMD.get(initMsg.Name()); msg != nil {
		p.writeErrStatusf(w, r, http.StatusConflict, "%s: etl job %s already exists", p, initMsg.Name())
		return
	}

	// start initialization and add to cluster MD
	if err := p.initETL(w, r, initMsg); err != nil {
		p.writeErr(w, r, err)
		return
	}
	if cmn.Rom.FastV(4, cos.SmoduleETL) {
		nlog.Infoln(p.String() + ": " + initMsg.String())
	}
}

// POST /v1/etl/<etl-name>/stop (or) /v1/etl/<etl-name>/start
// start/stop ETL pods
func (p *proxy) httpetlpost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.parseURL(w, r, apc.URLPathETL.L, 2, true)
	if err != nil {
		return
	}
	etlName := apiItems[0]
	if err := k8s.ValidateEtlName(etlName); err != nil {
		p.writeErr(w, r, err)
		return
	}
	etlMD := p.owner.etl.get()
	etlMsg, xid := etlMD.get(etlName)
	if etlMsg == nil {
		p.writeErr(w, r, cos.NewErrNotFound(p, "etl job "+etlName))
		return
	}

	switch op := apiItems[1]; op {
	case apc.ETLStop:
		p.stopETL(w, r, etlMsg)
	case apc.ETLStart:
		p.startETL(w, r, etlMsg, xid)
	default:
		debug.Assert(false, "invalid operation: "+op)
		p.writeErrAct(w, r, "invalid operation: "+op)
	}
}

// DELETE /v1/etl/<etl-name>
func (p *proxy) httpetldel(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.parseURL(w, r, apc.URLPathETL.L, 1, true)
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

	// 1. broadcast stop to all targets
	argsTerm := allocBcArgs()
	argsTerm.req = cmn.HreqArgs{Method: http.MethodPost, Path: apc.URLPathETL.Join(etlName, apc.ETLStop)}
	argsTerm.timeout = apc.LongTimeout
	results := p.bcastGroup(argsTerm)
	defer freeBcArgs(argsTerm)

	for _, res := range results {
		// ignore not found error, as the ETL might be manually stopped before
		if res.err == nil || res.status == http.StatusNotFound {
			continue
		}
		p.writeErr(w, r, res.toErr(), res.status)
		return
	}

	// 2. if successfully stopped, remove from etlMD
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
		err = cos.NewErrNotFound(p, "etl job "+ctx.etlName)
	}
	return
}

// broadcast (init ETL) request to all targets
func (p *proxy) initETL(w http.ResponseWriter, r *http.Request, msg etl.InitMsg) error {
	var (
		err  error
		args = allocBcArgs()
		xid  = etl.PrefixXactID + cos.GenUUID()
	)

	// 1. update stage to "Initializing"
	ctx := &etlMDModifier{
		pre:   _addETLPre,
		final: p._syncEtlMDFinal,
		msg:   msg,
		stage: etl.Initializing,
		xid:   xid,
		wait:  true,
	}
	p.owner.etl.modify(ctx)

	// 2. broadcast the init request
	{
		args.req = cmn.HreqArgs{
			Method: http.MethodPut,
			Path:   apc.URLPathETL.S,
			Body:   cos.MustMarshal(msg),
			Query:  url.Values{apc.QparamUUID: []string{xid}},
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
		nlog.Errorln(err)
	}
	freeBcastRes(results)

	if err != nil {
		// At least one target failed. Terminate all.
		// (Termination calls may succeed for the targets that already succeeded in starting ETL,
		//  or fail otherwise - ignore the failures).
		p.stopETL(w, r, msg) // will handle etlMD stage update
		nlog.Errorln(err)
		return err
	}

	// 3. update stage to "Running"
	ctx.stage = etl.Running
	p.owner.etl.modify(ctx)

	// 4. init calls succeeded - return running xaction
	w.Header().Set(cos.HdrContentLength, strconv.Itoa(len(xid)))
	w.Write(cos.UnsafeB(xid))
	return nil
}

func (p *proxy) startETL(w http.ResponseWriter, r *http.Request, msg etl.InitMsg, xid string) {
	var (
		err  error
		args = allocBcArgs()
	)
	{
		args.req = cmn.HreqArgs{
			Method: http.MethodPost,
			Path:   r.URL.Path,
			Body:   cos.MustMarshal(msg),
			Query:  url.Values{apc.QparamUUID: []string{xid}},
		}
		args.timeout = apc.LongTimeout
	}
	results := p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err != nil {
			p.writeErr(w, r, res.toErr(), res.status)
			err = res.toErr()
			nlog.Errorln(err)
			break
		}
	}
	freeBcastRes(results)

	if err != nil {
		// At least one target failed. Terminate all.
		p.stopETL(w, r, msg)
		return
	}

	// if succeed, resume Running stage
	ctx := &etlMDModifier{
		pre:   _addETLPre,
		final: p._syncEtlMDFinal,
		msg:   msg,
		stage: etl.Running,
		xid:   xid,
		wait:  true,
	}
	p.owner.etl.modify(ctx)
}

func _addETLPre(ctx *etlMDModifier, clone *etlMD) (_ error) {
	debug.Assert(ctx.msg != nil)
	clone.add(ctx.msg, ctx.stage, ctx.xid)
	return
}

func (p *proxy) _syncEtlMDFinal(ctx *etlMDModifier, clone *etlMD) {
	wg := p.metasyncer.sync(revsPair{clone, p.newAmsgStr("etl-reg", nil)})
	if ctx.wait {
		wg.Wait()
	}
}

// GET /v1/etl/<etl-name>
func (p *proxy) infoETL(w http.ResponseWriter, r *http.Request, etlName string) {
	if err := k8s.ValidateEtlName(etlName); err != nil {
		p.writeErr(w, r, err)
		return
	}

	etlMD := p.owner.etl.get()
	initMsg, _ := etlMD.get(etlName)
	if initMsg == nil {
		p.writeErr(w, r, cos.NewErrNotFound(p, "etl job "+etlName))
		return
	}
	p.writeJSON(w, r, initMsg, "info-etl")
}

// GET /v1/etl
func (p *proxy) listETL(w http.ResponseWriter, r *http.Request) {
	var (
		args = allocBcArgs()
		etls = &etl.InfoList{}
	)
	{
		args.req = cmn.HreqArgs{Method: http.MethodGet, Path: apc.URLPathETL.S}
		args.timeout = apc.DefaultTimeout
		args.cresv = cresjGeneric[etl.InfoList]{}
	}

	etlMD := p.owner.etl.get()
	etlMDClone := make(etl.ETLs, len(etlMD.ETLs))
	for k, v := range etlMD.ETLs {
		etlMDClone[k] = v
	}

	results := p.bcastGroup(args)
	freeBcArgs(args)

	// verify all targets return the same InfoList
	for _, res := range results {
		if res.err != nil {
			p.writeErr(w, r, res.toErr())
			freeBcastRes(results)
			return
		}

		infoList, ok := res.v.(*etl.InfoList)
		if !ok {
			p.writeErrMsg(w, r, "Invalid response type from target", http.StatusInternalServerError)
			break
		}

		sort.Sort(infoList)

		if etls.Len() != 0 && !reflect.DeepEqual(etls, infoList) {
			p.writeErrMsg(w, r, fmt.Sprintf("Targets returned different ETLs: %v vs %v", etls, infoList), http.StatusInternalServerError)
			freeBcastRes(results)
			return
		}
		etls = infoList
	}
	freeBcastRes(results)

	// NOTE: Targets should only manage and report "Running" ETL instances to avoid unnecessary resource usage.
	// All other ETL stages are tracked and maintained independently by the proxy's etlMD.
	for i, info := range *etls {
		inst, tracked := etlMDClone[info.Name]

		debug.Assertf(tracked, "Unexpected ETL instance %q returned from targets (not tracked by etlMD)", inst.Name())
		debug.Assertf(inst.Stage == etl.Running, "Non-running ETL instance %q returned by targets", inst.Name())

		// If an instance is unexpectedly reported or not running, mark it as "Unknown".
		if !tracked || inst.Stage != etl.Running || info.Stage != etl.Running.String() {
			(*etls)[i].Stage = etl.Unknown.String()
		}
		delete(etlMDClone, info.Name)
	}

	// Append non-running ETL instances from etlMD, which are managed only by the proxy.
	for _, inst := range etlMDClone {
		debug.Assertf(inst.Stage != etl.Running, "Running ETL instance %s missing from target responses", inst.Name())
		if inst.Stage == etl.Running {
			inst.Stage = etl.Unknown
		}
		etls.Append(etl.Info{Name: inst.Name(), Stage: inst.Stage.String(), XactID: inst.XactID})
	}

	p.writeJSON(w, r, *etls, "list-etl")
}

// GET /v1/etl/<etl-name>/logs[/<target_id>]
func (p *proxy) logsETL(w http.ResponseWriter, r *http.Request, etlName string, apiItems ...string) {
	var (
		results sliceResults
		args    *bcastArgs
	)
	if len(apiItems) > 0 {
		// specific target
		var (
			tid  = apiItems[0]
			smap = p.owner.smap.get()
			si   = smap.GetTarget(tid)
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
			cargs.cresv = cresjGeneric[etl.Logs]{}
		}
		results[0] = p.call(cargs, smap)
		freeCargs(cargs)
	} else {
		// all targets
		args = allocBcArgs()
		args.req = cmn.HreqArgs{Method: http.MethodGet, Path: r.URL.Path}
		args.timeout = apc.DefaultTimeout
		args.cresv = cresjGeneric[etl.Logs]{}
		results = p.bcastGroup(args)
		freeBcArgs(args)
	}
	logs := make(etl.LogsByTarget, 0, len(results))
	for _, res := range results {
		if res.err != nil {
			p.writeErr(w, r, res.toErr(), res.status)
			freeBcastRes(results)
			return
		}
		logs = append(logs, *res.v.(*etl.Logs))
	}
	freeBcastRes(results)
	p.writeJSON(w, r, logs, "logs-etl")
}

// GET /v1/etl/<etl-name>/health
func (p *proxy) healthETL(w http.ResponseWriter, r *http.Request) {
	var (
		results sliceResults
		args    *bcastArgs
	)
	args = allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodGet, Path: r.URL.Path}
	results = p.bcastGroup(args)
	defer freeBcastRes(results)
	freeBcArgs(args)

	healths := make(etl.HealthByTarget, 0, len(results))
	for _, res := range results {
		if res.err != nil {
			p.writeErr(w, r, res.toErr(), res.status)
			return
		}
		msg := etl.HealthStatus{
			TargetID: res.si.ID(),
			Status:   string(res.bytes),
		}
		healths = append(healths, &msg)
	}
	p.writeJSON(w, r, healths, "health-etl")
}

// GET /v1/etl/<etl-name>/metrics
func (p *proxy) metricsETL(w http.ResponseWriter, r *http.Request) {
	var (
		results sliceResults
		args    *bcastArgs
	)
	args = allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodGet, Path: r.URL.Path}
	args.timeout = apc.DefaultTimeout
	args.cresv = cresjGeneric[etl.CPUMemUsed]{}
	results = p.bcastGroup(args)
	defer freeBcastRes(results)
	freeBcArgs(args)

	metrics := make(etl.CPUMemByTarget, 0, len(results))
	for _, res := range results {
		if res.err != nil {
			p.writeErr(w, r, res.toErr(), res.status)
			return
		}
		metrics = append(metrics, res.v.(*etl.CPUMemUsed))
	}
	sort.SliceStable(metrics, func(i, j int) bool { return metrics[i].TargetID < metrics[j].TargetID })
	p.writeJSON(w, r, metrics, "metrics-etl")
}

// POST /v1/etl/<etl-name>/stop
func (p *proxy) stopETL(w http.ResponseWriter, r *http.Request, msg etl.InitMsg) {
	etlMD := p.owner.etl.get()
	inst, ok := etlMD.ETLs[msg.Name()]
	if !ok {
		p.writeErrStatusf(w, r, http.StatusNotFound, "%q not found", msg.Name())
		return
	}
	if inst.Stage == etl.Stopped {
		// already stopped, nothing to do
		return
	}
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodPost, Path: apc.URLPathETL.Join(msg.Name(), apc.ETLStop)}
	args.timeout = apc.LongTimeout
	results := p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		// 404 from target implies it's already stopped
		nlog.Errorln(res.err)
		if res.err == nil || cos.IsErrNotFound(res.err) {
			nlog.Errorln("Skipped")
			continue
		}
		p.writeErr(w, r, res.toErr(), res.status)
		break
	}
	freeBcastRes(results)

	ctx := &etlMDModifier{
		pre:   _addETLPre,
		final: p._syncEtlMDFinal,
		msg:   msg,
		stage: etl.Stopped,
		wait:  true,
	}
	p.owner.etl.modify(ctx)
}
