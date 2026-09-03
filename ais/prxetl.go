//go:build etl

// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */

//go:generate go run ../tools/gendocs/
package ais

import (
	"fmt"
	"net/http"
	"sort"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/xact"
)

// EtlMD mutations are proxy-only. Keep their context and helpers with the ETL
// proxy handlers so the upcoming `etl` build tag excludes the entire path.
type etlMDModifier struct {
	msg etl.InitMsg // interface

	pre   func(ctx *etlMDModifier, clone *etlMD) (err error)
	final func(ctx *etlMDModifier, clone *etlMD)

	podMap  etl.PodMap
	etlName string
	stage   etl.Stage
	wait    bool
}

func (p *proxy) preModifyEtlMD(ctx *etlMDModifier) (clone *etlMD, err error) {
	eo := p.owner.etl
	eo.Lock()
	defer eo.Unlock()
	clone = eo.get().clone()
	if err = ctx.pre(ctx, clone); err == nil {
		err = eo.putPersist(clone, nil)
	}
	return
}

func (p *proxy) modifyEtlMD(ctx *etlMDModifier) (clone *etlMD, err error) {
	if clone, err = p.preModifyEtlMD(ctx); err == nil && ctx.final != nil {
		ctx.final(ctx, clone)
	}
	return
}

// [METHOD] /v1/etl
// ETL handler router - dispatches to specific HTTP method handlers
func (p *proxy) etlHandler(w http.ResponseWriter, r *http.Request) {
	if !p.cluStartedWithRetry() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodDelete, http.MethodGet, http.MethodPost, http.MethodPut:
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodPost, http.MethodPut)
		return
	}
	// require Admin access (a no-op if AuthN is not used)
	if err := p.checkAccess(w, r, nil, apc.AceAdmin); err != nil {
		return
	}
	switch r.Method {
	case http.MethodPut:
		p.httpetlput(w, r)
	case http.MethodPost:
		p.httpetlpost(w, r)
	case http.MethodGet:
		p.httpetlget(w, r)
	case http.MethodDelete:
		p.httpetldel(w, r)
	}
}

// List ETL jobs or get information, logs, health, and metrics for specific ETL jobs
func (p *proxy) httpetlget(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.parseURL(w, r, apc.URLPathETL.L, 0, true)
	if err != nil {
		return
	}

	if p.forwardCP(w, r, nil, "get ETL") {
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

// +gen:endpoint PUT /v1/etl model=etl.ETLSpecMsg
// +gen:payload etl.ETLSpecMsg={"name": "echo-etl", "communication": "hpush://", "runtime": {"image": "aistorage/transformer_echo:latest"}}
// Create and initialize a new ETL job to transform data during transfers.
func (p *proxy) httpetlput(w http.ResponseWriter, r *http.Request) {
	if _, err := p.parseURL(w, r, apc.URLPathETL.L, 0, false); err != nil {
		return
	}
	if p.forwardCP(w, r, nil, "init ETL") {
		return
	}

	// TODO: introduce 2PC and move all these parsing/validation logics to the begin phase
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

	// TODO: introduce 2PC and move the following calls to the commit phase
	p.startETL(w, r, initMsg)

	if cmn.Rom.V(4, cos.ModETL) {
		nlog.Infoln(p.String() + ": " + initMsg.String())
	}
}

// +gen:endpoint POST /v1/etl/{etl-name}/start
// +gen:endpoint POST /v1/etl/{etl-name}/stop
// Start or stop ETL jobs by name
func (p *proxy) httpetlpost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.parseURL(w, r, apc.URLPathETL.L, 2, true)
	if err != nil {
		return
	}

	if p.forwardCP(w, r, nil, "post ETL") {
		return
	}

	etlName := apiItems[0]
	if err := k8s.ValidateEtlName(etlName); err != nil {
		p.writeErr(w, r, err)
		return
	}

	// must exist
	etlMD := p.owner.etl.get()
	etlMsg, stage := etlMD.get(etlName)
	if etlMsg == nil {
		p.writeErr(w, r, cos.NewErrNotFound(p, "etl job "+etlName))
		return
	}

	switch op := apiItems[1]; op {
	case apc.ETLStop:
		p.stopETL(w, r, etlMsg)
	case apc.ETLStart:
		if stage != etl.Aborted {
			p.writeErrAct(w, r, "can't start "+etlMsg.Cname()+" during "+stage.String()+" stage")
			return
		}
		p.startETL(w, r, etlMsg)
	default:
		debug.Func(func() { debug.Assert(false, "invalid operation: "+op) })
		p.writeErrAct(w, r, "invalid operation: "+op)
	}
}

// +gen:endpoint DELETE /v1/etl/{etl-name}
// Delete and remove an ETL job by name
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

	// must exist
	etlMD := p.owner.etl.get()
	etlMsg, _ := etlMD.get(etlName)
	if etlMsg == nil {
		p.writeErr(w, r, cos.NewErrNotFound(p, "etl job "+etlName))
		return
	}

	// 1. broadcast stop to all targets
	p.stopETL(w, r, etlMsg)

	// 2. if successfully stopped, remove from etlMD
	ctx := &etlMDModifier{
		pre:     p._deleteETLPre,
		final:   p._syncEtlMDFinal,
		etlName: etlName,
		wait:    true,
	}
	if _, err := p.modifyEtlMD(ctx); err != nil {
		p.writeErr(w, r, err)
	}
}

func (p *proxy) _deleteETLPre(ctx *etlMDModifier, clone *etlMD) (err error) {
	debug.Func(func() { debug.AssertNoErr(k8s.ValidateEtlName(ctx.etlName)) })
	if exists := clone.del(ctx.etlName); !exists {
		err = cos.NewErrNotFound(p, "etl job "+ctx.etlName)
	}
	return
}

func (p *proxy) startETL(w http.ResponseWriter, r *http.Request, msg etl.InitMsg) {
	// 1. update etlMD to initializing stage
	ctx := &etlMDModifier{
		pre:   _addETLPre,
		final: p._syncEtlMDFinal,
		msg:   msg,
		stage: etl.Initializing,
		wait:  true,
	}
	if _, err := p.modifyEtlMD(ctx); err != nil {
		p.writeErr(w, r, err)
	}

	// 2. start 2PC - initialize across all targets
	var (
		xid    = xact.PrefixEtlID + cos.GenUUID()
		secret = cos.CryptoRandS(10)
	)
	rxid, podMap, err := p.etlInitTxn(msg, xid, secret)
	if err != nil { // if transaction fails, put etlMD to Aborted stage
		ctx.stage = etl.Aborted
		if _, errMD := p.modifyEtlMD(ctx); errMD != nil {
			nlog.Errorf("failed to update etlMD for %s after init failure: %v", msg.Name(), errMD)
		}
		p.writeErr(w, r, err)
		return
	}

	// 3. update etlMD to Running stage
	ctx.stage = etl.Running
	ctx.podMap = podMap
	if _, err := p.modifyEtlMD(ctx); err != nil {
		p.writeErr(w, r, err)
	}

	// 4. init calls succeeded - return running xaction ID
	writeXid(w, rxid)
}

func _addETLPre(ctx *etlMDModifier, clone *etlMD) error {
	return clone.add(ctx.msg, ctx.stage, ctx.podMap)
}

func (p *proxy) _syncEtlMDFinal(ctx *etlMDModifier, clone *etlMD) {
	wg := p.metasyncer.sync(revsPair{clone, p.newAmsgStr("etl-reg", nil)})
	if ctx.wait {
		wg.Wait()
	}
}

// +gen:endpoint GET /v1/etl/{etl-name}
// Get detailed information about a specific ETL job
func (p *proxy) infoETL(w http.ResponseWriter, r *http.Request, etlName string) {
	if err := k8s.ValidateEtlName(etlName); err != nil {
		p.writeErr(w, r, err)
		return
	}

	// get init message
	etlMD := p.owner.etl.get()
	initMsg, _ := etlMD.get(etlName)
	if initMsg == nil {
		p.writeErr(w, r, cos.NewErrNotFound(p, "etl job "+etlName))
		return
	}

	// get details (contain errors)
	args := allocBcArgs()
	args.req = cmn.HreqArgs{
		Method: http.MethodGet,
		Path:   apc.URLPathETL.Join(etlName, apc.ETLDetails),
		Query:  r.URL.Query(),
	}
	args.timeout = apc.DefaultTimeout
	args.cresv = cresjGeneric[etl.ObjErrs]{}
	results := p.bcastGroup(args)
	freeBcArgs(args)
	errs := make([]etl.ObjErr, 0, len(results))
	for _, res := range results {
		if res.err != nil {
			p.writeErr(w, r, res.toErr(), res.status)
			freeBcastRes(results)
			return
		}
		errs = append(errs, *res.v.(*etl.ObjErrs)...)
	}
	freeBcastRes(results)
	p.writeJSON(w, r, etl.Details{InitMsg: initMsg, ObjErrs: errs}, "etl-details")
}

// +gen:endpoint GET /v1/etl
// List all ETL jobs in the cluster
func (p *proxy) listETL(w http.ResponseWriter, r *http.Request) {
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodGet, Path: apc.URLPathETL.S}
	args.timeout = apc.DefaultTimeout
	args.cresv = cresjGeneric[etl.InfoList]{}

	etlMD := p.owner.etl.get()
	etls := make(map[string]*etl.Info, len(etlMD.ETLs))

	results := p.bcastGroup(args)
	freeBcArgs(args)
	defer freeBcastRes(results)

	// verify all targets return the same InfoList
	for _, res := range results {
		if res.err != nil {
			p.writeErr(w, r, res.toErr())
			return
		}

		infoList, ok := res.v.(*etl.InfoList)
		if !ok {
			p.writeErrMsg(w, r, "invalid response type from target", http.StatusInternalServerError)
			break
		}

		for _, another := range *infoList {
			etls[another.Name] = &another
			// ETLs present in `infoList` but not in `etlMD`: considered unknown (proxy notification abort might not be processed yet)
			if _, inMD := etlMD.ETLs[another.Name]; !inMD {
				nlog.Errorf("unexpected etl instance %q returned from targets (not tracked by etlMD)\n", another.Name)
				etls[another.Name].Stage = etl.Unknown.String()
			}
		}
	}

	for _, en := range etlMD.ETLs {
		if _, ok := etls[en.InitMsg.Name()]; ok {
			etls[en.InitMsg.Name()].Stage = en.Stage.String()
			continue
		}

		etls[en.InitMsg.Name()] = &etl.Info{
			Name:  en.InitMsg.Name(),
			Stage: en.Stage.String(),
		}
	}

	list := etl.InfoList{}
	for i := range etls {
		list.Append(*etls[i])
	}
	p.writeJSON(w, r, list, "list-etl")
}

// +gen:endpoint GET /v1/etl/{etl-name}/logs
// +gen:endpoint GET /v1/etl/{etl-name}/logs/{target-id}
// Get logs from ETL job execution
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

// +gen:endpoint GET /v1/etl/{etl-name}/health
// Get health status of ETL job
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

// +gen:endpoint GET /v1/etl/{etl-name}/metrics
// Get CPU and memory metrics for ETL job
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
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: http.MethodDelete, Path: apc.URLPathETL.Join(msg.Name())}
	args.timeout = apc.LongTimeout
	results := p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		// 404 from target implies it's already stopped
		if res.err == nil || cos.IsNotExist(res.err, res.status) {
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
		stage: etl.Aborted,
		wait:  true,
	}
	if _, err := p.modifyEtlMD(ctx); err != nil {
		p.writeErr(w, r, err)
	}
}

func (p *proxy) etlExists(etlName string) error {
	if !k8s.IsK8s() {
		return k8s.ErrK8sRequired
	}
	if err := k8s.ValidateEtlName(etlName); err != nil {
		return err
	}
	etlMD := p.owner.etl.get()
	if _, ok := etlMD.ETLs[etlName]; !ok {
		return fmt.Errorf("ETL %s doesn't exist", etlName)
	}
	return nil
}

///////////////////
// _etlFinalizer //
//////////////////

type _etlFinalizer struct {
	p   *proxy
	msg etl.InitMsg
}

// when target shuts down (graceful or not) => rebalance triggered => globally abort apc.ActETLInline xaction => finalizer triggered through proxy notification => cleanup remaining ETL resources
func (ef *_etlFinalizer) cb(nl nl.Listener) {
	nlog.Errorf("ETL finalizer triggered: %s, %v", ef.msg.Cname(), nl.Err())
	etlMD := ef.p.owner.etl.get()
	entry, ok := etlMD.ETLs[ef.msg.Name()]
	if !ok {
		return
	}

	if err := nl.Err(); err != nil {
		// TODO: record nl.Err() and show on listETL call
		for _, pod := range entry.PodMap {
			nlog.Warningf("%s finalizer triggered with error: %v, removing pod/svc: %s/%s", ef.msg.Cname(), nl.Err(), pod.PodName, pod.SvcName)
			etl.CleanupEntities(nil, pod.PodName, pod.SvcName)
		}
	}

	ctx := &etlMDModifier{
		pre:   _addETLPre,
		final: ef.p._syncEtlMDFinal,
		msg:   ef.msg,
		stage: etl.Aborted,
		wait:  true,
	}
	_, err := ef.p.modifyEtlMD(ctx)
	if err != nil {
		nlog.Errorf("failed to update etlMD for %s: %v", ef.msg.Name(), err)
	}
}

//
// ETL
// initialize ETL pods on the nodes and connect them with of all participant targets
// etlMD and stages won't be updated in this call (caller's responsibility)
//

func (p *proxy) etlInitTxn(initMsg etl.InitMsg, xid, secret string) (string, etl.PodMap, error) {
	// 1. initialize transaction client
	c := &txnCln{p: p}
	actMsg := &apc.ActMsg{Action: apc.ActETLInline, Value: initMsg, Name: secret}
	c.init(actMsg, nil /*bucket*/, xid, false /*waitmsync*/)

	// 2. begin - broadcast initMsg, xid, secret to targets and wait for all pods to be ready
	// Pre-stamp QparamNotifyMe before begin so the Begin2PC request carries it to every target.
	// Targets read this param in addNotif() to register their xact notifier — required for
	// runtime pod failure propagation. The IC listener itself is registered only in step 3.
	c.req.Query.Set(apc.QparamNotifyMe, equalIC)
	podMap, err := etlTxnBegin(c, initMsg)
	if err != nil {
		c.bcastAbort(initMsg, err)
		return "", nil, err
	}

	// 3. IC
	smap := p.owner.smap.get()
	nl := xact.NewXactNL(c.uuid, apc.ActETLInline, &smap.Smap, nil)
	ef := &_etlFinalizer{p, initMsg} // TODO: add pod watcher to etlFinilazer
	nl.F = ef.cb

	nl.SetOwner(equalIC)
	p.ic.registerEqual(regIC{nl: nl, smap: smap, query: c.req.Query})

	// 4. commit
	rxid, _, errV := c.commit(initMsg, c.cmtTout(false))
	debug.Assertf(rxid == xid, "committed %q vs proposed %q", rxid, xid)
	if errV != nil {
		c.bcastAbort(initMsg, errV)
		return "", nil, errV
	}

	return rxid, podMap, err
}

// begin phase customized to collect pod info from nodes
func etlTxnBegin(c *txnCln, initMsg etl.InitMsg) (podMap etl.PodMap, err error) {
	// Broadcast initMsg with init timeout + network timeout
	// (wait for initialization error propagation from target)
	initTimeout, _ := initMsg.Timeouts()
	// TODO: currently, ETL init requests are broadcasted to at most `MaxParallelism()` targets concurrently (see `htrun.bcastNodes()`)
	// therefore, targets beyond that concurrency limit will block until the previous batch of targets completes.
	// could be optimized by issuing more than `MaxParallelism()` requests at once in a single broadcast.
	results := c.bcast(apc.Begin2PC, initTimeout.D()+c.timeout.netw)
	podMap = make(etl.PodMap, len(results))
	for _, res := range results {
		podInfo := etl.PodInfo{}
		if res.err != nil {
			err = res.toErr()
			break
		}
		debug.Func(func() {
			debug.Assert(c.uuid == res.header.Get(apc.HdrXactionID), "expected xid", c.uuid, "got", res.header.Get(apc.HdrXactionID))
		})
		cos.MustMarshalFromString(res.header.Get(apc.HdrETLPodInfo), &podInfo)
		podMap[res.si.ID()] = podInfo
	}
	freeBcastRes(results)
	if err != nil {
		return nil, err
	}
	return podMap, nil
}
