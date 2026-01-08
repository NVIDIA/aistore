// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
)

// [METHOD] /v1/etl
func (t *target) etlHandler(w http.ResponseWriter, r *http.Request) {
	if !k8s.IsK8s() {
		t.writeErr(w, r, k8s.ErrK8sRequired, 0, Silent)
		return
	}
	switch r.Method {
	case http.MethodPut:
		t.handleETLPut(w, r) // TODO: move to proxy (control plane operation)
	case http.MethodDelete:
		t.handleETLDelete(w, r) // TODO: move to proxy (control plane operation)
	case http.MethodGet:
		dpq := dpqAlloc()
		t.handleETLGet(w, r, dpq)
		dpqFree(dpq)
	case http.MethodHead:
		t.headObjectETL(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodGet, http.MethodHead, http.MethodPost)
	}
}

// PUT /v1/etl
// init ETL spec/code
func (t *target) handleETLPut(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.parseURL(w, r, apc.URLPathETL.L, 0, true)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	// disallow to run when above high wm (let alone OOS)
	cs := fs.Cap()
	if err := cs.Err(); err != nil {
		t.writeErr(w, r, err, http.StatusInsufficientStorage)
		return
	}

	// /v1/etl/_object/<secret>/<uname>
	if apiItems[0] == apc.ETLObject {
		t.putObjectETL(w, r)
		return
	}
}

func (t *target) handleETLGet(w http.ResponseWriter, r *http.Request, dpq *dpq) {
	apiItems, err := t.parseURL(w, r, apc.URLPathETL.L, 0, true)
	if err != nil {
		return
	}

	if err := dpq.parse(r.URL.RawQuery); err != nil {
		t.writeErr(w, r, err)
		return
	}

	// /v1/etl
	if len(apiItems) == 0 {
		t.writeJSON(w, r, etl.List(), "list-etl")
		return
	}

	// /v1/etl/_object/<secret>/<uname>
	if apiItems[0] == apc.ETLObject {
		t.getObjectETL(w, r, dpq)
		return
	}

	// /v1/etl/<etl-name>
	if len(apiItems) == 1 {
		t.writeErr(w, r, fmt.Errorf("GET(ETL[%s] info) not implemented yet", apiItems[0]), http.StatusNotImplemented)
		return
	}

	// /v1/etl/<etl-name>/logs or /v1/etl/<etl-name>/health or /v1/etl/<etl-name>/metrics
	switch apiItems[1] {
	case apc.ETLLogs:
		t.logsETL(w, r, apiItems[0])
	case apc.ETLHealth:
		t.healthETL(w, r, apiItems[0])
	case apc.ETLDetails:
		t.detailsETL(w, r, dpq, apiItems[0])
	case apc.ETLMetrics:
		k8s.InitMetricsClient()
		t.metricsETL(w, r, apiItems[0])
	default:
		t.writeErrURL(w, r)
	}
}

// DELETE /v1/etl/<etl-name>
//
// Handles deleting ETL pods
func (t *target) handleETLDelete(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.parseURL(w, r, apc.URLPathETL.L, 1, true)
	if err != nil {
		return
	}
	if err := etl.Delete(apiItems[0]); err != nil {
		t.writeErr(w, r, err)
	}
}

func (t *target) inlineETL(w http.ResponseWriter, r *http.Request, dpq *dpq, lom *core.LOM) {
	started := mono.NanoTime()
	comm, errN := etl.GetCommunicator(dpq.get(apc.QparamETLName))
	if errN != nil {
		switch {
		case cos.IsErrNotFound(errN):
			smap := t.owner.smap.Get()
			pub := smap.Primary.URL(cmn.NetPublic)
			err := fmt.Errorf("%v - try starting new ETL with \"%s/v1/etl/init\" endpoint", errN, pub)
			t.writeErr(w, r, err, http.StatusNotFound)
		default:
			t.writeErr(w, r, errN)
		}
		return
	}

	// do
	var (
		xetl     = comm.Xact()
		pipeline apc.ETLPipeline
		err      error
	)
	if etlPipeline := dpq.get(apc.QparamETLPipeline); etlPipeline != "" {
		pipeline, err = etl.GetPipeline(strings.Split(etlPipeline, apc.ETLPipelineSeparator))
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
	}
	size, ecode, err := comm.InlineTransform(w, r, lom, &etl.InlineTransArgs{
		LatestVer:     dpq.latestVer,
		TransformArgs: dpq.get(apc.QparamETLTransformArgs),
		Pipeline:      pipeline,
	})

	// error handling
	switch {
	case err == nil:
		if size >= 0 {
			tstats := core.T.StatsUpdater()
			tstats.IncWith(stats.ETLInlineCount, xetl.Vlabs)
			tstats.AddWith(
				cos.NamedVal64{Name: stats.ETLInlineLatencyTotal, Value: mono.SinceNano(started), VarLabs: xetl.Vlabs},
				cos.NamedVal64{Name: stats.ETLInlineSize, Value: size, VarLabs: xetl.Vlabs},
			)
			xetl.ObjsAdd(1, size)
		}
	case cos.IsNotExist(err, ecode):
		xetl.InlineObjErrs.Add(&etl.ObjErr{
			ObjName: lom.Cname(),
			Message: "object not found",
			Ecode:   ecode,
		})
		t.writeErr(w, r, err, ecode)
	default:
		xetl.InlineObjErrs.Add(&etl.ObjErr{
			ObjName: lom.Cname(),
			Message: err.Error(),
			Ecode:   ecode,
		})
		t.writeErr(w, r, err, ecode)
	}
}

func (t *target) logsETL(w http.ResponseWriter, r *http.Request, etlName string) {
	logs, err := etl.PodLogs(etlName)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	t.writeJSON(w, r, logs, "logs-etl")
}

func (t *target) healthETL(w http.ResponseWriter, r *http.Request, etlName string) {
	health, err := etl.PodHealth(etlName)
	if err != nil {
		if cos.IsErrNotFound(err) {
			t.writeErr(w, r, err, http.StatusNotFound, Silent)
		} else {
			t.writeErr(w, r, err)
		}
		return
	}
	writeXid(w, health)
}

// GET /v1/etl/<etl-name>/details
//
// Returns ETL errors (if any) for the given ETL xaction.
func (t *target) detailsETL(w http.ResponseWriter, r *http.Request, dpq *dpq, etlName string) {
	var (
		offlineXid = dpq.sys.uuid
		errs       []error
	)
	comm, err := etl.GetCommunicator(etlName)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	xetl := comm.Xact()
	if offlineXid == "" {
		errs = xetl.InlineObjErrs.Unwrap()
	} else {
		errs = xetl.GetObjErrs(offlineXid)
	}

	objErrs := make([]etl.ObjErr, 0, len(errs))
	for _, e := range errs {
		var objErr *etl.ObjErr
		if !errors.As(e, &objErr) {
			continue
		}
		objErrs = append(objErrs, *objErr)
	}
	t.writeJSON(w, r, objErrs, "etl-obj-errors")
}

func (t *target) metricsETL(w http.ResponseWriter, r *http.Request, etlName string) {
	metricMsg, err := etl.PodMetrics(etlName)
	if err != nil {
		if cos.IsErrNotFound(err) {
			t.writeErr(w, r, err, http.StatusNotFound, Silent)
		} else {
			t.writeErr(w, r, err)
		}
		return
	}
	t.writeJSON(w, r, metricMsg, "metrics-etl")
}

func etlParseObjectReq(r *http.Request) (etlName, secret string, bck *meta.Bck, objName string, err error) {
	var items []string
	items, err = cmn.ParseURL(r.URL.EscapedPath(), apc.URLPathETLObject.L, 3, false)
	if err != nil {
		return
	}
	etlName = items[0]
	secret = items[1]

	// Encoding is done in `transformerPath`.
	var uname string
	uname, err = url.PathUnescape(items[2])
	if err != nil {
		return
	}

	bck, objName, err = meta.ParseUname(uname, true /*object name required*/)
	return
}

// GET /v1/etl/_object/<etl-name>/<secret>/<uname>
// Handles GET requests from ETL containers (K8s Pods).
// Validates the secret that was injected into a Pod during its initialization
// (see boot.go `_setPodEnv`).
//
// NOTE: this is an internal URL with "_object" in its path intended to avoid
// conflicts with ETL name in `/v1/elt/<etl-name>/...`
func (t *target) getObjectETL(w http.ResponseWriter, r *http.Request, dpq *dpq) {
	etlName, secret, bck, objName, err := etlParseObjectReq(r)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	if err := etl.ValidateSecret(etlName, secret); err != nil {
		t.writeErr(w, r, err)
		return
	}

	lom := core.AllocLOM(objName)
	lom, err = t.getObject(w, r, dpq, bck, lom)
	core.FreeLOM(lom)

	if err != nil {
		t._erris(w, r, err, 0, dpq.silent)
	}
}

// PUT /v1/etl/_object/<etl-name>/<secret>/<uname>?uuid=<xid>
// Handles PUT requests from ETL containers (K8s Pods).
// Validates the secret that was injected into a Pod during its initialization
// (see boot.go `_setPodEnv`).
func (t *target) putObjectETL(w http.ResponseWriter, r *http.Request) {
	var config = cmn.GCO.Get()

	etlName, secret, bck, objName, err := etlParseObjectReq(r)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	if err := etl.ValidateSecret(etlName, secret); err != nil {
		t.writeErr(w, r, err)
		return
	}

	lom := core.AllocLOM(objName)
	if err := lom.InitBck(bck); err != nil {
		if cmn.IsErrRemoteBckNotFound(err) {
			t.BMDVersionFixup(r)
			err = lom.InitBck(bck)
		}
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
	}

	dpq := dpqAlloc() // xid should be included in the query
	if err := dpq.parse(r.URL.RawQuery); err != nil {
		dpqFree(dpq)
		t.writeErr(w, r, err)
		return
	}
	ecode, err := t.putObject(w, r, dpq, lom, true /*t2t*/, config)
	core.FreeLOM(lom)
	dpqFree(dpq)

	if err != nil {
		t.writeErr(w, r, err, ecode)
	}
}

// HEAD /v1/etl/_object/<etl-name>/<secret>/<uname>
//
// Handles HEAD requests from ETL containers (K8s Pods).
// Validates the secret that was injected into a Pod during its initialization.
func (t *target) headObjectETL(w http.ResponseWriter, r *http.Request) {
	etlName, secret, bck, objName, err := etlParseObjectReq(r)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	if err := etl.ValidateSecret(etlName, secret); err != nil {
		t.writeErr(w, r, err)
		return
	}
	dpq := dpqAlloc()
	if err := dpq.parse(r.URL.RawQuery); err != nil {
		dpqFree(dpq)
		t.writeErr(w, r, err)
		return
	}

	lom := core.AllocLOM(objName)
	ecode, err := t.objHead(r, w.Header(), dpq, bck, lom)
	core.FreeLOM(lom)
	dpqFree(dpq)
	if err != nil {
		// always silent (compare w/ httpobjhead)
		t.writeErr(w, r, err, ecode, Silent)
	}
}
