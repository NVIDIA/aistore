// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/dsort/shard"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/xact/xreg"
	jsoniter "github.com/json-iterator/go"
	"github.com/tinylib/msgp/msgp"
)

type response struct {
	si         *meta.Snode
	res        []byte
	err        error
	statusCode int
}

//////////////////
///// PROXY //////
//////////////////

var psi core.Node

// POST /v1/sort
func PstartHandler(w http.ResponseWriter, r *http.Request, parsc *ParsedReq) {
	var (
		err  error
		pars = parsc.pars
	)
	pars.TargetOrderSalt = []byte(cos.FormatNowStamp())

	// TODO: handle case when bucket was removed during dsort job - this should
	// stop whole operation. Maybe some listeners as we have on smap change?
	// This would also be helpful for Downloader (in the middle of downloading
	// large file the bucket can be easily deleted).

	pars.DsorterType, err = dsorterType(pars)
	if err != nil {
		cmn.WriteErr(w, r, err)
		return
	}

	b, err := js.Marshal(pars)
	if err != nil {
		s := fmt.Sprintf("unable to marshal RequestSpec: %+v, err: %v", pars, err)
		cmn.WriteErrMsg(w, r, s, http.StatusInternalServerError)
		return
	}

	var (
		managerUUID = PrefixJobID + cos.GenUUID() // compare w/ p.httpdlpost
		smap        = psi.Sowner().Get()
	)

	// Starting dsort has two phases:
	// 1. Initialization, ensures that all targets successfully initialized all
	//    structures and are ready to receive requests: start, metrics, abort
	// 2. Start, where we request targets to start the dsort.
	//
	// This prevents bugs where one targets would just start dsort (other did
	// not have yet initialized) and starts to communicate with other targets
	// but because they are not ready with their initialization will not recognize
	// given dsort job. Also bug where we could send abort (which triggers cleanup)
	// to not yet initialized target.

	// phase 1
	if cmn.Rom.FastV(4, cos.SmoduleDsort) {
		nlog.Infof("[dsort] %s broadcasting init request to all targets", managerUUID)
	}
	path := apc.URLPathdSortInit.Join(managerUUID)
	responses := bcast(http.MethodPost, path, nil, b, smap)
	if err := _handleResp(w, r, smap, managerUUID, responses); err != nil {
		return
	}

	// phase 2
	if cmn.Rom.FastV(4, cos.SmoduleDsort) {
		nlog.Infof("[dsort] %s broadcasting start request to all targets", managerUUID)
	}
	path = apc.URLPathdSortStart.Join(managerUUID)
	responses = bcast(http.MethodPost, path, nil, nil, smap)
	if err := _handleResp(w, r, smap, managerUUID, responses); err != nil {
		return
	}

	w.Write([]byte(managerUUID))
}

func _handleResp(w http.ResponseWriter, r *http.Request, smap *meta.Smap, managerUUID string, responses []response) error {
	for _, resp := range responses {
		if resp.err == nil {
			continue
		}
		// cleanup
		path := apc.URLPathdSortAbort.Join(managerUUID)
		_ = bcast(http.MethodDelete, path, nil, nil, smap)

		msg := fmt.Sprintf("failed to start [dsort] %s: %v(%d)", managerUUID, resp.err, resp.statusCode)
		cmn.WriteErrMsg(w, r, msg, http.StatusInternalServerError)
		return resp.err
	}
	return nil
}

// GET /v1/sort
func PgetHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodGet) {
		return
	}
	query := r.URL.Query()
	managerUUID := query.Get(apc.QparamUUID)
	if managerUUID == "" {
		plistHandler(w, r, query)
		return
	}

	pmetricsHandler(w, r, query)
}

// GET /v1/sort?regex=...
func plistHandler(w http.ResponseWriter, r *http.Request, query url.Values) {
	var (
		path     = apc.URLPathdSortList.S
		regexStr = query.Get(apc.QparamRegex)
	)
	if regexStr != "" {
		if _, err := regexp.CompilePOSIX(regexStr); err != nil {
			cmn.WriteErr(w, r, err)
			return
		}
	}
	responses := bcast(http.MethodGet, path, query, nil, psi.Sowner().Get())

	resultList := make([]*JobInfo, 0, 4)
	for _, r := range responses {
		if r.err != nil {
			nlog.Errorln(r.err)
			continue
		}

		var targetMetrics []*JobInfo
		err := jsoniter.Unmarshal(r.res, &targetMetrics)
		debug.AssertNoErr(err)

		for _, job := range targetMetrics {
			var found bool
			for _, oldMetric := range resultList {
				if oldMetric.ID == job.ID {
					oldMetric.Aggregate(job)
					found = true
					break
				}
			}
			if !found {
				resultList = append(resultList, job)
			}
		}
	}

	w.Write(cos.MustMarshal(resultList))
}

// GET /v1/sort?id=...
func pmetricsHandler(w http.ResponseWriter, r *http.Request, query url.Values) {
	var (
		smap        = psi.Sowner().Get()
		all         = make(map[string]*JobInfo, smap.CountActiveTs())
		managerUUID = query.Get(apc.QparamUUID)
		path        = apc.URLPathdSortMetrics.Join(managerUUID)
		responses   = bcast(http.MethodGet, path, nil, nil, smap)
		notFound    int
	)
	for _, resp := range responses {
		if resp.statusCode == http.StatusNotFound {
			// Probably new target which does not know anything about this dsort op.
			notFound++
			continue
		}
		if resp.err != nil {
			cmn.WriteErr(w, r, resp.err, resp.statusCode)
			return
		}
		j := &JobInfo{}
		if err := js.Unmarshal(resp.res, j); err != nil {
			cmn.WriteErr(w, r, err, http.StatusInternalServerError)
			return
		}
		all[resp.si.ID()] = j
	}

	if notFound == len(responses) && notFound > 0 {
		msg := fmt.Sprintf("%s: [dsort] %s does not exist", core.T, managerUUID)
		cmn.WriteErrMsg(w, r, msg, http.StatusNotFound)
		return
	}
	w.Write(cos.MustMarshal(all))
}

// DELETE /v1/sort/abort
func PabortHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodDelete) {
		return
	}
	_, err := parseURL(w, r, 0, apc.URLPathdSortAbort.L)
	if err != nil {
		return
	}

	var (
		query       = r.URL.Query()
		managerUUID = query.Get(apc.QparamUUID)
		path        = apc.URLPathdSortAbort.Join(managerUUID)
		responses   = bcast(http.MethodDelete, path, nil, nil, psi.Sowner().Get())
	)
	allNotFound := true
	for _, resp := range responses {
		if resp.statusCode == http.StatusNotFound {
			continue
		}
		allNotFound = false

		if resp.err != nil {
			cmn.WriteErr(w, r, resp.err, resp.statusCode)
			return
		}
	}
	if allNotFound {
		err := cos.NewErrNotFound(core.T, "dsort job "+managerUUID)
		cmn.WriteErr(w, r, err, http.StatusNotFound)
		return
	}
}

// DELETE /v1/sort
func PremoveHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodDelete) {
		return
	}
	_, err := parseURL(w, r, 0, apc.URLPathdSort.L)
	if err != nil {
		return
	}

	var (
		smap        = psi.Sowner().Get()
		query       = r.URL.Query()
		managerUUID = query.Get(apc.QparamUUID)
		path        = apc.URLPathdSortMetrics.Join(managerUUID)
		responses   = bcast(http.MethodGet, path, nil, nil, smap)
	)

	// First, broadcast to see if process is cleaned up first
	seenOne := false
	for _, resp := range responses {
		if resp.statusCode == http.StatusNotFound {
			// Probably new target which does not know anything about this dsort op.
			continue
		}
		if resp.err != nil {
			cmn.WriteErr(w, r, resp.err, resp.statusCode)
			return
		}
		metrics := &Metrics{}
		if err := js.Unmarshal(resp.res, &metrics); err != nil {
			cmn.WriteErr(w, r, err, http.StatusInternalServerError)
			return
		}
		if !metrics.Archived.Load() {
			cmn.WriteErrMsg(w, r, fmt.Sprintf("%s process %s still in progress and cannot be removed",
				apc.ActDsort, managerUUID))
			return
		}
		seenOne = true
	}
	if !seenOne {
		s := fmt.Sprintf("invalid request: job %q does not exist", managerUUID)
		cmn.WriteErrMsg(w, r, s, http.StatusNotFound)
		return
	}

	// Next, broadcast the remove once we've checked that all targets have run cleanup
	path = apc.URLPathdSortRemove.Join(managerUUID)
	responses = bcast(http.MethodDelete, path, nil, nil, smap)
	var failed []string //nolint:prealloc // will remain not allocated when no errors
	for _, r := range responses {
		if r.statusCode == http.StatusOK {
			continue
		}
		failed = append(failed, fmt.Sprintf("%v: (%v) %v", r.si.ID(), r.statusCode, string(r.res)))
	}
	if len(failed) != 0 {
		err := fmt.Errorf("got errors while broadcasting remove: %v", failed)
		cmn.WriteErr(w, r, err)
	}
}

// Determine dsorter type. We need to make this decision based on (e.g.) size targets' memory.
func dsorterType(pars *parsedReqSpec) (string, error) {
	if pars.DsorterType != "" {
		return pars.DsorterType, nil // in case the dsorter type is already set, we need to respect it
	}

	// Get memory stats from targets
	var (
		totalAvailMemory  uint64
		err               error
		path              = apc.URLPathDae.S
		moreThanThreshold = true
	)

	dsorterMemThreshold, err := cos.ParseSize(pars.DsorterMemThreshold, cos.UnitsIEC)
	debug.AssertNoErr(err)

	query := make(url.Values)
	query.Add(apc.QparamWhat, apc.WhatNodeStatsAndStatus)
	responses := bcast(http.MethodGet, path, query, nil, psi.Sowner().Get())
	for _, response := range responses {
		if response.err != nil {
			return "", response.err
		}

		daemonStatus := stats.NodeStatus{}
		if err := jsoniter.Unmarshal(response.res, &daemonStatus); err != nil {
			return "", err
		}

		memStat := sys.MemStat{Total: daemonStatus.MemCPUInfo.MemAvail + daemonStatus.MemCPUInfo.MemUsed}
		dsortAvailMemory := calcMaxMemoryUsage(pars.MaxMemUsage, &memStat)
		totalAvailMemory += dsortAvailMemory
		moreThanThreshold = moreThanThreshold && dsortAvailMemory > uint64(dsorterMemThreshold)
	}

	// TODO: currently, we have import cycle: dsort -> api -> dsort. Need to
	// think of a way to get the total size of bucket without copy-and-paste
	// the API code.
	//
	// baseParams := &api.BaseParams{
	// 	Client: http.DefaultClient,
	// 	URL:    g.smap.Get().Primary.URL(cmn.NetIntraControl),
	// }
	// msg := &apc.LsoMsg{Props: "size,status"}
	// objList, err := api.ListObjects(baseParams, pars.Bucket, msg, 0)
	// if err != nil {
	// 	return "", err
	// }
	//
	// totalBucketSize := uint64(0)
	// for _, obj := range objList.Entries {
	// 	if obj.IsStatusOK() {
	// 		totalBucketSize += uint64(obj.Size)
	// 	}
	// }
	//
	// if totalBucketSize < totalAvailMemory {
	// 	// "general type" is capable of extracting whole dataset into memory
	// 	// In this case the creation phase is super fast.
	// 	return GeneralType, nil
	// }

	if moreThanThreshold {
		// If there is enough memory to use "memory type", we should do that.
		// It behaves better for cases when we have a lot of memory available.
		return MemType, nil
	}

	// For all other cases we should use "general type", as we don't know
	// exactly what to expect, so we should prepare for the worst.
	return GeneralType, nil
}

///////////////////
///// TARGET //////
///////////////////

// [METHOD] /v1/sort
func TargetHandler(w http.ResponseWriter, r *http.Request) {
	apiItems, err := parseURL(w, r, 1, apc.URLPathdSort.L)
	if err != nil {
		return
	}

	switch apiItems[0] {
	case apc.Init:
		tinitHandler(w, r)
	case apc.Start:
		tstartHandler(w, r)
	case apc.Records:
		Managers.recordsHandler(w, r)
	case apc.Shards:
		Managers.shardsHandler(w, r)
	case apc.Abort:
		tabortHandler(w, r)
	case apc.Remove:
		tremoveHandler(w, r)
	case apc.List:
		tlistHandler(w, r)
	case apc.Metrics:
		tmetricsHandler(w, r)
	case apc.FinishedAck:
		tfiniHandler(w, r)
	default:
		cmn.WriteErrMsg(w, r, "invalid path")
	}
}

// /v1/sort/init.
// receive parsedReqSpec and initialize dsort manager
func tinitHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodPost) {
		return
	}
	// disallow to run when above high wm (let alone OOS)
	cs := fs.Cap()
	if errCap := cs.Err(); errCap != nil {
		cmn.WriteErr(w, r, errCap, http.StatusInsufficientStorage)
		return
	}

	apiItems, errV := parseURL(w, r, 1, apc.URLPathdSortInit.L)
	if errV != nil {
		return
	}
	var (
		pars   *parsedReqSpec
		b, err = io.ReadAll(r.Body)
	)
	if err != nil {
		cmn.WriteErr(w, r, fmt.Errorf("[dsort]: failed to receive request: %w", err))
		return
	}
	if err = js.Unmarshal(b, &pars); err != nil {
		err := fmt.Errorf(cmn.FmtErrUnmarshal, apc.ActDsort, "parsedReqSpec", cos.BHead(b), err)
		cmn.WriteErr(w, r, err)
		return
	}

	managerUUID := apiItems[0]
	m, err := Managers.Add(managerUUID) // NOTE: returns manager locked iff err == nil
	if err != nil {
		cmn.WriteErr(w, r, err)
		return
	}
	if err = m.init(pars); err != nil {
		cmn.WriteErr(w, r, err)
	} else {
		// setup xaction
		debug.Assert(!pars.OutputBck.IsEmpty())
		custom := &xreg.DsortArgs{BckFrom: meta.CloneBck(&pars.InputBck), BckTo: meta.CloneBck(&pars.OutputBck)}
		rns := xreg.RenewDsort(managerUUID, custom)
		debug.AssertNoErr(rns.Err)
		xctn := rns.Entry.Get()
		debug.Assert(xctn.ID() == managerUUID, xctn.ID()+" vs "+managerUUID)

		m.xctn = xctn.(*xaction)
	}
	m.unlock()
}

// /v1/sort/start.
// There are three major phases to this function:
//  1. extractLocalShards
//  2. participateInRecordDistribution
//  3. distributeShardRecords
func tstartHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodPost) {
		return
	}
	apiItems, err := parseURL(w, r, 1, apc.URLPathdSortStart.L)
	if err != nil {
		return
	}

	managerUUID := apiItems[0]
	m, exists := Managers.Get(managerUUID, false /*incl. archived*/)
	if !exists {
		s := fmt.Sprintf("invalid request: job %q does not exist", managerUUID)
		cmn.WriteErrMsg(w, r, s, http.StatusNotFound)
		return
	}

	go m.startDsort()
}

func (m *Manager) startDsort() {
	if err := m.start(); err != nil {
		m.errHandler(err)
		return
	}

	nlog.Infof("[dsort] %s broadcasting finished ack to other targets", m.ManagerUUID)
	path := apc.URLPathdSortAck.Join(m.ManagerUUID, core.T.SID())
	bcast(http.MethodPut, path, nil, nil, core.T.Sowner().Get(), core.T.Snode())
}

func (m *Manager) errHandler(err error) {
	nlog.Infoln(err)

	// If we were aborted by some other process this means that we do not
	// broadcast abort (we assume that daemon aborted us, aborted also others).
	if !m.aborted() {
		// Self-abort: better do it before sending broadcast to avoid
		// inconsistent state: other have aborted but we didn't due to some
		// problem.
		if isReportableError(err) {
			m.abort(err)
		} else {
			m.abort(nil)
		}

		nlog.Warningln("broadcasting abort to other targets")
		path := apc.URLPathdSortAbort.Join(m.ManagerUUID)
		bcast(http.MethodDelete, path, nil, nil, core.T.Sowner().Get(), core.T.Snode())
	}
}

// shardsHandler is the handler for the HTTP endpoint /v1/sort/shards.
// A valid POST to this endpoint results in a new shard being created locally based on the contents
// of the incoming request body. The shard is then sent to the correct target in the cluster as per HRW.
func (managers *ManagerGroup) shardsHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodPost) {
		return
	}
	apiItems, err := parseURL(w, r, 1, apc.URLPathdSortShards.L)
	if err != nil {
		return
	}
	managerUUID := apiItems[0]
	m, exists := managers.Get(managerUUID, false /*incl. archived*/)
	if !exists {
		s := fmt.Sprintf("invalid request: job %q does not exist", managerUUID)
		cmn.WriteErrMsg(w, r, s, http.StatusNotFound)
		return
	}

	if !m.inProgress() {
		cmn.WriteErrMsg(w, r, fmt.Sprintf("no %s process in progress", apc.ActDsort))
		return
	}
	if m.aborted() {
		cmn.WriteErrMsg(w, r, fmt.Sprintf("%s process was aborted", apc.ActDsort))
		return
	}

	var (
		buf, slab   = g.mm.AllocSize(serializationBufSize)
		tmpMetadata = &CreationPhaseMetadata{}
	)
	defer slab.Free(buf)

	if err := tmpMetadata.DecodeMsg(msgp.NewReaderBuf(r.Body, buf)); err != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, apc.ActDsort, "creation phase metadata", "-", err)
		cmn.WriteErr(w, r, err, http.StatusInternalServerError)
		return
	}

	if !m.inProgress() || m.aborted() {
		cmn.WriteErrMsg(w, r, fmt.Sprintf("no %s process", apc.ActDsort))
		return
	}

	m.creationPhase.metadata = *tmpMetadata
	m.startShardCreation <- struct{}{}
}

// recordsHandler is the handler /v1/sort/records.
// A valid POST to this endpoint updates this target's dsortManager.Records with the
// []Records from the request body, along with some related state variables.
func (managers *ManagerGroup) recordsHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodPost) {
		return
	}
	apiItems, err := parseURL(w, r, 1, apc.URLPathdSortRecords.L)
	if err != nil {
		return
	}
	managerUUID := apiItems[0]
	m, exists := managers.Get(managerUUID, false /*incl. archived*/)
	if !exists {
		s := fmt.Sprintf("invalid request: job %q does not exist", managerUUID)
		cmn.WriteErrMsg(w, r, s, http.StatusNotFound)
		return
	}
	if !m.inProgress() {
		cmn.WriteErrMsg(w, r, fmt.Sprintf("no %s process in progress", apc.ActDsort))
		return
	}
	if m.aborted() {
		cmn.WriteErrMsg(w, r, fmt.Sprintf("%s process was aborted", apc.ActDsort))
		return
	}

	query := r.URL.Query()
	totalShardSize, err := strconv.ParseInt(query.Get(apc.QparamTotalCompressedSize), 10, 64)
	if err != nil {
		s := fmt.Sprintf("invalid %s in request to %s, err: %v",
			apc.QparamTotalCompressedSize, r.URL.String(), err)
		cmn.WriteErrMsg(w, r, s)
		return
	}
	totalExtractedSize, err := strconv.ParseInt(query.Get(apc.QparamTotalUncompressedSize), 10, 64)
	if err != nil {
		s := fmt.Sprintf("invalid %s in request to %s, err: %v",
			apc.QparamTotalUncompressedSize, r.URL.String(), err)
		cmn.WriteErrMsg(w, r, s)
		return
	}
	d, err := strconv.ParseUint(query.Get(apc.QparamTotalInputShardsExtracted), 10, 64)
	if err != nil {
		s := fmt.Sprintf("invalid %s in request to %s, err: %v",
			apc.QparamTotalInputShardsExtracted, r.URL.String(), err)
		cmn.WriteErrMsg(w, r, s)
		return
	}

	var (
		buf, slab = g.mm.AllocSize(serializationBufSize)
		records   = shard.NewRecords(int(d))
	)
	defer slab.Free(buf)

	if err := records.DecodeMsg(msgp.NewReaderBuf(r.Body, buf)); err != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, apc.ActDsort, "records", "-", err)
		cmn.WriteErr(w, r, err, http.StatusInternalServerError)
		return
	}

	m.addSizes(totalShardSize, totalExtractedSize)
	m.recm.EnqueueRecords(records)
	m.incrementReceived()

	if cmn.Rom.FastV(4, cos.SmoduleDsort) {
		nlog.Infof(
			"[dsort] %s total times received records from another target: %d",
			m.ManagerUUID, m.received.count.Load(),
		)
	}
}

// /v1/sort/abort.
// A valid DELETE to this endpoint aborts currently running sort job and cleans
// up the state.
func tabortHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodDelete) {
		return
	}
	apiItems, err := parseURL(w, r, 1, apc.URLPathdSortAbort.L)
	if err != nil {
		return
	}

	managerUUID := apiItems[0]
	m, exists := Managers.Get(managerUUID, true /*incl. archived*/)
	if !exists {
		s := fmt.Sprintf("%s: [dsort] %s does not exist", core.T, managerUUID)
		cmn.WriteErrMsg(w, r, s, http.StatusNotFound)
		return
	}
	if m.Metrics.Archived.Load() {
		s := fmt.Sprintf("%s: [dsort] %s is already archived", core.T, managerUUID)
		cmn.WriteErrMsg(w, r, s, http.StatusGone)
		return
	}

	err = fmt.Errorf("%s: [dsort] %s aborted", core.T, managerUUID)
	m.abort(err)
}

func tremoveHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodDelete) {
		return
	}
	apiItems, err := parseURL(w, r, 1, apc.URLPathdSortRemove.L)
	if err != nil {
		return
	}

	managerUUID := apiItems[0]
	if err := Managers.Remove(managerUUID); err != nil {
		cmn.WriteErr(w, r, err)
		return
	}
}

func tlistHandler(w http.ResponseWriter, r *http.Request) {
	var (
		query      = r.URL.Query()
		regexStr   = query.Get(apc.QparamRegex)
		onlyActive = cos.IsParseBool(query.Get(apc.QparamOnlyActive))
		regex      *regexp.Regexp
	)
	if !checkHTTPMethod(w, r, http.MethodGet) {
		return
	}
	if regexStr != "" {
		var err error
		if regex, err = regexp.CompilePOSIX(regexStr); err != nil {
			cmn.WriteErr(w, r, err)
			return
		}
	}

	w.Write(cos.MustMarshal(Managers.List(regex, onlyActive)))
}

// /v1/sort/metrics.
// A valid GET to this endpoint sends response with sort metrics.
func tmetricsHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodGet) {
		return
	}
	apiItems, err := parseURL(w, r, 1, apc.URLPathdSortMetrics.L)
	if err != nil {
		return
	}

	managerUUID := apiItems[0]
	m, exists := Managers.Get(managerUUID, true /*incl. archived*/)
	if !exists {
		s := fmt.Sprintf("%s: [dsort] %s does not exist", core.T, managerUUID)
		cmn.WriteErrMsg(w, r, s, http.StatusNotFound)
		return
	}

	m.Metrics.lock()
	m.Metrics.update()
	j := m.Metrics.ToJobInfo(m.ManagerUUID, m.Pars)
	j.Metrics = m.Metrics
	body := cos.MustMarshal(j)
	m.Metrics.unlock()

	w.Write(body)
}

// /v1/sort/finished-ack.
// A valid PUT to this endpoint acknowledges that tid has finished dsort operation.
func tfiniHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodPut) {
		return
	}
	apiItems, err := parseURL(w, r, 2, apc.URLPathdSortAck.L)
	if err != nil {
		return
	}

	managerUUID, tid := apiItems[0], apiItems[1]
	m, exists := Managers.Get(managerUUID, false /*incl. archived*/)
	if !exists {
		s := fmt.Sprintf("invalid request: job %q does not exist", managerUUID)
		cmn.WriteErrMsg(w, r, s, http.StatusNotFound)
		return
	}

	m.updateFinishedAck(tid)
}

//
// http helpers
//

func checkHTTPMethod(w http.ResponseWriter, r *http.Request, expected string) bool {
	if r.Method != expected {
		s := fmt.Sprintf("invalid method '%s %s', expecting '%s'", r.Method, r.URL.String(), expected)
		cmn.WriteErrMsg(w, r, s)
		return false
	}
	return true
}

func parseURL(w http.ResponseWriter, r *http.Request, itemsAfter int, items []string) ([]string, error) {
	items, err := cmn.ParseURL(r.URL.Path, items, itemsAfter, true)
	if err != nil {
		cmn.WriteErr(w, r, err)
		return nil, err
	}

	return items, err
}
