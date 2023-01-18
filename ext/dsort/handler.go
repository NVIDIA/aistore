// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"sync"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/dsort/extract"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	jsoniter "github.com/json-iterator/go"
	"github.com/tinylib/msgp/msgp"
)

type response struct {
	si         *cluster.Snode
	res        []byte
	err        error
	statusCode int
}

//////////////////
///// PROXY //////
//////////////////

// POST /v1/sort
func ProxyStartSortHandler(w http.ResponseWriter, r *http.Request, parsedRS *ParsedRequestSpec) {
	var err error
	parsedRS.TargetOrderSalt = []byte(cos.FormatNowStamp())

	// TODO: handle case when bucket was removed during dSort job - this should
	// stop whole operation. Maybe some listeners as we have on smap change?
	// This would also be helpful for Downloader (in the middle of downloading
	// large file the bucket can be easily deleted).

	parsedRS.DSorterType, err = determineDSorterType(parsedRS)
	if err != nil {
		cmn.WriteErr(w, r, err)
		return
	}

	b, err := js.Marshal(parsedRS)
	if err != nil {
		s := fmt.Sprintf("unable to marshal RequestSpec: %+v, err: %v", parsedRS, err)
		cmn.WriteErrMsg(w, r, s, http.StatusInternalServerError)
		return
	}

	var (
		managerUUID = PrefixJobID + cos.GenUUID() // compare w/ p.httpdlpost
		smap        = ctx.smapOwner.Get()
	)
	checkResponses := func(responses []response) error {
		for _, resp := range responses {
			if resp.err == nil {
				continue
			}
			glog.Errorf("[%s] start sort request failed to be broadcast, err: %s",
				managerUUID, resp.err.Error())

			path := apc.URLPathdSortAbort.Join(managerUUID)
			broadcastTargets(http.MethodDelete, path, nil, nil, smap)

			s := fmt.Sprintf("failed to execute start sort, err: %s, status: %d",
				resp.err.Error(), resp.statusCode)
			cmn.WriteErrMsg(w, r, s, http.StatusInternalServerError)
			return resp.err
		}

		return nil
	}

	// Starting dSort has two phases:
	// 1. Initialization, ensures that all targets successfully initialized all
	//    structures and are ready to receive requests: start, metrics, abort
	// 2. Start, where we request targets to start the dSort.
	//
	// This prevents bugs where one targets would just start dSort (other did
	// not have yet initialized) and starts to communicate with other targets
	// but because they are not ready with their initialization will not recognize
	// given dSort job. Also bug where we could send abort (which triggers cleanup)
	// to not yet initialized target.

	if glog.V(4) {
		glog.Infof("[dsort] %s broadcasting init request to all targets", managerUUID)
	}
	path := apc.URLPathdSortInit.Join(managerUUID)
	responses := broadcastTargets(http.MethodPost, path, nil, b, smap)
	if err := checkResponses(responses); err != nil {
		return
	}

	if glog.V(4) {
		glog.Infof("[dsort] %s broadcasting start request to all targets", managerUUID)
	}
	path = apc.URLPathdSortStart.Join(managerUUID)
	responses = broadcastTargets(http.MethodPost, path, nil, nil, smap)
	if err := checkResponses(responses); err != nil {
		return
	}

	w.Write([]byte(managerUUID))
}

// GET /v1/sort
func ProxyGetHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodGet) {
		return
	}

	query := r.URL.Query()
	managerUUID := query.Get(apc.QparamUUID)

	if managerUUID == "" {
		proxyListSortHandler(w, r, query)
		return
	}

	proxyMetricsSortHandler(w, r, query)
}

// GET /v1/sort?regex=...
func proxyListSortHandler(w http.ResponseWriter, r *http.Request, query url.Values) {
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
	responses := broadcastTargets(http.MethodGet, path, query, nil, ctx.smapOwner.Get())

	resultList := make([]*JobInfo, 0)
	for _, r := range responses {
		if r.err != nil {
			glog.Error(r.err)
			continue
		}

		var newMetrics []*JobInfo
		err := jsoniter.Unmarshal(r.res, &newMetrics)
		debug.AssertNoErr(err)

		for _, job := range newMetrics {
			found := false
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

	body := cos.MustMarshal(resultList)
	if _, err := w.Write(body); err != nil {
		glog.Error(err)
		// When we fail write we cannot call InvalidHandler since it will be
		// double header write.
		return
	}
}

// GET /v1/sort?id=...
func proxyMetricsSortHandler(w http.ResponseWriter, r *http.Request, query url.Values) {
	var (
		smap        = ctx.smapOwner.Get()
		allMetrics  = make(map[string]*Metrics, smap.CountActiveTargets())
		managerUUID = query.Get(apc.QparamUUID)
		path        = apc.URLPathdSortMetrics.Join(managerUUID)
		responses   = broadcastTargets(http.MethodGet, path, nil, nil, smap)
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
		metrics := &Metrics{}
		if err := js.Unmarshal(resp.res, &metrics); err != nil {
			cmn.WriteErr(w, r, err, http.StatusInternalServerError)
			return
		}
		allMetrics[resp.si.ID()] = metrics
	}

	if notFound == len(responses) && notFound > 0 {
		msg := fmt.Sprintf("%s job %q not found", DSortName, managerUUID)
		cmn.WriteErrMsg(w, r, msg, http.StatusNotFound)
		return
	}

	body, err := js.Marshal(allMetrics)
	if err != nil {
		cmn.WriteErr(w, r, err, http.StatusInternalServerError)
		return
	}
	w.Write(body)
}

// DELETE /v1/sort/abort
func ProxyAbortSortHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodDelete) {
		return
	}
	_, err := checkRESTItems(w, r, 0, apc.URLPathdSortAbort.L)
	if err != nil {
		return
	}

	var (
		query       = r.URL.Query()
		managerUUID = query.Get(apc.QparamUUID)
		path        = apc.URLPathdSortAbort.Join(managerUUID)
		responses   = broadcastTargets(http.MethodDelete, path, nil, nil, ctx.smapOwner.Get())
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
		err := cmn.NewErrNotFound("%s job %q", DSortName, managerUUID)
		cmn.WriteErr(w, r, err, http.StatusNotFound)
		return
	}
}

// DELETE /v1/sort
func ProxyRemoveSortHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodDelete) {
		return
	}
	_, err := checkRESTItems(w, r, 0, apc.URLPathdSort.L)
	if err != nil {
		return
	}

	var (
		smap        = ctx.smapOwner.Get()
		query       = r.URL.Query()
		managerUUID = query.Get(apc.QparamUUID)
		path        = apc.URLPathdSortMetrics.Join(managerUUID)
		responses   = broadcastTargets(http.MethodGet, path, nil, nil, smap)
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
				DSortName, managerUUID))
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
	responses = broadcastTargets(http.MethodDelete, path, nil, nil, smap)
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

///////////////////
///// TARGET //////
///////////////////

// [METHOD] /v1/sort
func TargetHandler(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 1, apc.URLPathdSort.L)
	if err != nil {
		return
	}

	switch apiItems[0] {
	case apc.Init:
		initSortHandler(w, r)
	case apc.Start:
		startSortHandler(w, r)
	case apc.Records:
		recordsHandler(Managers)(w, r)
	case apc.Shards:
		shardsHandler(Managers)(w, r)
	case apc.Abort:
		abortSortHandler(w, r)
	case apc.Remove:
		removeSortHandler(w, r)
	case apc.List:
		listSortHandler(w, r)
	case apc.Metrics:
		metricsHandler(w, r)
	case apc.FinishedAck:
		finishedAckHandler(w, r)
	default:
		cmn.WriteErrMsg(w, r, "invalid path")
	}
}

// initSortHandler is the handler called for the HTTP endpoint /v1/sort/init.
// It is responsible for initializing the dSort manager so it will be ready
// to start receiving requests.
func initSortHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodPost) {
		return
	}
	apiItems, err := checkRESTItems(w, r, 1, apc.URLPathdSortInit.L)
	if err != nil {
		return
	}
	var rs *ParsedRequestSpec
	b, err := io.ReadAll(r.Body)
	if err != nil {
		cmn.WriteErr(w, r, fmt.Errorf("could not read request body, err: %w", err))
		return
	}
	if err = js.Unmarshal(b, &rs); err != nil {
		err := fmt.Errorf(cmn.FmtErrUnmarshal, DSortName, "ParsedRequestSpec", cos.BHead(b), err)
		cmn.WriteErr(w, r, err)
		return
	}

	managerUUID := apiItems[0]
	dsortManager, err := Managers.Add(managerUUID)
	if err != nil {
		cmn.WriteErr(w, r, err)
		return
	}
	defer dsortManager.unlock()
	if err = dsortManager.init(rs); err != nil {
		cmn.WriteErr(w, r, err)
		return
	}
}

// startSortHandler is the handler called for the HTTP endpoint /v1/sort/start.
// There are three major phases to this function:
//  1. extractLocalShards
//  2. participateInRecordDistribution
//  3. distributeShardRecords
func startSortHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodPost) {
		return
	}
	apiItems, err := checkRESTItems(w, r, 1, apc.URLPathdSortStart.L)
	if err != nil {
		return
	}

	managerUUID := apiItems[0]
	dsortManager, exists := Managers.Get(managerUUID)
	if !exists {
		s := fmt.Sprintf("invalid request: job %q does not exist", managerUUID)
		cmn.WriteErrMsg(w, r, s, http.StatusNotFound)
		return
	}

	go dsortManager.startDSort()
}

func (m *Manager) startDSort() {
	errHandler := func(err error) {
		glog.Errorf("%+v", err) // print error with stack trace

		// If we were aborted by some other process this means that we do not
		// broadcast abort (we assume that daemon aborted us, aborted also others).
		if !m.aborted() {
			// Self-abort: better do it before sending broadcast to avoid
			// inconsistent state: other have aborted but we didn't due to some
			// problem.
			if isReportableError(err) {
				m.abort(err)
			} else {
				m.abort()
			}

			glog.Warning("broadcasting abort to other targets")
			path := apc.URLPathdSortAbort.Join(m.ManagerUUID)
			broadcastTargets(http.MethodDelete, path, nil, nil, ctx.smapOwner.Get(), ctx.node)
		}
	}

	if err := m.start(); err != nil {
		errHandler(err)
		return
	}

	glog.Infof("[dsort] %s broadcasting finished ack to other targets", m.ManagerUUID)
	path := apc.URLPathdSortAck.Join(m.ManagerUUID, m.ctx.node.ID())
	broadcastTargets(http.MethodPut, path, nil, nil, ctx.smapOwner.Get(), ctx.node)
}

// shardsHandler is the handler for the HTTP endpoint /v1/sort/shards.
// A valid POST to this endpoint results in a new shard being created locally based on the contents
// of the incoming request body. The shard is then sent to the correct target in the cluster as per HRW.
func shardsHandler(managers *ManagerGroup) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !checkHTTPMethod(w, r, http.MethodPost) {
			return
		}
		apiItems, err := checkRESTItems(w, r, 1, apc.URLPathdSortShards.L)
		if err != nil {
			return
		}
		managerUUID := apiItems[0]
		dsortManager, exists := managers.Get(managerUUID)
		if !exists {
			s := fmt.Sprintf("invalid request: job %q does not exist", managerUUID)
			cmn.WriteErrMsg(w, r, s, http.StatusNotFound)
			return
		}

		if !dsortManager.inProgress() {
			cmn.WriteErrMsg(w, r, fmt.Sprintf("no %s process in progress", DSortName))
			return
		}
		if dsortManager.aborted() {
			cmn.WriteErrMsg(w, r, fmt.Sprintf("%s process was aborted", DSortName))
			return
		}

		var (
			buf, slab   = mm.AllocSize(serializationBufSize)
			tmpMetadata = &CreationPhaseMetadata{}
		)
		defer slab.Free(buf)

		if err := tmpMetadata.DecodeMsg(msgp.NewReaderBuf(r.Body, buf)); err != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, DSortName, "creation phase metadata", "-", err)
			cmn.WriteErr(w, r, err, http.StatusInternalServerError)
			return
		}

		if !dsortManager.inProgress() || dsortManager.aborted() {
			cmn.WriteErrMsg(w, r, fmt.Sprintf("no %s process", DSortName))
			return
		}

		dsortManager.creationPhase.metadata = *tmpMetadata
		dsortManager.startShardCreation <- struct{}{}
	}
}

// recordsHandler is the handler called for the HTTP endpoint /v1/sort/records.
// A valid POST to this endpoint updates this target's dsortManager.Records with the
// []Records from the request body, along with some related state variables.
func recordsHandler(managers *ManagerGroup) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !checkHTTPMethod(w, r, http.MethodPost) {
			return
		}
		apiItems, err := checkRESTItems(w, r, 1, apc.URLPathdSortRecords.L)
		if err != nil {
			return
		}
		managerUUID := apiItems[0]
		dsortManager, exists := managers.Get(managerUUID)
		if !exists {
			s := fmt.Sprintf("invalid request: job %q does not exist", managerUUID)
			cmn.WriteErrMsg(w, r, s, http.StatusNotFound)
			return
		}
		if !dsortManager.inProgress() {
			cmn.WriteErrMsg(w, r, fmt.Sprintf("no %s process in progress", DSortName))
			return
		}
		if dsortManager.aborted() {
			cmn.WriteErrMsg(w, r, fmt.Sprintf("%s process was aborted", DSortName))
			return
		}
		var (
			query     = r.URL.Query()
			compStr   = query.Get(apc.QparamTotalCompressedSize)
			uncompStr = query.Get(apc.QparamTotalUncompressedSize)
			dStr      = query.Get(apc.QparamTotalInputShardsExtracted)
		)

		compressed, err := strconv.ParseInt(compStr, 10, 64)
		if err != nil {
			s := fmt.Sprintf("invalid %s in request to %s, err: %v",
				apc.QparamTotalCompressedSize, r.URL.String(), err)
			cmn.WriteErrMsg(w, r, s)
			return
		}
		uncompressed, err := strconv.ParseInt(uncompStr, 10, 64)
		if err != nil {
			s := fmt.Sprintf("invalid %s in request to %s, err: %v",
				apc.QparamTotalUncompressedSize, r.URL.String(), err)
			cmn.WriteErrMsg(w, r, s)
			return
		}
		d, err := strconv.ParseUint(dStr, 10, 64)
		if err != nil {
			s := fmt.Sprintf("invalid %s in request to %s, err: %v",
				apc.QparamTotalInputShardsExtracted, r.URL.String(), err)
			cmn.WriteErrMsg(w, r, s)
			return
		}

		var (
			buf, slab = mm.AllocSize(serializationBufSize)
			records   = extract.NewRecords(int(d))
		)
		defer slab.Free(buf)

		if err := records.DecodeMsg(msgp.NewReaderBuf(r.Body, buf)); err != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, DSortName, "records", "-", err)
			cmn.WriteErr(w, r, err, http.StatusInternalServerError)
			return
		}

		dsortManager.addCompressionSizes(compressed, uncompressed)
		dsortManager.recManager.EnqueueRecords(records)
		dsortManager.incrementReceived()
		if glog.V(4) {
			glog.Infof(
				"[dsort] %s total times received records from another target: %d",
				dsortManager.ManagerUUID, dsortManager.received.count.Load(),
			)
		}
	}
}

// abortSortHandler is the handler called for the HTTP endpoint /v1/sort/abort.
// A valid DELETE to this endpoint aborts currently running sort job and cleans
// up the state.
func abortSortHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodDelete) {
		return
	}
	apiItems, err := checkRESTItems(w, r, 1, apc.URLPathdSortAbort.L)
	if err != nil {
		return
	}

	managerUUID := apiItems[0]
	dsortManager, exists := Managers.Get(managerUUID, true /*allowPersisted*/)
	if !exists {
		s := fmt.Sprintf("invalid request: job %q does not exist", managerUUID)
		cmn.WriteErrMsg(w, r, s, http.StatusNotFound)
		return
	}
	if dsortManager.Metrics.Archived.Load() {
		s := fmt.Sprintf("invalid request: %s job %q has already finished", DSortName, managerUUID)
		cmn.WriteErrMsg(w, r, s, http.StatusGone)
		return
	}

	dsortManager.abort(fmt.Errorf("%s has been aborted via API (remotely)", DSortName))
}

func removeSortHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodDelete) {
		return
	}
	apiItems, err := checkRESTItems(w, r, 1, apc.URLPathdSortRemove.L)
	if err != nil {
		return
	}

	managerUUID := apiItems[0]
	if err := Managers.Remove(managerUUID); err != nil {
		cmn.WriteErr(w, r, err)
		return
	}
}

func listSortHandler(w http.ResponseWriter, r *http.Request) {
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

	body := cos.MustMarshal(Managers.List(regex, onlyActive))
	if _, err := w.Write(body); err != nil {
		glog.Error(err)
		// When we fail write we cannot call InvalidHandler since it will be
		// double header write.
		return
	}
}

// metricsHandler is the handler called for the HTTP endpoint /v1/sort/metrics.
// A valid GET to this endpoint sends response with sort metrics.
func metricsHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodGet) {
		return
	}
	apiItems, err := checkRESTItems(w, r, 1, apc.URLPathdSortMetrics.L)
	if err != nil {
		return
	}

	managerUUID := apiItems[0]
	dsortManager, exists := Managers.Get(managerUUID, true /*allowPersisted*/)
	if !exists {
		s := fmt.Sprintf("invalid request: job %q does not exist", managerUUID)
		cmn.WriteErrMsg(w, r, s, http.StatusNotFound)
		return
	}

	dsortManager.Metrics.update()
	body := dsortManager.Metrics.Marshal()
	if _, err := w.Write(body); err != nil {
		glog.Error(err)
		// When we fail write we cannot call InvalidHandler since it will be
		// double header write.
		return
	}
}

// finishedAckHandler is the handler called for the HTTP endpoint /v1/sort/finished-ack.
// A valid PUT to this endpoint acknowledges that daemonID has finished dSort operation.
func finishedAckHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodPut) {
		return
	}
	apiItems, err := checkRESTItems(w, r, 2, apc.URLPathdSortAck.L)
	if err != nil {
		return
	}

	managerUUID, daemonID := apiItems[0], apiItems[1]
	dsortManager, exists := Managers.Get(managerUUID)
	if !exists {
		s := fmt.Sprintf("invalid request: job %q does not exist", managerUUID)
		cmn.WriteErrMsg(w, r, s, http.StatusNotFound)
		return
	}

	dsortManager.updateFinishedAck(daemonID)
}

func broadcastTargets(method, path string, urlParams url.Values, body []byte, smap *cluster.Smap, ignore ...*cluster.Snode) []response {
	var (
		responses = make([]response, smap.CountActiveTargets())
		wg        = &sync.WaitGroup{}
	)

	call := func(idx int, node *cluster.Snode) {
		defer wg.Done()

		reqArgs := cmn.HreqArgs{
			Method: method,
			Base:   node.URL(cmn.NetIntraControl),
			Path:   path,
			Query:  urlParams,
			Body:   body,
		}
		req, err := reqArgs.Req()
		if err != nil {
			responses[idx] = response{
				si:         node,
				err:        err,
				statusCode: http.StatusInternalServerError,
			}
			return
		}

		resp, err := ctx.client.Do(req) //nolint:bodyclose // Closed inside `cos.Close`.
		if err != nil {
			responses[idx] = response{
				si:         node,
				err:        err,
				statusCode: http.StatusInternalServerError,
			}
			return
		}
		out, err := io.ReadAll(resp.Body)
		cos.Close(resp.Body)

		responses[idx] = response{
			si:         node,
			res:        out,
			err:        err,
			statusCode: resp.StatusCode,
		}
	}

	idx := 0
outer:
	for _, node := range smap.Tmap {
		if smap.PresentInMaint(node) {
			continue
		}
		for _, ignoreNode := range ignore {
			if ignoreNode.Equals(node) {
				continue outer
			}
		}

		wg.Add(1)
		go call(idx, node)
		idx++
	}
	wg.Wait()

	return responses[:idx]
}

func checkHTTPMethod(w http.ResponseWriter, r *http.Request, expected string) bool {
	if r.Method != expected {
		s := fmt.Sprintf("invalid method: %s to %s, should be %s", r.Method, r.URL.String(), expected)
		cmn.WriteErrMsg(w, r, s)
		return false
	}
	return true
}

func checkRESTItems(w http.ResponseWriter, r *http.Request, itemsAfter int, items []string) ([]string, error) {
	items, err := cmn.MatchItems(r.URL.Path, itemsAfter, true, items)
	if err != nil {
		cmn.WriteErr(w, r, err)
		return nil, err
	}

	return items, err
}

// Determine what dsorter type we should use. We need to make this decision
// based on eg. how much memory targets have.
func determineDSorterType(parsedRS *ParsedRequestSpec) (string, error) {
	if parsedRS.DSorterType != "" {
		return parsedRS.DSorterType, nil // in case the dsorter type is already set, we need to respect it
	}

	// Get memory stats from targets
	var (
		totalAvailMemory  uint64
		err               error
		path              = apc.URLPathDae.S
		moreThanThreshold = true
	)

	dsorterMemThreshold, err := cos.S2B(parsedRS.DSorterMemThreshold)
	debug.AssertNoErr(err)

	query := make(url.Values)
	query.Add(apc.QparamWhat, apc.GetWhatDaemonStatus)
	responses := broadcastTargets(http.MethodGet, path, query, nil, ctx.smapOwner.Get())
	for _, response := range responses {
		if response.err != nil {
			return "", response.err
		}

		daemonStatus := stats.DaemonStatus{}
		if err := jsoniter.Unmarshal(response.res, &daemonStatus); err != nil {
			return "", err
		}

		memStat := sys.MemStat{Total: daemonStatus.MemCPUInfo.MemAvail + daemonStatus.MemCPUInfo.MemUsed}
		dsortAvailMemory := calcMaxMemoryUsage(parsedRS.MaxMemUsage, &memStat)
		totalAvailMemory += dsortAvailMemory
		moreThanThreshold = moreThanThreshold && dsortAvailMemory > uint64(dsorterMemThreshold)
	}

	// TODO: currently, we have import cycle: dsort -> api -> dsort. Need to
	// think of a way to get the total size of bucket without copy-and-paste
	// the API code.
	//
	// baseParams := &api.BaseParams{
	// 	Client: http.DefaultClient,
	// 	URL:    ctx.smap.Get().Primary.URL(cmn.NetIntraControl),
	// }
	// msg := &apc.LsoMsg{Props: "size,status"}
	// objList, err := api.ListObjects(baseParams, parsedRS.Bucket, msg, 0)
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
	// 	return DSorterGeneralType, nil
	// }

	if moreThanThreshold {
		// If there is enough memory to use "memory type", we should do that.
		// It behaves better for cases when we have a lot of memory available.
		return DSorterMemType, nil
	}

	// For all other cases we should use "general type", as we don't know
	// exactly what to expect, so we should prepare for the worst.
	return DSorterGeneralType, nil
}
