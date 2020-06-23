// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dsort

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tinylib/msgp/msgp"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/dsort/extract"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	jsoniter "github.com/json-iterator/go"
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

// [METHOD] /v1/sort/...
func ProxySortHandler(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 0, cmn.Version, cmn.Sort)
	if err != nil {
		return
	}

	switch r.Method {
	case http.MethodPost:
		proxyStartSortHandler(w, r)
	case http.MethodGet:
		proxyGetHandler(w, r)
	case http.MethodDelete:
		if len(apiItems) == 1 && apiItems[0] == cmn.Abort {
			proxyAbortSortHandler(w, r)
		} else if len(apiItems) == 0 {
			proxyRemoveSortHandler(w, r)
		} else {
			cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("invalid request %s", apiItems[0]))
		}
	default:
		cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("invalid request %s", apiItems[0]))
	}
}

// POST /v1/sort
func proxyStartSortHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodPost) {
		return
	}
	rs := &RequestSpec{}
	if cmn.ReadJSON(w, r, &rs) != nil {
		return
	}
	parsedRS, err := rs.Parse()
	if err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error())
		return
	}
	parsedRS.TargetOrderSalt = []byte(time.Now().Format("15:04:05.000000"))

	// TODO: handle case when bucket was removed during dSort job - this should
	// stop whole operation. Maybe some listeners as we have on smap change?
	// This would also be helpful for Downloader (in the middle of downloading
	// large file the bucket can be easily deleted).

	bck := cluster.NewBck(parsedRS.Bucket, parsedRS.Provider, cmn.NsGlobal)
	if err = bck.Init(ctx.bmdOwner, nil); err != nil { // TODO: ctx.t.Snode()
		cmn.InvalidHandlerWithMsg(w, r, err.Error())
		return
	}
	if err := bck.Allow(cmn.AccessObjLIST); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error(), http.StatusForbidden)
		return
	}
	if err := bck.Allow(cmn.AccessGET); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error(), http.StatusForbidden)
		return
	}

	bck = cluster.NewBck(parsedRS.OutputBucket, parsedRS.OutputProvider, cmn.NsGlobal)
	if err = bck.Init(ctx.bmdOwner, nil); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error())
		return
	}
	if err = bck.Allow(cmn.AccessPUT); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error(), http.StatusForbidden)
		return
	}

	parsedRS.DSorterType, err = determineDSorterType(parsedRS)
	if err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error())
		return
	}

	b, err := js.Marshal(parsedRS)
	if err != nil {
		s := fmt.Sprintf("unable to marshal RequestSpec: %+v, err: %v", parsedRS, err)
		cmn.InvalidHandlerWithMsg(w, r, s, http.StatusInternalServerError)
		return
	}

	managerUUID := cmn.GenUUID()
	checkResponses := func(responses []response) error {
		for _, resp := range responses {
			if resp.err == nil {
				continue
			}

			glog.Errorf("[%s] start sort request failed to be broadcast, err: %s", managerUUID, resp.err.Error())

			path := cmn.URLPath(cmn.Version, cmn.Sort, cmn.Abort, managerUUID)
			broadcast(http.MethodDelete, path, nil, nil, ctx.smapOwner.Get().Tmap)

			s := fmt.Sprintf("failed to execute start sort, err: %s, status: %d", resp.err.Error(), resp.statusCode)
			cmn.InvalidHandlerWithMsg(w, r, s, http.StatusInternalServerError)
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

	glog.V(4).Infof("[%s] broadcasting init request to all targets", managerUUID)
	path := cmn.URLPath(cmn.Version, cmn.Sort, cmn.Init, managerUUID)
	responses := broadcast(http.MethodPost, path, nil, b, ctx.smapOwner.Get().Tmap)
	if err := checkResponses(responses); err != nil {
		return
	}

	glog.V(4).Infof("[%s] broadcasting start request to all targets", managerUUID)
	path = cmn.URLPath(cmn.Version, cmn.Sort, cmn.Start, managerUUID)
	responses = broadcast(http.MethodPost, path, nil, nil, ctx.smapOwner.Get().Tmap)
	if err := checkResponses(responses); err != nil {
		return
	}

	w.Write([]byte(managerUUID))
}

// GET /v1/sort
func proxyGetHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodGet) {
		return
	}

	query := r.URL.Query()
	managerUUID := query.Get(cmn.URLParamUUID)

	if managerUUID == "" {
		proxyListSortHandler(w, r)
		return
	}

	proxyMetricsSortHandler(w, r)
}

// GET /v1/sort?regex=...
func proxyListSortHandler(w http.ResponseWriter, r *http.Request) {
	regexStr := r.URL.Query().Get(cmn.URLParamRegex)
	if _, err := regexp.CompilePOSIX(regexStr); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error())
		return
	}

	targets := ctx.smapOwner.Get().Tmap
	path := cmn.URLPath(cmn.Version, cmn.Sort, cmn.List)
	responses := broadcast(http.MethodGet, path, r.URL.Query(), nil, targets)

	resultList := make([]*JobInfo, 0)
	for _, r := range responses {
		if r.err != nil {
			glog.Error(r.err)
			continue
		}
		var newMetrics []*JobInfo
		err := jsoniter.Unmarshal(r.res, &newMetrics)
		cmn.AssertNoErr(err)

		for _, v := range newMetrics {
			found := false
			for _, oldMetric := range resultList {
				if oldMetric.ID == v.ID {
					oldMetric.Aggregate(v)
					found = true
					break
				}
			}

			if !found {
				resultList = append(resultList, v)
			}
		}
	}

	body := cmn.MustMarshal(resultList)
	if _, err := w.Write(body); err != nil {
		glog.Error(err)
		// When we fail write we cannot call InvalidHandler since it will be
		// double header write.
		return
	}
}

// GET /v1/sort?id=...
func proxyMetricsSortHandler(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	managerUUID := query.Get(cmn.URLParamUUID)
	path := cmn.URLPath(cmn.Version, cmn.Sort, cmn.Metrics, managerUUID)
	targets := ctx.smapOwner.Get().Tmap
	responses := broadcast(http.MethodGet, path, nil, nil, targets)

	notFound := 0
	allMetrics := make(map[string]*Metrics, len(targets))
	for _, resp := range responses {
		if resp.statusCode == http.StatusNotFound {
			// Probably new target which does not know anything about this dsort op.
			notFound++
			continue
		}
		if resp.err != nil {
			cmn.InvalidHandlerWithMsg(w, r, resp.err.Error(), resp.statusCode)
			return
		}
		metrics := &Metrics{}
		if err := js.Unmarshal(resp.res, &metrics); err != nil {
			cmn.InvalidHandlerWithMsg(w, r, err.Error(), http.StatusInternalServerError)
			return
		}
		allMetrics[resp.si.DaemonID] = metrics
	}

	if notFound == len(responses) && notFound > 0 {
		msg := fmt.Sprintf("%s job %q not found", cmn.DSortName, managerUUID)
		cmn.InvalidHandlerWithMsg(w, r, msg, http.StatusNotFound)
		return
	}

	body, err := js.Marshal(allMetrics)
	if err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write(body)
}

// DELETE /v1/sort/abort
func proxyAbortSortHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodDelete) {
		return
	}
	_, err := checkRESTItems(w, r, 0, cmn.Version, cmn.Sort, cmn.Abort)
	if err != nil {
		return
	}

	query := r.URL.Query()
	managerUUID := query.Get(cmn.URLParamUUID)
	path := cmn.URLPath(cmn.Version, cmn.Sort, cmn.Abort, managerUUID)
	responses := broadcast(http.MethodDelete, path, nil, nil, ctx.smapOwner.Get().Tmap)

	allNotFound := true
	for _, resp := range responses {
		if resp.statusCode == http.StatusNotFound {
			continue
		}
		allNotFound = false

		if resp.err != nil {
			cmn.InvalidHandlerDetailed(w, r, resp.err.Error(), resp.statusCode)
			return
		}
	}
	if allNotFound {
		msg := fmt.Sprintf("%s job %q not found", cmn.DSortName, managerUUID)
		cmn.InvalidHandlerDetailed(w, r, msg, http.StatusNotFound)
		return
	}
}

// DELETE /v1/sort
func proxyRemoveSortHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodDelete) {
		return
	}
	_, err := checkRESTItems(w, r, 0, cmn.Version, cmn.Sort)
	if err != nil {
		return
	}

	query := r.URL.Query()
	managerUUID := query.Get(cmn.URLParamUUID)
	targets := ctx.smapOwner.Get().Tmap

	// First, broadcast to see if process is cleaned up first
	path := cmn.URLPath(cmn.Version, cmn.Sort, cmn.Metrics, managerUUID)
	responses := broadcast(http.MethodGet, path, nil, nil, targets)
	seenOne := false
	for _, resp := range responses {
		if resp.statusCode == http.StatusNotFound {
			// Probably new target which does not know anything about this dsort op.
			continue
		}
		if resp.err != nil {
			cmn.InvalidHandlerWithMsg(w, r, resp.err.Error(), resp.statusCode)
			return
		}
		metrics := &Metrics{}
		if err := js.Unmarshal(resp.res, &metrics); err != nil {
			cmn.InvalidHandlerWithMsg(w, r, err.Error(), http.StatusInternalServerError)
			return
		}
		if !metrics.Archived.Load() {
			cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("%s process %s still in progress and cannot be removed", cmn.DSortName, managerUUID))
			return
		}
		seenOne = true
	}
	if !seenOne {
		s := fmt.Sprintf("invalid request: manager with uuid %s does not exist", managerUUID)
		cmn.InvalidHandlerWithMsg(w, r, s, http.StatusNotFound)
		return
	}

	// Next, broadcast the remove once we've checked that all targets have run cleanup
	path = cmn.URLPath(cmn.Version, cmn.Sort, cmn.Remove, managerUUID)
	responses = broadcast(http.MethodDelete, path, nil, nil, targets, ctx.node)
	failed := make([]string, 0)
	for _, r := range responses {
		if r.statusCode != http.StatusOK {
			failed = append(failed, fmt.Sprintf("%v: (%v) %v", r.si.DaemonID, r.statusCode, string(r.res)))
		}
	}

	if len(failed) != 0 {
		cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("Got errors while broadcasting remove: %v", strings.Join(failed, ",")))
	}
}

///////////////////
///// TARGET //////
///////////////////

// SortHandler is the handler called for the HTTP endpoint /v1/sort.
func SortHandler(w http.ResponseWriter, r *http.Request) {
	apiItems, err := checkRESTItems(w, r, 1, cmn.Version, cmn.Sort)
	if err != nil {
		return
	}

	switch apiItems[0] {
	case cmn.Init:
		initSortHandler(w, r)
	case cmn.Start:
		startSortHandler(w, r)
	case cmn.Records:
		recordsHandler(Managers)(w, r)
	case cmn.Shards:
		shardsHandler(Managers)(w, r)
	case cmn.Abort:
		abortSortHandler(w, r)
	case cmn.Remove:
		removeSortHandler(w, r)
	case cmn.List:
		listSortHandler(w, r)
	case cmn.Metrics:
		metricsHandler(w, r)
	case cmn.FinishedAck:
		finishedAckHandler(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid path")
	}
}

// initSortHandler is the handler called for the HTTP endpoint /v1/sort/init.
// It is responsible for initializing the dSort manager so it will be ready
// to start receiving requests.
func initSortHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodPost) {
		return
	}
	apiItems, err := checkRESTItems(w, r, 1, cmn.Version, cmn.Sort, cmn.Init)
	if err != nil {
		return
	}
	var rs *ParsedRequestSpec
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("could not read request body, err: %v", err))
		return
	}
	if err = js.Unmarshal(b, &rs); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("could not unmarshal request body, err: %v", err))
		return
	}

	managerUUID := apiItems[0]
	dsortManager, err := Managers.Add(managerUUID)
	if err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error())
		return
	}
	defer dsortManager.unlock()
	if err = dsortManager.init(rs); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error())
		return
	}
}

// startSortHandler is the handler called for the HTTP endpoint /v1/sort/start.
// There are three major phases to this function:
//
// 1. extractLocalShards
// 2. participateInRecordDistribution
// 3. distributeShardRecords
func startSortHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodPost) {
		return
	}
	apiItems, err := checkRESTItems(w, r, 1, cmn.Version, cmn.Sort, cmn.Start)
	if err != nil {
		return
	}

	managerUUID := apiItems[0]
	dsortManager, exists := Managers.Get(managerUUID)
	if !exists {
		s := fmt.Sprintf("invalid request: manager with uuid %s does not exist", managerUUID)
		cmn.InvalidHandlerWithMsg(w, r, s, http.StatusNotFound)
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
			path := cmn.URLPath(cmn.Version, cmn.Sort, cmn.Abort, m.ManagerUUID)
			broadcast(http.MethodDelete, path, nil, nil, ctx.smapOwner.Get().Tmap, ctx.node)
		}
	}

	if err := m.start(); err != nil {
		errHandler(err)
		return
	}

	glog.Info("broadcasting finished ack to other targets")
	path := cmn.URLPath(cmn.Version, cmn.Sort, cmn.FinishedAck, m.ManagerUUID, m.ctx.node.DaemonID)
	broadcast(http.MethodPut, path, nil, nil, ctx.smapOwner.Get().Tmap, ctx.node)
}

// shardsHandler is the handler for the HTTP endpoint /v1/sort/shards.
// A valid POST to this endpoint results in a new shard being created locally based on the contents
// of the incoming request body. The shard is then sent to the correct target in the cluster as per HRW.
func shardsHandler(managers *ManagerGroup) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !checkHTTPMethod(w, r, http.MethodPost) {
			return
		}
		apiItems, err := checkRESTItems(w, r, 1, cmn.Version, cmn.Sort, cmn.Shards)
		if err != nil {
			return
		}
		managerUUID := apiItems[0]
		dsortManager, exists := managers.Get(managerUUID)
		if !exists {
			s := fmt.Sprintf("invalid request: manager with uuid %s does not exist", managerUUID)
			cmn.InvalidHandlerWithMsg(w, r, s, http.StatusNotFound)
			return
		}

		if !dsortManager.inProgress() {
			cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("no %s process in progress", cmn.DSortName))
			return
		}
		if dsortManager.aborted() {
			cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("%s process was aborted", cmn.DSortName))
			return
		}

		tmpMetadata := &CreationPhaseMetadata{}
		if err := tmpMetadata.DecodeMsg(msgp.NewReaderSize(r.Body, serializationBufSize)); err != nil {
			cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("could not unmarshal request body, err: %v", err), http.StatusInternalServerError)
			return
		}

		if !dsortManager.inProgress() || dsortManager.aborted() {
			cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("no %s process", cmn.DSortName))
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
		apiItems, err := checkRESTItems(w, r, 1, cmn.Version, cmn.Sort, cmn.Records)
		if err != nil {
			return
		}
		managerUUID := apiItems[0]
		dsortManager, exists := managers.Get(managerUUID)
		if !exists {
			s := fmt.Sprintf("invalid request: manager with uuid %s does not exist", managerUUID)
			cmn.InvalidHandlerWithMsg(w, r, s, http.StatusNotFound)
			return
		}
		if !dsortManager.inProgress() {
			cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("no %s process in progress", cmn.DSortName))
			return
		}
		if dsortManager.aborted() {
			cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("%s process was aborted", cmn.DSortName))
			return
		}
		compStr := r.URL.Query().Get(cmn.URLParamTotalCompressedSize)
		compressed, err := strconv.ParseInt(compStr, 10, 64)
		if err != nil {
			s := fmt.Sprintf("invalid %s in request to %s, err: %v", cmn.URLParamTotalCompressedSize, r.URL.String(), err)
			cmn.InvalidHandlerWithMsg(w, r, s)
			return
		}
		uncompStr := r.URL.Query().Get(cmn.URLParamTotalUncompressedSize)
		uncompressed, err := strconv.ParseInt(uncompStr, 10, 64)
		if err != nil {
			s := fmt.Sprintf("invalid %s in request to %s, err: %v", cmn.URLParamTotalUncompressedSize, r.URL.String(), err)
			cmn.InvalidHandlerWithMsg(w, r, s)
			return
		}
		dStr := r.URL.Query().Get(cmn.URLParamTotalInputShardsExtracted)
		d, err := strconv.ParseUint(dStr, 10, 64)
		if err != nil {
			s := fmt.Sprintf("invalid %s in request to %s, err: %v", cmn.URLParamTotalInputShardsExtracted, r.URL.String(), err)
			cmn.InvalidHandlerWithMsg(w, r, s)
			return
		}

		records := extract.NewRecords(int(d))
		if err := records.DecodeMsg(msgp.NewReaderSize(r.Body, serializationBufSize)); err != nil {
			cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("could not unmarshal request body, err: %v", err), http.StatusInternalServerError)
			return
		}

		dsortManager.addCompressionSizes(compressed, uncompressed)
		dsortManager.recManager.EnqueueRecords(records)
		dsortManager.incrementReceived()
		glog.V(4).Infof("total times received records from another target: %d", dsortManager.received.count.Load())
	}
}

// abortSortHandler is the handler called for the HTTP endpoint /v1/sort/abort.
// A valid DELETE to this endpoint aborts currently running sort job and cleans
// up the state.
func abortSortHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodDelete) {
		return
	}
	apiItems, err := checkRESTItems(w, r, 1, cmn.Version, cmn.Sort, cmn.Abort)
	if err != nil {
		return
	}

	managerUUID := apiItems[0]
	dsortManager, exists := Managers.Get(managerUUID, true /*allowPersisted*/)
	if !exists {
		s := fmt.Sprintf("invalid request: manager with uuid %s does not exist", managerUUID)
		cmn.InvalidHandlerWithMsg(w, r, s, http.StatusNotFound)
		return
	}
	if dsortManager.Metrics.Archived.Load() {
		s := fmt.Sprintf("invalid request: %s job with uuid %s has already finished", cmn.DSortName, managerUUID)
		cmn.InvalidHandlerWithMsg(w, r, s, http.StatusGone)
		return
	}

	dsortManager.abort(fmt.Errorf("%s has been aborted via API (remotely)", cmn.DSortName))
}

func removeSortHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodDelete) {
		return
	}
	apiItems, err := checkRESTItems(w, r, 1, cmn.Version, cmn.Sort, cmn.Remove)
	if err != nil {
		return
	}

	managerUUID := apiItems[0]
	if err := Managers.Remove(managerUUID); err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error())
		return
	}
}

func listSortHandler(w http.ResponseWriter, r *http.Request) {
	if !checkHTTPMethod(w, r, http.MethodGet) {
		return
	}

	// Fetch regex
	regexStr := r.URL.Query().Get(cmn.URLParamRegex)
	var regex *regexp.Regexp
	if regexStr != "" {
		var err error
		if regex, err = regexp.CompilePOSIX(regexStr); err != nil {
			cmn.InvalidHandlerWithMsg(w, r, err.Error())
			return
		}
	}

	body := cmn.MustMarshal(Managers.List(regex))
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
	apiItems, err := checkRESTItems(w, r, 1, cmn.Version, cmn.Sort, cmn.Metrics)
	if err != nil {
		return
	}

	managerUUID := apiItems[0]
	dsortManager, exists := Managers.Get(managerUUID, true /*allowPersisted*/)
	if !exists {
		s := fmt.Sprintf("invalid request: manager with uuid %s does not exist", managerUUID)
		cmn.InvalidHandlerWithMsg(w, r, s, http.StatusNotFound)
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
	apiItems, err := checkRESTItems(w, r, 2, cmn.Version, cmn.Sort, cmn.FinishedAck)
	if err != nil {
		return
	}

	managerUUID, daemonID := apiItems[0], apiItems[1]
	dsortManager, exists := Managers.Get(managerUUID)
	if !exists {
		s := fmt.Sprintf("invalid request: manager with uuid %s does not exist", managerUUID)
		cmn.InvalidHandlerWithMsg(w, r, s, http.StatusNotFound)
		return
	}

	dsortManager.updateFinishedAck(daemonID)
}

func broadcast(method, path string, urlParams url.Values, body []byte, nodes cluster.NodeMap, ignore ...*cluster.Snode) []response {
	config := cmn.GCO.Get()
	client := cmn.NewClient(cmn.TransportArgs{
		Timeout:    config.Client.Timeout,
		UseHTTPS:   config.Net.HTTP.UseHTTPS,
		SkipVerify: config.Net.HTTP.SkipVerify,
	})
	responses := make([]response, len(nodes))

	wg := &sync.WaitGroup{}
	call := func(idx int, node *cluster.Snode) {
		defer wg.Done()

		reqArgs := cmn.ReqArgs{
			Method: method,
			Base:   node.URL(cmn.NetworkIntraControl),
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

		resp, err := client.Do(req)
		if err != nil {
			responses[idx] = response{
				si:         node,
				err:        err,
				statusCode: http.StatusInternalServerError,
			}
			return
		}
		out, err := ioutil.ReadAll(resp.Body)
		debug.AssertNoErr(resp.Body.Close())

		responses[idx] = response{
			si:         node,
			res:        out,
			err:        err,
			statusCode: resp.StatusCode,
		}
	}

	idx := 0
outer:
	for _, node := range nodes {
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

	return responses
}

func checkHTTPMethod(w http.ResponseWriter, r *http.Request, expected string) bool {
	if r.Method != expected {
		s := fmt.Sprintf("invalid method: %s to %s, should be %s", r.Method, r.URL.String(), expected)
		cmn.InvalidHandlerWithMsg(w, r, s)
		return false
	}
	return true
}

func checkRESTItems(w http.ResponseWriter, r *http.Request, itemsAfter int, items ...string) ([]string, error) {
	items, err := cmn.MatchRESTItems(r.URL.Path, itemsAfter, true, items...)
	if err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error())
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
		err  error
		path = cmn.URLPath(cmn.Version, cmn.Daemon)

		totalAvailMemory  = uint64(0)
		moreThanThreshold = true
	)

	dsorterMemThreshold, err := cmn.S2B(parsedRS.DSorterMemThreshold)
	cmn.AssertNoErr(err)

	query := make(url.Values)
	query.Add(cmn.URLParamWhat, cmn.GetWhatDaemonStatus)
	responses := broadcast(http.MethodGet, path, query, nil, ctx.smapOwner.Get().Tmap)
	for _, response := range responses {
		if response.err != nil {
			return "", response.err
		}

		daemonStatus := stats.DaemonStatus{}
		if err := jsoniter.Unmarshal(response.res, &daemonStatus); err != nil {
			return "", err
		}

		memStat := sys.MemStat{Total: daemonStatus.SysInfo.MemAvail + daemonStatus.SysInfo.MemUsed}
		dsortAvailMemory := calcMaxMemoryUsage(parsedRS.MaxMemUsage, memStat)
		totalAvailMemory += dsortAvailMemory
		moreThanThreshold = moreThanThreshold && dsortAvailMemory > uint64(dsorterMemThreshold)
	}

	// TODO: currently we have import cycle: dsort -> api -> dsort. Need to
	// think of a way to get the total size of bucket without copy-and-paste
	// the API code.
	//
	// baseParams := &api.BaseParams{
	// 	Client: http.DefaultClient,
	// 	URL:    ctx.smap.Get().ProxySI.URL(cmn.NetworkIntraControl),
	// }
	// msg := &cmn.SelectMsg{Props: "size,status", PageSize: cmn.DefaultListPageSize}
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
