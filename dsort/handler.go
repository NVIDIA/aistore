/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/dsort/extract"
	"github.com/google/uuid"
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
	apiItems, err := checkRESTItems(w, r, 1, cmn.Version, cmn.Sort)
	if err != nil {
		return
	}

	switch apiItems[0] {
	case cmn.Start:
		proxyStartSortHandler(w, r)
	case cmn.Metrics:
		proxyMetricsSortHandler(w, r)
	case cmn.Abort:
		proxyAbortSortHandler(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("invalid request %s", apiItems[0]))
	}
}

// POST /v1/sort/start
func proxyStartSortHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("invalid HTTP method: %s, must be POST", r.Method))
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
	b, err := js.Marshal(parsedRS)
	if err != nil {
		s := fmt.Sprintf("unable to marshal RequestSpec: %+v, err: %v", parsedRS, err)
		cmn.InvalidHandlerWithMsg(w, r, s, http.StatusInternalServerError)
		return
	}

	generatedUUID, err := uuid.NewRandom()
	managerUUID := generatedUUID.String()
	if err != nil {
		s := fmt.Sprintf("unable to create new uuid for manager: %v", err)
		cmn.InvalidHandlerWithMsg(w, r, s, http.StatusInternalServerError)
		return
	}

	checkResponses := func(responses []response) error {
		for _, resp := range responses {
			if resp.err != nil {
				glog.Errorf("[%s] start sort request failed to be broadcast, err: %s", managerUUID, resp.err.Error())

				path := cmn.URLPath(cmn.Version, cmn.Sort, cmn.Abort, managerUUID)
				broadcast(http.MethodDelete, path, nil, ctx.smap.Get().Tmap)

				s := fmt.Sprintf("failed to execute start sort, err: %s, status: %d", resp.err.Error(), resp.statusCode)
				cmn.InvalidHandlerWithMsg(w, r, s, http.StatusInternalServerError)
				return resp.err
			}
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
	// but because the are not ready with their initialization will not recognize
	// given dSort job. Also bug where we could send abort (which triggers cleanup)
	// to not yet initialized target.

	glog.V(4).Infof("[%s] broadcasting init request to all targets", managerUUID)
	path := cmn.URLPath(cmn.Version, cmn.Sort, cmn.Init, managerUUID)
	responses := broadcast(http.MethodPost, path, b, ctx.smap.Get().Tmap)
	if err := checkResponses(responses); err != nil {
		return
	}

	glog.V(4).Infof("[%s] broadcasting start request to all targets", managerUUID)
	path = cmn.URLPath(cmn.Version, cmn.Sort, cmn.Start, managerUUID)
	responses = broadcast(http.MethodPost, path, nil, ctx.smap.Get().Tmap)
	if err := checkResponses(responses); err != nil {
		return
	}

	w.Write([]byte(managerUUID))
}

// GET /v1/sort/metrics
func proxyMetricsSortHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("invalid HTTP method: %s, must be GET", r.Method))
		return
	}

	apiItems, err := checkRESTItems(w, r, 1, cmn.Version, cmn.Sort, cmn.Metrics)
	if err != nil {
		return
	}
	managerUUID := apiItems[0]
	path := cmn.URLPath(cmn.Version, cmn.Sort, cmn.Metrics, managerUUID)
	targets := ctx.smap.Get().Tmap
	responses := broadcast(http.MethodGet, path, nil, targets)

	allMetrics := make(map[string]*Metrics, len(targets))
	for _, resp := range responses {
		if resp.err != nil {
			cmn.InvalidHandlerWithMsg(w, r, resp.err.Error(), resp.statusCode)
			return
		}
		metrics := newMetrics()
		if err := js.Unmarshal(resp.res, &metrics); err != nil {
			cmn.InvalidHandlerWithMsg(w, r, err.Error(), http.StatusInternalServerError)
			return
		}
		allMetrics[resp.si.DaemonID] = metrics
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
	if r.Method != http.MethodDelete {
		s := fmt.Sprintf("invalid HTTP method: %s, must be DELETE", r.Method)
		cmn.InvalidHandlerWithMsg(w, r, s)
		return
	}
	apiItems, err := checkRESTItems(w, r, 1, cmn.Version, cmn.Sort, cmn.Abort)
	if err != nil {
		return
	}

	managerUUID := apiItems[0]
	path := cmn.URLPath(cmn.Version, cmn.Sort, cmn.Abort, managerUUID)
	broadcast(http.MethodDelete, path, nil, ctx.smap.Get().Tmap)
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
	case cmn.Metrics:
		metricsHandler(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid path")
	}
}

// initSortHandler is the handler called for the HTTP endpoint /v1/sort/init.
// It is responsible for initializing the dSort manager so it will be ready
// to start receiving requests.
func initSortHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("invalid method: %s", r.Method))
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
		cmn.InvalidHandlerWithMsg(w, r, err.Error(), http.StatusInternalServerError)
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
	if r.Method != http.MethodPost {
		cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("invalid method: %s", r.Method))
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
		glog.Error(err)

		// If we were aborted by some other process this means that we do not
		// broadcast abort (we assume that daemon aborted us, aborted also others).
		if !m.aborted() {
			glog.Warning("broadcasting abort to other targets")
			path := cmn.URLPath(cmn.Version, cmn.Sort, cmn.Abort, m.ManagerUUID)
			broadcast(http.MethodDelete, path, nil, ctx.smap.Get().Tmap)
		}
	}

	if err := m.start(); err != nil {
		errHandler(err)
		return
	}
}

// shardsHandler is the handler for the HTTP endpoint /v1/sort/shards.
// A valid POST to this endpoint results in a new shard being created locally based on the contents
// of the incoming request body. The shard is then sent to the correct target in the cluster as per HRW.
func shardsHandler(managers *ManagerGroup) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			s := fmt.Sprintf("invalid method: %s for %s, should be POST", r.Method, r.URL.String())
			cmn.InvalidHandlerWithMsg(w, r, s)
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
			cmn.InvalidHandlerWithMsg(w, r, "no dsort process in progress")
			return
		}
		if dsortManager.aborted() {
			cmn.InvalidHandlerWithMsg(w, r, "dsort process was aborted")
			return
		}

		decoder := js.NewDecoder(r.Body)
		if err := decoder.Decode(&dsortManager.shardManager.Shards); err != nil {
			cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("could not unmarshal request body, err: %v", err), http.StatusInternalServerError)
			return
		}
		dsortManager.startShardCreation <- struct{}{}
	}
}

// recordsHandler is the handler called for the HTTP endpoint /v1/sort/records.
// A valid POST to this endpoint updates this target's dsortManager.Records with the
// []Records from the request body, along with some related state variables.
func recordsHandler(managers *ManagerGroup) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			s := fmt.Sprintf("invalid method: %s to %s, should be POST", r.Method, r.URL.String())
			cmn.InvalidHandlerDetailed(w, r, s)
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
			cmn.InvalidHandlerWithMsg(w, r, "no dsort process in progress")
			return
		}
		if dsortManager.aborted() {
			cmn.InvalidHandlerWithMsg(w, r, "dsort process was aborted")
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
		dStr := r.URL.Query().Get(cmn.URLParamTotalInputShardsSeen)
		d, err := strconv.ParseUint(dStr, 10, 64)
		if err != nil {
			s := fmt.Sprintf("invalid %s in request to %s, err: %v", cmn.URLParamTotalInputShardsSeen, r.URL.String(), err)
			cmn.InvalidHandlerWithMsg(w, r, s)
			return
		}

		records := extract.NewRecords(int(d))
		decoder := js.NewDecoder(r.Body)
		if err := decoder.Decode(records); err != nil {
			cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf("could not unmarshal request body, err: %v", err), http.StatusInternalServerError)
			return
		}

		dsortManager.addCompressionSizes(compressed, uncompressed)
		dsortManager.addToTotalInputShardsSeen(d)
		dsortManager.recManager.EnqueueRecords(records)
		dsortManager.incrementReceived()
		glog.V(4).Infof("total times received records from another target: %d", dsortManager.received.count)
	}
}

// abortSortHandler is the handler called for the HTTP endpoint /v1/sort/abort.
// A valid DELETE to this endpoint aborts currently running sort job and cleans
// up the state.
func abortSortHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodDelete {
		s := fmt.Sprintf("invalid method: %s to %s, should be DELETE", r.Method, r.URL.String())
		cmn.InvalidHandlerWithMsg(w, r, s)
		return
	}
	apiItems, err := checkRESTItems(w, r, 1, cmn.Version, cmn.Sort, cmn.Abort)
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

	dsortManager.abort()
	// No need to perform cleanup since abort already scheduled one.
	go Managers.persist(managerUUID, false /*cleanup*/)
}

// metricsHandler is the handler called for the HTTP endpoint /v1/sort/metrics.
// A valid GET to this endpoint sends response with sort metrics.
func metricsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		s := fmt.Sprintf("invalid method: %s to %s, should be GET", r.Method, r.URL.String())
		cmn.InvalidHandlerWithMsg(w, r, s)
		return
	}
	apiItems, err := checkRESTItems(w, r, 1, cmn.Version, cmn.Sort, cmn.Metrics)
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

	dsortManager.Metrics.update()
	body, err := jsoniter.Marshal(dsortManager.Metrics)
	if err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error(), http.StatusInternalServerError)
		return
	}

	if _, err := w.Write(body); err != nil {
		glog.Error(err)
		// When we fail write we cannot call InvalidHandler since it will be
		// double header write.
		return
	}
}

func broadcast(method, path string, body []byte, nodes cluster.NodeMap) []response {
	client := http.DefaultClient
	responses := make([]response, len(nodes))

	wg := &sync.WaitGroup{}
	call := func(idx int, node *cluster.Snode) {
		defer wg.Done()

		var buffer io.Reader
		if body != nil {
			buffer = bytes.NewBuffer(body)
		}

		url := node.URL(cmn.NetworkIntraControl)
		req, err := http.NewRequest(method, url+path, buffer)
		if err != nil {
			responses[idx] = response{
				si:  node,
				err: err,
			}
			return
		}

		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, err := client.Do(req)
		if err != nil {
			responses[idx] = response{
				si:  node,
				err: err,
			}
			return
		}
		out, err := ioutil.ReadAll(resp.Body)

		responses[idx] = response{
			si:  node,
			res: out,
			err: err,
		}
	}

	idx := 0
	for _, node := range nodes {
		wg.Add(1)
		go call(idx, node)
		idx++
	}
	wg.Wait()

	return responses
}

func checkRESTItems(w http.ResponseWriter, r *http.Request, itemsAfter int, items ...string) ([]string, error) {
	items, err := cmn.MatchRESTItems(r.URL.Path, itemsAfter, true, items...)
	if err != nil {
		cmn.InvalidHandlerWithMsg(w, r, err.Error())
		return nil, err
	}

	return items, err
}
