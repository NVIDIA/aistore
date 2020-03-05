// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	jsoniter "github.com/json-iterator/go"
)

type dlResponse struct {
	body       []byte
	statusCode int
	err        error
}

func (p *proxyrunner) targetDownloadRequest(method string, path string, body []byte, query url.Values, si *cluster.Snode) dlResponse {
	fullQuery := url.Values{}
	for k, vs := range query {
		for _, v := range vs {
			fullQuery.Add(k, v)
		}
	}
	fullQuery.Add(cmn.URLParamProxyID, p.si.ID())
	fullQuery.Add(cmn.URLParamUnixTime, cmn.UnixNano2S(time.Now().UnixNano()))

	args := callArgs{
		si: si,
		req: cmn.ReqArgs{
			Method: method,
			Path:   cmn.URLPath(cmn.Version, cmn.Download, path),
			Query:  fullQuery,
			Body:   body,
		},
		timeout: cmn.DefaultTimeout,
	}
	res := p.call(args)
	return dlResponse{
		body:       res.outjson,
		statusCode: res.status,
		err:        res.err,
	}
}

func (p *proxyrunner) broadcastDownloadRequest(method string, path string, body []byte, query url.Values) []dlResponse {
	var (
		smap        = p.owner.smap.get()
		wg          = &sync.WaitGroup{}
		targetCnt   = smap.CountTargets()
		responsesCh = make(chan dlResponse, targetCnt)
	)

	for _, si := range smap.Tmap {
		wg.Add(1)
		go func(si *cluster.Snode) {
			responsesCh <- p.targetDownloadRequest(method, path, body, query, si)
			wg.Done()
		}(si)
	}

	wg.Wait()
	close(responsesCh)

	// FIXME: consider adding new stats: downloader failures
	responses := make([]dlResponse, 0, 10)
	for resp := range responsesCh {
		responses = append(responses, resp)
	}

	return responses
}

func (p *proxyrunner) broadcastDownloadAdminRequest(method string, path string, msg *cmn.DlAdminBody) ([]byte, int, error) {
	body := cmn.MustMarshal(msg)
	responses := p.broadcastDownloadRequest(method, path, body, url.Values{})
	if len(responses) == 0 {
		return nil, http.StatusInternalServerError, cluster.ErrNoTargets
	}

	notFoundCnt := 0
	errs := make([]dlResponse, 0, 10) // errors other than than 404 (not found)
	validResponses := responses[:0]
	for _, resp := range responses {
		if resp.statusCode >= http.StatusBadRequest {
			if resp.statusCode == http.StatusNotFound {
				notFoundCnt++
			} else {
				errs = append(errs, resp)
			}
		} else {
			validResponses = append(validResponses, resp)
		}
	}

	if notFoundCnt == len(responses) { // all responded with 404
		return nil, http.StatusNotFound, responses[0].err
	} else if len(errs) > 0 {
		return nil, errs[0].statusCode, errs[0].err
	}

	switch method {
	case http.MethodGet:
		if msg.ID == "" {
			// If ID is empty, return the list of downloads
			listDownloads := make(map[string]cmn.DlJobInfo)
			for _, resp := range validResponses {
				var parsedResp map[string]cmn.DlJobInfo
				err := jsoniter.Unmarshal(resp.body, &parsedResp)
				cmn.AssertNoErr(err)
				for k, v := range parsedResp {
					if oldMetric, ok := listDownloads[k]; ok {
						v.Aggregate(oldMetric)
					}
					listDownloads[k] = v
				}
			}

			result := cmn.MustMarshal(listDownloads)
			return result, http.StatusOK, nil
		}

		stats := make([]cmn.DlStatusResp, len(validResponses))
		for i, resp := range validResponses {
			err := jsoniter.Unmarshal(resp.body, &stats[i])
			cmn.AssertNoErr(err)
		}

		finished, total, numPending, scheduled := 0, 0, 0, 0
		allDispatchedCnt := 0
		aborted := false

		currTasks := make([]cmn.TaskDlInfo, 0, len(stats))
		finishedTasks := make([]cmn.TaskDlInfo, 0, len(stats))
		downloadErrs := make([]cmn.TaskErrInfo, 0)
		for _, stat := range stats {
			finished += stat.Finished
			total += stat.Total
			numPending += stat.Pending
			scheduled += stat.Scheduled

			aborted = aborted || stat.Aborted
			if stat.AllDispatched {
				allDispatchedCnt++
			}

			currTasks = append(currTasks, stat.CurrentTasks...)
			finishedTasks = append(finishedTasks, stat.FinishedTasks...)
			downloadErrs = append(downloadErrs, stat.Errs...)
		}

		resp := cmn.DlStatusResp{
			Finished:      finished,
			Total:         total,
			CurrentTasks:  currTasks,
			FinishedTasks: finishedTasks,
			Aborted:       aborted,
			Pending:       numPending,
			Errs:          downloadErrs,
			AllDispatched: allDispatchedCnt == len(stats),
			Scheduled:     scheduled,
		}

		respJSON := cmn.MustMarshal(resp)
		return respJSON, http.StatusOK, nil
	case http.MethodDelete:
		response := responses[0]
		return response.body, response.statusCode, response.err
	default:
		cmn.AssertMsg(false, method)
		return nil, http.StatusInternalServerError, nil
	}
}

func (p *proxyrunner) broadcastStartDownloadRequest(r *http.Request, id string) (err error, errCode int) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return err, http.StatusInternalServerError
	}
	query := r.URL.Query()
	query.Set(cmn.URLParamID, id)

	responses := p.broadcastDownloadRequest(http.MethodPost, r.URL.Path, body, query)

	failures := make([]error, 0, 10)
	for _, resp := range responses {
		if resp.err != nil {
			failures = append(failures, resp.err)
		}
	}

	if len(failures) > 0 {
		return fmt.Errorf("following downloads failed: %v", failures), http.StatusBadRequest
	}

	return nil, http.StatusOK
}

// [METHOD] /v1/download
func (p *proxyrunner) downloadHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet, http.MethodDelete:
		p.httpDownloadAdmin(w, r)
	case http.MethodPost:
		p.httpDownloadPost(w, r)
	default:
		s := fmt.Sprintf("invalid method %s for /download path; expected one of %s, %s, %s",
			r.Method, http.MethodGet, http.MethodDelete, http.MethodPost)
		cmn.InvalidHandlerWithMsg(w, r, s)
	}
}

// httpDownloadAdmin is meant for aborting, removing and getting status updates for downloads.
// GET /v1/download?id=...
// DELETE /v1/download/{abort, remove}?id=...
func (p *proxyrunner) httpDownloadAdmin(w http.ResponseWriter, r *http.Request) {
	var (
		payload = &cmn.DlAdminBody{}
	)

	payload.InitWithQuery(r.URL.Query())
	if err := payload.Validate(r.Method == http.MethodDelete); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	path := ""
	if r.Method == http.MethodDelete {
		items, err := cmn.MatchRESTItems(r.URL.Path, 1, false, cmn.Version, cmn.Download)
		if err != nil {
			cmn.InvalidHandlerWithMsg(w, r, err.Error())
			return
		}

		path = items[0]
		if path != cmn.Abort && path != cmn.Remove {
			s := fmt.Sprintf("Invalid action for DELETE request: %s (expected either %s or %s).",
				items[0], cmn.Abort, cmn.Remove)
			cmn.InvalidHandlerWithMsg(w, r, s)
			return
		}
	}

	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("httpDownloadAdmin payload %v", payload)
	}

	resp, statusCode, err := p.broadcastDownloadAdminRequest(r.Method, path, payload)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), statusCode)
		return
	}

	_, err = w.Write(resp)
	if err != nil {
		glog.Errorf("Failed to write to http response: %v.", err)
	}
}

// POST /v1/download
func (p *proxyrunner) httpDownloadPost(w http.ResponseWriter, r *http.Request) {
	if _, err := p.checkRESTItems(w, r, 0, false, cmn.Version, cmn.Download); err != nil {
		return
	}

	if ok := p.validateStartDownloadRequest(w, r); !ok {
		return
	}

	id := cmn.GenUserID()
	if err, errCode := p.broadcastStartDownloadRequest(r, id); err != nil {
		p.invalmsghdlr(w, r, fmt.Sprintf("Error starting download: %v.", err.Error()), errCode)
		return
	}

	p.respondWithID(w, id)
}

// Helper methods

func (p *proxyrunner) validateStartDownloadRequest(w http.ResponseWriter, r *http.Request) (ok bool) {
	var (
		bucket  string
		query   = r.URL.Query()
		payload = &cmn.DlBase{}
	)
	payload.InitWithQuery(query)
	bck := cluster.NewBckEmbed(payload.Bck)
	if err := bck.Init(p.owner.bmd, p.si); err != nil {
		if bck, err = p.syncCBmeta(w, r, bucket, err); err != nil {
			return
		}
	}
	if err := bck.AllowColdGET(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	return true
}

func (p *proxyrunner) respondWithID(w http.ResponseWriter, id string) {
	resp := cmn.DlPostResp{
		ID: id,
	}

	w.Header().Set("Content-Type", "application/json")
	b := cmn.MustMarshal(resp)
	if _, err := w.Write(b); err != nil {
		glog.Errorf("Failed to write to http response: %v.", err)
	}
}
