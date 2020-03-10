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
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/downloader"
	jsoniter "github.com/json-iterator/go"
)

func (p *proxyrunner) broadcastDownloadRequest(method string, path string, body []byte, query url.Values) chan callResult {
	query.Add(cmn.URLParamProxyID, p.si.ID())
	query.Add(cmn.URLParamUnixTime, cmn.UnixNano2S(time.Now().UnixNano()))
	args := bcastArgs{
		req: cmn.ReqArgs{
			Method: method,
			Path:   cmn.URLPath(cmn.Version, cmn.Download, path),
			Query:  query,
			Body:   body,
		},
		timeout: cmn.DefaultTimeout,
		to:      cluster.Targets,
		smap:    p.owner.smap.get(),
	}
	return p.bcastTo(args)
}

func (p *proxyrunner) broadcastDownloadAdminRequest(method string, path string, msg *downloader.DlAdminBody) ([]byte, int, error) {
	var (
		notFoundCnt int
		err         *callResult
	)
	body := cmn.MustMarshal(msg)
	responses := p.broadcastDownloadRequest(method, path, body, url.Values{})
	respCnt := len(responses)
	if respCnt == 0 {
		return nil, http.StatusInternalServerError, cluster.ErrNoTargets
	}

	validResponses := make([]callResult, 0, respCnt)
	for resp := range responses {
		if resp.status == http.StatusOK {
			validResponses = append(validResponses, resp)
			continue
		}
		if resp.status != http.StatusNotFound {
			return nil, resp.status, resp.err
		}
		notFoundCnt++
		err = &resp
	}

	if notFoundCnt == respCnt { // all responded with 404
		return nil, http.StatusNotFound, err.err
	}

	switch method {
	case http.MethodGet:
		if msg.ID == "" {
			// If ID is empty, return the list of downloads
			listDownloads := make(map[string]downloader.DlJobInfo)
			for _, resp := range validResponses {
				var parsedResp map[string]downloader.DlJobInfo
				err := jsoniter.Unmarshal(resp.outjson, &parsedResp)
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

		stats := make([]downloader.DlStatusResp, len(validResponses))
		for i, resp := range validResponses {
			err := jsoniter.Unmarshal(resp.outjson, &stats[i])
			cmn.AssertNoErr(err)
		}

		finished, total, numPending, scheduled := 0, 0, 0, 0
		allDispatchedCnt := 0
		aborted := false

		currTasks := make([]downloader.TaskDlInfo, 0, len(stats))
		finishedTasks := make([]downloader.TaskDlInfo, 0, len(stats))
		downloadErrs := make([]downloader.TaskErrInfo, 0)
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

		resp := downloader.DlStatusResp{
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
		response := validResponses[0]
		return response.outjson, response.status, response.err
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
	failures := make([]error, 0, len(responses))
	for resp := range responses {
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
		payload = &downloader.DlAdminBody{}
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
		payload = &downloader.DlBase{}
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
	resp := downloader.DlPostResp{
		ID: id,
	}

	w.Header().Set("Content-Type", "application/json")
	b := cmn.MustMarshal(resp)
	if _, err := w.Write(b); err != nil {
		glog.Errorf("Failed to write to http response: %v.", err)
	}
}
