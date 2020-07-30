// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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

func (p *proxyrunner) broadcastDownloadRequest(method, path string, body []byte, query url.Values) chan callResult {
	query.Add(cmn.URLParamProxyID, p.si.ID())
	query.Add(cmn.URLParamUnixTime, cmn.UnixNano2S(time.Now().UnixNano()))
	return p.callTargets(method, path, body, query)
}

func (p *proxyrunner) broadcastDownloadAdminRequest(method, path string, msg *downloader.DlAdminBody) ([]byte, int, error) {
	var (
		notFoundCnt int
		err         error
	)
	body := cmn.MustMarshal(msg)
	responses := p.broadcastDownloadRequest(method, path, body, url.Values{})
	respCnt := len(responses)
	cmn.Assert(respCnt > 0)
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
		err = resp.err
	}

	if notFoundCnt == respCnt { // all responded with 404
		return nil, http.StatusNotFound, err
	}

	switch method {
	case http.MethodGet:
		if msg.ID == "" {
			// If ID is empty, return the list of downloads
			aggregate := make(map[string]*downloader.DlJobInfo)
			for _, resp := range validResponses {
				var parsedResp map[string]*downloader.DlJobInfo
				err := jsoniter.Unmarshal(resp.bytes, &parsedResp)
				cmn.AssertNoErr(err)
				for k, v := range parsedResp {
					if oldMetric, ok := aggregate[k]; ok {
						v.Aggregate(oldMetric)
					}
					aggregate[k] = v
				}
			}

			listDownloads := make(downloader.DlJobInfos, 0, len(aggregate))
			for _, v := range aggregate {
				listDownloads = append(listDownloads, v)
			}
			result := cmn.MustMarshal(listDownloads)
			return result, http.StatusOK, nil
		}

		stats := make([]downloader.DlStatusResp, len(validResponses))
		for i, resp := range validResponses {
			err := jsoniter.Unmarshal(resp.bytes, &stats[i])
			cmn.AssertNoErr(err)
		}

		resp := stats[0]
		for i := 1; i < len(stats); i++ {
			resp.Aggregate(stats[i])
		}

		respJSON := cmn.MustMarshal(resp)
		return respJSON, http.StatusOK, nil
	case http.MethodDelete:
		res := validResponses[0]
		return res.bytes, res.status, res.err
	default:
		cmn.AssertMsg(false, method)
		return nil, http.StatusInternalServerError, nil
	}
}

func (p *proxyrunner) broadcastStartDownloadRequest(r *http.Request, id string, body []byte) (err error, errCode int) {
	query := r.URL.Query()
	query.Set(cmn.URLParamUUID, id)

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
	query := r.URL.Query()
	if err := p.checkPermissions(query, r.Header, nil, cmn.AccessDOWNLOAD); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
		return
	}
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
	if !p.ClusterStarted() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if err := cmn.ReadJSON(w, r, &payload); err != nil {
		return
	}
	if err := payload.Validate(r.Method == http.MethodDelete); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	if r.Method == http.MethodDelete {
		items, err := cmn.MatchRESTItems(r.URL.Path, 1, false, cmn.Version, cmn.Download)
		if err != nil {
			cmn.InvalidHandlerWithMsg(w, r, err.Error())
			return
		}

		if items[0] != cmn.Abort && items[0] != cmn.Remove {
			s := fmt.Sprintf("Invalid action for DELETE request: %s (expected either %s or %s).",
				items[0], cmn.Abort, cmn.Remove)
			cmn.InvalidHandlerWithMsg(w, r, s)
			return
		}
	}

	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("httpDownloadAdmin payload %v", payload)
	}

	resp, statusCode, err := p.broadcastDownloadAdminRequest(r.Method, r.URL.Path, payload)
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

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		p.invalmsghdlrstatusf(w, r, http.StatusInternalServerError, "Error starting download: %v.", err.Error())
		return
	}

	if ok := p.validateStartDownloadRequest(w, r, body); !ok {
		return
	}

	id := cmn.GenUUID()
	if err, errCode := p.broadcastStartDownloadRequest(r, id, body); err != nil {
		p.invalmsghdlrstatusf(w, r, errCode, "Error starting download: %v.", err.Error())
		return
	}

	p.respondWithID(w, id)
}

// Helper methods

func (p *proxyrunner) validateStartDownloadRequest(w http.ResponseWriter, r *http.Request, body []byte) (ok bool) {
	dlb := downloader.DlBody{}
	if err := jsoniter.Unmarshal(body, &dlb); err != nil {
		return
	}
	payload := downloader.DlBase{}
	err := jsoniter.Unmarshal(dlb.Data, &payload)
	if err != nil {
		return
	}
	bck := cluster.NewBckEmbed(payload.Bck)
	if err := bck.Init(p.owner.bmd, p.si); err != nil {
		args := remBckAddArgs{p: p, w: w, r: r, queryBck: bck, err: err}
		if bck, err = args.try(); err != nil {
			return
		}
	}
	if err := bck.Allow(cmn.AccessDOWNLOAD); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
		return
	}
	return true
}

func (p *proxyrunner) respondWithID(w http.ResponseWriter, id string) {
	resp := downloader.DlPostResp{
		ID: id,
	}

	w.Header().Set(cmn.HeaderContentType, cmn.ContentJSON)
	b := cmn.MustMarshal(resp)
	if _, err := w.Write(b); err != nil {
		glog.Errorf("Failed to write to http response: %v.", err)
	}
}
