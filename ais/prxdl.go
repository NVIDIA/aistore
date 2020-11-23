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
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/downloader"
	jsoniter "github.com/json-iterator/go"
)

func (p *proxyrunner) broadcastDownloadRequest(method, path string, body []byte, query url.Values) chan callResult {
	return p.bcastToGroup(bcastArgs{
		req:     cmn.ReqArgs{Method: method, Path: path, Body: body, Query: query},
		timeout: cmn.GCO.Get().Timeout.MaxKeepalive,
	})
}

func (p *proxyrunner) broadcastDownloadAdminRequest(method, path string, msg *downloader.DlAdminBody) ([]byte, int, error) {
	var (
		notFoundCnt int
		err         error
	)
	if msg.ID != "" && method == http.MethodGet && msg.OnlyActiveTasks {
		if stats, exists := p.notifs.queryStats(msg.ID); exists {
			var resp *downloader.DlStatusResp
			stats.Range(func(_ string, status interface{}) bool {
				var (
					dlStatus *downloader.DlStatusResp
					ok       bool
				)
				if dlStatus, ok = status.(*downloader.DlStatusResp); !ok {
					dlStatus = &downloader.DlStatusResp{}
					err := cmn.MorphMarshal(status, dlStatus)
					cmn.AssertNoErr(err)
				}

				resp = resp.Aggregate(*dlStatus)
				return true
			})

			respJSON := cmn.MustMarshal(resp)
			return respJSON, http.StatusOK, nil
		}
	}

	var (
		body      = cmn.MustMarshal(msg)
		responses = p.broadcastDownloadRequest(method, path, body, url.Values{})
		respCnt   = len(responses)
	)

	if respCnt == 0 {
		return nil, http.StatusBadRequest, cmn.NewNoNodesError(cmn.Target)
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
		err = resp.err
	}

	if notFoundCnt == respCnt { // All responded with 404.
		return nil, http.StatusNotFound, err
	}

	switch method {
	case http.MethodGet:
		if msg.ID == "" {
			// If ID is empty, return the list of downloads
			aggregate := make(map[string]*downloader.DlJobInfo)
			for _, resp := range validResponses {
				var parsedResp map[string]*downloader.DlJobInfo
				if err := jsoniter.Unmarshal(resp.bytes, &parsedResp); err != nil {
					return nil, http.StatusInternalServerError, err
				}
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

		var stResp *downloader.DlStatusResp
		for _, resp := range validResponses {
			status := downloader.DlStatusResp{}
			if err := jsoniter.Unmarshal(resp.bytes, &status); err != nil {
				return nil, http.StatusInternalServerError, err
			}
			stResp = stResp.Aggregate(status)
		}
		body := cmn.MustMarshal(stResp)
		return body, http.StatusOK, nil
	case http.MethodDelete:
		res := validResponses[0]
		return res.bytes, res.status, res.err
	default:
		cmn.AssertMsg(false, method)
		return nil, http.StatusInternalServerError, nil
	}
}

func (p *proxyrunner) broadcastStartDownloadRequest(r *http.Request, id string, body []byte) (errCode int, err error) {
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
		return http.StatusBadRequest, fmt.Errorf("following downloads failed: %v", failures)
	}

	return http.StatusOK, nil
}

// [METHOD] /v1/download
func (p *proxyrunner) downloadHandler(w http.ResponseWriter, r *http.Request) {
	if !p.ClusterStarted() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if err := p.checkPermissions(r.Header, nil, cmn.AccessDOWNLOAD); err != nil {
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
	payload := &downloader.DlAdminBody{}
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
	if payload.ID != "" && p.ic.redirectToIC(w, r) {
		return
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
	var (
		body             []byte
		dlb              downloader.DlBody
		dlBase           downloader.DlBase
		err              error
		ok               bool
		progressInterval = downloader.DownloadProgressInterval
	)

	if _, err = p.checkRESTItems(w, r, 0, false, cmn.Version, cmn.Download); err != nil {
		return
	}

	if body, err = ioutil.ReadAll(r.Body); err != nil {
		p.invalmsghdlrstatusf(w, r, http.StatusInternalServerError, "Error starting download: %v.", err.Error())
		return
	}

	if dlb, dlBase, ok = p.validateStartDownloadRequest(w, r, body); !ok {
		return
	}

	if dlBase.ProgressInterval != "" {
		if dur, err := time.ParseDuration(dlBase.ProgressInterval); err == nil {
			progressInterval = dur
		} else {
			p.invalmsghdlrf(w, r, "%s: invalid progress interval %q, err: %v", p.si, dlBase.ProgressInterval, err)
			return
		}
	}

	id := cmn.GenUUID()
	smap := p.owner.smap.get()

	if errCode, err := p.broadcastStartDownloadRequest(r, id, body); err != nil {
		p.invalmsghdlrstatusf(w, r, errCode, "Error starting download: %v.", err.Error())
		return
	}
	nl := downloader.NewDownloadNL(id, string(dlb.Type), &smap.Smap, progressInterval)
	nl.SetOwner(equalIC)
	p.ic.registerEqual(regIC{nl: nl, smap: smap})

	p.respondWithID(w, id)
}

// Helper methods

func (p *proxyrunner) validateStartDownloadRequest(w http.ResponseWriter, r *http.Request,
	body []byte) (dlb downloader.DlBody, dlBase downloader.DlBase, ok bool) {
	if err := jsoniter.Unmarshal(body, &dlb); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}

	err := jsoniter.Unmarshal(dlb.RawMessage, &dlBase)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	bck := cluster.NewBckEmbed(dlBase.Bck)
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
	ok = true
	return
}

func (p *proxyrunner) respondWithID(w http.ResponseWriter, id string) {
	w.Header().Set(cmn.HeaderContentType, cmn.ContentJSON)
	b := cmn.MustMarshal(downloader.DlPostResp{ID: id})
	_, err := w.Write(b)
	debug.AssertNoErr(err)
}
