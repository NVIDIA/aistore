// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ext/dload"
	"github.com/NVIDIA/aistore/nl"
	jsoniter "github.com/json-iterator/go"
)

// [METHOD] /v1/download
func (p *proxy) downloadHandler(w http.ResponseWriter, r *http.Request) {
	if !p.ClusterStarted() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet, http.MethodDelete:
		p.httpdladm(w, r)
	case http.MethodPost:
		p.httpdlpost(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodPost)
	}
}

// httpDownloadAdmin is meant for aborting, removing and getting status updates for downloads.
// GET /v1/download?id=...
// DELETE /v1/download/{abort, remove}?id=...
func (p *proxy) httpdladm(w http.ResponseWriter, r *http.Request) {
	if !p.ClusterStarted() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	msg := &dload.AdminBody{}
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}
	if err := msg.Validate(r.Method == http.MethodDelete); err != nil {
		p.writeErr(w, r, err)
		return
	}

	if r.Method == http.MethodDelete {
		items, err := cmn.ParseURL(r.URL.Path, apc.URLPathDownload.L, 1, false)
		if err != nil {
			p.writeErr(w, r, err)
			return
		}

		if items[0] != apc.Abort && items[0] != apc.Remove {
			p.writeErrAct(w, r, items[0])
			return
		}
	}
	if msg.ID != "" && p.ic.redirectToIC(w, r) {
		return
	}
	resp, statusCode, err := p.dladm(r.Method, r.URL.Path, msg)
	if err != nil {
		p.writeErr(w, r, err, statusCode)
	} else {
		w.Header().Set(cos.HdrContentLength, strconv.Itoa(len(resp)))
		w.Write(resp)
	}
}

// POST /v1/download
func (p *proxy) httpdlpost(w http.ResponseWriter, r *http.Request) {
	if _, err := p.parseURL(w, r, apc.URLPathDownload.L, 0, false); err != nil {
		return
	}

	jobID := dload.PrefixJobID + cos.GenUUID() // prefix to visually differentiate vs. xaction IDs

	body, err := io.ReadAll(r.Body)
	if err != nil {
		p.writeErrStatusf(w, r, http.StatusInternalServerError, "failed to receive download request: %v", err)
		return
	}
	dlb, dlBase, ok := p.validateDownload(w, r, body)
	if !ok {
		return
	}

	var progressInterval = dload.DownloadProgressInterval
	if dlBase.ProgressInterval != "" {
		ival, err := time.ParseDuration(dlBase.ProgressInterval)
		if err != nil {
			p.writeErrf(w, r, "%s: invalid progress interval %q: %v", p, dlBase.ProgressInterval, err)
			return
		}
		progressInterval = ival
	}

	xid := cos.GenUUID()
	if errCode, err := p.dlstart(r, xid, jobID, body); err != nil {
		p.writeErrStatusf(w, r, errCode, "Error starting download: %v", err)
		return
	}
	smap := p.owner.smap.get()
	nl := dload.NewDownloadNL(jobID, string(dlb.Type), &smap.Smap, progressInterval)
	nl.SetOwner(equalIC)
	p.ic.registerEqual(regIC{nl: nl, smap: smap})

	b := cos.MustMarshal(dload.DlPostResp{ID: jobID})
	w.Header().Set(cos.HdrContentType, cos.ContentJSON)
	w.Header().Set(cos.HdrContentLength, strconv.Itoa(len(b)))
	w.Write(b)
}

func (p *proxy) dladm(method, path string, msg *dload.AdminBody) ([]byte, int, error) {
	config := cmn.GCO.Get()
	if msg.ID != "" && method == http.MethodGet && msg.OnlyActive {
		nl := p.notifs.entry(msg.ID)
		if nl != nil {
			return p.dlstatus(nl, config)
		}
	}
	var (
		body        = cos.MustMarshal(msg)
		args        = allocBcArgs()
		xid         = cos.GenUUID()
		q           = url.Values{apc.QparamUUID: []string{xid}}
		notFoundCnt int
	)
	args.req = cmn.HreqArgs{Method: method, Path: path, Body: body, Query: q}
	args.timeout = config.Timeout.MaxHostBusy.D()
	results := p.bcastGroup(args)
	defer freeBcastRes(results)
	freeBcArgs(args)
	respCnt := len(results)
	if respCnt == 0 {
		smap := p.owner.smap.get()
		if smap.CountActiveTs() < 1 {
			return nil, http.StatusBadRequest, cmn.NewErrNoNodes(apc.Target, smap.CountTargets())
		}
		err := fmt.Errorf("%s: target(s) temporarily unavailable? (%s)", p, smap.StringEx())
		return nil, http.StatusInternalServerError, err
	}

	var (
		validResponses = make([]*callResult, 0, respCnt) // TODO: avoid allocation
		err            error
	)
	for _, res := range results {
		if res.status == http.StatusOK {
			validResponses = append(validResponses, res)
			continue
		}
		if res.status != http.StatusNotFound {
			return nil, res.status, res.err
		}
		notFoundCnt++
		err = res.err
	}
	if notFoundCnt == respCnt { // All responded with 404.
		return nil, http.StatusNotFound, err
	}

	switch method {
	case http.MethodGet:
		if msg.ID == "" {
			// If ID is empty, return the list of downloads
			aggregate := make(map[string]*dload.Job)
			for _, resp := range validResponses {
				if len(resp.bytes) == 0 {
					continue
				}
				var parsedResp map[string]*dload.Job
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

			listDownloads := make(dload.JobInfos, 0, len(aggregate))
			for _, v := range aggregate {
				listDownloads = append(listDownloads, v)
			}
			result := cos.MustMarshal(listDownloads)
			return result, http.StatusOK, nil
		}

		var stResp *dload.StatusResp
		for _, resp := range validResponses {
			status := dload.StatusResp{}
			if err := jsoniter.Unmarshal(resp.bytes, &status); err != nil {
				return nil, http.StatusInternalServerError, err
			}
			stResp = stResp.Aggregate(status)
		}
		body := cos.MustMarshal(stResp)
		return body, http.StatusOK, nil
	case http.MethodDelete:
		res := validResponses[0]
		return res.bytes, res.status, res.err
	default:
		debug.Assert(false, method)
		return nil, http.StatusInternalServerError, nil
	}
}

func (p *proxy) dlstatus(nl nl.Listener, config *cmn.Config) ([]byte, int, error) {
	// bcast
	p.notifs.bcastGetStats(nl, config.Periodic.NotifTime.D())
	stats := nl.NodeStats()

	var resp *dload.StatusResp
	stats.Range(func(_ string, status any) bool {
		var (
			dlStatus *dload.StatusResp
			ok       bool
		)
		if dlStatus, ok = status.(*dload.StatusResp); !ok {
			dlStatus = &dload.StatusResp{}
			if err := cos.MorphMarshal(status, dlStatus); err != nil {
				debug.AssertNoErr(err)
				return false
			}
		}
		resp = resp.Aggregate(*dlStatus)
		return true
	})

	respJSON := cos.MustMarshal(resp)
	return respJSON, http.StatusOK, nil
}

func (p *proxy) dlstart(r *http.Request, xid, jobID string, body []byte) (errCode int, err error) {
	var (
		config = cmn.GCO.Get()
		query  = make(url.Values, 2)
		args   = allocBcArgs()
	)
	query.Set(apc.QparamUUID, xid)
	query.Set(apc.QparamJobID, jobID)
	args.req = cmn.HreqArgs{Method: http.MethodPost, Path: r.URL.Path, Body: body, Query: query}
	args.timeout = config.Timeout.MaxHostBusy.D()

	results := p.bcastGroup(args)
	freeBcArgs(args)

	errCode = http.StatusOK
	for _, res := range results {
		if res.err != nil {
			errCode, err = res.status, res.err
			break
		}
	}
	freeBcastRes(results)
	return
}

func (p *proxy) validateDownload(w http.ResponseWriter, r *http.Request, body []byte) (dlb dload.Body, dlBase dload.Base, ok bool) {
	if err := jsoniter.Unmarshal(body, &dlb); err != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, p, "download request", cos.BHead(body), err)
		p.writeErr(w, r, err)
		return
	}
	if err := jsoniter.Unmarshal(dlb.RawMessage, &dlBase); err != nil {
		err = fmt.Errorf(cmn.FmtErrUnmarshal, p, "download message", cos.BHead(dlb.RawMessage), err)
		p.writeErr(w, r, err)
		return
	}
	bck := meta.CloneBck(&dlBase.Bck)
	args := bckInitArgs{p: p, w: w, r: r, reqBody: body, bck: bck, perms: apc.AccessRW}
	args.createAIS = true
	if _, err := args.initAndTry(); err == nil {
		ok = true
	}
	return
}
