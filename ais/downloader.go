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
	"regexp"
	"strconv"
	"strings"
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

// Removes everything that goes after '?', eg. "?query=key..." so it will not
// be part of final object name.
func normalizeObjName(objName string) string {
	idx := strings.IndexByte(objName, '?')
	if idx < 0 {
		return objName
	}
	return objName[:idx]
}

///////////
// PROXY //
///////////

func (p *proxyrunner) targetDownloadRequest(method string, si *cluster.Snode, msg interface{}) dlResponse {
	query := url.Values{}
	query.Add(cmn.URLParamProxyID, p.si.DaemonID)
	query.Add(cmn.URLParamUnixTime, strconv.FormatInt(int64(time.Now().UnixNano()), 10))

	body, err := jsoniter.Marshal(msg)
	if err != nil {
		return dlResponse{
			body:       nil,
			statusCode: http.StatusInternalServerError,
			err:        err,
		}
	}
	args := callArgs{
		si: si,
		req: reqArgs{
			method: method,
			path:   cmn.URLPath(cmn.Version, cmn.Download),
			query:  query,
			body:   body,
		},
		timeout: defaultTimeout,
	}
	res := p.call(args)
	return dlResponse{
		body:       res.outjson,
		statusCode: res.status,
		err:        res.err,
	}
}

func (p *proxyrunner) broadcastDownloadRequest(method string, msg *cmn.DlAdminBody) ([]byte, int, error) {
	var (
		smap        = p.smapowner.get()
		wg          = &sync.WaitGroup{}
		targetCnt   = len(smap.Tmap)
		responsesCh = make(chan dlResponse, targetCnt)
	)

	for _, si := range smap.Tmap {
		wg.Add(1)
		go func(si *cluster.Snode) {
			responsesCh <- p.targetDownloadRequest(method, si, msg)
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
					//FIXME: add aggregation when more stats added to DlJobInfo
					listDownloads[k] = v
				}
			}

			result, err := jsoniter.Marshal(listDownloads)
			cmn.AssertNoErr(err)
			return result, http.StatusOK, nil
		}

		stats := make([]cmn.DlStatusResp, len(validResponses))
		for i, resp := range validResponses {
			err := jsoniter.Unmarshal(resp.body, &stats[i])
			cmn.AssertNoErr(err)
		}

		finished, total := 0, 0
		currTasks := make([]cmn.TaskDlInfo, 0, len(stats))
		downloadErrs := make([]cmn.TaskErrInfo, 0)
		for _, stat := range stats {
			finished += stat.Finished
			total += stat.Total
			currTasks = append(currTasks, stat.CurrentTasks...)
			downloadErrs = append(downloadErrs, stat.Errs...)
		}
		pct := float64(finished) / float64(total) * 100

		resp := cmn.DlStatusResp{
			Finished:     finished,
			Total:        total,
			Percentage:   pct,
			CurrentTasks: currTasks,
			Errs:         downloadErrs,
		}

		respJSON, err := jsoniter.Marshal(resp)
		if err != nil {
			return nil, http.StatusInternalServerError, err
		}

		return respJSON, http.StatusOK, nil
	case http.MethodDelete:
		response := responses[0]
		return response.body, response.statusCode, response.err
	default:
		cmn.AssertMsg(false, method)
		return nil, http.StatusInternalServerError, nil
	}
}

// objects is a map of objnames (keys) where the corresponding
// value is the link that the download will be saved as.
func (p *proxyrunner) bulkDownloadProcessor(id string, payload *cmn.DlBase, objects cmn.SimpleKVs, cloud bool) error {
	var (
		smap  = p.smapowner.get()
		wg    = &sync.WaitGroup{}
		errCh chan error
	)

	bulkTargetRequest := make(map[*cluster.Snode]*cmn.DlBody, smap.CountTargets())
	for objName, link := range objects {
		// Make sure that objName doesn't contain "?query=smth" suffix.
		objName = normalizeObjName(objName)
		// Make sure that link contains protocol (absence of protocol can result in errors).
		link = cmn.PrependProtocol(link)

		si, errstr := hrwTarget(payload.Bucket, objName, smap)
		if errstr != "" {
			return fmt.Errorf(errstr)
		}

		dlObj := cmn.DlObj{
			Objname:   objName,
			Link:      link,
			FromCloud: cloud,
		}

		b, ok := bulkTargetRequest[si]
		if !ok {
			dlBody := &cmn.DlBody{
				ID: id,
			}
			dlBody.Bucket = payload.Bucket
			dlBody.BckProvider = payload.BckProvider
			dlBody.Timeout = payload.Timeout
			dlBody.Description = payload.Description

			bulkTargetRequest[si] = dlBody
			b = dlBody
		}

		b.Objs = append(b.Objs, dlObj)
	}

	errCh = make(chan error, len(bulkTargetRequest))
	for si, dlBody := range bulkTargetRequest {
		wg.Add(1)
		go func(si *cluster.Snode, dlBody *cmn.DlBody) {
			if resp := p.targetDownloadRequest(http.MethodPost, si, dlBody); resp.err != nil {
				errCh <- resp.err
			}
			wg.Done()
		}(si, dlBody)
	}
	wg.Wait()
	close(errCh)

	// FIXME: consider adding new stats: downloader failures
	failures := make([]error, 0, 10)
	for err := range errCh {
		if err != nil {
			failures = append(failures, err)
		}
	}
	if len(failures) > 0 {
		glog.Error(failures, len(failures))
		return fmt.Errorf("following downloads failed: %v", failures)
	}
	return nil
}

// [METHOD] /v1/download
func (p *proxyrunner) downloadHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet, http.MethodDelete:
		p.httpDownloadAdmin(w, r)
	case http.MethodPost:
		p.httpDownloadPost(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /download path")
	}
}

// httpDownloadAdmin is meant for cancelling and getting status updates for
// downloads.
// GET /v1/download?id=...
// DELETE /v1/download?id=...
func (p *proxyrunner) httpDownloadAdmin(w http.ResponseWriter, r *http.Request) {
	var (
		payload = &cmn.DlAdminBody{}
	)

	payload.InitWithQuery(r.URL.Query())
	if err := payload.Validate(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	glog.V(4).Infof("httpDownloadAdmin payload %v", payload)
	resp, statusCode, err := p.broadcastDownloadRequest(r.Method, payload)
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

	id, err := cmn.GenUUID()
	if err != nil {
		glog.Error(err)
		p.invalmsghdlr(w, r, "failed to generate id for the request", http.StatusInternalServerError)
		return
	}

	p.objectDownloadHandler(w, r, id)
}

// POST /v1/download?bucket=...&link=...&objname=...&template=...
func (p *proxyrunner) objectDownloadHandler(w http.ResponseWriter, r *http.Request, id string) {
	var (
		// link -> objname
		objects cmn.SimpleKVs
		query   = r.URL.Query()

		payload        = &cmn.DlBase{}
		singlePayload  = &cmn.DlSingleBody{}
		rangePayload   = &cmn.DlRangeBody{}
		multiPayload   = &cmn.DlMultiBody{}
		cloudPayload   = &cmn.DlCloudBody{}
		objectsPayload interface{}

		bckIsLocal, fromCloud, ok bool
	)

	payload.InitWithQuery(query)
	if bckIsLocal, ok = p.validateBucket(w, r, payload.Bucket, payload.BckProvider); !ok {
		return
	}

	singlePayload.InitWithQuery(query)
	rangePayload.InitWithQuery(query)
	multiPayload.InitWithQuery(query)
	cloudPayload.InitWithQuery(query)

	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	if err := singlePayload.Validate(); err == nil {
		if objects, err = singlePayload.ExtractPayload(); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
	} else if err := rangePayload.Validate(); err == nil {
		if objects, err = rangePayload.ExtractPayload(); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
	} else if err := multiPayload.Validate(b); err == nil {
		if err := jsoniter.Unmarshal(b, &objectsPayload); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}

		if objects, err = multiPayload.ExtractPayload(objectsPayload); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
	} else if err := cloudPayload.Validate(bckIsLocal); err == nil {
		msg := cmn.SelectMsg{
			Prefix:     cloudPayload.Prefix,
			PageMarker: "",
			Fast:       true,
		}

		bckEntries := make([]*cmn.BucketEntry, 0, 1024)
		for {
			curBckEntries, err := p.listBucket(r, payload.Bucket, payload.BckProvider, msg)
			if err != nil {
				p.invalmsghdlr(w, r, err.Error())
				return
			}

			// filter only with matching suffix
			for _, entry := range curBckEntries.Entries {
				if strings.HasSuffix(entry.Name, cloudPayload.Suffix) {
					bckEntries = append(bckEntries, entry)
				}
			}

			msg.PageMarker = curBckEntries.PageMarker
			if msg.PageMarker == "" {
				break
			}
		}

		objects = make(cmn.SimpleKVs, 10)
		for _, entry := range bckEntries {
			objects[entry.Name] = ""
		}
		fromCloud = true
	} else {
		p.invalmsghdlr(w, r, "invalid query keys or query values")
		return
	}

	if err := p.bulkDownloadProcessor(id, payload, objects, fromCloud); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	p.respondWithID(w, r, id)
}

// Helper methods

func (p *proxyrunner) respondWithID(w http.ResponseWriter, r *http.Request, id string) {
	resp := cmn.DlPostResp{
		ID: id,
	}

	b, err := jsoniter.Marshal(resp)
	if err != nil {
		p.invalmsghdlr(w, r, "error marshalling response", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, err = w.Write(b)
	if err != nil {
		glog.Errorf("Failed to write to http response: %v.", err)
	}
}

////////////
// TARGET //
////////////

// NOTE: This request is internal so we can have asserts there.
// [METHOD] /v1/download
func (t *targetrunner) downloadHandler(w http.ResponseWriter, r *http.Request) {
	if !t.verifyProxyRedirection(w, r, "", "", cmn.Download) {
		return
	}

	var (
		response   interface{}
		err        error
		statusCode int
	)

	downloader, err := t.xactions.renewDownloader(t)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusInternalServerError)
		return
	}

	switch r.Method {
	case http.MethodPost:
		payload := &cmn.DlBody{}
		if err := cmn.ReadJSON(w, r, payload); err != nil {
			return
		}
		cmn.AssertNoErr(payload.Validate())

		glog.V(4).Infof("Downloading: %s", payload)
		response, err, statusCode = downloader.Download(payload)
	case http.MethodGet:
		payload := &cmn.DlAdminBody{}
		if err := cmn.ReadJSON(w, r, payload); err != nil {
			return
		}
		cmn.AssertNoErr(payload.Validate())

		if payload.ID != "" {
			glog.V(4).Infof("Getting status of download: %s", payload)
			response, err, statusCode = downloader.Status(payload.ID)
		} else {
			var regex *regexp.Regexp
			if payload.Regex != "" {
				if regex, err = regexp.CompilePOSIX(payload.Regex); err != nil {
					cmn.InvalidHandlerWithMsg(w, r, err.Error())
					return
				}
			}
			glog.V(4).Infof("Listing downloads")
			response, err, statusCode = downloader.List(regex)
		}

	case http.MethodDelete:
		payload := &cmn.DlAdminBody{}
		if err := cmn.ReadJSON(w, r, payload); err != nil {
			return
		}
		cmn.AssertNoErr(payload.Validate())

		glog.V(4).Infof("Cancelling download: %s", payload)
		response, err, statusCode = downloader.Cancel(payload.ID)
	default:
		cmn.AssertMsg(false, r.Method)
		return
	}

	if statusCode >= http.StatusBadRequest {
		cmn.InvalidHandlerWithMsg(w, r, err.Error(), statusCode)
		return
	}

	if response != nil {
		b, err := jsoniter.Marshal(response)
		cmn.AssertNoErr(err)
		_, err = w.Write(b)
		if err != nil {
			glog.Errorf("Failed to write to http response: %s.", err.Error())
		}
	}
}
