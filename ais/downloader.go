// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/stats"
)

///////////
// PROXY //
///////////

func (p *proxyrunner) downloadRedirectURL(bucket, objname string, started time.Time, query url.Values) (redirectURL, daemonID, err string) {
	smap := p.smapowner.get()
	si, errstr := hrwTarget(bucket, objname, smap)
	if errstr != "" {
		err = errstr
		return
	}
	daemonID = si.DaemonID
	redirectURL = si.URL(cmn.NetworkIntraControl) + cmn.URLPath(cmn.Version, cmn.Download) + "?"
	query.Add(cmn.URLParamProxyID, p.si.DaemonID)
	query.Add(cmn.URLParamUnixTime, strconv.FormatInt(int64(started.UnixNano()), 10))
	redirectURL += query.Encode()
	return
}

func (p *proxyrunner) targetDownloadRequest(bucket, objname string, query url.Values) (int, error) {
	smap := p.smapowner.get()
	// determine where to make a download request
	si, errstr := hrwTarget(bucket, objname, smap)
	if errstr != "" {
		return 0, fmt.Errorf(errstr)
	}

	query.Add(cmn.URLParamProxyID, p.si.DaemonID)
	query.Add(cmn.URLParamUnixTime, strconv.FormatInt(int64(time.Now().UnixNano()), 10))

	args := callArgs{
		si: si,
		req: reqArgs{
			method: http.MethodPost,
			path:   cmn.URLPath(cmn.Version, cmn.Download),
			query:  query,
		},
		timeout: defaultTimeout,
	}
	res := p.call(args)
	return res.status, res.err
}

// objects is a map of objnames (keys) where the corresponding
// value is the link that the download will be saved as.
func (p *proxyrunner) bulkDownloadProcessor(bucket string, bckIsLocal bool, objects cmn.SimpleKVs) error {
	var (
		failures     = cmn.SimpleKVs{}
		failureMutex = &sync.Mutex{}
		wg           = &sync.WaitGroup{}
	)

	for objname, link := range objects {
		wg.Add(1)
		go func(objname, link string) {
			payload := cmn.DlBody{
				Objname: objname,
				Link:    link,
			}
			payload.Bucket = bucket
			query := payload.AsQuery()

			if _, err := p.targetDownloadRequest(bucket, objname, query); err != nil {
				// FIXME: consider adding threadsafe SimpleKV store
				failureMutex.Lock()
				failures[fmt.Sprintf("%q, %q", objname, link)] = err.Error()
				failureMutex.Unlock()
			}
			wg.Done()
		}(objname, link)
	}
	wg.Wait()
	// FIXME: consider adding new stats: downloader failures
	if len(failures) > 0 {
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
// GET /v1/download?bucket=...&link=...&objname=...
// DELETE /v1/download?bucket=...&link=...&objname=...
func (p *proxyrunner) httpDownloadAdmin(w http.ResponseWriter, r *http.Request) {
	var (
		started                       = time.Now()
		redirectURL, daemonID, errstr string
		payload                       = &cmn.DlBody{}
		query                         = r.URL.Query()
	)

	payload.InitWithQuery(query)
	if err := payload.Validate(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	glog.V(4).Infof("httpDownloadAdmin payload %v", payload)

	if _, ok := p.validateBucket(w, r, payload.Bucket, payload.BckProvider); !ok {
		return
	}

	if redirectURL, daemonID, errstr = p.downloadRedirectURL(payload.Bucket, payload.Objname, started, query); errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}

	glog.V(4).Infof("Download %s %s/%s => %s", r.Method, payload.Bucket, payload.Objname, daemonID)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

// POST /v1/download
func (p *proxyrunner) httpDownloadPost(w http.ResponseWriter, r *http.Request) {
	apitems, err := p.checkRESTItems(w, r, 1, true, cmn.Version, cmn.Download)
	if err != nil {
		return
	}
	if len(apitems) >= 1 {
		switch apitems[0] {
		case cmn.DownloadSingle:
			p.singleDownloadHandler(w, r)
			return
		case cmn.DownloadRange:
			p.rangeDownloadHandler(w, r)
			return
		case cmn.DownloadMulti:
			p.multiDownloadHandler(w, r)
			return
		case cmn.DownloadBucket:
			p.bucketDownloadHandler(w, r)
			return
		}
	}
	p.invalmsghdlr(w, r, fmt.Sprintf("%q is not a valid download request", apitems))
}

// POST /v1/download/single?bucket=...&link=...&objname=...
func (p *proxyrunner) singleDownloadHandler(w http.ResponseWriter, r *http.Request) {
	var (
		started                       = time.Now()
		redirectURL, daemonID, errstr string
		payload                       = &cmn.DlBody{}
		query                         = r.URL.Query()
	)

	payload.InitWithQuery(query)
	if err := payload.Validate(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	glog.V(4).Infof("singleDownloadHandler payload %v", payload)

	if _, ok := p.validateBucket(w, r, payload.Bucket, payload.BckProvider); !ok {
		return
	}

	if redirectURL, daemonID, errstr = p.downloadRedirectURL(payload.Bucket, payload.Objname, started, query); errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}
	glog.V(4).Infof("Download %s %s/%s => %s", r.Method, payload.Bucket, payload.Objname, daemonID)

	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
	p.statsif.Add(stats.PutCount, 1)

}

// POST /v1/download/range?bucket=...&base=...&template=...
func (p *proxyrunner) rangeDownloadHandler(w http.ResponseWriter, r *http.Request) {
	var (
		payload = &cmn.DlRangeBody{}
		// link -> objname
		objects        = make(cmn.SimpleKVs)
		bckIsLocal, ok bool
	)

	payload.InitWithQuery(r.URL.Query())
	if err := payload.Validate(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	if bckIsLocal, ok = p.validateBucket(w, r, payload.Bucket, payload.BckProvider); !ok {
		return
	}

	glog.V(4).Infof("rangeDownloadHandler payload: %s", payload)

	prefix, suffix, start, end, step, digitCount, err := cmn.ParseBashTemplate(payload.Template)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	for i := start; i <= end; i += step {
		objname := fmt.Sprintf("%s%0*d%s", prefix, digitCount, i, suffix)
		objects[objname] = payload.Base + objname
	}

	if err := p.bulkDownloadProcessor(payload.Bucket, bckIsLocal, objects); err != nil {
		p.invalmsghdlr(w, r, err.Error())
	}
}

// POST /v1/download/multi?bucket=...&timeout=...
func (p *proxyrunner) multiDownloadHandler(w http.ResponseWriter, r *http.Request) {
	var (
		payload = &cmn.DlMultiBody{}
		objects = make(cmn.SimpleKVs)

		objectsPayload interface{}

		bckIsLocal, ok bool
	)

	if err := cmn.ReadJSON(w, r, &objectsPayload); err != nil {
		return
	}

	payload.InitWithQuery(r.URL.Query())
	if err := payload.Validate(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	glog.V(4).Infof("multiDownloadHandler payload: %s", payload)

	if bckIsLocal, ok = p.validateBucket(w, r, payload.Bucket, payload.BckProvider); !ok {
		return
	}

	switch ty := objectsPayload.(type) {
	case map[string]interface{}:
		for key, val := range ty {
			switch v := val.(type) {
			case string:
				objects[key] = v
			default:
				p.invalmsghdlr(w, r, fmt.Sprintf("values in map should be strings, found: %T", v))
				return
			}
		}
	case []interface{}:
		// process list of links
		for _, val := range ty {
			switch link := val.(type) {
			case string:
				objName := path.Base(link)
				if objName == "." || objName == "/" {
					// should we continue and let the use worry about this after?
					p.invalmsghdlr(w, r, fmt.Sprintf("can not extract a valid `object name` from the provided download link: %q.", link))
					return
				}
				objects[objName] = link
			default:
				p.invalmsghdlr(w, r, fmt.Sprintf("values in array should be strings, found: %T", link))
				return
			}
		}
	default:
		p.invalmsghdlr(w, r, fmt.Sprintf("JSON body should be map (string -> string) or array of strings, found: %T", ty))
		return
	}

	// process the downloads
	if err := p.bulkDownloadProcessor(payload.Bucket, bckIsLocal, objects); err != nil {
		p.invalmsghdlr(w, r, err.Error())
	}
}

// POST /v1/download/bucket/name?provider=...&prefix=...&suffix=...
func (p *proxyrunner) bucketDownloadHandler(w http.ResponseWriter, r *http.Request) {
	var (
		payload = &cmn.DlBucketBody{}
	)

	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Download, cmn.DownloadBucket)
	if err != nil {
		return
	}
	payload.Bucket = apiItems[0]

	query := r.URL.Query()
	payload.InitWithQuery(query)

	if err := payload.Validate(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	if bckIsLocal, ok := p.validateBucket(w, r, payload.Bucket, payload.BckProvider); !ok {
		return
	} else if bckIsLocal {
		p.invalmsghdlr(w, r, "/download/bucket requires cloud bucket")
		return
	}

	msg := cmn.GetMsg{
		GetPrefix:     payload.Prefix,
		GetPageMarker: "",
		GetFast:       true,
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
			if strings.HasSuffix(entry.Name, payload.Suffix) {
				bckEntries = append(bckEntries, entry)
			}
		}

		msg.GetPageMarker = curBckEntries.PageMarker
		if msg.GetPageMarker == "" {
			break
		}
	}

	objects := make([]string, len(bckEntries))
	for idx, entry := range bckEntries {
		objects[idx] = entry.Name
	}
	actionMsg := &cmn.ActionMsg{
		Action: cmn.ActPrefetch,
		Name:   "download/bucket",
		Value:  map[string]interface{}{"objnames": objects},
	}
	if err := p.listRange(http.MethodPost, payload.Bucket, actionMsg, nil); err != nil {
		p.invalmsghdlr(w, r, err.Error())
	}
}

////////////
// TARGET //
////////////

// [METHOD] /v1/download
func (t *targetrunner) downloadHandler(w http.ResponseWriter, r *http.Request) {
	payload := &cmn.DlBody{}
	payload.InitWithQuery(r.URL.Query())
	cmn.AssertNoErr(payload.Validate())

	glog.V(4).Infof("downloadHandler payload %s", payload)

	if !t.verifyProxyRedirection(w, r, payload.Bucket, payload.Objname, cmn.Download) {
		return
	}

	var (
		response   string
		err        error
		statusCode int
	)
	switch r.Method {
	case http.MethodGet:
		glog.V(4).Infof("Getting status of download: %s", payload)
		response, err, statusCode = t.xactions.renewDownloader(t).Status(payload)
	case http.MethodDelete:
		glog.V(4).Infof("Cancelling download: %s", payload)
		response, err, statusCode = t.xactions.renewDownloader(t).Cancel(payload)
	case http.MethodPost:
		glog.V(4).Infof("Downloading: %s", payload)
		response, err, statusCode = t.xactions.renewDownloader(t).Download(payload)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /download path")
		return
	}

	if statusCode >= http.StatusBadRequest {
		cmn.InvalidHandlerWithMsg(w, r, err.Error(), statusCode)
		return
	}
	w.Write([]byte(response))
}
