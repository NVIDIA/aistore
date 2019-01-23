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
	jsoniter "github.com/json-iterator/go"
)

///////////
// PROXY //
///////////

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

func (p *proxyrunner) downloadRedirectURL(bucket, objname string, started time.Time) (redirectURL, daemonID, err string) {
	smap := p.smapowner.get()
	si, errstr := hrwTarget(bucket, objname, smap)
	if errstr != "" {
		err = errstr
		return
	}
	daemonID = si.DaemonID
	redirectURL = si.URL(cmn.NetworkPublic) + cmn.URLPath(cmn.Version, cmn.Download) + "?"
	var query = url.Values{}
	query.Add(cmn.URLParamProxyID, p.si.DaemonID)
	query.Add(cmn.URLParamUnixTime, strconv.FormatInt(int64(started.UnixNano()), 10))
	redirectURL += query.Encode()
	return
}

// httpDownloadAdmin is meant for cancelling and getting status updates for
// downloads.
// GET /v1/download or DELETE /v1/download
func (p *proxyrunner) httpDownloadAdmin(w http.ResponseWriter, r *http.Request) {
	var (
		started                       = time.Now()
		redirectURL, daemonID, errstr string
		payload                       = cmn.DlBody{}
	)
	if err := cmn.ReadJSON(w, r, &payload); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if glog.V(4) {
		glog.Infof("httpDownloadAdmin payload %v", payload)
	}

	if err := payload.Validate(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	if !p.validatebckname(w, r, payload.Bucket) {
		return
	}

	if redirectURL, daemonID, errstr = p.downloadRedirectURL(payload.Bucket, payload.Objname, started); errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}
	if glog.V(4) {
		glog.Infof("Download %s %s/%s => %s", r.Method, payload.Bucket, payload.Objname, daemonID)
	}
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

// POST /v1/download
func (p *proxyrunner) httpDownloadPost(w http.ResponseWriter, r *http.Request) {
	apitems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Download)
	if err != nil {
		return
	}
	if len(apitems) == 1 {
		switch apitems[0] {
		case cmn.DownloadMulti:
			p.multiDownloadHandler(w, r)
			return
		case cmn.DownloadList:
			p.listDownloadHandler(w, r)
			return
		case cmn.DownloadSingle:
			p.singleDownloadHandler(w, r)
			return
		}
	}
	p.invalmsghdlr(w, r, fmt.Sprintf("%q is not a valid download request", apitems))
}

// POST /v1/download/single
func (p *proxyrunner) singleDownloadHandler(w http.ResponseWriter, r *http.Request) {
	var (
		started                       = time.Now()
		redirectURL, daemonID, errstr string
		payload                       = cmn.DlBody{}
	)
	if err := cmn.ReadJSON(w, r, &payload); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if glog.V(4) {
		glog.Infof("singleDownloadHandler payload %v", payload)
	}

	if payload.Objname == "" {
		objName := path.Base(payload.Link)
		if objName == "." || objName == "/" {
			p.invalmsghdlr(w, r, "can not extract a valid objName from the provided download link")
			return
		}
		payload.Objname = objName
	}

	if err := payload.Validate(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	// check if the bucket exists
	if !p.validatebckname(w, r, payload.Bucket) {
		return
	}
	bucketmd := p.bmdowner.get()
	if _, ok := bucketmd.LBmap[payload.Bucket]; !ok {
		p.invalmsghdlr(w, r, "bucket does not exist")
		return
	}

	if redirectURL, daemonID, errstr = p.downloadRedirectURL(payload.Bucket, payload.Objname, started); errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}
	if glog.V(4) {
		glog.Infof("Download %s %s/%s => %s", r.Method, payload.Bucket, payload.Objname, daemonID)
	}

	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
	p.statsif.Add(stats.PutCount, 1)

}

func (p *proxyrunner) targetDownloadRequest(bucket, objname string, body []byte) (int, error) {
	smap := p.smapowner.get()
	// determine where to make a download request
	si, errstr := hrwTarget(bucket, objname, smap)
	if errstr != "" {
		return 0, fmt.Errorf(errstr)
	}

	var query = url.Values{}
	query.Add(cmn.URLParamProxyID, p.si.DaemonID)
	query.Add(cmn.URLParamUnixTime, strconv.FormatInt(int64(time.Now().UnixNano()), 10))

	args := callArgs{
		si: si,
		req: reqArgs{
			method: http.MethodPost,
			path:   cmn.URLPath(cmn.Version, cmn.Download),
			query:  query,
			body:   body,
		},
		timeout: defaultTimeout,
	}
	res := p.call(args)
	return res.status, res.err
}

// objects is a map of objnames (keys) where the corresponding
// value is the link that the download will be saved as.
func (p *proxyrunner) bulkDownloadProcessor(w http.ResponseWriter, r *http.Request, bucket string, objects cmn.SimpleKVs, headers map[string]string) {
	var (
		failures     = cmn.SimpleKVs{}
		failureMutex = &sync.Mutex{}
		wg           = &sync.WaitGroup{}
	)

	if !p.validatebckname(w, r, bucket) {
		return
	}
	// check if the bucket exists
	bucketmd := p.bmdowner.get()
	if _, ok := bucketmd.LBmap[bucket]; !ok {
		p.invalmsghdlr(w, r, "specified bucket does not exist")
		return
	}

	for objname, link := range objects {
		wg.Add(1)
		go func(objname, link string) {
			payload := cmn.DlBody{
				Objname: objname,
				Link:    link,
			}
			payload.Bucket = bucket

			if len(headers) > 0 {
				payload.Headers = headers
			}
			body, err := jsoniter.Marshal(payload)

			if err == nil {
				_, err = p.targetDownloadRequest(bucket, objname, body)
			}

			if err != nil {
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
		p.invalmsghdlr(w, r, fmt.Sprintf("following downloads failed: %v", failures))
	}
}

// POST /v1/download/list
func (p *proxyrunner) listDownloadHandler(w http.ResponseWriter, r *http.Request) {
	var (
		payload = cmn.DlListBody{}
		// link -> objname
		objects = make(cmn.SimpleKVs)
	)
	if err := cmn.ReadJSON(w, r, &payload); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	if payload.Base == "" {
		glog.Errorf("No base provided for list download request.")
		p.invalmsghdlr(w, r, "no prefix for list found, prefix is required")
		return
	} else if !strings.HasSuffix(payload.Base, "/") {
		payload.Base += "/"
	}

	if payload.Step == 0 {
		payload.Step = 1
	}

	if payload.Start > payload.End {
		p.invalmsghdlr(w, r, "start value greater than end")
		return
	}

	if glog.V(4) {
		glog.Infof("List Download. %v", payload)
	}
	for i := payload.Start; i <= payload.End; i += payload.Step {
		objname := fmt.Sprintf("%s%0*d%s", payload.Prefix, payload.DigitCount, i, payload.Suffix)
		objects[objname] = payload.Base + objname
	}
	if glog.V(4) {
		glog.Infof("got a request to download the following objects: %v", objects)
	}
	p.bulkDownloadProcessor(w, r, payload.Bucket, objects, payload.Headers)
}

// POST /v1/download/multi
func (p *proxyrunner) multiDownloadHandler(w http.ResponseWriter, r *http.Request) {
	var (
		payload = cmn.DlMultiBody{}
		objects = make(cmn.SimpleKVs)
	)

	if err := cmn.ReadJSON(w, r, &payload); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if glog.V(4) {
		glog.Infof("multiDownloadHandler payload %v", payload)
	}

	// check if an objectMap or objectList is present
	if len(payload.ObjectMap) == 0 && len(payload.ObjectList) == 0 {
		p.invalmsghdlr(w, r, "missing object map or object list for multi download request")
		return
	}

	if len(payload.ObjectMap) != 0 {
		for k, v := range payload.ObjectMap {
			objects[k] = v
		}
	} else {
		// process list of links
		for _, link := range payload.ObjectList {
			objName := path.Base(link)
			if objName == "." || objName == "/" {
				// should we continue and let the use worry about this after?
				p.invalmsghdlr(w, r, fmt.Sprintf("Can not extract a valid objName from the provided download link: %q.", link))
				return
			}
			objects[objName] = link
		}
	}

	// process the downloads
	if glog.V(4) {
		glog.Infof("Got a request to download the following objects: %v", objects)
	}
	p.bulkDownloadProcessor(w, r, payload.Bucket, objects, payload.Headers)
}

////////////
// TARGET //
////////////

// [METHOD] /v1/download
func (t *targetrunner) downloadHandler(w http.ResponseWriter, r *http.Request) {
	payload := cmn.DlBody{}
	if err := cmn.ReadJSON(w, r, &payload); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if glog.V(4) {
		glog.Infof("downloadHandler payload %v", payload)
	}

	if err := payload.Validate(); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

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
		glog.Infof("Getting status of download: %s", payload)
		response, err, statusCode = t.xactions.renewDownloader(t, payload.Bucket).Status(&payload)
	case http.MethodDelete:
		glog.Infof("Cancelling download: %s", payload)
		response, err, statusCode = t.xactions.renewDownloader(t, payload.Bucket).Cancel(&payload)
	case http.MethodPost:
		glog.Infof("Downloading: %s", payload)
		response, err, statusCode = t.xactions.renewDownloader(t, payload.Bucket).Download(&payload)
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
