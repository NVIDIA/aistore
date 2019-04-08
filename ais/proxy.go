// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/mirror"
	"github.com/NVIDIA/aistore/stats"
	jsoniter "github.com/json-iterator/go"
)

const (
	internalPageSize = 10000 // number of objects in a page for internal call between target and proxy to get atime/iscached

	tokenStart = "Bearer"
)

type (
	ClusterMountpathsRaw struct {
		Targets map[string]jsoniter.RawMessage `json:"targets"`
	}
	// Keeps a target response when doing parallel requests to all targets
	bucketResp struct {
		outjson []byte
		err     error
		id      string
	}
	// A list of target local objects (cached or local bucket)
	// Maximum number of objects in the response is `pageSize`
	localFilePage struct {
		entries []*cmn.BucketEntry
		err     error
		id      string
		marker  string
	}
	// two pieces of metadata a self-registering (joining) target wants to know right away
	targetRegMeta struct {
		Smap *smapX    `json:"smap"`
		Bmd  *bucketMD `json:"bmd"`
	}
)

func wrapHandler(h http.HandlerFunc, wraps ...func(http.HandlerFunc) http.HandlerFunc) http.HandlerFunc {
	for _, w := range wraps {
		h = w(h)
	}

	return h
}

//===========================================================================
//
// proxy runner
//
//===========================================================================
type proxyrunner struct {
	httprunner
	starttime  time.Time
	authn      *authManager
	startedUp  int64
	metasyncer *metasyncer
	rproxy     struct {
		sync.Mutex
		cloud *httputil.ReverseProxy            // unmodified GET requests => storage.googleapis.com
		p     *httputil.ReverseProxy            // requests that modify cluster-level metadata => current primary gateway
		u     string                            // URL of the current primary
		tmap  map[string]*httputil.ReverseProxy // map of reverse proxies keyed by target DaemonIDs
	}
}

// start proxy runner
func (p *proxyrunner) Run() error {
	config := cmn.GCO.Get()
	p.httprunner.init(getproxystatsrunner(), true)
	p.httprunner.keepalive = getproxykeepalive()

	bucketmdfull := filepath.Join(config.Confdir, cmn.BucketmdBackupFile)
	bucketmd := newBucketMD()
	if cmn.LocalLoad(bucketmdfull, bucketmd) != nil {
		// create empty
		bucketmd.Version = 1
		if err := cmn.LocalSave(bucketmdfull, bucketmd); err != nil {
			glog.Fatalf("FATAL: cannot store %s, err: %v", bmdTermName, err)
		}
	}
	p.bmdowner.put(bucketmd)

	p.metasyncer = getmetasyncer()

	// startup sequence - see earlystart.go for the steps and commentary
	cmn.Assert(p.smapowner.get() == nil)
	p.bootstrap()

	p.authn = &authManager{
		tokens:        make(map[string]*authRec),
		revokedTokens: make(map[string]bool),
		version:       1,
	}

	if config.Net.HTTP.RevProxy == cmn.RevProxyCloud {
		p.rproxy.cloud = &httputil.ReverseProxy{
			Director: func(r *http.Request) {},
			Transport: cmn.NewTransport(cmn.ClientArgs{
				UseHTTPS: config.Net.HTTP.UseHTTPS,
			}),
		}
	}

	//
	// REST API: register proxy handlers and start listening
	//

	// Public network
	if config.Net.HTTP.RevProxy == cmn.RevProxyCloud {
		p.registerPublicNetHandler("/", p.reverseProxyHandler)
	} else {
		p.registerPublicNetHandler("/", cmn.InvalidHandler)
	}

	bucketHandler, objectHandler := p.bucketHandler, p.objectHandler
	if config.Auth.Enabled {
		bucketHandler, objectHandler = wrapHandler(p.bucketHandler, p.checkHTTPAuth), wrapHandler(p.objectHandler, p.checkHTTPAuth)
	}

	networkHandlers := []networkHandler{
		networkHandler{r: cmn.Buckets, h: bucketHandler, net: []string{cmn.NetworkPublic}},
		networkHandler{r: cmn.Objects, h: objectHandler, net: []string{cmn.NetworkPublic}},
		networkHandler{r: cmn.Download, h: p.downloadHandler, net: []string{cmn.NetworkPublic}},
		networkHandler{r: cmn.Daemon, h: p.daemonHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl}},
		networkHandler{r: cmn.Cluster, h: p.clusterHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl}},
		networkHandler{r: cmn.Tokens, h: p.tokenHandler, net: []string{cmn.NetworkPublic}},
		networkHandler{r: cmn.Sort, h: dsort.ProxySortHandler, net: []string{cmn.NetworkPublic}},

		networkHandler{r: cmn.Metasync, h: p.metasyncHandler, net: []string{cmn.NetworkIntraControl}},
		networkHandler{r: cmn.Health, h: p.healthHandler, net: []string{cmn.NetworkIntraControl}},
		networkHandler{r: cmn.Vote, h: p.voteHandler, net: []string{cmn.NetworkIntraControl}},

		networkHandler{r: "/", h: cmn.InvalidHandler, net: []string{cmn.NetworkIntraControl, cmn.NetworkIntraData}},
	}
	p.registerNetworkHandlers(networkHandlers)

	glog.Infof("%s: [public net] listening on: %s", p.si.Name(), p.si.PublicNet.DirectURL)
	if p.si.PublicNet.DirectURL != p.si.IntraControlNet.DirectURL {
		glog.Infof("%s: [intra control net] listening on: %s", p.si.Name(), p.si.IntraControlNet.DirectURL)
	}
	if p.si.PublicNet.DirectURL != p.si.IntraDataNet.DirectURL {
		glog.Infof("%s: [intra data net] listening on: %s", p.si.Name(), p.si.IntraDataNet.DirectURL)
	}
	if config.Net.HTTP.RevProxy != "" {
		glog.Warningf("Warning: serving GET /object as a reverse-proxy ('%s')", config.Net.HTTP.RevProxy)
	}
	p.starttime = time.Now()

	dsort.RegisterNode(p.smapowner, p.bmdowner, p.si, nil, nil)
	return p.httprunner.run()
}

func (p *proxyrunner) register(keepalive bool, timeout time.Duration) (status int, err error) {
	var (
		res  callResult
		smap = p.smapowner.get()
	)
	if smap != nil && smap.isPrimary(p.si) {
		return
	}
	if !keepalive {
		if cmn.GCO.Get().Proxy.NonElectable {
			query := url.Values{}
			query.Add(cmn.URLParamNonElectable, "true")
			res = p.join(true, query)
		} else {
			res = p.join(true, nil)
		}
	} else { // keepalive
		url, psi := p.getPrimaryURLAndSI()
		res = p.registerToURL(url, psi, timeout, true, nil, keepalive)
	}
	return res.status, res.err
}

func (p *proxyrunner) unregister() (int, error) {
	smap := p.smapowner.get()
	args := callArgs{
		si: smap.ProxySI,
		req: reqArgs{
			method: http.MethodDelete,
			path:   cmn.URLPath(cmn.Version, cmn.Cluster, cmn.Daemon, cmn.Proxy, p.si.DaemonID),
		},
		timeout: defaultTimeout,
	}
	res := p.call(args)
	return res.status, res.err
}

// stop gracefully
func (p *proxyrunner) Stop(err error) {
	var isPrimary bool
	smap := p.smapowner.get()
	if smap != nil { // in tests
		isPrimary = smap.isPrimary(p.si)
	}
	glog.Infof("Stopping %s (%s, primary=%t), err: %v", p.Getname(), p.si.Name(), isPrimary, err)
	p.xactions.abortAll()

	if isPrimary {
		// give targets and non primary proxies some time to unregister
		version := smap.version()
		for i := 0; i < 20; i++ {
			time.Sleep(time.Second)
			v := p.smapowner.get().version()
			if version == v {
				break
			}

			version = v
		}
	}

	if p.publicServer.s != nil && !isPrimary {
		_, unregerr := p.unregister()
		if unregerr != nil {
			glog.Warningf("Failed to unregister when terminating: %v", unregerr)
		}
	}

	p.httprunner.stop(err)
}

//==================================
//
// http /bucket and /objects handlers
//
//==================================

// verb /v1/buckets/
func (p *proxyrunner) bucketHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.httpbckget(w, r)
	case http.MethodDelete:
		p.httpbckdelete(w, r)
	case http.MethodPost:
		p.httpbckpost(w, r)
	case http.MethodHead:
		p.httpbckhead(w, r)
	case http.MethodPut:
		p.httpbckput(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /buckets path")
	}
}

// verb /v1/objects/
func (p *proxyrunner) objectHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.httpobjget(w, r)
	case http.MethodPut:
		p.httpobjput(w, r)
	case http.MethodDelete:
		p.httpobjdelete(w, r)
	case http.MethodPost:
		p.httpobjpost(w, r)
	case http.MethodHead:
		p.httpobjhead(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /objects path")
	}
}

// GET /v1/buckets/bucket-name
func (p *proxyrunner) httpbckget(w http.ResponseWriter, r *http.Request) {
	if p.smapowner.get().CountTargets() < 1 {
		p.invalmsghdlr(w, r, "No registered targets yet")
		return
	}
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apiItems[0]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)

	normalizedBckProvider, err := cmn.BckProviderFromStr(bckProvider)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	// list bucket names
	if bucket == cmn.ListAll {
		p.getbucketnames(w, r, bucket, normalizedBckProvider)
		return
	}
	s := fmt.Sprintf("Invalid route /buckets/%s", bucket)
	p.invalmsghdlr(w, r, s)
}

// Used for redirecting ReverseProxy GCP/AWS GET request to target
func (p *proxyrunner) objGetRProxy(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	apitems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	if _, ok := p.validateBucket(w, r, bucket, bckProvider); !ok {
		return
	}
	smap := p.smapowner.get()
	si, errstr := hrwTarget(bucket, objname, smap)
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}
	redirecturl := p.redirectURL(r, si.PublicNet.DirectURL, started, bucket)
	pReq, err := http.NewRequest(r.Method, redirecturl, r.Body)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	for header, values := range r.Header {
		for _, value := range values {
			pReq.Header.Add(header, value)
		}
	}

	pRes, err := p.httpclient.Do(pReq)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if pRes.StatusCode >= http.StatusBadRequest {
		p.invalmsghdlr(w, r, "Failed to read object", pRes.StatusCode)
		return
	}
	io.Copy(w, pRes.Body)
	pRes.Body.Close()
}

// GET /v1/objects/bucket-name/object-name
func (p *proxyrunner) httpobjget(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	apitems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	if _, ok := p.validateBucket(w, r, bucket, bckProvider); !ok {
		return
	}

	smap := p.smapowner.get()
	si, errstr := hrwTarget(bucket, objname, smap)
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}

	config := cmn.GCO.Get()
	if config.Net.HTTP.RevProxy == cmn.RevProxyTarget {
		if glog.V(4) {
			glog.Infof("reverse-proxy: %s %s/%s <= %s", r.Method, bucket, objname, si)
		}
		p.reverseDP(w, r, si)
		delta := time.Since(started)
		p.statsif.Add(stats.GetLatency, int64(delta))
	} else {
		if glog.V(4) {
			glog.Infof("%s %s/%s => %s", r.Method, bucket, objname, si)
		}
		redirecturl := p.redirectURL(r, si.PublicNet.DirectURL, started, bucket)
		if config.Readahead.Enabled && config.Readahead.ByProxy {
			go func(url string) {
				url += "&" + cmn.URLParamReadahead + "=true"
				args := callArgs{
					si: nil, // already inside url
					req: reqArgs{
						method: r.Method,
						header: r.Header,
						base:   url,
					},
					timeout: config.Timeout.ProxyPing,
				}
				res := p.call(args)
				if res.err != nil {
					glog.Errorf("Failed readahead %s/%s => %s: %v", bucket, objname, si, res.err)
				}
			}(redirecturl)
		}
		http.Redirect(w, r, redirecturl, http.StatusMovedPermanently)
	}
	p.statsif.Add(stats.GetCount, 1)
}

// PUT /v1/objects
func (p *proxyrunner) httpobjput(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	apitems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	if _, ok := p.validateBucket(w, r, bucket, bckProvider); !ok {
		return
	}
	smap := p.smapowner.get()
	si, errstr := hrwTarget(bucket, objname, smap)
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}
	if glog.V(4) {
		glog.Infof("%s %s/%s => %s", r.Method, bucket, objname, si)
	}
	var redirecturl string
	if replica, _ := isReplicationPUT(r); !replica {
		// regular PUT
		redirecturl = p.redirectURL(r, si.PublicNet.DirectURL, started, bucket)
	} else {
		// replication PUT
		redirecturl = p.redirectURL(r, si.IntraDataNet.DirectURL, started, bucket)
	}
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)

	p.statsif.Add(stats.PutCount, 1)
}

// DELETE { action } /v1/buckets
func (p *proxyrunner) httpbckdelete(w http.ResponseWriter, r *http.Request) {
	var (
		msg            cmn.ActionMsg
		bckIsLocal, ok bool
	)

	apitems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}
	bucket := apitems[0]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	if bckIsLocal, ok = p.validateBucket(w, r, bucket, bckProvider); !ok {
		return
	}
	switch msg.Action {
	case cmn.ActDestroyLB:
		if p.forwardCP(w, r, &msg, bucket, nil) {
			return
		}
		p.bmdowner.Lock()
		bucketmd := p.bmdowner.get()
		if !bucketmd.IsLocal(bucket) {
			p.bmdowner.Unlock()
			p.invalmsghdlr(w, r, fmt.Sprintf("Bucket %s does not appear to be local or %s",
				bucket, cmn.DoesNotExist))
			return
		}
		clone := bucketmd.clone()
		if !clone.del(bucket, true) {
			p.bmdowner.Unlock()
			s := fmt.Sprintf("Local bucket %s "+cmn.DoesNotExist, bucket)
			p.invalmsghdlr(w, r, s)
			return
		}
		if errstr := p.savebmdconf(clone, cmn.GCO.Get()); errstr != "" {
			p.bmdowner.Unlock()
			p.invalmsghdlr(w, r, errstr)
			return
		}
		p.bmdowner.put(clone)
		p.bmdowner.Unlock()

		msgInt := p.newActionMsgInternal(&msg, nil, clone)
		p.metasyncer.sync(true, revspair{clone, msgInt})
	case cmn.ActEvictCB:
		if bckProvider == "" {
			p.invalmsghdlr(w, r, fmt.Sprintf("%s is empty. Please specify '?%s=%s'",
				cmn.URLParamBckProvider, cmn.URLParamBckProvider, cmn.CloudBs))
			return
		} else if bckIsLocal {
			p.invalmsghdlr(w, r, fmt.Sprintf("Bucket %s appears to be local (not cloud)", bucket))
			return
		}
		if p.forwardCP(w, r, &msg, bucket, nil) {
			return
		}
		bucketmd := p.bmdowner.get()
		if bucketmd.isPresent(bucket, bckIsLocal) {
			p.bmdowner.Lock()
			clone := bucketmd.clone()
			clone.del(bucket, false)
			if errstr := p.savebmdconf(clone, cmn.GCO.Get()); errstr != "" {
				p.bmdowner.Unlock()
				p.invalmsghdlr(w, r, errstr)
				return
			}
			p.bmdowner.put(clone)
			p.bmdowner.Unlock()
			bucketmd = clone

			msgInt := p.newActionMsgInternal(&msg, nil, clone)
			p.metasyncer.sync(true, revspair{clone, msgInt})
		} else {
			if glog.V(3) {
				glog.Infof("%s: %s %s - nothing to do", cmn.ActEvictCB, bucket, cmn.DoesNotExist)
			}
		}

		msgInt := p.newActionMsgInternal(&msg, nil, bucketmd)
		jsonbytes, err := jsoniter.Marshal(msgInt)
		cmn.AssertNoErr(err)

		// wipe the local copy of the cloud bucket on targets
		smap := p.smapowner.get()
		results := p.broadcastTo(
			cmn.URLPath(cmn.Version, cmn.Buckets, bucket),
			nil, // query
			http.MethodDelete,
			jsonbytes,
			smap,
			defaultTimeout,
			cmn.NetworkIntraControl,
			cluster.Targets,
		)
		for result := range results {
			if result.err != nil {
				p.invalmsghdlr(w, r, fmt.Sprintf("%s failed, err: %s", msg.Action, result.errstr))
				return
			}
		}

	case cmn.ActDelete, cmn.ActEvictObjects:
		if msg.Action == cmn.ActEvictObjects {
			if bckProvider == "" {
				p.invalmsghdlr(w, r, fmt.Sprintf("%s is empty. Please specify '?%s=%s'",
					cmn.URLParamBckProvider, cmn.URLParamBckProvider, cmn.CloudBs))
				return
			} else if bckIsLocal {
				p.invalmsghdlr(w, r, fmt.Sprintf("Bucket %s appears to be local (not cloud)", bucket))
				return
			}
		}
		p.listRangeHandler(w, r, &msg, http.MethodDelete, bckProvider)
	default:
		p.invalmsghdlr(w, r, fmt.Sprintf("Unsupported Action: %s", msg.Action))
	}
}

// DELETE /v1/objects/object-name
func (p *proxyrunner) httpobjdelete(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	apitems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	if _, ok := p.validateBucket(w, r, bucket, bckProvider); !ok {
		return
	}
	smap := p.smapowner.get()
	si, errstr := hrwTarget(bucket, objname, smap)
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}
	if glog.V(4) {
		glog.Infof("%s %s/%s => %s", r.Method, bucket, objname, si)
	}
	redirecturl := p.redirectURL(r, si.PublicNet.DirectURL, started, bucket)
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)

	p.statsif.Add(stats.DeleteCount, 1)
}

// [METHOD] /v1/metasync
func (p *proxyrunner) metasyncHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		p.metasyncHandlerPut(w, r)
	case http.MethodPost:
		p.metasyncHandlerPost(w, r)
	default:
		p.invalmsghdlr(w, r, "invalid method for metasync", http.StatusBadRequest)
	}
}

// PUT /v1/metasync
func (p *proxyrunner) metasyncHandlerPut(w http.ResponseWriter, r *http.Request) {
	// FIXME: may not work if got disconnected for a while and have missed elections (#109)
	if p.smapowner.get().isPrimary(p.si) {
		_, xx := p.xactions.findL(cmn.ActElection)
		vote := xx != nil
		s := fmt.Sprintf("Primary %s cannot receive cluster meta (election=%t)", p.si, vote)
		p.invalmsghdlr(w, r, s)
		return
	}
	var payload = make(cmn.SimpleKVs)
	if err := cmn.ReadJSON(w, r, &payload); err != nil {
		return
	}

	newsmap, _, errstr := p.extractSmap(payload)
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}

	if newsmap != nil {
		errstr = p.smapowner.synchronize(newsmap, true /*saveSmap*/, true /* lesserIsErr */)
		if errstr != "" {
			p.invalmsghdlr(w, r, errstr)
			return
		}
		if !newsmap.isPresent(p.si) {
			s := fmt.Sprintf("Warning: not finding self '%s' in the received %s", p.si, newsmap.pp())
			glog.Errorln(s)
		}
	}

	newbucketmd, actionlb, errstr := p.extractbucketmd(payload)
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}
	if newbucketmd != nil {
		if errstr = p.receiveBucketMD(newbucketmd, actionlb); errstr != "" {
			p.invalmsghdlr(w, r, errstr)
			return
		}
	}

	revokedTokens, errstr := p.extractRevokedTokenList(payload)
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}
	p.authn.updateRevokedList(revokedTokens)
}

// POST /v1/metasync
func (p *proxyrunner) metasyncHandlerPost(w http.ResponseWriter, r *http.Request) {
	var msgInt actionMsgInternal
	if err := cmn.ReadJSON(w, r, &msgInt); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	glog.Infof("metasync NIY: %+v", msgInt) // TODO
}

// GET /v1/health
func (p *proxyrunner) healthHandler(w http.ResponseWriter, r *http.Request) {
	rr := getproxystatsrunner()
	v := rr.Core.Tracker[stats.Uptime]
	v.Lock()
	v.Value = int64(time.Since(p.starttime) / time.Microsecond)
	if p.startedup(0) == 0 {
		rr.Core.Tracker[stats.Uptime].Value = 0
	}
	v.Unlock()
	jsbytes, err := jsoniter.Marshal(rr.Core)

	cmn.AssertNoErr(err)
	p.writeJSON(w, r, jsbytes, "proxycorestats")
}

func (p *proxyrunner) createLocalBucket(msg *cmn.ActionMsg, bucket string) error {
	config := cmn.GCO.Get()

	p.bmdowner.Lock()
	clone := p.bmdowner.get().clone()
	bucketProps := cmn.DefaultBucketProps()
	if !clone.add(bucket, true, bucketProps) {
		p.bmdowner.Unlock()
		return fmt.Errorf("local bucket %s already exists", bucket)
	}
	if errstr := p.savebmdconf(clone, config); errstr != "" {
		p.bmdowner.Unlock()
		return errors.New(errstr)
	}
	p.bmdowner.put(clone)
	p.bmdowner.Unlock()
	msgInt := p.newActionMsgInternal(msg, nil, clone)
	p.metasyncer.sync(true, revspair{clone, msgInt})
	return nil
}

// POST { action } /v1/buckets/bucket-name
func (p *proxyrunner) httpbckpost(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	var (
		msg            cmn.ActionMsg
		bckIsLocal, ok bool
	)
	apitems, err := p.checkRESTItems(w, r, 1, true, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	if bckIsLocal, ok = p.validateBucket(w, r, bucket, bckProvider); !ok {
		return
	}

	if len(apitems) > 1 {
		switch apitems[1] {
		case cmn.ActListObjects:
			p.listBucketAndCollectStats(w, r, bucket, bckProvider, msg, started, true /* fast listing */)
			return
		default:
			p.invalmsghdlr(w, r, fmt.Sprintf("Invalid bucket action: %s", apitems[1]))
			return
		}
	}

	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}
	config := cmn.GCO.Get()
	switch msg.Action {
	case cmn.ActCreateLB:
		if p.forwardCP(w, r, &msg, bucket, nil) {
			return
		}
		if err := p.createLocalBucket(&msg, bucket); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
	case cmn.ActRenameLB:
		if p.forwardCP(w, r, &msg, "", nil) {
			return
		}
		bucketFrom, bucketTo := bucket, msg.Name
		if bucketFrom == "" || bucketTo == "" {
			errstr := fmt.Sprintf("Invalid rename local bucket request: empty name %q or %q", bucketFrom, bucketTo)
			p.invalmsghdlr(w, r, errstr)
			return
		}
		clone := p.bmdowner.get().clone()
		props, ok := clone.Get(bucketFrom, true)
		if !ok {
			s := fmt.Sprintf("Local bucket %s "+cmn.DoesNotExist, bucketFrom)
			p.invalmsghdlr(w, r, s, http.StatusNotFound)
			return
		}
		if _, ok = clone.Get(bucketTo, true); ok {
			s := fmt.Sprintf("Local bucket %s already exists", bucketTo)
			p.invalmsghdlr(w, r, s)
			return
		}
		if !p.renameLB(bucketFrom, bucketTo, clone, props, &msg, config) {
			errstr := fmt.Sprintf("Failed to rename local bucket %s => %s", bucketFrom, bucketTo)
			p.invalmsghdlr(w, r, errstr)
			return
		}
		glog.Infof("renamed local bucket %s => %s, %s v%d", bucketFrom, bucketTo, bmdTermName, clone.version())
	case cmn.ActSyncLB:
		if p.forwardCP(w, r, &msg, "", nil) {
			return
		}
		bmd := p.bmdowner.get()
		msgInt := p.newActionMsgInternal(&msg, nil, bmd)
		p.metasyncer.sync(false, revspair{bmd, msgInt})
	case cmn.ActPrefetch:
		// Check that users didn't specify bprovider=cloud
		if bckProvider == "" {
			p.invalmsghdlr(w, r, fmt.Sprintf("%s is empty. Please specify '?%s=%s'",
				cmn.URLParamBckProvider, cmn.URLParamBckProvider, cmn.CloudBs))
			return
		} else if bckIsLocal {
			p.invalmsghdlr(w, r, fmt.Sprintf("Bucket %s appears to be local (not cloud)", bucket))
			return
		}
		p.listRangeHandler(w, r, &msg, http.MethodPost, bckProvider)
	case cmn.ActListObjects:
		p.listBucketAndCollectStats(w, r, bucket, bckProvider, msg, started, false /* fast listing */)
	case cmn.ActMakeNCopies:
		p.makeNCopies(w, r, bucket, &msg, config, bckIsLocal)
	default:
		s := fmt.Sprintf("Unexpected cmn.ActionMsg <- JSON [%v]", msg)
		p.invalmsghdlr(w, r, s)
	}
}

func (p *proxyrunner) listBucketAndCollectStats(w http.ResponseWriter, r *http.Request,
	bucket, bckProvider string, actionMsg cmn.ActionMsg, started time.Time, fastListing bool) {
	var (
		msg     = cmn.SelectMsg{}
		bckList *cmn.BucketList
		query   = r.URL.Query()
	)

	listMsgJSON, err := jsoniter.Marshal(actionMsg.Value)
	if err != nil {
		s := fmt.Sprintf("Unable to marshal action message: %v. Error: %v", actionMsg, err)
		p.invalmsghdlr(w, r, s)
		return
	}
	if err = jsoniter.Unmarshal(listMsgJSON, &msg); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	// override fastListing if it set
	if fastListing {
		msg.Fast = fastListing
	}
	// override prefix if it is set in URL query values
	if prefix := query.Get(cmn.URLParamPrefix); prefix != "" {
		msg.Prefix = prefix
	}

	if bckList, err = p.listBucket(r, bucket, bckProvider, msg); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	b, err := jsoniter.Marshal(bckList)
	cmn.AssertNoErr(err)
	pageMarker := bckList.PageMarker
	bckList.Entries = nil
	bckList = nil

	if p.writeJSON(w, r, b, "listbucket") {
		delta := time.Since(started)
		p.statsif.AddMany(stats.NamedVal64{stats.ListCount, 1}, stats.NamedVal64{stats.ListLatency, int64(delta)})
		if glog.FastV(4, glog.SmoduleAIS) {
			lat := int64(delta / time.Microsecond)
			if pageMarker != "" {
				glog.Infof("LIST: %s, page %s, %d µs", bucket, pageMarker, lat)
			} else {
				glog.Infof("LIST: %s, %d µs", bucket, lat)
			}
		}
	}
}

// POST { action } /v1/objects/bucket-name
func (p *proxyrunner) httpobjpost(w http.ResponseWriter, r *http.Request) {
	var msg cmn.ActionMsg
	apitems, err := p.checkRESTItems(w, r, 1, true, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	// We only support actions for local buckets
	bucket := apitems[0]
	if _, ok := p.validateBucket(w, r, bucket, cmn.LocalBs); !ok {
		return
	}
	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case cmn.ActRename:
		p.filrename(w, r, &msg)
		return
	case cmn.ActReplicate:
		p.replicate(w, r, &msg)
		return
	default:
		s := fmt.Sprintf("Unexpected cmn.ActionMsg <- JSON [%v]", msg)
		p.invalmsghdlr(w, r, s)
		return
	}
}

// HEAD /v1/buckets/bucket-name
func (p *proxyrunner) httpbckhead(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	apitems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	if _, ok := p.validateBucket(w, r, bucket, bckProvider); !ok {
		return
	}

	var si *cluster.Snode
	// Use random map iteration order to choose a random target to redirect to
	smap := p.smapowner.get()
	for _, si = range smap.Tmap {
		break
	}
	if glog.V(3) {
		glog.Infof("%s %s => %s", r.Method, bucket, si)
	}
	redirecturl := p.redirectURL(r, si.PublicNet.DirectURL, started, bucket)
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)
}

func (p *proxyrunner) updateBucketProps(bucket string, bckIsLocal bool, nvs cmn.SimpleKVs) (errstr string) {
	const errFmt = "Invalid %s value %q: %v"
	p.bmdowner.Lock()
	clone := p.bmdowner.get().clone()
	config := cmn.GCO.Get()

	bprops, exists := clone.Get(bucket, bckIsLocal)
	if !exists {
		cmn.Assert(!bckIsLocal)
		bprops = cmn.DefaultBucketProps()
		clone.add(bucket, false /* bucket is local */, bprops)
	}

	// HTTP headers display property names title-cased so that LRULowWM becomes Lrulowwm, etc.
	// - make sure to lowercase therefore
	for key, val := range nvs {
		name, value := strings.ToLower(key), val
		if glog.V(4) {
			glog.Infof("Updating bucket %s property %s => %s", bucket, name, value)
		}
		// Disable ability to set EC properties once enabled
		if !bprops.EC.Updatable(name) {
			errstr = "Cannot change EC configuration after it is enabled"
			p.bmdowner.Unlock()
			return
		}

		switch name {
		case cmn.HeaderBucketECEnabled:
			if v, err := strconv.ParseBool(value); err == nil {
				if v {
					if bprops.EC.DataSlices == 0 {
						bprops.EC.DataSlices = 2
					}
					if bprops.EC.ParitySlices == 0 {
						bprops.EC.ParitySlices = 2
					}
				}
				bprops.EC.Enabled = v
			} else {
				errstr = fmt.Sprintf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketECData:
			if v, err := cmn.ParseIntRanged(value, 10, 32, 1, 32); err == nil {
				bprops.EC.DataSlices = int(v)
			} else {
				errstr = fmt.Sprintf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketECParity:
			if v, err := cmn.ParseIntRanged(value, 10, 32, 1, 32); err == nil {
				bprops.EC.ParitySlices = int(v)
			} else {
				errstr = fmt.Sprintf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketECMinSize:
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				bprops.EC.ObjSizeLimit = v
			} else {
				errstr = fmt.Sprintf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketCopies:
			if v, err := cmn.ParseIntRanged(value, 10, 32, 1, mirror.MaxNCopies); err == nil {
				bprops.Mirror.Copies = v
			} else {
				errstr = fmt.Sprintf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketMirrorThresh:
			if v, err := cmn.ParseIntRanged(value, 10, 64, 0, 100); err == nil {
				bprops.Mirror.UtilThresh = v
			} else {
				errstr = fmt.Sprintf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketMirrorEnabled:
			if v, err := strconv.ParseBool(value); err == nil {
				bprops.Mirror.Enabled = v
			} else {
				errstr = fmt.Sprintf(errFmt, name, value, err)
			}
		default:
			errstr = fmt.Sprintf("Changing property %s is not supported", name)
		}

		if errstr != "" {
			p.bmdowner.Unlock()
			return
		}
	}

	clone.set(bucket, bckIsLocal, bprops)
	if e := p.savebmdconf(clone, config); e != "" {
		glog.Errorln(e)
	}
	p.bmdowner.put(clone)
	p.bmdowner.Unlock()
	msgInt := p.newActionMsgInternalStr(cmn.ActSetProps, nil, clone)
	p.metasyncer.sync(true, revspair{clone, msgInt})
	return
}

// PUT /v1/buckets/bucket-name
// PUT /v1/buckets/bucket-name/setprops
func (p *proxyrunner) httpbckput(w http.ResponseWriter, r *http.Request) {
	var bckIsLocal, ok bool
	apitems, err := p.checkRESTItems(w, r, 1, true, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	if bckIsLocal, ok = p.validateBucket(w, r, bucket, bckProvider); !ok {
		return
	}

	// Setting bucket props using URL query strings
	if len(apitems) > 1 {
		if apitems[1] != cmn.ActSetProps {
			s := fmt.Sprintf("Invalid request [%s] - expecting '%s'", apitems[1], cmn.ActSetProps)
			p.invalmsghdlr(w, r, s)
			return
		}

		query := r.URL.Query()
		query.Del(cmn.URLParamBckProvider)
		kvs := cmn.NewSimpleKVsFromQuery(query)

		if errstr := p.updateBucketProps(bucket, bckIsLocal, kvs); errstr != "" {
			p.invalmsghdlr(w, r, errstr)
		}
		return
	}

	// otherwise, handle the general case: unmarshal into cmn.BucketProps and treat accordingly
	// Note: this use case is for setting all bucket props
	nprops := cmn.DefaultBucketProps()
	msg := cmn.ActionMsg{Value: nprops}
	if err = cmn.ReadJSON(w, r, &msg); err != nil {
		s := fmt.Sprintf("Failed to unmarshal: %v", err)
		p.invalmsghdlr(w, r, s)
		return
	}

	if msg.Action != cmn.ActSetProps && msg.Action != cmn.ActResetProps {
		s := fmt.Sprintf("Invalid cmn.ActionMsg [%v] - expecting '%s' or '%s' action", msg, cmn.ActSetProps, cmn.ActResetProps)
		p.invalmsghdlr(w, r, s)
		return
	}

	p.bmdowner.Lock()
	clone := p.bmdowner.get().clone()
	config := cmn.GCO.Get()

	bprops, exists := clone.Get(bucket, bckIsLocal)
	if !exists {
		cmn.Assert(!bckIsLocal)
		bprops = cmn.DefaultBucketProps()
		clone.add(bucket, false /* bucket is local */, bprops)
	}

	switch msg.Action {
	case cmn.ActSetProps:
		targetCnt := p.smapowner.Get().CountTargets()
		if err := nprops.Validate(bckIsLocal, targetCnt, p.urlOutsideCluster); err != nil {
			p.bmdowner.Unlock()
			p.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
			return
		}

		// forbid changing EC properties if EC is already enabled (for now)
		if bprops.EC.Enabled && nprops.EC.Enabled &&
			(bprops.EC.ParitySlices != nprops.EC.ParitySlices ||
				bprops.EC.DataSlices != nprops.EC.DataSlices ||
				bprops.EC.ObjSizeLimit != nprops.EC.ObjSizeLimit) {
			p.bmdowner.Unlock()
			p.invalmsghdlr(w, r, "Cannot change EC configuration after it is enabled",
				http.StatusBadRequest)
			return
		}

		recksum := rechecksumRequired(cmn.GCO.Get().Cksum.Type, bprops.Cksum.Type, nprops.Cksum.Type)
		bprops.CopyFrom(nprops)
		if recksum {
			go p.notifyTargetsRechecksum(bucket)
		}
	case cmn.ActResetProps:
		if bprops.EC.Enabled {
			p.bmdowner.Unlock()
			p.invalmsghdlr(w, r, "Cannot reset bucket properties after EC is enabled",
				http.StatusBadRequest)
			return
		}
		bprops = cmn.DefaultBucketProps()
	}
	clone.set(bucket, bckIsLocal, bprops)
	if e := p.savebmdconf(clone, config); e != "" {
		glog.Errorln(e)
	}
	p.bmdowner.put(clone)
	p.bmdowner.Unlock()

	msgInt := p.newActionMsgInternal(&msg, nil, clone)
	p.metasyncer.sync(true, revspair{clone, msgInt})
}

// HEAD /v1/objects/bucket-name/object-name
func (p *proxyrunner) httpobjhead(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	query := r.URL.Query()
	checkCached, _ := parsebool(query.Get(cmn.URLParamCheckCached))
	apitems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	bckProvider := query.Get(cmn.URLParamBckProvider)
	if _, ok := p.validateBucket(w, r, bucket, bckProvider); !ok {
		return
	}

	smap := p.smapowner.get()
	si, errstr := hrwTarget(bucket, objname, smap)
	if errstr != "" {
		return
	}
	if glog.V(4) {
		glog.Infof("%s %s/%s => %s", r.Method, bucket, objname, si)
	}
	redirecturl := p.redirectURL(r, si.PublicNet.DirectURL, started, bucket)
	if checkCached {
		redirecturl += fmt.Sprintf("&%s=true", cmn.URLParamCheckCached)
	}
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)
}

//============================
//
// supporting methods and misc
//
//============================
// forward control plane request to the current primary proxy
// return: forf (forwarded or failed) where forf = true means exactly that: forwarded or failed
func (p *proxyrunner) forwardCP(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg, s string, body []byte) (forf bool) {
	smap := p.smapowner.get()
	if smap == nil || !smap.isValid() {
		s := fmt.Sprintf("%s must be starting up: cannot execute %s:%s", p.si, msg.Action, s)
		p.invalmsghdlr(w, r, s)
		return true
	}
	if smap.isPrimary(p.si) {
		return
	}
	if body == nil {
		var err error
		body, err = jsoniter.Marshal(msg)
		cmn.AssertNoErr(err)
	}
	p.rproxy.Lock()
	if p.rproxy.u != smap.ProxySI.PublicNet.DirectURL {
		p.rproxy.u = smap.ProxySI.PublicNet.DirectURL
		uparsed, err := url.Parse(smap.ProxySI.PublicNet.DirectURL)
		cmn.AssertNoErr(err)
		p.rproxy.p = httputil.NewSingleHostReverseProxy(uparsed)
		p.rproxy.p.Transport = cmn.NewTransport(cmn.ClientArgs{
			UseHTTPS: cmn.GCO.Get().Net.HTTP.UseHTTPS,
		})
	}
	p.rproxy.Unlock()
	if len(body) > 0 {
		r.Body = ioutil.NopCloser(bytes.NewBuffer(body))
		r.ContentLength = int64(len(body)) // directly setting content-length
	}
	glog.Infof("%s: forwarding '%s:%s' to the primary %s", p.si.Name(), msg.Action, s, smap.ProxySI)
	p.rproxy.p.ServeHTTP(w, r)
	return true
}

// reverse-proxy GET object request
func (p *proxyrunner) reverseDP(w http.ResponseWriter, r *http.Request, tsi *cluster.Snode) {
	p.rproxy.Lock()
	if p.rproxy.tmap == nil {
		smap := p.smapowner.get()
		p.rproxy.tmap = make(map[string]*httputil.ReverseProxy, smap.CountTargets())
	}
	rproxy, ok := p.rproxy.tmap[tsi.DaemonID]
	if !ok || rproxy == nil {
		uparsed, err := url.Parse(tsi.PublicNet.DirectURL)
		cmn.AssertNoErr(err)
		rproxy = httputil.NewSingleHostReverseProxy(uparsed)
		rproxy.Transport = cmn.NewTransport(cmn.ClientArgs{
			UseHTTPS: cmn.GCO.Get().Net.HTTP.UseHTTPS,
		})
		p.rproxy.tmap[tsi.DaemonID] = rproxy
	}
	p.rproxy.Unlock()
	rproxy.ServeHTTP(w, r)
}

func (p *proxyrunner) renameLB(bucketFrom, bucketTo string, clone *bucketMD, props *cmn.BucketProps,
	actionMsg *cmn.ActionMsg, config *cmn.Config) bool {
	smap4bcast := p.smapowner.get()
	msgInt := p.newActionMsgInternal(actionMsg, smap4bcast, clone)
	jsbytes, err := jsoniter.Marshal(msgInt)
	cmn.AssertNoErr(err)

	res := p.broadcastTo(
		cmn.URLPath(cmn.Version, cmn.Buckets, bucketFrom),
		nil, // query
		http.MethodPost,
		jsbytes,
		smap4bcast,
		config.Timeout.Default,
		cmn.NetworkIntraControl,
		cluster.Targets,
	)

	for r := range res {
		if r.err != nil {
			glog.Errorf("Target %s failed to rename local bucket %s => %s, err: %v (%d)",
				r.si.DaemonID, bucketFrom, bucketTo, r.err, r.status)
			return false // FIXME
		}
	}

	p.bmdowner.Lock()
	clone = p.bmdowner.get().clone()
	clone.del(bucketFrom, true)
	clone.add(bucketTo, true, props)
	if errstr := p.savebmdconf(clone, config); errstr != "" {
		glog.Errorln(errstr)
	}
	p.bmdowner.put(clone)
	p.bmdowner.Unlock()
	msgInt = p.newActionMsgInternal(actionMsg, smap4bcast, clone)
	p.metasyncer.sync(true, revspair{clone, msgInt})
	return true
}

func (p *proxyrunner) makeNCopies(w http.ResponseWriter, r *http.Request, bucket string,
	actionMsg *cmn.ActionMsg, cfg *cmn.Config, bckIsLocal bool) {
	copies, err := p.parseValidateNCopies(actionMsg.Value)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	smap := p.smapowner.get()
	msgInt := p.newActionMsgInternal(actionMsg, smap, nil)
	jsbytes, err := jsoniter.Marshal(msgInt)
	cmn.AssertNoErr(err)
	results := p.broadcastTo(
		cmn.URLPath(cmn.Version, cmn.Buckets, bucket),
		nil, // query
		http.MethodPost,
		jsbytes,
		smap,
		cfg.Timeout.CplaneOperation,
		cmn.NetworkIntraControl,
		cluster.Targets,
	)
	for res := range results {
		if res.err != nil {
			s := fmt.Sprintf("Failed to make-num-copies: %s, bucket %s, err: %v(%d)",
				res.si.Name(), bucket, res.err, res.status)
			if res.errstr != "" {
				glog.Errorln(res.errstr)
			}
			p.invalmsghdlr(w, r, s)
			return
		}
	}
	// enable/disable accordingly and
	// finalize via updating the bucket's mirror configuration (compare with httpbckput)
	var (
		nvs       cmn.SimpleKVs
		copiesStr = strconv.FormatInt(int64(copies), 10)
	)
	if copies > 1 {
		nvs = cmn.SimpleKVs{cmn.HeaderBucketCopies: copiesStr, cmn.HeaderBucketMirrorEnabled: "true"}
	} else {
		cmn.Assert(copies == 1)
		nvs = cmn.SimpleKVs{cmn.HeaderBucketCopies: copiesStr, cmn.HeaderBucketMirrorEnabled: "false"}
	}
	//
	// NOTE: cmn.ActMakeNCopies automatically does (copies > 1) ? enable : disable on the bucket's MirrorConf
	//
	if errstr := p.updateBucketProps(bucket, bckIsLocal, nvs); errstr != "" {
		p.invalmsghdlr(w, r, errstr)
	}
}

func (p *proxyrunner) getbucketnames(w http.ResponseWriter, r *http.Request, bucketspec, bckProvider string) {
	bucketmd := p.bmdowner.get()
	bckProviderStr := "?" + cmn.URLParamBckProvider + "=" + bckProvider
	if bckProvider == cmn.LocalBs {
		bucketNames := &cmn.BucketNames{Cloud: []string{}, Local: make([]string, 0, 64)}
		for bucket := range bucketmd.LBmap {
			bucketNames.Local = append(bucketNames.Local, bucket)
		}
		jsbytes, err := jsoniter.Marshal(bucketNames)
		cmn.AssertNoErr(err)
		p.writeJSON(w, r, jsbytes, "getbucketnames"+bckProviderStr)
		return
	}

	var si *cluster.Snode
	smap := p.smapowner.get()
	for _, si = range smap.Tmap {
		break
	}
	args := callArgs{
		si: si,
		req: reqArgs{
			method: r.Method,
			header: r.Header,
			path:   cmn.URLPath(cmn.Version, cmn.Buckets, cmn.ListAll),
			query:  r.URL.Query(),
		},
		timeout: defaultTimeout,
	}
	res := p.call(args)
	if res.err != nil {
		p.invalmsghdlr(w, r, res.errstr)
		p.keepalive.onerr(res.err, res.status)
	} else {
		p.writeJSON(w, r, res.outjson, "getbucketnames"+bckProviderStr)
	}
}

func (p *proxyrunner) redirectURL(r *http.Request, to string, ts time.Time, bucket string) (redirect string) {
	var (
		query    = url.Values{}
		bucketmd = p.bmdowner.get()
	)
	redirect = to + r.URL.Path + "?"
	if r.URL.RawQuery != "" {
		redirect += r.URL.RawQuery + "&"
	}

	query.Add(cmn.URLParamProxyID, p.si.DaemonID)
	query.Add(cmn.URLParamBMDVersion, bucketmd.vstr)
	query.Add(cmn.URLParamUnixTime, strconv.FormatInt(int64(ts.UnixNano()), 10))
	redirect += query.Encode()
	return
}

// For cached = false goes to the Cloud, otherwise returns locally cached files
func (p *proxyrunner) targetListBucket(r *http.Request, bucket, bckProvider string, dinfo *cluster.Snode,
	getMsg *cmn.SelectMsg, cached bool) (*bucketResp, error) {
	bmd := p.bmdowner.get()
	actionMsgBytes, err := jsoniter.Marshal(p.newActionMsgInternal(&cmn.ActionMsg{Action: cmn.ActListObjects, Value: getMsg}, nil, bmd))
	if err != nil {
		return &bucketResp{
			outjson: nil,
			err:     err,
			id:      dinfo.DaemonID,
		}, err
	}

	var header http.Header
	if r != nil {
		header = r.Header
	}

	query := url.Values{}
	query.Add(cmn.URLParamBckProvider, bckProvider)
	query.Add(cmn.URLParamCached, strconv.FormatBool(cached))
	query.Add(cmn.URLParamBMDVersion, bmd.vstr)

	args := callArgs{
		si: dinfo,
		req: reqArgs{
			method: http.MethodPost,
			header: header,
			base:   dinfo.URL(cmn.NetworkPublic),
			path:   cmn.URLPath(cmn.Version, cmn.Buckets, bucket),
			query:  query,
			body:   actionMsgBytes,
		},
		timeout: cmn.GCO.Get().Timeout.List,
	}
	res := p.call(args)
	if res.err != nil {
		p.keepalive.onerr(res.err, res.status)
	}

	return &bucketResp{
		outjson: res.outjson,
		err:     res.err,
		id:      dinfo.DaemonID,
	}, res.err
}

// Receives and aggregates info on locally cached objects and merges the two lists
func (p *proxyrunner) consumeCachedList(bmap map[string]*cmn.BucketEntry, dataCh chan *localFilePage, errCh chan error) {
	for rb := range dataCh {
		if rb.err != nil {
			if errCh != nil {
				errCh <- rb.err
			}
			glog.Errorf("Failed to get local object info: %v", rb.err)
			return
		}
		if rb.entries == nil || len(rb.entries) == 0 {
			continue
		}

		for _, newEntry := range rb.entries {
			nm := newEntry.Name
			// Do not update the entry if it already contains up-to-date information
			if entry, ok := bmap[nm]; ok && !entry.IsCached {
				entry.Atime = newEntry.Atime
				entry.Status = newEntry.Status
				// Status not OK means the object is temporarily misplaced and
				// the object cannot be marked as cached.
				// Such objects will retrieve data from Cloud on GET request
				entry.IsCached = newEntry.Status == cmn.ObjStatusOK
				entry.Copies = newEntry.Copies
			}
		}
	}
}

// Request list of all cached files from a target.
// The target returns its list in batches `pageSize` length
func (p *proxyrunner) generateCachedList(bucket, bckProvider string, daemon *cluster.Snode, dataCh chan *localFilePage, origmsg *cmn.SelectMsg) {
	var msg cmn.SelectMsg
	cmn.CopyStruct(&msg, origmsg)
	msg.PageSize = internalPageSize
	for {
		resp, err := p.targetListBucket(nil, bucket, bckProvider, daemon, &msg, true /* cachedObjects */)
		if err != nil {
			if dataCh != nil {
				dataCh <- &localFilePage{
					id:  daemon.DaemonID,
					err: err,
				}
			}
			glog.Errorf("Failed to get cached objects info from %s: %v", daemon.Name(), err)
			return
		}

		if resp.outjson == nil || len(resp.outjson) == 0 {
			return
		}

		entries := cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, 128)}
		if err := jsoniter.Unmarshal(resp.outjson, &entries); err != nil {
			if dataCh != nil {
				dataCh <- &localFilePage{
					id:  daemon.DaemonID,
					err: err,
				}
			}
			glog.Errorf("Failed to unmarshall cached objects list from %s: %v", daemon.Name(), err)
			return
		}

		msg.PageMarker = entries.PageMarker
		if dataCh != nil {
			dataCh <- &localFilePage{
				err:     nil,
				id:      daemon.DaemonID,
				marker:  entries.PageMarker,
				entries: entries.Entries,
			}
		}

		// empty PageMarker means that there are no more files to
		// return. So, the loop can be interrupted
		if len(entries.Entries) == 0 || entries.PageMarker == "" {
			break
		}
	}
}

// Get list of cached files from all targets and update the list
// of files from cloud with local metadata (iscached, atime etc)
func (p *proxyrunner) collectCachedFileList(bucket, bckProvider string, fileList *cmn.BucketList, msg cmn.SelectMsg) (err error) {
	bucketMap := make(map[string]*cmn.BucketEntry, initialBucketListSize)
	for _, entry := range fileList.Entries {
		bucketMap[entry.Name] = entry
	}

	smap := p.smapowner.get()
	dataCh := make(chan *localFilePage, smap.CountTargets())
	errCh := make(chan error, 1)
	wgConsumer := &sync.WaitGroup{}
	wgConsumer.Add(1)
	go func() {
		p.consumeCachedList(bucketMap, dataCh, errCh)
		wgConsumer.Done()
	}()

	// since cached file page marker is not compatible with any cloud
	// marker, it should be empty for the first call
	msg.PageMarker = ""

	wg := &sync.WaitGroup{}
	for _, daemon := range smap.Tmap {
		wg.Add(1)
		// without this copy all goroutines get the same pointer
		go func(d *cluster.Snode) {
			p.generateCachedList(bucket, bckProvider, d, dataCh, &msg)
			wg.Done()
		}(daemon)
	}
	wg.Wait()
	close(dataCh)
	wgConsumer.Wait()

	select {
	case err = <-errCh:
		return
	default:
	}

	fileList.Entries = make([]*cmn.BucketEntry, 0, len(bucketMap))
	for _, entry := range bucketMap {
		fileList.Entries = append(fileList.Entries, entry)
	}

	// sort the result to be consistent with other API
	ifLess := func(i, j int) bool {
		return fileList.Entries[i].Name < fileList.Entries[j].Name
	}
	sort.Slice(fileList.Entries, ifLess)

	return
}

func (p *proxyrunner) getLocalBucketObjects(bucket, bckProvider string, msg cmn.SelectMsg) (allEntries *cmn.BucketList, err error) {
	type targetReply struct {
		resp *bucketResp
		err  error
	}
	const (
		cachedObjs = false
	)
	pageSize := cmn.DefaultPageSize
	if msg.PageSize != 0 {
		pageSize = msg.PageSize
	}

	if pageSize > maxPageSize {
		glog.Warningf("Page size(%d) for local bucket %s exceeds the limit(%d)", msg.PageSize, bucket, maxPageSize)
	}

	smap := p.smapowner.get()
	targetResults := make(chan *targetReply, len(smap.Tmap))
	wg := &sync.WaitGroup{}

	targetCallFn := func(si *cluster.Snode) {
		resp, err := p.targetListBucket(nil, bucket, bckProvider, si, &msg, cachedObjs)
		targetResults <- &targetReply{resp, err}
		wg.Done()
	}
	smap = p.smapowner.get()
	for _, si := range smap.Tmap {
		wg.Add(1)
		go func(d *cluster.Snode) {
			targetCallFn(d)
		}(si)
	}
	wg.Wait()
	close(targetResults)

	// combine results
	allEntries = &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, pageSize)}
	bucketList := &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, pageSize)}
	for r := range targetResults {
		if r.err != nil {
			err = r.err
			return
		}
		if r.resp.outjson == nil || len(r.resp.outjson) == 0 {
			continue
		}

		// a small performance boost: preallocate memory either for the page
		// or for the size of allocated for previous target. When object are
		// evenly distributed the latter can help to avoid reallocations
		bucketList.Entries = make([]*cmn.BucketEntry, 0, cmn.Max(pageSize, len(bucketList.Entries)))
		if err = jsoniter.Unmarshal(r.resp.outjson, &bucketList); err != nil {
			return
		}
		r.resp.outjson = nil

		if len(bucketList.Entries) == 0 {
			continue
		}

		allEntries.Entries = append(allEntries.Entries, bucketList.Entries...)
	}
	bucketList.Entries = nil
	bucketList = nil

	// For regular object list request:
	//   - return the list always sorted in alphabetical order
	//   - prioritize items with Status=OK
	// For fast listing:
	//   - return the entire list unsorted(sorting may take upto 10-15
	//     second for big data sets - millions of objects - eventually it may
	//     result in timeout between target and waiting for response proxy)
	//   - the only valid value for every object in resulting list is `Name`,
	//     all other fields(including `Status`) have zero value
	if !msg.Fast {
		entryLess := func(i, j int) bool {
			if allEntries.Entries[i].Name == allEntries.Entries[j].Name {
				return allEntries.Entries[i].Status < allEntries.Entries[j].Status
			}
			return allEntries.Entries[i].Name < allEntries.Entries[j].Name
		}
		sort.Slice(allEntries.Entries, entryLess)

		// shrink the result to `pageSize` entries. If the page is full than
		// mark the result incomplete by setting PageMarker
		if len(allEntries.Entries) >= pageSize {
			for i := pageSize; i < len(allEntries.Entries); i++ {
				allEntries.Entries[i] = nil
			}

			allEntries.Entries = allEntries.Entries[:pageSize]
			allEntries.PageMarker = allEntries.Entries[pageSize-1].Name
		}
	}

	return allEntries, nil
}

func (p *proxyrunner) getCloudBucketObjects(r *http.Request, bucket, bckProvider string, msg cmn.SelectMsg) (allentries *cmn.BucketList, err error) {
	const (
		cachedObjects = false
	)
	var resp *bucketResp
	allentries = &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, initialBucketListSize)}
	if msg.PageSize > maxPageSize {
		glog.Warningf("Page size(%d) for cloud bucket %s exceeds the limit(%d)", msg.PageSize, bucket, maxPageSize)
	}

	// first, get the cloud object list from a random target
	smap := p.smapowner.get()
	for _, si := range smap.Tmap {
		resp, err = p.targetListBucket(r, bucket, bckProvider, si, &msg, cachedObjects)
		if err != nil {
			return
		}
		break
	}

	if resp.outjson == nil || len(resp.outjson) == 0 {
		return
	}
	if err = jsoniter.Unmarshal(resp.outjson, &allentries); err != nil {
		return
	}
	if len(allentries.Entries) == 0 {
		return
	}
	if strings.Contains(msg.Props, cmn.GetTargetURL) {
		smap := p.smapowner.get()
		for _, e := range allentries.Entries {
			si, errStr := hrwTarget(bucket, e.Name, smap)
			if errStr != "" {
				err = errors.New(errStr)
				return
			}
			e.TargetURL = si.PublicNet.DirectURL
		}
	}
	if strings.Contains(msg.Props, cmn.GetPropsAtime) ||
		strings.Contains(msg.Props, cmn.GetPropsStatus) ||
		strings.Contains(msg.Props, cmn.GetPropsCopies) ||
		strings.Contains(msg.Props, cmn.GetPropsIsCached) {
		// Now add local properties to the cloud objects
		// The call replaces allentries.Entries with new values
		err = p.collectCachedFileList(bucket, bckProvider, allentries, msg)
	}
	return
}

// Local bucket:
//   - reads object list from all targets, combines, sorts and returns the list
// Cloud bucket:
//   - selects a random target to read the list of objects from cloud
//   - if iscached or atime property is requested it does extra steps:
//      * get list of cached files info from all targets
//      * updates the list of objects from the cloud with cached info
//   - returns the list
func (p *proxyrunner) listBucket(r *http.Request, bucket, bckProvider string, msg cmn.SelectMsg) (bckList *cmn.BucketList, err error) {
	if bckProvider == cmn.CloudBs || !p.bmdowner.get().IsLocal(bucket) {
		bckList, err = p.getCloudBucketObjects(r, bucket, bckProvider, msg)
	} else {
		bckList, err = p.getLocalBucketObjects(bucket, bckProvider, msg)
	}
	return
}

func (p *proxyrunner) savebmdconf(bucketmd *bucketMD, config *cmn.Config) (errstr string) {
	bucketmdfull := filepath.Join(config.Confdir, cmn.BucketmdBackupFile)
	if err := cmn.LocalSave(bucketmdfull, bucketmd); err != nil {
		errstr = fmt.Sprintf("Failed to store %s at %s, err: %v", bmdTermName, bucketmdfull, err)
	}
	return
}

func (p *proxyrunner) filrename(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg) {
	started := time.Now()
	apitems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	lbucket, objname := apitems[0], apitems[1]
	if !p.bmdowner.get().IsLocal(lbucket) {
		s := fmt.Sprintf("Rename/move is supported only for cache-only buckets (%s does not appear to be local)", lbucket)
		p.invalmsghdlr(w, r, s)
		return
	}

	smap := p.smapowner.get()
	si, errstr := hrwTarget(lbucket, objname, smap)
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}
	if glog.V(3) {
		glog.Infof("RENAME %s %s/%s => %s", r.Method, lbucket, objname, si)
	}

	// NOTE:
	//       code 307 is the only way to http-redirect with the
	//       original JSON payload (SelectMsg - see pkg/api/constant.go)
	redirecturl := p.redirectURL(r, si.PublicNet.DirectURL, started, lbucket)
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)

	p.statsif.Add(stats.RenameCount, 1)
}

func (p *proxyrunner) replicate(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg) {
	p.invalmsghdlr(w, r, cmn.NotSupported) // see also: daemon.go, config.sh, and tests/replication
}

func (p *proxyrunner) listRangeHandler(w http.ResponseWriter, r *http.Request, actionMsg *cmn.ActionMsg, method, bckProvider string) {
	var (
		err   error
		query = r.URL.Query()
	)

	apitems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	if _, ok := p.validateBucket(w, r, bucket, bckProvider); !ok {
		return
	}

	if err := p.listRange(method, bucket, actionMsg, query); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
}

func (p *proxyrunner) listRange(method, bucket string, actionMsg *cmn.ActionMsg, query url.Values) error {
	var (
		timeout time.Duration
		results chan callResult
	)

	wait := false
	if jsmap, ok := actionMsg.Value.(map[string]interface{}); !ok {
		return fmt.Errorf("failed to unmarshal JSMAP: Not a map[string]interface")
	} else if waitstr, ok := jsmap["wait"]; ok {
		if wait, ok = waitstr.(bool); !ok {
			return fmt.Errorf("failed to read ListRangeMsgBase Wait: Not a bool")
		}
	}
	// Send json message to all
	smap := p.smapowner.get()
	bmd := p.bmdowner.get()
	msgInt := p.newActionMsgInternal(actionMsg, smap, bmd)
	jsonbytes, err := jsoniter.Marshal(msgInt)
	cmn.AssertNoErr(err)

	if wait {
		timeout = cmn.GCO.Get().Timeout.List
	} else {
		timeout = defaultTimeout
	}

	results = p.broadcastTo(
		cmn.URLPath(cmn.Version, cmn.Buckets, bucket),
		query,
		method,
		jsonbytes,
		smap,
		timeout,
		cmn.NetworkIntraData,
		cluster.Targets,
	)

	for result := range results {
		if result.err != nil {
			return fmt.Errorf(
				"failed to execute List/Range request: %v (%d: %s)",
				result.err,
				result.status,
				result.errstr,
			)
		}
	}
	return nil
}

//============
//
// AuthN stuff
//
//============
func (p *proxyrunner) httpTokenDelete(w http.ResponseWriter, r *http.Request) {
	tokenList := &TokenList{}
	if _, err := p.checkRESTItems(w, r, 0, false, cmn.Version, cmn.Tokens); err != nil {
		return
	}

	msg := &cmn.ActionMsg{Action: cmn.ActRevokeToken}
	if p.forwardCP(w, r, msg, "revoke token", nil) {
		return
	}

	if err := cmn.ReadJSON(w, r, tokenList); err != nil {
		return
	}

	p.authn.updateRevokedList(tokenList)

	if p.smapowner.get().isPrimary(p.si) {
		msgInt := p.newActionMsgInternalStr(cmn.ActNewPrimary, nil, nil)
		p.metasyncer.sync(false, revspair{p.authn.revokedTokenList(), msgInt})
	}
}

// Read a token from request header and validates it
// Header format:
//		'Authorization: Bearer <token>'
func (p *proxyrunner) validateToken(r *http.Request) (*authRec, error) {
	s := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
	if len(s) != 2 || s[0] != tokenStart {
		return nil, fmt.Errorf("invalid request")
	}

	if p.authn == nil {
		return nil, fmt.Errorf("invalid credentials")
	}

	auth, err := p.authn.validateToken(s[1])
	if err != nil {
		glog.Errorf("Invalid token: %v", err)
		return nil, fmt.Errorf("invalid token")
	}

	return auth, nil
}

// A wrapper to check any request before delegating the request to real handler
// If authentication is disabled, it does nothing.
// If authentication is enabled, it looks for token in request header and
// makes sure that it is valid
func (p *proxyrunner) checkHTTPAuth(h http.HandlerFunc) http.HandlerFunc {
	wrappedFunc := func(w http.ResponseWriter, r *http.Request) {
		var (
			auth *authRec
			err  error
		)

		if cmn.GCO.Get().Auth.Enabled {
			if auth, err = p.validateToken(r); err != nil {
				glog.Error(err)
				p.invalmsghdlr(w, r, "Not authorized", http.StatusUnauthorized)
				return
			}
			if glog.V(3) {
				glog.Infof("Logged as %s", auth.userID)
			}
		}

		h.ServeHTTP(w, r)
	}
	return wrappedFunc
}

//======================
//
// http /daemon handlers
//
//======================

// [METHOD] /v1/daemon
func (p *proxyrunner) daemonHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.httpdaeget(w, r)
	case http.MethodPut:
		p.httpdaeput(w, r)
	default:
		p.invalmsghdlr(w, r, "invalid method for /daemon path", http.StatusBadRequest)
	}
}

func (p *proxyrunner) httpdaeget(w http.ResponseWriter, r *http.Request) {
	getWhat := r.URL.Query().Get(cmn.URLParamWhat)
	httpdaeWhat := "httpdaeget-" + getWhat
	switch getWhat {
	case cmn.GetWhatConfig, cmn.GetWhatBucketMeta, cmn.GetWhatSmapVote, cmn.GetWhatSnode:
		p.httprunner.httpdaeget(w, r)
	case cmn.GetWhatStats:
		rst := getproxystatsrunner()
		jsbytes, err := rst.GetWhatStats()
		cmn.AssertNoErr(err)
		p.writeJSON(w, r, jsbytes, httpdaeWhat)
	case cmn.GetWhatSysInfo:
		jsbytes, err := jsoniter.Marshal(gmem2.FetchSysInfo())
		cmn.AssertNoErr(err)
		p.writeJSON(w, r, jsbytes, httpdaeWhat)
	case cmn.GetWhatSmap:
		smap := p.smapowner.get()
		for smap == nil || !smap.isValid() {
			if p.startedup(0) != 0 { // must be starting up
				smap = p.smapowner.get()
				cmn.Assert(smap.isValid())
				break
			}
			glog.Errorf("%s is starting up: cannot execute GET %s yet...", p.si.Name(), cmn.GetWhatSmap)
			time.Sleep(time.Second)
			smap = p.smapowner.get()
		}
		jsbytes, err := jsoniter.Marshal(smap)
		cmn.AssertNoErr(err)
		p.writeJSON(w, r, jsbytes, httpdaeWhat)
	case cmn.GetWhatDaemonStatus:
		msg := &stats.DaemonStatus{
			Snode:       p.httprunner.si,
			SmapVersion: p.smapowner.get().Version,
			SysInfo:     gmem2.FetchSysInfo(),
			Stats:       getproxystatsrunner().Core,
		}
		jsbytes, err := jsoniter.Marshal(msg)
		cmn.AssertNoErr(err)
		p.writeJSON(w, r, jsbytes, httpdaeWhat)
	default:
		p.httprunner.httpdaeget(w, r)
	}
}

func (p *proxyrunner) httpdaeput(w http.ResponseWriter, r *http.Request) {
	apitems, err := p.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Daemon)
	if err != nil {
		return
	}
	if len(apitems) > 0 {
		switch apitems[0] {
		case cmn.Proxy:
			p.httpdaesetprimaryproxy(w, r)
			return
		case cmn.SyncSmap:
			var newsmap = &smapX{}
			if cmn.ReadJSON(w, r, newsmap) != nil {
				return
			}
			if !newsmap.isValid() {
				s := fmt.Sprintf("Received invalid Smap at startup/registration: %s", newsmap.pp())
				p.invalmsghdlr(w, r, s)
				return
			}
			if !newsmap.isPresent(p.si) {
				s := fmt.Sprintf("Not finding self %s in the %s", p.si, newsmap.pp())
				p.invalmsghdlr(w, r, s)
				return
			}
			if s := p.smapowner.synchronize(newsmap, true /*saveSmap*/, true /* lesserIsErr */); s != "" {
				p.invalmsghdlr(w, r, s)
				return
			}
			glog.Infof("%s: %s v%d done", p.si.Name(), cmn.SyncSmap, newsmap.version())
			return
		case cmn.ActSetConfig: // setconfig #1 - via query parameters and "?n1=v1&n2=v2..."
			kvs := cmn.NewSimpleKVsFromQuery(r.URL.Query())
			if err := cmn.SetConfigMany(kvs); err != nil {
				p.invalmsghdlr(w, r, err.Error())
				return
			}
			return
		}
	}

	//
	// other PUT /daemon actions
	//
	var msg cmn.ActionMsg
	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case cmn.ActSetConfig: // setconfig #2 - via action message
		var (
			value string
			ok    bool
		)
		if value, ok = msg.Value.(string); !ok {
			p.invalmsghdlr(w, r, fmt.Sprintf("Failed to parse ActionMsg value: not a string"))
			return
		}
		kvs := cmn.NewSimpleKVs(cmn.SimpleKVsEntry{Key: msg.Name, Value: value})
		if err := cmn.SetConfigMany(kvs); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
	case cmn.ActShutdown:
		q := r.URL.Query()
		force, _ := parsebool(q.Get(cmn.URLParamForce))
		if p.smapowner.get().isPrimary(p.si) && !force {
			s := fmt.Sprintf("Cannot shutdown primary proxy without %s=true query parameter", cmn.URLParamForce)
			p.invalmsghdlr(w, r, s)
			return
		}
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	default:
		s := fmt.Sprintf("Unexpected ActionMsg <- JSON [%v]", msg)
		p.invalmsghdlr(w, r, s)
	}
}

func (p *proxyrunner) smapFromURL(baseURL string) (smap *smapX, errstr string) {
	query := url.Values{}
	query.Add(cmn.URLParamWhat, cmn.GetWhatSmap)
	req := reqArgs{
		method: http.MethodGet,
		base:   baseURL,
		path:   cmn.URLPath(cmn.Version, cmn.Daemon),
		query:  query,
	}
	args := callArgs{req: req, timeout: defaultTimeout}
	res := p.call(args)
	if res.err != nil {
		return nil, fmt.Sprintf("Failed to get smap from %s: %v", baseURL, res.err)
	}
	smap = &smapX{}
	if err := jsoniter.Unmarshal(res.outjson, smap); err != nil {
		return nil, fmt.Sprintf("Failed to unmarshal smap: %v", err)
	}
	if !smap.isValid() {
		return nil, fmt.Sprintf("Invalid Smap from %s: %s", baseURL, smap.pp())
	}
	return
}

// forceful primary change - is used when the original primary network is down
// for a while and the remained nodes selected a new primary. After the
// original primary is back it does not attach automatically to the new primary
// and the cluster gets into split-brain mode. This request makes original
// primary connect to the new primary
func (p *proxyrunner) forcefulJoin(w http.ResponseWriter, r *http.Request, proxyID string) {
	newPrimaryURL := r.URL.Query().Get(cmn.URLParamPrimaryCandidate)
	glog.Infof("%s: force new primary %s (URL: %s)", p.si.Name(), proxyID, newPrimaryURL)

	if p.si.DaemonID == proxyID {
		glog.Warningf("%s is already the primary", p.si.Name())
		return
	}
	smap := p.smapowner.get()
	psi := smap.GetProxy(proxyID)
	if psi == nil && newPrimaryURL == "" {
		s := fmt.Sprintf("%s: failed to find new primary %s in local %s", p.si.Name(), proxyID, smap.pp())
		p.invalmsghdlr(w, r, s)
		return
	}
	if newPrimaryURL == "" {
		newPrimaryURL = psi.IntraControlNet.DirectURL
	}
	if newPrimaryURL == "" {
		s := fmt.Sprintf("%s: failed to get new primary %s direct URL", p.si.Name(), proxyID)
		p.invalmsghdlr(w, r, s)
		return
	}
	newSmap, errstr := p.smapFromURL(newPrimaryURL)
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}
	if proxyID != newSmap.ProxySI.DaemonID {
		s := fmt.Sprintf("%s: proxy %s is not the primary, current %s", p.si.Name(), proxyID, newSmap.pp())
		p.invalmsghdlr(w, r, s)
		return
	}

	// notify metasync to cancel all pending sync requests
	p.metasyncer.becomeNonPrimary()
	p.smapowner.put(newSmap)
	res := p.registerToURL(newSmap.ProxySI.IntraControlNet.DirectURL, newSmap.ProxySI, defaultTimeout, true, nil, false)
	if res.err != nil {
		p.invalmsghdlr(w, r, res.err.Error())
		return
	}
}

func (p *proxyrunner) httpdaesetprimaryproxy(w http.ResponseWriter, r *http.Request) {
	var (
		prepare bool
	)

	apitems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Daemon)
	if err != nil {
		return
	}

	proxyID := apitems[1]
	force, _ := parsebool(r.URL.Query().Get(cmn.URLParamForce))
	// forceful primary change
	if force && apitems[0] == cmn.Proxy {
		if !p.smapowner.get().isPrimary(p.si) {
			s := fmt.Sprintf("%s is not the primary", p.si.Name())
			p.invalmsghdlr(w, r, s)
		}
		p.forcefulJoin(w, r, proxyID)
		return
	}

	query := r.URL.Query()
	preparestr := query.Get(cmn.URLParamPrepare)
	if prepare, err = strconv.ParseBool(preparestr); err != nil {
		s := fmt.Sprintf("Failed to parse %s URL parameter: %v", cmn.URLParamPrepare, err)
		p.invalmsghdlr(w, r, s)
		return
	}

	if p.smapowner.get().isPrimary(p.si) {
		s := fmt.Sprint("Expecting 'cluster' (RESTful) resource when designating primary proxy via API")
		p.invalmsghdlr(w, r, s)
		return
	}

	if p.si.DaemonID == proxyID {
		if !prepare {
			if s := p.becomeNewPrimary(""); s != "" {
				p.invalmsghdlr(w, r, s)
			}
		}
		return
	}

	smap := p.smapowner.get()
	psi := smap.GetProxy(proxyID)

	if psi == nil {
		s := fmt.Sprintf("New primary proxy %s not present in the local %s", proxyID, smap.pp())
		p.invalmsghdlr(w, r, s)
		return
	}
	if prepare {
		if glog.V(4) {
			glog.Info("Preparation step: do nothing")
		}
		return
	}
	p.smapowner.Lock()
	clone := smap.clone()
	clone.ProxySI = psi
	if s := p.smapowner.persist(clone, true); s != "" {
		p.smapowner.Unlock()
		p.invalmsghdlr(w, r, s)
		return
	}
	p.smapowner.put(clone)
	p.smapowner.Unlock()
}

func (p *proxyrunner) becomeNewPrimary(proxyidToRemove string) (errstr string) {
	p.smapowner.Lock()
	smap := p.smapowner.get()
	if !smap.isPresent(p.si) {
		cmn.AssertMsg(false, "This proxy '"+p.si.DaemonID+"' must always be present in the local "+smap.pp())
	}
	clone := smap.clone()
	// FIXME: may be premature at this point
	if proxyidToRemove != "" {
		glog.Infof("Removing failed primary proxy %s", proxyidToRemove)
		clone.delProxy(proxyidToRemove)
	}

	clone.ProxySI = p.si
	clone.Version += 100
	if errstr = p.smapowner.persist(clone, true); errstr != "" {
		p.smapowner.Unlock()
		glog.Errorln(errstr)
		return
	}
	p.smapowner.put(clone)
	p.smapowner.Unlock()

	bucketmd := p.bmdowner.get()
	if glog.V(3) {
		glog.Infof("Distributing Smap v%d with the newly elected primary %s(self)", clone.version(), p.si.Name())
		glog.Infof("Distributing %s v%d as well", bmdTermName, bucketmd.version())
	}
	msgInt := p.newActionMsgInternalStr(cmn.ActNewPrimary, clone, nil)
	p.metasyncer.sync(true, revspair{clone, msgInt}, revspair{bucketmd, msgInt})
	return
}

func (p *proxyrunner) httpclusetprimaryproxy(w http.ResponseWriter, r *http.Request) {
	apitems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Cluster, cmn.Proxy)
	if err != nil {
		return
	}
	proxyid := apitems[0]
	s := "designate new primary proxy '" + proxyid + "'"
	if p.forwardCP(w, r, &cmn.ActionMsg{}, s, nil) {
		return
	}
	smap := p.smapowner.get()
	psi, ok := smap.Pmap[proxyid]
	if !ok {
		s := fmt.Sprintf("New primary proxy %s not present in the local %s", proxyid, smap.pp())
		p.invalmsghdlr(w, r, s)
		return
	}
	if proxyid == p.si.DaemonID {
		cmn.Assert(p.si.DaemonID == smap.ProxySI.DaemonID) // must be forwardCP-ed
		glog.Warningf("Request to set primary = %s = self: nothing to do", proxyid)
		return
	}

	// (I) prepare phase
	urlPath := cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Proxy, proxyid)
	q := url.Values{}
	q.Set(cmn.URLParamPrepare, "true")
	method := http.MethodPut
	results := p.broadcastTo(
		urlPath,
		q,
		method,
		nil, // body
		smap,
		cmn.GCO.Get().Timeout.CplaneOperation,
		cmn.NetworkIntraControl,
		cluster.AllNodes,
	)
	for result := range results {
		if result.err != nil {
			s := fmt.Sprintf("Failed to set primary %s: err %v from %s in the prepare phase", proxyid, result.err, result.si)
			p.invalmsghdlr(w, r, s)
			return
		}
	}

	// (I.5) commit locally
	p.smapowner.Lock()
	clone := p.smapowner.get().clone()
	clone.ProxySI = psi
	p.metasyncer.becomeNonPrimary()
	if s := p.smapowner.persist(clone, true); s != "" {
		glog.Errorf("Failed to save Smap locally after having transitioned to non-primary:\n%s", s)
	}
	p.smapowner.put(clone)
	p.smapowner.Unlock()

	// (II) commit phase
	q.Set(cmn.URLParamPrepare, "false")
	results = p.broadcastTo(
		urlPath,
		q,
		method,
		nil, // body
		p.smapowner.get(),
		cmn.GCO.Get().Timeout.CplaneOperation,
		cmn.NetworkIntraControl,
		cluster.AllNodes,
	)

	// FIXME: retry
	for result := range results {
		if result.err != nil {
			if result.si.DaemonID == proxyid {
				glog.Fatalf("Commit phase failure: new primary %s returned err %v", proxyid, result.err)
			} else {
				glog.Errorf("Commit phase failure: %s returned err %v when setting primary = %s",
					result.si.DaemonID, result.err, proxyid)
			}
		}
	}
}

//=======================
//
// http /cluster handlers
//
//=======================

// [METHOD] /v1/cluster
func (p *proxyrunner) clusterHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.httpcluget(w, r)
	case http.MethodPost:
		p.httpclupost(w, r)
	case http.MethodDelete:
		p.httpcludel(w, r)
	case http.MethodPut:
		p.httpcluput(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /cluster path")
	}
}

// [METHOD] /v1/tokens
func (p *proxyrunner) tokenHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodDelete:
		p.httpTokenDelete(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /token path")
	}
}

// http reverse-proxy handler, to handle unmodified requests
// (not to confuse with p.rproxy)
func (p *proxyrunner) reverseProxyHandler(w http.ResponseWriter, r *http.Request) {
	baseURL := r.URL.Scheme + "://" + r.URL.Host
	if glog.V(4) {
		glog.Infof("RevProxy handler for: %s -> %s", baseURL, r.URL.Path)
	}

	// Make it work as a transparent proxy if we are not able to cache the requested object.
	// Caching is available only for GET HTTP requests to GCS either via
	// http://www.googleapi.com/storage or http://storage.googleapis.com
	if r.Method != http.MethodGet || (baseURL != cmn.GcsURL && baseURL != cmn.GcsURLAlt) {
		p.rproxy.cloud.ServeHTTP(w, r)
		return
	}

	s := cmn.RESTItems(r.URL.Path)
	if baseURL == cmn.GcsURLAlt {
		// remove redundant items from URL path to make it AIS-compatible
		// original looks like:
		//    http://www.googleapis.com/storage/v1/b/<bucket>/o/<object>
		if glog.V(4) {
			glog.Infof("JSON GCP request for object: %v", s)
		}

		if len(s) < 4 {
			p.invalmsghdlr(w, r, "Invalid object path", http.StatusBadRequest)
			return
		}
		if s[0] != "storage" || s[2] != "b" {
			p.invalmsghdlr(w, r, "Invalid object path", http.StatusBadRequest)
			return
		}
		s = s[3:]
		// remove 'o' if it exists
		if len(s) > 1 && s[1] == "o" {
			s = append(s[:1], s[2:]...)
		}

		r.URL.Path = cmn.URLPath(s...)
		if glog.V(4) {
			glog.Infof("Updated JSON URL Path: %s", r.URL.Path)
		}
	} else {
		// original url looks like - no cleanup required:
		//    http://storage.googleapis.com/<bucket>/<object>
		if glog.V(4) {
			glog.Infof("XML GCP request for object: %v", s)
		}
	}

	if len(s) > 1 {
		r.URL.Path = cmn.URLPath(cmn.Version, cmn.Objects) + r.URL.Path
		p.objGetRProxy(w, r)
	} else if len(s) == 1 {
		r.URL.Path = cmn.URLPath(cmn.Version, cmn.Buckets) + r.URL.Path
		p.httpbckget(w, r)
	}
}

// gets target info
func (p *proxyrunner) httpcluget(w http.ResponseWriter, r *http.Request) {
	getWhat := r.URL.Query().Get(cmn.URLParamWhat)
	switch getWhat {
	case cmn.GetWhatStats:
		p.invokeHTTPGetClusterStats(w, r)
	case cmn.GetWhatSysInfo:
		p.invokeHTTPGetClusterSysinfo(w, r)
	case cmn.GetWhatXaction:
		p.invokeHTTPGetXaction(w, r)
	case cmn.GetWhatMountpaths:
		p.invokeHTTPGetClusterMountpaths(w, r)
	default:
		s := fmt.Sprintf("Unexpected GET request, invalid param 'what': [%s]", getWhat)
		cmn.InvalidHandlerWithMsg(w, r, s)
	}
}

func (p *proxyrunner) invokeHTTPGetXaction(w http.ResponseWriter, r *http.Request) bool {
	results, ok := p.invokeHTTPSelectMsgOnTargets(w, r)
	if !ok {
		return false
	}
	var (
		jsbytes []byte
		err     error
		kind    = r.URL.Query().Get(cmn.URLParamProps)
	)
	if kind == cmn.ActGlobalReb || kind == cmn.ActPrefetch {
		outputXactionStats := &stats.XactionStats{}
		outputXactionStats.Kind = kind
		outputXactionStats.TargetStats = results
		jsbytes, err = jsoniter.Marshal(outputXactionStats)
	} else {
		jsbytes, err = jsoniter.Marshal(results)
	}
	cmn.AssertNoErr(err)
	return p.writeJSON(w, r, jsbytes, "getXaction")
}

func (p *proxyrunner) invokeHTTPSelectMsgOnTargets(w http.ResponseWriter, r *http.Request) (map[string]jsoniter.RawMessage, bool) {
	smapX := p.smapowner.get()
	results := p.broadcastTo(
		cmn.URLPath(cmn.Version, cmn.Daemon),
		r.URL.Query(),
		r.Method,
		nil, // message
		smapX,
		cmn.GCO.Get().Timeout.Default,
		cmn.NetworkIntraControl,
		cluster.Targets,
	)
	targetResults := make(map[string]jsoniter.RawMessage, smapX.CountTargets())
	for result := range results {
		if result.err != nil {
			p.invalmsghdlr(w, r, result.errstr)
			return nil, false
		}
		targetResults[result.si.DaemonID] = jsoniter.RawMessage(result.outjson)
	}
	return targetResults, true
}

func (p *proxyrunner) invokeHTTPGetClusterSysinfo(w http.ResponseWriter, r *http.Request) bool {
	smapX := p.smapowner.get()
	fetchResults := func(broadcastType int, expectedNodes int) (map[string]jsoniter.RawMessage, string) {
		results := p.broadcastTo(
			cmn.URLPath(cmn.Version, cmn.Daemon),
			r.URL.Query(),
			r.Method,
			nil, // message
			smapX,
			cmn.GCO.Get().Timeout.Default,
			cmn.NetworkIntraControl,
			broadcastType,
		)
		sysInfoMap := make(map[string]jsoniter.RawMessage, expectedNodes)
		for result := range results {
			if result.err != nil {
				return nil, result.errstr
			}
			sysInfoMap[result.si.DaemonID] = jsoniter.RawMessage(result.outjson)
		}
		return sysInfoMap, ""
	}

	out := &cmn.ClusterSysInfoRaw{}

	proxyResults, err := fetchResults(cluster.Proxies, smapX.CountProxies())
	if err != "" {
		p.invalmsghdlr(w, r, err)
		return false
	}

	out.Proxy = proxyResults

	targetResults, err := fetchResults(cluster.Targets, smapX.CountTargets())
	if err != "" {
		p.invalmsghdlr(w, r, err)
		return false
	}

	out.Target = targetResults

	jsbytes, errObj := jsoniter.Marshal(out)
	cmn.AssertNoErr(errObj)
	ok := p.writeJSON(w, r, jsbytes, "HttpGetClusterSysInfo")
	return ok
}

// FIXME: read-lock
func (p *proxyrunner) invokeHTTPGetClusterStats(w http.ResponseWriter, r *http.Request) bool {
	targetStats, ok := p.invokeHTTPSelectMsgOnTargets(w, r)
	if !ok {
		errstr := fmt.Sprintf(
			"Unable to invoke cmn.SelectMsg on targets. Query: [%s]",
			r.URL.RawQuery)
		glog.Errorf(errstr)
		p.invalmsghdlr(w, r, errstr)
		return false
	}

	out := &stats.ClusterStatsRaw{}
	out.Target = targetStats
	rr := getproxystatsrunner()
	out.Proxy = rr.Core
	jsbytes, err := jsoniter.Marshal(out)
	cmn.AssertNoErr(err)
	ok = p.writeJSON(w, r, jsbytes, "HttpGetClusterStats")
	return ok
}

func (p *proxyrunner) invokeHTTPGetClusterMountpaths(w http.ResponseWriter, r *http.Request) bool {
	targetMountpaths, ok := p.invokeHTTPSelectMsgOnTargets(w, r)
	if !ok {
		errstr := fmt.Sprintf(
			"Unable to invoke cmn.SelectMsg on targets. Query: [%s]", r.URL.RawQuery)
		glog.Errorf(errstr)
		p.invalmsghdlr(w, r, errstr)
		return false
	}

	out := &ClusterMountpathsRaw{}
	out.Targets = targetMountpaths
	jsbytes, err := jsoniter.Marshal(out)
	cmn.AssertNoErr(err)
	ok = p.writeJSON(w, r, jsbytes, "HttpGetClusterMountpaths")
	return ok
}

// register|keepalive target|proxy
func (p *proxyrunner) httpclupost(w http.ResponseWriter, r *http.Request) {
	var (
		nsi                 cluster.Snode
		msg                 *cmn.ActionMsg
		s                   string
		keepalive, register bool
		nonElectable        bool
	)
	apitems, err := p.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Cluster)
	if err != nil {
		return
	}
	if cmn.ReadJSON(w, r, &nsi) != nil {
		return
	}
	if err := nsi.Validate(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	isProxy := nsi.DaemonType == cmn.Proxy
	if len(apitems) > 0 {
		keepalive = apitems[0] == cmn.Keepalive
		register = apitems[0] == cmn.Register // user via REST API
		if isProxy {
			if len(apitems) > 1 {
				keepalive = apitems[1] == cmn.Keepalive
			}
			s := r.URL.Query().Get(cmn.URLParamNonElectable)
			if nonElectable, err = parsebool(s); err != nil {
				glog.Errorf("Failed to parse %s for non-electability: %v", s, err)
			}
		}
	}
	s = fmt.Sprintf("register %s (keepalive=%t)", &nsi, keepalive)
	msg = &cmn.ActionMsg{Action: cmn.ActRegTarget}
	if isProxy {
		msg = &cmn.ActionMsg{Action: cmn.ActRegProxy}
	}
	body, err := jsoniter.Marshal(nsi)
	cmn.AssertNoErr(err)
	if p.forwardCP(w, r, msg, s, body) {
		return
	}
	if net.ParseIP(nsi.PublicNet.NodeIPAddr) == nil {
		s := fmt.Sprintf("register %s: invalid IP address %v", (&nsi).Name(), nsi.PublicNet.NodeIPAddr)
		p.invalmsghdlr(w, r, s)
		return
	}

	p.statsif.Add(stats.PostCount, 1)

	p.smapowner.Lock()
	smap := p.smapowner.get()
	if isProxy {
		osi := smap.GetProxy(nsi.DaemonID)
		if !p.addOrUpdateNode(&nsi, osi, keepalive) {
			p.smapowner.Unlock()
			return
		}
	} else {
		osi := smap.GetTarget(nsi.DaemonID)

		// FIXME: If not keepalive then update smap anyway - either: new target,
		// updated target or target has powercycled. Except 'updated target' case,
		// rebalance needs to be triggered and updating smap is the easiest way.
		if keepalive && !p.addOrUpdateNode(&nsi, osi, keepalive) {
			p.smapowner.Unlock()
			return
		}
		if register {
			if glog.V(3) {
				glog.Infof("register %s (num targets before %d)", (&nsi).Name(), smap.CountTargets())
			}
			args := callArgs{
				si: &nsi,
				req: reqArgs{
					method: http.MethodPost,
					path:   cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Register),
				},
				timeout: cmn.GCO.Get().Timeout.CplaneOperation,
			}
			res := p.call(args)
			if res.err != nil {
				p.smapowner.Unlock()
				errstr := fmt.Sprintf("Failed to register %s: %v, %s", (&nsi).Name(), res.err, res.errstr)
				p.invalmsghdlr(w, r, errstr, res.status)
				return
			}
		}
	}
	if p.startedup(0) == 0 {
		clone := p.smapowner.get().clone()
		clone.putNode(&nsi, nonElectable)
		p.smapowner.put(clone)
		p.smapowner.Unlock()
		return
	}
	p.smapowner.Unlock()
	if !isProxy && !register { // case: self-registering target
		bmd := p.bmdowner.get()
		meta := targetRegMeta{smap, bmd}
		jsbytes, err := jsoniter.Marshal(&meta)
		cmn.AssertNoErr(err)
		p.writeJSON(w, r, jsbytes, path.Join(cmn.ActRegTarget, nsi.DaemonID) /* tag */)
	}

	// update and distribute Smap
	go func(nsi *cluster.Snode) {
		p.smapowner.Lock()

		clone := p.smapowner.get().clone()
		clone.putNode(nsi, nonElectable)

		// Notify proxies and targets about new node. Targets probably need
		// to set up GFN.
		notifyMsgInt := p.newActionMsgInternal(&cmn.ActionMsg{Action: cmn.ActStartGFN}, clone, nil)
		notifyPairs := revspair{clone, notifyMsgInt}
		failCnt := p.metasyncer.notify(true, notifyPairs)
		if failCnt > 1 {
			p.smapowner.Unlock()
			glog.Errorf("FATAL: cannot join %s - unable to reach %d nodes", nsi, failCnt)
			return
		} else if failCnt > 0 {
			glog.Warning("unable to reach 1 node")
		}

		p.smapowner.put(clone)
		if errstr := p.smapowner.persist(clone, true); errstr != "" {
			glog.Errorln(errstr)
		}
		p.smapowner.Unlock()
		tokens := p.authn.revokedTokenList()
		// storage targets make use of msgInt.NewDaemonID,
		// to figure out whether to rebalance the cluster, and how to execute the rebalancing
		msgInt := p.newActionMsgInternal(msg, clone, nil)
		msgInt.NewDaemonID = nsi.DaemonID

		// metasync
		pairs := []revspair{{clone, msgInt}}
		if !isProxy {
			bmd := p.bmdowner.get()
			pairs = append(pairs, revspair{bmd, msgInt})
		}
		if len(tokens.Tokens) > 0 {
			pairs = append(pairs, revspair{tokens, msgInt})
		}
		p.metasyncer.sync(false, pairs...)
	}(&nsi)
}

func (p *proxyrunner) addOrUpdateNode(nsi *cluster.Snode, osi *cluster.Snode, keepalive bool) bool {
	if keepalive {
		if osi == nil {
			glog.Warningf("register/keepalive %s %s: adding back to the cluster map", nsi.DaemonType, nsi)
			return true
		}

		if !osi.Equals(nsi) {
			if p.detectDaemonDuplicate(osi, nsi) {
				glog.Errorf("%s tried to register/keepalive with a duplicate ID %s", nsi.PublicNet.DirectURL, nsi)
				return false
			}
			glog.Warningf("register/keepalive %s %s: info changed - renewing", nsi.DaemonType, nsi)
			return true
		}

		p.keepalive.heardFrom(nsi.DaemonID, !keepalive /* reset */)
		return false
	}
	if osi != nil {
		if p.startedup(0) == 0 {
			return true
		}
		if osi.Equals(nsi) {
			glog.Infof("register %s: already done", nsi)
			return false
		}
		glog.Warningf("register %s: renewing the registration %+v => %+v", nsi, osi, nsi)
	}
	return true
}

// unregisters a target/proxy
func (p *proxyrunner) httpcludel(w http.ResponseWriter, r *http.Request) {
	apitems, err := p.checkRESTItems(w, r, 1, true, cmn.Version, cmn.Cluster, cmn.Daemon)
	if err != nil {
		return
	}
	var (
		isproxy bool
		msg     *cmn.ActionMsg
		osi     *cluster.Snode
		psi     *cluster.Snode
		sid     = apitems[0]
	)
	msg = &cmn.ActionMsg{Action: cmn.ActUnregTarget}
	if sid == cmn.Proxy {
		msg = &cmn.ActionMsg{Action: cmn.ActUnregProxy}
		isproxy = true
		sid = apitems[1]
	}
	if p.forwardCP(w, r, msg, sid, nil) {
		return
	}

	p.smapowner.Lock()

	smap := p.smapowner.get()
	clone := smap.clone()

	if isproxy {
		psi = clone.GetProxy(sid)
		if psi == nil {
			p.smapowner.Unlock()
			errstr := fmt.Sprintf("Unknown proxy %s", sid)
			p.invalmsghdlr(w, r, errstr, http.StatusNotFound)
			return
		}
		clone.delProxy(sid)
		if glog.V(3) {
			glog.Infof("unregistered %s (num proxies %d)", psi.Name(), clone.CountProxies())
		}
	} else {
		osi = clone.GetTarget(sid)
		if osi == nil {
			p.smapowner.Unlock()
			errstr := fmt.Sprintf("Unknown target %s", sid)
			p.invalmsghdlr(w, r, errstr, http.StatusNotFound)
			return
		}
		clone.delTarget(sid)
		if glog.V(3) {
			glog.Infof("unregistered %s (num targets %d)", osi.Name(), clone.CountTargets())
		}
		args := callArgs{
			si: osi,
			req: reqArgs{
				method: http.MethodDelete,
				path:   cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Unregister),
			},
			timeout: cmn.GCO.Get().Timeout.CplaneOperation,
		}
		res := p.call(args)
		if res.err != nil {
			glog.Warningf("%s that is being unregistered failed to respond back: %v, %s", osi.Name(), res.err, res.errstr)
		}
	}
	if p.startedup(0) == 0 {
		p.smapowner.put(clone)
		p.smapowner.Unlock()
		return
	}
	if errstr := p.smapowner.persist(clone, true); errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		p.smapowner.Unlock()
		return
	}
	p.smapowner.put(clone)
	p.smapowner.Unlock()

	if isPrimary := p.smapowner.get().isPrimary(p.si); !isPrimary {
		return
	}

	msgInt := p.newActionMsgInternal(msg, smap, nil)
	p.metasyncer.sync(true, revspair{clone, msgInt})
}

// '{"action": "shutdown"}' /v1/cluster => (proxy) =>
// '{"action": "syncsmap"}' /v1/cluster => (proxy) => PUT '{Smap}' /v1/daemon/syncsmap => target(s)
// '{"action": "rebalance"}' /v1/cluster => (proxy) => PUT '{Smap}' /v1/daemon/rebalance => target(s)
// '{"action": "setconfig"}' /v1/cluster => (proxy) =>
func (p *proxyrunner) httpcluput(w http.ResponseWriter, r *http.Request) {
	var msg cmn.ActionMsg
	apitems, err := p.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Cluster)
	if err != nil {
		return
	}
	// cluster-wide: designate a new primary proxy administratively
	if len(apitems) > 0 {
		switch apitems[0] {
		case cmn.Proxy:
			p.httpclusetprimaryproxy(w, r)
			return
		case cmn.ActSetConfig: // setconfig #1 - via query parameters and "?n1=v1&n2=v2..."
			query := r.URL.Query()
			kvs := cmn.NewSimpleKVsFromQuery(query)
			if err := cmn.SetConfigMany(kvs); err != nil {
				p.invalmsghdlr(w, r, err.Error())
				return
			}
			results := p.broadcastTo(
				cmn.URLPath(cmn.Version, cmn.Daemon, cmn.ActSetConfig),
				query,
				http.MethodPut,
				nil,
				p.smapowner.get(),
				defaultTimeout,
				cmn.NetworkIntraControl,
				cluster.AllNodes,
			)
			for result := range results {
				if result.err != nil {
					p.invalmsghdlr(w, r, result.err.Error())
					p.keepalive.onerr(err, result.status)
					return
				}
			}
			return
		}
	}
	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}
	if p.forwardCP(w, r, &msg, "", nil) {
		return
	}

	switch msg.Action {
	case cmn.ActSetConfig:
		var (
			value string
			ok    bool
		)
		if value, ok = msg.Value.(string); !ok {
			p.invalmsghdlr(w, r, fmt.Sprintf("%s: invalid value format (%+v, %T)", cmn.ActSetConfig, msg.Value, msg.Value))
			return
		}
		kvs := cmn.NewSimpleKVs(cmn.SimpleKVsEntry{Key: msg.Name, Value: value})
		if err := cmn.SetConfigMany(kvs); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}

		msgbytes, err := jsoniter.Marshal(msg) // same message -> all targets and proxies
		cmn.AssertNoErr(err)

		results := p.broadcastTo(
			cmn.URLPath(cmn.Version, cmn.Daemon),
			nil, // query
			http.MethodPut,
			msgbytes,
			p.smapowner.get(),
			defaultTimeout,
			cmn.NetworkIntraControl,
			cluster.AllNodes,
		)
		for result := range results {
			if result.err != nil {
				p.invalmsghdlr(
					w,
					r,
					fmt.Sprintf("%s: (%s = %s) failed, err: %s", msg.Action, msg.Name, value, result.errstr),
				)
				p.keepalive.onerr(err, result.status)
				return
			}
		}

	case cmn.ActShutdown:
		glog.Infoln("Proxy-controlled cluster shutdown...")
		msgbytes, err := jsoniter.Marshal(msg) // same message -> all targets
		cmn.AssertNoErr(err)

		p.broadcastTo(
			cmn.URLPath(cmn.Version, cmn.Daemon),
			nil, // query
			http.MethodPut,
			msgbytes,
			p.smapowner.get(),
			defaultTimeout,
			cmn.NetworkIntraControl,
			cluster.AllNodes,
		)

		time.Sleep(time.Second)
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)

	case cmn.ActGlobalReb:
		smap := p.smapowner.get()
		msgInt := p.newActionMsgInternal(&msg, smap, nil)
		p.metasyncer.sync(false, revspair{smap, msgInt})

	default:
		s := fmt.Sprintf("Unexpected cmn.ActionMsg <- JSON [%v]", msg)
		p.invalmsghdlr(w, r, s)
	}
}

//========================
//
// broadcasts: Rx and Tx
//
//========================
func (p *proxyrunner) receiveBucketMD(newbucketmd *bucketMD, msgInt *actionMsgInternal) (errstr string) {
	if glog.V(3) {
		s := fmt.Sprintf("receive %s: v%d", bmdTermName, newbucketmd.version())
		if msgInt.Action == "" {
			glog.Infoln(s)
		} else {
			glog.Infof("%s, action %s", s, msgInt.Action)
		}
	}
	p.bmdowner.Lock()
	myver := p.bmdowner.get().version()
	if newbucketmd.version() <= myver {
		p.bmdowner.Unlock()
		if newbucketmd.version() < myver {
			errstr = fmt.Sprintf("Attempt to downgrade %s v%d to v%d", bmdTermName, myver, newbucketmd.version())
		}
		return
	}
	p.bmdowner.put(newbucketmd)
	if errstr := p.savebmdconf(newbucketmd, cmn.GCO.Get()); errstr != "" {
		glog.Errorln(errstr)
	}
	p.bmdowner.Unlock()
	return
}

func (p *proxyrunner) urlOutsideCluster(url string) bool {
	smap := p.smapowner.get()
	for _, proxyInfo := range smap.Pmap {
		if proxyInfo.PublicNet.DirectURL == url || proxyInfo.IntraControlNet.DirectURL == url ||
			proxyInfo.IntraDataNet.DirectURL == url {
			return false
		}
	}
	for _, targetInfo := range smap.Tmap {
		if targetInfo.PublicNet.DirectURL == url || targetInfo.IntraControlNet.DirectURL == url ||
			targetInfo.IntraDataNet.DirectURL == url {
			return false
		}
	}
	return true
}

func rechecksumRequired(globalChecksum string, bucketChecksumOld string, bucketChecksumNew string) bool {
	checksumOld := globalChecksum
	if bucketChecksumOld != cmn.PropInherit {
		checksumOld = bucketChecksumOld
	}
	checksumNew := globalChecksum
	if bucketChecksumNew != cmn.PropInherit {
		checksumNew = bucketChecksumNew
	}
	return checksumNew != cmn.ChecksumNone && checksumNew != checksumOld
}

func (p *proxyrunner) notifyTargetsRechecksum(bucket string) {
	smap := p.smapowner.get()
	jsbytes, err := jsoniter.Marshal(p.newActionMsgInternalStr(cmn.ActRechecksum, smap, nil))
	cmn.AssertNoErr(err)

	res := p.broadcastTo(
		cmn.URLPath(cmn.Version, cmn.Buckets, bucket),
		nil,
		http.MethodPost,
		jsbytes,
		smap,
		cmn.GCO.Get().Timeout.Default,
		cmn.NetworkIntraControl,
		cluster.Targets,
	)
	for r := range res {
		if r.err != nil {
			glog.Warningf("Target %s failed to re-checksum objects in bucket %s", r.si, bucket)
		}
	}
}

// detectDaemonDuplicate queries osi for its daemon info in order to determine if info has changed
// and is equal to nsi
func (p *proxyrunner) detectDaemonDuplicate(osi *cluster.Snode, nsi *cluster.Snode) bool {
	query := url.Values{}
	query.Add(cmn.URLParamWhat, cmn.GetWhatSnode)
	args := callArgs{
		si: osi,
		req: reqArgs{
			method: http.MethodGet,
			path:   cmn.URLPath(cmn.Version, cmn.Daemon),
			query:  query,
		},
		timeout: cmn.GCO.Get().Timeout.CplaneOperation,
	}
	res := p.call(args)
	if res.err != nil || res.outjson == nil || len(res.outjson) == 0 {
		// error getting response from osi
		return false
	}
	si := &cluster.Snode{}
	err := jsoniter.Unmarshal(res.outjson, si)
	cmn.AssertNoErr(err)
	return !nsi.Equals(si)
}
