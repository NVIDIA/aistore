/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

import (
	"bytes"
	"errors"
	"fmt"
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

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/api"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/common"
	"github.com/json-iterator/go"
)

const tokenStart = "Bearer"

type ClusterMountpathsRaw struct {
	Targets map[string]jsoniter.RawMessage `json:"targets"`
}

// Keeps a target response when doing parallel requests to all targets
type bucketResp struct {
	outjson []byte
	err     error
	id      string
}

// A list of target local files: cached or local bucket
// Maximum number of files in the response is `pageSize` entries
type localFilePage struct {
	entries []*api.BucketEntry
	err     error
	id      string
	marker  string
}

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
	p.httprunner.init(getproxystatsrunner(), true)
	p.httprunner.keepalive = getproxykeepalive()

	bucketmdfull := filepath.Join(ctx.config.Confdir, bucketmdbase)
	bucketmd := newBucketMD()
	if common.LocalLoad(bucketmdfull, bucketmd) != nil {
		// create empty
		bucketmd.Version = 1
		if err := common.LocalSave(bucketmdfull, bucketmd); err != nil {
			glog.Fatalf("FATAL: cannot store bucket-metadata, err: %v", err)
		}
	}
	p.bmdowner.put(bucketmd)

	p.metasyncer = getmetasyncer()

	// startup sequence - see earlystart.go for the steps and commentary
	common.Assert(p.smapowner.get() == nil)
	p.bootstrap()

	p.authn = &authManager{
		tokens:        make(map[string]*authRec),
		revokedTokens: make(map[string]bool),
		version:       1,
	}

	if ctx.config.Net.HTTP.RevProxy == RevProxyCloud {
		p.rproxy.cloud = &httputil.ReverseProxy{
			Director:  func(r *http.Request) {},
			Transport: p.createTransport(0, 0), // default idle connections per host, unlimited idle total
		}
	}

	//
	// REST API: register proxy handlers and start listening
	//

	// Public network
	if ctx.config.Auth.Enabled {
		p.registerPublicNetHandler(common.URLPath(api.Version, api.Buckets)+"/", wrapHandler(p.bucketHandler, p.checkHTTPAuth))
		p.registerPublicNetHandler(common.URLPath(api.Version, api.Objects)+"/", wrapHandler(p.objectHandler, p.checkHTTPAuth))
	} else {
		p.registerPublicNetHandler(common.URLPath(api.Version, api.Buckets)+"/", p.bucketHandler)
		p.registerPublicNetHandler(common.URLPath(api.Version, api.Objects)+"/", p.objectHandler)
	}

	p.registerPublicNetHandler(common.URLPath(api.Version, api.Daemon), p.daemonHandler)
	p.registerPublicNetHandler(common.URLPath(api.Version, api.Cluster), p.clusterHandler)
	p.registerPublicNetHandler(common.URLPath(api.Version, api.Tokens), p.tokenHandler)

	if ctx.config.Net.HTTP.RevProxy == RevProxyCloud {
		p.registerPublicNetHandler("/", p.reverseProxyHandler)
	} else {
		p.registerPublicNetHandler("/", common.InvalidHandler)
	}

	// Internal network
	p.registerInternalNetHandler(common.URLPath(api.Version, api.Metasync), p.metasyncHandler)
	p.registerInternalNetHandler(common.URLPath(api.Version, api.Health), p.healthHandler)
	p.registerInternalNetHandler(common.URLPath(api.Version, api.Vote), p.voteHandler)
	if ctx.config.Net.UseIntra {
		if ctx.config.Net.HTTP.RevProxy == RevProxyCloud {
			p.registerInternalNetHandler("/", p.reverseProxyHandler)
		} else {
			p.registerInternalNetHandler("/", common.InvalidHandler)
		}
	}

	// Replication network
	if ctx.config.Net.UseRepl {
		p.registerReplNetHandler(common.URLPath(api.Version, api.Objects)+"/", p.objectHandler)
		p.registerReplNetHandler("/", common.InvalidHandler)
	}

	glog.Infof("%s: [public net] listening on: %s", p.si.DaemonID, p.si.PublicNet.DirectURL)
	if p.si.PublicNet.DirectURL != p.si.InternalNet.DirectURL {
		glog.Infof("%s: [internal net] listening on: %s", p.si.DaemonID, p.si.InternalNet.DirectURL)
	}
	if ctx.config.Net.HTTP.RevProxy != "" {
		glog.Warningf("Warning: serving GET /object as a reverse-proxy ('%s')", ctx.config.Net.HTTP.RevProxy)
	}
	p.starttime = time.Now()

	_ = p.initStatsD("dfcproxy")
	sr := getproxystatsrunner()
	sr.Core.statsdC = &p.statsdC

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
		if ctx.config.Proxy.NonElectable {
			query := url.Values{}
			query.Add(api.URLParamNonElectable, "true")
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
			path:   common.URLPath(api.Version, api.Cluster, api.Daemon, api.Proxy, p.si.DaemonID),
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
	glog.Infof("Stopping %s (ID %s, primary=%t), err: %v", p.Getname(), p.si.DaemonID, isPrimary, err)
	p.xactinp.abortAll()

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
		common.InvalidHandlerWithMsg(w, r, "invalid method for /buckets path")
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
		common.InvalidHandlerWithMsg(w, r, "invalid method for /objects path")
	}
}

// GET /v1/buckets/bucket-name
func (p *proxyrunner) httpbckget(w http.ResponseWriter, r *http.Request) {
	if p.smapowner.get().CountTargets() < 1 {
		p.invalmsghdlr(w, r, "No registered targets yet")
		return
	}
	apitems, err := p.checkRESTItems(w, r, 1, false, api.Version, api.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	if !p.validatebckname(w, r, bucket) {
		return
	}
	// all bucket names
	if bucket == "*" {
		p.getbucketnames(w, r, bucket)
		return
	}
	s := fmt.Sprintf("Invalid route /buckets/%s", bucket)
	p.invalmsghdlr(w, r, s)
}

// GET /v1/objects/bucket-name/object-name
func (p *proxyrunner) httpobjget(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	apitems, err := p.checkRESTItems(w, r, 2, false, api.Version, api.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	if !p.validatebckname(w, r, bucket) {
		return
	}
	smap := p.smapowner.get()
	si, errstr := hrwTarget(bucket, objname, smap)
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}

	if ctx.config.Net.HTTP.RevProxy == RevProxyTarget {
		if glog.V(4) {
			glog.Infof("reverse-proxy: %s %s/%s <= %s", r.Method, bucket, objname, si.DaemonID)
		}
		p.reverseDP(w, r, si)
		delta := time.Since(started)
		p.statsif.add(statGetLatency, int64(delta))
	} else {
		if glog.V(4) {
			glog.Infof("%s %s/%s => %s", r.Method, bucket, objname, si.DaemonID)
		}
		redirecturl := p.redirectURL(r, si.PublicNet.DirectURL, started, bucket)
		if ctx.config.Readahead.Enabled && ctx.config.Readahead.ByProxy {
			go func(url string) {
				url += "&" + api.URLParamReadahead + "=true"
				args := callArgs{
					si: nil, // already inside url
					req: reqArgs{
						method: r.Method,
						header: r.Header,
						base:   url,
					},
					timeout: ctx.config.Timeout.ProxyPing,
				}
				res := p.call(args)
				if res.err != nil {
					glog.Errorf("Failed readahead %s/%s => %s: %v", bucket, objname, si.DaemonID, res.err)
				}
			}(redirecturl)
		}
		http.Redirect(w, r, redirecturl, http.StatusMovedPermanently)
	}
	p.statsif.add(statGetCount, 1)
}

// PUT /v1/objects
func (p *proxyrunner) httpobjput(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	apitems, err := p.checkRESTItems(w, r, 2, false, api.Version, api.Objects)
	if err != nil {
		return
	}
	//
	// FIXME: add protection against putting into non-existing local bucket
	//
	bucket, objname := apitems[0], apitems[1]
	smap := p.smapowner.get()
	si, errstr := hrwTarget(bucket, objname, smap)
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}
	if glog.V(4) {
		glog.Infof("%s %s/%s => %s", r.Method, bucket, objname, si.DaemonID)
	}
	var redirecturl string
	if replica, _ := isReplicationPUT(r); !replica {
		// regular PUT
		redirecturl = p.redirectURL(r, si.PublicNet.DirectURL, started, bucket)
	} else {
		// replication PUT
		redirecturl = p.redirectURL(r, si.ReplNet.DirectURL, started, bucket)
	}
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)

	p.statsif.add(statPutCount, 1)
}

// DELETE { action } /v1/buckets
func (p *proxyrunner) httpbckdelete(w http.ResponseWriter, r *http.Request) {
	var msg api.ActionMsg
	apitems, err := p.checkRESTItems(w, r, 1, false, api.Version, api.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	if err := p.readJSON(w, r, &msg); err != nil {
		return
	}
	switch msg.Action {
	case api.ActDestroyLB:
		if p.forwardCP(w, r, &msg, bucket, nil) {
			return
		}
		bucketmd := p.bmdowner.get()
		if !bucketmd.islocal(bucket) {
			p.invalmsghdlr(w, r, fmt.Sprintf("Bucket %s does not appear to be local", bucket))
			return
		}
		p.bmdowner.Lock()
		clone := bucketmd.clone()
		if !clone.del(bucket, true) {
			p.bmdowner.Unlock()
			s := fmt.Sprintf("Local bucket %s "+doesnotexist, bucket)
			p.invalmsghdlr(w, r, s)
			return
		}
		if errstr := p.savebmdconf(clone); errstr != "" {
			p.bmdowner.Unlock()
			p.invalmsghdlr(w, r, errstr)
			return
		}
		p.bmdowner.put(clone)
		p.bmdowner.Unlock()
		msg.Action = path.Join(msg.Action, bucket)
		p.metasyncer.sync(true, clone, &msg)
	case api.ActDelete, api.ActEvict:
		p.actionlistrange(w, r, &msg)
	default:
		p.invalmsghdlr(w, r, fmt.Sprintf("Unsupported Action: %s", msg.Action))
	}
}

// DELETE /v1/objects/object-name
func (p *proxyrunner) httpobjdelete(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	apitems, err := p.checkRESTItems(w, r, 2, false, api.Version, api.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	smap := p.smapowner.get()
	si, errstr := hrwTarget(bucket, objname, smap)
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}
	if glog.V(4) {
		glog.Infof("%s %s/%s => %s", r.Method, bucket, objname, si.DaemonID)
	}
	redirecturl := p.redirectURL(r, si.PublicNet.DirectURL, started, bucket)
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)

	p.statsif.add(statDeleteCount, 1)
}

// [METHOD] /v1/metasync
func (p *proxyrunner) metasyncHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		p.metasyncHandlerPut(w, r)
	default:
		p.invalmsghdlr(w, r, "invalid method for metasync", http.StatusBadRequest)
	}
}

// PUT /v1/metasync
func (p *proxyrunner) metasyncHandlerPut(w http.ResponseWriter, r *http.Request) {
	// FIXME: may not work if got disconnected for a while and have missed elections (#109)
	if p.smapowner.get().isPrimary(p.si) {
		_, xx := p.xactinp.findL(api.ActElection)
		vote := xx != nil
		s := fmt.Sprintf("Primary %s cannot receive cluster meta (election=%t)", p.si.DaemonID, vote)
		p.invalmsghdlr(w, r, s)
		return
	}
	var payload = make(common.SimpleKVs)
	if err := p.readJSON(w, r, &payload); err != nil {
		p.invalmsghdlr(w, r, err.Error())
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
		if !newsmap.isPresent(p.si, true) {
			s := fmt.Sprintf("Warning: not finding self '%s' in the received %s", p.si.DaemonID, newsmap.pp())
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

// GET /v1/health
func (p *proxyrunner) healthHandler(w http.ResponseWriter, r *http.Request) {
	rr := getproxystatsrunner()
	rr.Lock()
	rr.Core.Tracker[statUptimeLatency].Value = int64(time.Since(p.starttime) / time.Microsecond)
	if p.startedup(0) == 0 {
		rr.Core.Tracker[statUptimeLatency].Value = 0
	}
	jsbytes, err := jsoniter.Marshal(rr.Core)
	rr.Unlock()

	common.Assert(err == nil, err)
	p.writeJSON(w, r, jsbytes, "proxycorestats")
}

// POST { action } /v1/buckets/bucket-name
func (p *proxyrunner) httpbckpost(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	var msg api.ActionMsg
	apitems, err := p.checkRESTItems(w, r, 1, false, api.Version, api.Buckets)
	if err != nil {
		return
	}
	lbucket := apitems[0]
	if !p.validatebckname(w, r, lbucket) {
		return
	}
	if p.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case api.ActCreateLB:
		if p.forwardCP(w, r, &msg, lbucket, nil) {
			return
		}
		p.bmdowner.Lock()
		clone := p.bmdowner.get().clone()
		if !clone.add(lbucket, true, *NewBucketProps()) {
			p.bmdowner.Unlock()
			p.invalmsghdlr(w, r, fmt.Sprintf("Local bucket %s already exists", lbucket))
			return
		}
		if errstr := p.savebmdconf(clone); errstr != "" {
			p.bmdowner.Unlock()
			p.invalmsghdlr(w, r, errstr)
			return
		}
		p.bmdowner.put(clone)
		p.bmdowner.Unlock()
		msg.Action = path.Join(msg.Action, lbucket)
		p.metasyncer.sync(true, clone, &msg)
	case api.ActRenameLB:
		if p.forwardCP(w, r, &msg, "", nil) {
			return
		}
		bucketFrom, bucketTo := lbucket, msg.Name
		if bucketFrom == "" || bucketTo == "" {
			errstr := fmt.Sprintf("Invalid rename local bucket request: empty name %s => %s",
				bucketFrom, bucketTo)
			p.invalmsghdlr(w, r, errstr)
			return
		}
		clone := p.bmdowner.get().clone()
		ok, props := clone.get(bucketFrom, true)
		if !ok {
			s := fmt.Sprintf("Local bucket %s "+doesnotexist, bucketFrom)
			p.invalmsghdlr(w, r, s)
			return
		}
		ok, _ = clone.get(bucketTo, true)
		if ok {
			s := fmt.Sprintf("Local bucket %s already exists", bucketTo)
			p.invalmsghdlr(w, r, s)
			return
		}
		if !p.renamelocalbucket(bucketFrom, bucketTo, clone, props, &msg, r.Method) {
			errstr := fmt.Sprintf("Failed to rename local bucket %s => %s", bucketFrom, bucketTo)
			p.invalmsghdlr(w, r, errstr)
		}
		glog.Infof("renamed local bucket %s => %s, bucket-metadata version %d", bucketFrom, bucketTo, clone.version())
	case api.ActSyncLB:
		if p.forwardCP(w, r, &msg, "", nil) {
			return
		}
		p.metasyncer.sync(false, p.bmdowner.get(), &msg)
	case api.ActPrefetch:
		p.actionlistrange(w, r, &msg)
	case api.ActListObjects:
		p.listBucketAndCollectStats(w, r, lbucket, msg, started)
	default:
		s := fmt.Sprintf("Unexpected api.ActionMsg <- JSON [%v]", msg)
		p.invalmsghdlr(w, r, s)
	}
}

func (p *proxyrunner) listBucketAndCollectStats(w http.ResponseWriter,
	r *http.Request, lbucket string, msg api.ActionMsg, started time.Time) {
	pagemarker, ok := p.listbucket(w, r, lbucket, &msg)
	if ok {
		delta := time.Since(started)
		p.statsif.addMany(namedVal64{statListCount, 1}, namedVal64{statListLatency, int64(delta)})
		if glog.V(3) {
			lat := int64(delta / time.Microsecond)
			if pagemarker != "" {
				glog.Infof("LIST: %s, page %s, %d µs", lbucket, pagemarker, lat)
			} else {
				glog.Infof("LIST: %s, %d µs", lbucket, lat)
			}
		}
	}
}

// POST { action } /v1/objects/bucket-name
func (p *proxyrunner) httpobjpost(w http.ResponseWriter, r *http.Request) {
	var msg api.ActionMsg
	apitems, err := p.checkRESTItems(w, r, 1, true, api.Version, api.Objects)
	if err != nil {
		return
	}
	lbucket := apitems[0]
	if !p.validatebckname(w, r, lbucket) {
		return
	}
	if p.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case api.ActRename:
		p.filrename(w, r, &msg)
		return
	case api.ActReplicate:
		p.replicate(w, r, &msg)
		return
	default:
		s := fmt.Sprintf("Unexpected api.ActionMsg <- JSON [%v]", msg)
		p.invalmsghdlr(w, r, s)
		return
	}
}

// HEAD /v1/buckets/bucket-name
func (p *proxyrunner) httpbckhead(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	apitems, err := p.checkRESTItems(w, r, 1, false, api.Version, api.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	if !p.validatebckname(w, r, bucket) {
		return
	}
	var si *cluster.Snode
	// Use random map iteration order to choose a random target to redirect to
	smap := p.smapowner.get()
	for _, si = range smap.Tmap {
		break
	}
	if glog.V(3) {
		glog.Infof("%s %s => %s", r.Method, bucket, si.DaemonID)
	}
	redirecturl := p.redirectURL(r, si.PublicNet.DirectURL, started, bucket)
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)
}

// PUT /v1/buckets/bucket-name
func (p *proxyrunner) httpbckput(w http.ResponseWriter, r *http.Request) {
	apitems, err := p.checkRESTItems(w, r, 1, false, api.Version, api.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	if !p.validatebckname(w, r, bucket) {
		return
	}
	props := &BucketProps{} // every field has to have zero value
	msg := api.ActionMsg{Value: props}
	if p.readJSON(w, r, &msg) != nil {
		return
	}
	if msg.Action != api.ActSetProps && msg.Action != api.ActResetProps {
		s := fmt.Sprintf("Invalid api.ActionMsg [%v] - expecting '%s' or '%s' action", msg, api.ActSetProps, api.ActResetProps)
		p.invalmsghdlr(w, r, s)
		return
	}

	p.bmdowner.Lock()
	clone := p.bmdowner.get().clone()
	isLocal := clone.islocal(bucket)

	exists, oldProps := clone.get(bucket, isLocal)
	if !exists {
		common.Assert(!isLocal)
		oldProps = *NewBucketProps()
		clone.add(bucket, false, oldProps)
	}

	switch msg.Action {
	case api.ActSetProps:
		if err := p.validateBucketProps(props, isLocal); err != nil {
			p.bmdowner.Unlock()
			p.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
			return
		}
		p.copyBucketProps(&oldProps, props, bucket)
	case api.ActResetProps:
		oldProps = *NewBucketProps()
	}

	clone.set(bucket, isLocal, oldProps)
	if e := p.savebmdconf(clone); e != "" {
		glog.Errorln(e)
	}
	p.bmdowner.put(clone)
	p.bmdowner.Unlock()
	p.metasyncer.sync(true, clone, &msg)
}

// HEAD /v1/objects/bucket-name/object-name
func (p *proxyrunner) httpobjhead(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	checkCached, _ := parsebool(r.URL.Query().Get(api.URLParamCheckCached))
	apitems, err := p.checkRESTItems(w, r, 2, false, api.Version, api.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	if !p.validatebckname(w, r, bucket) {
		return
	}
	smap := p.smapowner.get()
	si, errstr := hrwTarget(bucket, objname, smap)
	if errstr != "" {
		return
	}
	if glog.V(4) {
		glog.Infof("%s %s/%s => %s", r.Method, bucket, objname, si.DaemonID)
	}
	redirecturl := p.redirectURL(r, si.PublicNet.DirectURL, started, bucket)
	if checkCached {
		redirecturl += fmt.Sprintf("&%s=true", api.URLParamCheckCached)
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
func (p *proxyrunner) forwardCP(w http.ResponseWriter, r *http.Request, msg *api.ActionMsg, s string, body []byte) (forf bool) {
	smap := p.smapowner.get()
	if smap == nil || !smap.isValid() {
		s := fmt.Sprintf("%s must be starting up: cannot execute %s:%s", p.si.DaemonID, msg.Action, s)
		p.invalmsghdlr(w, r, s)
		return true
	}
	if smap.isPrimary(p.si) {
		return
	}
	if body == nil {
		var err error
		body, err = jsoniter.Marshal(msg)
		common.Assert(err == nil, err)
	}
	p.rproxy.Lock()
	if p.rproxy.u != smap.ProxySI.PublicNet.DirectURL {
		p.rproxy.u = smap.ProxySI.PublicNet.DirectURL
		uparsed, err := url.Parse(smap.ProxySI.PublicNet.DirectURL)
		common.Assert(err == nil, err)
		p.rproxy.p = httputil.NewSingleHostReverseProxy(uparsed)
		p.rproxy.p.Transport = p.createTransport(proxyMaxIdleConnsPer, 0)
	}
	p.rproxy.Unlock()
	if len(body) > 0 {
		r.Body = ioutil.NopCloser(bytes.NewBuffer(body))
		r.ContentLength = int64(len(body)) // directly setting content-length
	}
	glog.Infof("%s: forwarding '%s:%s' to the primary %s", p.si.DaemonID, msg.Action, s, smap.ProxySI.DaemonID)
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
		common.Assert(err == nil, err)
		rproxy = httputil.NewSingleHostReverseProxy(uparsed)
		rproxy.Transport = p.createTransport(targetMaxIdleConnsPer, 0)
		p.rproxy.tmap[tsi.DaemonID] = rproxy
	}
	p.rproxy.Unlock()
	rproxy.ServeHTTP(w, r)
}

func (p *proxyrunner) renamelocalbucket(bucketFrom, bucketTo string, clone *bucketMD, props BucketProps,
	msg *api.ActionMsg, method string) bool {
	smap4bcast := p.smapowner.get()

	msg.Value = clone
	jsbytes, err := jsoniter.Marshal(msg)
	common.Assert(err == nil, err)

	res := p.broadcastTargets(
		common.URLPath(api.Version, api.Buckets, bucketFrom),
		nil, // query
		method,
		jsbytes,
		smap4bcast,
		ctx.config.Timeout.Default,
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
	if errstr := p.savebmdconf(clone); errstr != "" {
		glog.Errorln(errstr)
	}
	p.bmdowner.put(clone)
	p.bmdowner.Unlock()
	p.metasyncer.sync(true, clone, msg)
	return true
}

func (p *proxyrunner) getbucketnames(w http.ResponseWriter, r *http.Request, bucketspec string) {
	q := r.URL.Query()
	localonly, _ := parsebool(q.Get(api.URLParamLocal))
	bucketmd := p.bmdowner.get()
	if localonly {
		bucketnames := &api.BucketNames{Cloud: []string{}, Local: make([]string, 0, 64)}
		for bucket := range bucketmd.LBmap {
			bucketnames.Local = append(bucketnames.Local, bucket)
		}
		jsbytes, err := jsoniter.Marshal(bucketnames)
		common.Assert(err == nil, err)
		p.writeJSON(w, r, jsbytes, "getbucketnames?local=true")
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
			path:   common.URLPath(api.Version, api.Buckets, bucketspec),
		},
		timeout: defaultTimeout,
	}
	res := p.call(args)
	if res.err != nil {
		p.invalmsghdlr(w, r, res.errstr)
		p.keepalive.onerr(res.err, res.status)
	} else {
		p.writeJSON(w, r, res.outjson, "getbucketnames")
	}
}

func (p *proxyrunner) redirectURL(r *http.Request, to string, ts time.Time, bucket string) (redirect string) {
	var (
		query    = url.Values{}
		bucketmd = p.bmdowner.get()
		islocal  = bucketmd.islocal(bucket)
	)
	redirect = to + r.URL.Path + "?"
	if r.URL.RawQuery != "" {
		redirect += r.URL.RawQuery + "&"
	}

	query.Add(api.URLParamLocal, strconv.FormatBool(islocal))
	query.Add(api.URLParamProxyID, p.si.DaemonID)
	query.Add(api.URLParamBMDVersion, bucketmd.vstr)
	query.Add(api.URLParamUnixTime, strconv.FormatInt(int64(ts.UnixNano()), 10))
	redirect += query.Encode()
	return
}

// For cached = false goes to the Cloud, otherwise returns locally cached files
func (p *proxyrunner) targetListBucket(r *http.Request, bucket string, dinfo *cluster.Snode,
	getMsg *api.GetMsg, islocal bool, cached bool) (*bucketResp, error) {
	actionMsgBytes, err := jsoniter.Marshal(api.ActionMsg{Action: api.ActListObjects, Value: getMsg})
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
	query.Add(api.URLParamLocal, strconv.FormatBool(islocal))
	query.Add(api.URLParamCached, strconv.FormatBool(cached))
	query.Add(api.URLParamBMDVersion, p.bmdowner.get().vstr)

	args := callArgs{
		si: dinfo,
		req: reqArgs{
			method: http.MethodPost,
			header: header,
			path:   common.URLPath(api.Version, api.Buckets, bucket),
			query:  query,
			body:   actionMsgBytes,
		},
		timeout: defaultTimeout,
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

// Receives info about locally cached files from targets in batches
// and merges with existing list of cloud files
func (p *proxyrunner) consumeCachedList(bmap map[string]*api.BucketEntry, dataCh chan *localFilePage, errCh chan error) {
	for rb := range dataCh {
		if rb.err != nil {
			if errCh != nil {
				errCh <- rb.err
			}
			glog.Errorf("Failed to get information about file in DFC cache: %v", rb.err)
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
				// Such objects will retreive data from Cloud on GET request
				entry.IsCached = newEntry.Status == api.ObjStatusOK
			}
		}
	}
}

// Request list of all cached files from a target.
// The target returns its list in batches `pageSize` length
func (p *proxyrunner) generateCachedList(bucket string, daemon *cluster.Snode, dataCh chan *localFilePage, origmsg *api.GetMsg) {
	var msg api.GetMsg
	common.CopyStruct(&msg, origmsg)
	msg.GetPageSize = internalPageSize
	for {
		resp, err := p.targetListBucket(nil, bucket, daemon, &msg, false /* islocal */, true /* cachedObjects */)
		if err != nil {
			if dataCh != nil {
				dataCh <- &localFilePage{
					id:  daemon.DaemonID,
					err: err,
				}
			}
			glog.Errorf("Failed to get information about cached objects on target %v: %v", daemon.DaemonID, err)
			return
		}

		if resp.outjson == nil || len(resp.outjson) == 0 {
			return
		}

		entries := api.BucketList{Entries: make([]*api.BucketEntry, 0, 128)}
		if err := jsoniter.Unmarshal(resp.outjson, &entries); err != nil {
			if dataCh != nil {
				dataCh <- &localFilePage{
					id:  daemon.DaemonID,
					err: err,
				}
			}
			glog.Errorf("Failed to unmarshall cached objects list from target %v: %v", daemon.DaemonID, err)
			return
		}

		msg.GetPageMarker = entries.PageMarker
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
func (p *proxyrunner) collectCachedFileList(bucket string, fileList *api.BucketList, getmsgjson []byte) (err error) {
	reqParams := &api.GetMsg{}
	err = jsoniter.Unmarshal(getmsgjson, reqParams)
	if err != nil {
		return
	}

	bucketMap := make(map[string]*api.BucketEntry, initialBucketListSize)
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
	reqParams.GetPageMarker = ""

	wg := &sync.WaitGroup{}
	for _, daemon := range smap.Tmap {
		wg.Add(1)
		// without this copy all goroutines get the same pointer
		go func(d *cluster.Snode) {
			p.generateCachedList(bucket, d, dataCh, reqParams)
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

	fileList.Entries = make([]*api.BucketEntry, 0, len(bucketMap))
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

func (p *proxyrunner) getLocalBucketObjects(bucket string, listmsgjson []byte) (allentries *api.BucketList, err error) {
	type targetReply struct {
		resp *bucketResp
		err  error
	}
	const (
		islocal    = true
		cachedObjs = false
	)
	msg := &api.GetMsg{}
	if err = jsoniter.Unmarshal(listmsgjson, msg); err != nil {
		return
	}
	pageSize := api.DefaultPageSize
	if msg.GetPageSize != 0 {
		pageSize = msg.GetPageSize
	}

	if pageSize > MaxPageSize {
		glog.Warningf("Page size(%d) for local bucket %s exceeds the limit(%d)", msg.GetPageSize, bucket, MaxPageSize)
	}

	smap := p.smapowner.get()
	chresult := make(chan *targetReply, len(smap.Tmap))
	wg := &sync.WaitGroup{}

	targetCallFn := func(si *cluster.Snode) {
		resp, err := p.targetListBucket(nil, bucket, si, msg, islocal, cachedObjs)
		chresult <- &targetReply{resp, err}
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
	close(chresult)

	// combine results
	allentries = &api.BucketList{Entries: make([]*api.BucketEntry, 0, pageSize)}
	for r := range chresult {
		if r.err != nil {
			err = r.err
			return
		}

		if r.resp.outjson == nil || len(r.resp.outjson) == 0 {
			continue
		}

		bucketList := &api.BucketList{Entries: make([]*api.BucketEntry, 0, pageSize)}
		if err = jsoniter.Unmarshal(r.resp.outjson, &bucketList); err != nil {
			return
		}

		if len(bucketList.Entries) == 0 {
			continue
		}

		allentries.Entries = append(allentries.Entries, bucketList.Entries...)
	}

	// return the list always sorted in alphabetical order
	entryLess := func(i, j int) bool {
		return allentries.Entries[i].Name < allentries.Entries[j].Name
	}
	sort.Slice(allentries.Entries, entryLess)

	// shrink the result to `pageSize` entries. If the page is full than
	// mark the result incomplete by setting PageMarker
	if len(allentries.Entries) >= pageSize {
		for i := pageSize; i < len(allentries.Entries); i++ {
			allentries.Entries[i] = nil
		}

		allentries.Entries = allentries.Entries[:pageSize]
		allentries.PageMarker = allentries.Entries[pageSize-1].Name
	}

	return allentries, nil
}

func (p *proxyrunner) getCloudBucketObjects(r *http.Request, bucket string, listmsgjson []byte) (allentries *api.BucketList, err error) {
	const (
		islocal       = false
		cachedObjects = false
	)
	var resp *bucketResp
	allentries = &api.BucketList{Entries: make([]*api.BucketEntry, 0, initialBucketListSize)}
	msg := api.GetMsg{}
	err = jsoniter.Unmarshal(listmsgjson, &msg)
	if err != nil {
		return
	}
	if msg.GetPageSize > MaxPageSize {
		glog.Warningf("Page size(%d) for cloud bucket %s exceeds the limit(%d)", msg.GetPageSize, bucket, MaxPageSize)
	}

	// first, get the cloud object list from a random target
	smap := p.smapowner.get()
	for _, si := range smap.Tmap {
		resp, err = p.targetListBucket(r, bucket, si, &msg, islocal, cachedObjects)
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
	if strings.Contains(msg.GetProps, api.GetTargetURL) {
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
	if strings.Contains(msg.GetProps, api.GetPropsAtime) ||
		strings.Contains(msg.GetProps, api.GetPropsStatus) ||
		strings.Contains(msg.GetProps, api.GetPropsIsCached) {
		// Now add local properties to the cloud objects
		// The call replaces allentries.Entries with new values
		err = p.collectCachedFileList(bucket, allentries, listmsgjson)
	}
	return
}

// Local bucket:
//   - reads object list from all targets, combines, sorts and returns the
//     first pageSize objects
// Cloud bucket:
//   - selects a random target to read the list of objects from cloud
//   - if iscached or atime property is requested it does extra steps:
//      * get list of cached files info from all targets
//      * updates the list of objects from the cloud with cached info
//   - returns the list
func (p *proxyrunner) listbucket(w http.ResponseWriter, r *http.Request, bucket string, actionMsg *api.ActionMsg) (pagemarker string, ok bool) {
	var allentries *api.BucketList
	listmsgjson, err := jsoniter.Marshal(actionMsg.Value)
	if err != nil {
		s := fmt.Sprintf("Unable to marshal action message: %v. Error: %v", actionMsg, err)
		p.invalmsghdlr(w, r, s)
		return
	}

	if p.bmdowner.get().islocal(bucket) {
		allentries, err = p.getLocalBucketObjects(bucket, listmsgjson)
	} else {
		allentries, err = p.getCloudBucketObjects(r, bucket, listmsgjson)
	}
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	jsbytes, err := jsoniter.Marshal(allentries)
	common.Assert(err == nil, err)
	ok = p.writeJSON(w, r, jsbytes, "listbucket")
	pagemarker = allentries.PageMarker
	return
}

func (p *proxyrunner) savebmdconf(bucketmd *bucketMD) (errstr string) {
	bucketmdfull := filepath.Join(ctx.config.Confdir, bucketmdbase)
	if err := common.LocalSave(bucketmdfull, bucketmd); err != nil {
		errstr = fmt.Sprintf("Failed to store bucket-metadata at %s, err: %v", bucketmdfull, err)
	}
	return
}

func (p *proxyrunner) filrename(w http.ResponseWriter, r *http.Request, msg *api.ActionMsg) {
	started := time.Now()
	apitems, err := p.checkRESTItems(w, r, 2, false, api.Version, api.Objects)
	if err != nil {
		return
	}
	lbucket, objname := apitems[0], apitems[1]
	if !p.bmdowner.get().islocal(lbucket) {
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
		glog.Infof("RENAME %s %s/%s => %s", r.Method, lbucket, objname, si.DaemonID)
	}

	// NOTE:
	//       code 307 is the only way to http-redirect with the
	//       original JSON payload (GetMsg - see pkg/api/constant.go)
	redirecturl := p.redirectURL(r, si.PublicNet.DirectURL, started, lbucket)
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)

	p.statsif.add(statRenameCount, 1)
}

func (p *proxyrunner) replicate(w http.ResponseWriter, r *http.Request, msg *api.ActionMsg) {
	started := time.Now()
	apitems, err := p.checkRESTItems(w, r, 2, false, api.Version, api.Objects)
	if err != nil {
		return
	}
	bucket, object := apitems[0], apitems[1]
	smap := p.smapowner.get()
	si, errstr := hrwTarget(bucket, object, smap)
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}
	if glog.V(3) {
		glog.Infof("REPLICATE %s %s/%s", r.Method, bucket, object)
	}

	// NOTE:
	//       code 307 is the only way to http-redirect with the
	//       original JSON payload (GetMsg - see pkg/api/constant.go)
	redirectURL := p.redirectURL(r, si.PublicNet.DirectURL, started, bucket)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

func (p *proxyrunner) actionlistrange(w http.ResponseWriter, r *http.Request, actionMsg *api.ActionMsg) {
	var (
		err    error
		method string
	)

	apitems, err := p.checkRESTItems(w, r, 1, false, api.Version, api.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	islocal := p.bmdowner.get().islocal(bucket)
	wait := false
	if jsmap, ok := actionMsg.Value.(map[string]interface{}); !ok {
		s := fmt.Sprintf("Failed to unmarshal JSMAP: Not a map[string]interface")
		p.invalmsghdlr(w, r, s)
		return
	} else if waitstr, ok := jsmap["wait"]; ok {
		if wait, ok = waitstr.(bool); !ok {
			s := fmt.Sprintf("Failed to read ListRangeMsgBase Wait: Not a bool")
			p.invalmsghdlr(w, r, s)
			return
		}
	}
	// Send json message to all
	jsonbytes, err := jsoniter.Marshal(actionMsg)
	common.Assert(err == nil, err)

	switch actionMsg.Action {
	case api.ActEvict, api.ActDelete:
		method = http.MethodDelete
	case api.ActPrefetch:
		method = http.MethodPost
	default:
		s := fmt.Sprintf("Action unavailable for List/Range Operations: %s", actionMsg.Action)
		p.invalmsghdlr(w, r, s)
		return
	}

	var (
		q       = url.Values{}
		results chan callResult
		timeout time.Duration
	)

	q.Set(api.URLParamLocal, strconv.FormatBool(islocal))
	if wait {
		timeout = longTimeout
	} else {
		timeout = defaultTimeout
	}

	smap := p.smapowner.get()
	results = p.broadcastTargets(
		common.URLPath(api.Version, api.Buckets, bucket),
		q,
		method,
		jsonbytes,
		smap,
		timeout,
	)

	for result := range results {
		if result.err != nil {
			p.invalmsghdlr(
				w,
				r,
				fmt.Sprintf(
					"Failed to execute List/Range request: %v (%d: %s)",
					result.err,
					result.status,
					result.errstr,
				),
			)

			return
		}
	}
}

//============
//
// AuthN stuff
//
//============
func (p *proxyrunner) httpTokenDelete(w http.ResponseWriter, r *http.Request) {
	tokenList := &TokenList{}
	if _, err := p.checkRESTItems(w, r, 0, false, api.Version, api.Tokens); err != nil {
		return
	}

	msg := &api.ActionMsg{Action: api.ActRevokeToken}
	if p.forwardCP(w, r, msg, "revoke token", nil) {
		return
	}

	if err := p.readJSON(w, r, tokenList); err != nil {
		s := fmt.Sprintf("Invalid token list: %v", err)
		p.invalmsghdlr(w, r, s)
		return
	}

	p.authn.updateRevokedList(tokenList)

	if p.smapowner.get().isPrimary(p.si) {
		p.metasyncer.sync(false, p.authn.revokedTokenList(), msg)
	}
}

// Read a token from request header and validates it
// Header format:
//		'Authorization: Bearer <token>'
func (p *proxyrunner) validateToken(r *http.Request) (*authRec, error) {
	s := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
	if len(s) != 2 || s[0] != tokenStart {
		return nil, fmt.Errorf("Invalid request")
	}

	if p.authn == nil {
		return nil, fmt.Errorf("Invalid credentials")
	}

	auth, err := p.authn.validateToken(s[1])
	if err != nil {
		glog.Errorf("Invalid token: %v", err)
		return nil, fmt.Errorf("Invalid token")
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

		if ctx.config.Auth.Enabled {
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
	getWhat := r.URL.Query().Get(api.URLParamWhat)
	switch getWhat {
	case api.GetWhatConfig, api.GetWhatBucketMeta, api.GetWhatSmapVote, api.GetWhatDaemonInfo:
		p.httprunner.httpdaeget(w, r)
	case api.GetWhatStats:
		rst := getproxystatsrunner()
		rst.RLock()
		jsbytes, err := jsoniter.Marshal(rst)
		rst.RUnlock()
		common.Assert(err == nil, err)
		p.writeJSON(w, r, jsbytes, "httpdaeget-"+getWhat)
	case api.GetWhatSmap:
		smap := p.smapowner.get()
		for smap == nil || !smap.isValid() {
			if p.startedup(0) != 0 { // must be starting up
				smap = p.smapowner.get()
				common.Assert(smap.isValid())
				break
			}
			glog.Errorf("%s is starting up: cannot execute GET %s yet...", p.si.DaemonID, api.GetWhatSmap)
			time.Sleep(time.Second)
			smap = p.smapowner.get()
		}
		jsbytes, err := jsoniter.Marshal(smap)
		common.Assert(err == nil, err)
		p.writeJSON(w, r, jsbytes, "httpdaeget-"+getWhat)
	default:
		p.httprunner.httpdaeget(w, r)
	}
}

func (p *proxyrunner) httpdaeput(w http.ResponseWriter, r *http.Request) {
	apitems, err := p.checkRESTItems(w, r, 0, true, api.Version, api.Daemon)
	if err != nil {
		return
	}
	if len(apitems) > 0 {
		switch apitems[0] {
		case api.Proxy:
			p.httpdaesetprimaryproxy(w, r)
			return
		case api.SyncSmap:
			var newsmap = &SmapX{}
			if p.readJSON(w, r, newsmap) != nil {
				return
			}
			if !newsmap.isValid() {
				s := fmt.Sprintf("Received invalid Smap at startup/registration: %s", newsmap.pp())
				glog.Errorln(s)
				p.invalmsghdlr(w, r, s)
				return
			}
			if !newsmap.isPresent(p.si, true) {
				s := fmt.Sprintf("Not finding self %s in the %s", p.si.DaemonID, newsmap.pp())
				glog.Errorln(s)
				p.invalmsghdlr(w, r, s)
				return
			}
			if s := p.smapowner.synchronize(newsmap, true /*saveSmap*/, true /* lesserIsErr */); s != "" {
				p.invalmsghdlr(w, r, s)
			}
			glog.Infof("%s: %s v%d done", p.si.DaemonID, api.SyncSmap, newsmap.version())
			return
		default:
		}
	}

	//
	// other PUT /daemon actions
	//
	var msg api.ActionMsg
	if p.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case api.ActSetConfig:
		var (
			value string
			ok    bool
		)
		if value, ok = msg.Value.(string); !ok {
			p.invalmsghdlr(w, r, fmt.Sprintf("Failed to parse ActionMsg value: not a string"))
			return
		}
		if errstr := p.setconfig(msg.Name, value); errstr != "" {
			p.invalmsghdlr(w, r, errstr)
		} else {
			// NOTE: "loglevel", "stats_time", "vmodule" are supported by both proxies and targets;
			// other knobs are being set so that all proxies remain in-sync in the case when
			// the primary broadcasts the change to all nodes...
			glog.Infof("setconfig %s=%s", msg.Name, value)
		}
	case api.ActShutdown:
		q := r.URL.Query()
		force, _ := parsebool(q.Get(api.URLParamForce))
		if p.smapowner.get().isPrimary(p.si) && !force {
			s := fmt.Sprintf("Cannot shutdown primary proxy without %s=true query parameter", api.URLParamForce)
			p.invalmsghdlr(w, r, s)
			return
		}
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	default:
		s := fmt.Sprintf("Unexpected ActionMsg <- JSON [%v]", msg)
		p.invalmsghdlr(w, r, s)
	}
}

func (p *proxyrunner) smapFromURL(baseURL string) (smap *SmapX, errstr string) {
	query := url.Values{}
	query.Add(api.URLParamWhat, api.GetWhatSmap)
	req := reqArgs{
		method: http.MethodGet,
		base:   baseURL,
		path:   common.URLPath(api.Version, api.Daemon),
		query:  query,
	}
	args := callArgs{req: req, timeout: defaultTimeout}
	res := p.call(args)
	if res.err != nil {
		return nil, fmt.Sprintf("Failed to get smap from %s: %v", baseURL, res.err)
	}
	smap = &SmapX{}
	if err := jsoniter.Unmarshal(res.outjson, smap); err != nil {
		return nil, fmt.Sprintf("Failed to unmarshal smap: %v", err)
	}
	if !smap.isValid() {
		return nil, fmt.Sprintf("Invalid Smap from %s: %s", baseURL, smap.pp())
	}

	return smap, ""
}

// forceful primary change - is used when the original primary network is down
// for a while and the remained nodes selected a new primary. After the
// original primary is back it does not attach automatically to the new primary
// and the cluster gets into split-brain mode. This request makes original
// primary connect to the new primary
func (p *proxyrunner) forcefulJoin(w http.ResponseWriter, r *http.Request, proxyID string) {
	newPrimaryURL := r.URL.Query().Get(api.URLParamPrimaryCandidate)
	glog.Infof("Force new primary %s (URL: %s)", proxyID, newPrimaryURL)

	if p.si.DaemonID == proxyID {
		glog.Warningf("Proxy %s(self) is already the primary", proxyID)
		return
	}

	smap := p.smapowner.get()
	psi := smap.GetProxy(proxyID)

	if psi == nil && newPrimaryURL == "" {
		s := fmt.Sprintf("Failed to find new primary %s in local smap: %s",
			proxyID, smap.pp())
		p.invalmsghdlr(w, r, s)
		return
	}

	if newPrimaryURL == "" {
		newPrimaryURL = psi.PublicNet.DirectURL
	}

	if newPrimaryURL == "" {
		s := fmt.Sprintf("Failed to get new primary %s direct URL", proxyID)
		p.invalmsghdlr(w, r, s)
		return
	}

	newSmap, errstr := p.smapFromURL(newPrimaryURL)
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}

	if proxyID != newSmap.ProxySI.DaemonID {
		s := fmt.Sprintf("Proxy %s is not a primary. Current smap: %s", proxyID, newSmap.pp())
		p.invalmsghdlr(w, r, s)
		return
	}

	// notify metasync to cancel all pending sync requests
	p.metasyncer.becomeNonPrimary()
	p.smapowner.put(newSmap)
	res := p.registerToURL(newSmap.ProxySI.PublicNet.DirectURL, newSmap.ProxySI, defaultTimeout, true, nil, false)
	if res.err != nil {
		p.invalmsghdlr(w, r, res.err.Error())
		return
	}
}

func (p *proxyrunner) httpdaesetprimaryproxy(w http.ResponseWriter, r *http.Request) {
	var (
		prepare bool
	)

	apitems, err := p.checkRESTItems(w, r, 2, false, api.Version, api.Daemon)
	if err != nil {
		return
	}

	proxyID := apitems[1]
	force, _ := parsebool(r.URL.Query().Get(api.URLParamForce))
	// forceful primary change
	if force && apitems[0] == api.Proxy {
		if !p.smapowner.get().isPrimary(p.si) {
			s := fmt.Sprintf("Proxy %s is not a primary", p.si.DaemonID)
			p.invalmsghdlr(w, r, s)
		}
		p.forcefulJoin(w, r, proxyID)
		return
	}

	query := r.URL.Query()
	preparestr := query.Get(api.URLParamPrepare)
	if prepare, err = strconv.ParseBool(preparestr); err != nil {
		s := fmt.Sprintf("Failed to parse %s URL parameter: %v", api.URLParamPrepare, err)
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
	if !smap.isPresent(p.si, true) {
		common.Assert(false, "This proxy '"+p.si.DaemonID+"' must always be present in the local "+smap.pp())
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

	msg := &api.ActionMsg{Action: api.ActNewPrimary}
	bucketmd := p.bmdowner.get()
	if glog.V(3) {
		glog.Infof("Distributing Smap v%d with the newly elected primary %s = self", clone.version(), p.si.DaemonID)
		glog.Infof("Distributing bucket-metadata v%d as well", bucketmd.version())
	}
	p.metasyncer.sync(true, clone, msg, bucketmd, msg)
	return
}

func (p *proxyrunner) httpclusetprimaryproxy(w http.ResponseWriter, r *http.Request) {
	apitems, err := p.checkRESTItems(w, r, 1, false, api.Version, api.Cluster, api.Proxy)
	if err != nil {
		return
	}
	proxyid := apitems[0]
	s := "designate new primary proxy '" + proxyid + "'"
	if p.forwardCP(w, r, &api.ActionMsg{}, s, nil) {
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
		common.Assert(p.si.DaemonID == smap.ProxySI.DaemonID) // must be forwardCP-ed
		glog.Warningf("Request to set primary = %s = self: nothing to do", proxyid)
		return
	}

	// (I) prepare phase
	urlPath := common.URLPath(api.Version, api.Daemon, api.Proxy, proxyid)
	q := url.Values{}
	q.Set(api.URLParamPrepare, "true")
	method := http.MethodPut
	results := p.broadcastCluster(
		urlPath,
		q,
		method,
		nil, // body
		smap,
		ctx.config.Timeout.CplaneOperation,
		false,
	)
	for result := range results {
		if result.err != nil {
			s := fmt.Sprintf("Failed to set primary %s: err %v from %s in the prepare phase", proxyid, result.err, result.si.DaemonID)
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
	q.Set(api.URLParamPrepare, "false")
	results = p.broadcastCluster(
		urlPath,
		q,
		method,
		nil, // body
		p.smapowner.get(),
		ctx.config.Timeout.CplaneOperation,
		false,
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
		common.InvalidHandlerWithMsg(w, r, "invalid method for /cluster path")
	}
}

// [METHOD] /v1/tokens
func (p *proxyrunner) tokenHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodDelete:
		p.httpTokenDelete(w, r)
	default:
		common.InvalidHandlerWithMsg(w, r, "invalid method for /token path")
	}
}

// handler for DFC when utilized as a reverse proxy to handle unmodified requests
// (not to confuse with p.rproxy)
func (p *proxyrunner) reverseProxyHandler(w http.ResponseWriter, r *http.Request) {
	baseURL := r.URL.Scheme + "://" + r.URL.Host
	if baseURL == gcsURL && r.Method == http.MethodGet {
		s := common.RESTItems(r.URL.Path)
		if len(s) == 2 {
			r.URL.Path = common.URLPath(api.Version, api.Objects) + r.URL.Path
			p.httpobjget(w, r)
			return
		} else if len(s) == 1 {
			r.URL.Path = common.URLPath(api.Version, api.Buckets) + r.URL.Path
			p.httpbckget(w, r)
			return
		}
	}
	p.rproxy.cloud.ServeHTTP(w, r)
}

// gets target info
func (p *proxyrunner) httpcluget(w http.ResponseWriter, r *http.Request) {
	getWhat := r.URL.Query().Get(api.URLParamWhat)
	switch getWhat {
	case api.GetWhatStats:
		ok := p.invokeHttpGetClusterStats(w, r)
		if !ok {
			return
		}
	case api.GetWhatXaction:
		ok := p.invokeHttpGetXaction(w, r)
		if !ok {
			return
		}
	case api.GetWhatMountpaths:
		if ok := p.invokeHttpGetClusterMountpaths(w, r); !ok {
			return
		}
	default:
		s := fmt.Sprintf("Unexpected GET request, invalid param 'what': [%s]", getWhat)
		common.InvalidHandlerWithMsg(w, r, s)
	}
}

func (p *proxyrunner) invokeHttpGetXaction(w http.ResponseWriter, r *http.Request) bool {
	kind := r.URL.Query().Get(api.URLParamProps)
	if errstr := validateXactionQueryable(kind); errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return false
	}
	outputXactionStats := &XactionStats{}
	outputXactionStats.Kind = kind
	targetStats, ok := p.invokeHttpGetMsgOnTargets(w, r)
	if !ok {
		e := fmt.Sprintf(
			"Unable to invoke api.GetMsg on targets. Query: [%s]",
			r.URL.RawQuery)
		glog.Errorf(e)
		p.invalmsghdlr(w, r, e)
		return false
	}

	outputXactionStats.TargetStats = targetStats
	jsonBytes, err := jsoniter.Marshal(outputXactionStats)
	if err != nil {
		glog.Errorf(
			"Unable to marshal outputXactionStats. Error: [%s]", err)
		p.invalmsghdlr(w, r, err.Error())
		return false
	}

	ok = p.writeJSON(w, r, jsonBytes, "getXaction")
	return ok
}

func (p *proxyrunner) invokeHttpGetMsgOnTargets(w http.ResponseWriter, r *http.Request) (map[string]jsoniter.RawMessage, bool) {
	results := p.broadcastTargets(
		common.URLPath(api.Version, api.Daemon),
		r.URL.Query(),
		r.Method,
		nil, // message
		p.smapowner.get(),
		ctx.config.Timeout.Default,
	)

	targetResults := make(map[string]jsoniter.RawMessage, p.smapowner.get().CountTargets())
	for result := range results {
		if result.err != nil {
			glog.Errorf(
				"Failed to fetch xaction, query: %s",
				r.URL.RawQuery)
			p.invalmsghdlr(w, r, result.errstr)
			return nil, false
		}

		targetResults[result.si.DaemonID] = jsoniter.RawMessage(result.outjson)
	}

	return targetResults, true
}

// FIXME: read-lock
func (p *proxyrunner) invokeHttpGetClusterStats(w http.ResponseWriter, r *http.Request) bool {
	targetStats, ok := p.invokeHttpGetMsgOnTargets(w, r)
	if !ok {
		errstr := fmt.Sprintf(
			"Unable to invoke api.GetMsg on targets. Query: [%s]",
			r.URL.RawQuery)
		glog.Errorf(errstr)
		p.invalmsghdlr(w, r, errstr)
		return false
	}

	out := &ClusterStatsRaw{}
	out.Target = targetStats
	rr := getproxystatsrunner()
	rr.RLock()
	out.Proxy = rr.Core
	jsbytes, err := jsoniter.Marshal(out)
	rr.RUnlock()
	common.Assert(err == nil, err)
	ok = p.writeJSON(w, r, jsbytes, "HttpGetClusterStats")
	return ok
}

func (p *proxyrunner) invokeHttpGetClusterMountpaths(w http.ResponseWriter, r *http.Request) bool {
	targetMountpaths, ok := p.invokeHttpGetMsgOnTargets(w, r)
	if !ok {
		errstr := fmt.Sprintf(
			"Unable to invoke api.GetMsg on targets. Query: [%s]", r.URL.RawQuery)
		glog.Errorf(errstr)
		p.invalmsghdlr(w, r, errstr)
		return false
	}

	out := &ClusterMountpathsRaw{}
	out.Targets = targetMountpaths
	jsbytes, err := jsoniter.Marshal(out)
	common.Assert(err == nil, err)
	ok = p.writeJSON(w, r, jsbytes, "HttpGetClusterMountpaths")
	return ok
}

// register|keepalive target|proxy
func (p *proxyrunner) httpclupost(w http.ResponseWriter, r *http.Request) {
	var (
		nsi                   cluster.Snode
		keepalive, register   bool
		isproxy, nonelectable bool
		msg                   *api.ActionMsg
		s                     string
	)
	apitems, err := p.checkRESTItems(w, r, 0, true, api.Version, api.Cluster)
	if err != nil {
		return
	}
	if p.readJSON(w, r, &nsi) != nil {
		return
	}
	if len(apitems) > 0 {
		keepalive = apitems[0] == api.Keepalive
		register = apitems[0] == api.Register
		isproxy = apitems[0] == api.Proxy
		if isproxy {
			if len(apitems) > 1 {
				keepalive = apitems[1] == api.Keepalive
			}
			s := r.URL.Query().Get(api.URLParamNonElectable)
			var err error
			if nonelectable, err = parsebool(s); s != "" && err != nil {
				glog.Errorf("Failed to parse %s for non-electability: %v", s, err)
			}
		}
	}
	s = fmt.Sprintf("register %s (isproxy=%t, keepalive=%t)", nsi.DaemonID, isproxy, keepalive)
	msg = &api.ActionMsg{Action: api.ActRegTarget}
	if isproxy {
		msg = &api.ActionMsg{Action: api.ActRegProxy}
	}
	body, err := jsoniter.Marshal(nsi)
	common.Assert(err == nil, err)
	if p.forwardCP(w, r, msg, s, body) {
		return
	}
	if net.ParseIP(nsi.PublicNet.NodeIPAddr) == nil {
		s := fmt.Sprintf("register target %s: invalid IP address %v", nsi.DaemonID, nsi.PublicNet.NodeIPAddr)
		p.invalmsghdlr(w, r, s)
		return
	}

	p.statsif.add(statPostCount, 1)

	p.smapowner.Lock()
	smap := p.smapowner.get()
	if isproxy {
		osi := smap.GetProxy(nsi.DaemonID)
		if !p.addOrUpdateNode(&nsi, osi, keepalive, "proxy") {
			p.smapowner.Unlock()
			return
		}
	} else {
		osi := smap.GetTarget(nsi.DaemonID)
		if !p.addOrUpdateNode(&nsi, osi, keepalive, "target") {
			p.smapowner.Unlock()
			return
		}
		if register {
			if glog.V(3) {
				glog.Infof("register target %s (num targets before %d)", nsi.DaemonID, smap.CountTargets())
			}
			args := callArgs{
				si: &nsi,
				req: reqArgs{
					method: http.MethodPost,
					path:   common.URLPath(api.Version, api.Daemon, api.Register),
				},
				timeout: ctx.config.Timeout.ProxyPing,
			}
			res := p.call(args)
			if res.err != nil {
				p.smapowner.Unlock()
				errstr := fmt.Sprintf("Failed to register target %s: %v, %s", nsi.DaemonID, res.err, res.errstr)
				p.invalmsghdlr(w, r, errstr, res.status)
				return
			}
		}
	}
	if p.startedup(0) == 0 { // see clusterStartup()
		p.registerToSmap(isproxy, &nsi, nonelectable)
		p.smapowner.Unlock()
		return
	}
	p.smapowner.Unlock()

	// upon joining a running cluster new target gets bucket-metadata right away
	if !isproxy {
		bmd4reg := p.bmdowner.get()
		outjson, err := jsoniter.Marshal(bmd4reg)
		common.Assert(err == nil, err)
		p.writeJSON(w, r, outjson, "bucket-metadata")
	}
	// update and distribute Smap
	go func(isproxy, nonelectable bool) {
		p.smapowner.Lock()

		p.registerToSmap(isproxy, &nsi, nonelectable)
		smap := p.smapowner.get()

		if errstr := p.smapowner.persist(smap, true); errstr != "" {
			glog.Errorln(errstr)
		}
		p.smapowner.Unlock()
		tokens := p.authn.revokedTokenList()
		msg.Action = path.Join(msg.Action, nsi.DaemonID)
		if len(tokens.Tokens) == 0 {
			p.metasyncer.sync(false, smap, msg)
		} else {
			p.metasyncer.sync(false, smap, msg, tokens, msg)
		}
	}(isproxy, nonelectable)
}

func (p *proxyrunner) registerToSmap(isproxy bool, nsi *cluster.Snode, nonelectable bool) {
	clone := p.smapowner.get().clone()
	id := nsi.DaemonID
	if isproxy {
		if clone.GetProxy(id) != nil { // update if need be - see addOrUpdateNode()
			clone.delProxy(id)
		}
		clone.addProxy(nsi)
		if nonelectable {
			if clone.NonElects == nil {
				clone.NonElects = make(common.SimpleKVs)
			}
			clone.NonElects[id] = ""
			glog.Infof("Note: proxy %s won't be electable", id)
		}
		if glog.V(3) {
			glog.Infof("joined proxy %s (num proxies %d)", id, clone.CountProxies())
		}
	} else {
		if clone.GetTarget(id) != nil { // ditto
			clone.delTarget(id)
		}
		clone.addTarget(nsi)
		if glog.V(3) {
			glog.Infof("joined target %s (num targets %d)", id, clone.CountTargets())
		}
	}
	p.smapowner.put(clone)
}

func (p *proxyrunner) addOrUpdateNode(nsi *cluster.Snode, osi *cluster.Snode, keepalive bool, kind string) bool {
	if keepalive {
		if osi == nil {
			glog.Warningf("register/keepalive %s %s: adding back to the cluster map", kind, nsi.DaemonID)
			return true
		}

		if !osi.Equals(nsi) {
			if p.detectDaemonDuplicate(osi, nsi) {
				glog.Errorf("Daemon %s tried to register/keepalive with a duplicate ID %s", nsi.PublicNet.DirectURL, nsi.DaemonID)
				return false
			}
			glog.Warningf("register/keepalive %s %s: info changed - renewing", kind, nsi.DaemonID)
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
			glog.Infof("register %s %s: already done", kind, nsi.DaemonID)
			return false
		}
		glog.Warningf("register %s %s: renewing the registration %+v => %+v", kind, nsi.DaemonID, osi, nsi)
	}
	return true
}

// unregisters a target/proxy
func (p *proxyrunner) httpcludel(w http.ResponseWriter, r *http.Request) {
	apitems, err := p.checkRESTItems(w, r, 1, true, api.Version, api.Cluster, api.Daemon)
	if err != nil {
		return
	}
	var (
		isproxy bool
		msg     *api.ActionMsg
		osi     *cluster.Snode
		psi     *cluster.Snode
		sid     = apitems[0]
	)
	msg = &api.ActionMsg{Action: api.ActUnregTarget}
	if sid == api.Proxy {
		msg = &api.ActionMsg{Action: api.ActUnregProxy}
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
			glog.Infof("unregistered proxy {%s} (num proxies %d)", sid, clone.CountProxies())
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
			glog.Infof("unregistered target {%s} (num targets %d)", sid, clone.CountTargets())
		}
		args := callArgs{
			si: osi,
			req: reqArgs{
				method: http.MethodDelete,
				path:   common.URLPath(api.Version, api.Daemon, api.Unregister),
			},
			timeout: ctx.config.Timeout.ProxyPing,
		}
		res := p.call(args)
		if res.err != nil {
			glog.Warningf("The target %s that is being unregistered failed to respond back: %v, %s",
				osi.DaemonID, res.err, res.errstr)
		}
	}
	if p.startedup(0) == 0 { // see clusterStartup()
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

	p.metasyncer.sync(true, clone, msg)
}

// '{"action": "shutdown"}' /v1/cluster => (proxy) =>
// '{"action": "syncsmap"}' /v1/cluster => (proxy) => PUT '{Smap}' /v1/daemon/syncsmap => target(s)
// '{"action": "rebalance"}' /v1/cluster => (proxy) => PUT '{Smap}' /v1/daemon/rebalance => target(s)
// '{"action": "setconfig"}' /v1/cluster => (proxy) =>
func (p *proxyrunner) httpcluput(w http.ResponseWriter, r *http.Request) {
	var msg api.ActionMsg
	apitems, err := p.checkRESTItems(w, r, 0, true, api.Version, api.Cluster)
	if err != nil {
		return
	}
	// cluster-wide: designate a new primary proxy administratively
	if len(apitems) > 0 && apitems[0] == api.Proxy {
		p.httpclusetprimaryproxy(w, r)
		return
	}
	if p.readJSON(w, r, &msg) != nil {
		return
	}
	if p.forwardCP(w, r, &msg, "", nil) {
		return
	}

	switch msg.Action {
	case api.ActSetConfig:
		if value, ok := msg.Value.(string); !ok {
			p.invalmsghdlr(w, r, fmt.Sprintf("Invalid Value format (%+v, %T)", msg.Value, msg.Value))
		} else if errstr := p.setconfig(msg.Name, value); errstr != "" {
			p.invalmsghdlr(w, r, errstr)
		} else {
			glog.Infof("setconfig %s=%s", msg.Name, value)
			msgbytes, err := jsoniter.Marshal(msg) // same message -> all targets and proxies
			common.Assert(err == nil, err)

			results := p.broadcastCluster(
				common.URLPath(api.Version, api.Daemon),
				nil, // query
				http.MethodPut,
				msgbytes,
				p.smapowner.get(),
				defaultTimeout,
				false,
			)

			for result := range results {
				if result.err != nil {
					p.invalmsghdlr(
						w,
						r,
						fmt.Sprintf("%s (%s = %s) failed, err: %s", msg.Action, msg.Name, value, result.errstr),
					)
					p.keepalive.onerr(err, result.status)
				}
			}
		}
	case api.ActShutdown:
		glog.Infoln("Proxy-controlled cluster shutdown...")
		msgbytes, err := jsoniter.Marshal(msg) // same message -> all targets
		common.Assert(err == nil, err)

		p.broadcastCluster(
			common.URLPath(api.Version, api.Daemon),
			nil, // query
			http.MethodPut,
			msgbytes,
			p.smapowner.get(),
			defaultTimeout,
			false,
		)

		time.Sleep(time.Second)
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)

	case api.ActGlobalReb:
		p.metasyncer.sync(false, p.smapowner.get(), &msg)

	default:
		s := fmt.Sprintf("Unexpected api.ActionMsg <- JSON [%v]", msg)
		p.invalmsghdlr(w, r, s)
	}
}

//========================
//
// broadcasts: Rx and Tx
//
//========================
func (p *proxyrunner) receiveBucketMD(newbucketmd *bucketMD, msg *api.ActionMsg) (errstr string) {
	if msg.Action == "" {
		glog.Infof("receive bucket-metadata: version %d", newbucketmd.version())
	} else {
		glog.Infof("receive bucket-metadata: version %d, action %s", newbucketmd.version(), msg.Action)
	}
	p.bmdowner.Lock()
	myver := p.bmdowner.get().version()
	if newbucketmd.version() <= myver {
		p.bmdowner.Unlock()
		if newbucketmd.version() < myver {
			errstr = fmt.Sprintf("Attempt to downgrade bucket-metadata version %d to %d", myver, newbucketmd.version())
		}
		return
	}
	p.bmdowner.put(newbucketmd)
	if errstr := p.savebmdconf(newbucketmd); errstr != "" {
		glog.Errorln(errstr)
	}
	p.bmdowner.Unlock()
	return
}

// broadcastCluster sends a message ([]byte) to all proxies and targets belongs to a smap
func (p *proxyrunner) broadcastCluster(path string, query url.Values, method string, body []byte,
	smap *SmapX, timeout time.Duration, internal bool) chan callResult {

	bcastArgs := bcastCallArgs{
		req: reqArgs{
			method: method,
			path:   path,
			query:  query,
			body:   body,
		},
		internal: internal,
		timeout:  timeout,
		servers:  []map[string]*cluster.Snode{smap.Pmap, smap.Tmap},
	}
	return p.broadcast(bcastArgs)
}

// broadcastTargets sends a message ([]byte) to all targets belongs to a smap
func (p *proxyrunner) broadcastTargets(path string, query url.Values, method string, body []byte,
	smap *SmapX, timeout time.Duration) chan callResult {

	bcastArgs := bcastCallArgs{
		req: reqArgs{
			method: method,
			path:   path,
			query:  query,
			body:   body,
		},
		timeout: timeout,
		servers: []map[string]*cluster.Snode{smap.Tmap},
	}
	return p.broadcast(bcastArgs)
}

func (p *proxyrunner) urlOutsideCluster(url string) bool {
	smap := p.smapowner.get()
	for _, proxyInfo := range smap.Pmap {
		if proxyInfo.InternalNet.DirectURL == url || proxyInfo.PublicNet.DirectURL == url ||
			proxyInfo.ReplNet.DirectURL == url {
			return false
		}
	}
	for _, targetInfo := range smap.Tmap {
		if targetInfo.InternalNet.DirectURL == url || targetInfo.PublicNet.DirectURL == url ||
			targetInfo.ReplNet.DirectURL == url {
			return false
		}
	}
	return true
}

func (p *proxyrunner) validateBucketProps(props *BucketProps, isLocal bool) error {
	if props.NextTierURL != "" {
		if _, err := url.ParseRequestURI(props.NextTierURL); err != nil {
			return fmt.Errorf("invalid next tier URL: %s, err: %v", props.NextTierURL, err)
		}
		if !p.urlOutsideCluster(props.NextTierURL) {
			return fmt.Errorf("Invalid next tier URL: %s, URL is in current cluster", props.NextTierURL)
		}

	}
	if err := validateCloudProvider(props.CloudProvider, isLocal); err != nil {
		return err
	}
	if props.ReadPolicy != "" && props.ReadPolicy != RWPolicyCloud && props.ReadPolicy != RWPolicyNextTier {
		return fmt.Errorf("invalid read policy: %s", props.ReadPolicy)
	}
	if props.ReadPolicy == RWPolicyCloud && isLocal {
		return fmt.Errorf("read policy for local bucket cannot be '%s'", RWPolicyCloud)
	}
	if props.WritePolicy != "" && props.WritePolicy != RWPolicyCloud && props.WritePolicy != RWPolicyNextTier {
		return fmt.Errorf("invalid write policy: %s", props.WritePolicy)
	}
	if props.WritePolicy == RWPolicyCloud && isLocal {
		return fmt.Errorf("write policy for local bucket cannot be '%s'", RWPolicyCloud)
	}
	if props.NextTierURL != "" {
		if props.CloudProvider == "" {
			return fmt.Errorf("tiered bucket must use one of the supported cloud providers (%s | %s | %s)",
				api.ProviderAmazon, api.ProviderGoogle, api.ProviderDFC)
		}
		if props.ReadPolicy == "" {
			props.ReadPolicy = RWPolicyNextTier
		}
		if props.WritePolicy == "" && !isLocal {
			props.WritePolicy = RWPolicyCloud
		} else if props.WritePolicy == "" && isLocal {
			props.WritePolicy = RWPolicyNextTier
		}
	}
	if props.CksumConf.Checksum != api.ChecksumInherit &&
		props.CksumConf.Checksum != api.ChecksumNone && props.CksumConf.Checksum != api.ChecksumXXHash {
		return fmt.Errorf("invalid checksum: %s - expecting %s or %s or %s",
			props.CksumConf.Checksum, api.ChecksumXXHash, api.ChecksumNone, api.ChecksumInherit)
	}

	lwm, hwm := props.LRUProps.LowWM, props.LRUProps.HighWM
	if lwm < 0 || hwm < 0 || lwm > 100 || hwm > 100 || lwm > hwm {
		return fmt.Errorf("Invalid WM configuration. LowWM: %d, HighWM: %d, LowWM and HighWM must be in the range [0, 100] with LowWM <= HighWM", lwm, hwm)
	}
	if props.LRUProps.AtimeCacheMax < 0 {
		return fmt.Errorf("Invalid value: %d, AtimeCacheMax cannot be negative", props.LRUProps.AtimeCacheMax)
	}
	if props.LRUProps.DontEvictTimeStr != "" {
		dontEvictTime, err := time.ParseDuration(props.LRUProps.DontEvictTimeStr)
		if err != nil {
			return fmt.Errorf("Bad dont_evict_time format %s, err: %v", dontEvictTime, err)
		}
		props.LRUProps.DontEvictTime = dontEvictTime

	}
	if props.LRUProps.CapacityUpdTimeStr != "" {
		capacityUpdTime, err := time.ParseDuration(props.LRUProps.CapacityUpdTimeStr)
		if err != nil {
			return fmt.Errorf("Bad capacity_upd_time format %s, err: %v", capacityUpdTime, err)
		}
		props.LRUProps.CapacityUpdTime = capacityUpdTime
	}
	return nil
}

func rechecksumRequired(globalChecksum string, bucketChecksumOld string, bucketChecksumNew string) bool {
	checksumOld := globalChecksum
	if bucketChecksumOld != api.ChecksumInherit {
		checksumOld = bucketChecksumOld
	}
	checksumNew := globalChecksum
	if bucketChecksumNew != api.ChecksumInherit {
		checksumNew = bucketChecksumNew
	}
	return checksumNew != api.ChecksumNone && checksumNew != checksumOld
}

func (p *proxyrunner) notifyTargetsRechecksum(bucket string) {
	jsbytes, err := jsoniter.Marshal(api.ActionMsg{Action: api.ActRechecksum})
	common.Assert(err == nil, err)

	res := p.broadcastTargets(
		common.URLPath(api.Version, api.Buckets, bucket),
		nil,
		http.MethodPost,
		jsbytes,
		p.smapowner.get(),
		ctx.config.Timeout.Default,
	)

	for r := range res {
		if r.err != nil {
			glog.Warningf("Target %s failed to re-checksum objects in bucket %s", r.si.DaemonID, bucket)
		}
	}
}

// detectDaemonDuplicate queries osi for its daemon info in order to determine if info has changed
// and is equal to nsi
func (p *proxyrunner) detectDaemonDuplicate(osi *cluster.Snode, nsi *cluster.Snode) bool {
	query := url.Values{}
	query.Add(api.URLParamWhat, api.GetWhatDaemonInfo)
	args := callArgs{
		si: osi,
		req: reqArgs{
			method: http.MethodGet,
			path:   common.URLPath(api.Version, api.Daemon),
			query:  query,
		},
		timeout: ctx.config.Timeout.CplaneOperation,
	}
	res := p.call(args)
	if res.err != nil || res.outjson == nil || len(res.outjson) == 0 {
		// error getting response from osi
		return false
	}
	si := &cluster.Snode{}
	if err := jsoniter.Unmarshal(res.outjson, si); err != nil {
		common.Assert(false, err)
	}
	return !nsi.Equals(si)
}

func validateCloudProvider(provider string, isLocal bool) error {
	if provider != "" && provider != api.ProviderAmazon && provider != api.ProviderGoogle && provider != api.ProviderDFC {
		return fmt.Errorf("invalid cloud provider: %s, must be one of (%s | %s | %s)", provider,
			api.ProviderAmazon, api.ProviderGoogle, api.ProviderDFC)
	} else if isLocal && provider != api.ProviderDFC && provider != "" {
		return fmt.Errorf("local bucket can only have '%s' as the cloud provider", api.ProviderDFC)
	}
	return nil
}

func (p *proxyrunner) copyBucketProps(oldProps *BucketProps, newProps *BucketProps, bucket string) {
	oldProps.NextTierURL = newProps.NextTierURL
	oldProps.CloudProvider = newProps.CloudProvider
	if newProps.ReadPolicy != "" {
		oldProps.ReadPolicy = newProps.ReadPolicy
	}
	if newProps.WritePolicy != "" {
		oldProps.WritePolicy = newProps.WritePolicy
	}
	if rechecksumRequired(ctx.config.Cksum.Checksum, oldProps.CksumConf.Checksum, newProps.CksumConf.Checksum) {
		go p.notifyTargetsRechecksum(bucket)
	}
	if newProps.CksumConf.Checksum != "" {
		oldProps.CksumConf.Checksum = newProps.CksumConf.Checksum
		if newProps.CksumConf.Checksum != api.ChecksumInherit {
			oldProps.CksumConf.ValidateColdGet = newProps.CksumConf.ValidateColdGet
			oldProps.CksumConf.ValidateWarmGet = newProps.CksumConf.ValidateWarmGet
			oldProps.CksumConf.EnableReadRangeChecksum = newProps.CksumConf.EnableReadRangeChecksum
		}
	}
	oldProps.LRUProps.LowWM = newProps.LRUProps.LowWM // can't conditionally assign if value != 0 since 0 is valid
	oldProps.LRUProps.HighWM = newProps.LRUProps.HighWM
	oldProps.LRUProps.AtimeCacheMax = newProps.LRUProps.AtimeCacheMax
	if newProps.LRUProps.DontEvictTimeStr != "" {
		oldProps.LRUProps.DontEvictTimeStr = newProps.LRUProps.DontEvictTimeStr
		oldProps.LRUProps.DontEvictTime = newProps.LRUProps.DontEvictTime // parsing done in validateBucketProps()
	}
	if newProps.LRUProps.CapacityUpdTimeStr != "" {
		oldProps.LRUProps.CapacityUpdTimeStr = newProps.LRUProps.CapacityUpdTimeStr
		oldProps.LRUProps.CapacityUpdTime = newProps.LRUProps.CapacityUpdTime // parsing done in validateBucketProps()
	}
	oldProps.LRUProps.LRUEnabled = newProps.LRUProps.LRUEnabled
}
