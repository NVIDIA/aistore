// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
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
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/dsort"
	"github.com/NVIDIA/dfcpub/stats"
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
	entries []*cmn.BucketEntry
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
	config := cmn.GCO.Get()
	p.httprunner.init(getproxystatsrunner(), true)
	p.httprunner.keepalive = getproxykeepalive()

	bucketmdfull := filepath.Join(config.Confdir, cmn.BucketmdBackupFile)
	bucketmd := newBucketMD()
	if cmn.LocalLoad(bucketmdfull, bucketmd) != nil {
		// create empty
		bucketmd.Version = 1
		if err := cmn.LocalSave(bucketmdfull, bucketmd); err != nil {
			glog.Fatalf("FATAL: cannot store bucket-metadata, err: %v", err)
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
			Director:  func(r *http.Request) {},
			Transport: p.createTransport(0, 0), // default idle connections per host, unlimited idle total
		}
	}

	//
	// REST API: register proxy handlers and start listening
	//

	// Public network
	if config.Auth.Enabled {
		p.registerPublicNetHandler(cmn.URLPath(cmn.Version, cmn.Buckets)+"/", wrapHandler(p.bucketHandler, p.checkHTTPAuth))
		p.registerPublicNetHandler(cmn.URLPath(cmn.Version, cmn.Objects)+"/", wrapHandler(p.objectHandler, p.checkHTTPAuth))
	} else {
		p.registerPublicNetHandler(cmn.URLPath(cmn.Version, cmn.Buckets)+"/", p.bucketHandler)
		p.registerPublicNetHandler(cmn.URLPath(cmn.Version, cmn.Objects)+"/", p.objectHandler)
	}

	p.registerPublicNetHandler(cmn.URLPath(cmn.Version, cmn.Daemon), p.daemonHandler)
	p.registerPublicNetHandler(cmn.URLPath(cmn.Version, cmn.Cluster), p.clusterHandler)
	p.registerPublicNetHandler(cmn.URLPath(cmn.Version, cmn.Tokens), p.tokenHandler)
	p.registerPublicNetHandler(cmn.URLPath(cmn.Version, cmn.Sort), dsort.ProxySortHandler)

	if config.Net.HTTP.RevProxy == cmn.RevProxyCloud {
		p.registerPublicNetHandler("/", p.reverseProxyHandler)
	} else {
		p.registerPublicNetHandler("/", cmn.InvalidHandler)
	}

	// Intra control network
	p.registerIntraControlNetHandler(cmn.URLPath(cmn.Version, cmn.Metasync), p.metasyncHandler)
	p.registerIntraControlNetHandler(cmn.URLPath(cmn.Version, cmn.Health), p.healthHandler)
	p.registerIntraControlNetHandler(cmn.URLPath(cmn.Version, cmn.Vote), p.voteHandler)
	if config.Net.UseIntraControl {
		if config.Net.HTTP.RevProxy == cmn.RevProxyCloud {
			p.registerIntraControlNetHandler("/", p.reverseProxyHandler)
		} else {
			p.registerIntraControlNetHandler("/", cmn.InvalidHandler)
		}
	}

	// Intra data network
	if config.Net.UseIntraData {
		p.registerIntraDataNetHandler(cmn.URLPath(cmn.Version, cmn.Objects)+"/", p.objectHandler)
		p.registerIntraDataNetHandler("/", cmn.InvalidHandler)
	}

	glog.Infof("%s: [public net] listening on: %s", p.si, p.si.PublicNet.DirectURL)
	if p.si.PublicNet.DirectURL != p.si.IntraControlNet.DirectURL {
		glog.Infof("%s: [intra control net] listening on: %s", p.si, p.si.IntraControlNet.DirectURL)
	}
	if p.si.PublicNet.DirectURL != p.si.IntraDataNet.DirectURL {
		glog.Infof("%s: [intra data net] listening on: %s", p.si, p.si.IntraDataNet.DirectURL)
	}
	if config.Net.HTTP.RevProxy != "" {
		glog.Warningf("Warning: serving GET /object as a reverse-proxy ('%s')", config.Net.HTTP.RevProxy)
	}
	p.starttime = time.Now()

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
	glog.Infof("Stopping %s (ID %s, primary=%t), err: %v", p.Getname(), p.si, isPrimary, err)
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
	apitems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
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
	apitems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
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
	var msg cmn.ActionMsg
	apitems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}
	switch msg.Action {
	case cmn.ActDestroyLB:
		if p.forwardCP(w, r, &msg, bucket, nil) {
			return
		}
		bucketmd := p.bmdowner.get()
		if !bucketmd.IsLocal(bucket) {
			p.invalmsghdlr(w, r, fmt.Sprintf("Bucket %s does not appear to be local", bucket))
			return
		}
		p.bmdowner.Lock()
		clone := bucketmd.clone()
		if !clone.del(bucket, true) {
			p.bmdowner.Unlock()
			s := fmt.Sprintf("Local bucket %s "+cmn.DoesNotExist, bucket)
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
	case cmn.ActDelete, cmn.ActEvict:
		p.doListRange(w, r, &msg, http.MethodDelete)
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

	cmn.Assert(err == nil, err)
	p.writeJSON(w, r, jsbytes, "proxycorestats")
}

// POST { action } /v1/buckets/bucket-name
func (p *proxyrunner) httpbckpost(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	var msg cmn.ActionMsg
	apitems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	lbucket := apitems[0]
	if !p.validatebckname(w, r, lbucket) {
		return
	}
	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case cmn.ActCreateLB:
		if p.forwardCP(w, r, &msg, lbucket, nil) {
			return
		}
		p.bmdowner.Lock()
		clone := p.bmdowner.get().clone()
		bprops := cmn.BucketProps{
			CksumConf: cmn.CksumConf{Checksum: cmn.ChecksumInherit},
			LRUConf:   cmn.GCO.Get().LRU,
		}
		if !clone.add(lbucket, true, &bprops) {
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
	case cmn.ActRenameLB:
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
		props, ok := clone.Get(bucketFrom, true)
		if !ok {
			s := fmt.Sprintf("Local bucket %s "+cmn.DoesNotExist, bucketFrom)
			p.invalmsghdlr(w, r, s)
			return
		}
		_, ok = clone.Get(bucketTo, true)
		if ok {
			s := fmt.Sprintf("Local bucket %s already exists", bucketTo)
			p.invalmsghdlr(w, r, s)
			return
		}
		if !p.renameLB(bucketFrom, bucketTo, clone, props, &msg) {
			errstr := fmt.Sprintf("Failed to rename local bucket %s => %s", bucketFrom, bucketTo)
			p.invalmsghdlr(w, r, errstr)
		}
		glog.Infof("renamed local bucket %s => %s, bucket-metadata version %d", bucketFrom, bucketTo, clone.version())
	case cmn.ActSyncLB:
		if p.forwardCP(w, r, &msg, "", nil) {
			return
		}
		p.metasyncer.sync(false, p.bmdowner.get(), &msg)
	case cmn.ActPrefetch:
		p.doListRange(w, r, &msg, http.MethodPost)
	case cmn.ActListObjects:
		p.listBucketAndCollectStats(w, r, lbucket, msg, started)
	default:
		s := fmt.Sprintf("Unexpected cmn.ActionMsg <- JSON [%v]", msg)
		p.invalmsghdlr(w, r, s)
	}
}

func (p *proxyrunner) listBucketAndCollectStats(w http.ResponseWriter,
	r *http.Request, lbucket string, msg cmn.ActionMsg, started time.Time) {
	pagemarker, ok := p.listbucket(w, r, lbucket, &msg)
	if ok {
		delta := time.Since(started)
		p.statsif.AddMany(stats.NamedVal64{stats.ListCount, 1}, stats.NamedVal64{stats.ListLatency, int64(delta)})
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
	var msg cmn.ActionMsg
	apitems, err := p.checkRESTItems(w, r, 1, true, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	lbucket := apitems[0]
	if !p.validatebckname(w, r, lbucket) {
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
		glog.Infof("%s %s => %s", r.Method, bucket, si)
	}
	redirecturl := p.redirectURL(r, si.PublicNet.DirectURL, started, bucket)
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)
}

// PUT /v1/buckets/bucket-name
func (p *proxyrunner) httpbckput(w http.ResponseWriter, r *http.Request) {
	apitems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	if !p.validatebckname(w, r, bucket) {
		return
	}
	props := &cmn.BucketProps{} // every field has to have zero value
	msg := cmn.ActionMsg{Value: props}
	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}
	if msg.Action != cmn.ActSetProps && msg.Action != cmn.ActResetProps {
		s := fmt.Sprintf("Invalid cmn.ActionMsg [%v] - expecting '%s' or '%s' action", msg, cmn.ActSetProps, cmn.ActResetProps)
		p.invalmsghdlr(w, r, s)
		return
	}

	p.bmdowner.Lock()
	clone := p.bmdowner.get().clone()
	isLocal := clone.IsLocal(bucket)

	oldProps, exists := clone.Get(bucket, isLocal)
	if !exists {
		cmn.Assert(!isLocal)
		oldProps = &cmn.BucketProps{
			CksumConf: cmn.CksumConf{Checksum: cmn.ChecksumInherit},
			LRUConf:   cmn.GCO.Get().LRU,
		}
		clone.add(bucket, false, oldProps)
	}

	switch msg.Action {
	case cmn.ActSetProps:
		if err := p.validateBucketProps(props, isLocal); err != nil {
			p.bmdowner.Unlock()
			p.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
			return
		}
		p.copyBucketProps(oldProps, props, bucket)
	case cmn.ActResetProps:
		oldProps = &cmn.BucketProps{
			CksumConf: cmn.CksumConf{Checksum: cmn.ChecksumInherit},
			LRUConf:   cmn.GCO.Get().LRU,
		}
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
	checkCached, _ := parsebool(r.URL.Query().Get(cmn.URLParamCheckCached))
	apitems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
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
		cmn.Assert(err == nil, err)
	}
	p.rproxy.Lock()
	if p.rproxy.u != smap.ProxySI.PublicNet.DirectURL {
		p.rproxy.u = smap.ProxySI.PublicNet.DirectURL
		uparsed, err := url.Parse(smap.ProxySI.PublicNet.DirectURL)
		cmn.Assert(err == nil, err)
		p.rproxy.p = httputil.NewSingleHostReverseProxy(uparsed)
		p.rproxy.p.Transport = p.createTransport(proxyMaxIdleConnsPer, 0)
	}
	p.rproxy.Unlock()
	if len(body) > 0 {
		r.Body = ioutil.NopCloser(bytes.NewBuffer(body))
		r.ContentLength = int64(len(body)) // directly setting content-length
	}
	glog.Infof("%s: forwarding '%s:%s' to the primary %s", p.si, msg.Action, s, smap.ProxySI)
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
		cmn.Assert(err == nil, err)
		rproxy = httputil.NewSingleHostReverseProxy(uparsed)
		rproxy.Transport = p.createTransport(targetMaxIdleConnsPer, 0)
		p.rproxy.tmap[tsi.DaemonID] = rproxy
	}
	p.rproxy.Unlock()
	rproxy.ServeHTTP(w, r)
}

func (p *proxyrunner) renameLB(bucketFrom, bucketTo string, clone *bucketMD, props *cmn.BucketProps,
	msg *cmn.ActionMsg) bool {
	smap4bcast := p.smapowner.get()

	msg.Value = clone
	jsbytes, err := jsoniter.Marshal(msg)
	cmn.Assert(err == nil, err)

	res := p.broadcastTo(
		cmn.URLPath(cmn.Version, cmn.Buckets, bucketFrom),
		nil, // query
		http.MethodPost,
		jsbytes,
		smap4bcast,
		cmn.GCO.Get().Timeout.Default,
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
	localonly, _ := parsebool(q.Get(cmn.URLParamLocal))
	bucketmd := p.bmdowner.get()
	if localonly {
		bucketnames := &cmn.BucketNames{Cloud: []string{}, Local: make([]string, 0, 64)}
		for bucket := range bucketmd.LBmap {
			bucketnames.Local = append(bucketnames.Local, bucket)
		}
		jsbytes, err := jsoniter.Marshal(bucketnames)
		cmn.Assert(err == nil, err)
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
			path:   cmn.URLPath(cmn.Version, cmn.Buckets, bucketspec),
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
		islocal  = bucketmd.IsLocal(bucket)
	)
	redirect = to + r.URL.Path + "?"
	if r.URL.RawQuery != "" {
		redirect += r.URL.RawQuery + "&"
	}

	query.Add(cmn.URLParamLocal, strconv.FormatBool(islocal))
	query.Add(cmn.URLParamProxyID, p.si.DaemonID)
	query.Add(cmn.URLParamBMDVersion, bucketmd.vstr)
	query.Add(cmn.URLParamUnixTime, strconv.FormatInt(int64(ts.UnixNano()), 10))
	redirect += query.Encode()
	return
}

// For cached = false goes to the Cloud, otherwise returns locally cached files
func (p *proxyrunner) targetListBucket(r *http.Request, bucket string, dinfo *cluster.Snode,
	getMsg *cmn.GetMsg, islocal bool, cached bool) (*bucketResp, error) {
	actionMsgBytes, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActListObjects, Value: getMsg})
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
	query.Add(cmn.URLParamLocal, strconv.FormatBool(islocal))
	query.Add(cmn.URLParamCached, strconv.FormatBool(cached))
	query.Add(cmn.URLParamBMDVersion, p.bmdowner.get().vstr)

	args := callArgs{
		si: dinfo,
		req: reqArgs{
			method: http.MethodPost,
			header: header,
			path:   cmn.URLPath(cmn.Version, cmn.Buckets, bucket),
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
func (p *proxyrunner) consumeCachedList(bmap map[string]*cmn.BucketEntry, dataCh chan *localFilePage, errCh chan error) {
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
				entry.IsCached = newEntry.Status == cmn.ObjStatusOK
			}
		}
	}
}

// Request list of all cached files from a target.
// The target returns its list in batches `pageSize` length
func (p *proxyrunner) generateCachedList(bucket string, daemon *cluster.Snode, dataCh chan *localFilePage, origmsg *cmn.GetMsg) {
	var msg cmn.GetMsg
	cmn.CopyStruct(&msg, origmsg)
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
			glog.Errorf("Failed to get information about cached objects on target %s: %v", daemon, err)
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
			glog.Errorf("Failed to unmarshall cached objects list from target %s: %v", daemon, err)
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
func (p *proxyrunner) collectCachedFileList(bucket string, fileList *cmn.BucketList, getmsgjson []byte) (err error) {
	reqParams := &cmn.GetMsg{}
	err = jsoniter.Unmarshal(getmsgjson, reqParams)
	if err != nil {
		return
	}

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

func (p *proxyrunner) getLocalBucketObjects(bucket string, listmsgjson []byte) (allentries *cmn.BucketList, err error) {
	type targetReply struct {
		resp *bucketResp
		err  error
	}
	const (
		islocal    = true
		cachedObjs = false
	)
	msg := &cmn.GetMsg{}
	if err = jsoniter.Unmarshal(listmsgjson, msg); err != nil {
		return
	}
	pageSize := cmn.DefaultPageSize
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
	allentries = &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, pageSize)}
	for r := range chresult {
		if r.err != nil {
			err = r.err
			return
		}

		if r.resp.outjson == nil || len(r.resp.outjson) == 0 {
			continue
		}

		bucketList := &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, pageSize)}
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

func (p *proxyrunner) getCloudBucketObjects(r *http.Request, bucket string, listmsgjson []byte) (allentries *cmn.BucketList, err error) {
	const (
		islocal       = false
		cachedObjects = false
	)
	var resp *bucketResp
	allentries = &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, initialBucketListSize)}
	msg := cmn.GetMsg{}
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
	if strings.Contains(msg.GetProps, cmn.GetTargetURL) {
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
	if strings.Contains(msg.GetProps, cmn.GetPropsAtime) ||
		strings.Contains(msg.GetProps, cmn.GetPropsStatus) ||
		strings.Contains(msg.GetProps, cmn.GetPropsIsCached) {
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
func (p *proxyrunner) listbucket(w http.ResponseWriter, r *http.Request, bucket string, actionMsg *cmn.ActionMsg) (pagemarker string, ok bool) {
	var allentries *cmn.BucketList
	listmsgjson, err := jsoniter.Marshal(actionMsg.Value)
	if err != nil {
		s := fmt.Sprintf("Unable to marshal action message: %v. Error: %v", actionMsg, err)
		p.invalmsghdlr(w, r, s)
		return
	}

	if p.bmdowner.get().IsLocal(bucket) {
		allentries, err = p.getLocalBucketObjects(bucket, listmsgjson)
	} else {
		allentries, err = p.getCloudBucketObjects(r, bucket, listmsgjson)
	}
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	jsbytes, err := jsoniter.Marshal(allentries)
	cmn.Assert(err == nil, err)
	ok = p.writeJSON(w, r, jsbytes, "listbucket")
	pagemarker = allentries.PageMarker
	return
}

func (p *proxyrunner) savebmdconf(bucketmd *bucketMD) (errstr string) {
	bucketmdfull := filepath.Join(cmn.GCO.Get().Confdir, cmn.BucketmdBackupFile)
	if err := cmn.LocalSave(bucketmdfull, bucketmd); err != nil {
		errstr = fmt.Sprintf("Failed to store bucket-metadata at %s, err: %v", bucketmdfull, err)
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
	//       original JSON payload (GetMsg - see pkg/api/constant.go)
	redirecturl := p.redirectURL(r, si.PublicNet.DirectURL, started, lbucket)
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)

	p.statsif.Add(stats.RenameCount, 1)
}

func (p *proxyrunner) replicate(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg) {
	p.invalmsghdlr(w, r, "not supported yet") // see also: daemon.go, config.sh, and tests/replication
}

func (p *proxyrunner) doListRange(w http.ResponseWriter, r *http.Request, actionMsg *cmn.ActionMsg, method string) {
	var (
		err error
	)

	apitems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	islocal := p.bmdowner.get().IsLocal(bucket)
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
	cmn.Assert(err == nil, err)

	var (
		q       = url.Values{}
		results chan callResult
		timeout time.Duration
	)

	q.Set(cmn.URLParamLocal, strconv.FormatBool(islocal))
	if wait {
		timeout = longTimeout
	} else {
		timeout = defaultTimeout
	}

	smap := p.smapowner.get()
	results = p.broadcastTo(
		cmn.URLPath(cmn.Version, cmn.Buckets, bucket),
		q,
		method,
		jsonbytes,
		smap,
		timeout,
		cmn.NetworkIntraData,
		cluster.Targets,
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
	if _, err := p.checkRESTItems(w, r, 0, false, cmn.Version, cmn.Tokens); err != nil {
		return
	}

	msg := &cmn.ActionMsg{Action: cmn.ActRevokeToken}
	if p.forwardCP(w, r, msg, "revoke token", nil) {
		return
	}

	if err := cmn.ReadJSON(w, r, tokenList); err != nil {
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
	switch getWhat {
	case cmn.GetWhatConfig, cmn.GetWhatBucketMeta, cmn.GetWhatSmapVote, cmn.GetWhatDaemonInfo:
		p.httprunner.httpdaeget(w, r)
	case cmn.GetWhatStats:
		rst := getproxystatsrunner()
		jsbytes, err := rst.GetWhatStats()
		cmn.Assert(err == nil, err)
		p.writeJSON(w, r, jsbytes, "httpdaeget-"+getWhat)
	case cmn.GetWhatSmap:
		smap := p.smapowner.get()
		for smap == nil || !smap.isValid() {
			if p.startedup(0) != 0 { // must be starting up
				smap = p.smapowner.get()
				cmn.Assert(smap.isValid())
				break
			}
			glog.Errorf("%s is starting up: cannot execute GET %s yet...", p.si, cmn.GetWhatSmap)
			time.Sleep(time.Second)
			smap = p.smapowner.get()
		}
		jsbytes, err := jsoniter.Marshal(smap)
		cmn.Assert(err == nil, err)
		p.writeJSON(w, r, jsbytes, "httpdaeget-"+getWhat)
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
				glog.Errorln(s)
				p.invalmsghdlr(w, r, s)
				return
			}
			if !newsmap.isPresent(p.si, true) {
				s := fmt.Sprintf("Not finding self %s in the %s", p.si, newsmap.pp())
				glog.Errorln(s)
				p.invalmsghdlr(w, r, s)
				return
			}
			if s := p.smapowner.synchronize(newsmap, true /*saveSmap*/, true /* lesserIsErr */); s != "" {
				p.invalmsghdlr(w, r, s)
			}
			glog.Infof("%s: %s v%d done", p.si, cmn.SyncSmap, newsmap.version())
			return
		default:
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
	case cmn.ActSetConfig:
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

	return smap, ""
}

// forceful primary change - is used when the original primary network is down
// for a while and the remained nodes selected a new primary. After the
// original primary is back it does not attach automatically to the new primary
// and the cluster gets into split-brain mode. This request makes original
// primary connect to the new primary
func (p *proxyrunner) forcefulJoin(w http.ResponseWriter, r *http.Request, proxyID string) {
	newPrimaryURL := r.URL.Query().Get(cmn.URLParamPrimaryCandidate)
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

	apitems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Daemon)
	if err != nil {
		return
	}

	proxyID := apitems[1]
	force, _ := parsebool(r.URL.Query().Get(cmn.URLParamForce))
	// forceful primary change
	if force && apitems[0] == cmn.Proxy {
		if !p.smapowner.get().isPrimary(p.si) {
			s := fmt.Sprintf("Proxy %s is not a primary", p.si)
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
	if !smap.isPresent(p.si, true) {
		cmn.Assert(false, "This proxy '"+p.si.DaemonID+"' must always be present in the local "+smap.pp())
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

	msg := &cmn.ActionMsg{Action: cmn.ActNewPrimary}
	bucketmd := p.bmdowner.get()
	if glog.V(3) {
		glog.Infof("Distributing Smap v%d with the newly elected primary %s = self", clone.version(), p.si)
		glog.Infof("Distributing bucket-metadata v%d as well", bucketmd.version())
	}
	p.metasyncer.sync(true, clone, msg, bucketmd, msg)
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

// handler for DFC when utilized as a reverse proxy to handle unmodified requests
// (not to confuse with p.rproxy)
func (p *proxyrunner) reverseProxyHandler(w http.ResponseWriter, r *http.Request) {
	baseURL := r.URL.Scheme + "://" + r.URL.Host
	if baseURL == gcsURL && r.Method == http.MethodGet {
		s := cmn.RESTItems(r.URL.Path)
		if len(s) == 2 {
			r.URL.Path = cmn.URLPath(cmn.Version, cmn.Objects) + r.URL.Path
			p.httpobjget(w, r)
			return
		} else if len(s) == 1 {
			r.URL.Path = cmn.URLPath(cmn.Version, cmn.Buckets) + r.URL.Path
			p.httpbckget(w, r)
			return
		}
	}
	p.rproxy.cloud.ServeHTTP(w, r)
}

// gets target info
func (p *proxyrunner) httpcluget(w http.ResponseWriter, r *http.Request) {
	getWhat := r.URL.Query().Get(cmn.URLParamWhat)
	switch getWhat {
	case cmn.GetWhatStats:
		ok := p.invokeHTTPGetClusterStats(w, r)
		if !ok {
			return
		}
	case cmn.GetWhatXaction:
		ok := p.invokeHTTPGetXaction(w, r)
		if !ok {
			return
		}
	case cmn.GetWhatMountpaths:
		if ok := p.invokeHTTPGetClusterMountpaths(w, r); !ok {
			return
		}
	default:
		s := fmt.Sprintf("Unexpected GET request, invalid param 'what': [%s]", getWhat)
		cmn.InvalidHandlerWithMsg(w, r, s)
	}
}

func (p *proxyrunner) invokeHTTPGetXaction(w http.ResponseWriter, r *http.Request) bool {
	kind := r.URL.Query().Get(cmn.URLParamProps)
	if errstr := validateXactionQueryable(kind); errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return false
	}
	outputXactionStats := &stats.XactionStats{}
	outputXactionStats.Kind = kind
	targetStats, ok := p.invokeHTTPGetMsgOnTargets(w, r)
	if !ok {
		e := fmt.Sprintf(
			"Unable to invoke cmn.GetMsg on targets. Query: [%s]",
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

func (p *proxyrunner) invokeHTTPGetMsgOnTargets(w http.ResponseWriter, r *http.Request) (map[string]jsoniter.RawMessage, bool) {
	results := p.broadcastTo(
		cmn.URLPath(cmn.Version, cmn.Daemon),
		r.URL.Query(),
		r.Method,
		nil, // message
		p.smapowner.get(),
		cmn.GCO.Get().Timeout.Default,
		cmn.NetworkIntraControl,
		cluster.Targets,
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
func (p *proxyrunner) invokeHTTPGetClusterStats(w http.ResponseWriter, r *http.Request) bool {
	targetStats, ok := p.invokeHTTPGetMsgOnTargets(w, r)
	if !ok {
		errstr := fmt.Sprintf(
			"Unable to invoke cmn.GetMsg on targets. Query: [%s]",
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
	cmn.Assert(err == nil, err)
	ok = p.writeJSON(w, r, jsbytes, "HttpGetClusterStats")
	return ok
}

func (p *proxyrunner) invokeHTTPGetClusterMountpaths(w http.ResponseWriter, r *http.Request) bool {
	targetMountpaths, ok := p.invokeHTTPGetMsgOnTargets(w, r)
	if !ok {
		errstr := fmt.Sprintf(
			"Unable to invoke cmn.GetMsg on targets. Query: [%s]", r.URL.RawQuery)
		glog.Errorf(errstr)
		p.invalmsghdlr(w, r, errstr)
		return false
	}

	out := &ClusterMountpathsRaw{}
	out.Targets = targetMountpaths
	jsbytes, err := jsoniter.Marshal(out)
	cmn.Assert(err == nil, err)
	ok = p.writeJSON(w, r, jsbytes, "HttpGetClusterMountpaths")
	return ok
}

// register|keepalive target|proxy
func (p *proxyrunner) httpclupost(w http.ResponseWriter, r *http.Request) {
	var (
		nsi                   cluster.Snode
		keepalive, register   bool
		isproxy, nonelectable bool
		msg                   *cmn.ActionMsg
		s                     string
	)
	apitems, err := p.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Cluster)
	if err != nil {
		return
	}
	if cmn.ReadJSON(w, r, &nsi) != nil {
		return
	}
	if len(apitems) > 0 {
		keepalive = apitems[0] == cmn.Keepalive
		register = apitems[0] == cmn.Register
		isproxy = apitems[0] == cmn.Proxy
		if isproxy {
			if len(apitems) > 1 {
				keepalive = apitems[1] == cmn.Keepalive
			}
			s := r.URL.Query().Get(cmn.URLParamNonElectable)
			var err error
			if nonelectable, err = parsebool(s); s != "" && err != nil {
				glog.Errorf("Failed to parse %s for non-electability: %v", s, err)
			}
		}
	}
	s = fmt.Sprintf("register %s (isproxy=%t, keepalive=%t)", &nsi, isproxy, keepalive)
	msg = &cmn.ActionMsg{Action: cmn.ActRegTarget}
	if isproxy {
		msg = &cmn.ActionMsg{Action: cmn.ActRegProxy}
	}
	body, err := jsoniter.Marshal(nsi)
	cmn.Assert(err == nil, err)
	if p.forwardCP(w, r, msg, s, body) {
		return
	}
	if net.ParseIP(nsi.PublicNet.NodeIPAddr) == nil {
		s := fmt.Sprintf("register target %s: invalid IP address %v", &nsi, nsi.PublicNet.NodeIPAddr)
		p.invalmsghdlr(w, r, s)
		return
	}

	p.statsif.Add(stats.PostCount, 1)

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
				glog.Infof("register target %s (num targets before %d)", &nsi, smap.CountTargets())
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
				errstr := fmt.Sprintf("Failed to register target %s: %v, %s", &nsi, res.err, res.errstr)
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
		cmn.Assert(err == nil, err)
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
				clone.NonElects = make(cmn.SimpleKVs)
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
			glog.Warningf("register/keepalive %s %s: adding back to the cluster map", kind, nsi)
			return true
		}

		if !osi.Equals(nsi) {
			if p.detectDaemonDuplicate(osi, nsi) {
				glog.Errorf("%s tried to register/keepalive with a duplicate ID %s", nsi.PublicNet.DirectURL, nsi)
				return false
			}
			glog.Warningf("register/keepalive %s %s: info changed - renewing", kind, nsi)
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
			glog.Infof("register %s %s: already done", kind, nsi)
			return false
		}
		glog.Warningf("register %s %s: renewing the registration %+v => %+v", kind, nsi, osi, nsi)
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
				path:   cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Unregister),
			},
			timeout: cmn.GCO.Get().Timeout.CplaneOperation,
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
	var msg cmn.ActionMsg
	apitems, err := p.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Cluster)
	if err != nil {
		return
	}
	// cluster-wide: designate a new primary proxy administratively
	if len(apitems) > 0 && apitems[0] == cmn.Proxy {
		p.httpclusetprimaryproxy(w, r)
		return
	}
	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}
	if p.forwardCP(w, r, &msg, "", nil) {
		return
	}

	switch msg.Action {
	case cmn.ActSetConfig:
		if value, ok := msg.Value.(string); !ok {
			p.invalmsghdlr(w, r, fmt.Sprintf("Invalid Value format (%+v, %T)", msg.Value, msg.Value))
		} else if errstr := p.setconfig(msg.Name, value); errstr != "" {
			p.invalmsghdlr(w, r, errstr)
		} else {
			glog.Infof("setconfig %s=%s", msg.Name, value)
			msgbytes, err := jsoniter.Marshal(msg) // same message -> all targets and proxies
			cmn.Assert(err == nil, err)

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
						fmt.Sprintf("%s (%s = %s) failed, err: %s", msg.Action, msg.Name, value, result.errstr),
					)
					p.keepalive.onerr(err, result.status)
				}
			}
		}
	case cmn.ActShutdown:
		glog.Infoln("Proxy-controlled cluster shutdown...")
		msgbytes, err := jsoniter.Marshal(msg) // same message -> all targets
		cmn.Assert(err == nil, err)

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
		p.metasyncer.sync(false, p.smapowner.get(), &msg)

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
func (p *proxyrunner) receiveBucketMD(newbucketmd *bucketMD, msg *cmn.ActionMsg) (errstr string) {
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

func (p *proxyrunner) validateBucketProps(props *cmn.BucketProps, isLocal bool) error {
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
	if props.ReadPolicy != "" && props.ReadPolicy != cmn.RWPolicyCloud && props.ReadPolicy != cmn.RWPolicyNextTier {
		return fmt.Errorf("invalid read policy: %s", props.ReadPolicy)
	}
	if props.ReadPolicy == cmn.RWPolicyCloud && isLocal {
		return fmt.Errorf("read policy for local bucket cannot be '%s'", cmn.RWPolicyCloud)
	}
	if props.WritePolicy != "" && props.WritePolicy != cmn.RWPolicyCloud && props.WritePolicy != cmn.RWPolicyNextTier {
		return fmt.Errorf("invalid write policy: %s", props.WritePolicy)
	}
	if props.WritePolicy == cmn.RWPolicyCloud && isLocal {
		return fmt.Errorf("write policy for local bucket cannot be '%s'", cmn.RWPolicyCloud)
	}
	if props.NextTierURL != "" {
		if props.CloudProvider == "" {
			return fmt.Errorf("tiered bucket must use one of the supported cloud providers (%s | %s | %s)",
				cmn.ProviderAmazon, cmn.ProviderGoogle, cmn.ProviderDFC)
		}
		if props.ReadPolicy == "" {
			props.ReadPolicy = cmn.RWPolicyNextTier
		}
		if props.WritePolicy == "" && !isLocal {
			props.WritePolicy = cmn.RWPolicyCloud
		} else if props.WritePolicy == "" && isLocal {
			props.WritePolicy = cmn.RWPolicyNextTier
		}
	}
	if props.Checksum != cmn.ChecksumInherit &&
		props.Checksum != cmn.ChecksumNone && props.Checksum != cmn.ChecksumXXHash {
		return fmt.Errorf("invalid checksum: %s - expecting %s or %s or %s",
			props.Checksum, cmn.ChecksumXXHash, cmn.ChecksumNone, cmn.ChecksumInherit)
	}

	lwm, hwm := props.LowWM, props.HighWM
	if lwm < 0 || hwm < 0 || lwm > 100 || hwm > 100 || lwm > hwm {
		return fmt.Errorf("Invalid WM configuration. LowWM: %d, HighWM: %d, LowWM and HighWM must be in the range [0, 100] with LowWM <= HighWM", lwm, hwm)
	}
	if props.DontEvictTimeStr != "" {
		dontEvictTime, err := time.ParseDuration(props.DontEvictTimeStr)
		if err != nil {
			return fmt.Errorf("Bad dont_evict_time format %s, err: %v", dontEvictTime, err)
		}
		props.DontEvictTime = dontEvictTime

	}
	if props.CapacityUpdTimeStr != "" {
		capacityUpdTime, err := time.ParseDuration(props.CapacityUpdTimeStr)
		if err != nil {
			return fmt.Errorf("Bad capacity_upd_time format %s, err: %v", capacityUpdTime, err)
		}
		props.CapacityUpdTime = capacityUpdTime
	}
	return nil
}

func rechecksumRequired(globalChecksum string, bucketChecksumOld string, bucketChecksumNew string) bool {
	checksumOld := globalChecksum
	if bucketChecksumOld != cmn.ChecksumInherit {
		checksumOld = bucketChecksumOld
	}
	checksumNew := globalChecksum
	if bucketChecksumNew != cmn.ChecksumInherit {
		checksumNew = bucketChecksumNew
	}
	return checksumNew != cmn.ChecksumNone && checksumNew != checksumOld
}

func (p *proxyrunner) notifyTargetsRechecksum(bucket string) {
	jsbytes, err := jsoniter.Marshal(cmn.ActionMsg{Action: cmn.ActRechecksum})
	cmn.Assert(err == nil, err)

	res := p.broadcastTo(
		cmn.URLPath(cmn.Version, cmn.Buckets, bucket),
		nil,
		http.MethodPost,
		jsbytes,
		p.smapowner.get(),
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
	query.Add(cmn.URLParamWhat, cmn.GetWhatDaemonInfo)
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
	if err := jsoniter.Unmarshal(res.outjson, si); err != nil {
		cmn.Assert(false, err)
	}
	return !nsi.Equals(si)
}

func validateCloudProvider(provider string, isLocal bool) error {
	if provider != "" && provider != cmn.ProviderAmazon && provider != cmn.ProviderGoogle && provider != cmn.ProviderDFC {
		return fmt.Errorf("invalid cloud provider: %s, must be one of (%s | %s | %s)", provider,
			cmn.ProviderAmazon, cmn.ProviderGoogle, cmn.ProviderDFC)
	} else if isLocal && provider != cmn.ProviderDFC && provider != "" {
		return fmt.Errorf("local bucket can only have '%s' as the cloud provider", cmn.ProviderDFC)
	}
	return nil
}

func (p *proxyrunner) copyBucketProps(oldProps, newProps *cmn.BucketProps, bucket string) {
	oldProps.NextTierURL = newProps.NextTierURL
	oldProps.CloudProvider = newProps.CloudProvider
	if newProps.ReadPolicy != "" {
		oldProps.ReadPolicy = newProps.ReadPolicy
	}
	if newProps.WritePolicy != "" {
		oldProps.WritePolicy = newProps.WritePolicy
	}
	if rechecksumRequired(cmn.GCO.Get().Cksum.Checksum, oldProps.Checksum, newProps.Checksum) {
		go p.notifyTargetsRechecksum(bucket)
	}
	if newProps.Checksum != "" {
		oldProps.Checksum = newProps.Checksum
		if newProps.Checksum != cmn.ChecksumInherit {
			oldProps.ValidateColdGet = newProps.ValidateColdGet
			oldProps.ValidateWarmGet = newProps.ValidateWarmGet
			oldProps.EnableReadRangeChecksum = newProps.EnableReadRangeChecksum
		}
	}
	oldProps.LowWM = newProps.LowWM // can't conditionally assign if value != 0 since 0 is valid
	oldProps.HighWM = newProps.HighWM
	oldProps.AtimeCacheMax = newProps.AtimeCacheMax
	if newProps.DontEvictTimeStr != "" {
		oldProps.DontEvictTimeStr = newProps.DontEvictTimeStr
		oldProps.DontEvictTime = newProps.DontEvictTime // parsing done in validateBucketProps()
	}
	if newProps.CapacityUpdTimeStr != "" {
		oldProps.CapacityUpdTimeStr = newProps.CapacityUpdTimeStr
		oldProps.CapacityUpdTime = newProps.CapacityUpdTime // parsing done in validateBucketProps()
	}
	oldProps.LRUEnabled = newProps.LRUEnabled
}
