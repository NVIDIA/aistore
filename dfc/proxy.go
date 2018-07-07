/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/dfc/statsd"
)

const tokenStart = "Bearer"

// Keeps a target response when doing parallel requests to all targets
type bucketResp struct {
	outjson []byte
	err     error
	id      string
}

// A list of target local files: cached or local bucket
// Maximum number of files in the response is `pageSize` entries
type localFilePage struct {
	entries []*BucketEntry
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
	starttime   time.Time
	smapversion int64
	xactinp     *xactInProgress
	syncmapinp  int64
	statsdC     statsd.Client
	authn       *authManager
	startedUp   int64
	metasyncer  *metasyncer
}

// start proxy runner
func (p *proxyrunner) run() error {
	// note: call stats worker has to started before the first call()
	p.callStatsServer = NewCallStatsServer(
		ctx.config.CallStats.RequestIncluded,
		ctx.config.CallStats.Factor,
		&p.statsdC,
	)
	p.callStatsServer.Start()

	p.httprunner.init(getproxystatsrunner(), true)
	p.httprunner.kalive = getproxykalive()

	p.xactinp = newxactinp()

	bucketmdfull := filepath.Join(ctx.config.Confdir, bucketmdbase)
	bucketmd := newBucketMD()
	if LocalLoad(bucketmdfull, bucketmd) != nil {
		// create empty
		bucketmd.Version = 1
		if err := LocalSave(bucketmdfull, bucketmd); err != nil {
			glog.Fatalf("FATAL: cannot store bucket-metadata, err: %v", err)
		}
	}
	p.bmdowner.put(bucketmd)

	p.metasyncer = getmetasyncer()

	// startup sequence - see earlystart.go for the steps and commentary
	p.bootstrap()

	p.authn = &authManager{
		tokens:        make(map[string]*authRec),
		revokedTokens: make(map[string]bool),
	}

	//
	// REST API: register proxy handlers and start listening
	//
	if ctx.config.Auth.Enabled {
		p.httprunner.registerhdlr(URLPath(Rversion, Rbuckets)+"/", wrapHandler(p.bucketHandler, p.checkHTTPAuth))
		p.httprunner.registerhdlr(URLPath(Rversion, Robjects)+"/", wrapHandler(p.objectHandler, p.checkHTTPAuth))
	} else {
		p.httprunner.registerhdlr(URLPath(Rversion, Rbuckets)+"/", p.bucketHandler)
		p.httprunner.registerhdlr(URLPath(Rversion, Robjects)+"/", p.objectHandler)
	}

	p.httprunner.registerhdlr(URLPath(Rversion, Rdaemon), p.daemonHandler)
	p.httprunner.registerhdlr(URLPath(Rversion, Rcluster), p.clusterHandler)
	p.httprunner.registerhdlr(URLPath(Rversion, Rhealth), p.httpHealth)
	p.httprunner.registerhdlr(URLPath(Rversion, Rvote)+"/", p.voteHandler)
	p.httprunner.registerhdlr(URLPath(Rversion, Rtokens), p.tokenHandler)

	if ctx.config.Net.HTTP.UseAsProxy {
		p.httprunner.registerhdlr("/", p.reverseProxyHandler)
	} else {
		p.httprunner.registerhdlr("/", invalhdlr)
	}

	glog.Infof("%s: listening on :%s", p.si.DaemonID, p.si.DaemonPort)
	p.starttime = time.Now()

	// Note: hard coding statsd's IP and port for two reasons:
	// 1. it is well known, conflicts are unlikely, less config is better
	// 2. if do need configuable, will make a separate change, easier to manage
	// Potentially there is a race here, &p.statsdC is given to call stats tracker already
	var err error
	p.statsdC, err = statsd.New("localhost", 8125,
		fmt.Sprintf("dfcproxy.%s", strings.Replace(p.si.DaemonID, ":", "_", -1)))
	if err != nil {
		glog.Info("Failed to connect to statd, running without statsd")
	}

	return p.httprunner.run()
}

func (p *proxyrunner) register(timeout time.Duration) (status int, err error) {
	var (
		res  callResult
		smap = p.smapowner.get()
	)
	if smap != nil && smap.isPrimary(p.si) {
		return
	}
	if timeout == 0 {
		if ctx.config.Proxy.NonElectable {
			res = p.join(true, URLParamNonElectable+"=true")
		} else {
			res = p.join(true, "")
		}
	} else { // keepalive
		url, psi := p.getPrimaryURLAndSI()
		res = p.registerToURL(url, psi, timeout, true, "")
	}
	return res.status, res.err
}

func (p *proxyrunner) unregister() (int, error) {
	url := fmt.Sprintf("%s/%s/%s/%s/%s/%s", ctx.config.Proxy.PrimaryURL, Rversion, Rcluster, Rdaemon, Rproxy, p.si.DaemonID)
	res := p.call(nil, nil, url, http.MethodDelete, nil)
	return res.status, res.err
}

// stop gracefully
func (p *proxyrunner) stop(err error) {
	var isPrimary bool
	smap := p.smapowner.get()
	if smap != nil { // in tests
		isPrimary = smap.isPrimary(p.si)
	}
	glog.Infof("Stopping %s (ID %s, primary=%t), err: %v", p.name, p.si.DaemonID, isPrimary, err)
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

	if p.httprunner.h != nil && !isPrimary {
		_, unregerr := p.unregister()
		if unregerr != nil {
			glog.Warningf("Failed to unregister when terminating: %v", unregerr)
		}
	}

	p.statsdC.Close()
	p.httprunner.stop(err)
	p.callStatsServer.Stop()
}

//===========================================================================================
//
// http handlers: data and metadata
//
//===========================================================================================

// verb /Rversion/Rbuckets/
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
		invalhdlr(w, r)
	}
}

// verb /Rversion/Robjects/
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
		invalhdlr(w, r)
	}
}

// GET /v1/buckets/bucket-name
func (p *proxyrunner) httpbckget(w http.ResponseWriter, r *http.Request) {
	if p.smapowner.get().countTargets() < 1 {
		p.invalmsghdlr(w, r, "No registered targets yet")
		return
	}
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rbuckets); apitems == nil {
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
	if p.smapowner.get().countTargets() < 1 {
		p.invalmsghdlr(w, r, "No registered targets yet")
		return
	}
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 2, Rversion, Robjects); apitems == nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	if !p.validatebckname(w, r, bucket) {
		return
	}

	si, errstr := HrwTarget(bucket, objname, p.smapowner.get())
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}
	var redirecturl string
	islocal := p.bmdowner.get().islocal(bucket)
	if r.URL.RawQuery != "" {
		redirecturl = fmt.Sprintf("%s%s?%s&%s=%t", si.DirectURL, r.URL.Path, r.URL.RawQuery, URLParamLocal, islocal)
	} else {
		redirecturl = fmt.Sprintf("%s%s?%s=%t", si.DirectURL, r.URL.Path, URLParamLocal, islocal)
	}
	if glog.V(4) {
		glog.Infof("%s %s/%s => %s", r.Method, bucket, objname, si.DaemonID)
	}
	if ctx.config.Net.HTTP.UseAsProxy {
		req, err := http.NewRequest(http.MethodGet, redirecturl, r.Body)
		if err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		p.revProxy.ServeHTTP(w, req)
	} else {
		http.Redirect(w, r, redirecturl, http.StatusMovedPermanently)
	}

	// Note: ideally, would prefer to call statsd in statsif.add(), but it doesn't have type support,
	//       the name schema is different(statd doesn't need dup 'numget', it is get.count), statsif's dependency
	//       on the names, and (not that importantly) the unit is different (ms vs us).
	delta := time.Since(started)
	p.statsdC.Send("get",
		statsd.Metric{
			Type:  statsd.Counter,
			Name:  "count",
			Value: 1,
		},
		statsd.Metric{
			Type:  statsd.Timer,
			Name:  "latency",
			Value: float64(delta / time.Millisecond),
		},
	)

	p.statsif.addMany("numget", int64(1), "getlatency", int64(delta/1000))
}

// PUT "/"+Rversion+"/"+Robjects
func (p *proxyrunner) httpobjput(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Robjects); apitems == nil {
		return
	}
	bucket := apitems[0]
	//
	// FIXME: add protection against putting into non-existing local bucket
	//
	objname := strings.Join(apitems[1:], "/")
	si, errstr := HrwTarget(bucket, objname, p.smapowner.get())
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}
	redirecturl := fmt.Sprintf("%s%s?%s=%t&%s=%s", si.DirectURL, r.URL.Path, URLParamLocal,
		p.bmdowner.get().islocal(bucket), URLParamDaemonID, p.httprunner.si.DaemonID)
	if glog.V(4) {
		glog.Infof("%s %s/%s => %s", r.Method, bucket, objname, si.DaemonID)
	}
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)

	delta := time.Since(started)
	p.statsdC.Send("put",
		statsd.Metric{
			Type:  statsd.Counter,
			Name:  "count",
			Value: 1,
		},
		statsd.Metric{
			Type:  statsd.Timer,
			Name:  "latency",
			Value: float64(delta / time.Millisecond),
		},
	)

	p.statsif.addMany("numput", int64(1), "putlatency", int64(delta/1000))
}

// DELETE { action } /Rversion/Rbuckets
func (p *proxyrunner) httpbckdelete(w http.ResponseWriter, r *http.Request) {
	var msg ActionMsg
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rbuckets); apitems == nil {
		return
	}
	bucket := apitems[0]
	if err := p.readJSON(w, r, &msg); err != nil {
		return
	}
	switch msg.Action {
	case ActDestroyLB:
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
		pair := &revspair{clone, &msg}
		p.metasyncer.sync(true, pair)
	case ActDelete, ActEvict:
		p.actionlistrange(w, r, &msg)
	default:
		p.invalmsghdlr(w, r, fmt.Sprintf("Unsupported Action: %s", msg.Action))
	}
}

// DELETE /Rversion/Robjects/object-name
func (p *proxyrunner) httpobjdelete(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 2, Rversion, Robjects); apitems == nil {
		return
	}
	bucket := apitems[0]
	objname := strings.Join(apitems[1:], "/")
	si, errstr := HrwTarget(bucket, objname, p.smapowner.get())
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}
	redirecturl := si.DirectURL + r.URL.Path
	if glog.V(4) {
		glog.Infof("%s %s/%s => %s", r.Method, bucket, objname, si.DaemonID)
	}

	p.statsdC.Send("delete",
		statsd.Metric{
			Type:  statsd.Counter,
			Name:  "count",
			Value: 1,
		},
	)

	p.statsif.add("numdelete", 1)
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)
}

// GET /Rversion/Rhealth
func (p *proxyrunner) httpHealth(w http.ResponseWriter, r *http.Request) {
	proxycorestats := getproxystats()
	jsbytes, err := json.Marshal(proxycorestats)
	assert(err == nil, err)
	p.writeJSON(w, r, jsbytes, "targetcorestats")
}

// POST { action } /v1/buckets/bucket-name
func (p *proxyrunner) httpbckpost(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	var msg ActionMsg
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rbuckets); apitems == nil {
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
	case ActCreateLB:
		if !p.checkPrimaryProxy("create local bucket", w, r) {
			return
		}
		p.bmdowner.Lock()
		clone := p.bmdowner.get().clone()
		if !clone.add(lbucket, true, BucketProps{}) {
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
		pair := &revspair{clone, &msg}
		p.metasyncer.sync(true, pair)
	case ActRenameLB:
		if !p.checkPrimaryProxy("rename local bucket", w, r) {
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
	case ActSyncLB:
		if !p.checkPrimaryProxy("synchronize local buckets", w, r) {
			return
		}
		p.metasyncer.sync(false, p.bmdowner.get())
	case ActPrefetch:
		p.actionlistrange(w, r, &msg)
	case ActListObjects:
		p.listBucketAndCollectStats(w, r, lbucket, msg, started)
	default:
		s := fmt.Sprintf("Unexpected ActionMsg <- JSON [%v]", msg)
		p.invalmsghdlr(w, r, s)
	}
}

func (p *proxyrunner) listBucketAndCollectStats(w http.ResponseWriter,
	r *http.Request, lbucket string, msg ActionMsg, started time.Time) {
	pagemarker, ok := p.listbucket(w, r, lbucket, &msg)
	if ok {
		delta := time.Since(started)
		p.statsdC.Send("list",
			statsd.Metric{
				Type:  statsd.Counter,
				Name:  "count",
				Value: 1,
			},
			statsd.Metric{
				Type:  statsd.Timer,
				Name:  "latency",
				Value: float64(delta / time.Millisecond),
			},
		)

		lat := int64(delta / 1000)
		p.statsif.addMany("numlist", int64(1), "listlatency", lat)

		if glog.V(3) {
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
	var msg ActionMsg
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Robjects); apitems == nil {
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
	case ActRename:
		p.filrename(w, r, &msg)
		return
	default:
		s := fmt.Sprintf("Unexpected ActionMsg <- JSON [%v]", msg)
		p.invalmsghdlr(w, r, s)
		return
	}
}

// HEAD /v1/buckets/bucket-name
func (p *proxyrunner) httpbckhead(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rbuckets); apitems == nil {
		return
	}
	bucket := apitems[0]
	if !p.validatebckname(w, r, bucket) {
		return
	}
	var si *daemonInfo
	// Use random map iteration order to choose a random target to redirect to
	smap := p.smapowner.get()
	for _, si = range smap.Tmap {
		break
	}
	redirecturl := fmt.Sprintf("%s%s?%s=%t", si.DirectURL, r.URL.Path, URLParamLocal, p.bmdowner.get().islocal(bucket))
	if glog.V(3) {
		glog.Infof("%s %s => %s", r.Method, bucket, si.DaemonID)
	}
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)
}

// PUT /v1/buckets/bucket-name
func (p *proxyrunner) httpbckput(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rbuckets); apitems == nil {
		return
	}

	bucket := apitems[0]
	if !p.validatebckname(w, r, bucket) {
		return
	}
	props := &BucketProps{}
	msg := ActionMsg{Value: props}
	if p.readJSON(w, r, &msg) != nil {
		return
	}
	if msg.Action != ActSetProps {
		s := fmt.Sprintf("Invalid ActionMsg [%v] - expecting '%s' action", msg, ActSetProps)
		p.invalmsghdlr(w, r, s)
		return
	}

	bucketmd := p.bmdowner.get()
	isLocal := bucketmd.islocal(bucket)

	if err := validateBucketProps(props, isLocal); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}

	p.bmdowner.Lock()
	clone := bucketmd.clone()
	exists, oldProps := clone.get(bucket, isLocal)
	if !exists {
		assert(!isLocal)
		clone.add(bucket, false, BucketProps{})
	}
	oldProps.NextTierURL = props.NextTierURL
	oldProps.CloudProvider = props.CloudProvider
	if props.ReadPolicy != "" {
		oldProps.ReadPolicy = props.ReadPolicy
	}
	if props.WritePolicy != "" {
		oldProps.WritePolicy = props.WritePolicy
	}

	clone.set(bucket, isLocal, oldProps)
	if e := p.savebmdconf(clone); e != "" {
		glog.Errorln(e)
	}
	p.bmdowner.put(clone)
	p.bmdowner.Unlock()
	p.metasyncer.sync(true, clone)
}

// HEAD /v1/objects/bucket-name/object-name
func (p *proxyrunner) httpobjhead(w http.ResponseWriter, r *http.Request) {
	checkCached, _ := parsebool(r.URL.Query().Get(URLParamCheckCached))
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 2, Rversion, Robjects); apitems == nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	if !p.validatebckname(w, r, bucket) {
		return
	}
	var si *daemonInfo
	si, errstr := HrwTarget(bucket, objname, p.smapowner.get())
	if errstr != "" {
		return
	}
	redirecturl := fmt.Sprintf("%s%s?%s=%t", si.DirectURL, r.URL.Path, URLParamLocal, p.bmdowner.get().islocal(bucket))
	if checkCached {
		redirecturl += fmt.Sprintf("&%s=true", URLParamCheckCached)
	}
	if glog.V(3) {
		glog.Infof("%s %s/%s => %s", r.Method, bucket, objname, si.DaemonID)
	}
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)
}

//====================================================================================
//
// supporting methods and misc
//
//====================================================================================
func (p *proxyrunner) renamelocalbucket(bucketFrom, bucketTo string, clone *bucketMD, props BucketProps,
	msg *ActionMsg, method string) bool {
	smap4bcast := p.smapowner.get()

	msg.Value = clone
	jsbytes, err := json.Marshal(msg)
	assert(err == nil, err)

	res := p.broadcastTargets(
		URLPath(Rversion, Rbuckets, bucketFrom),
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
	p.metasyncer.sync(true, clone)
	return true
}

func (p *proxyrunner) getbucketnames(w http.ResponseWriter, r *http.Request, bucketspec string) {
	q := r.URL.Query()
	localonly, _ := parsebool(q.Get(URLParamLocal))
	bucketmd := p.bmdowner.get()
	if localonly {
		bucketnames := &BucketNames{Cloud: []string{}, Local: make([]string, 0, 64)}
		for bucket := range bucketmd.LBmap {
			bucketnames.Local = append(bucketnames.Local, bucket)
		}
		jsbytes, err := json.Marshal(bucketnames)
		assert(err == nil, err)
		p.writeJSON(w, r, jsbytes, "getbucketnames?local=true")
		return
	}

	var si *daemonInfo
	smap := p.smapowner.get()
	for _, si = range smap.Tmap {
		break
	}

	u := si.DirectURL + URLPath(Rversion, Rbuckets, bucketspec)
	res := p.call(r, si, u, r.Method, nil)
	if res.err != nil {
		p.invalmsghdlr(w, r, res.errstr)
		p.kalive.onerr(res.err, res.status)
	} else {
		p.writeJSON(w, r, res.outjson, "getbucketnames")
	}
}

// For cached = false goes to the Cloud, otherwise returns locally cached files
func (p *proxyrunner) targetListBucket(r *http.Request, bucket string, dinfo *daemonInfo,
	getMsg *GetMsg, islocal bool, cached bool) (*bucketResp, error) {
	url := fmt.Sprintf("%s/%s/%s/%s?%s=%v&%s=%v", dinfo.DirectURL, Rversion,
		Rbuckets, bucket, URLParamLocal, islocal, URLParamCached, cached)

	actionMsgBytes, err := json.Marshal(ActionMsg{Action: ActListObjects, Value: getMsg})
	if err != nil {
		return &bucketResp{
			outjson: nil,
			err:     err,
			id:      dinfo.DaemonID,
		}, err
	}

	res := p.call(r, dinfo, url, http.MethodPost, actionMsgBytes, ctx.config.Timeout.Default)
	if res.err != nil {
		p.kalive.onerr(res.err, res.status)
	}

	return &bucketResp{
		outjson: res.outjson,
		err:     res.err,
		id:      dinfo.DaemonID,
	}, res.err
}

// Receives info about locally cached files from targets in batches
// and merges with existing list of cloud files
func (p *proxyrunner) consumeCachedList(bmap map[string]*BucketEntry, dataCh chan *localFilePage, errch chan error) {
	for rb := range dataCh {
		if rb.err != nil {
			if errch != nil {
				errch <- rb.err
			}
			glog.Errorf("Failed to get information about file in DFC cache: %v", rb.err)
			return
		}
		if rb.entries == nil || len(rb.entries) == 0 {
			continue
		}

		for _, newEntry := range rb.entries {
			nm := newEntry.Name
			if entry, ok := bmap[nm]; ok {
				entry.IsCached = true
				entry.Atime = newEntry.Atime
			}
		}
	}
}

// Request list of all cached files from a target.
// The target returns its list in batches `pageSize` length
func (p *proxyrunner) generateCachedList(bucket string, daemon *daemonInfo, dataCh chan *localFilePage, origmsg *GetMsg) {
	var msg GetMsg
	copyStruct(&msg, origmsg)
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

		entries := BucketList{Entries: make([]*BucketEntry, 0, 128)}
		if err := json.Unmarshal(resp.outjson, &entries); err != nil {
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
func (p *proxyrunner) collectCachedFileList(bucket string, fileList *BucketList, getmsgjson []byte) (err error) {
	reqParams := &GetMsg{}
	err = json.Unmarshal(getmsgjson, reqParams)
	if err != nil {
		return
	}

	bucketMap := make(map[string]*BucketEntry, initialBucketListSize)
	for _, entry := range fileList.Entries {
		bucketMap[entry.Name] = entry
	}

	smap := p.smapowner.get()
	dataCh := make(chan *localFilePage, smap.countTargets())
	errch := make(chan error, 1)
	wgConsumer := &sync.WaitGroup{}
	wgConsumer.Add(1)
	go func() {
		p.consumeCachedList(bucketMap, dataCh, errch)
		wgConsumer.Done()
	}()

	// since cached file page marker is not compatible with any cloud
	// marker, it should be empty for the first call
	reqParams.GetPageMarker = ""

	wg := &sync.WaitGroup{}
	for _, daemon := range smap.Tmap {
		wg.Add(1)
		// without this copy all goroutines get the same pointer
		go func(d *daemonInfo) {
			p.generateCachedList(bucket, d, dataCh, reqParams)
			wg.Done()
		}(daemon)
	}
	wg.Wait()
	close(dataCh)
	wgConsumer.Wait()

	select {
	case err = <-errch:
		return
	default:
	}

	fileList.Entries = make([]*BucketEntry, 0, len(bucketMap))
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

func (p *proxyrunner) getLocalBucketObjects(bucket string, listmsgjson []byte) (allentries *BucketList, err error) {
	type targetReply struct {
		resp *bucketResp
		err  error
	}
	const (
		islocal    = true
		cachedObjs = false
	)
	msg := &GetMsg{}
	if err = json.Unmarshal(listmsgjson, msg); err != nil {
		return
	}
	pageSize := DefaultPageSize
	if msg.GetPageSize != 0 {
		pageSize = msg.GetPageSize
	}

	if pageSize > MaxPageSize {
		glog.Warningf("Page size(%d) for local bucket %s exceeds the limit(%d)", msg.GetPageSize, bucket, MaxPageSize)
	}

	smap := p.smapowner.get()
	chresult := make(chan *targetReply, len(smap.Tmap))
	wg := &sync.WaitGroup{}

	targetCallFn := func(si *daemonInfo) {
		resp, err := p.targetListBucket(nil, bucket, si, msg, islocal, cachedObjs)
		chresult <- &targetReply{resp, err}
		wg.Done()
	}
	smap = p.smapowner.get()
	for _, si := range smap.Tmap {
		wg.Add(1)
		go func(d *daemonInfo) {
			targetCallFn(d)
		}(si)
	}
	wg.Wait()
	close(chresult)

	// combine results
	allentries = &BucketList{Entries: make([]*BucketEntry, 0, pageSize)}
	for r := range chresult {
		if r.err != nil {
			err = r.err
			return
		}

		if r.resp.outjson == nil || len(r.resp.outjson) == 0 {
			continue
		}

		bucketList := &BucketList{Entries: make([]*BucketEntry, 0, pageSize)}
		if err = json.Unmarshal(r.resp.outjson, &bucketList); err != nil {
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

func (p *proxyrunner) getCloudBucketObjects(r *http.Request, bucket string, listmsgjson []byte) (allentries *BucketList, err error) {
	const (
		islocal       = false
		cachedObjects = false
	)
	var resp *bucketResp
	allentries = &BucketList{Entries: make([]*BucketEntry, 0, initialBucketListSize)}
	msg := GetMsg{}
	err = json.Unmarshal(listmsgjson, &msg)
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
	if err = json.Unmarshal(resp.outjson, &allentries); err != nil {
		return
	}
	if len(allentries.Entries) == 0 {
		return
	}
	if strings.Contains(msg.GetProps, GetTargetURL) {
		for _, e := range allentries.Entries {
			si, errStr := HrwTarget(bucket, e.Name, p.smapowner.get())
			if errStr != "" {
				err = errors.New(errStr)
				return
			}
			e.TargetURL = si.DirectURL
		}
	}
	if strings.Contains(msg.GetProps, GetPropsAtime) ||
		strings.Contains(msg.GetProps, GetPropsIsCached) {
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
func (p *proxyrunner) listbucket(w http.ResponseWriter, r *http.Request, bucket string, actionMsg *ActionMsg) (pagemarker string, ok bool) {
	var allentries *BucketList
	listmsgjson, err := json.Marshal(actionMsg.Value)
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
	jsbytes, err := json.Marshal(allentries)
	assert(err == nil, err)
	ok = p.writeJSON(w, r, jsbytes, "listbucket")
	pagemarker = allentries.PageMarker
	return
}

func (p *proxyrunner) savebmdconf(bucketmd *bucketMD) (errstr string) {
	bucketmdfull := filepath.Join(ctx.config.Confdir, bucketmdbase)
	if err := LocalSave(bucketmdfull, bucketmd); err != nil {
		errstr = fmt.Sprintf("Failed to store bucket-metadata at %s, err: %v", bucketmdfull, err)
	}
	return
}

func (p *proxyrunner) filrename(w http.ResponseWriter, r *http.Request, msg *ActionMsg) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 2, Rversion, Robjects); apitems == nil {
		return
	}
	lbucket, objname := apitems[0], strings.Join(apitems[1:], "/")
	if !p.bmdowner.get().islocal(lbucket) {
		s := fmt.Sprintf("Rename/move is supported only for cache-only buckets (%s does not appear to be local)", lbucket)
		p.invalmsghdlr(w, r, s)
		return
	}

	si, errstr := HrwTarget(lbucket, objname, p.smapowner.get())
	if errstr != "" {
		p.invalmsghdlr(w, r, errstr)
		return
	}
	redirecturl := si.DirectURL + r.URL.Path
	if glog.V(3) {
		glog.Infof("RENAME %s %s/%s => %s", r.Method, lbucket, objname, si.DaemonID)
	}

	p.statsdC.Send("rename",
		statsd.Metric{
			Type:  statsd.Counter,
			Name:  "count",
			Value: 1,
		},
	)

	p.statsif.add("numrename", 1)

	// NOTE:
	//       code 307 is the only way to http-redirect with the
	//       original JSON payload (GetMsg - see REST.go)
	http.Redirect(w, r, redirecturl, http.StatusTemporaryRedirect)
}

func (p *proxyrunner) actionlistrange(w http.ResponseWriter, r *http.Request, actionMsg *ActionMsg) {
	var (
		err    error
		method string
	)

	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 1, Rversion, Rbuckets); apitems == nil {
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
	jsonbytes, err := json.Marshal(actionMsg)
	assert(err == nil, err)

	switch actionMsg.Action {
	case ActEvict, ActDelete:
		method = http.MethodDelete
	case ActPrefetch:
		method = http.MethodPost
	default:
		s := fmt.Sprintf("Action unavailable for List/Range Operations: %s", actionMsg.Action)
		p.invalmsghdlr(w, r, s)
		return
	}

	var (
		q       = url.Values{}
		results chan callResult
		timeOut []time.Duration
	)

	if islocal {
		q.Set(URLParamLocal, "true")
	} else {
		q.Set(URLParamLocal, "false")
	}

	if wait {
		timeOut = append(timeOut, 0)
	}

	smap := p.smapowner.get()
	results = p.broadcastTargets(
		URLPath(Rversion, Rbuckets, bucket),
		q,
		method,
		jsonbytes,
		smap,
		timeOut...,
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

//===========================
//
// control plane
//
//===========================

// "/"+Rversion+"/"+Rdaemon
func (p *proxyrunner) daemonHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.httpdaeget(w, r)
	case http.MethodPut:
		p.httpdaeput(w, r)
	default:
		invalhdlr(w, r)
	}
}

func (p *proxyrunner) httpdaeget(w http.ResponseWriter, r *http.Request) {
	getWhat := r.URL.Query().Get(URLParamWhat)
	switch getWhat {
	case GetWhatConfig:
		jsbytes, err := json.Marshal(ctx.config)
		assert(err == nil)
		p.writeJSON(w, r, jsbytes, "httpdaeget")

	case GetWhatSmap:
		jsbytes, err := json.Marshal(p.smapowner.get())
		assert(err == nil, err)
		p.writeJSON(w, r, jsbytes, "httpdaeget")

	case GetWhatSmapVote:
		_, xx := p.xactinp.findL(ActElection)
		vote := xx != nil
		msg := SmapVoteMsg{
			VoteInProgress: vote,
			Smap:           p.smapowner.get(),
			BucketMD:       p.bmdowner.get(),
		}
		jsbytes, err := json.Marshal(msg)
		assert(err == nil, err)
		p.writeJSON(w, r, jsbytes, "httpdaeget")
	default:
		s := fmt.Sprintf("Unexpected GET request, invalid param 'what': [%s]", getWhat)
		p.invalmsghdlr(w, r, s)
	}
}

func (p *proxyrunner) httpdaeput(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 0, Rversion, Rdaemon); apitems == nil {
		return
	}
	if len(apitems) > 0 {
		switch apitems[0] {
		case Rproxy:
			p.httpdaesetprimaryproxy(w, r)
			return
		case Rsyncsmap:
			var newsmap = &Smap{}
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
			glog.Infof("%s: %s v%d done", p.si.DaemonID, Rsyncsmap, newsmap.version())
			return
		case Rmetasync:
			p.receiveMeta(w, r)
			return
		default:
		}
	}

	//
	// other PUT /daemon actions
	//
	var msg ActionMsg
	if p.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case ActSetConfig:
		var (
			value string
			ok    bool
		)
		if value, ok = msg.Value.(string); !ok {
			p.invalmsghdlr(w, r, fmt.Sprintf("Failed to parse ActionMsg value: not a string"))
			return
		}
		switch msg.Name {
		case "loglevel", "stats_time", "vmodule":
			if errstr := p.setconfig(msg.Name, value); errstr != "" {
				p.invalmsghdlr(w, r, errstr)
			}
		default:
			glog.Warningf("Invalid setconfig request: proxy does not support (updating) '%s'", msg.Name)
		}
	case ActShutdown:
		q := r.URL.Query()
		force, _ := parsebool(q.Get(URLParamForce))
		if p.smapowner.get().isPrimary(p.si) && !force {
			s := fmt.Sprintf("Cannot shutdown primary proxy without %s=true query parameter", URLParamForce)
			p.invalmsghdlr(w, r, s)
			return
		}
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	default:
		s := fmt.Sprintf("Unexpected ActionMsg <- JSON [%v]", msg)
		p.invalmsghdlr(w, r, s)
	}
}

func (p *proxyrunner) httpdaesetprimaryproxy(w http.ResponseWriter, r *http.Request) {
	var (
		prepare bool
		err     error
	)
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 2, Rversion, Rdaemon); apitems == nil {
		return
	}
	if p.smapowner.get().isPrimary(p.si) {
		s := fmt.Sprint("Expecting 'cluster' (RESTful) resource when designating primary proxy via API")
		p.invalmsghdlr(w, r, s)
		return
	}

	proxyid := apitems[1]
	query := r.URL.Query()
	preparestr := query.Get(URLParamPrepare)
	if prepare, err = strconv.ParseBool(preparestr); err != nil {
		s := fmt.Sprintf("Failed to parse %s URL parameter: %v", URLParamPrepare, err)
		p.invalmsghdlr(w, r, s)
		return
	}

	if p.si.DaemonID == proxyid {
		if !prepare {
			if s := p.becomeNewPrimary(""); s != "" {
				p.invalmsghdlr(w, r, s)
			}
		}
		return
	}

	smap := p.smapowner.get()
	psi := smap.getProxy(proxyid)
	if psi == nil {
		s := fmt.Sprintf("New primary proxy %s not present in the local %s", proxyid, smap.pp())
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
		assert(false, "This proxy '"+p.si.DaemonID+"' must always be present in the local "+smap.pp())
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

	msg := &ActionMsg{Action: ActNewPrimary}
	pair := &revspair{clone, msg}
	if glog.V(3) {
		glog.Infof("Distributing Smap v%d with the newly elected primary %s = self", clone.version(), p.si.DaemonID)
	}
	p.metasyncer.sync(true, pair)
	return
}

func (p *proxyrunner) httpclusetprimaryproxy(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 2, Rversion, Rcluster); apitems == nil {
		return
	}
	proxyid := apitems[1]
	if !p.checkPrimaryProxy("designate new primary proxy '"+proxyid+"'", w, r) {
		return
	}
	if proxyid == p.si.DaemonID {
		glog.Warningf("Request to set primary = %s = self: nothing to do", proxyid)
		return
	}

	smap := p.smapowner.get()
	psi, ok := smap.Pmap[proxyid]
	if !ok {
		s := fmt.Sprintf("New primary proxy %s not present in the local %s", proxyid, smap.pp())
		p.invalmsghdlr(w, r, s)
		return
	}

	// (I) prepare phase
	urlPath := URLPath(Rversion, Rdaemon, Rproxy, proxyid)
	q := url.Values{}
	q.Set(URLParamPrepare, "true")
	method := http.MethodPut
	results := p.broadcastCluster(
		urlPath,
		q,
		method,
		nil, // body
		smap,
		ctx.config.Timeout.CplaneOperation,
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
	if s := p.smapowner.persist(clone, true); s != "" {
		glog.Errorf("Failed to save Smap locally after having transitioned to non-primary:\n%s", s)
	}
	p.smapowner.put(clone)
	p.smapowner.Unlock()

	// (II) commit phase
	q.Set(URLParamPrepare, "false")
	results = p.broadcastCluster(
		urlPath,
		q,
		method,
		nil, // body
		p.smapowner.get(),
		ctx.config.Timeout.CplaneOperation,
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

// handler for: "/"+Rversion+"/"+Rcluster
func (p *proxyrunner) clusterHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.httpcluget(w, r)
	case http.MethodPost:
		p.httpclupost(w, r)
	case http.MethodDelete:
		p.httpcludel(w, r)
		glog.Flush()
	case http.MethodPut:
		p.httpcluput(w, r)
		glog.Flush()
	default:
		invalhdlr(w, r)
	}
}

// handler for: "/"+Rversion+"/"+Rtokens
func (p *proxyrunner) tokenHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodDelete:
		p.httpTokenDelete(w, r)
	default:
		invalhdlr(w, r)
	}
}

// handler for DFC to be used as an HTTP proxy
func (p *proxyrunner) reverseProxyHandler(w http.ResponseWriter, r *http.Request) {
	baseURL := r.URL.Scheme + "://" + r.URL.Host
	if baseURL == gcsURL && r.Method == http.MethodGet {
		s := p.restAPIItems(r.URL.Path, -1)
		if len(s) == 2 {
			r.URL.Path = URLPath(Rversion, Robjects) + r.URL.Path
			p.httpobjget(w, r)
			return
		} else if len(s) == 1 {
			r.URL.Path = URLPath(Rversion, Rbuckets) + r.URL.Path
			p.httpbckget(w, r)
			return
		}
	}
	p.httprunner.revProxy.ServeHTTP(w, r)
}

// gets target info
func (p *proxyrunner) httpcluget(w http.ResponseWriter, r *http.Request) {
	getWhat := r.URL.Query().Get(URLParamWhat)
	switch getWhat {
	case GetWhatStats:
		ok := p.invokeHttpGetClusterStats(w, r)
		if !ok {
			return
		}
	case GetWhatXaction:
		ok := p.invokeHttpGetXaction(w, r)
		if !ok {
			return
		}
	default:
		s := fmt.Sprintf("Unexpected GET request, invalid param 'what': [%s]", getWhat)
		p.invalmsghdlr(w, r, s)
	}
}

func (p *proxyrunner) invokeHttpGetXaction(w http.ResponseWriter, r *http.Request) bool {
	getProps := r.URL.Query().Get(URLParamProps)
	kind, err := p.getXactionKindFromProperties(getProps)
	if err != nil {
		glog.Errorf(
			"Unable to get kind from props: [%s]. Error: [%s]",
			getProps,
			err)
		p.invalmsghdlr(w, r, err.Error())
		return false
	}

	outputXactionStats := &XactionStats{}
	outputXactionStats.Kind = kind
	targetStats, ok := p.invokeHttpGetMsgOnTargets(w, r)
	if !ok {
		e := fmt.Sprintf(
			"Unable to invoke GetMsg on targets. Query: [%s]",
			r.URL.RawQuery)
		glog.Errorf(e)
		p.invalmsghdlr(w, r, e)
		return false
	}

	outputXactionStats.TargetStats = targetStats
	jsonBytes, err := json.Marshal(outputXactionStats)
	if err != nil {
		glog.Errorf(
			"Unable to marshal outputXactionStats. Error: [%s]", err)
		p.invalmsghdlr(w, r, err.Error())
		return false
	}

	ok = p.writeJSON(w, r, jsonBytes, "getXaction")
	return ok
}

func (p *proxyrunner) invokeHttpGetMsgOnTargets(w http.ResponseWriter, r *http.Request) (
	map[string]json.RawMessage, bool) {
	results := p.broadcastTargets(
		URLPath(Rversion, Rdaemon),
		r.URL.Query(),
		r.Method,
		nil, // message
		p.smapowner.get(),
		ctx.config.Timeout.Default,
	)

	targetResults := make(map[string]json.RawMessage, p.smapowner.get().countTargets())
	for result := range results {
		if result.err != nil {
			glog.Errorf(
				"Failed to fetch xaction, query: %s",
				r.URL.RawQuery)
			p.invalmsghdlr(w, r, result.errstr)
			return nil, false
		}

		targetResults[result.si.DaemonID] = json.RawMessage(
			result.outjson)
	}

	return targetResults, true
}

// FIXME: read-lock
func (p *proxyrunner) invokeHttpGetClusterStats(
	w http.ResponseWriter, r *http.Request) bool {
	targetStats, ok := p.invokeHttpGetMsgOnTargets(w, r)
	if !ok {
		errstr := fmt.Sprintf(
			"Unable to invoke GetMsg on targets. Query: [%s]",
			r.URL.RawQuery)
		glog.Errorf(errstr)
		p.invalmsghdlr(w, r, errstr)
		return false
	}

	out := &ClusterStatsRaw{}
	out.Target = targetStats
	rr := getproxystatsrunner()
	rr.Lock()
	out.Proxy = &rr.Core
	jsbytes, err := json.Marshal(out)
	rr.Unlock()
	assert(err == nil, err)
	ok = p.writeJSON(w, r, jsbytes, "HttpGetClusterStats")
	return ok
}

// register|keepalive target
func (p *proxyrunner) httpclupost(w http.ResponseWriter, r *http.Request) {
	var (
		nsi                   daemonInfo
		keepalive, register   bool
		isproxy, nonelectable bool
		msg                   *ActionMsg
	)
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 0, Rversion, Rcluster); apitems == nil {
		return
	}
	if len(apitems) > 0 {
		keepalive = apitems[0] == Rkeepalive
		register = apitems[0] == Rregister
		isproxy = apitems[0] == Rproxy
		if isproxy {
			if len(apitems) > 1 {
				keepalive = apitems[1] == Rkeepalive
			}
			s := r.URL.Query().Get(URLParamNonElectable)
			var err error
			if nonelectable, err = parsebool(s); s != "" && err != nil {
				glog.Errorf("Failed to parse %s for non-electability: %v", s, err)
			}
		}
	}
	if p.readJSON(w, r, &nsi) != nil {
		return
	}
	if !p.checkPrimaryProxy(fmt.Sprintf("register %s (isproxy=%t, keepalive=%t)", nsi.DaemonID, isproxy, keepalive), w, r) {
		return
	}
	if net.ParseIP(nsi.NodeIPAddr) == nil {
		s := fmt.Sprintf("register target %s: invalid IP address %v", nsi.DaemonID, nsi.NodeIPAddr)
		p.invalmsghdlr(w, r, s)
		return
	}

	p.statsdC.Send("post",
		statsd.Metric{
			Type:  statsd.Counter,
			Name:  "count",
			Value: 1,
		},
	)
	p.statsif.add("numpost", 1)

	p.smapowner.Lock()
	smap := p.smapowner.get()
	if isproxy {
		osi := smap.getProxy(nsi.DaemonID)
		if !p.addOrUpdateNode(&nsi, osi, keepalive, "proxy") {
			p.smapowner.Unlock()
			return
		}
		msg = &ActionMsg{Action: ActRegProxy}
	} else {
		osi := smap.getTarget(nsi.DaemonID)
		if !p.addOrUpdateNode(&nsi, osi, keepalive, "target") {
			p.smapowner.Unlock()
			return
		}
		if register {
			if glog.V(3) {
				glog.Infof("register target %s (num targets before %d)", nsi.DaemonID, smap.countTargets())
			}
			u := nsi.DirectURL + URLPath(Rversion, Rdaemon, Rregister)
			res := p.call(nil, &nsi, u, http.MethodPost, nil, ProxyPingTimeout)
			if res.err != nil {
				p.smapowner.Unlock()
				errstr := fmt.Sprintf("Failed to register target %s: %v, %s", nsi.DaemonID, res.err, res.errstr)
				p.invalmsghdlr(w, r, errstr, res.status)
				return
			}
		}
		msg = &ActionMsg{Action: ActRegTarget}
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
		outjson, err := json.Marshal(bmd4reg)
		assert(err == nil, err)
		p.writeJSON(w, r, outjson, "bucket-metadata")
	}
	// update and distribute Smap
	go func(isproxy, nonelectable bool) {
		p.smapowner.Lock()

		p.registerToSmap(isproxy, &nsi, nonelectable)
		smap := p.smapowner.get()
		pair := &revspair{smap, msg}

		if errstr := p.smapowner.persist(smap, true); errstr != "" {
			glog.Errorln(errstr)
		}
		p.smapowner.Unlock()
		p.metasyncer.sync(false, pair)
	}(isproxy, nonelectable)
}

func (p *proxyrunner) registerToSmap(isproxy bool, nsi *daemonInfo, nonelectable bool) {
	clone := p.smapowner.get().clone()
	if isproxy {
		clone.addProxy(nsi)
		if nonelectable {
			if clone.NonElects == nil {
				clone.NonElects = make(simplekvs)
				clone.NonElects[nsi.DaemonID] = ""
			}
			glog.Infof("Note: proxy %s won't be electable", nsi.DaemonID)
		}
		if glog.V(3) {
			glog.Infof("joined proxy %s (num proxies %d)", nsi.DaemonID, clone.countProxies())
		}
	} else {
		clone.addTarget(nsi)
		if glog.V(3) {
			glog.Infof("joined target %s (num targets %d)", nsi.DaemonID, clone.countTargets())
		}
	}
	p.smapowner.put(clone)
}

func (p *proxyrunner) addOrUpdateNode(nsi *daemonInfo, osi *daemonInfo, keepalive bool, kind string) bool {
	if keepalive {
		if osi == nil {
			glog.Warningf("register/keepalive %s %s: adding back to the cluster map", kind, nsi.DaemonID)
			return true
		}

		if osi.NodeIPAddr != nsi.NodeIPAddr || osi.DaemonPort != nsi.DaemonPort {
			glog.Warningf("register/keepalive %s %s: info changed - renewing", kind, nsi.DaemonID)
			return true
		}

		p.kalive.heardFrom(nsi.DaemonID, !keepalive /* reset */)
		return false
	}
	if osi != nil {
		if p.startedup(0) == 0 {
			return true
		}
		if osi.NodeIPAddr == nsi.NodeIPAddr && osi.DaemonPort == nsi.DaemonPort && osi.DirectURL == nsi.DirectURL {
			glog.Infof("register %s %s: already done", kind, nsi.DaemonID)
			return false
		}
		glog.Warningf("register %s %s: renewing the registration %+v => %+v", kind, nsi.DaemonID, osi, nsi)
	}
	return true
}

// unregisters a target/proxy
func (p *proxyrunner) httpcludel(w http.ResponseWriter, r *http.Request) {
	if !p.checkPrimaryProxy("unregister target/proxy", w, r) {
		return
	}
	apitems := p.restAPIItems(r.URL.Path, 6)
	if apitems = p.checkRestAPI(w, r, apitems, 2, Rversion, Rcluster); apitems == nil {
		return
	}
	if apitems[0] != Rdaemon {
		s := fmt.Sprintf("Invalid API element: %s (expecting %s)", apitems[0], Rdaemon)
		p.invalmsghdlr(w, r, s)
		return
	}
	var (
		isproxy bool
		msg     *ActionMsg
		osi     *daemonInfo
		psi     *daemonInfo
		sid     = apitems[1]
	)
	if sid == Rproxy {
		isproxy = true
		sid = apitems[2]
	}

	p.smapowner.Lock()

	smap := p.smapowner.get()
	clone := smap.clone()

	if isproxy {
		psi = clone.getProxy(sid)
		if psi == nil {
			glog.Errorf("Unknown proxy %s", sid)
			p.smapowner.Unlock()
			return
		}
		clone.delProxy(sid)
		if glog.V(3) {
			glog.Infof("unregistered proxy {%s} (num proxies %d)", sid, clone.countProxies())
		}
		msg = &ActionMsg{Action: ActUnregProxy}
	} else {
		osi = clone.getTarget(sid)
		if osi == nil {
			glog.Errorf("Unknown target %s", sid)
			p.smapowner.Unlock()
			return
		}
		clone.delTarget(sid)
		if glog.V(3) {
			glog.Infof("unregistered target {%s} (num targets %d)", sid, clone.countTargets())
		}
		u := osi.DirectURL + URLPath(Rversion, Rdaemon)
		res := p.call(nil, osi, u, http.MethodDelete, nil, ProxyPingTimeout)
		if res.err != nil {
			glog.Warningf("The target %s that is being unregistered failed to respond back: %v, %s",
				osi.DaemonID, res.err, res.errstr)
		}
		msg = &ActionMsg{Action: ActUnregTarget}
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

	pair := &revspair{clone, msg}
	p.metasyncer.sync(true, pair)
}

// '{"action": "shutdown"}' /v1/cluster => (proxy) =>
// '{"action": "syncsmap"}' /v1/cluster => (proxy) => PUT '{Smap}' /v1/daemon/syncsmap => target(s)
// '{"action": "rebalance"}' /v1/cluster => (proxy) => PUT '{Smap}' /v1/daemon/rebalance => target(s)
// '{"action": "setconfig"}' /v1/cluster => (proxy) =>
func (p *proxyrunner) httpcluput(w http.ResponseWriter, r *http.Request) {
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 0, Rversion, Rcluster); apitems == nil {
		return
	}
	// cluster-wide: designate a new primary proxy (administratively)
	if len(apitems) > 0 && apitems[0] == Rproxy {
		p.httpclusetprimaryproxy(w, r)
		return
	}

	var msg ActionMsg
	if p.readJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case ActSetConfig:
		if value, ok := msg.Value.(string); !ok {
			p.invalmsghdlr(w, r, fmt.Sprintf("Failed to parse ActionMsg value: Not a string"))
		} else if errstr := p.setconfig(msg.Name, value); errstr != "" {
			p.invalmsghdlr(w, r, errstr)
		} else {
			msgbytes, err := json.Marshal(msg) // same message -> all targets and proxies
			assert(err == nil, err)

			results := p.broadcastCluster(
				URLPath(Rversion, Rdaemon),
				nil, // query
				http.MethodPut,
				msgbytes,
				p.smapowner.get(),
			)

			for result := range results {
				if result.err != nil {
					p.invalmsghdlr(
						w,
						r,
						fmt.Sprintf("%s (%s = %s) failed, err: %s", msg.Action, msg.Name, value, result.errstr),
					)
					p.kalive.onerr(err, result.status)
				}
			}
		}
	case ActShutdown:
		glog.Infoln("Proxy-controlled cluster shutdown...")
		msgbytes, err := json.Marshal(msg) // same message -> all targets
		assert(err == nil, err)

		p.broadcastCluster(
			URLPath(Rversion, Rdaemon),
			nil, // query
			http.MethodPut,
			msgbytes,
			p.smapowner.get(),
		)

		time.Sleep(time.Second)
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)

	case ActRebalance:
		if !p.checkPrimaryProxy("initiate rebalance", w, r) {
			return
		}
		pair := &revspair{p.smapowner.get(), &msg}
		p.metasyncer.sync(false, pair)

	default:
		s := fmt.Sprintf("Unexpected ActionMsg <- JSON [%v]", msg)
		p.invalmsghdlr(w, r, s)
	}
}

//========================
//
// delayed broadcasts
//
//========================

func (p *proxyrunner) checkPrimaryProxy(action string, w http.ResponseWriter, r *http.Request) bool {
	smap := p.smapowner.get()
	if smap.isPrimary(p.si) {
		return true
	}
	s := fmt.Sprintf("%s is non-primary: cannot execute '%v'", p.si.DaemonID, action)
	if smap.ProxySI != nil {
		w.Header().Add(HeaderPrimaryProxyURL, smap.ProxySI.DirectURL)
		w.Header().Add(HeaderPrimaryProxyID, smap.ProxySI.DaemonID)
		s = fmt.Sprintf("%s, %s=%s", s, HeaderPrimaryProxyURL, smap.ProxySI.DirectURL)
	}
	p.invalmsghdlr(w, r, s)
	return false
}

func (p *proxyrunner) httpTokenDelete(w http.ResponseWriter, r *http.Request) {
	tokenList := &TokenList{}
	apitems := p.restAPIItems(r.URL.Path, 5)
	if apitems = p.checkRestAPI(w, r, apitems, 0, Rversion, Rtokens); apitems == nil {
		return
	}

	if err := p.readJSON(w, r, tokenList); err != nil {
		s := fmt.Sprintf("Invalid token list: %v", err)
		p.invalmsghdlr(w, r, s)
		return
	}

	p.authn.updateRevokedList(tokenList)

	if p.smapowner.get().isPrimary(p.si) {
		url := URLPath(Rversion, Rtokens)
		jsonList, err := json.Marshal(tokenList)
		if err != nil {
			glog.Errorf("Failed to marshal token list: %v", err)
			return
		}
		go func() {
			ch := p.broadcastCluster(url, nil, http.MethodDelete, jsonList, p.smapowner.get(),
				ctx.config.Timeout.CplaneOperation)
			for r := range ch {
				if r.err != nil {
					glog.Errorf("Failed to broadcast token revoke request: %v", r.err)
				}
			}
		}()
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

func (p *proxyrunner) receiveMeta(w http.ResponseWriter, r *http.Request) {
	if p.smapowner.get().isPrimary(p.si) {
		_, xx := p.xactinp.findL(ActElection)
		vote := xx != nil
		s := fmt.Sprintf("Primary %s cannot receive cluster meta (election=%t)", p.si.DaemonID, vote)
		p.invalmsghdlr(w, r, s)
		return
	}
	var payload = make(simplekvs)

	if p.readJSON(w, r, &payload) != nil {
		return
	}

	newsmap, _, _, errstr := p.extractSmap(payload)
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
		}
	}
}

func (p *proxyrunner) receiveBucketMD(newbucketmd *bucketMD, msg *ActionMsg) (errstr string) {
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
	smap *Smap, timeout ...time.Duration) chan callResult {

	var servers []*daemonInfo
	for _, s := range smap.Tmap {
		servers = append(servers, s)
	}

	for _, s := range smap.Pmap {
		// Don't broadcast to self
		if s.DaemonID != p.si.DaemonID {
			servers = append(servers, s)
		}
	}

	return p.broadcast(path, query, method, body, servers, timeout...)
}

// broadcastTargets sends a message ([]byte) to all targets belongs to a smap
func (p *proxyrunner) broadcastTargets(path string, query url.Values, method string, body []byte,
	smap *Smap, timeout ...time.Duration) chan callResult {

	var servers []*daemonInfo
	for _, s := range smap.Tmap {
		servers = append(servers, s)
	}

	return p.broadcast(path, query, method, body, servers, timeout...)
}

func validateBucketProps(props *BucketProps, isLocal bool) error {
	if props.NextTierURL != "" {
		if _, err := url.ParseRequestURI(props.NextTierURL); err != nil {
			return fmt.Errorf("invalid next tier URL: %s, err: %v", props.NextTierURL, err)
		}
	}
	if err := ValidateCloudProvider(props.CloudProvider, isLocal); err != nil {
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
				ProviderAmazon, ProviderGoogle, ProviderDfc)
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
	return nil
}

func ValidateCloudProvider(provider string, isLocal bool) error {
	if provider != "" && provider != ProviderAmazon && provider != ProviderGoogle && provider != ProviderDfc {
		return fmt.Errorf("invalid cloud provider: %s, must be one of (%s | %s | %s)", provider,
			ProviderAmazon, ProviderGoogle, ProviderDfc)
	} else if isLocal && provider != ProviderDfc && provider != "" {
		return fmt.Errorf("local bucket can only have '%s' as the cloud provider", ProviderDfc)
	}
	return nil
}
