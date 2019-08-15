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
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/mirror"
	"github.com/NVIDIA/aistore/objwalk"
	"github.com/NVIDIA/aistore/stats"
	jsoniter "github.com/json-iterator/go"
)

const (
	tokenStart = "Bearer"
)

type (
	ClusterMountpathsRaw struct {
		Targets map[string]jsoniter.RawMessage `json:"targets"`
	}
	// two pieces of metadata a self-registering (joining) target wants to know right away
	targetRegMeta struct {
		Smap *smapX         `json:"smap"`
		BMD  *bucketMD      `json:"bmd"`
		SI   *cluster.Snode `json:"si"`
	}

	reverseProxy struct {
		cloud   *httputil.ReverseProxy // unmodified GET requests => storage.googleapis.com
		primary struct {
			sync.Mutex
			rp  *httputil.ReverseProxy // requests that modify cluster-level metadata => current primary gateway
			url string                 // URL of the current primary
		}
		nodes sync.Map // map of reverse proxies keyed by node DaemonIDs
	}

	// proxy runner
	proxyrunner struct {
		httprunner
		authn      *authManager
		startedUp  atomic.Bool
		metasyncer *metasyncer
		rproxy     reverseProxy
	}
)

func wrapHandler(h http.HandlerFunc, wraps ...func(http.HandlerFunc) http.HandlerFunc) http.HandlerFunc {
	for _, w := range wraps {
		h = w(h)
	}

	return h
}

func (rp *reverseProxy) init() {
	cfg := cmn.GCO.Get()
	if cfg.Net.HTTP.RevProxy == cmn.RevProxyCloud {
		rp.cloud = &httputil.ReverseProxy{
			Director: func(r *http.Request) {},
			Transport: cmn.NewTransport(cmn.TransportArgs{
				UseHTTPS: cfg.Net.HTTP.UseHTTPS,
			}),
		}
	}
}

func (p *proxyrunner) initClusterCIDR() {
	if nodeCIDR := os.Getenv("AIS_CLUSTER_CIDR"); nodeCIDR != "" {
		_, network, err := net.ParseCIDR(nodeCIDR)
		p.si.LocalNet = network
		cmn.AssertNoErr(err)
		glog.Infof("local network: %+v", *network)
	}
}

// start proxy runner
func (p *proxyrunner) Run() error {
	config := cmn.GCO.Get()
	p.httprunner.init(getproxystatsrunner(), config)
	p.httprunner.keepalive = getproxykeepalive()

	bucketmdfull := filepath.Join(config.Confdir, cmn.BucketmdBackupFile)
	bmd := newBucketMD()
	if cmn.LocalLoad(bucketmdfull, bmd) != nil {
		// create empty
		bmd.Version = 1
		if err := cmn.LocalSave(bucketmdfull, bmd); err != nil {
			glog.Fatalf("FATAL: cannot store %s, err: %v", bmdTermName, err)
		}
	}
	p.bmdowner.put(bmd)

	p.metasyncer = getmetasyncer()

	// startup sequence - see earlystart.go for the steps and commentary
	cmn.Assert(p.smapowner.get() == nil)
	p.bootstrap()

	p.authn = &authManager{
		tokens:        make(map[string]*authRec),
		revokedTokens: make(map[string]bool),
		version:       1,
	}

	p.rproxy.init()

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
		networkHandler{r: cmn.Reverse, h: p.reverseHandler, net: []string{cmn.NetworkPublic}},

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

	dsort.RegisterNode(p.smapowner, p.bmdowner, p.si, nil, p.statsif)
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
			res = p.join(query)
		} else {
			res = p.join(nil)
		}
	} else { // keepalive
		url, psi := p.getPrimaryURLAndSI()
		res = p.registerToURL(url, psi, timeout, nil, keepalive)
	}
	return res.status, res.err
}

func (p *proxyrunner) unregister() (int, error) {
	smap := p.smapowner.get()
	args := callArgs{
		si: smap.ProxySI,
		req: cmn.ReqArgs{
			Method: http.MethodDelete,
			Path:   cmn.URLPath(cmn.Version, cmn.Cluster, cmn.Daemon, p.si.DaemonID),
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

	normalizedBckProvider, err := cmn.ProviderFromStr(bckProvider)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	// list bucket names
	if bucket == cmn.ListAll {
		p.getbucketnames(w, r, normalizedBckProvider)
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
	if bmd, _ := p.validateBucket(w, r, bucket, bckProvider); bmd == nil {
		return
	}
	smap := p.smapowner.get()
	si, err := cluster.HrwTarget(bucket, objname, &smap.Smap)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraControl)
	pReq, err := http.NewRequest(r.Method, redirectURL, r.Body)
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
	bmd, bckIsLocal := p.validateBucket(w, r, bucket, bckProvider)
	if bmd == nil {
		return
	}
	if err := bmd.AllowGET(bucket, bckIsLocal); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	smap := p.smapowner.get()
	si, err := cluster.HrwTarget(bucket, objname, &smap.Smap)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	config := cmn.GCO.Get()
	if config.Net.HTTP.RevProxy == cmn.RevProxyTarget {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("reverse-proxy: %s %s/%s <= %s", r.Method, bmd.Bstring(bucket, bckIsLocal), objname, si)
		}
		p.reverseNodeRequest(w, r, si)
		delta := time.Since(started)
		p.statsif.Add(stats.GetLatency, int64(delta))
	} else {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s %s/%s => %s", r.Method, bmd.Bstring(bucket, bckIsLocal), objname, si)
		}
		redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraData)
		http.Redirect(w, r, redirectURL, http.StatusMovedPermanently)
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
	bucket, objName := apitems[0], apitems[1]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	bmd, bckIsLocal := p.validateBucket(w, r, bucket, bckProvider)
	if bmd == nil {
		return
	}
	if err := bmd.AllowPUT(bucket, bckIsLocal); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	smap := p.smapowner.get()
	si, err := cluster.HrwTarget(bucket, objName, &smap.Smap)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		bmd := p.bmdowner.get()
		glog.Infof("%s %s/%s => %s", r.Method, bmd.Bstring(bucket, bckIsLocal), objName, si)
	}
	redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraData)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)

	p.statsif.Add(stats.PutCount, 1)
}

// DELETE { action } /v1/buckets
func (p *proxyrunner) httpbckdelete(w http.ResponseWriter, r *http.Request) {
	var msg cmn.ActionMsg
	apitems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}
	bucket := apitems[0]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	bmd, bckIsLocal := p.validateBucket(w, r, bucket, bckProvider)
	if bmd == nil {
		return
	}
	if err := bmd.AllowDELETE(bucket, bckIsLocal); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	switch msg.Action {
	case cmn.ActDestroyLB:
		if p.forwardCP(w, r, &msg, bucket, nil) {
			return
		}
		if _, err, errCode := p.destroyBucket(&msg, bucket, true); err != nil {
			p.invalmsghdlr(w, r, err.Error(), errCode)
			return
		}
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
		bmd, err, errCode := p.destroyBucket(&msg, bucket, false)
		if err != nil {
			p.invalmsghdlr(w, r, err.Error(), errCode)
			return
		}

		msgInt := p.newActionMsgInternal(&msg, nil, bmd)
		body := cmn.MustMarshal(msgInt)

		// wipe the local copy of the cloud bucket on targets
		smap := p.smapowner.get()
		results := p.broadcastTo(
			cmn.URLPath(cmn.Version, cmn.Buckets, bucket),
			nil, // query
			http.MethodDelete,
			body,
			smap,
			defaultTimeout,
			cmn.NetworkIntraControl,
			cluster.Targets,
		)
		for res := range results {
			if res.err != nil {
				p.invalmsghdlr(w, r, fmt.Sprintf("%s failed, err: %s", msg.Action, res.err))
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
	bmd, bckIsLocal := p.validateBucket(w, r, bucket, bckProvider)
	if bmd == nil {
		return
	}
	if err := bmd.AllowDELETE(bucket, bckIsLocal); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	smap := p.smapowner.get()
	si, err := cluster.HrwTarget(bucket, objname, &smap.Smap)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s/%s => %s", r.Method, bmd.Bstring(bucket, bckIsLocal), objname, si)
	}
	redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)

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
	smap := p.smapowner.get()
	if smap.isPrimary(p.si) {
		vote := p.xactions.globalXactRunning(cmn.ActElection)
		s := fmt.Sprintf("Primary %s cannot receive cluster meta (election=%t)", p.si, vote)
		p.invalmsghdlr(w, r, s)
		return
	}
	var payload = make(cmn.SimpleKVs)
	if err := cmn.ReadJSON(w, r, &payload); err != nil {
		return
	}

	newSmap, _, err := p.extractSmap(payload)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	if newSmap != nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			caller := r.Header.Get(cmn.HeaderCallerName)
			glog.Infof("new Smap v%d from %s", newSmap.version(), caller)
		}
		err = p.smapowner.synchronize(newSmap, true /*saveSmap*/, true /* lesserIsErr */)
		if err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}

		// When some node was removed from the cluster we need to clean up the
		// reverse proxy structure.
		p.rproxy.nodes.Range(func(key, _ interface{}) bool {
			nodeID := key.(string)
			if smap.containsID(nodeID) && !newSmap.containsID(nodeID) {
				p.rproxy.nodes.Delete(nodeID)
			}
			return true
		})

		if !newSmap.isPresent(p.si) {
			s := fmt.Sprintf("Warning: not finding self '%s' in the received %s", p.si, newSmap.pp())
			glog.Errorln(s)
		}
	}

	newBMD, actionLB, err := p.extractbucketmd(payload)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if newBMD != nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			caller := r.Header.Get(cmn.HeaderCallerName)
			glog.Infof("new %s v%d from %s", bmdTermName, newBMD.version(), caller)
		}
		if err = p.receiveBucketMD(newBMD, actionLB); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
	}

	revokedTokens, err := p.extractRevokedTokenList(payload)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
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
	if !p.startedUp.Load() {
		// If not yet ready we need to send status accepted -> completed but
		// not yet ready.
		w.WriteHeader(http.StatusAccepted)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (p *proxyrunner) createBucket(msg *cmn.ActionMsg, bucket string, config *cmn.Config, local bool) error {
	p.bmdowner.Lock()
	clone := p.bmdowner.get().clone()
	bucketProps := cmn.DefaultBucketProps(local)
	if !clone.add(bucket, local, bucketProps) {
		p.bmdowner.Unlock()
		return cmn.NewErrorBucketAlreadyExists(bucket)
	}
	if err := p.savebmdconf(clone, config); err != nil {
		p.bmdowner.Unlock()
		return err
	}
	p.bmdowner.put(clone)
	p.bmdowner.Unlock()
	msgInt := p.newActionMsgInternal(msg, nil, clone)
	p.metasyncer.sync(true, revspair{clone, msgInt})
	return nil
}

func (p *proxyrunner) destroyBucket(msg *cmn.ActionMsg, bucket string, local bool) (*bucketMD, error, int) {
	p.bmdowner.Lock()
	bmd := p.bmdowner.get()
	if local {
		if !bmd.IsLocal(bucket) {
			err := fmt.Errorf("bucket %s does not appear to be local or %s", bucket, cmn.DoesNotExist)
			p.bmdowner.Unlock()
			return bmd, err, http.StatusNotFound
		}
	} else if !bmd.IsCloud(bucket) {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s: %s %s - nothing to do", cmn.ActEvictCB, bmd.Bstring(bucket, local), cmn.DoesNotExist)
		}
		p.bmdowner.Unlock()
		return bmd, nil, 0
	}
	clone := bmd.clone()
	if !clone.del(bucket, local) {
		p.bmdowner.Unlock()
		return nil, fmt.Errorf("bucket %s %s", bucket, cmn.DoesNotExist), http.StatusNotFound
	}
	if err := p.savebmdconf(clone, cmn.GCO.Get()); err != nil {
		p.bmdowner.Unlock()
		return nil, err, http.StatusInternalServerError
	}
	p.bmdowner.put(clone)
	p.bmdowner.Unlock()

	msgInt := p.newActionMsgInternal(msg, nil, clone)
	p.metasyncer.sync(true, revspair{clone, msgInt})

	return clone, nil, 0
}

// POST { action } /v1/buckets/bucket-name
func (p *proxyrunner) httpbckpost(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	var (
		msg                  cmn.ActionMsg
		bucket, bckProvider  string
		bmd                  *bucketMD
		bckIsLocal, actLocal bool
	)
	apitems, err := p.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}

	// detect bucket name if it is single-bucket request
	bckProvider = r.URL.Query().Get(cmn.URLParamBckProvider)
	if len(apitems) != 0 {
		bucket = apitems[0]
		if bmd, bckIsLocal = p.validateBucket(w, r, bucket, bckProvider); bmd == nil {
			return
		}
	}

	// bucket name + action
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
	// bucketMD-wide action
	if len(apitems) == 0 {
		if msg.Action == cmn.ActRecoverBck {
			p.recoverBuckets(w, r, &msg)
		} else {
			p.invalmsghdlr(w, r, "path is too short: bucket name expected")
		}
		return
	}
	switch msg.Action {
	case cmn.ActCreateLB:
		actLocal = true
		bckIsLocal = bckProvider == "" || cmn.IsProviderLocal(bckProvider)
	case cmn.ActCopyLB, cmn.ActRenameLB, cmn.ActSyncLB:
		actLocal = true
	}
	if actLocal && !bckIsLocal {
		p.invalmsghdlr(w, r, msg.Action+": invalid bucket provider "+bckProvider)
		return
	}
	config := cmn.GCO.Get()
	switch msg.Action {
	case cmn.ActCreateLB:
		if p.forwardCP(w, r, &msg, bucket, nil) {
			return
		}
		if err := p.createBucket(&msg, bucket, config, true); err != nil {
			errCode := http.StatusInternalServerError
			if _, ok := err.(*cmn.ErrorBucketAlreadyExists); ok {
				errCode = http.StatusConflict
			}
			p.invalmsghdlr(w, r, err.Error(), errCode)
			return
		}
	case cmn.ActCopyLB, cmn.ActRenameLB:
		if p.forwardCP(w, r, &msg, "", nil) {
			return
		}
		bucketFrom, bucketTo := bucket, msg.Name
		if bucketFrom == bucketTo {
			s := fmt.Sprintf("invalid %s request: duplicate bucket name %q", msg.Action, bucketFrom)
			p.invalmsghdlr(w, r, s)
			return
		}
		if err := cmn.ValidateBucketName(bucketTo); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		if err := p.copyRenameLB(bucketFrom, bucketTo, &msg, config); err != nil {
			p.invalmsghdlr(w, r, err.Error())
		}
	case cmn.ActSyncLB:
		if p.forwardCP(w, r, &msg, "", nil) {
			return
		}
		msgInt := p.newActionMsgInternal(&msg, nil, bmd)
		p.metasyncer.sync(false, revspair{bmd, msgInt})
	case cmn.ActRegisterCB:
		if p.forwardCP(w, r, &msg, bucket, nil) {
			return
		}
		if err := p.createBucket(&msg, bucket, config, false); err != nil {
			errCode := http.StatusInternalServerError
			if _, ok := err.(*cmn.ErrorBucketAlreadyExists); ok {
				errCode = http.StatusConflict
			}
			p.invalmsghdlr(w, r, err.Error(), errCode)
			return
		}
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
	bucket, bckProvider string, amsg cmn.ActionMsg, started time.Time, fastListing bool) {
	var (
		taskID  int64
		err     error
		bckList *cmn.BucketList
		smsg    = cmn.SelectMsg{}
		query   = r.URL.Query()
	)

	listMsgJSON := cmn.MustMarshal(amsg.Value)
	if err := jsoniter.Unmarshal(listMsgJSON, &smsg); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	// override fastListing if it set
	if fastListing {
		smsg.Fast = fastListing
	}
	// override prefix if it is set in URL query values
	if prefix := query.Get(cmn.URLParamPrefix); prefix != "" {
		smsg.Prefix = prefix
	}

	idstr := r.URL.Query().Get(cmn.URLParamTaskID)
	if idstr != "" {
		id, err := strconv.ParseInt(idstr, 10, 64)
		if err != nil {
			p.invalmsghdlr(w, r, fmt.Sprintf("Invalid task id: %s", idstr))
			return
		}
		smsg.TaskID = id
	}

	if bckList, taskID, err = p.listBucket(r, bucket, bckProvider, smsg); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	// taskID == 0 means that async runner has completed and the result is available
	// otherwise it is an ID of a still running task
	if taskID != 0 {
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(strconv.FormatInt(taskID, 10)))
		return
	}

	b := cmn.MustMarshal(bckList)
	pageMarker := bckList.PageMarker
	// free memory allocated for temporary slice immediately as it can take up to a few GB
	bckList.Entries = bckList.Entries[:0]
	bckList.Entries = nil
	bckList = nil
	go cmn.FreeMemToOS(time.Second)

	if p.writeJSON(w, r, b, "listbucket") {
		delta := time.Since(started)
		p.statsif.AddMany(stats.NamedVal64{stats.ListCount, 1}, stats.NamedVal64{stats.ListLatency, int64(delta)})
		if glog.FastV(4, glog.SmoduleAIS) {
			var (
				lat = int64(delta / time.Microsecond)
				s   string
			)
			if pageMarker != "" {
				s = ", page " + pageMarker
			}
			glog.Infof("LIST: %s%s, %d µs", bucket, s, lat)
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
	if bmd, _ := p.validateBucket(w, r, bucket, cmn.LocalBs); bmd == nil {
		return
	}
	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case cmn.ActRename:
		p.objRename(w, r)
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
	bmd, _ := p.validateBucket(w, r, bucket, bckProvider)
	if bmd == nil {
		return
	}
	var si *cluster.Snode
	// Use random map iteration order to choose a random target to redirect to
	smap := p.smapowner.get()
	for _, si = range smap.Tmap {
		break
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s => %s", r.Method, bucket, si)
	}
	redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

func (p *proxyrunner) updateBucketProps(bucket string, bckIsLocal bool, nvs cmn.SimpleKVs) (errRet error) {
	const errFmt = "invalid %s value %q: %v"
	p.bmdowner.Lock()
	clone := p.bmdowner.get().clone()
	config := cmn.GCO.Get()

	bprops, exists := clone.Get(bucket, bckIsLocal)
	if !exists {
		cmn.Assert(!bckIsLocal)

		existsInCloud, err := p.doesCloudBucketExist(bucket)
		if err != nil {
			p.bmdowner.Unlock()
			return err
		}
		if !existsInCloud {
			p.bmdowner.Unlock()
			return cmn.NewErrorCloudBucketDoesNotExist(bucket)
		}

		bprops = cmn.DefaultBucketProps(bckIsLocal)
		clone.add(bucket, false /* bucket is local */, bprops)
	}

	// HTTP headers display property names title-cased so that LRULowWM becomes Lrulowwm, etc.
	// - make sure to lowercase therefore
	for key, val := range nvs {
		name, value := strings.ToLower(key), val
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("Updating bucket %s property %s => %s", bucket, name, value)
		}
		// Disable ability to set EC properties once enabled
		if !bprops.EC.Updatable(name) {
			errRet = errors.New("cannot change EC configuration after it is enabled")
			p.bmdowner.Unlock()
			return
		}

		readonly, ok := cmn.BucketPropList[name]
		if !ok {
			errRet = fmt.Errorf("unknown property '%s'", name)
			p.bmdowner.Unlock()
			return
		}
		if ok && readonly {
			errRet = fmt.Errorf("property '%s' is read-only", name)
			p.bmdowner.Unlock()
			return
		}

		switch name {
		case cmn.HeaderBucketECEnabled:
			if v, err := cmn.ParseBool(value); err == nil {
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
				errRet = fmt.Errorf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketECData:
			if v, err := cmn.ParseI64Range(value, 10, 32, 1, 32); err == nil {
				bprops.EC.DataSlices = int(v)
			} else {
				errRet = fmt.Errorf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketECParity:
			if v, err := cmn.ParseI64Range(value, 10, 32, 1, 32); err == nil {
				bprops.EC.ParitySlices = int(v)
			} else {
				errRet = fmt.Errorf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketECMinSize:
			if v, err := strconv.ParseInt(value, 10, 64); err == nil {
				bprops.EC.ObjSizeLimit = v
			} else {
				errRet = fmt.Errorf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketMirrorEnabled:
			if v, err := cmn.ParseBool(value); err == nil {
				bprops.Mirror.Enabled = v
				if v && bprops.Mirror.Copies == 1 {
					bprops.Mirror.Copies = 2
				}
			} else {
				errRet = fmt.Errorf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketCopies:
			if v, err := cmn.ParseI64Range(value, 10, 32, 1, mirror.MaxNCopies); err == nil {
				bprops.Mirror.Copies = v
				if v == 1 {
					bprops.Mirror.Enabled = false
				}
			} else {
				errRet = fmt.Errorf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketMirrorThresh:
			if v, err := cmn.ParseI64Range(value, 10, 64, 0, 100); err == nil {
				bprops.Mirror.UtilThresh = v
			} else {
				errRet = fmt.Errorf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketVerEnabled:
			if v, err := cmn.ParseBool(value); err == nil {
				bprops.Versioning.Enabled = v
			} else {
				errRet = fmt.Errorf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketVerValidateWarm:
			if v, err := cmn.ParseBool(value); err == nil {
				bprops.Versioning.ValidateWarmGet = v
			} else {
				errRet = fmt.Errorf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketLRUEnabled:
			if v, err := cmn.ParseBool(value); err == nil {
				bprops.LRU.Enabled = v
			} else {
				errRet = fmt.Errorf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketLRULowWM:
			if v, err := cmn.ParseI64Range(value, 10, 64, 0, 100); err == nil {
				bprops.LRU.LowWM = v
			} else {
				errRet = fmt.Errorf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketLRUHighWM:
			if v, err := cmn.ParseI64Range(value, 10, 64, 0, 100); err == nil {
				bprops.LRU.HighWM = v
			} else {
				errRet = fmt.Errorf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketValidateColdGet:
			if v, err := cmn.ParseBool(value); err == nil {
				bprops.Cksum.ValidateColdGet = v
				if bprops.Cksum.Type == cmn.PropInherit {
					bprops.Cksum.Type = config.Cksum.Type
				}
			} else {
				errRet = fmt.Errorf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketValidateWarmGet:
			if v, err := cmn.ParseBool(value); err == nil {
				bprops.Cksum.ValidateWarmGet = v
				if bprops.Cksum.Type == cmn.PropInherit {
					bprops.Cksum.Type = config.Cksum.Type
				}
			} else {
				errRet = fmt.Errorf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketValidateObjMove:
			if v, err := cmn.ParseBool(value); err == nil {
				bprops.Cksum.ValidateObjMove = v
				if bprops.Cksum.Type == cmn.PropInherit {
					bprops.Cksum.Type = config.Cksum.Type
				}
			} else {
				errRet = fmt.Errorf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketEnableReadRange: // true: Return range checksum, false: return the obj's
			if v, err := cmn.ParseBool(value); err == nil {
				bprops.Cksum.EnableReadRange = v
				if bprops.Cksum.Type == cmn.PropInherit {
					bprops.Cksum.Type = config.Cksum.Type
				}
			} else {
				errRet = fmt.Errorf(errFmt, name, value, err)
			}
		case cmn.HeaderBucketAccessAttrs:
			if v, err := strconv.ParseUint(value, 10, 64); err == nil {
				bprops.AccessAttrs = v
			} else {
				errRet = fmt.Errorf(errFmt, name, value, err)
			}
		default:
			cmn.AssertMsg(false, "unknown property: "+name)
		}

		if errRet != nil {
			p.bmdowner.Unlock()
			return
		}
	}

	clone.set(bucket, bckIsLocal, bprops)
	if err := p.savebmdconf(clone, config); err != nil {
		glog.Error(err)
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
	apitems, err := p.checkRESTItems(w, r, 1, true, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	bmd, bckIsLocal := p.validateBucket(w, r, bucket, bckProvider)
	if bmd == nil {
		return
	}
	// This request has to be handled by primary proxy
	if p.forwardCP(w, r, &cmn.ActionMsg{}, "httpbckput", nil) {
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

		err := p.updateBucketProps(bucket, bckIsLocal, kvs)
		if _, ok := err.(*cmn.ErrorCloudBucketDoesNotExist); ok {
			p.invalmsghdlr(w, r, err.Error(), http.StatusNotFound)
			return
		}
		if err != nil {
			p.invalmsghdlr(w, r, err.Error())
		}

		return
	}

	// otherwise, handle the general case: unmarshal into cmn.BucketProps and treat accordingly
	// Note: this use case is for setting all bucket props
	nprops := cmn.DefaultBucketProps(bckIsLocal)
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

		existsInCloud, err := p.doesCloudBucketExist(bucket)
		if err != nil {
			p.bmdowner.Unlock()
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		if !existsInCloud {
			p.bmdowner.Unlock()
			p.invalmsghdlr(w, r, fmt.Sprintf("Bucket %s does not exist.", bucket), http.StatusNotFound)
			return
		}
		bprops = cmn.DefaultBucketProps(bckIsLocal)
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
		bprops.CopyFrom(nprops)
	case cmn.ActResetProps:
		bprops = cmn.DefaultBucketProps(bckIsLocal)
	}
	clone.set(bucket, bckIsLocal, bprops)
	if err := p.savebmdconf(clone, config); err != nil {
		glog.Error(err)
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
	checkCached, _ := cmn.ParseBool(query.Get(cmn.URLParamCheckCached))
	apitems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	bckProvider := query.Get(cmn.URLParamBckProvider)
	bmd, bckIsLocal := p.validateBucket(w, r, bucket, bckProvider)
	if bmd == nil {
		return
	}
	if err := bmd.AllowHEAD(bucket, bckIsLocal); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	smap := p.smapowner.get()
	si, err := cluster.HrwTarget(bucket, objname, &smap.Smap)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusInternalServerError)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s/%s => %s", r.Method, bucket, objname, si)
	}
	redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraControl)
	if checkCached {
		redirectURL += fmt.Sprintf("&%s=true", cmn.URLParamCheckCached)
	}
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
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
		body = cmn.MustMarshal(msg)
	}
	primary := &p.rproxy.primary
	primary.Lock()
	if primary.url != smap.ProxySI.PublicNet.DirectURL {
		primary.url = smap.ProxySI.PublicNet.DirectURL
		uparsed, err := url.Parse(smap.ProxySI.PublicNet.DirectURL)
		cmn.AssertNoErr(err)
		primary.rp = httputil.NewSingleHostReverseProxy(uparsed)
		primary.rp.Transport = cmn.NewTransport(cmn.TransportArgs{
			UseHTTPS: cmn.GCO.Get().Net.HTTP.UseHTTPS,
		})
	}
	primary.Unlock()
	if len(body) > 0 {
		r.Body = ioutil.NopCloser(bytes.NewBuffer(body))
		r.ContentLength = int64(len(body)) // directly setting content-length
	}
	glog.Infof("%s: forwarding '%s:%s' to the primary %s", p.si.Name(), msg.Action, s, smap.ProxySI)
	primary.rp.ServeHTTP(w, r)
	return true
}

// reverse-proxy request
func (p *proxyrunner) reverseNodeRequest(w http.ResponseWriter, r *http.Request, si *cluster.Snode) {
	parsedURL, err := url.Parse(si.URL(cmn.NetworkPublic))
	cmn.AssertNoErr(err)
	p.reverseRequest(w, r, si.ID(), parsedURL)
}

func (p *proxyrunner) reverseRequest(w http.ResponseWriter, r *http.Request, nodeID string, parsedURL *url.URL) {
	var (
		rproxy *httputil.ReverseProxy
	)

	val, ok := p.rproxy.nodes.Load(nodeID)
	if ok {
		rproxy = val.(*httputil.ReverseProxy)
	} else {
		rproxy = httputil.NewSingleHostReverseProxy(parsedURL)
		rproxy.Transport = cmn.NewTransport(cmn.TransportArgs{
			UseHTTPS: cmn.GCO.Get().Net.HTTP.UseHTTPS,
		})
		p.rproxy.nodes.Store(nodeID, rproxy)
	}

	rproxy.ServeHTTP(w, r)
}

func (p *proxyrunner) copyRenameLB(bucketFrom, bucketTo string, msg *cmn.ActionMsg, config *cmn.Config) (err error) {
	smap := p.smapowner.get()
	bmd := p.bmdowner.get().clone()
	tout := config.Timeout.Default

	bprops, ok := bmd.Get(bucketFrom, true)
	if !ok {
		return fmt.Errorf("local bucket %s %s", bucketFrom, cmn.DoesNotExist)
	}
	if _, ok = bmd.Get(bucketTo, true); ok {
		if msg.Action == cmn.ActRenameLB {
			return fmt.Errorf("%s already exists, cannot rename %s=>%s", bucketTo, bucketFrom, bucketTo)
		}
		// allow to copy into existing bucket
		glog.Warningf("%s already exists, proceeding to %s %s=>%s anyway", bucketTo, msg.Action, bucketFrom, bucketTo)
	}
	// begin
	msgInt := p.newActionMsgInternal(msg, smap, bmd)
	body := cmn.MustMarshal(msgInt)
	results := p.broadcastTo(
		cmn.URLPath(cmn.Version, cmn.Buckets, bucketFrom, cmn.ActBegin),
		nil, http.MethodPost, body, smap, tout, cmn.NetworkIntraControl, cluster.Targets)

	nr := 0
	for res := range results {
		if res.err != nil {
			err = fmt.Errorf("%s: cannot %s bucket %s: %v(%d)", res.si.Name(), msg.Action, bucketFrom, res.err, res.status)
			glog.Errorln(err.Error())
			nr++
		}
	}
	if err != nil {
		// abort
		if nr < len(results) {
			_ = p.broadcastTo(
				cmn.URLPath(cmn.Version, cmn.Buckets, bucketFrom, cmn.ActAbort),
				nil, http.MethodPost, body, smap, tout, cmn.NetworkIntraControl, cluster.Targets)
		}
		return
	}

	// commit
	results = p.broadcastTo(
		cmn.URLPath(cmn.Version, cmn.Buckets, bucketFrom, cmn.ActCommit),
		nil, http.MethodPost, body, smap, tout, cmn.NetworkIntraControl, cluster.Targets)
	for res := range results {
		if res.err != nil {
			err = fmt.Errorf("%s: failed to %s bucket %s: %v(%d)", res.si.Name(), msg.Action, bucketFrom, res.err, res.status)
			glog.Errorln(err.Error())
		}
	}
	if err != nil {
		return
	}

	p.bmdowner.Lock()
	nbmd := p.bmdowner.get().clone()
	nbmd.add(bucketTo, true, bprops)
	p.bmdowner.put(nbmd)
	p.bmdowner.Unlock()

	// if msg.Action == cmn.ActRenameLB {
	// TODO -- FIXME: remove bucketFrom upon completion of the (local, global) rebalancing

	// finalize
	if err = p.savebmdconf(nbmd, config); err != nil {
		glog.Error(err)
	}
	msgInt = p.newActionMsgInternal(msg, nil, nbmd)
	p.metasyncer.sync(true, revspair{nbmd, msgInt})
	glog.Infof("%s local bucket %s => %s, %s v%d", msg.Action, bucketFrom, bucketTo, bmdTermName, nbmd.version())
	return
}

func (p *proxyrunner) makeNCopies(w http.ResponseWriter, r *http.Request, bucket string,
	msg *cmn.ActionMsg, cfg *cmn.Config, bckIsLocal bool) {
	copies, err := p.parseValidateNCopies(msg.Value)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	smap := p.smapowner.get()
	msgInt := p.newActionMsgInternal(msg, smap, nil)
	body := cmn.MustMarshal(msgInt)
	results := p.broadcastTo(
		cmn.URLPath(cmn.Version, cmn.Buckets, bucket),
		nil, // query
		http.MethodPost,
		body,
		smap,
		cfg.Timeout.CplaneOperation,
		cmn.NetworkIntraControl,
		cluster.Targets,
	)
	for res := range results {
		if res.err != nil {
			msg := fmt.Sprintf("Failed to make-num-copies: %s, bucket %s, err: %v(%d)",
				res.si.Name(), bucket, res.err, res.status)
			if res.details != "" {
				glog.Errorln(res.details)
			}
			p.invalmsghdlr(w, r, msg)
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
	if err := p.updateBucketProps(bucket, bckIsLocal, nvs); err != nil {
		p.invalmsghdlr(w, r, err.Error())
	}
}

func (p *proxyrunner) getbucketnames(w http.ResponseWriter, r *http.Request, bckProvider string) {
	bmd := p.bmdowner.get()
	bckProviderStr := "?" + cmn.URLParamBckProvider + "=" + bckProvider
	if cmn.IsProviderLocal(bckProvider) {
		bucketNames := &cmn.BucketNames{Cloud: []string{}, Local: make([]string, 0, 64)}
		for bucket := range bmd.LBmap {
			bucketNames.Local = append(bucketNames.Local, bucket)
		}
		body := cmn.MustMarshal(bucketNames)
		p.writeJSON(w, r, body, "getbucketnames"+bckProviderStr)
		return
	}

	var si *cluster.Snode
	smap := p.smapowner.get()
	for _, si = range smap.Tmap {
		break
	}
	args := callArgs{
		si: si,
		req: cmn.ReqArgs{
			Method: r.Method,
			Path:   cmn.URLPath(cmn.Version, cmn.Buckets, cmn.ListAll),
			Query:  r.URL.Query(),
			Header: r.Header,
		},
		timeout: defaultTimeout,
	}
	res := p.call(args)
	if res.err != nil {
		p.invalmsghdlr(w, r, res.details)
		p.keepalive.onerr(res.err, res.status)
	} else {
		p.writeJSON(w, r, res.outjson, "getbucketnames"+bckProviderStr)
	}
}

func (p *proxyrunner) redirectURL(r *http.Request, si *cluster.Snode, ts time.Time, netName string) (redirect string) {
	var (
		nodeURL string
		query   = url.Values{}
		bmd     = p.bmdowner.get()
	)
	if p.si.LocalNet == nil {
		nodeURL = si.URL(cmn.NetworkPublic)
	} else {
		var local bool
		remote := r.RemoteAddr
		if colon := strings.Index(remote, ":"); colon != -1 {
			remote = remote[:colon]
		}
		if ip := net.ParseIP(remote); ip != nil {
			local = p.si.LocalNet.Contains(ip)
		}
		if local {
			nodeURL = si.URL(netName)
		} else {
			nodeURL = si.URL(cmn.NetworkPublic)
		}
	}
	redirect = nodeURL + r.URL.Path + "?"
	if r.URL.RawQuery != "" {
		redirect += r.URL.RawQuery + "&"
	}

	query.Add(cmn.URLParamProxyID, p.si.DaemonID)
	query.Add(cmn.URLParamBMDVersion, bmd.vstr)
	query.Add(cmn.URLParamUnixTime, strconv.FormatInt(ts.UnixNano(), 10))
	redirect += query.Encode()
	return
}

func (p *proxyrunner) doesCloudBucketExist(bucket string) (bool, error) {
	var si *cluster.Snode
	// Use random map iteration order to choose a random target to ask
	smap := p.smapowner.get()
	for _, si = range smap.Tmap {
		break
	}

	args := callArgs{
		si: si,
		req: cmn.ReqArgs{
			Method: http.MethodHead,
			Base:   si.URL(cmn.NetworkIntraData),
			Path:   cmn.URLPath(cmn.Version, cmn.Buckets, bucket),
		},
		timeout: defaultTimeout,
	}

	res := p.call(args)

	if res.err != nil && res.status != http.StatusNotFound {
		return false, fmt.Errorf("failed to check if bucket exists contacting %s: %v", si.URL(cmn.NetworkIntraData), res.err)
	}

	return res.status != http.StatusNotFound, nil
}

func (p *proxyrunner) initBckListQuery(headerID string, msg *cmn.SelectMsg) (bool, url.Values, error) {
	isNew := headerID == "" && msg.TaskID == 0
	q := url.Values{}
	if isNew {
		msg.TaskID, _ = cmn.GenUUID64()
		q.Set(cmn.URLParamTaskAction, cmn.ListTaskStart)
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("proxy: starting new async task %d", msg.TaskID)
		}
	} else {
		// make both msg and headers to have the same taskID
		if msg.TaskID == 0 {
			n, err := strconv.ParseInt(headerID, 10, 64)
			if err != nil {
				return false, q, err
			}
			msg.TaskID = n
		}
		// first request is always 'Status' to avoid wasting gigabytes of
		// traffic in case when few targets have finished their tasks
		q.Set(cmn.URLParamTaskAction, cmn.ListTaskStatus)
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("proxy: reading async task %d result", msg.TaskID)
		}
	}

	return isNew, q, nil
}

func (p *proxyrunner) checkBckTaskResp(taskID int64, results chan callResult) (allOk bool, err error) {
	// check response codes of all targets
	// Target that has completed its async task returns 200, and 202 otherwise
	allOK, allNotFound := true, true
	for res := range results {
		if res.status == http.StatusNotFound {
			continue
		}
		allNotFound = false
		if res.err != nil {
			return false, res.err
		}
		if res.status != http.StatusOK {
			allOK = false
			break
		}
	}

	if allNotFound {
		err = fmt.Errorf("Task %d %s", taskID, cmn.DoesNotExist)
	}

	return allOK, err
}

func (p *proxyrunner) getLocalBucketObjects(bucket string, msg cmn.SelectMsg, headerID string) (allEntries *cmn.BucketList, code int64, err error) {
	pageSize := cmn.DefaultPageSize
	if msg.PageSize != 0 {
		pageSize = msg.PageSize
	}

	// if a client does not provide taskID(neither in Headers nor in SelectMsg),
	// it is a request to start a new async task
	isNew, q, err := p.initBckListQuery(headerID, &msg)
	if err != nil {
		return nil, 0, err
	}

	smap := p.smapowner.get()
	reqTimeout := cmn.GCO.Get().Timeout.ListBucket
	urlPath := cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	method := http.MethodPost
	msgInt := p.newActionMsgInternal(&cmn.ActionMsg{Action: cmn.ActListObjects, Value: &msg}, nil, nil)
	msgBytes := cmn.MustMarshal(msgInt)
	results := p.broadcastTo(
		urlPath,
		q,
		method,
		msgBytes,
		smap,
		reqTimeout,
		cmn.NetworkPublic,
		cluster.Targets,
	)

	allOK, err := p.checkBckTaskResp(msg.TaskID, results)
	if err != nil {
		return nil, 0, err
	}

	// some targets are still executing their tasks or it is request to start
	// an async task. The proxy returns only taskID to a caller
	if !allOK || isNew {
		return nil, msg.TaskID, nil
	}

	// all targets are ready, prepare the final result
	q = url.Values{}
	q.Set(cmn.URLParamTaskAction, cmn.ListTaskResult)
	q.Set(cmn.URLParamSilent, "true")
	results = p.broadcastTo(
		urlPath,
		q,
		method,
		msgBytes,
		smap,
		reqTimeout,
		cmn.NetworkPublic,
		cluster.Targets,
	)

	// combine results
	allEntries = &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, pageSize)}
	for res := range results {
		if res.err != nil {
			return nil, 0, res.err
		}
		if len(res.outjson) == 0 {
			continue
		}
		bucketList := &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, pageSize)}
		if err = jsoniter.Unmarshal(res.outjson, &bucketList); err != nil {
			return
		}
		res.outjson = nil

		if len(bucketList.Entries) == 0 {
			continue
		}

		allEntries.Entries = append(allEntries.Entries, bucketList.Entries...)
		bucketList.Entries = nil
		bucketList = nil
	}

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
				return allEntries.Entries[i].Flags&cmn.EntryStatusMask < allEntries.Entries[j].Flags&cmn.EntryStatusMask
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

	// no active tasks - return TaskID=0
	return allEntries, 0, nil
}

func (p *proxyrunner) getCloudBucketObjects(r *http.Request, bucket, headerID string,
	msg cmn.SelectMsg) (allEntries *cmn.BucketList, code int64, err error) {
	useCache, _ := cmn.ParseBool(r.URL.Query().Get(cmn.URLParamCached))
	if msg.PageSize > maxPageSize {
		glog.Warningf("Page size(%d) for cloud bucket %s exceeds the limit(%d)", msg.PageSize, bucket, maxPageSize)
	}
	pageSize := msg.PageSize
	if pageSize == 0 {
		pageSize = cmn.DefaultPageSize
	}

	// if a client does not provide taskID(neither in Headers nor in SelectMsg),
	// it is a request to start a new async task
	isNew, q, err := p.initBckListQuery(headerID, &msg)
	if err != nil {
		return nil, 0, err
	}
	if useCache {
		q.Set(cmn.URLParamCached, "true")
	}
	q.Set(cmn.URLParamBckProvider, cmn.CloudBs)

	smap := p.smapowner.get()
	reqTimeout := cmn.GCO.Get().Timeout.ListBucket
	urlPath := cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	method := http.MethodPost
	msgInt := p.newActionMsgInternal(&cmn.ActionMsg{Action: cmn.ActListObjects, Value: &msg}, nil, nil)
	msgBytes := cmn.MustMarshal(msgInt)
	needLocalData := msg.NeedLocalData()

	var results chan callResult
	if needLocalData || !isNew {
		q.Set(cmn.URLParamSilent, "true")
		results = p.broadcastTo(
			urlPath,
			q,
			method,
			msgBytes,
			smap,
			reqTimeout,
			cmn.NetworkPublic,
			cluster.Targets,
		)
	} else {
		// only cloud options are requested - call one random target for data
		for _, si := range smap.Tmap {
			bcastArgs := bcastCallArgs{
				req: cmn.ReqArgs{
					Method: method,
					Path:   urlPath,
					Query:  q,
					Body:   msgBytes,
				},
				network: cmn.NetworkPublic,
				timeout: reqTimeout,
				nodes:   []cluster.NodeMap{{si.DaemonID: si}},
			}
			results = p.broadcast(bcastArgs)
			break
		}
	}

	allOK, err := p.checkBckTaskResp(msg.TaskID, results)
	if err != nil {
		return nil, 0, err
	}

	// some targets are still executing their tasks or it is request to start
	// an async task. The proxy returns only taskID to a caller
	if !allOK || isNew {
		return nil, msg.TaskID, nil
	}

	// all targets are ready, prepare the final result
	q = url.Values{}
	q.Set(cmn.URLParamTaskAction, cmn.ListTaskResult)
	q.Set(cmn.URLParamSilent, "true")
	q.Set(cmn.URLParamBckProvider, cmn.CloudBs)
	if useCache {
		q.Set(cmn.URLParamCached, "true")
	}
	results = p.broadcastTo(
		urlPath,
		q,
		method,
		msgBytes,
		smap,
		reqTimeout,
		cmn.NetworkPublic,
		cluster.Targets,
	)

	// combine results
	allEntries = &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, pageSize)}
	var pageMarker string
	for res := range results {
		if res.status == http.StatusNotFound {
			continue
		}
		if res.err != nil {
			return nil, 0, res.err
		}
		if len(res.outjson) == 0 {
			continue
		}
		bucketList := &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, pageSize)}
		if err = jsoniter.Unmarshal(res.outjson, bucketList); err != nil {
			return
		}
		res.outjson = nil

		if len(bucketList.Entries) == 0 {
			continue
		}

		if useCache {
			allEntries.Entries, pageMarker = objwalk.ConcatObjLists(
				[][]*cmn.BucketEntry{allEntries.Entries, bucketList.Entries}, pageSize)
		} else {
			allEntries, pageMarker = objwalk.MergeObjLists(
				[]*cmn.BucketList{allEntries, bucketList}, pageSize)
		}

		bucketList.Entries = bucketList.Entries[:0]
		bucketList.Entries = nil
		bucketList = nil
	}
	allEntries.PageMarker = pageMarker

	if msg.WantProp(cmn.GetTargetURL) {
		for _, e := range allEntries.Entries {
			si, err := cluster.HrwTarget(bucket, e.Name, &smap.Smap)
			if err == nil {
				e.TargetURL = si.URL(cmn.NetworkPublic)
			}
		}
	}

	// no active tasks - return TaskID=0
	return allEntries, 0, nil
}

// Local bucket:
//   - reads object list from all targets, combines, sorts and returns the list
// Cloud bucket:
//   - selects a random target to read the list of objects from cloud
//   - if iscached or atime property is requested it does extra steps:
//      * get list of cached files info from all targets
//      * updates the list of objects from the cloud with cached info
//   - returns:
//      * the list of objects if the aync task finished (taskID is 0 in this case)
//      * non-zero taskID if the task is still running
//      * error
func (p *proxyrunner) listBucket(r *http.Request, bucket, bckProvider string, msg cmn.SelectMsg) (bckList *cmn.BucketList, taskID int64, err error) {
	headerID := r.URL.Query().Get(cmn.URLParamTaskID)
	if cmn.IsProviderCloud(bckProvider) || !p.bmdowner.get().IsLocal(bucket) {
		bckList, taskID, err = p.getCloudBucketObjects(r, bucket, headerID, msg)
	} else {
		bckList, taskID, err = p.getLocalBucketObjects(bucket, msg, headerID)
	}
	return
}

func (p *proxyrunner) savebmdconf(bmd *bucketMD, config *cmn.Config) (err error) {
	bmdFullPath := filepath.Join(config.Confdir, cmn.BucketmdBackupFile)
	if err := cmn.LocalSave(bmdFullPath, bmd); err != nil {
		return fmt.Errorf("failed to store %s at %s, err: %v", bmdTermName, bmdFullPath, err)
	}
	return nil
}

func (p *proxyrunner) objRename(w http.ResponseWriter, r *http.Request) {
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
	si, err := cluster.HrwTarget(lbucket, objname, &smap.Smap)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("RENAME %s %s/%s => %s", r.Method, lbucket, objname, si)
	}

	// NOTE:
	//       code 307 is the only way to http-redirect with the
	//       original JSON payload (SelectMsg - see pkg/api/constant.go)
	redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)

	p.statsif.Add(stats.RenameCount, 1)
}

func (p *proxyrunner) listRangeHandler(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg, method, bckProvider string) {
	var (
		err   error
		query = r.URL.Query()
	)

	apitems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	if bmd, _ := p.validateBucket(w, r, bucket, bckProvider); bmd == nil {
		return
	}
	if err := p.listRange(method, bucket, msg, query); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
}

func (p *proxyrunner) listRange(method, bucket string, msg *cmn.ActionMsg, query url.Values) error {
	var (
		timeout time.Duration
		results chan callResult
	)

	wait := false
	if jsmap, ok := msg.Value.(map[string]interface{}); !ok {
		return fmt.Errorf("failed to unmarshal JSMAP: Not a map[string]interface")
	} else if waitstr, ok := jsmap["wait"]; ok {
		if wait, ok = waitstr.(bool); !ok {
			return fmt.Errorf("failed to read ListRangeMsgBase Wait: Not a bool")
		}
	}
	// Send json message to all
	smap := p.smapowner.get()
	bmd := p.bmdowner.get()
	msgInt := p.newActionMsgInternal(msg, smap, bmd)
	body := cmn.MustMarshal(msgInt)
	if wait {
		timeout = cmn.GCO.Get().Timeout.ListBucket
	} else {
		timeout = defaultTimeout
	}

	results = p.broadcastTo(
		cmn.URLPath(cmn.Version, cmn.Buckets, bucket),
		query,
		method,
		body,
		smap,
		timeout,
		cmn.NetworkIntraData,
		cluster.Targets,
	)
	for res := range results {
		if res.err != nil {
			return fmt.Errorf("failed to List/Range: %v (%d: %s)", res.err, res.status, res.details)
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

func (p *proxyrunner) reverseHandler(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Reverse)
	if err != nil {
		return
	}

	// rewrite URL path (removing `cmn.Reverse`)
	r.URL.Path = cmn.URLPath(cmn.Version, apiItems[0])

	nodeID := r.Header.Get(cmn.HeaderNodeID)
	if nodeID == "" {
		p.invalmsghdlr(w, r, "node_id was not provided in header")
		return
	}
	si := p.smapowner.get().GetNode(nodeID)
	if si != nil {
		p.reverseNodeRequest(w, r, si)
		return
	}

	// Node not found but maybe we could contact it directly. This is for
	// special case where we need to contact target which is not part of the
	// cluster eg. mountpaths: when target is not part of the cluster after
	// removing all mountpaths.
	nodeURL := r.Header.Get(cmn.HeaderNodeURL)
	if nodeURL == "" {
		msg := fmt.Sprintf("node with provided id (%s) does not exists", nodeID)
		p.invalmsghdlr(w, r, msg)
		return
	}

	parsedURL, err := url.Parse(nodeURL)
	if err != nil {
		msg := fmt.Sprintf("provided node_url is invalid: %s", nodeURL)
		p.invalmsghdlr(w, r, msg)
		return
	}

	p.reverseRequest(w, r, nodeID, parsedURL)
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
		pst := getproxystatsrunner()
		body := pst.GetWhatStats()
		p.writeJSON(w, r, body, httpdaeWhat)
	case cmn.GetWhatSysInfo:
		body := cmn.MustMarshal(nodeCtx.mm.FetchSysInfo())
		p.writeJSON(w, r, body, httpdaeWhat)
	case cmn.GetWhatSmap:
		smap := p.smapowner.get()
		for smap == nil || !smap.isValid() {
			if p.startedUp.Load() { // must be starting up
				smap = p.smapowner.get()
				cmn.Assert(smap.isValid())
				break
			}
			glog.Errorf("%s is starting up: cannot execute GET %s yet...", p.si.Name(), cmn.GetWhatSmap)
			time.Sleep(time.Second)
			smap = p.smapowner.get()
		}
		body := cmn.MustMarshal(smap)
		p.writeJSON(w, r, body, httpdaeWhat)
	case cmn.GetWhatDaemonStatus:
		pst := getproxystatsrunner()
		msg := &stats.DaemonStatus{
			Snode:       p.httprunner.si,
			SmapVersion: p.smapowner.get().Version,
			SysInfo:     nodeCtx.mm.FetchSysInfo(),
			Stats:       pst.Core,
		}
		body := cmn.MustMarshal(msg)
		p.writeJSON(w, r, body, httpdaeWhat)
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
			if err := p.smapowner.synchronize(newsmap, true /*saveSmap*/, true /* lesserIsErr */); err != nil {
				p.invalmsghdlr(w, r, err.Error())
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
		force, _ := cmn.ParseBool(q.Get(cmn.URLParamForce))
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

func (p *proxyrunner) smapFromURL(baseURL string) (smap *smapX, err error) {
	query := url.Values{}
	query.Add(cmn.URLParamWhat, cmn.GetWhatSmap)
	req := cmn.ReqArgs{
		Method: http.MethodGet,
		Base:   baseURL,
		Path:   cmn.URLPath(cmn.Version, cmn.Daemon),
		Query:  query,
	}
	args := callArgs{req: req, timeout: defaultTimeout}
	res := p.call(args)
	if res.err != nil {
		return nil, fmt.Errorf("failed to get smap from %s: %v", baseURL, res.err)
	}
	smap = &smapX{}
	if err := jsoniter.Unmarshal(res.outjson, smap); err != nil {
		return nil, fmt.Errorf("failed to unmarshal smap: %v", err)
	}
	if !smap.isValid() {
		return nil, fmt.Errorf("invalid Smap from %s: %s", baseURL, smap.pp())
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
	newSmap, err := p.smapFromURL(newPrimaryURL)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
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
	res := p.registerToURL(newSmap.ProxySI.IntraControlNet.DirectURL, newSmap.ProxySI, defaultTimeout, nil, false)
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
	force, _ := cmn.ParseBool(r.URL.Query().Get(cmn.URLParamForce))
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
	if prepare, err = cmn.ParseBool(preparestr); err != nil {
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
			if err := p.becomeNewPrimary(""); err != nil {
				p.invalmsghdlr(w, r, err.Error())
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
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Info("Preparation step: do nothing")
		}
		return
	}
	p.smapowner.Lock()
	clone := smap.clone()
	clone.ProxySI = psi
	if err := p.smapowner.persist(clone, true); err != nil {
		p.smapowner.Unlock()
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	p.smapowner.put(clone)
	p.smapowner.Unlock()
}

func (p *proxyrunner) becomeNewPrimary(proxyIDToRemove string) (err error) {
	p.smapowner.Lock()
	smap := p.smapowner.get()
	if !smap.isPresent(p.si) {
		cmn.AssertMsg(false, "This proxy '"+p.si.DaemonID+"' must always be present in the local "+smap.pp())
	}
	clone := smap.clone()
	// FIXME: may be premature at this point
	if proxyIDToRemove != "" {
		glog.Infof("Removing failed primary proxy %s", proxyIDToRemove)
		clone.delProxy(proxyIDToRemove)
	}

	clone.ProxySI = p.si
	clone.Version += 100
	if err = p.smapowner.persist(clone, true); err != nil {
		p.smapowner.Unlock()
		glog.Error(err)
		return
	}
	p.smapowner.put(clone)
	p.smapowner.Unlock()

	bmd := p.bmdowner.get()
	if glog.V(3) {
		glog.Infof("Distributing Smap v%d with the newly elected primary %s(self)", clone.version(), p.si.Name())
		glog.Infof("Distributing %s v%d as well", bmdTermName, bmd.version())
	}
	msgInt := p.newActionMsgInternalStr(cmn.ActNewPrimary, clone, nil)
	p.metasyncer.sync(true, revspair{clone, msgInt}, revspair{bmd, msgInt})
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
	for res := range results {
		if res.err != nil {
			s := fmt.Sprintf("Failed to set primary %s: err %v from %s in the prepare phase", proxyid, res.err, res.si)
			p.invalmsghdlr(w, r, s)
			return
		}
	}

	// (I.5) commit locally
	p.smapowner.Lock()
	clone := p.smapowner.get().clone()
	clone.ProxySI = psi
	p.metasyncer.becomeNonPrimary()
	if err := p.smapowner.persist(clone, true); err != nil {
		glog.Errorf("Failed to save Smap locally after having transitioned to non-primary:\n%s", err)
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
	for res := range results {
		if res.err != nil {
			if res.si.DaemonID == proxyid {
				glog.Fatalf("Commit phase failure: new primary %s returned err %v", proxyid, res.err)
			} else {
				glog.Errorf("Commit phase failure: %s returned err %v when setting primary = %s",
					res.si.DaemonID, res.err, proxyid)
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
	if glog.FastV(4, glog.SmoduleAIS) {
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
		if glog.FastV(4, glog.SmoduleAIS) {
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
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("Updated JSON URL Path: %s", r.URL.Path)
		}
	} else if glog.FastV(4, glog.SmoduleAIS) {
		// original url looks like - no cleanup required:
		//    http://storage.googleapis.com/<bucket>/<object>
		glog.Infof("XML GCP request for object: %v", s)
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
	results, ok := p.invokeHTTPSelectMsgOnTargets(w, r, true /*silent*/)
	if !ok {
		return false
	}

	body := cmn.MustMarshal(results)
	return p.writeJSON(w, r, body, "getXaction")
}

func (p *proxyrunner) invokeHTTPSelectMsgOnTargets(w http.ResponseWriter, r *http.Request,
	silent bool) (map[string]jsoniter.RawMessage, bool) {
	smapX := p.smapowner.get()

	var (
		msgBody []byte
		err     error
	)

	if r.Body != nil {
		msgBody, err = cmn.ReadBytes(r)
		if err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return nil, false
		}
	}

	results := p.broadcastTo(
		cmn.URLPath(cmn.Version, cmn.Daemon),
		r.URL.Query(),
		r.Method,
		msgBody, // message
		smapX,
		cmn.GCO.Get().Timeout.Default,
		cmn.NetworkIntraControl,
		cluster.Targets,
	)
	targetResults := make(map[string]jsoniter.RawMessage, smapX.CountTargets())
	for res := range results {
		if res.err != nil {
			if silent {
				p.invalmsghdlrsilent(w, r, res.details)
			} else {
				p.invalmsghdlr(w, r, res.details)
			}
			return nil, false
		}
		targetResults[res.si.DaemonID] = jsoniter.RawMessage(res.outjson)
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
		for res := range results {
			if res.err != nil {
				return nil, res.details
			}
			sysInfoMap[res.si.DaemonID] = jsoniter.RawMessage(res.outjson)
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

	body := cmn.MustMarshal(out)
	ok := p.writeJSON(w, r, body, "HttpGetClusterSysInfo")
	return ok
}

// FIXME: read-lock
func (p *proxyrunner) invokeHTTPGetClusterStats(w http.ResponseWriter, r *http.Request) bool {
	targetStats, ok := p.invokeHTTPSelectMsgOnTargets(w, r, false /*not silent*/)
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
	body := cmn.MustMarshal(out)
	ok = p.writeJSON(w, r, body, "HttpGetClusterStats")
	return ok
}

func (p *proxyrunner) invokeHTTPGetClusterMountpaths(w http.ResponseWriter, r *http.Request) bool {
	targetMountpaths, ok := p.invokeHTTPSelectMsgOnTargets(w, r, false /*not silent*/)
	if !ok {
		errstr := fmt.Sprintf(
			"Unable to invoke cmn.SelectMsg on targets. Query: [%s]", r.URL.RawQuery)
		glog.Errorf(errstr)
		p.invalmsghdlr(w, r, errstr)
		return false
	}

	out := &ClusterMountpathsRaw{}
	out.Targets = targetMountpaths
	body := cmn.MustMarshal(out)
	ok = p.writeJSON(w, r, body, "HttpGetClusterMountpaths")
	return ok
}

// register|keepalive target|proxy
func (p *proxyrunner) httpclupost(w http.ResponseWriter, r *http.Request) {
	var (
		regReq                            targetRegMeta
		keepalive, register, selfRegister bool
		nonElectable                      bool
	)
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Cluster)
	if err != nil {
		return
	}

	switch apiItems[0] {
	case cmn.Register: // manual by user (API)
		if cmn.ReadJSON(w, r, &regReq.SI) != nil {
			return
		}
		register = true
	case cmn.Keepalive:
		if cmn.ReadJSON(w, r, &regReq) != nil {
			return
		}
		keepalive = true
	case cmn.AutoRegister: // node self-register
		if cmn.ReadJSON(w, r, &regReq) != nil {
			return
		}
		selfRegister = true
	default:
		p.invalmsghdlr(w, r, fmt.Sprintf("invalid path specified: %q", apiItems[0]))
		return
	}

	nsi := regReq.SI
	if err := nsi.Validate(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if regReq.BMD != nil && glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("Received %s from %s(%s) on registering:\n%s]",
			bmdTermName, nsi.DaemonID, nsi.DaemonType, regReq.BMD.Dump())
	}

	isProxy := nsi.IsProxy()
	if isProxy {
		s := r.URL.Query().Get(cmn.URLParamNonElectable)
		if nonElectable, err = cmn.ParseBool(s); err != nil {
			glog.Errorf("Failed to parse %s for non-electability: %v", s, err)
		}
	}

	s := fmt.Sprintf("register %s (keepalive=%t, register=%t, self-register=%t)", nsi, keepalive, register, selfRegister)
	msg := &cmn.ActionMsg{Action: cmn.ActRegTarget}
	if isProxy {
		msg = &cmn.ActionMsg{Action: cmn.ActRegProxy}
	}
	body := cmn.MustMarshal(regReq)
	if p.forwardCP(w, r, msg, s, body) {
		return
	}
	if net.ParseIP(nsi.PublicNet.NodeIPAddr) == nil {
		s := fmt.Sprintf("register %s: invalid IP address %v", nsi.Name(), nsi.PublicNet.NodeIPAddr)
		p.invalmsghdlr(w, r, s)
		return
	}

	p.statsif.Add(stats.PostCount, 1)

	p.smapowner.Lock()
	smap := p.smapowner.get()
	if isProxy {
		osi := smap.GetProxy(nsi.DaemonID)
		if !p.addOrUpdateNode(nsi, osi, keepalive) {
			p.smapowner.Unlock()
			return
		}
	} else {
		osi := smap.GetTarget(nsi.DaemonID)

		// FIXME: If not keepalive then update smap anyway - either: new target,
		// updated target or target has powercycled. Except 'updated target' case,
		// rebalance needs to be triggered and updating smap is the easiest way.
		if keepalive && !p.addOrUpdateNode(nsi, osi, keepalive) {
			p.smapowner.Unlock()
			return
		}
		if register {
			if glog.V(3) {
				glog.Infof("register %s (num targets before %d)", nsi.Name(), smap.CountTargets())
			}
			args := callArgs{
				si: nsi,
				req: cmn.ReqArgs{
					Method: http.MethodPost,
					Path:   cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Register),
				},
				timeout: cmn.GCO.Get().Timeout.CplaneOperation,
			}
			res := p.call(args)
			if res.err != nil {
				p.smapowner.Unlock()
				errstr := fmt.Sprintf("Failed to register %s: %v, %s", nsi.Name(), res.err, res.details)
				p.invalmsghdlr(w, r, errstr, res.status)
				return
			}
		}
	}
	if !p.startedUp.Load() {
		clone := p.smapowner.get().clone()
		clone.putNode(nsi, nonElectable)
		p.smapowner.put(clone)
		p.smapowner.Unlock()
		return
	}
	p.smapowner.Unlock()

	// Return current metadata to registering target
	if !isProxy && selfRegister { // case: self-registering target
		bmd := p.bmdowner.get()
		meta := &targetRegMeta{smap, bmd, p.si}
		body := cmn.MustMarshal(meta)
		p.writeJSON(w, r, body, path.Join(cmn.ActRegTarget, nsi.DaemonID) /* tag */)
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
		if err := p.smapowner.persist(clone, true); err != nil {
			glog.Error(err)
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
	}(nsi)
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
		if !p.startedUp.Load() {
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
	apitems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Cluster, cmn.Daemon)
	if err != nil {
		return
	}

	var (
		msg  *cmn.ActionMsg
		sid  = apitems[0]
		smap = p.smapowner.get()
		node = smap.GetNode(sid)
	)

	if node == nil {
		errstr := fmt.Sprintf("Unknown node %s", sid)
		p.invalmsghdlr(w, r, errstr, http.StatusNotFound)
		return
	}

	msg = &cmn.ActionMsg{Action: cmn.ActUnregTarget}
	if node.IsProxy() {
		msg = &cmn.ActionMsg{Action: cmn.ActUnregProxy}
	}
	if p.forwardCP(w, r, msg, sid, nil) {
		return
	}

	p.smapowner.Lock()

	smap = p.smapowner.get()
	clone := smap.clone()
	node = smap.GetNode(sid)
	if node == nil {
		errstr := fmt.Sprintf("Unknown node %s", sid)
		p.invalmsghdlr(w, r, errstr, http.StatusNotFound)
		return
	}

	if node.IsProxy() {
		clone.delProxy(sid)
		if glog.V(3) {
			glog.Infof("unregistered %s (num proxies %d)", node.Name(), clone.CountProxies())
		}
	} else {
		clone.delTarget(sid)
		if glog.V(3) {
			glog.Infof("unregistered %s (num targets %d)", node.Name(), clone.CountTargets())
		}
		args := callArgs{
			si: node,
			req: cmn.ReqArgs{
				Method: http.MethodDelete,
				Path:   cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Unregister),
			},
			timeout: cmn.GCO.Get().Timeout.CplaneOperation,
		}
		res := p.call(args)
		if res.err != nil {
			glog.Warningf("%s that is being unregistered failed to respond back: %v, %s", node.Name(), res.err, res.details)
		}
	}
	if !p.startedUp.Load() {
		p.smapowner.put(clone)
		p.smapowner.Unlock()
		return
	}
	if err := p.smapowner.persist(clone, true); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		p.smapowner.Unlock()
		return
	}
	p.smapowner.put(clone)
	p.smapowner.Unlock()

	if isPrimary := p.smapowner.get().isPrimary(p.si); !isPrimary {
		return
	}

	msgInt := p.newActionMsgInternal(msg, clone, nil)
	p.metasyncer.sync(true, revspair{clone, msgInt})
}

// '{"action": "shutdown"}' /v1/cluster => (proxy) =>
// '{"action": "syncsmap"}' /v1/cluster => (proxy) => PUT '{Smap}' /v1/daemon/syncsmap => target(s)
// '{"action": cmn.ActGlobalReb}' /v1/cluster => (proxy) => PUT '{Smap}' /v1/daemon/rebalance => target(s)
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
			for res := range results {
				if res.err != nil {
					p.invalmsghdlr(w, r, res.err.Error())
					p.keepalive.onerr(err, res.status)
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

		body := cmn.MustMarshal(msg) // same message -> all targets and proxies
		results := p.broadcastTo(
			cmn.URLPath(cmn.Version, cmn.Daemon),
			nil, // query
			http.MethodPut,
			body,
			p.smapowner.get(),
			defaultTimeout,
			cmn.NetworkIntraControl,
			cluster.AllNodes,
		)
		for res := range results {
			if res.err != nil {
				p.invalmsghdlr(
					w,
					r,
					fmt.Sprintf("%s: (%s = %s) failed, err: %s", msg.Action, msg.Name, value, res.details),
				)
				p.keepalive.onerr(err, res.status)
				return
			}
		}

	case cmn.ActShutdown:
		glog.Infoln("Proxy-controlled cluster shutdown...")
		body := cmn.MustMarshal(msg) // same message -> all targets
		p.broadcastTo(
			cmn.URLPath(cmn.Version, cmn.Daemon),
			nil, // query
			http.MethodPut,
			body,
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
func (p *proxyrunner) receiveBucketMD(newBMD *bucketMD, msgInt *actionMsgInternal) (err error) {
	if glog.V(3) {
		s := fmt.Sprintf("receive %s: v%d", bmdTermName, newBMD.version())
		if msgInt.Action == "" {
			glog.Infoln(s)
		} else {
			glog.Infof("%s, action %s", s, msgInt.Action)
		}
	}
	p.bmdowner.Lock()
	curVersion := p.bmdowner.get().version()
	if newBMD.version() <= curVersion {
		p.bmdowner.Unlock()
		if newBMD.version() < curVersion {
			err = fmt.Errorf("attempt to downgrade %s v%d to v%d", bmdTermName, curVersion, newBMD.version())
		}
		return
	}
	p.bmdowner.put(newBMD)
	if err := p.savebmdconf(newBMD, cmn.GCO.Get()); err != nil {
		glog.Error(err)
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

// detectDaemonDuplicate queries osi for its daemon info in order to determine if info has changed
// and is equal to nsi
func (p *proxyrunner) detectDaemonDuplicate(osi *cluster.Snode, nsi *cluster.Snode) bool {
	query := url.Values{}
	query.Add(cmn.URLParamWhat, cmn.GetWhatSnode)
	args := callArgs{
		si: osi,
		req: cmn.ReqArgs{
			Method: http.MethodGet,
			Path:   cmn.URLPath(cmn.Version, cmn.Daemon),
			Query:  query,
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

// User request to recover buckets found on all targets. On primary size:
// 1. Broadcasts request to get all bucket lists stored in xattrs
// 2. Sorts results by BMD version in descending order
// 3. Force=true: use BMD with highest version number as new BMD
//    Force=false: use targets' BMD only if they are of the same version
// 4. Set primary's BMD version to be greater than any target's one
// 4. Metasync the merged BMD
func (p *proxyrunner) recoverBuckets(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg) {
	force, _ := cmn.ParseBool(r.URL.Query().Get(cmn.URLParamForce))
	query := url.Values{}
	query.Add(cmn.URLParamWhat, cmn.GetWhatBucketMetaX)
	smapX := p.smapowner.get()

	// request all targets for their BMD saved to xattrs
	results := p.broadcastTo(
		cmn.URLPath(cmn.Version, cmn.Buckets, cmn.ListAll),
		query,
		http.MethodGet,
		nil, // message
		smapX,
		cmn.GCO.Get().Timeout.Default,
		cmn.NetworkIntraControl,
		cluster.Targets,
	)

	bmdList := make([]*bucketMD, 0, len(results))
	for res := range results {
		if res.err != nil || res.status >= http.StatusBadRequest {
			glog.Errorf("failed to execute list buckets request: %v (%d: %s)", res.err, res.status, res.details)
			continue
		}
		bmd := &bucketMD{}
		err := jsoniter.Unmarshal(res.outjson, bmd)
		if err != nil {
			glog.Errorf("Failed to unmarshal %s from %s: %v", bmdTermName, res.si.DaemonID, err)
			continue
		}
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("Received %s from %s (version %d):\n%s", bmdTermName, res.si.DaemonID, bmd.Version, string(res.outjson))
		}

		bmdList = append(bmdList, bmd)
	}

	bmdLen := len(bmdList)
	if bmdLen == 0 {
		p.invalmsghdlr(w, r, "No bucket metadata found on targets")
		return
	}

	// sort in descending order by BMD.Version
	sort.Slice(bmdList, func(i, j int) bool {
		return bmdList[i].Version > bmdList[j].Version
	})

	// without force all targets must have BMD of the same version
	if !force && bmdLen > 1 && bmdList[0].Version != bmdList[bmdLen-1].Version {
		p.invalmsghdlr(w, r, "Targets have different bucket metadata versions")
		return
	}

	// now take the bucket metadata with highest version
	clone := bmdList[0]
	// set BMD version greater than the highest target's BMD
	clone.Version += 100
	p.bmdowner.Lock()
	p.bmdowner.put(clone)
	p.bmdowner.Unlock()

	msgInt := p.newActionMsgInternal(msg, nil, clone)
	p.metasyncer.sync(true, revspair{clone, msgInt})
}
