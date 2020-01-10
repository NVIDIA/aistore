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
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/objwalk"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xaction"
	jsoniter "github.com/json-iterator/go"
)

const (
	whatRenamedLB = "renamedlb"
	fmtNotCloud   = "%q appears to be ais bucket (expecting cloud)"
	fmtUnknownAct = "unexpected action message <- JSON [%v]"
	guestError    = "guest account does not have permissions for the operation"
	ciePrefix     = "cluster integrity error: cie#"
	githubHome    = "https://github.com/NVIDIA/aistore"
)

type (
	ClusterMountpathsRaw struct {
		Targets cmn.JSONRawMsgs `json:"targets"`
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
		globRebID  int64
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
	p.bmdowner = newBMDOwnerPrx(config)

	p.httprunner.keepalive = getproxykeepalive()

	p.bmdowner.init() // initialize owner and load BMD
	p.metasyncer = getmetasyncer()

	// startup sequence - see earlystart.go for the steps and commentary
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
	dsortHandler, downloadHandler := dsort.ProxySortHandler, p.downloadHandler
	if config.Auth.Enabled {
		bucketHandler, objectHandler = wrapHandler(p.bucketHandler, p.checkHTTPAuth),
			wrapHandler(p.objectHandler, p.checkHTTPAuth)
		dsortHandler, downloadHandler = wrapHandler(dsort.ProxySortHandler, p.checkHTTPAuth),
			wrapHandler(p.downloadHandler, p.checkHTTPAuth)
	}
	networkHandlers := []networkHandler{
		{r: cmn.Reverse, h: p.reverseHandler, net: []string{cmn.NetworkPublic}},

		{r: cmn.Buckets, h: bucketHandler, net: []string{cmn.NetworkPublic}},
		{r: cmn.Objects, h: objectHandler, net: []string{cmn.NetworkPublic}},
		{r: cmn.Download, h: downloadHandler, net: []string{cmn.NetworkPublic}},
		{r: cmn.Daemon, h: p.daemonHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl}},
		{r: cmn.Cluster, h: p.clusterHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl}},
		{r: cmn.Tokens, h: p.tokenHandler, net: []string{cmn.NetworkPublic}},
		{r: cmn.Sort, h: dsortHandler, net: []string{cmn.NetworkPublic}},

		{r: cmn.Metasync, h: p.metasyncHandler, net: []string{cmn.NetworkIntraControl}},
		{r: cmn.Health, h: p.healthHandler, net: []string{cmn.NetworkIntraControl}},
		{r: cmn.Vote, h: p.voteHandler, net: []string{cmn.NetworkIntraControl}},

		{r: "/", h: cmn.InvalidHandler, net: []string{cmn.NetworkIntraControl, cmn.NetworkIntraData}},
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

// NOTE: caller is responsible to take smapowner lock
func (p *proxyrunner) setGlobRebID(smap *smapX, msgInt *actionMsgInternal, inc bool) {
	ngid := smap.version() * 100
	if inc && p.globRebID >= ngid {
		p.globRebID++
	} else {
		p.globRebID = ngid
	}
	msgInt.GlobRebID = p.globRebID
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

func (p *proxyrunner) unregisterSelf() (int, error) {
	smap := p.smapowner.get()
	args := callArgs{
		si: smap.ProxySI,
		req: cmn.ReqArgs{
			Method: http.MethodDelete,
			Path:   cmn.URLPath(cmn.Version, cmn.Cluster, cmn.Daemon, p.si.DaemonID),
		},
		timeout: cmn.DefaultTimeout,
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
	xaction.Registry.AbortAll()

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

	if !isPrimary {
		_, unregerr := p.unregisterSelf()
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
	case http.MethodPatch:
		p.httpbckpatch(w, r)
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

	switch apiItems[0] {
	case cmn.AllBuckets:
		provider := r.URL.Query().Get(cmn.URLParamProvider)

		normalizedProvider, err := cmn.ProviderFromStr(provider)
		if err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}

		p.getbucketnames(w, r, normalizedProvider)
	default:
		s := fmt.Sprintf("Invalid route /buckets/%s", apiItems[0])
		p.invalmsghdlr(w, r, s)
	}
}

// Used for redirecting ReverseProxy GCP/AWS GET request to target
func (p *proxyrunner) objGetRProxy(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	apitems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	provider := r.URL.Query().Get(cmn.URLParamProvider)
	bck := &cluster.Bck{Name: bucket, Provider: provider}
	if err = bck.Init(p.bmdowner); err != nil {
		if _, err = p.syncCBmeta(w, r, bucket, err); err != nil {
			return
		}
	}
	smap := p.smapowner.get()
	si, err := cluster.HrwTarget(bck.MakeUname(objname), &smap.Smap)
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
	provider := r.URL.Query().Get(cmn.URLParamProvider)
	bck := &cluster.Bck{Name: bucket, Provider: provider}
	if err = bck.Init(p.bmdowner); err != nil {
		if bck, err = p.syncCBmeta(w, r, bucket, err); err != nil {
			return
		}
	}
	if err := bck.AllowGET(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	smap := p.smapowner.get()
	si, err := cluster.HrwTarget(bck.MakeUname(objname), &smap.Smap)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	config := cmn.GCO.Get()
	if config.Net.HTTP.RevProxy == cmn.RevProxyTarget {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("reverse-proxy: %s %s/%s <= %s", r.Method, bucket, objname, si)
		}
		p.reverseNodeRequest(w, r, si)
		delta := time.Since(started)
		p.statsif.Add(stats.GetLatency, int64(delta))
	} else {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s %s/%s => %s", r.Method, bucket, objname, si)
		}
		redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraData)
		http.Redirect(w, r, redirectURL, http.StatusMovedPermanently)
	}
	p.statsif.Add(stats.GetCount, 1)
}

// PUT /v1/objects/bucket-name/object-name
func (p *proxyrunner) httpobjput(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	apiItems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	query := r.URL.Query()
	bucket, objName := apiItems[0], apiItems[1]
	provider := query.Get(cmn.URLParamProvider)
	bck := &cluster.Bck{Name: bucket, Provider: provider}
	if err = bck.Init(p.bmdowner); err != nil {
		if bck, err = p.syncCBmeta(w, r, bucket, err); err != nil {
			return
		}
	}

	var (
		si       *cluster.Snode
		smap     = p.smapowner.get()
		appendTy = query.Get(cmn.URLParamAppendType)
		nodeID   string
	)
	if appendTy == "" {
		err = bck.AllowPUT()
	} else {
		nodeID, _ = parseAppendHandle(query.Get(cmn.URLParamAppendHandle))
		err = bck.AllowAPPEND()
	}

	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	if nodeID == "" {
		si, err = cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
		if err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
	} else {
		si = smap.Smap.GetTarget(nodeID)
		if si == nil {
			p.invalmsghdlr(w, r, cmn.DoesNotExist)
			return
		}
	}

	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s/%s => %s (append: %v)", r.Method, bucket, objName, si, appendTy != "")
	}
	redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraData)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)

	if appendTy == "" {
		p.statsif.Add(stats.PutCount, 1)
	} else {
		p.statsif.Add(stats.AppendCount, 1)
	}
}

// DELETE /v1/objects/bucket-name/object-name
func (p *proxyrunner) httpobjdelete(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	apitems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	provider := r.URL.Query().Get(cmn.URLParamProvider)
	bck := &cluster.Bck{Name: bucket, Provider: provider}
	if err = bck.Init(p.bmdowner); err != nil {
		if bck, err = p.syncCBmeta(w, r, bucket, err); err != nil {
			return
		}
	}
	if err = bck.AllowDELETE(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	smap := p.smapowner.get()
	si, err := cluster.HrwTarget(bck.MakeUname(objname), &smap.Smap)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s/%s => %s", r.Method, bucket, objname, si)
	}
	redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)

	p.statsif.Add(stats.DeleteCount, 1)
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
	provider := r.URL.Query().Get(cmn.URLParamProvider)
	bck := &cluster.Bck{Name: bucket, Provider: provider}
	if err = bck.Init(p.bmdowner); err != nil {
		if bck, err = p.syncCBmeta(w, r, bucket, err); err != nil {
			return
		}
	}
	if err = bck.AllowDELETE(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	switch msg.Action {
	case cmn.ActDestroyLB:
		if p.forwardCP(w, r, &msg, bucket, nil) {
			return
		}
		if _, err, errCode := p.destroyBucket(&msg, bck); err != nil {
			p.invalmsghdlr(w, r, err.Error(), errCode)
			return
		}
	case cmn.ActEvictCB:
		if bck.IsAIS() {
			p.invalmsghdlr(w, r, fmt.Sprintf(fmtNotCloud, bucket))
			return
		}
		if p.forwardCP(w, r, &msg, bucket, nil) {
			return
		}
		bmd, err, errCode := p.destroyBucket(&msg, bck)
		if err != nil {
			p.invalmsghdlr(w, r, err.Error(), errCode)
			return
		}
		msgInt := p.newActionMsgInternal(&msg, nil, bmd)

		// Delete on all targets
		results := p.bcastTo(bcastArgs{
			req: cmn.ReqArgs{
				Method: http.MethodDelete,
				Path:   cmn.URLPath(cmn.Version, cmn.Buckets, bucket),
				Body:   cmn.MustMarshal(msgInt),
			},
		})
		for res := range results {
			if res.err != nil {
				p.invalmsghdlr(w, r, fmt.Sprintf("%s failed, err: %s", msg.Action, res.err))
				return
			}
		}
	case cmn.ActDelete, cmn.ActEvictObjects:
		if msg.Action == cmn.ActEvictObjects && bck.IsAIS() {
			p.invalmsghdlr(w, r, fmt.Sprintf(fmtNotCloud, bucket))
			return
		}
		if err = p.listRange(http.MethodDelete, bucket, &msg, r.URL.Query()); err != nil {
			p.invalmsghdlr(w, r, err.Error())
		}
	default:
		p.invalmsghdlr(w, r, "unsupported action: "+msg.Action)
	}
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
		vote := xaction.Registry.GlobalXactRunning(cmn.ActElection)
		s := fmt.Sprintf("primary %s cannot receive cluster meta (election=%t)", p.si, vote)
		p.invalmsghdlr(w, r, s)
		return
	}
	var payload = make(cmn.SimpleKVs)
	if err := cmn.ReadJSON(w, r, &payload); err != nil {
		return
	}
	caller := r.Header.Get(cmn.HeaderCallerName)
	newSmap, _, err := p.extractSmap(payload, caller)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	if newSmap != nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("new %s from %s", newSmap.StringEx(), caller)
		}
		err = p.smapowner.synchronize(newSmap, true /* lesserIsErr */)
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

	newBMD, actionLB, err := p.extractBMD(payload)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if newBMD != nil {
		caller := r.Header.Get(cmn.HeaderCallerName)
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("new %s from %s", newBMD.StringEx(), caller)
		}
		if err = p.receiveBucketMD(newBMD, actionLB, caller); err != nil {
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

func (p *proxyrunner) createBucket(msg *cmn.ActionMsg, bck *cluster.Bck, cloudHeader ...http.Header) error {
	p.bmdowner.Lock()
	clone := p.bmdowner.get().clone()
	bucketProps := cmn.DefaultBucketProps()
	if len(cloudHeader) != 0 {
		p.copyBckPropsFromHeader(bucketProps, cloudHeader[0])
	}
	if !clone.add(bck, bucketProps) {
		p.bmdowner.Unlock()
		return cmn.NewErrorBucketAlreadyExists(bck.Name)
	}
	p.bmdowner.put(clone)
	p.bmdowner.Unlock()
	msgInt := p.newActionMsgInternal(msg, nil, clone)
	p.metasyncer.sync(true, revspair{clone, msgInt})
	return nil
}

func (p *proxyrunner) destroyBucket(msg *cmn.ActionMsg, bck *cluster.Bck) (*bucketMD, error, int) {
	p.bmdowner.Lock()
	bmd := p.bmdowner.get()
	if bck.IsAIS() {
		if !bmd.IsAIS(bck.Name) {
			p.bmdowner.Unlock()
			return bmd, cmn.NewErrorBucketDoesNotExist(bck.Name), http.StatusNotFound
		}
	} else if !bmd.IsCloud(bck.Name) {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s: %s %s - nothing to do", cmn.ActEvictCB, bck, cmn.DoesNotExist)
		}
		p.bmdowner.Unlock()
		return bmd, nil, 0
	}
	clone := bmd.clone()
	if !clone.del(bck) {
		p.bmdowner.Unlock()
		return nil, cmn.NewErrorBucketDoesNotExist(bck.Name), http.StatusNotFound
	}
	p.bmdowner.put(clone)
	p.bmdowner.Unlock()

	msgInt := p.newActionMsgInternal(msg, nil, clone)
	p.metasyncer.sync(true, revspair{clone, msgInt})

	return clone, nil, 0
}

// POST { action } /v1/buckets/bucket-name
func (p *proxyrunner) httpbckpost(w http.ResponseWriter, r *http.Request) {
	const fmtErr = "cannot %s: invalid provider %q"
	var (
		msg cmn.ActionMsg
	)
	apiItems, err := p.checkRESTItems(w, r, 0, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}
	guestAccess := r.Header.Get(cmn.HeaderGuestAccess) == "1"
	provider := r.URL.Query().Get(cmn.URLParamProvider)

	// 1. "all buckets"
	if len(apiItems) == 0 {
		if guestAccess {
			p.invalmsghdlr(w, r, guestError, http.StatusUnauthorized)
			return
		}

		switch msg.Action {
		case cmn.ActRecoverBck:
			p.recoverBuckets(w, r, &msg)
		case cmn.ActSyncLB:
			if p.forwardCP(w, r, &msg, "", nil) {
				return
			}
			bmd := p.bmdowner.get()
			msgInt := p.newActionMsgInternal(&msg, nil, bmd)
			p.metasyncer.sync(false, revspair{bmd, msgInt})
		case cmn.ActSummaryBucket:
			bck := &cluster.Bck{Name: "", Provider: provider}
			p.bucketSummary(w, r, bck, msg)
		default:
			p.invalmsghdlr(w, r, "URL path is too short: expecting bucket name")
		}
		return
	}

	bucket := apiItems[0]
	bck := &cluster.Bck{Name: bucket, Provider: provider}
	config := cmn.GCO.Get()

	if msg.Action != cmn.ActListObjects && guestAccess {
		p.invalmsghdlr(w, r, guestError, http.StatusUnauthorized)
		return
	}

	// 3. createlb
	if msg.Action == cmn.ActCreateLB {
		if err = cmn.ValidateBucketName(bucket); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		if bck.IsCloud() {
			p.invalmsghdlr(w, r, fmt.Sprintf(fmtErr, msg.Action, provider))
			return
		}
		if p.forwardCP(w, r, &msg, bucket, nil) {
			return
		}
		bck.Provider = cmn.AIS
		if err := p.createBucket(&msg, bck); err != nil {
			errCode := http.StatusInternalServerError
			if _, ok := err.(*cmn.ErrorBucketAlreadyExists); ok {
				errCode = http.StatusConflict
			}
			p.invalmsghdlr(w, r, err.Error(), errCode)
		}
		return
	}

	if err = bck.Init(p.bmdowner); err != nil {
		_, ok := err.(*cmn.ErrorCloudBucketDoesNotExist)
		if ok && msg.Action == cmn.ActRenameLB {
			errMsg := fmt.Sprintf("cannot %q: ais bucket %q does not exist", msg.Action, bucket)
			p.invalmsghdlr(w, r, errMsg, http.StatusNotFound)
			return
		}
		if bck, err = p.syncCBmeta(w, r, bucket, err); err != nil {
			return
		}
	}

	// 4. {action} on bucket
	switch msg.Action {
	case cmn.ActCopyBucket, cmn.ActRenameLB:
		if p.forwardCP(w, r, &msg, bucket, nil) {
			return
		}
		if msg.Action == cmn.ActRenameLB && !bck.IsAIS() {
			p.invalmsghdlr(w, r, fmt.Sprintf(fmtErr, msg.Action, bck.Provider))
			return
		}
		bckFrom, bucketTo := bck, msg.Name
		if bucket == bucketTo {
			s := fmt.Sprintf("cannot %q: duplicate bucket name %q", msg.Action, bucket)
			p.invalmsghdlr(w, r, s)
			return
		}
		if err := cmn.ValidateBucketName(bucketTo); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		if err := p.copyRenameLB(bckFrom, bucketTo, &msg, config); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
	case cmn.ActRegisterCB:
		if p.forwardCP(w, r, &msg, bucket, nil) {
			return
		}
		bck.Provider = cmn.GCO.Get().CloudProvider
		if err := p.createBucket(&msg, bck); err != nil {
			errCode := http.StatusInternalServerError
			if _, ok := err.(*cmn.ErrorBucketAlreadyExists); ok {
				errCode = http.StatusConflict
			}
			p.invalmsghdlr(w, r, err.Error(), errCode)
			return
		}
	case cmn.ActPrefetch:
		if bck.IsAIS() {
			p.invalmsghdlr(w, r, fmt.Sprintf(fmtNotCloud, bucket))
			return
		}
		if err = p.listRange(http.MethodPost, bucket, &msg, r.URL.Query()); err != nil {
			p.invalmsghdlr(w, r, err.Error())
		}
	case cmn.ActListObjects:
		started := time.Now()
		p.listBucketAndCollectStats(w, r, bck, msg, started, false /* fast listing */)
	case cmn.ActSummaryBucket:
		p.bucketSummary(w, r, bck, msg)
	case cmn.ActMakeNCopies:
		p.makeNCopies(w, r, bck, &msg, true /*updateBckProps*/)
	case cmn.ActECEncode:
		if !bck.Props.EC.Enabled {
			p.invalmsghdlr(w, r, fmt.Sprintf("Could not start: bucket %q has EC disabled", bck.Name))
			return
		}
		if err := p.ecEncode(bck, &msg); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
	default:
		s := fmt.Sprintf(fmtUnknownAct, msg)
		p.invalmsghdlr(w, r, s)
	}
}

func (p *proxyrunner) listBucketAndCollectStats(w http.ResponseWriter, r *http.Request, bck *cluster.Bck,
	amsg cmn.ActionMsg, started time.Time, fast bool) {
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
	if fast {
		smsg.Fast = fast
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
	headerID := r.URL.Query().Get(cmn.URLParamTaskID)
	if bck.IsAIS() || smsg.Cached {
		bckList, taskID, err = p.listAISBucket(bck, smsg, headerID)
	} else {
		var status int
		//
		// NOTE: for async tasks, user must check for StatusAccepted and use returned TaskID
		//
		bckList, taskID, status, err = p.listCloudBucket(bck, headerID, smsg)
		if status == http.StatusGone {
			// at this point we know that this cloud bucket exists and is offline
			smsg.Cached = true
			smsg.TaskID = 0
			headerID = ""
			bckList, taskID, err = p.listAISBucket(bck, smsg, headerID)
		}
	}
	if err != nil {
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
		p.statsif.AddMany(
			stats.NamedVal64{Name: stats.ListCount, Value: 1},
			stats.NamedVal64{Name: stats.ListLatency, Value: int64(delta)},
		)
		if glog.FastV(4, glog.SmoduleAIS) {
			var (
				lat = int64(delta / time.Microsecond)
				s   string
			)
			if pageMarker != "" {
				s = ", page " + pageMarker
			}
			glog.Infof("LIST: %s%s, %d µs", bck.Name, s, lat)
		}
	}
}

// bucket == "": all buckets for a given provider
func (p *proxyrunner) bucketSummary(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, amsg cmn.ActionMsg) {
	var (
		taskID    int64
		err       error
		summaries cmn.BucketsSummaries
		smsg      = cmn.SelectMsg{}
	)
	listMsgJSON := cmn.MustMarshal(amsg.Value)
	if err := jsoniter.Unmarshal(listMsgJSON, &smsg); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
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

	headerID := r.URL.Query().Get(cmn.URLParamTaskID)
	if summaries, taskID, err = p.gatherBucketSummary(bck, smsg, headerID); err != nil {
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

	b := cmn.MustMarshal(summaries)
	p.writeJSON(w, r, b, "bucket_summary")
}

func (p *proxyrunner) gatherBucketSummary(bck *cluster.Bck, msg cmn.SelectMsg,
	headerID string) (summaries cmn.BucketsSummaries, code int64, err error) {
	// if a client does not provide taskID(neither in Headers nor in SelectMsg),
	// it is a request to start a new async task
	isNew, q, err := p.initAsyncQuery(headerID, &msg)
	if err != nil {
		return nil, 0, err
	}

	q.Add(cmn.URLParamProvider, bck.Provider)

	var (
		smap   = p.smapowner.get()
		msgInt = p.newActionMsgInternal(&cmn.ActionMsg{Action: cmn.ActSummaryBucket, Value: &msg}, smap, nil)
		body   = cmn.MustMarshal(msgInt)

		args = bcastArgs{
			req: cmn.ReqArgs{
				Path:  cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name),
				Query: q,
				Body:  body,
			},
			smap:    smap,
			timeout: cmn.GCO.Get().Timeout.ListBucket,
		}
	)
	results := p.bcastPost(args)
	allOK, _, err := p.checkBckTaskResp(msg.TaskID, results)
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
	summaries = make(cmn.BucketsSummaries)

	q.Set(cmn.URLParamTaskAction, cmn.TaskResult)
	q.Add(cmn.URLParamProvider, bck.Provider)
	q.Set(cmn.URLParamSilent, "true")
	args.req.Query = q
	results = p.bcastPost(args)
	for result := range results {
		if result.err != nil {
			return nil, 0, result.err
		}

		var targetSummary cmn.BucketsSummaries
		if err := jsoniter.Unmarshal(result.outjson, &targetSummary); err != nil {
			return nil, 0, err
		}

		for bckName, v := range targetSummary {
			if prevV, ok := summaries[bckName]; !ok {
				summaries[bckName] = v
			} else {
				prevV.Aggregate(v)
				summaries[bckName] = prevV
			}
		}
	}

	return summaries, 0, nil
}

// POST { action } /v1/objects/bucket-name
func (p *proxyrunner) httpobjpost(w http.ResponseWriter, r *http.Request) {
	var msg cmn.ActionMsg
	apitems, err := p.checkRESTItems(w, r, 1, true, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket := apitems[0]
	provider := r.URL.Query().Get(cmn.URLParamProvider)
	if provider == "" {
		provider = cmn.AIS /*must be ais*/
	}
	bck := &cluster.Bck{Name: bucket, Provider: provider}
	if err = bck.Init(p.bmdowner); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case cmn.ActRename:
		if !bck.IsAIS() {
			err := cmn.NewErrorBucketDoesNotExist(bucket)
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		p.objRename(w, r, bck)
		return
	case cmn.ActPromote:
		p.promoteFQN(w, r, bck, &msg)
		return
	default:
		s := fmt.Sprintf(fmtUnknownAct, msg)
		p.invalmsghdlr(w, r, s)
	}
}

// HEAD /v1/buckets/bucket-name
func (p *proxyrunner) httpbckhead(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apiItems[0]
	provider := r.URL.Query().Get(cmn.URLParamProvider)
	bck := &cluster.Bck{Name: bucket, Provider: provider}
	if err = bck.Init(p.bmdowner); err != nil {
		if _, ok := err.(*cmn.ErrorBucketDoesNotExist); ok {
			p.invalmsghdlr(w, r, err.Error(), http.StatusNotFound)
			return
		}

		if _, err = p.syncCBmeta(w, r, bucket, err); err != nil {
			return
		}
	}
	if bck.IsAIS() {
		p.bucketPropsToHdr(bck, w.Header(), cmn.GCO.Get(), "")
		return
	}
	si, err := p.smapowner.get().GetRandTarget()
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s => %s", r.Method, bck, si)
	}
	redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

func (p *proxyrunner) applyNewProps(bck *cluster.Bck, propsToUpdate cmn.BucketPropsToUpdate) (nprops *cmn.BucketProps, err error) {
	var (
		cfg        = cmn.GCO.Get()
		cloudProps http.Header
	)

	bprops, exists := p.bmdowner.get().Get(bck)
	if !exists {
		cmn.Assert(!bck.IsAIS())
		if cloudProps, err = p.cbExists(bck.Name); err != nil {
			return nprops, err
		}
		bprops = cmn.DefaultBucketProps()
		p.copyBckPropsFromHeader(bprops, cloudProps)
	}

	nprops = bprops.Clone()
	nprops.Apply(propsToUpdate)

	if bprops.EC.Enabled && nprops.EC.Enabled {
		if !reflect.DeepEqual(bprops.EC, nprops.EC) {
			err = errors.New("cannot change EC configuration after it is enabled")
			return
		}
	} else if nprops.EC.Enabled {
		if nprops.EC.DataSlices == 0 {
			nprops.EC.DataSlices = 1
		}
		if nprops.EC.ParitySlices == 0 {
			nprops.EC.ParitySlices = 1
		}
	}

	if !bprops.Mirror.Enabled && nprops.Mirror.Enabled {
		if nprops.Mirror.Copies == 1 {
			nprops.Mirror.Copies = cmn.MaxI64(cfg.Mirror.Copies, 2)
		}
	} else if nprops.Mirror.Copies == 1 {
		nprops.Mirror.Enabled = false
	}

	if nprops.Cksum.Type == cmn.PropInherit {
		nprops.Cksum.Type = cfg.Cksum.Type
	}

	targetCnt := p.smapowner.Get().CountTargets()
	err = nprops.Validate(bck.IsAIS(), targetCnt, p.urlOutsideCluster)
	return
}

func (p *proxyrunner) updateBucketProps(bck *cluster.Bck, propsToUpdate cmn.BucketPropsToUpdate) (nprops *cmn.BucketProps, err error) {
	nprops, err = p.applyNewProps(bck, propsToUpdate)
	if err != nil {
		return
	}

	// 1. Begin update
	if err := p.beginUpdateBckProps(bck, nprops); err != nil {
		return nprops, err
	}

	// 2. Commit update
	p.bmdowner.Lock()
	clone := p.bmdowner.get().clone()
	_, exists := clone.Get(bck)
	// TODO: Bucket props could have changed between applying new props and this
	// lock. We should check and merge the bucket props if such situation happens.
	// Currently we just assume that the last wins.
	if !exists {
		cmn.Assert(!bck.IsAIS())
		var cloudProps http.Header
		if cloudProps, err = p.cbExists(bck.Name); err != nil {
			p.bmdowner.Unlock()
			return nprops, err
		}
		bprops := cmn.DefaultBucketProps()
		p.copyBckPropsFromHeader(bprops, cloudProps)
		clone.add(bck, bprops)
	}

	clone.set(bck, nprops)
	p.bmdowner.put(clone)
	p.bmdowner.Unlock()
	msgInt := p.newActionMsgInternalStr(cmn.ActSetProps, nil, clone)
	p.metasyncer.sync(true, revspair{clone, msgInt})
	return
}

func (p *proxyrunner) beginUpdateBckProps(bck *cluster.Bck, nprops *cmn.BucketProps) error {
	var (
		smap = p.smapowner.get()
		msg  = &cmn.ActionMsg{
			Action: cmn.ActSetProps,
			Value:  nprops,
		}
		msgInt = p.newActionMsgInternal(msg, smap, nil)
		body   = cmn.MustMarshal(msgInt)
	)
	results := p.bcastPost(bcastArgs{
		req: cmn.ReqArgs{
			Path: cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name, cmn.ActBegin),
			Body: body,
		},
		smap: smap,
	})
	for result := range results {
		if result.err != nil {
			return result.err
		}
	}
	return nil
}

// PUT /v1/buckets/bucket-name
func (p *proxyrunner) httpbckput(w http.ResponseWriter, r *http.Request) {
	apitems, err := p.checkRESTItems(w, r, 1, true, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	provider := r.URL.Query().Get(cmn.URLParamProvider)
	bck := &cluster.Bck{Name: bucket, Provider: provider}
	if err = bck.Init(p.bmdowner); err != nil {
		if bck, err = p.syncCBmeta(w, r, bucket, err); err != nil {
			return
		}
	}
	if p.forwardCP(w, r, &cmn.ActionMsg{}, "httpbckput", nil) {
		return
	}

	// otherwise, handle the general case: unmarshal into cmn.BucketProps and treat accordingly
	// Note: this use case is for setting all bucket props
	msg := cmn.ActionMsg{}
	if err = cmn.ReadJSON(w, r, &msg); err != nil {
		s := fmt.Sprintf("Failed to unmarshal: %v", err)
		p.invalmsghdlr(w, r, s)
		return
	}

	if err := p.checkAction(msg, cmn.ActResetProps); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	p.bmdowner.Lock()
	clone := p.bmdowner.get().clone()
	_, exists := clone.Get(bck)
	if !exists {
		cmn.Assert(!bck.IsAIS())
		var (
			err        error
			cloudProps http.Header
		)
		if cloudProps, err = p.cbExists(bucket); err != nil {
			p.bmdowner.Unlock()
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		bprops := cmn.DefaultBucketProps()
		p.copyBckPropsFromHeader(bprops, cloudProps)
		clone.add(bck, bprops)
	}

	bprops := cmn.DefaultBucketProps()
	clone.set(bck, bprops)
	p.bmdowner.put(clone)
	p.bmdowner.Unlock()

	msgInt := p.newActionMsgInternal(&msg, nil, clone)
	p.metasyncer.sync(true, revspair{clone, msgInt})
}

// PATCH /v1/buckets/bucket-name
func (p *proxyrunner) httpbckpatch(w http.ResponseWriter, r *http.Request) {
	apitems, err := p.checkRESTItems(w, r, 1, true, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	provider := r.URL.Query().Get(cmn.URLParamProvider)
	bck := &cluster.Bck{Name: bucket, Provider: provider}
	if err = bck.Init(p.bmdowner); err != nil {
		if bck, err = p.syncCBmeta(w, r, bucket, err); err != nil {
			return
		}
	}
	if p.forwardCP(w, r, &cmn.ActionMsg{}, "httpbckpatch", nil) {
		return
	}

	prevMirrorConf := bck.Props.Mirror

	var propsToUpdate cmn.BucketPropsToUpdate
	msg := cmn.ActionMsg{Value: &propsToUpdate}
	if err = cmn.ReadJSON(w, r, &msg); err != nil {
		s := fmt.Sprintf("Failed to unmarshal: %v", err)
		p.invalmsghdlr(w, r, s)
		return
	}

	if err := p.checkAction(msg, cmn.ActSetProps); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	nprops, err := p.updateBucketProps(bck, propsToUpdate)
	if _, ok := err.(*cmn.ErrorCloudBucketDoesNotExist); ok {
		p.invalmsghdlr(w, r, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	if requiresRemirroring(prevMirrorConf, nprops.Mirror) {
		msg := &cmn.ActionMsg{
			Action: cmn.ActMakeNCopies,
			Value:  fmt.Sprintf("%d", nprops.Mirror.Copies),
		}
		p.makeNCopies(w, r, bck, msg, false /*updateBckProps*/)
	}
}

// HEAD /v1/objects/bucket-name/object-name
func (p *proxyrunner) httpobjhead(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	query := r.URL.Query()
	checkExists, _ := cmn.ParseBool(query.Get(cmn.URLParamCheckExists))
	apitems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	provider := query.Get(cmn.URLParamProvider)
	bck := &cluster.Bck{Name: bucket, Provider: provider}
	if err = bck.Init(p.bmdowner); err != nil {
		if bck, err = p.syncCBmeta(w, r, bucket, err); err != nil {
			return
		}
	}
	if err := bck.AllowHEAD(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	smap := p.smapowner.get()
	si, err := cluster.HrwTarget(bck.MakeUname(objname), &smap.Smap)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusInternalServerError)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s/%s => %s", r.Method, bucket, objname, si)
	}
	redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraControl)
	if checkExists {
		redirectURL += fmt.Sprintf("&%s=true", cmn.URLParamCheckExists)
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

func (p *proxyrunner) copyRenameLB(bckFrom *cluster.Bck, bucketTo string, msg *cmn.ActionMsg, config *cmn.Config) (err error) {
	var (
		smap  = p.smapowner.get()
		tout  = config.Timeout.Default
		bckTo = &cluster.Bck{Name: bucketTo, Provider: cmn.AIS}
	)

	p.bmdowner.Lock()
	clone := p.bmdowner.get().clone()

	// Re-init under a lock
	if err = bckFrom.Init(p.bmdowner); err != nil {
		p.bmdowner.Unlock()
		return
	}
	if err := bckTo.Init(p.bmdowner); err == nil {
		if msg.Action == cmn.ActRenameLB {
			p.bmdowner.Unlock()
			return cmn.NewErrorBucketAlreadyExists(bckTo.Name)
		}
		// Allow to copy into existing bucket
		glog.Warningf("destination bucket %s already exists, proceeding to %s %s => %s anyway",
			bckFrom, msg.Action, bckFrom, bckTo)
	} else {
		bckToProps := bckFrom.Props.Clone()
		clone.add(bckTo, bckToProps)
	}
	clone.downgrade(bckFrom)
	clone.downgrade(bckTo)
	p.bmdowner.put(clone)
	p.bmdowner.Unlock()

	// Distribute temporary bucket
	msgInt := p.newActionMsgInternal(msg, smap, clone)
	p.metasyncer.sync(true, revspair{clone, msgInt})
	glog.Infof("%s ais bucket %s => %s %s", msg.Action, bckFrom, bckTo, clone)

	errHandler := func() {
		p.bmdowner.Lock()
		clone := p.bmdowner.get().clone()
		clone.upgrade(bckFrom)
		clone.upgrade(bckTo)
		clone.del(bckTo)
		p.bmdowner.put(clone)
		p.bmdowner.Unlock()

		msgInt := p.newActionMsgInternal(msg, smap, clone)
		p.metasyncer.sync(true, revspair{clone, msgInt})
	}

	// Begin
	var (
		body   = cmn.MustMarshal(msgInt)
		path   = cmn.URLPath(cmn.Version, cmn.Buckets, bckFrom.Name)
		errmsg = fmt.Sprintf("cannot %s bucket %s", msg.Action, bckFrom)
	)
	args := bcastArgs{req: cmn.ReqArgs{Path: path, Body: body}, smap: smap, timeout: tout}
	err = p.bcast2Phase(args, errmsg, false /*commit*/)
	if err != nil {
		// Abort
		errHandler()
		return
	}

	// Commit
	if msg.Action == cmn.ActRenameLB {
		p.smapowner.Lock()
		p.setGlobRebID(smap, msgInt, true) // FIXME: race vs change in membership
		p.smapowner.Unlock()
		body = cmn.MustMarshal(msgInt)
	}
	args.req.Body = body
	args.req.Path = cmn.URLPath(path, cmn.ActCommit)
	results := p.bcastPost(args)
	for res := range results {
		if res.err != nil {
			err = fmt.Errorf("%s: failed to %s bucket %s: %v(%d)",
				res.si.Name(), msg.Action, bckFrom, res.err, res.status)
			glog.Error(err)
			break
		}
	}
	if err != nil {
		errHandler()
		return
	}

	p.bmdowner.Lock()
	clone = p.bmdowner.get().clone()
	clone.upgrade(bckFrom)
	clone.upgrade(bckTo)
	if msg.Action == cmn.ActRenameLB {
		bckFromProps := bckFrom.Props.Clone()
		bckFromProps.Renamed = cmn.ActRenameLB
		clone.set(bckFrom, bckFromProps)
	}
	p.bmdowner.put(clone)
	p.bmdowner.Unlock()

	// Finalize
	msgInt = p.newActionMsgInternal(msg, nil, clone)
	p.metasyncer.sync(true, revspair{clone, msgInt})
	glog.Infof("%s ais bucket %s => %s, %s", msg.Action, bckFrom, bckTo, clone)
	return
}

func (p *proxyrunner) makeNCopies(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, msg *cmn.ActionMsg, updateBckProps bool) {
	copies, err := p.parseValidateNCopies(msg.Value)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	var (
		smap   = p.smapowner.get()
		msgInt = p.newActionMsgInternal(msg, smap, nil)
		body   = cmn.MustMarshal(msgInt)
		path   = cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name)
		errmsg = fmt.Sprintf("failed to execute '%s' on bucket %s", msg.Action, bck)
	)
	err = p.bcast2Phase(bcastArgs{req: cmn.ReqArgs{Path: path, Body: body}}, errmsg, true /*commit*/)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	if updateBckProps {
		if err := bck.Init(p.bmdowner); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}

		cmn.Assert(copies >= 1)
		nprops := cmn.BucketPropsToUpdate{
			Mirror: &cmn.MirrorConfToUpdate{
				Enabled: api.Bool(true),
				Copies:  api.Int64(int64(copies)),
			},
		}
		// NOTE: cmn.ActMakeNCopies automatically does
		// (copies > 1 ? enable : disable) on the bucket's MirrorConf
		if _, err := p.updateBucketProps(bck, nprops); err != nil {
			p.invalmsghdlr(w, r, err.Error())
		}
	}
}

func (p *proxyrunner) ecEncode(bck *cluster.Bck, msg *cmn.ActionMsg) (err error) {
	var (
		msgInt  = p.newActionMsgInternal(msg, nil, p.bmdowner.get())
		body    = cmn.MustMarshal(msgInt)
		urlPath = cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name)
		errmsg  = fmt.Sprintf("cannot %s bucket %s", msg.Action, bck)
	)
	// execute 2-phase
	err = p.bcast2Phase(bcastArgs{req: cmn.ReqArgs{Path: urlPath, Body: body}}, errmsg, true /*commit*/)
	return
}

func (p *proxyrunner) getbucketnames(w http.ResponseWriter, r *http.Request, provider string) {
	bmd := p.bmdowner.get()
	providerStr := "?" + cmn.URLParamProvider + "=" + provider
	if cmn.IsProviderAIS(provider) {
		bucketNames := &cmn.BucketNames{Cloud: []string{}, AIS: make([]string, 0, 64)}
		for bucket := range bmd.LBmap {
			bucketNames.AIS = append(bucketNames.AIS, bucket)
		}
		body := cmn.MustMarshal(bucketNames)
		p.writeJSON(w, r, body, "getbucketnames"+providerStr)
		return
	}

	si, err := p.smapowner.get().GetRandTarget()
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	args := callArgs{
		si: si,
		req: cmn.ReqArgs{
			Method: r.Method,
			Path:   cmn.URLPath(cmn.Version, cmn.Buckets, cmn.AllBuckets),
			Query:  r.URL.Query(),
			Header: r.Header,
		},
		timeout: cmn.DefaultTimeout,
	}
	res := p.call(args)
	if res.err != nil {
		p.invalmsghdlr(w, r, res.details)
		p.keepalive.onerr(res.err, res.status)
	} else {
		p.writeJSON(w, r, res.outjson, "getbucketnames"+providerStr)
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

// for cloud buckets only - check if exists (in the Cloud) and update BMD
func (p *proxyrunner) syncCBmeta(w http.ResponseWriter, r *http.Request, bucket string, erc error) (bck *cluster.Bck, err error) {
	if _, ok := erc.(*cmn.ErrorCloudBucketDoesNotExist); !ok {
		err = erc
		p.invalmsghdlr(w, r, erc.Error())
		return
	}
	msg := &cmn.ActionMsg{Action: cmn.ActRegisterCB}
	if p.forwardCP(w, r, msg, "ensure_cloud_bucket", nil) {
		err = errors.New("forwarded")
		return
	}
	var cloudProps http.Header
	if cloudProps, err = p.cbExists(bucket); err != nil {
		if _, ok := err.(*cmn.ErrorCloudBucketDoesNotExist); ok {
			p.invalmsghdlr(w, r, err.Error(), http.StatusNotFound)
		} else {
			p.invalmsghdlr(w, r, err.Error())
		}
		return
	}
	bck = &cluster.Bck{Name: bucket, Provider: cmn.GCO.Get().CloudProvider}
	if err = p.createBucket(msg, bck, cloudProps); err != nil {
		if _, ok := err.(*cmn.ErrorBucketAlreadyExists); !ok {
			p.invalmsghdlr(w, r, err.Error(), http.StatusConflict)
			return
		}
	}
	err = bck.Init(p.bmdowner)
	cmn.AssertNoErr(err)
	return
}

func (p *proxyrunner) copyBckPropsFromHeader(props *cmn.BucketProps, header http.Header) {
	if props == nil || len(header) == 0 {
		return
	}

	if verStr := header.Get(cmn.HeaderBucketVerEnabled); verStr != "" {
		if versioning, err := cmn.ParseBool(verStr); err == nil {
			props.Versioning.Enabled = versioning
		}
	}
}

func (p *proxyrunner) cbExists(bucket string) (header http.Header, err error) {
	var tsi *cluster.Snode
	if tsi, err = p.smapowner.get().GetRandTarget(); err != nil {
		return
	}
	args := callArgs{
		si: tsi,
		req: cmn.ReqArgs{
			Method: http.MethodHead,
			Base:   tsi.URL(cmn.NetworkIntraData),
			Path:   cmn.URLPath(cmn.Version, cmn.Buckets, bucket),
		},
		timeout: cmn.DefaultTimeout,
	}
	res := p.call(args)
	if res.status == http.StatusNotFound {
		err = cmn.NewErrorCloudBucketDoesNotExist(bucket)
	} else if res.status == http.StatusGone {
		err = cmn.NewErrorCloudBucketOffline(bucket, cmn.Cloud) // TODO -- FIXME
	} else if res.err != nil {
		err = fmt.Errorf("%s: bucket %s, target %s, err: %v", p.si.Name(), bucket, tsi.Name(), res.err)
	} else {
		header = res.header
	}
	return
}

func (p *proxyrunner) initAsyncQuery(headerID string, msg *cmn.SelectMsg) (bool, url.Values, error) {
	isNew := headerID == "" && msg.TaskID == 0
	q := url.Values{}
	if isNew {
		msg.TaskID, _ = cmn.GenUUID64()
		q.Set(cmn.URLParamTaskAction, cmn.TaskStart)
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
		q.Set(cmn.URLParamTaskAction, cmn.TaskStatus)
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("proxy: reading async task %d result", msg.TaskID)
		}
	}

	return isNew, q, nil
}

func (p *proxyrunner) checkBckTaskResp(taskID int64, results chan callResult) (allOK bool, status int, err error) {
	// check response codes of all targets
	// Target that has completed its async task returns 200, and 202 otherwise
	allOK = true
	allNotFound := true
	for res := range results {
		if res.status == http.StatusNotFound {
			continue
		}
		allNotFound = false
		if res.err != nil {
			return false, res.status, res.err
		}
		if res.status != http.StatusOK {
			allOK = false
			status = res.status
			break
		}
	}
	if allNotFound {
		err = fmt.Errorf("Task %d %s", taskID, cmn.DoesNotExist)
	}
	return
}

// reads object list from all targets, combines, sorts and returns the list
// returns:
//      * the list of objects if the aync task finished (taskID is 0 in this case)
//      * non-zero taskID if the task is still running
//      * error
func (p *proxyrunner) listAISBucket(bck *cluster.Bck, msg cmn.SelectMsg,
	headerID string) (allEntries *cmn.BucketList, taskID int64, err error) {
	// Start new async task if client did not provide taskID (neither in headers nor in SelectMsg).
	isNew, q, err := p.initAsyncQuery(headerID, &msg)
	if err != nil {
		return nil, 0, err
	}

	var (
		msgInt = p.newActionMsgInternal(&cmn.ActionMsg{Action: cmn.ActListObjects, Value: &msg}, nil, nil)
		body   = cmn.MustMarshal(msgInt)

		args = bcastArgs{
			req: cmn.ReqArgs{
				Path:  cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name),
				Query: q,
				Body:  body,
			},
			timeout: cmn.GCO.Get().Timeout.ListBucket,
		}
	)

	results := p.bcastPost(args)
	allOK, _, err := p.checkBckTaskResp(msg.TaskID, results)
	if err != nil {
		return nil, 0, err
	}

	// Some targets are still executing their tasks or it is request to start
	// an async task. The proxy returns only taskID to a caller.
	if !allOK || isNew {
		return nil, msg.TaskID, nil
	}

	// All targets are ready, prepare the final result.
	q = url.Values{}
	q.Set(cmn.URLParamTaskAction, cmn.TaskResult)
	q.Set(cmn.URLParamSilent, "true")
	args.req.Query = q
	results = p.bcastPost(args)

	preallocSize := cmn.DefaultListPageSize
	if msg.PageSize != 0 {
		preallocSize = msg.PageSize
	}

	// Combine the results.
	bckLists := make([]*cmn.BucketList, 0, len(results))
	for res := range results {
		if res.err != nil {
			return nil, 0, res.err
		}
		if len(res.outjson) == 0 {
			continue
		}
		bucketList := &cmn.BucketList{Entries: make([]*cmn.BucketEntry, 0, preallocSize)}
		if err = jsoniter.Unmarshal(res.outjson, &bucketList); err != nil {
			return
		}
		res.outjson = nil

		if len(bucketList.Entries) == 0 {
			continue
		}

		bckLists = append(bckLists, bucketList)
	}

	var pageSize int
	if msg.PageSize != 0 {
		// When `PageSize` is set, then regardless of the listing type (slow/fast)
		// we need respect it.
		pageSize = msg.PageSize
	} else if !msg.Fast {
		// When listing slow, we need to return a single page of default size.
		pageSize = cmn.DefaultListPageSize
	} else if msg.Fast {
		// When listing fast, we need to return all entries (without paging).
		pageSize = 0
	}

	allEntries = objwalk.ConcatObjLists(bckLists, pageSize)
	return allEntries, 0, nil
}

// selects a random target to GET the list of objects from the cloud
// - if iscached or atime property is requested performs extra steps:
//      * get list of cached files info from all targets
//      * updates the list of objects from the cloud with cached info
// returns:
//      * the list of objects if the aync task finished (taskID is 0 in this case)
//      * non-zero taskID if the task is still running
//      * error
//
func (p *proxyrunner) listCloudBucket(bck *cluster.Bck, headerID string,
	msg cmn.SelectMsg) (allEntries *cmn.BucketList, taskID int64, status int, err error) {
	if msg.PageSize > cmn.DefaultListPageSize {
		glog.Warningf("list-bucket page size %d for bucket %s exceeds the default maximum %d ",
			msg.PageSize, bck, cmn.DefaultListPageSize)
	}
	pageSize := msg.PageSize
	if pageSize == 0 {
		pageSize = cmn.DefaultListPageSize
	}

	// start new async task if client did not provide taskID (neither in headers nor in SelectMsg)
	isNew, q, err := p.initAsyncQuery(headerID, &msg)
	if err != nil {
		return nil, 0, 0, err
	}
	q.Set(cmn.URLParamProvider, cmn.Cloud)

	var (
		smap          = p.smapowner.get()
		reqTimeout    = cmn.GCO.Get().Timeout.ListBucket
		msgInt        = p.newActionMsgInternal(&cmn.ActionMsg{Action: cmn.ActListObjects, Value: &msg}, smap, nil)
		body          = cmn.MustMarshal(msgInt)
		needLocalData = msg.NeedLocalData()
	)

	args := bcastArgs{
		req: cmn.ReqArgs{
			Method: http.MethodPost,
			Path:   cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name),
			Query:  q,
			Body:   body,
		},
		timeout: reqTimeout,
	}

	var results chan callResult
	if needLocalData || !isNew {
		q.Set(cmn.URLParamSilent, "true")
		results = p.bcastTo(args)
	} else {
		// only cloud options are requested - call one random target for data
		for _, si := range smap.Tmap {
			results = p.bcast(bcastArgs{
				req:     args.req,
				network: cmn.NetworkIntraControl,
				timeout: reqTimeout,
				nodes:   []cluster.NodeMap{{si.DaemonID: si}},
			})
			break
		}
	}

	allOK, status, err := p.checkBckTaskResp(msg.TaskID, results)
	if err != nil {
		return nil, 0, status, err
	}

	// some targets are still executing their tasks or it is request to start
	// an async task. The proxy returns only taskID to a caller
	if !allOK || isNew {
		return nil, msg.TaskID, 0, nil
	}

	// all targets are ready, prepare the final result
	q = url.Values{}
	q.Set(cmn.URLParamTaskAction, cmn.TaskResult)
	q.Set(cmn.URLParamSilent, "true")
	q.Set(cmn.URLParamProvider, cmn.Cloud)
	args.req.Query = q
	results = p.bcastTo(args)

	// combine results
	bckLists := make([]*cmn.BucketList, 0, len(results))
	for res := range results {
		if res.status == http.StatusNotFound { // TODO -- FIXME
			continue
		}
		if res.err != nil {
			return nil, 0, res.status, res.err
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

		bckLists = append(bckLists, bucketList)
	}

	if msg.Cached {
		allEntries = objwalk.ConcatObjLists(bckLists, pageSize)
	} else {
		allEntries = objwalk.MergeObjLists(bckLists, pageSize)
	}

	if msg.WantProp(cmn.GetTargetURL) {
		for _, e := range allEntries.Entries {
			si, err := cluster.HrwTarget(bck.MakeUname(e.Name), &smap.Smap)
			if err == nil {
				e.TargetURL = si.URL(cmn.NetworkPublic)
			}
		}
	}

	// no active tasks - return TaskID=0
	return allEntries, 0, 0, nil
}

func (p *proxyrunner) objRename(w http.ResponseWriter, r *http.Request, bck *cluster.Bck) {
	started := time.Now()
	apitems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	objname := apitems[1]
	smap := p.smapowner.get()
	si, err := cluster.HrwTarget(bck.MakeUname(objname), &smap.Smap)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("RENAME %s %s/%s => %s", r.Method, bck.Name, objname, si)
	}

	// NOTE:
	//       code 307 is the only way to http-redirect with the
	//       original JSON payload (SelectMsg - see pkg/api/constant.go)
	redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)

	p.statsif.Add(stats.RenameCount, 1)
}

func (p *proxyrunner) promoteFQN(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, msg *cmn.ActionMsg) {
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}

	params := cmn.ActValPromote{}
	if err := cmn.TryUnmarshal(msg.Value, &params); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	var (
		started = time.Now()
		smap    = p.smapowner.get()
		bucket  = apiItems[0]
		srcFQN  = msg.Name
	)
	if err = cmn.ValidateOmitBase(srcFQN, params.OmitBase); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	// designated target ID
	if params.Target != "" {
		tsi := smap.GetTarget(params.Target)
		if tsi == nil {
			p.invalmsghdlr(w, r, fmt.Sprintf("target %q does not exist", params.Target))
			return
		}
		// NOTE:
		//       code 307 is the only way to http-redirect with the
		//       original JSON payload (SelectMsg - see pkg/api/constant.go)
		redirectURL := p.redirectURL(r, tsi, started, cmn.NetworkIntraControl)
		http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
		return
	}

	// all targets
	//
	// TODO -- FIXME: 2phase begin to check space, validate params, and check vs running xactions
	//
	var (
		body  = cmn.MustMarshal(msg)
		query = url.Values{}
	)
	query.Add(cmn.URLParamProvider, bck.Provider)
	results := p.bcastPost(bcastArgs{
		req: cmn.ReqArgs{
			Path:  cmn.URLPath(cmn.Version, cmn.Objects, bucket),
			Query: query,
			Body:  body,
		},
		timeout: cmn.DefaultTimeout,
	})

	for res := range results {
		if res.err != nil {
			p.invalmsghdlr(w, r, fmt.Sprintf("%s failed, err: %s", msg.Action, res.err))
			return
		}
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
		timeout = cmn.DefaultTimeout
	}

	results = p.bcastTo(bcastArgs{
		req: cmn.ReqArgs{
			Method: method,
			Path:   cmn.URLPath(cmn.Version, cmn.Buckets, bucket),
			Query:  query,
			Body:   body,
		},
		smap:    smap,
		timeout: timeout,
	})
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
	authToken := r.Header.Get(cmn.HeaderAuthorization)
	if authToken == "" && cmn.GCO.Get().Auth.AllowGuest {
		return guestAcc, nil
	}

	s := strings.SplitN(authToken, " ", 2)
	if len(s) != 2 || s[0] != cmn.HeaderBearer {
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
			if auth.isGuest && r.Method == http.MethodPost && strings.Contains(r.URL.Path, "/"+cmn.Buckets+"/") {
				// get bucket objects is POST request, it cannot check the action
				// here because it "emties" payload and httpbckpost gets nothing.
				// So, we just set flag 'Guest' for httpbckpost to use
				r.Header.Set(cmn.HeaderGuestAccess, "1")
			} else if auth.isGuest && r.Method != http.MethodGet && r.Method != http.MethodHead {
				p.invalmsghdlr(w, r, guestError, http.StatusUnauthorized)
				return
			}
			if glog.FastV(4, glog.SmoduleAIS) {
				if auth.isGuest {
					glog.Info("Guest access granted")
				} else {
					glog.Infof("Logged as %s", auth.userID)
				}
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

func (p *proxyrunner) handlePendingRenamedLB(renamedBucket string) {
	p.bmdowner.Lock()
	bmd := p.bmdowner.get()
	bck := &cluster.Bck{Name: renamedBucket, Provider: cmn.AIS}
	if err := bck.Init(p.bmdowner); err != nil {
		p.bmdowner.Unlock() // already removed via the the very first target calling here..
		return
	}
	if bck.Props.Renamed == "" {
		glog.Errorf("%s: renamed bucket %s: unexpected props %+v", p.si.Name(), renamedBucket, *bck.Props)
		p.bmdowner.Unlock()
		return
	}
	bmd = bmd.clone()
	bmd.del(bck)
	p.bmdowner.put(bmd)
	p.bmdowner.Unlock()
}

func (p *proxyrunner) httpdaeget(w http.ResponseWriter, r *http.Request) {
	var (
		q           = r.URL.Query()
		getWhat     = q.Get(cmn.URLParamWhat)
		httpdaeWhat = "httpdaeget-" + getWhat
	)
	switch getWhat {
	case cmn.GetWhatBMD:
		if renamedBucket := q.Get(whatRenamedLB); renamedBucket != "" {
			p.handlePendingRenamedLB(renamedBucket)
		}
		fallthrough
	case cmn.GetWhatConfig, cmn.GetWhatSmapVote, cmn.GetWhatSnode:
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
			if err := p.smapowner.synchronize(newsmap, true /* lesserIsErr */); err != nil {
				p.invalmsghdlr(w, r, err.Error())
				return
			}
			glog.Infof("%s: %s %s done", p.si.Name(), cmn.SyncSmap, newsmap)
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
		s := fmt.Sprintf(fmtUnknownAct, msg)
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
	args := callArgs{req: req, timeout: cmn.DefaultTimeout}
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
		s := fmt.Sprintf("%s: failed to find new primary %s in %s", p.si.Name(), proxyID, smap.pp())
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
	res := p.registerToURL(newSmap.ProxySI.IntraControlNet.DirectURL, newSmap.ProxySI, cmn.DefaultTimeout, nil, false)
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
		s := fmt.Sprintf("New primary proxy %s not present in the %s", proxyID, smap.pp())
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
	if err := p.smapowner.persist(clone); err != nil {
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
		cmn.AssertMsg(false, "This proxy '"+p.si.DaemonID+"' must always be present in the "+smap.pp())
	}
	clone := smap.clone()
	// FIXME: may be premature at this point
	if proxyIDToRemove != "" {
		glog.Infof("Removing failed primary proxy %s", proxyIDToRemove)
		clone.delProxy(proxyIDToRemove)
	}

	clone.ProxySI = p.si
	clone.Version += 100
	if err = p.smapowner.persist(clone); err != nil {
		p.smapowner.Unlock()
		glog.Error(err)
		return
	}
	p.smapowner.put(clone)
	msgInt := p.newActionMsgInternalStr(cmn.ActNewPrimary, clone, nil)
	p.setGlobRebID(clone, msgInt, true)
	p.smapowner.Unlock()

	bmd := p.bmdowner.get()
	if glog.V(3) {
		glog.Infof("%s: distributing %s with newly elected primary (self)", p.si.Name(), clone)
		glog.Infof("%s: distributing %s as well", p.si.Name(), bmd)
	}
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
		s := fmt.Sprintf("New primary proxy %s not present in the %s", proxyid, smap.pp())
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
	results := p.bcastTo(bcastArgs{
		req: cmn.ReqArgs{
			Method: method,
			Path:   urlPath,
			Query:  q,
		},
		smap: smap,
		to:   cluster.AllNodes,
	})

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
	if err := p.smapowner.persist(clone); err != nil {
		glog.Errorf("Failed to save Smap locally after having transitioned to non-primary:\n%s", err)
	}
	p.smapowner.put(clone)
	p.smapowner.Unlock()

	// (II) commit phase
	q.Set(cmn.URLParamPrepare, "false")
	results = p.bcastTo(bcastArgs{
		req: cmn.ReqArgs{
			Method: method,
			Path:   urlPath,
			Query:  q,
		},
		to: cluster.AllNodes,
	})

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
		s := fmt.Sprintf("unexpected GET request, invalid param 'what': [%s]", getWhat)
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

func (p *proxyrunner) invokeHTTPSelectMsgOnTargets(w http.ResponseWriter, r *http.Request, silent bool) (cmn.JSONRawMsgs, bool) {
	var (
		err  error
		body []byte
	)
	if r.Body != nil {
		body, err = cmn.ReadBytes(r)
		if err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return nil, false
		}
	}
	results := p.bcastTo(bcastArgs{
		req: cmn.ReqArgs{
			Method: r.Method,
			Path:   cmn.URLPath(cmn.Version, cmn.Daemon),
			Query:  r.URL.Query(),
			Body:   body,
		},
		timeout: cmn.GCO.Get().Timeout.Default,
	})
	targetResults := make(cmn.JSONRawMsgs, len(results))
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
	fetchResults := func(broadcastType int) (cmn.JSONRawMsgs, string) {
		results := p.bcastTo(bcastArgs{
			req: cmn.ReqArgs{
				Method: r.Method,
				Path:   cmn.URLPath(cmn.Version, cmn.Daemon),
				Query:  r.URL.Query(),
			},
			timeout: cmn.GCO.Get().Timeout.Default,
			to:      broadcastType,
		})
		sysInfoMap := make(cmn.JSONRawMsgs, len(results))
		for res := range results {
			if res.err != nil {
				return nil, res.details
			}
			sysInfoMap[res.si.DaemonID] = jsoniter.RawMessage(res.outjson)
		}
		return sysInfoMap, ""
	}

	out := &cmn.ClusterSysInfoRaw{}

	proxyResults, err := fetchResults(cluster.Proxies)
	if err != "" {
		p.invalmsghdlr(w, r, err)
		return false
	}

	out.Proxy = proxyResults

	targetResults, err := fetchResults(cluster.Targets)
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
		errstr := fmt.Sprintf("Unable to invoke cmn.SelectMsg on targets. Query: [%s]", r.URL.RawQuery)
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
// start|stop xaction
func (p *proxyrunner) httpclupost(w http.ResponseWriter, r *http.Request) {
	var (
		regReq                                targetRegMeta
		pname                                 = p.si.Name()
		keepalive, userRegister, selfRegister bool
		nonElectable                          bool
	)
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Cluster)
	if err != nil {
		return
	}
	switch apiItems[0] {
	case cmn.UserRegister: // manual by user (API)
		if cmn.ReadJSON(w, r, &regReq.SI) != nil {
			return
		}
		userRegister = true
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
		p.invalmsghdlr(w, r, fmt.Sprintf("invalid URL path: %q", apiItems[0]))
		return
	}

	nsi := regReq.SI
	if err := nsi.Validate(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if p.startedUp.Load() {
		bmd := p.bmdowner.get()
		if err := p.validateOriginBMD(bmd, nsi, regReq.BMD, ""); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
	}
	isProxy := nsi.IsProxy()
	if isProxy {
		s := r.URL.Query().Get(cmn.URLParamNonElectable)
		if nonElectable, err = cmn.ParseBool(s); err != nil {
			glog.Errorf("%s: failed to parse %s for non-electability: %v", pname, s, err)
		}
	}

	s := fmt.Sprintf("register %s, (keepalive, user, self)=(%t, %t, %t)", nsi, keepalive, userRegister, selfRegister)
	msg := &cmn.ActionMsg{Action: cmn.ActRegTarget}
	if isProxy {
		msg = &cmn.ActionMsg{Action: cmn.ActRegProxy}
	}
	body := cmn.MustMarshal(regReq)
	if p.forwardCP(w, r, msg, s, body) {
		return
	}
	if net.ParseIP(nsi.PublicNet.NodeIPAddr) == nil {
		s := fmt.Sprintf("%s: failed to register %s - invalid IP address %v", pname, nsi.Name(), nsi.PublicNet.NodeIPAddr)
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

		// FIXME: If !keepalive then update Smap anyway - either: new target,
		// updated target or target has powercycled. Except 'updated target' case,
		// rebalance needs to be triggered and updating smap is the easiest.
		if keepalive && !p.addOrUpdateNode(nsi, osi, keepalive) {
			p.smapowner.Unlock()
			return
		}

		if userRegister {
			if glog.FastV(3, glog.SmoduleAIS) {
				glog.Infof("%s: register %s (num targets before: %d)", pname, nsi.Name(), smap.CountTargets())
			}
			// when registration is done by user send the current BMD and Smap
			bmd := p.bmdowner.get()
			meta := &targetRegMeta{smap, bmd, p.si}
			body := cmn.MustMarshal(meta)

			args := callArgs{
				si: nsi,
				req: cmn.ReqArgs{
					Method: http.MethodPost,
					Path:   cmn.URLPath(cmn.Version, cmn.Daemon, cmn.UserRegister),
					Body:   body,
				},
				timeout: cmn.GCO.Get().Timeout.CplaneOperation,
			}
			res := p.call(args)
			if res.err != nil {
				p.smapowner.Unlock()
				msg := fmt.Sprintf("failed to register %s: %v, %s", nsi.Name(), res.err, res.details)
				if cmn.IsErrConnectionRefused(res.err) {
					msg = fmt.Sprintf("failed to reach %s on %s:%s",
						nsi.Name(), nsi.PublicNet.NodeIPAddr, nsi.PublicNet.DaemonPort)
				}
				p.invalmsghdlr(w, r, msg, res.status)
				return
			}
		}
	}
	// check for cluster integrity errors (cie)
	if err = p.validateOriginSmap(smap, nsi, regReq.Smap, ""); err != nil {
		p.smapowner.Unlock()
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	if !p.startedUp.Load() {
		clone := smap.clone()
		clone.putNode(nsi, nonElectable)
		p.smapowner.put(clone)
		p.smapowner.Unlock()
		if !isProxy && selfRegister {
			glog.Infof("%s: joining %s (%s)", p.si, nsi, regReq.Smap)
		}
		return
	}
	p.smapowner.Unlock()

	// Return current metadata to registering target
	if !isProxy && selfRegister { // case: self-registering target
		glog.Infof("%s: joining %s (%s)", p.si, nsi, regReq.Smap)
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

		// Notify all nodes about a new one (targets probably need to set up GFN)
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
		if err := p.smapowner.persist(clone); err != nil {
			glog.Error(err)
		}
		msgInt := p.newActionMsgInternal(msg, clone, nil)
		p.setGlobRebID(clone, msgInt, true)
		p.smapowner.Unlock()
		tokens := p.authn.revokedTokenList()
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
	var pname = p.si.Name()
	if keepalive {
		if osi == nil {
			glog.Warningf("register/keepalive %s: adding back to the cluster map", nsi.Name())
			return true
		}

		if !osi.Equals(nsi) {
			if p.detectDaemonDuplicate(osi, nsi) {
				glog.Errorf("%s: %s(%s) is trying to register/keepalive with duplicate ID",
					pname, nsi.Name(), nsi.PublicNet.DirectURL)
				return false
			}
			glog.Warningf("%s: renewing registration %s (info changed!)", pname, nsi.Name())
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
			glog.Infof("%s: %s is already registered", pname, nsi.Name())
			return false
		}
		glog.Warningf("%s: renewing %s registration %+v => %+v", pname, nsi.Name(), osi, nsi)
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
	if status, err := p.unregisterNode(sid, msg); err != nil {
		p.invalmsghdlr(w, r, err.Error(), status)
	}
}

func (p *proxyrunner) unregisterNode(sid string, msg *cmn.ActionMsg) (status int, err error) {
	p.smapowner.Lock()
	defer p.smapowner.Unlock()
	var (
		smap  = p.smapowner.get()
		clone = smap.clone()
		node  = smap.GetNode(sid)
	)
	if node == nil {
		return http.StatusNotFound, fmt.Errorf("Unknown node %s", sid)
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
		return
	}
	if err = p.smapowner.persist(clone); err != nil {
		return
	}
	p.smapowner.put(clone)

	if isPrimary := p.smapowner.get().isPrimary(p.si); !isPrimary {
		return 0, fmt.Errorf("%s: is not a primary", p.si.Name())
	}
	msgInt := p.newActionMsgInternal(msg, clone, nil)
	p.setGlobRebID(clone, msgInt, true)
	p.metasyncer.sync(true, revspair{clone, msgInt})
	return
}

// '{"action": "shutdown"}' /v1/cluster => (proxy) =>
// '{"action": "syncsmap"}' /v1/cluster => (proxy) => PUT '{Smap}' /v1/daemon/syncsmap => target(s)
// '{"action": cmn.ActXactStart}' /v1/cluster
// '{"action": cmn.ActXactStop}' /v1/cluster
// '{"action": cmn.ActGlobalReb}' /v1/cluster => (proxy) => PUT '{Smap}' /v1/daemon/rebalance => target(s)
// '{"action": "setconfig"}' /v1/cluster => (proxy) =>
func (p *proxyrunner) httpcluput(w http.ResponseWriter, r *http.Request) {
	var (
		msg = &cmn.ActionMsg{}
	)
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
			results := p.bcastPut(bcastArgs{
				req: cmn.ReqArgs{
					Path:  cmn.URLPath(cmn.Version, cmn.Daemon, cmn.ActSetConfig),
					Query: query,
				},
				to: cluster.AllNodes,
			})
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
	if cmn.ReadJSON(w, r, msg) != nil {
		return
	}
	if p.forwardCP(w, r, msg, "", nil) {
		return
	}
	if msg.Action == cmn.ActXactStart && msg.Name /*kind*/ == cmn.ActGlobalReb {
		msg.Action = cmn.ActGlobalReb
	}
	// handle the action
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
		results := p.bcastTo(bcastArgs{
			req: cmn.ReqArgs{
				Method: http.MethodPut, Path: cmn.URLPath(cmn.Version, cmn.Daemon), Body: body},
			to: cluster.AllNodes,
		})
		for res := range results {
			if res.err != nil {
				s := fmt.Sprintf("%s: (%s = %s) failed, err: %s", msg.Action, msg.Name, value, res.details)
				p.invalmsghdlr(w, r, s)
				p.keepalive.onerr(err, res.status)
				return
			}
		}
	case cmn.ActShutdown:
		glog.Infoln("Proxy-controlled cluster shutdown...")
		body := cmn.MustMarshal(msg)
		args := bcastArgs{
			req: cmn.ReqArgs{Method: http.MethodPut, Path: cmn.URLPath(cmn.Version, cmn.Daemon), Body: body},
			to:  cluster.AllNodes,
		}
		p.bcastTo(args)
		time.Sleep(time.Second)
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	case cmn.ActGlobalReb:
		p.smapowner.Lock()
		smap := p.smapowner.get()
		msgInt := p.newActionMsgInternal(msg, smap, nil)
		p.setGlobRebID(smap, msgInt, true)
		p.smapowner.Unlock()
		p.metasyncer.sync(false, revspair{smap, msgInt})
	case cmn.ActXactStart, cmn.ActXactStop:
		body := cmn.MustMarshal(msg)
		results := p.bcastTo(bcastArgs{
			req: cmn.ReqArgs{
				Method: http.MethodPut,
				Path:   cmn.URLPath(cmn.Version, cmn.Daemon),
				Body:   body,
			},
		})
		for res := range results {
			if res.err != nil {
				p.invalmsghdlr(w, r, res.err.Error())
				return
			}
		}
	default:
		s := fmt.Sprintf(fmtUnknownAct, msg)
		p.invalmsghdlr(w, r, s)
	}
}

//========================
//
// broadcasts: Rx and Tx
//
//========================
func (p *proxyrunner) receiveBucketMD(newBMD *bucketMD, msgInt *actionMsgInternal, caller string) (err error) {
	var (
		pname = p.si.Name()
	)
	if glog.V(3) {
		s := fmt.Sprintf("receive %s", newBMD.StringEx())
		if msgInt.Action == "" {
			glog.Infoln(s)
		} else {
			glog.Infof("%s, action %s", s, msgInt.Action)
		}
	}
	p.bmdowner.Lock()
	bmd := p.bmdowner.get()
	if err = p.validateOriginBMD(bmd, nil, newBMD, caller); err != nil {
		cmn.Assert(!p.smapowner.get().isPrimary(p.si))
		// cluster integrity error: making exception for non-primary proxies
		glog.Errorf("%s (non-primary): %v - proceeding to override BMD", pname, err)
	} else if newBMD.version() <= bmd.version() {
		p.bmdowner.Unlock()
		if newBMD.version() < bmd.version() {
			err = fmt.Errorf("%s: attempt to downgrade %s to %s", pname, bmd, newBMD)
		}
		return
	}
	p.bmdowner.put(newBMD)
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

// Upon user request to recover bucket metadata, primary:
// 1. Broadcasts request to get target BMDs
// 2. Sorts results by BMD version in a descending order
// 3. Force=true: use BMD with highest version number as new BMD
//    Force=false: use targets' BMD only if they are of the same version
// 4. Set primary's BMD version to be greater than any target's one
// 4. Metasync the merged BMD
func (p *proxyrunner) recoverBuckets(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg) {
	var (
		origin       uint64
		rbmd         *bucketMD
		err          error
		bmds         map[*cluster.Snode]*bucketMD
		force, slowp bool
	)
	if p.forwardCP(w, r, msg, "recover-buckets", nil) {
		return
	}
	results := p.bcastGet(bcastArgs{
		req:     cmn.ReqArgs{Path: cmn.URLPath(cmn.Version, cmn.Buckets, cmn.AllBuckets)},
		timeout: cmn.GCO.Get().Timeout.Default,
	})
	bmds = make(map[*cluster.Snode]*bucketMD, len(results))
	for res := range results {
		var bmd = &bucketMD{}
		if res.err != nil || res.status >= http.StatusBadRequest {
			glog.Errorf("%v (%d: %s)", res.err, res.status, res.details)
			continue
		}
		if err = jsoniter.Unmarshal(res.outjson, bmd); err != nil {
			glog.Errorf("unexpected: failed to unmarshal %s from %s: %v", bmdTermName, res.si.Name(), err)
			continue
		}
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s from %s", bmd, res.si.Name())
		}
		if rbmd == nil { // 1. init
			origin, rbmd = bmd.Origin, bmd
		} else if origin != bmd.Origin { // 2. slow path
			slowp = true
		} else if !slowp && rbmd.Version < bmd.Version { // 3. fast path max(version)
			rbmd = bmd
		}
		bmds[res.si] = bmd
	}
	if slowp {
		force, _ = cmn.ParseBool(r.URL.Query().Get(cmn.URLParamForce))
		if rbmd, err = resolveOriginBMD(bmds); err != nil {
			_, split := err.(*errBmdOriginSplit)
			if !force || errors.Is(err, errNoBMD) || split {
				p.invalmsghdlr(w, r, err.Error())
				return
			}
			if _, ok := err.(*errTgtBmdOriginDiffer); ok {
				glog.Error(err.Error())
			}
		}
	}
	rbmd.Version += 100
	p.bmdowner.Lock()
	p.bmdowner.put(rbmd)
	p.bmdowner.Unlock()

	msgInt := p.newActionMsgInternal(msg, nil, rbmd)
	p.metasyncer.sync(true, revspair{rbmd, msgInt})
}

////////////////
// misc utils //
////////////////

func resolveOriginBMD(bmds map[*cluster.Snode]*bucketMD) (*bucketMD, error) {
	var (
		mlist = make(map[uint64][]targetRegMeta) // origin => list(targetRegMeta)
		maxor = make(map[uint64]*bucketMD)       // origin => max-ver BMD
	)
	// results => (mlist, maxor)
	for si, bmd := range bmds {
		if bmd.Version == 0 {
			continue
		}
		mlist[bmd.Origin] = append(mlist[bmd.Origin], targetRegMeta{nil, bmd, si})

		if rbmd, ok := maxor[bmd.Origin]; !ok {
			maxor[bmd.Origin] = bmd
		} else if rbmd.Version < bmd.Version {
			maxor[bmd.Origin] = bmd
		}
	}
	cmn.Assert(len(maxor) == len(mlist)) // TODO: remove
	if len(maxor) == 0 {
		return nil, errNoBMD
	}
	// by simple majority
	var origin, l = uint64(0), int(0)
	for o, lst := range mlist {
		if l < len(lst) {
			origin, l = o, len(lst)
		}
	}
	for o, lst := range mlist {
		if l == len(lst) && o != origin {
			s := fmt.Sprintf("%s: BMDs have different origins with no simple majority:\n%v", ciError(60), mlist)
			return nil, &errBmdOriginSplit{s}
		}
	}
	s := fmt.Sprintf("%s: BMDs have different origins with simple majority: ...%d:\n%v", ciError(70), origin%1000, mlist)
	bmd := maxor[origin]
	cmn.Assert(bmd.Origin != 0)
	return bmd, &errTgtBmdOriginDiffer{s}
}

func requiresRemirroring(prevm, newm cmn.MirrorConf) bool {
	if !prevm.Enabled && newm.Enabled {
		return true
	}
	if prevm.Enabled && newm.Enabled {
		return prevm.Copies != newm.Copies
	}
	return false
}

func ciError(num int) string {
	const s = "[%s%d - for details, see %s/blob/master/docs/troubleshooting.md]"
	return fmt.Sprintf(s, ciePrefix, num, githubHome)
}
