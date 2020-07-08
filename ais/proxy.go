// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/xaction"
	jsoniter "github.com/json-iterator/go"
)

const (
	whatRenamedLB = "renamedlb"
	fmtNotCloud   = "%q appears to be ais bucket (expecting cloud)"
	fmtUnknownAct = "unexpected action message <- JSON [%v]"
	fmtUnknownQue = "unexpected query [what=%s]"
	ciePrefix     = "cluster integrity error: cie#"
	githubHome    = "https://github.com/NVIDIA/aistore"
	listBuckets   = "listBuckets"
)

type (
	ClusterMountpathsRaw struct {
		Targets cmn.JSONRawMsgs `json:"targets"`
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
		metasyncer *metasyncer
		rproxy     reverseProxy
		notifs     notifs
		jtx        *jtx
		gmm        *memsys.MMSA // system pagesize-based memory manager and slab allocator
	}
	remBckAddArgs struct {
		p        *proxyrunner
		w        http.ResponseWriter
		r        *http.Request
		queryBck *cluster.Bck
		err      error
		msg      *cmn.ActionMsg
	}
)

func (rp *reverseProxy) init() {
	cfg := cmn.GCO.Get()
	if cfg.Net.HTTP.RevProxy == cmn.RevProxyCloud {
		rp.cloud = &httputil.ReverseProxy{
			Director: func(r *http.Request) {},
			Transport: cmn.NewTransport(cmn.TransportArgs{
				UseHTTPS:   cfg.Net.HTTP.UseHTTPS,
				SkipVerify: cfg.Net.HTTP.SkipVerify,
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
	p.owner.bmd = newBMDOwnerPrx(config)

	p.httprunner.keepalive = getproxykeepalive()

	p.owner.bmd.init() // initialize owner and load BMD
	p.metasyncer = getmetasyncer()

	cluster.InitProxy()

	// startup sequence - see earlystart.go for the steps and commentary
	p.bootstrap()

	p.authn = &authManager{
		tokens:        make(map[string]*cmn.AuthToken),
		revokedTokens: make(map[string]bool),
		version:       1,
	}

	p.rproxy.init()
	initListObjectsCache(p)

	p.notifs.init(p)
	p.jtx = newJTX(p)

	//
	// REST API: register proxy handlers and start listening
	//

	networkHandlers := []networkHandler{
		{r: cmn.Reverse, h: p.reverseHandler, net: []string{cmn.NetworkPublic}},

		{r: cmn.Buckets, h: p.bucketHandler, net: []string{cmn.NetworkPublic}},
		{r: cmn.Objects, h: p.objectHandler, net: []string{cmn.NetworkPublic}},
		{r: cmn.Download, h: p.downloadHandler, net: []string{cmn.NetworkPublic}},
		{r: cmn.Daemon, h: p.daemonHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl}},
		{r: cmn.Cluster, h: p.clusterHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl}},
		{r: cmn.Tokens, h: p.tokenHandler, net: []string{cmn.NetworkPublic}},
		{r: cmn.Sort, h: p.dsortHandler, net: []string{cmn.NetworkPublic}},
		{r: cmn.Query, h: p.queryHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl}},

		{r: cmn.Metasync, h: p.metasyncHandler, net: []string{cmn.NetworkIntraControl}},
		{r: cmn.Health, h: p.healthHandler, net: []string{cmn.NetworkIntraControl}},
		{r: cmn.Vote, h: p.voteHandler, net: []string{cmn.NetworkIntraControl}},

		{r: cmn.Notifs, h: p.notifs.handler, net: []string{cmn.NetworkIntraControl}},
		{r: cmn.Jtx, h: p.jtx.handler, net: []string{cmn.NetworkIntraControl}},

		{r: "/" + cmn.S3, h: p.s3Handler, net: []string{cmn.NetworkPublic}},
	}

	// Public network
	if config.Net.HTTP.RevProxy == cmn.RevProxyCloud {
		networkHandlers = append(networkHandlers, networkHandler{
			r: "/", h: p.reverseProxyHandler,
			net: []string{cmn.NetworkPublic},
		})
	} else {
		networkHandlers = append(networkHandlers, networkHandler{
			r: "/", h: cmn.InvalidHandler,
			net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl, cmn.NetworkIntraData},
		})
	}

	p.registerNetworkHandlers(networkHandlers)

	glog.Infof("%s: [public net] listening on: %s", p.si, p.si.PublicNet.DirectURL)
	if p.si.PublicNet.DirectURL != p.si.IntraControlNet.DirectURL {
		glog.Infof("%s: [intra control net] listening on: %s", p.si, p.si.IntraControlNet.DirectURL)
	}
	if p.si.PublicNet.DirectURL != p.si.IntraDataNet.DirectURL {
		glog.Infof("%s: [intra data net] listening on: %s", p.si, p.si.IntraDataNet.DirectURL)
	}
	if config.Net.HTTP.RevProxy != "" {
		glog.Warningf("Warning: serving GET /object as a reverse-proxy (%q)", config.Net.HTTP.RevProxy)
	}

	dsort.RegisterNode(p.owner.smap, p.owner.bmd, p.si, nil, nil, p.statsT)
	return p.httprunner.run()
}

func (p *proxyrunner) sendKeepalive(timeout time.Duration) (status int, err error) {
	smap := p.owner.smap.get()
	if smap != nil && smap.isPrimary(p.si) {
		return
	}
	return p.httprunner.sendKeepalive(timeout)
}

func (p *proxyrunner) joinCluster(primaryURLs ...string) (status int, err error) {
	var query url.Values
	if smap := p.owner.smap.get(); smap.isPrimary(p.si) {
		return 0, fmt.Errorf("%s should not be joining: is primary, %s", p.si, smap)
	}
	if cmn.GCO.Get().Proxy.NonElectable {
		query = url.Values{cmn.URLParamNonElectable: []string{"true"}}
	}
	res := p.join(query, primaryURLs...)
	if res.err != nil {
		return res.status, res.err
	}
	// not being sent at cluster startup and keepalive
	if len(res.outjson) == 0 {
		return
	}
	err = p.applyRegMeta(res.outjson, "")
	return
}

func (p *proxyrunner) applyRegMeta(body []byte, caller string) (err error) {
	var regMeta nodeRegMeta
	err = jsoniter.Unmarshal(body, &regMeta)
	if err != nil {
		return fmt.Errorf("unexpected: %s failed to unmarshal reg-meta, err: %v", p.si, err)
	}

	// BMD
	msg := p.newAisMsgStr(cmn.ActRegTarget, regMeta.Smap, regMeta.BMD)
	if err = p.receiveBMD(regMeta.BMD, msg, caller); err != nil {
		glog.Infof("%s: %s", p.si, p.owner.bmd.get())
	}
	// Smap
	if err := p.owner.smap.synchronize(regMeta.Smap, true /* lesserIsErr */); err != nil {
		glog.Errorf("%s: sync Smap err %v", p.si, err)
	} else {
		glog.Infof("%s: sync %s", p.si, p.owner.smap.get())
	}
	return
}

func (p *proxyrunner) unregisterSelf() (int, error) {
	smap := p.owner.smap.get()
	args := callArgs{
		si: smap.ProxySI,
		req: cmn.ReqArgs{
			Method: http.MethodDelete,
			Path:   cmn.URLPath(cmn.Version, cmn.Cluster, cmn.Daemon, p.si.ID()),
		},
		timeout: cmn.DefaultTimeout,
	}
	res := p.call(args)
	return res.status, res.err
}

// stop gracefully
func (p *proxyrunner) Stop(err error) {
	var isPrimary bool
	smap := p.owner.smap.get()
	if smap != nil { // in tests
		isPrimary = smap.isPrimary(p.si)
	}
	glog.Infof("Stopping %s (%s, primary=%t), err: %v", p.GetRunName(), p.si, isPrimary, err)
	hk.Abort()
	xaction.Registry.AbortAll()

	if isPrimary {
		// give targets and non primary proxies some time to unregister
		version := smap.version()
		for i := 0; i < 20; i++ {
			time.Sleep(time.Second)
			v := p.owner.smap.get().version()
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
	if p.owner.smap.get().CountTargets() < 1 {
		p.invalmsghdlr(w, r, "No registered targets yet")
		return
	}

	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}

	switch apiItems[0] {
	case cmn.AllBuckets:
		if err := p.checkPermissions(r, nil, cmn.AccessBckLIST); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		bck, err := newBckFromQuery("", r.URL.Query())
		if err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
			return
		}
		p.listBuckets(w, r, cmn.QueryBcks(bck.Bck))
	default:
		p.invalmsghdlrf(w, r, "Invalid route /buckets/%s", apiItems[0])
	}
}

// Used for redirecting ReverseProxy GCP/AWS GET request to target
func (p *proxyrunner) objGetRProxy(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	apitems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objName := apitems[0], apitems[1]
	bck, err := newBckFromQuery(bucket, r.URL.Query())
	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	if err = bck.Init(p.owner.bmd, p.si); err != nil {
		args := remBckAddArgs{p: p, w: w, r: r, queryBck: bck, err: err}
		if _, err = args.try(); err != nil {
			return
		}
	}
	smap := p.owner.smap.get()
	si, err := cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
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
	bucket, objName := apitems[0], apitems[1]
	bck, err := newBckFromQuery(bucket, r.URL.Query())
	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	if err = bck.Init(p.owner.bmd, p.si); err != nil {
		args := remBckAddArgs{p: p, w: w, r: r, queryBck: bck, err: err}
		if bck, err = args.try(); err != nil {
			return
		}
	}
	if err := p.checkPermissions(r, &bck.Bck, cmn.AccessGET); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
		return
	}
	if err := bck.Allow(cmn.AccessGET); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
		return
	}
	smap := p.owner.smap.get()
	si, err := cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	config := cmn.GCO.Get()
	if config.Net.HTTP.RevProxy == cmn.RevProxyTarget {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("reverse-proxy: %s %s/%s <= %s", r.Method, bucket, objName, si)
		}
		p.reverseNodeRequest(w, r, si)
		delta := time.Since(started)
		p.statsT.Add(stats.GetLatency, int64(delta))
	} else {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s %s/%s => %s", r.Method, bucket, objName, si)
		}
		redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraData)
		http.Redirect(w, r, redirectURL, http.StatusMovedPermanently)
	}
	p.statsT.Add(stats.GetCount, 1)
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
	bck, err := newBckFromQuery(bucket, r.URL.Query())
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if err = bck.Init(p.owner.bmd, p.si); err != nil {
		args := remBckAddArgs{p: p, w: w, r: r, queryBck: bck, err: err}
		if bck, err = args.try(); err != nil {
			return
		}
	}

	var (
		si       *cluster.Snode
		nodeID   string
		smap     = p.owner.smap.get()
		appendTy = query.Get(cmn.URLParamAppendType)
	)
	if appendTy == "" {
		if err := p.checkPermissions(r, &bck.Bck, cmn.AccessPUT); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		err = bck.Allow(cmn.AccessPUT)
	} else {
		if err := p.checkPermissions(r, &bck.Bck, cmn.AccessAPPEND); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		var hi handleInfo
		hi, err = parseAppendHandle(query.Get(cmn.URLParamAppendHandle))
		if err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		nodeID = hi.nodeID
		err = bck.Allow(cmn.AccessAPPEND)
	}

	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
		return
	}

	if nodeID == "" {
		si, err = cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
		if err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
	} else {
		si = smap.GetTarget(nodeID)
		if si == nil {
			err = &errNodeNotFound{"PUT failure", nodeID, p.si, smap}
			p.invalmsghdlr(w, r, err.Error())
			return
		}
	}

	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s/%s => %s (append: %v)", r.Method, bucket, objName, si, appendTy != "")
	}
	redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraData)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)

	if appendTy == "" {
		p.statsT.Add(stats.PutCount, 1)
	} else {
		p.statsT.Add(stats.AppendCount, 1)
	}
}

// DELETE /v1/objects/bucket-name/object-name
func (p *proxyrunner) httpobjdelete(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	apitems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objName := apitems[0], apitems[1]
	bck, err := newBckFromQuery(bucket, r.URL.Query())
	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	if err = bck.Init(p.owner.bmd, p.si); err != nil {
		args := remBckAddArgs{p: p, w: w, r: r, queryBck: bck, err: err}
		if bck, err = args.try(); err != nil {
			return
		}
	}
	if err := p.checkPermissions(r, &bck.Bck, cmn.AccessObjDELETE); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
		return
	}
	if err = bck.Allow(cmn.AccessObjDELETE); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
		return
	}
	smap := p.owner.smap.get()
	si, err := cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s/%s => %s", r.Method, bucket, objName, si)
	}
	redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)

	p.statsT.Add(stats.DeleteCount, 1)
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
	bck, err := newBckFromQuery(bucket, r.URL.Query())
	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	if err = bck.Init(p.owner.bmd, p.si); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusNotFound)
		return
	}
	if err = bck.Allow(cmn.AccessBckDELETE); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
		return
	}
	switch msg.Action {
	case cmn.ActEvictCB:
		if bck.IsAIS() {
			p.invalmsghdlrf(w, r, fmtNotCloud, bucket)
			return
		}
		fallthrough // fallthrough
	case cmn.ActDestroyLB:
		if p.forwardCP(w, r, &msg, bucket, nil) {
			return
		}
		if err := p.checkPermissions(r, &bck.Bck, cmn.AccessBckDELETE); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		if bck.IsRemoteAIS() {
			if err := p.reverseReqRemote(w, r, &msg, bck.Bck); err != nil {
				return
			}
		}
		if err := p.destroyBucket(&msg, bck); err != nil {
			if _, ok := err.(*cmn.ErrorBucketDoesNotExist); ok { // race
				glog.Infof("%s: %s already %q-ed, nothing to do", p.si, bck, msg.Action)
			} else {
				p.invalmsghdlr(w, r, err.Error())
			}
		}
	case cmn.ActDelete, cmn.ActEvictObjects:
		if err := p.checkPermissions(r, &bck.Bck, cmn.AccessObjDELETE); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		if msg.Action == cmn.ActEvictObjects && bck.IsAIS() {
			p.invalmsghdlrf(w, r, fmtNotCloud, bucket)
			return
		}
		if err = p.doListRange(http.MethodDelete, bucket, &msg, r.URL.Query()); err != nil {
			p.invalmsghdlr(w, r, err.Error())
		}
	default:
		p.invalmsghdlrf(w, r, fmtUnknownAct, msg)
	}
}

// PUT /v1/metasync
func (p *proxyrunner) metasyncHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPut {
		p.invalmsghdlrf(w, r, "invalid method %s", r.Method)
		return
	}
	smap := p.owner.smap.get()
	if smap.isPrimary(p.si) {
		vote := xaction.Registry.IsXactRunning(xaction.RegistryXactFilter{Kind: cmn.ActElection})
		p.invalmsghdlrf(w, r, "primary %s cannot receive cluster meta (election=%t)", p.si, vote)
		return
	}
	var payload = make(msPayload)
	if err := jsp.Decode(r.Body, &payload, jsp.CCSign(), "metasync put"); err != nil {
		cmn.InvalidHandlerDetailed(w, r, err.Error())
		return
	}

	var (
		errs   []error
		caller = r.Header.Get(cmn.HeaderCallerName)
	)

	newSmap, _, err := p.extractSmap(payload, caller)
	if err != nil {
		errs = append(errs, err)
	} else if newSmap != nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("new %s from %s", newSmap.StringEx(), caller)
		}
		err = p.owner.smap.synchronize(newSmap, true /* lesserIsErr */)
		if err != nil {
			errs = append(errs, err)
			goto ExtractRMD
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
	}

ExtractRMD:
	newRMD, msgRMD, err := p.extractRMD(payload)
	if err != nil {
		errs = append(errs, err)
	} else if newRMD != nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("new %s from %s", newRMD.String(), caller)
		}
		if err = p.receiveRMD(newRMD, msgRMD); err != nil {
			errs = append(errs, err)
		}
	}

	newBMD, msgBMD, err := p.extractBMD(payload)
	if err != nil {
		errs = append(errs, err)
	} else if newBMD != nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("new %s from %s", newBMD.StringEx(), caller)
		}
		if err = p.receiveBMD(newBMD, msgBMD, caller); err != nil {
			errs = append(errs, err)
		}
	}

	revokedTokens, err := p.extractRevokedTokenList(payload)
	if err != nil {
		errs = append(errs, err)
	} else {
		p.authn.updateRevokedList(revokedTokens)
	}

	if len(errs) > 0 {
		p.invalmsghdlrf(w, r, "%v", errs)
		return
	}
}

// GET /v1/health
// TODO: split/separate ais-internal vs external calls
func (p *proxyrunner) healthHandler(w http.ResponseWriter, r *http.Request) {
	if !p.NodeStarted() {
		// respond with 503 as per https://tools.ietf.org/html/rfc7231#section-6.6.4
		// see also:
		// https://kubernetes.io/docs/tasks/configure-pod-container/configure-liveness-readiness-startup-probes
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	var (
		query   = r.URL.Query()
		smap    = p.owner.smap.get()
		primary = smap != nil && smap.version() > 0 && smap.isPrimary(p.si)
		getCii  = cmn.IsParseBool(query.Get(cmn.URLParamClusterInfo))
	)
	// NOTE: internal use
	if getCii {
		cii := &clusterInfo{}
		cii.fill(p)
		_ = p.writeJSON(w, r, cii, "cluster-info")
		return
	}
	// non-primary will keep returning 503 until cluster starts up
	if !primary && !p.ClusterStarted() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (cii *clusterInfo) fill(p *proxyrunner) {
	var (
		bmd  = p.owner.bmd.get()
		smap = p.owner.smap.get()
	)
	cii.BMD.Version = bmd.version()
	cii.BMD.UUID = bmd.UUID
	cii.Smap.Version = smap.version()
	cii.Smap.UUID = smap.UUID
	cii.Smap.PrimaryURL = smap.ProxySI.IntraControlNet.DirectURL
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

	// 1. "all buckets"
	if len(apiItems) == 0 {
		if err := p.checkPermissions(r, nil, cmn.AccessBckLIST); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}

		switch msg.Action {
		case cmn.ActRecoverBck:
			p.recoverBuckets(w, r, &msg)
		case cmn.ActSyncLB:
			if p.forwardCP(w, r, &msg, "", nil) {
				return
			}
			bmd := p.owner.bmd.get()
			msg := p.newAisMsg(&msg, nil, bmd)
			_ = p.metasyncer.sync(revsPair{bmd, msg})
		case cmn.ActSummaryBucket:
			bck, err := newBckFromQuery("", r.URL.Query())
			if err != nil {
				p.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
				return
			}
			// bck might be a query bck..
			if err = bck.Init(p.owner.bmd, p.si); err == nil {
				if err = bck.Allow(cmn.AccessBckHEAD); err != nil {
					p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
					return
				}
			}
			p.bucketSummary(w, r, bck, msg)
		default:
			p.invalmsghdlr(w, r, "URL path is too short: expecting bucket name")
		}
		return
	}

	bucket := apiItems[0]
	bck, err := newBckFromQuery(bucket, r.URL.Query())
	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}

	if bck.Bck.IsRemoteAIS() {
		// forward to remote AIS as is, with a few distinct exceptions
		switch msg.Action {
		case cmn.ActListObjects, cmn.ActInvalListCache:
			break
		default:
			p.reverseReqRemote(w, r, &msg, bck.Bck)
			return
		}
	}

	// 3. createlb
	if msg.Action == cmn.ActCreateLB {
		if err := p.checkPermissions(r, nil, cmn.AccessBckCREATE); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		if err = cmn.ValidateBckName(bucket); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		if bck.Bck.IsCloud(cmn.AnyCloud) {
			p.invalmsghdlrf(w, r, fmtErr, msg.Action, bck.Provider)
			return
		}
		if p.forwardCP(w, r, &msg, bucket, nil) {
			return
		}
		bck.Provider = cmn.ProviderAIS
		if msg.Value != nil {
			propsToUpdate := cmn.BucketPropsToUpdate{}
			if err := cmn.MorphMarshal(msg.Value, &propsToUpdate); err != nil {
				p.invalmsghdlr(w, r, err.Error())
				return
			}
			bck.Props = cmn.DefaultBucketProps()
			bck.Props.Provider = bck.Provider
			// make and validate nprops
			bck.Props, _, _, err = p.makeNprops(bck, propsToUpdate, true /*creating*/)
			if err != nil {
				p.invalmsghdlr(w, r, err.Error())
				return
			}
		}
		if err := p.createBucket(&msg, bck); err != nil {
			errCode := http.StatusInternalServerError
			if _, ok := err.(*cmn.ErrorBucketAlreadyExists); ok {
				errCode = http.StatusConflict
			}
			p.invalmsghdlr(w, r, err.Error(), errCode)
		}
		return
	}
	// only the primary can do metasync
	xactDtor := cmn.XactsDtor[msg.Action]
	if xactDtor.Metasync {
		if p.forwardCP(w, r, &msg, bucket, nil) {
			return
		}
	}

	if err = bck.Init(p.owner.bmd, p.si); err != nil {
		_, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist)
		if ok && msg.Action == cmn.ActRenameLB {
			p.invalmsghdlrstatusf(w, r, http.StatusNotFound, "cannot %q: ais bucket %q does not exist",
				msg.Action, bucket)
			return
		}
		args := remBckAddArgs{p: p, w: w, r: r, queryBck: bck, err: err, msg: &msg}
		if bck, err = args.try(); err != nil {
			return
		}
	}

	// 4. {action} on bucket
	switch msg.Action {
	case cmn.ActRenameLB:
		if err := p.checkPermissions(r, &bck.Bck, cmn.AccessBckRENAME); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		if !bck.IsAIS() {
			p.invalmsghdlrf(w, r, fmtErr, msg.Action, bck.Provider)
			return
		}
		if err = bck.Allow(cmn.AccessBckRENAME); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
			return
		}
		bckFrom, bucketTo := bck, msg.Name
		if bucket == bucketTo {
			p.invalmsghdlrf(w, r, "cannot rename bucket %q as %q", bucket, bucket)
			return
		}
		if err := cmn.ValidateBckName(bucketTo); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		bckTo := cluster.NewBck(bucketTo, cmn.ProviderAIS, cmn.NsGlobal)
		if _, present := p.owner.bmd.get().Get(bckTo); present {
			err := cmn.NewErrorBucketAlreadyExists(bckTo.Bck, p.si.String())
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		glog.Infof("%s bucket %s => %s", msg.Action, bckFrom, bucketTo)
		if err := p.renameBucket(bckFrom, bckTo, &msg); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
	case cmn.ActCopyBucket:
		if err := p.checkPermissions(r, &bck.Bck, cmn.AccessGET); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		bckFrom, bucketTo := bck, msg.Name
		if bucket == bucketTo {
			p.invalmsghdlrf(w, r, "cannot copy bucket %q onto itself", bucket)
			return
		}
		if err := cmn.ValidateBckName(bucketTo); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		glog.Infof("%s bucket %s => %s", msg.Action, bckFrom, bucketTo)

		// NOTE: destination MUST be AIS; TODO: support destination namespace via API
		bckTo := cluster.NewBck(bucketTo, cmn.ProviderAIS, cmn.NsGlobal)
		bmd := p.owner.bmd.get()
		if _, present := bmd.Get(bckTo); present {
			if err = bckTo.Init(p.owner.bmd, p.si); err == nil {
				if err = bckTo.Allow(cmn.AccessSYNC); err != nil {
					p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
					return
				}
			}
		}

		if err := p.copyBucket(bckFrom, bckTo, &msg); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
	case cmn.ActRegisterCB:
		// TODO: choose the best permission
		if err := p.checkPermissions(r, &bck.Bck, cmn.AccessBckCREATE); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		cloudConf := cmn.GCO.Get().Cloud
		bck.Provider = cloudConf.Provider
		bck.Ns = cloudConf.Ns
		if err := p.createBucket(&msg, bck); err != nil {
			errCode := http.StatusInternalServerError
			if _, ok := err.(*cmn.ErrorBucketAlreadyExists); ok {
				errCode = http.StatusConflict
			}
			p.invalmsghdlr(w, r, err.Error(), errCode)
			return
		}
	case cmn.ActPrefetch:
		// TODO: GET vs SYNC?
		if err := p.checkPermissions(r, &bck.Bck, cmn.AccessGET); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		if bck.IsAIS() {
			p.invalmsghdlrf(w, r, fmtNotCloud, bucket)
			return
		}
		if err = bck.Allow(cmn.AccessSYNC); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
			return
		}
		if err = p.doListRange(http.MethodPost, bucket, &msg, r.URL.Query()); err != nil {
			p.invalmsghdlr(w, r, err.Error())
		}
	case cmn.ActListObjects:
		begin := mono.NanoTime()
		if err := p.checkPermissions(r, &bck.Bck, cmn.AccessObjLIST); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		if err = bck.Allow(cmn.AccessObjLIST); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
			return
		}
		p.listObjectsAndCollectStats(w, r, bck, msg, begin, false /* fast listing */)
	case cmn.ActInvalListCache:
		if err = bck.Allow(cmn.AccessObjLIST); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
			return
		}
		p.invalidateListAISBucketCache(w, r, bck, msg)
	case cmn.ActSummaryBucket:
		if err := p.checkPermissions(r, &bck.Bck, cmn.AccessObjLIST); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		if err = bck.Allow(cmn.AccessBckHEAD); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
			return
		}
		p.bucketSummary(w, r, bck, msg)
	case cmn.ActMakeNCopies:
		if err := p.checkPermissions(r, &bck.Bck, cmn.AccessMAKENCOPIES); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		if err = bck.Allow(cmn.AccessMAKENCOPIES); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
			return
		}
		if err = p.makeNCopies(&msg, bck); err != nil {
			p.invalmsghdlr(w, r, err.Error())
		}
	case cmn.ActECEncode:
		if err := p.checkPermissions(r, &bck.Bck, cmn.AccessEC); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		if err = bck.Allow(cmn.AccessEC); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
			return
		}
		if err := p.ecEncode(bck, &msg); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
	default:
		p.invalmsghdlrf(w, r, fmtUnknownAct, msg)
	}
}

func (p *proxyrunner) listObjectsAndCollectStats(w http.ResponseWriter, r *http.Request, bck *cluster.Bck,
	amsg cmn.ActionMsg, begin int64, fast bool) {
	var (
		initRespMsg *cmn.InitTaskRespMsg
		err         error
		bckList     *cmn.BucketList
		smsg        = cmn.SelectMsg{}
		query       = r.URL.Query()
	)
	if err := cmn.MorphMarshal(amsg.Value, &smsg); err != nil {
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

	id := r.URL.Query().Get(cmn.URLParamUUID)
	if id != "" {
		smsg.UUID = id
	}
	if bck.IsAIS() || smsg.Cached {
		bckList, initRespMsg, err = p.listAISBucket(bck, smsg)
	} else {
		// NOTE: For async tasks, user must check for StatusAccepted and use returned uuid.
		bckList, initRespMsg, _, err = p.listObjectsRemote(bck, smsg)
		// TODO: `status == http.StatusGone` At this point we know that this
		//  cloud bucket exists and is offline. We should somehow try to list
		//  cached objects. This isn't easy as we basically need to start a new
		//  xaction and return new `UUID`.
	}
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	// uuid == "" means that async runner has completed and the result is available
	// otherwise it is an ID of a still running task
	if initRespMsg != nil {
		w.WriteHeader(http.StatusAccepted)
		w.Write(cmn.MustMarshal(initRespMsg))
		return
	}

	// TODO: replace cmn.Marshal with p.writeJSON
	b := cmn.MustMarshal(bckList)
	pageMarker := bckList.PageMarker
	// free memory allocated for temporary slice immediately as it can take up to a few GB
	bckList.Entries = bckList.Entries[:0]
	bckList.Entries = nil
	bckList = nil

	if p.writeJSONBytes(w, r, b, "list_objects") {
		delta := mono.Since(begin)
		p.statsT.AddMany(
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

func (p *proxyrunner) invalidateListAISBucketCache(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, amsg cmn.ActionMsg) {
	smsg := cmn.SelectMsg{}
	if err := cmn.MorphMarshal(amsg.Value, &smsg); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	listCache.discard(&smsg, bck)
}

// bucket == "": all buckets for a given provider
func (p *proxyrunner) bucketSummary(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, amsg cmn.ActionMsg) {
	var (
		initRespMsg *cmn.InitTaskRespMsg
		err         error
		summaries   cmn.BucketsSummaries
		smsg        = cmn.SelectMsg{}
	)
	listMsgJSON := cmn.MustMarshal(amsg.Value)
	if err := jsoniter.Unmarshal(listMsgJSON, &smsg); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	id := r.URL.Query().Get(cmn.URLParamUUID)
	if id != "" {
		smsg.UUID = id
	}

	if summaries, initRespMsg, err = p.gatherBucketSummary(bck, smsg); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	// uuid == "" means that async runner has completed and the result is available
	// otherwise it is an ID of a still running task
	if initRespMsg != nil {
		w.WriteHeader(http.StatusAccepted)
		w.Write(cmn.MustMarshal(initRespMsg))
		return
	}

	p.writeJSON(w, r, summaries, "bucket_summary")
}

func (p *proxyrunner) gatherBucketSummary(bck *cluster.Bck, smsg cmn.SelectMsg) (
	summaries cmn.BucketsSummaries, initRespMsg *cmn.InitTaskRespMsg, err error) {
	var (
		// start new async task if client did not provide taskID(neither in Headers nor in SelectMsg),
		isNew, q = p.initAsyncQuery(bck, &smsg, cmn.GenUUID())
		config   = cmn.GCO.Get()
	)
	var (
		smap   = p.owner.smap.get()
		aisMsg = p.newAisMsg(&cmn.ActionMsg{Action: cmn.ActSummaryBucket, Value: &smsg}, smap, nil)
		body   = cmn.MustMarshal(aisMsg)
		args   = bcastArgs{
			req: cmn.ReqArgs{
				Path:  cmn.URLPath(cmn.Version, cmn.Buckets, bck.Name),
				Query: q,
				Body:  body,
			},
			smap:    smap,
			timeout: config.Timeout.MaxHostBusy + config.Timeout.CplaneOperation,
			to:      cluster.Targets,
		}
	)
	results := p.bcastPost(args)
	allOK, _, err := p.checkBckTaskResp(smsg.UUID, results)
	if err != nil {
		return nil, nil, err
	}

	// some targets are still executing their tasks or it is request to start
	// an async task. The proxy returns only uuid to a caller
	if !allOK || isNew {
		return nil, &cmn.InitTaskRespMsg{UUID: smsg.UUID}, nil
	}

	// all targets are ready, prepare the final result
	q = url.Values{}
	q = cmn.AddBckToQuery(q, bck.Bck)
	q.Set(cmn.URLParamTaskAction, cmn.TaskResult)
	q.Set(cmn.URLParamSilent, "true")
	args.req.Query = q
	summaries = make(cmn.BucketsSummaries, 0)
	for result := range p.bcastPost(args) {
		if result.err != nil {
			return nil, nil, result.err
		}

		var targetSummary cmn.BucketsSummaries
		if err := jsoniter.Unmarshal(result.outjson, &targetSummary); err != nil {
			return nil, nil, err
		}

		for _, bckSummary := range targetSummary {
			summaries = summaries.Aggregate(bckSummary)
		}
	}

	return summaries, nil, nil
}

// POST { action } /v1/objects/bucket-name
func (p *proxyrunner) httpobjpost(w http.ResponseWriter, r *http.Request) {
	var msg cmn.ActionMsg
	apitems, err := p.checkRESTItems(w, r, 1, true, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket := apitems[0]
	bck, err := newBckFromQuery(bucket, r.URL.Query())
	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	if err = bck.Init(p.owner.bmd, p.si); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case cmn.ActRenameObject:
		if err := p.checkPermissions(r, &bck.Bck, cmn.AccessObjRENAME); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		if !bck.IsAIS() {
			p.invalmsghdlrf(w, r, "%q is not supported for Cloud buckets: %s", msg.Action, bck)
			return
		}
		if err = bck.Allow(cmn.AccessObjRENAME); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
			return
		}
		if bck.Props.EC.Enabled {
			p.invalmsghdlrf(w, r, "%q is not supported for erasure-coded buckets: %s", msg.Action, bck)
			return
		}
		p.objRename(w, r, bck)
		return
	case cmn.ActPromote:
		if err := p.checkPermissions(r, &bck.Bck, cmn.AccessPROMOTE); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		if err = bck.Allow(cmn.AccessPROMOTE); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
			return
		}
		p.promoteFQN(w, r, bck, &msg)
		return
	default:
		p.invalmsghdlrf(w, r, fmtUnknownAct, msg)
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
	bck, err := newBckFromQuery(bucket, r.URL.Query())
	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	if err = bck.Init(p.owner.bmd, p.si); err != nil {
		if _, ok := err.(*cmn.ErrorBucketDoesNotExist); ok {
			p.invalmsghdlr(w, r, err.Error(), http.StatusNotFound)
			return
		}
		args := remBckAddArgs{p: p, w: w, r: r, queryBck: bck, err: err}
		if _, err = args.try(); err != nil {
			return
		}
	}
	if err := p.checkPermissions(r, &bck.Bck, cmn.AccessBckHEAD); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
		return
	}
	if bck.IsAIS() {
		p.bucketPropsToHdr(bck, w.Header())
		return
	}
	si, err := p.owner.smap.get().GetRandTarget()
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

// PATCH /v1/buckets/bucket-name
func (p *proxyrunner) httpbckpatch(w http.ResponseWriter, r *http.Request) {
	var (
		propsToUpdate cmn.BucketPropsToUpdate
		bucket        string
		bck           *cluster.Bck
		msg           *cmn.ActionMsg
		apitems, err  = p.checkRESTItems(w, r, 1, true, cmn.Version, cmn.Buckets)
	)
	if err != nil {
		return
	}
	bucket = apitems[0]
	if bck, err = newBckFromQuery(bucket, r.URL.Query()); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	if p.forwardCP(w, r, msg, "httpbckpatch", nil) {
		return
	}
	msg = &cmn.ActionMsg{Value: &propsToUpdate}
	if err = cmn.ReadJSON(w, r, msg); err != nil {
		return
	}
	if err = bck.Init(p.owner.bmd, p.si); err != nil {
		args := remBckAddArgs{p: p, w: w, r: r, queryBck: bck, err: err, msg: msg}
		if bck, err = args.try(); err != nil {
			return
		}
	}
	if err := p.checkPermissions(r, &bck.Bck, cmn.AccessPATCH); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
		return
	}
	if err := bck.Allow(cmn.AccessPATCH); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
		return
	}
	if err = p.checkAction(msg, cmn.ActSetBprops, cmn.ActResetBprops); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if err = p.setBucketProps(msg, bck, propsToUpdate); err != nil {
		p.invalmsghdlr(w, r, err.Error())
	}
}

// HEAD /v1/objects/bucket-name/object-name
func (p *proxyrunner) httpobjhead(w http.ResponseWriter, r *http.Request) {
	var (
		started      = time.Now()
		query        = r.URL.Query()
		checkExists  = cmn.IsParseBool(query.Get(cmn.URLParamCheckExists))
		apitems, err = p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	)
	if err != nil {
		return
	}
	bucket, objName := apitems[0], apitems[1]
	bck, err := newBckFromQuery(bucket, r.URL.Query())
	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	if err = bck.Init(p.owner.bmd, p.si); err != nil {
		args := remBckAddArgs{p: p, w: w, r: r, queryBck: bck, err: err}
		if bck, err = args.try(); err != nil {
			return
		}
	}
	if err := p.checkPermissions(r, &bck.Bck, cmn.AccessObjHEAD); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
		return
	}
	if err := bck.Allow(cmn.AccessObjHEAD); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
		return
	}
	smap := p.owner.smap.get()
	si, err := cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusInternalServerError)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s/%s => %s", r.Method, bucket, objName, si)
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
	smap := p.owner.smap.get()
	if smap == nil || !smap.isValid() {
		if msg != nil {
			s = fmt.Sprintf(`%s must be starting up: cannot execute "%s:%s"`, p.si, msg.Action, s)
		} else {
			s = fmt.Sprintf("%s must be starting up: cannot execute %q", p.si, s)
		}
		p.invalmsghdlr(w, r, s)
		return true
	}
	if smap.isPrimary(p.si) {
		return
	}
	if r.Method == http.MethodHead {
		// We must **not** send any request body when doing HEAD request.
		// Otherwise, the request can be rejected and terminated.
		if len(body) > 0 {
			glog.Error("forwarded HEAD request contained non-empty body")
			body = nil
		}
	} else if body == nil && msg != nil {
		body = cmn.MustMarshal(msg)
	}
	primary := &p.rproxy.primary
	primary.Lock()
	if primary.url != smap.ProxySI.PublicNet.DirectURL {
		primary.url = smap.ProxySI.PublicNet.DirectURL
		uparsed, err := url.Parse(smap.ProxySI.PublicNet.DirectURL)
		cmn.AssertNoErr(err)
		primary.rp = httputil.NewSingleHostReverseProxy(uparsed)
		cfg := cmn.GCO.Get()
		primary.rp.Transport = cmn.NewTransport(cmn.TransportArgs{
			UseHTTPS:   cfg.Net.HTTP.UseHTTPS,
			SkipVerify: cfg.Net.HTTP.SkipVerify,
		})
	}
	primary.Unlock()
	if len(body) > 0 {
		r.Body = ioutil.NopCloser(bytes.NewBuffer(body))
		r.ContentLength = int64(len(body)) // directly setting content-length
	}
	if msg != nil {
		glog.Infof(`%s: forwarding "%s:%s" to the primary %s`, p.si, msg.Action, s, smap.ProxySI)
	} else {
		glog.Infof("%s: forwarding %q to the primary %s", p.si, s, smap.ProxySI)
	}
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
		cfg := cmn.GCO.Get()
		rproxy.Transport = cmn.NewTransport(cmn.TransportArgs{
			UseHTTPS:   cfg.Net.HTTP.UseHTTPS,
			SkipVerify: cfg.Net.HTTP.SkipVerify,
		})
		p.rproxy.nodes.Store(nodeID, rproxy)
	}

	rproxy.ServeHTTP(w, r)
}

func (p *proxyrunner) reverseReqRemote(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg, bck cmn.Bck) (err error) {
	var (
		remoteUUID = bck.Ns.UUID
		query      = r.URL.Query()

		v, configured = cmn.GCO.Get().Cloud.ProviderConf(cmn.ProviderAIS)
	)

	if !configured {
		err = errors.New("ais remote cloud is not configured")
		p.invalmsghdlr(w, r, err.Error())
		return err
	}

	aisConf := v.(cmn.CloudConfAIS)
	urls, exists := aisConf[remoteUUID]
	if !exists {
		err = fmt.Errorf("remote UUID/alias (%s) not found", remoteUUID)
		p.invalmsghdlr(w, r, err.Error())
		return err
	}

	cmn.Assert(len(urls) > 0)
	u, err := url.Parse(urls[0])
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return err
	}
	if msg != nil {
		body := cmn.MustMarshal(msg)
		r.Body = ioutil.NopCloser(bytes.NewReader(body))
	}

	bck.Ns.UUID = ""
	query = cmn.DelBckFromQuery(query)
	query = cmn.AddBckToQuery(query, bck)
	r.URL.RawQuery = query.Encode()
	p.reverseRequest(w, r, remoteUUID, u)
	return nil
}

func (p *proxyrunner) listBuckets(w http.ResponseWriter, r *http.Request, query cmn.QueryBcks) {
	bmd := p.owner.bmd.get()
	if query.IsAIS() {
		bcks := p.selectBMDBuckets(bmd, query)
		p.writeJSON(w, r, bcks, listBuckets)
		return
	}
	si, err := p.owner.smap.get().GetRandTarget()
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	p.reverseNodeRequest(w, r, si)
}

func (p *proxyrunner) redirectURL(r *http.Request, si *cluster.Snode, ts time.Time, netName string) (redirect string) {
	var (
		nodeURL string
		query   = url.Values{}
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

	query.Add(cmn.URLParamProxyID, p.si.ID())
	query.Add(cmn.URLParamUnixTime, cmn.UnixNano2S(ts.UnixNano()))
	redirect += query.Encode()
	return
}

func (p *proxyrunner) initAsyncQuery(bck *cluster.Bck, smsg *cmn.SelectMsg, newTaskID string) (bool, url.Values) {
	isNew := smsg.UUID == ""
	q := url.Values{}
	if isNew {
		smsg.UUID = newTaskID
		q.Set(cmn.URLParamTaskAction, cmn.TaskStart)
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("proxy: starting new async task %s", smsg.UUID)
		}
	} else {
		// First request is always 'Status' to avoid wasting gigabytes of
		// traffic in case when few targets have finished their tasks.
		q.Set(cmn.URLParamTaskAction, cmn.TaskStatus)
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("proxy: reading async task %s result", smsg.UUID)
		}
	}

	q = cmn.AddBckToQuery(q, bck.Bck)
	return isNew, q
}

func (p *proxyrunner) checkBckTaskResp(uuid string, results chan callResult) (allOK bool, status int, err error) {
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
		err = fmt.Errorf("task %s %s", uuid, cmn.DoesNotExist)
	}
	return
}

// reads object list from all targets, combines, sorts and returns the list
// returns:
//      * the list of objects if the async task finished (empty uuid)
//      * non-empty uuid if the task is still running
//      * error
//
// cmn.SelectMsg.UUID - TaskID which changes between next page requests
// cmn.Selectmsg.Handle - list operation handle which doesn't change between requests
func (p *proxyrunner) listAISBucket(bck *cluster.Bck, smsg cmn.SelectMsg) (allEntries *cmn.BucketList, msg *cmn.InitTaskRespMsg, err error) {
	var (
		smap     = p.owner.smap.Get()
		pageSize uint
	)
	if smsg.PageSize != 0 {
		// When `PageSize` is set, then regardless of the listing type (slow/fast)
		// we need respect it.
		pageSize = smsg.PageSize
	} else if !smsg.Fast {
		// When listing slow, we need to return a single page of default size.
		pageSize = cmn.DefaultListPageSize
	} else if smsg.Fast {
		// When listing fast, we need to return all entries (without paging).
		pageSize = 0
	}

	if smsg.UUID == "" {
		newTaskID := cmn.GenUUID()
		smsg.Handle = cmn.GenUUID()
		initResults := listCache.initResults(smap, smsg, bck, pageSize, newTaskID)
		if initResults.err != nil {
			return nil, nil, initResults.err
		}

		return nil, &cmn.InitTaskRespMsg{UUID: newTaskID, Handle: smsg.Handle}, nil
	}

	if smsg.Handle == "" {
		return nil, nil, errors.New("handle can't be empty")
	}

	fetchResult := listCache.next(smap, smsg, bck, pageSize)
	if fetchResult.err != nil {
		return nil, nil, fetchResult.err
	}
	if !fetchResult.allOK {
		return nil, &cmn.InitTaskRespMsg{UUID: smsg.UUID, Handle: smsg.Handle}, nil
	}

	allEntries = cmn.ConcatObjLists(fetchResult.lists, pageSize)
	allEntries.Handle = smsg.Handle

	return allEntries, nil, nil
}

// selects a random target to GET the list of objects from the remote bucket (cloud or remote AIS)
// if `cached` and/or `atime` is requested:
//      * get a list of cached objects from all targets
//      * add `cached`/`atime` to returned object metadata
// returns:
//      * list of objects and properties if the async task has finished (empty uuid)
//      * non-empty uuid if the task is still running
//      * error
func (p *proxyrunner) listObjectsRemote(bck *cluster.Bck, smsg cmn.SelectMsg) (
	allEntries *cmn.BucketList, initRespMsg *cmn.InitTaskRespMsg, status int, err error) {
	if smsg.PageSize > cmn.DefaultListPageSize {
		glog.Warningf("list_objects page size %d for bucket %s exceeds the default maximum %d ",
			smsg.PageSize, bck, cmn.DefaultListPageSize)
	}
	pageSize := smsg.PageSize
	if pageSize == 0 {
		pageSize = cmn.DefaultListPageSize
	}

	// start new async task if client did not provide uuid (neither in headers nor in SelectMsg)
	isNew, q := p.initAsyncQuery(bck, &smsg, cmn.GenUUID())
	var (
		smap          = p.owner.smap.get()
		reqTimeout    = cmn.GCO.Get().Client.ListObjects
		aisMsg        = p.newAisMsg(&cmn.ActionMsg{Action: cmn.ActListObjects, Value: &smsg}, smap, nil)
		body          = cmn.MustMarshal(aisMsg)
		needLocalData = smsg.NeedLocalData()
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
			res := p.call(callArgs{si: si, req: args.req, timeout: reqTimeout})
			results = make(chan callResult, 1)
			results <- res
			break
		}
	}

	allOK, status, err := p.checkBckTaskResp(smsg.UUID, results)
	if err != nil {
		return nil, nil, status, err
	}

	// some targets are still executing their tasks or it is request to start
	// an async task. The proxy returns only uuid to a caller
	if !allOK || isNew {
		return nil, &cmn.InitTaskRespMsg{UUID: smsg.UUID}, 0, nil
	}

	// all targets are ready, prepare the final result
	q = url.Values{}
	q = cmn.AddBckToQuery(q, bck.Bck)
	q.Set(cmn.URLParamTaskAction, cmn.TaskResult)
	q.Set(cmn.URLParamSilent, "true")
	args.req.Query = q
	results = p.bcastTo(args)

	// combine results
	bckLists := make([]*cmn.BucketList, 0, len(results))
	for res := range results {
		if res.status == http.StatusNotFound { // TODO -- FIXME
			continue
		}
		if res.err != nil {
			return nil, nil, res.status, res.err
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

	// Maximum objects in the final result page. Take all objects in
	// case of Cloud and no limit is set by a user. We cannot use
	// the single cmn.DefaultPageSize because Azure limit differs.
	maxSize := smsg.PageSize
	if bck.IsAIS() || smsg.Cached {
		maxSize = pageSize
	}
	if smsg.Cached {
		allEntries = cmn.ConcatObjLists(bckLists, maxSize)
	} else {
		allEntries = cmn.MergeObjLists(bckLists, maxSize)
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Errorf("Objects after merge %d, marker %s", len(allEntries.Entries), allEntries.PageMarker)
	}

	if smsg.WantProp(cmn.GetTargetURL) {
		for _, e := range allEntries.Entries {
			si, err := cluster.HrwTarget(bck.MakeUname(e.Name), &smap.Smap)
			if err == nil {
				e.TargetURL = si.URL(cmn.NetworkPublic)
			}
		}
	}

	// no active tasks - return empty initRespMsg
	return allEntries, nil, 0, nil
}

func (p *proxyrunner) objRename(w http.ResponseWriter, r *http.Request, bck *cluster.Bck) {
	started := time.Now()
	apitems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	objName := apitems[1]
	smap := p.owner.smap.get()
	si, err := cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("RENAME %s %s/%s => %s", r.Method, bck.Name, objName, si)
	}

	// NOTE:
	//       code 307 is the only way to http-redirect with the
	//       original JSON payload (SelectMsg - see pkg/api/constant.go)
	redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)

	p.statsT.Add(stats.RenameCount, 1)
}

func (p *proxyrunner) promoteFQN(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, msg *cmn.ActionMsg) {
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}

	params := cmn.ActValPromote{}
	if err := cmn.MorphMarshal(msg.Value, &params); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	var (
		started = time.Now()
		smap    = p.owner.smap.get()
		bucket  = apiItems[0]
	)

	// designated target ID
	if params.Target != "" {
		tsi := smap.GetTarget(params.Target)
		if tsi == nil {
			err = &errNodeNotFound{cmn.ActPromote + " failure", params.Target, p.si, smap}
			p.invalmsghdlr(w, r, err.Error())
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
	query := cmn.AddBckToQuery(nil, bck.Bck)
	results := p.callTargets(http.MethodPost, cmn.URLPath(cmn.Version, cmn.Objects, bucket), cmn.MustMarshal(msg), query)
	for res := range results {
		if res.err != nil {
			p.invalmsghdlrf(w, r, "%s failed, err: %s", msg.Action, res.err)
			return
		}
	}
}

func (p *proxyrunner) doListRange(method, bucket string, msg *cmn.ActionMsg, query url.Values) error {
	var (
		timeout time.Duration
		results chan callResult
		wait    bool
	)
	var (
		smap   = p.owner.smap.get()
		bmd    = p.owner.bmd.get()
		aisMsg = p.newAisMsg(msg, smap, bmd, cmn.GenUUID())
		body   = cmn.MustMarshal(aisMsg)
		path   = cmn.URLPath(cmn.Version, cmn.Buckets, bucket)
	)
	if wait {
		timeout = cmn.GCO.Get().Client.ListObjects
	} else {
		timeout = cmn.DefaultTimeout
	}
	results = p.bcastTo(bcastArgs{
		req:     cmn.ReqArgs{Method: method, Path: path, Query: query, Body: body},
		smap:    smap,
		timeout: timeout,
	})
	for res := range results {
		if res.err != nil {
			return fmt.Errorf("%s failed to %s List/Range: %v (%d: %s)",
				res.si, msg.Action, res.err, res.status, res.details)
		}
	}
	return nil
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
		p.invalmsghdlr(w, r, "missing node ID")
		return
	}
	smap := p.owner.smap.get()
	si := smap.GetNode(nodeID)
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
		err = &errNodeNotFound{"cannot rproxy", nodeID, p.si, smap}
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	parsedURL, err := url.Parse(nodeURL)
	if err != nil {
		p.invalmsghdlrf(w, r, "%s: invalid URL %q for node %s", p.si, nodeURL, nodeID)
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
	// TODO: mark internal calls with a special token?
	switch r.Method {
	case http.MethodGet:
		p.httpdaeget(w, r)
	case http.MethodPut:
		p.httpdaeput(w, r)
	case http.MethodDelete:
		p.httpdaedelete(w, r)
	default:
		p.invalmsghdlrf(w, r, "invalid method %s for /daemon path", r.Method)
	}
}

func (p *proxyrunner) handlePendingRenamedLB(renamedBucket string) {
	p.owner.bmd.Lock()
	defer p.owner.bmd.Unlock()
	bmd := p.owner.bmd.get()
	bck := cluster.NewBck(renamedBucket, cmn.ProviderAIS, cmn.NsGlobal)
	if err := bck.Init(p.owner.bmd, p.si); err != nil {
		// Already removed via the the very first target calling here.
		return
	}
	if bck.Props.Renamed == "" {
		glog.Errorf("%s: renamed bucket %s: unexpected props %+v", p.si, renamedBucket, *bck.Props)
		p.owner.bmd.Unlock()
		return
	}
	clone := bmd.clone()
	clone.del(bck)
	p.owner.bmd.put(clone)

	msg := p.newAisMsgStr(cmn.ActRenameLB, nil, clone)
	_ = p.metasyncer.sync(revsPair{clone, msg})
}

func (p *proxyrunner) httpdaeget(w http.ResponseWriter, r *http.Request) {
	var (
		q    = r.URL.Query()
		what = q.Get(cmn.URLParamWhat)
	)
	switch what {
	case cmn.GetWhatBMD:
		if renamedBucket := q.Get(whatRenamedLB); renamedBucket != "" {
			p.handlePendingRenamedLB(renamedBucket)
		}
		fallthrough // fallthrough
	case cmn.GetWhatConfig, cmn.GetWhatSmapVote, cmn.GetWhatSnode:
		p.httprunner.httpdaeget(w, r)
	case cmn.GetWhatStats:
		pst := getproxystatsrunner()
		ws := pst.GetWhatStats()
		p.writeJSON(w, r, ws, what)
	case cmn.GetWhatSysInfo:
		p.writeJSON(w, r, sys.FetchSysInfo(), what)
	case cmn.GetWhatSmap:
		var (
			smap  = p.owner.smap.get()
			sleep = cmn.GCO.Get().Timeout.CplaneOperation / 2
			max   = 5
		)
		for i := 0; !smap.isValid() && i < max; i++ {
			if !p.NodeStarted() {
				time.Sleep(sleep)
				smap = p.owner.smap.get()
				if !smap.isValid() {
					glog.Errorf("%s is starting up, cannot return vaid %s yet...", p.si, smap)
				}
				break
			}
			smap = p.owner.smap.get()
			time.Sleep(sleep)
		}
		if !smap.isValid() {
			glog.Errorf("%s: startup is taking unusually long time, %s", p.si, smap) // cluster
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		p.writeJSON(w, r, smap, what)
	case cmn.GetWhatDaemonStatus:
		pst := getproxystatsrunner()
		msg := &stats.DaemonStatus{
			Snode:       p.httprunner.si,
			SmapVersion: p.owner.smap.get().Version,
			SysInfo:     sys.FetchSysInfo(),
			Stats:       pst.Core,
		}
		p.writeJSON(w, r, msg, what)
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
				p.invalmsghdlrf(w, r, "received invalid Smap at startup/registration: %s", newsmap.pp())
				return
			}
			if !newsmap.isPresent(p.si) {
				p.invalmsghdlrf(w, r, "not finding self %s in the %s", p.si, newsmap.pp())
				return
			}
			if err := p.owner.smap.synchronize(newsmap, true /* lesserIsErr */); err != nil {
				p.invalmsghdlr(w, r, err.Error())
				return
			}
			glog.Infof("%s: %s %s done", p.si, cmn.SyncSmap, newsmap)
			return
		case cmn.ActSetConfig: // setconfig #1 - via query parameters and "?n1=v1&n2=v2..."
			kvs := cmn.NewSimpleKVsFromQuery(r.URL.Query())
			if err := jsp.SetConfigMany(kvs); err != nil {
				p.invalmsghdlr(w, r, err.Error())
				return
			}
			return
		case cmn.ActAttach, cmn.ActDetach:
			var (
				query = r.URL.Query()
				what  = query.Get(cmn.URLParamWhat)
			)
			if what != cmn.GetWhatRemoteAIS {
				p.invalmsghdlr(w, r, fmt.Sprintf(fmtUnknownQue, what))
				return
			}
			if err := p.attachDetachRemoteAIS(query, apitems[0]); err != nil {
				cmn.InvalidHandlerWithMsg(w, r, err.Error())
				return
			}
			if err := jsp.SaveConfig(fmt.Sprintf("%s(%s)", cmn.ActAttach, cmn.GetWhatRemoteAIS)); err != nil {
				glog.Error(err)
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
			p.invalmsghdlr(w, r, "failed to parse ActionMsg value: not a string")
			return
		}
		kvs := cmn.NewSimpleKVs(cmn.SimpleKVsEntry{Key: msg.Name, Value: value})
		if err := jsp.SetConfigMany(kvs); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
	case cmn.ActShutdown:
		var (
			q     = r.URL.Query()
			force = cmn.IsParseBool(q.Get(cmn.URLParamForce))
		)
		if p.owner.smap.get().isPrimary(p.si) && !force {
			p.invalmsghdlrf(w, r, "cannot shutdown primary proxy without %s=true query parameter", cmn.URLParamForce)
			return
		}
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	default:
		p.invalmsghdlrf(w, r, fmtUnknownAct, msg)
	}
}

// unregister
func (p *proxyrunner) httpdaedelete(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Daemon)
	if err != nil {
		return
	}

	switch apiItems[0] {
	case cmn.Unregister:
		if glog.V(3) {
			glog.Infoln("sending unregister on proxy keepalive control channel")
		}

		// Stop keepaliving
		getproxykeepalive().keepalive.send(kaUnregisterMsg)
		return
	default:
		p.invalmsghdlrf(w, r, "unrecognized path: %q in /daemon DELETE", apiItems[0])
		return
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
	glog.Infof("%s: force new primary %s (URL: %s)", p.si, proxyID, newPrimaryURL)

	if p.si.ID() == proxyID {
		glog.Warningf("%s is already the primary", p.si)
		return
	}
	smap := p.owner.smap.get()
	psi := smap.GetProxy(proxyID)
	if psi == nil && newPrimaryURL == "" {
		err := &errNodeNotFound{"failed to find new primary", proxyID, p.si, smap}
		p.invalmsghdlr(w, r, err.Error(), http.StatusNotFound)
		return
	}
	if newPrimaryURL == "" {
		newPrimaryURL = psi.IntraControlNet.DirectURL
	}
	if newPrimaryURL == "" {
		err := &errNodeNotFound{"failed to get new primary's direct URL", proxyID, p.si, smap}
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	newSmap, err := p.smapFromURL(newPrimaryURL)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if proxyID != newSmap.ProxySI.ID() {
		p.invalmsghdlrf(w, r, "%s: proxy %s is not the primary, current %s", p.si, proxyID, newSmap.pp())
		return
	}

	// notify metasync to cancel all pending sync requests
	p.metasyncer.becomeNonPrimary()
	p.owner.smap.put(newSmap)
	res := p.registerToURL(newSmap.ProxySI.IntraControlNet.DirectURL, newSmap.ProxySI, cmn.DefaultTimeout, nil, false)
	if res.err != nil {
		p.invalmsghdlr(w, r, res.err.Error())
		return
	}
}

func (p *proxyrunner) httpdaesetprimaryproxy(w http.ResponseWriter, r *http.Request) {
	var (
		query   = r.URL.Query()
		prepare bool
	)
	apitems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Daemon)
	if err != nil {
		return
	}
	proxyID := apitems[1]
	force := cmn.IsParseBool(query.Get(cmn.URLParamForce))
	// forceful primary change
	if force && apitems[0] == cmn.Proxy {
		if !p.owner.smap.get().isPrimary(p.si) {
			p.invalmsghdlrf(w, r, "%s is not the primary", p.si)
		}
		p.forcefulJoin(w, r, proxyID)
		return
	}

	preparestr := query.Get(cmn.URLParamPrepare)
	if prepare, err = cmn.ParseBool(preparestr); err != nil {
		p.invalmsghdlrf(w, r, "failed to parse %s URL parameter: %v", cmn.URLParamPrepare, err)
		return
	}

	if p.owner.smap.get().isPrimary(p.si) {
		p.invalmsghdlr(w, r, "expecting 'cluster' (RESTful) resource when designating primary proxy via API")
		return
	}

	if p.si.ID() == proxyID {
		if !prepare {
			p.becomeNewPrimary("")
		}
		return
	}

	smap := p.owner.smap.get()
	psi := smap.GetProxy(proxyID)
	if psi == nil {
		err := &errNodeNotFound{"cannot set new primary", proxyID, p.si, smap}
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	if prepare {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Info("Preparation step: do nothing")
		}
		return
	}

	err = p.owner.smap.modify(func(clone *smapX) error {
		clone.ProxySI = psi
		return nil
	})
	cmn.AssertNoErr(err)
}

func (p *proxyrunner) becomeNewPrimary(proxyIDToRemove string) {
	err := p.owner.smap.modify(
		func(clone *smapX) error {
			if !clone.isPresent(p.si) {
				cmn.AssertMsg(false, "This proxy '"+p.si.ID()+"' must always be present in the "+clone.pp())
			}
			// FIXME: may be premature at this point
			if proxyIDToRemove != "" {
				glog.Infof("%s: removing failed primary %s", p.si, proxyIDToRemove)
				clone.delProxy(proxyIDToRemove)
			}

			clone.ProxySI = p.si
			clone.Version += 100
			return nil
		},
		func(clone *smapX) {
			bmd := p.owner.bmd.get()
			rmd := p.owner.rmd.get()
			if glog.V(3) {
				glog.Infof("%s: distributing %s with newly elected primary (self)", p.si, clone)
				glog.Infof("%s: distributing %s as well", p.si, bmd)
				glog.Infof("%s: distributing %s as well", p.si, rmd)
			}

			msg := p.newAisMsgStr(cmn.ActNewPrimary, clone, nil)
			_ = p.metasyncer.sync(
				revsPair{clone, msg},
				revsPair{bmd, msg},
				revsPair{rmd, msg},
			)
		},
	)
	cmn.AssertNoErr(err)
}

func (p *proxyrunner) httpclusetprimaryproxy(w http.ResponseWriter, r *http.Request) {
	apitems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Cluster, cmn.Proxy)
	if err != nil {
		return
	}
	proxyid := apitems[0]
	s := "designate new primary proxy '" + proxyid + "'"
	if p.forwardCP(w, r, nil, s, nil) {
		return
	}
	smap := p.owner.smap.get()
	psi, ok := smap.Pmap[proxyid]
	if !ok {
		p.invalmsghdlrf(w, r, "new primary proxy %s not present in the %s", proxyid, smap.pp())
		return
	}
	if proxyid == p.si.ID() {
		cmn.Assert(p.si.ID() == smap.ProxySI.ID()) // must be forwardCP-ed
		glog.Warningf("Request to set primary = %s = self: nothing to do", proxyid)
		return
	}

	// (I) prepare phase
	urlPath := cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Proxy, proxyid)
	q := url.Values{}
	q.Set(cmn.URLParamPrepare, "true")
	results := p.callAll(http.MethodPut, urlPath, nil, q)
	for res := range results {
		if res.err != nil {
			p.invalmsghdlrf(w, r, "Failed to set primary %s: err %v from %s in the prepare phase", proxyid, res.err, res.si)
			return
		}
	}

	// (I.5) commit locally
	err = p.owner.smap.modify(func(clone *smapX) error {
		clone.ProxySI = psi
		p.metasyncer.becomeNonPrimary()
		return nil
	})
	cmn.AssertNoErr(err)

	// (II) commit phase
	q.Set(cmn.URLParamPrepare, "false")
	results = p.callAll(http.MethodPut, urlPath, nil, q)

	// FIXME: retry
	for res := range results {
		if res.err != nil {
			if res.si.ID() == proxyid {
				glog.Fatalf("Commit phase failure: new primary %s returned err %v", proxyid, res.err)
			} else {
				glog.Errorf("Commit phase failure: %s returned err %v when setting primary = %s",
					res.si.ID(), res.err, proxyid)
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
	// TODO: mark internal calls with a special token?
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

// [METHOD] /v1/dsort
func (p *proxyrunner) dsortHandler(w http.ResponseWriter, r *http.Request) {
	// TODO: separate permissions for dsort? xactions?
	if err := p.checkPermissions(r, nil, cmn.AccessADMIN); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
		return
	}
	dsort.ProxySortHandler(w, r)
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

///////////////////////////////////
// query target states and stats //
///////////////////////////////////

func (p *proxyrunner) httpcluget(w http.ResponseWriter, r *http.Request) {
	what := r.URL.Query().Get(cmn.URLParamWhat)
	switch what {
	case cmn.GetWhatStats:
		p.queryClusterStats(w, r, what)
	case cmn.GetWhatSysInfo:
		p.queryClusterSysinfo(w, r, what)
	case cmn.GetWhatXactStats, cmn.QueryXactStats:
		p.queryXaction(w, r, what)
	case cmn.GetWhatMountpaths:
		p.queryClusterMountpaths(w, r, what)
	case cmn.GetWhatRemoteAIS:
		config := cmn.GCO.Get()
		smap := p.owner.smap.get()
		for _, si := range smap.Tmap {
			args := callArgs{
				si: si,
				req: cmn.ReqArgs{
					Method: r.Method,
					Path:   cmn.URLPath(cmn.Version, cmn.Daemon),
					Query:  r.URL.Query(),
				},
				timeout: config.Timeout.CplaneOperation,
			}
			res := p.call(args)
			if res.err != nil {
				p.invalmsghdlr(w, r, res.err.Error())
				return
			}
			// TODO: figure out a way to switch to writeJSON
			p.writeJSONBytes(w, r, res.outjson, what)
			break
		}
	default:
		s := fmt.Sprintf(fmtUnknownQue, what)
		cmn.InvalidHandlerWithMsg(w, r, s)
	}
}

func (p *proxyrunner) queryXaction(w http.ResponseWriter, r *http.Request, what string) {
	var (
		body  []byte
		query = r.URL.Query()
	)
	switch what {
	case cmn.QueryXactStats:
		var xactMsg cmn.XactReqMsg
		if err := cmn.ReadJSON(w, r, &xactMsg); err != nil {
			return
		}
		body = cmn.MustMarshal(xactMsg)
	default:
		p.invalmsghdlrstatusf(w, r, http.StatusBadRequest, "invalid `what`: %v", what)
		return
	}
	results := p.bcastGet(bcastArgs{
		req: cmn.ReqArgs{
			Path:  cmn.URLPath(cmn.Version, cmn.Xactions),
			Query: query,
			Body:  body,
		},
		timeout: cmn.GCO.Get().Timeout.MaxKeepalive,
	})
	targetResults := p._queryResults(w, r, results)
	if targetResults != nil {
		p.writeJSON(w, r, targetResults, what)
	}
}

func (p *proxyrunner) queryClusterSysinfo(w http.ResponseWriter, r *http.Request, what string) {
	fetchResults := func(broadcastType int) (cmn.JSONRawMsgs, string) {
		results := p.bcastTo(bcastArgs{
			req: cmn.ReqArgs{
				Method: r.Method,
				Path:   cmn.URLPath(cmn.Version, cmn.Daemon),
				Query:  r.URL.Query(),
			},
			timeout: cmn.GCO.Get().Client.Timeout,
			to:      broadcastType,
		})
		sysInfoMap := make(cmn.JSONRawMsgs, len(results))
		for res := range results {
			if res.err != nil {
				return nil, res.details
			}
			sysInfoMap[res.si.ID()] = res.outjson
		}
		return sysInfoMap, ""
	}

	out := &cmn.ClusterSysInfoRaw{}
	proxyResults, err := fetchResults(cluster.Proxies)
	if err != "" {
		p.invalmsghdlr(w, r, err)
		return
	}

	out.Proxy = proxyResults
	targetResults, err := fetchResults(cluster.Targets)
	if err != "" {
		p.invalmsghdlr(w, r, err)
		return
	}

	out.Target = targetResults
	_ = p.writeJSON(w, r, out, what)
}

func (p *proxyrunner) queryClusterStats(w http.ResponseWriter, r *http.Request, what string) {
	targetStats := p._queryTargets(w, r)
	if targetStats == nil {
		return
	}
	out := &stats.ClusterStatsRaw{}
	out.Target = targetStats
	rr := getproxystatsrunner()
	out.Proxy = rr.Core
	_ = p.writeJSON(w, r, out, what)
}

func (p *proxyrunner) queryClusterMountpaths(w http.ResponseWriter, r *http.Request, what string) {
	targetMountpaths := p._queryTargets(w, r)
	if targetMountpaths == nil {
		return
	}
	out := &ClusterMountpathsRaw{}
	out.Targets = targetMountpaths
	_ = p.writeJSON(w, r, out, what)
}

// helper methods for querying targets

func (p *proxyrunner) _queryTargets(w http.ResponseWriter, r *http.Request) cmn.JSONRawMsgs {
	var (
		err  error
		body []byte
	)
	if r.Body != nil {
		body, err = cmn.ReadBytes(r)
		if err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return nil
		}
	}
	results := p.bcastTo(bcastArgs{
		req: cmn.ReqArgs{
			Method: r.Method,
			Path:   cmn.URLPath(cmn.Version, cmn.Daemon),
			Query:  r.URL.Query(),
			Body:   body,
		},
		timeout: cmn.GCO.Get().Timeout.MaxKeepalive,
	})
	return p._queryResults(w, r, results)
}

func (p *proxyrunner) _queryResults(w http.ResponseWriter, r *http.Request, results chan callResult) cmn.JSONRawMsgs {
	targetResults := make(cmn.JSONRawMsgs, len(results))
	for res := range results {
		if res.err != nil {
			p.invalmsghdlr(w, r, res.details)
			return nil
		}
		targetResults[res.si.ID()] = res.outjson
	}
	return targetResults
}

//////////////////////////////////////
// commands:
// - register|keepalive target|proxy
// - start|stop xaction
//////////////////////////////////////

func (p *proxyrunner) httpclupost(w http.ResponseWriter, r *http.Request) {
	var (
		regReq                                nodeRegMeta
		tag                                   string
		keepalive, userRegister, selfRegister bool
		nonElectable                          bool
	)
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Cluster)
	if err != nil {
		return
	}

	if p.forwardCP(w, r, nil, "httpclupost", nil) {
		return
	}
	switch apiItems[0] {
	case cmn.UserRegister: // manual by user (API)
		if cmn.ReadJSON(w, r, &regReq.SI) != nil {
			return
		}
		tag = "user-register"
		userRegister = true
	case cmn.Keepalive:
		if cmn.ReadJSON(w, r, &regReq) != nil {
			return
		}
		tag = "keepalive"
		keepalive = true
	case cmn.AutoRegister: // node self-register
		if cmn.ReadJSON(w, r, &regReq) != nil {
			return
		}
		tag = "join"
		selfRegister = true
	default:
		p.invalmsghdlrf(w, r, "invalid URL path: %q", apiItems[0])
		return
	}

	nsi := regReq.SI
	if err := nsi.Validate(); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if p.NodeStarted() {
		bmd := p.owner.bmd.get()
		if err := bmd.validateUUID(regReq.BMD, p.si, nsi, ""); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
	}
	isProxy := nsi.IsProxy()
	if isProxy {
		s := r.URL.Query().Get(cmn.URLParamNonElectable)
		if nonElectable, err = cmn.ParseBool(s); err != nil {
			glog.Errorf("%s: failed to parse %s for non-electability: %v", p.si, s, err)
		}
	}

	s := fmt.Sprintf("%s: %s %s", p.si, tag, nsi)
	msg := &cmn.ActionMsg{Action: cmn.ActRegTarget}
	if isProxy {
		msg = &cmn.ActionMsg{Action: cmn.ActRegProxy}
	}
	body := cmn.MustMarshal(regReq)
	if p.forwardCP(w, r, msg, s, body) {
		return
	}
	if net.ParseIP(nsi.PublicNet.NodeIPAddr) == nil {
		p.invalmsghdlrf(w, r, "%s: failed to %s %s - invalid IP address %v", p.si, tag, nsi, nsi.PublicNet.NodeIPAddr)
		return
	}

	p.statsT.Add(stats.PostCount, 1)

	p.owner.smap.Lock()
	smap, err, code, update := p.handleJoinKalive(nsi, regReq.Smap, tag, keepalive, userRegister, nonElectable)
	p.owner.smap.Unlock()

	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), code)
		return
	}
	if !update {
		return
	}

	// send the current Smap and BMD to self-registering target
	if !isProxy && selfRegister {
		glog.Infof("%s: %s %s (%s)...", p.si, tag, nsi, regReq.Smap)
		bmd := p.owner.bmd.get()
		meta := &nodeRegMeta{smap, bmd, p.si}
		p.writeJSON(w, r, meta, path.Join(cmn.ActRegTarget, nsi.ID()) /* tag */)
	}
	go p.updateAndDistribute(nsi, msg, nonElectable)
}

// NOTE: under lock
func (p *proxyrunner) handleJoinKalive(nsi *cluster.Snode, regSmap *smapX,
	tag string, keepalive, userRegister, nonElectable bool) (smap *smapX, err error, code int, update bool) {
	code = http.StatusBadRequest
	smap = p.owner.smap.get()

	if !smap.isPrimary(p.si) {
		err = fmt.Errorf("%s is not the primary(%s, %s): cannot %s %s", p.si, smap.ProxySI, smap, tag, nsi)
		return
	}

	if nsi.IsProxy() {
		osi := smap.GetProxy(nsi.ID())
		if !p.addOrUpdateNode(nsi, osi, keepalive) {
			return
		}
	} else {
		osi := smap.GetTarget(nsi.ID())
		if keepalive && !p.addOrUpdateNode(nsi, osi, keepalive) {
			return
		}
		if userRegister {
			//
			// join(node) => cluster via API
			//
			if glog.FastV(3, glog.SmoduleAIS) {
				glog.Infof("%s: %s %s => (%s)", p.si, tag, nsi, smap.StringEx())
			}
			// send the joining node the current BMD and Smap as well
			bmd := p.owner.bmd.get()
			meta := &nodeRegMeta{smap, bmd, p.si}
			body := cmn.MustMarshal(meta)
			path := cmn.URLPath(cmn.Version, cmn.Daemon, cmn.UserRegister)
			args := callArgs{
				si:      nsi,
				req:     cmn.ReqArgs{Method: http.MethodPost, Path: path, Body: body},
				timeout: cmn.GCO.Get().Timeout.CplaneOperation,
			}
			res := p.call(args)
			if res.err != nil {
				if cmn.IsErrConnectionRefused(res.err) {
					err = fmt.Errorf("failed to reach %s at %s:%s",
						nsi, nsi.PublicNet.NodeIPAddr, nsi.PublicNet.DaemonPort)
				} else {
					err = fmt.Errorf("%s: failed to %s %s: %v, %s", p.si, tag, nsi, res.err, res.details)
				}
				code = res.status
				return
			}
		}
	}
	// check for cluster integrity errors (cie)
	if err = smap.validateUUID(regSmap, p.si, nsi, ""); err != nil {
		return
	}
	// no further checks join when cluster's starting up
	if !p.ClusterStarted() {
		clone := smap.clone()
		clone.putNode(nsi, nonElectable)
		p.owner.smap.put(clone)
		return
	}
	if _, err = smap.IsDuplicateURL(nsi); err != nil {
		err = errors.New(p.si.String() + ": " + err.Error())
	}
	update = err == nil
	return
}

func (p *proxyrunner) updateAndDistribute(nsi *cluster.Snode, msg *cmn.ActionMsg, nonElectable bool) {
	var (
		smap   *smapX
		exists bool
	)
	err := p.owner.smap.modify(
		func(clone *smapX) error {
			smap = p.owner.smap.get()
			if !smap.isPrimary(p.si) {
				return fmt.Errorf("%s is not primary(%s, %s): cannot add %s",
					p.si, smap.ProxySI, smap, nsi)
			}
			exists = clone.putNode(nsi, nonElectable)
			if nsi.IsTarget() {
				// Notify targets that they need to set up GFN
				aisMsg := p.newAisMsg(&cmn.ActionMsg{Action: cmn.ActStartGFN}, clone, nil)
				notifyPairs := revsPair{clone, aisMsg}
				_ = p.metasyncer.notify(true, notifyPairs)
			}
			return nil
		},
		func(clone *smapX) {
			var (
				tokens = p.authn.revokedTokenList()
				bmd    = p.owner.bmd.get()
			)
			// metasync
			aisMsg := p.newAisMsg(msg, clone, nil)
			pairs := []revsPair{{clone, aisMsg}, {bmd, aisMsg}}
			if nsi.IsTarget() {
				// Trigger rebalance (in case target with given ID already exists
				// we must trigger the rebalance and assume it is a new target
				// with the same ID).
				if exists || p.requiresRebalance(smap, clone) {
					clone := p.owner.rmd.modify(func(clone *rebMD) {
						clone.TargetIDs = []string{nsi.ID()}
						clone.inc()
					})
					pairs = append(pairs, revsPair{clone, aisMsg})
				}
			} else {
				// Send RMD to proxies to make sure that they have
				// the latest one - newly joined can become primary in a second.
				rmd := p.owner.rmd.get()
				pairs = append(pairs, revsPair{rmd, aisMsg})
			}
			if len(tokens.Tokens) > 0 {
				pairs = append(pairs, revsPair{tokens, aisMsg})
			}
			_ = p.metasyncer.sync(pairs...)
		},
	)
	if err != nil {
		glog.Errorf("FATAL: %v", err)
		return
	}
}

func (p *proxyrunner) addOrUpdateNode(nsi, osi *cluster.Snode, keepalive bool) bool {
	if keepalive {
		if osi == nil {
			glog.Warningf("register/keepalive %s: adding back to the cluster map", nsi)
			return true
		}

		if !osi.Equals(nsi) {
			if p.detectDaemonDuplicate(osi, nsi) {
				glog.Errorf("%s: %s(%s) is trying to register/keepalive with duplicate ID",
					p.si, nsi, nsi.PublicNet.DirectURL)
				return false
			}
			glog.Warningf("%s: renewing registration %s (info changed!)", p.si, nsi)
			return true
		}

		p.keepalive.heardFrom(nsi.ID(), !keepalive /* reset */)
		return false
	}
	if osi != nil {
		if !p.NodeStarted() {
			return true
		}
		if osi.Equals(nsi) {
			glog.Infof("%s: %s is already registered", p.si, nsi)
			return false
		}
		glog.Warningf("%s: renewing %s registration %+v => %+v", p.si, nsi, osi, nsi)
	}
	return true
}

// unregister node
func (p *proxyrunner) httpcludel(w http.ResponseWriter, r *http.Request) {
	apitems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Cluster, cmn.Daemon)
	if err != nil {
		return
	}
	var (
		msg  *cmn.ActionMsg
		sid  = apitems[0]
		smap = p.owner.smap.get()
		node = smap.GetNode(sid)
	)
	if node == nil {
		err = &errNodeNotFound{"cannot remove", sid, p.si, smap}
		p.invalmsghdlr(w, r, err.Error(), http.StatusNotFound)
		return
	}
	msg = &cmn.ActionMsg{Action: cmn.ActUnregTarget}
	if node.IsProxy() {
		msg = &cmn.ActionMsg{Action: cmn.ActUnregProxy}
	}
	if p.forwardCP(w, r, msg, sid, nil) {
		return
	}

	var status int
	err = p.owner.smap.modify(
		func(clone *smapX) error {
			smap = p.owner.smap.get()
			status, err = p.unregisterNode(clone, sid)
			return err
		},
		func(clone *smapX) {
			aisMsg := p.newAisMsg(msg, clone, nil)
			pairs := []revsPair{{clone, aisMsg}}
			if p.requiresRebalance(smap, clone) {
				clone := p.owner.rmd.modify(func(clone *rebMD) {
					clone.inc()
				})
				pairs = append(pairs, revsPair{clone, aisMsg})
			}
			_ = p.metasyncer.sync(pairs...)
		},
	)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), status)
		return
	}
}

func (p *proxyrunner) unregisterNode(clone *smapX, sid string) (status int, err error) {
	smap := p.owner.smap.get()
	node := smap.GetNode(sid)
	if node == nil {
		err = &errNodeNotFound{"cannot unregister", sid, p.si, smap}
		return http.StatusNotFound, err
	}

	// Synchronously call the node to inform it that it no longer is part of
	// the cluster. The node should stop sending keepalive messages (that could
	// potentially reregister it back into the cluster).
	var (
		timeoutCfg = cmn.GCO.Get().Timeout
		args       = callArgs{
			si: node,
			req: cmn.ReqArgs{
				Method: http.MethodDelete,
				Path:   cmn.URLPath(cmn.Version, cmn.Daemon, cmn.Unregister),
			},
			timeout: timeoutCfg.CplaneOperation,
		}
	)
	for i := 0; i < 3; /*retries*/ i++ {
		res := p.call(args)
		if res.err == nil {
			break
		}
		glog.Warningf("%s that is being unregistered failed to respond back: %v, %s",
			node, res.err, res.details)
		time.Sleep(timeoutCfg.CplaneOperation / 2)
	}

	// NOTE: "failure to respond back" may have legitimate reasons - proceeeding even when all retries fail
	node = clone.GetNode(sid)
	if node == nil {
		err = &errNodeNotFound{"failed to unregister", sid, p.si, clone}
		return http.StatusNotFound, err
	}
	if node.IsProxy() {
		clone.delProxy(sid)
		glog.Infof("unregistered %s (num proxies %d)", node, clone.CountProxies())
	} else {
		clone.delTarget(sid)
		glog.Infof("unregistered %s (num targets %d)", node, clone.CountTargets())
	}

	if !p.NodeStarted() {
		return
	}

	if isPrimary := clone.isPrimary(p.si); !isPrimary {
		return http.StatusInternalServerError, fmt.Errorf("%s: is not a primary", p.si)
	}
	return
}

// '{"action": "shutdown"}' /v1/cluster => (proxy) =>
// '{"action": "syncsmap"}' /v1/cluster => (proxy) => PUT '{Smap}' /v1/daemon/syncsmap => target(s)
// '{"action": cmn.ActXactStart}' /v1/cluster
// '{"action": cmn.ActXactStop}' /v1/cluster
// '{"action": cmn.ActRebalance}' /v1/cluster => (proxy) => PUT '{Smap}' /v1/daemon/rebalance => target(s)
// '{"action": "setconfig"}' /v1/cluster => (proxy) =>
func (p *proxyrunner) httpcluput(w http.ResponseWriter, r *http.Request) {
	apitems, err := p.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Cluster)
	if err != nil {
		return
	}
	if len(apitems) == 0 {
		p.cluputJSON(w, r)
	} else {
		p.cluputQuery(w, r, apitems[0])
	}
}

func (p *proxyrunner) cluputJSON(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		msg = &cmn.ActionMsg{}
	)
	if cmn.ReadJSON(w, r, msg) != nil {
		return
	}
	if p.forwardCP(w, r, msg, "", nil) {
		return
	}
	// handle the action
	switch msg.Action {
	case cmn.ActSetConfig:
		var (
			value string
			ok    bool
		)
		if value, ok = msg.Value.(string); !ok {
			p.invalmsghdlrf(w, r, "%s: invalid value format (%+v, %T)", cmn.ActSetConfig, msg.Value, msg.Value)
			return
		}
		kvs := cmn.NewSimpleKVs(cmn.SimpleKVsEntry{Key: msg.Name, Value: value})
		if err := jsp.SetConfigMany(kvs); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}

		results := p.callAll(http.MethodPut, cmn.URLPath(cmn.Version, cmn.Daemon), cmn.MustMarshal(msg)) // same message -> all targets and proxies
		for res := range results {
			if res.err != nil {
				p.invalmsghdlrf(w, r, "%s: (%s = %s) failed, err: %s", msg.Action, msg.Name, value, res.details)
				p.keepalive.onerr(err, res.status)
				return
			}
		}
	case cmn.ActShutdown:
		glog.Infoln("Proxy-controlled cluster shutdown...")
		p.callAll(http.MethodPut, cmn.URLPath(cmn.Version, cmn.Daemon), cmn.MustMarshal(msg))
		time.Sleep(time.Second)
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	case cmn.ActXactStart, cmn.ActXactStop:
		xactMsg := cmn.XactReqMsg{}
		if err := cmn.MorphMarshal(msg.Value, &xactMsg); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		if msg.Action == cmn.ActXactStart && xactMsg.Kind == cmn.ActRebalance {
			if err := p.canStartRebalance(); err != nil {
				p.invalmsghdlr(w, r, err.Error())
				return
			}
			clone := p.owner.rmd.modify(func(clone *rebMD) {
				clone.inc()
			})
			msg := &cmn.ActionMsg{Action: cmn.ActRebalance}
			_ = p.metasyncer.sync(revsPair{clone, p.newAisMsg(msg, nil, nil)})
			return
		}

		if msg.Action == cmn.ActXactStart {
			xactMsg.ID = cmn.GenUUID()
		}
		results := p.callTargets(http.MethodPut, cmn.URLPath(cmn.Version, cmn.Xactions), cmn.MustMarshal(cmn.ActionMsg{Action: msg.Action, Value: xactMsg}))
		for res := range results {
			if res.err != nil {
				p.invalmsghdlr(w, r, res.err.Error())
				return
			}
		}
		if msg.Action == cmn.ActXactStart {
			w.Write([]byte(xactMsg.ID))
		}
	default:
		p.invalmsghdlrf(w, r, fmtUnknownAct, msg)
	}
}

func (p *proxyrunner) cluputQuery(w http.ResponseWriter, r *http.Request, action string) {
	var (
		err   error
		query = r.URL.Query()
	)
	switch action {
	case cmn.Proxy:
		// cluster-wide: designate a new primary proxy administratively
		p.httpclusetprimaryproxy(w, r)
	case cmn.ActSetConfig: // setconfig #1 - via query parameters and "?n1=v1&n2=v2..."
		kvs := cmn.NewSimpleKVsFromQuery(query)
		if err := jsp.SetConfigMany(kvs); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		results := p.callAll(http.MethodPut, cmn.URLPath(cmn.Version, cmn.Daemon, cmn.ActSetConfig), nil, query)
		for res := range results {
			if res.err != nil {
				p.invalmsghdlr(w, r, res.err.Error())
				p.keepalive.onerr(err, res.status)
				return
			}
		}
	case cmn.ActAttach, cmn.ActDetach:
		var (
			what = query.Get(cmn.URLParamWhat)
		)
		if what != cmn.GetWhatRemoteAIS {
			cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf(fmtUnknownQue, what))
			return
		}
		if err := p.attachDetachRemoteAIS(query, action); err != nil {
			cmn.InvalidHandlerWithMsg(w, r, err.Error())
			return
		}
		results := p.callAll(http.MethodPut, cmn.URLPath(cmn.Version, cmn.Daemon, action), nil, query)
		// NOTE: save this config unconditionally
		// TODO: metasync
		if err := jsp.SaveConfig(fmt.Sprintf("%s(%s)", action, cmn.GetWhatRemoteAIS)); err != nil {
			glog.Error(err)
		}
		for res := range results {
			if res.err != nil {
				p.invalmsghdlr(w, r, res.err.Error())
				p.keepalive.onerr(err, res.status)
				return
			}
		}
	}
}

//========================
//
// broadcasts: Rx and Tx
//
//========================
func (p *proxyrunner) receiveRMD(newRMD *rebMD, msg *aisMsg) (err error) {
	if glog.V(3) {
		s := fmt.Sprintf("receive %s", newRMD.String())
		if msg.Action == "" {
			glog.Infoln(s)
		} else {
			glog.Infof("%s, action %s", s, msg.Action)
		}
	}
	p.owner.rmd.Lock()
	rmd := p.owner.rmd.get()
	if newRMD.version() <= rmd.version() {
		p.owner.rmd.Unlock()
		if newRMD.version() < rmd.version() {
			err = fmt.Errorf("%s: attempt to downgrade %s to %s", p.si, rmd, newRMD)
		}
		return
	}
	p.owner.rmd.put(newRMD)
	p.owner.rmd.Unlock()
	return
}

func (p *proxyrunner) receiveBMD(newBMD *bucketMD, msg *aisMsg, caller string) (err error) {
	if glog.V(3) {
		s := fmt.Sprintf("receive %s", newBMD.StringEx())
		if msg.Action == "" {
			glog.Infoln(s)
		} else {
			glog.Infof("%s, action %s", s, msg.Action)
		}
	}
	p.owner.bmd.Lock()
	bmd := p.owner.bmd.get()
	if err = bmd.validateUUID(newBMD, p.si, nil, caller); err != nil {
		cmn.Assert(!p.owner.smap.get().isPrimary(p.si))
		// cluster integrity error: making exception for non-primary proxies
		glog.Errorf("%s (non-primary): %v - proceeding to override BMD", p.si, err)
	} else if newBMD.version() <= bmd.version() {
		p.owner.bmd.Unlock()
		if newBMD.version() < bmd.version() {
			err = fmt.Errorf("%s: attempt to downgrade %s to %s", p.si, bmd, newBMD)
		}
		return
	}
	p.owner.bmd.put(newBMD)
	p.owner.bmd.Unlock()
	return
}

// detectDaemonDuplicate queries osi for its daemon info in order to determine if info has changed
// and is equal to nsi
func (p *proxyrunner) detectDaemonDuplicate(osi, nsi *cluster.Snode) bool {
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
		uuid         string
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
		timeout: cmn.GCO.Get().Timeout.MaxKeepalive,
	})
	bmds = make(map[*cluster.Snode]*bucketMD, len(results))
	for res := range results {
		var bmd = &bucketMD{}
		if res.err != nil || res.status >= http.StatusBadRequest {
			glog.Errorf("%v (%d: %s)", res.err, res.status, res.details)
			continue
		}
		if err = jsoniter.Unmarshal(res.outjson, bmd); err != nil {
			glog.Errorf("unexpected: failed to unmarshal %s from %s: %v", bmdTermName, res.si, err)
			continue
		}
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s from %s", bmd, res.si)
		}
		if rbmd == nil { // 1. init
			uuid, rbmd = bmd.UUID, bmd
		} else if uuid != bmd.UUID { // 2. slow path
			slowp = true
		} else if !slowp && rbmd.Version < bmd.Version { // 3. fast path max(version)
			rbmd = bmd
		}
		bmds[res.si] = bmd
	}
	if slowp {
		force = cmn.IsParseBool(r.URL.Query().Get(cmn.URLParamForce))
		if rbmd, err = resolveUUIDBMD(bmds); err != nil {
			_, split := err.(*errBmdUUIDSplit)
			if !force || errors.Is(err, errNoBMD) || split {
				p.invalmsghdlr(w, r, err.Error())
				return
			}
			if _, ok := err.(*errTgtBmdUUIDDiffer); ok {
				glog.Error(err.Error())
			}
		}
	}
	rbmd.Version += 100
	p.owner.bmd.Lock()
	p.owner.bmd.put(rbmd)

	_ = p.metasyncer.sync(revsPair{rbmd, p.newAisMsg(msg, nil, rbmd)})

	p.owner.bmd.Unlock()
}

func (p *proxyrunner) canStartRebalance() error {
	cfg := cmn.GCO.Get().Rebalance
	if !cfg.Enabled {
		return fmt.Errorf("rebalance is not enabled in the configuration")
	}
	if dontRun := cfg.DontRunTime; dontRun > 0 {
		if !p.NodeStarted() {
			return fmt.Errorf("primary is not yet ready to start rebalance")
		}
		if time.Since(p.NodeStartedTime()) < dontRun {
			return fmt.Errorf("rebalance cannot be started before: %v", p.NodeStartedTime().Add(dontRun))
		}
	}
	return nil
}

func (p *proxyrunner) requiresRebalance(prev, cur *smapX) bool {
	if err := p.canStartRebalance(); err != nil {
		return false
	}
	if cur.CountTargets() > prev.CountTargets() {
		return true
	}
	for _, si := range cur.Tmap {
		if !prev.isPresent(si) {
			return true
		}
	}

	bmd := p.owner.bmd.get()
	if bmd.IsECUsed() {
		// If there is any target missing we must start rebalance.
		for _, si := range prev.Tmap {
			if !cur.isPresent(si) {
				return true
			}
		}
	}

	return false
}

/////////////////////////////////////////////
// lookup and add remote bucket on the fly //
/////////////////////////////////////////////

func (args *remBckAddArgs) try() (bck *cluster.Bck, err error) {
	var cloudProps http.Header
	if _, ok := args.err.(*cmn.ErrorRemoteBucketDoesNotExist); !ok {
		err = args.err
		if _, ok := err.(*cmn.ErrorBucketDoesNotExist); ok {
			args.p.invalmsghdlr(args.w, args.r, args.err.Error(), http.StatusNotFound)
			return
		}
		args.p.invalmsghdlr(args.w, args.r, args.err.Error())
		return
	}
	if !cmn.IsValidProvider(args.queryBck.Provider) {
		err = cmn.NewErrorBucketDoesNotExist(args.queryBck.Bck, args.p.si.Name())
		args.p.invalmsghdlr(args.w, args.r, err.Error(), http.StatusNotFound)
		return
	}
	if args.p.forwardCP(args.w, args.r, args.msg, "add-remote-bucket", nil) {
		err = errors.New("forwarded")
		return
	}
	//
	// from this point on, its the primary - lookup via random target and more checks
	//
	if cloudProps, err = args.lookup(); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); ok {
			args.p.invalmsghdlrsilent(args.w, args.r, err.Error(), http.StatusNotFound)
		} else {
			args.p.invalmsghdlr(args.w, args.r, err.Error())
		}
		return
	}
	if args.queryBck.Provider == cmn.ProviderAIS {
		bck = args.queryBck
	} else {
		cloudConf := cmn.GCO.Get().Cloud
		bck = cluster.NewBck(args.queryBck.Name, cloudConf.Provider, cloudConf.Ns)
	}
	if err = args.p.createBucket(&cmn.ActionMsg{Action: cmn.ActRegisterCB}, bck, cloudProps); err != nil {
		if _, ok := err.(*cmn.ErrorBucketAlreadyExists); !ok {
			args.p.invalmsghdlr(args.w, args.r, err.Error(), http.StatusConflict)
			return
		}
	}
	err = bck.Init(args.p.owner.bmd, args.p.si)
	if err != nil {
		glog.Errorf("%s: unexpected failure to add remote %s, err: %v", args.p.si, bck, err)
	}
	return
}

func (args *remBckAddArgs) lookup() (header http.Header, err error) {
	var (
		tsi   *cluster.Snode
		pname = args.p.si.String()
		path  = cmn.URLPath(cmn.Version, cmn.Buckets, args.queryBck.Name)
	)
	if tsi, err = args.p.owner.smap.get().GetRandTarget(); err != nil {
		return
	}
	q := cmn.AddBckToQuery(nil, args.queryBck.Bck)
	req := cmn.ReqArgs{Method: http.MethodHead, Base: tsi.URL(cmn.NetworkIntraData), Path: path, Query: q}
	res := args.p.call(callArgs{si: tsi, req: req, timeout: cmn.DefaultTimeout})
	if res.status == http.StatusNotFound {
		err = cmn.NewErrorRemoteBucketDoesNotExist(args.queryBck.Bck, pname)
	} else if res.status == http.StatusGone {
		err = cmn.NewErrorCloudBucketOffline(args.queryBck.Bck, pname)
	} else if res.err != nil {
		err = fmt.Errorf("%s: %s, target %s, err: %v", pname, args.queryBck, tsi, res.err)
	} else {
		header = res.header
	}
	return
}

////////////////
// misc utils //
////////////////

func resolveUUIDBMD(bmds map[*cluster.Snode]*bucketMD) (*bucketMD, error) {
	var (
		mlist = make(map[string][]nodeRegMeta) // uuid => list(targetRegMeta)
		maxor = make(map[string]*bucketMD)     // uuid => max-ver BMD
	)
	// results => (mlist, maxor)
	for si, bmd := range bmds {
		if bmd.Version == 0 {
			continue
		}
		mlist[bmd.UUID] = append(mlist[bmd.UUID], nodeRegMeta{nil, bmd, si})

		if rbmd, ok := maxor[bmd.UUID]; !ok {
			maxor[bmd.UUID] = bmd
		} else if rbmd.Version < bmd.Version {
			maxor[bmd.UUID] = bmd
		}
	}
	cmn.Assert(len(maxor) == len(mlist)) // TODO: remove
	if len(maxor) == 0 {
		return nil, errNoBMD
	}
	// by simple majority
	var uuid, l = "", 0
	for u, lst := range mlist {
		if l < len(lst) {
			uuid, l = u, len(lst)
		}
	}
	for u, lst := range mlist {
		if l == len(lst) && u != uuid {
			s := fmt.Sprintf("%s: BMDs have different uuids with no simple majority:\n%v", ciError(60), mlist)
			return nil, &errBmdUUIDSplit{s}
		}
	}
	var err error
	if len(mlist) > 1 {
		s := fmt.Sprintf("%s: BMDs have different uuids with simple majority: %s:\n%v", ciError(70), uuid, mlist)
		err = &errTgtBmdUUIDDiffer{s}
	}
	bmd := maxor[uuid]
	cmn.Assert(bmd.UUID != "")
	return bmd, err
}

func ciError(num int) string {
	const s = "[%s%d - for details, see %s/blob/master/docs/troubleshooting.md]"
	return fmt.Sprintf(s, ciePrefix, num, githubHome)
}
