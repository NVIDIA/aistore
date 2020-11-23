// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/jsp"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/etl"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
	jsoniter "github.com/json-iterator/go"
)

const (
	whatRenamedLB = "renamedlb"
	ciePrefix     = "cluster integrity error: cie#"
	listBuckets   = "listBuckets"
)

const (
	fmtNotCloud   = "%q appears to be ais bucket (expecting cloud)"
	fmtUnknownAct = "unexpected action message <- JSON [%v]"
	fmtUnknownQue = "unexpected query [what=%s]"
	fmtUnsupProv  = "cannot %s: unsupported provider %q"
)

type (
	ClusterMountpathsRaw struct {
		Targets cmn.JSONRawMsgs `json:"targets"`
	}
	reverseProxy struct {
		cloud   *httputil.ReverseProxy // unmodified GET requests => storage.googleapis.com
		primary struct {
			sync.Mutex
			rp  *httputil.ReverseProxy // modify cluster-level md => current primary gateway
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
		ic         ic
		qm         queryMem
		gmm        *memsys.MMSA // system pagesize-based memory manager and slab allocator
	}
)

// interface guard
var _ cmn.Runner = (*proxyrunner)(nil)

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
	p.httprunner.init(config)
	p.owner.bmd = newBMDOwnerPrx(config)

	p.owner.bmd.init() // initialize owner and load BMD

	cluster.InitBckLocker()

	// startup sequence - see earlystart.go for the steps and commentary
	p.bootstrap()

	p.authn = &authManager{
		tokens:        make(map[string]*cmn.AuthToken),
		revokedTokens: make(map[string]bool),
		version:       1,
	}

	p.rproxy.init()

	p.notifs.init(p)
	p.ic.init(p)
	p.qm.init()

	//
	// REST API: register proxy handlers and start listening
	//
	networkHandlers := []networkHandler{
		{r: cmn.Reverse, h: p.reverseHandler, net: []string{cmn.NetworkPublic}},

		// pubnet handlers: cluster must be started
		{r: cmn.Buckets, h: p.bucketHandler, net: []string{cmn.NetworkPublic}},
		{r: cmn.Objects, h: p.objectHandler, net: []string{cmn.NetworkPublic}},
		{r: cmn.Download, h: p.downloadHandler, net: []string{cmn.NetworkPublic}},
		{r: cmn.Query, h: p.queryHandler, net: []string{cmn.NetworkPublic}},
		{r: cmn.ETL, h: p.etlHandler, net: []string{cmn.NetworkPublic}},
		{r: cmn.Sort, h: p.dsortHandler, net: []string{cmn.NetworkPublic}},

		{r: cmn.IC, h: p.ic.handler, net: []string{cmn.NetworkIntraControl}},
		{r: cmn.Daemon, h: p.daemonHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl}},
		{r: cmn.Cluster, h: p.clusterHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl}},
		{r: cmn.Tokens, h: p.tokenHandler, net: []string{cmn.NetworkPublic}},

		{r: cmn.Metasync, h: p.metasyncHandler, net: []string{cmn.NetworkIntraControl}},
		{r: cmn.Health, h: p.healthHandler, net: []string{cmn.NetworkIntraControl}},
		{r: cmn.Vote, h: p.voteHandler, net: []string{cmn.NetworkIntraControl}},

		{r: cmn.Notifs, h: p.notifs.handler, net: []string{cmn.NetworkIntraControl}},

		{r: "/" + cmn.S3, h: p.s3Handler, net: []string{cmn.NetworkPublic}},

		{r: "/", h: p.httpCloudHandler, net: []string{cmn.NetworkPublic}},
	}

	p.registerNetworkHandlers(networkHandlers)

	glog.Infof("%s: [public net] listening on: %s", p.si, p.si.PublicNet.DirectURL)
	if p.si.PublicNet.DirectURL != p.si.IntraControlNet.DirectURL {
		glog.Infof("%s: [intra control net] listening on: %s", p.si, p.si.IntraControlNet.DirectURL)
	}
	if p.si.PublicNet.DirectURL != p.si.IntraDataNet.DirectURL {
		glog.Infof("%s: [intra data net] listening on: %s", p.si, p.si.IntraDataNet.DirectURL)
	}

	dsort.RegisterNode(p.owner.smap, p.owner.bmd, p.si, nil, p.statsT)
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
	if len(res.bytes) == 0 {
		return
	}
	err = p.applyRegMeta(res.bytes, "")
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
	if err = p.receiveBMD(regMeta.BMD, caller); err != nil {
		if !isErrDowngrade(err) {
			glog.Errorf("%s: failed to synch(%q) %s: %v", p.si, msg.Action, regMeta.BMD, err)
		}
	} else {
		glog.Infof("%s: synch %s", p.si, regMeta.BMD)
	}
	// Smap
	if err = p.owner.smap.synchronize(p.si, regMeta.Smap); err != nil {
		if !isErrDowngrade(err) {
			glog.Errorf("%s: failed to synch %s: %v", p.si, regMeta.Smap, err)
		}
	} else {
		glog.Infof("%s: synch %s", p.si, regMeta.Smap)
	}
	return
}

func (p *proxyrunner) unregisterSelf() (int, error) {
	smap := p.owner.smap.get()
	args := callArgs{
		si: smap.Primary,
		req: cmn.ReqArgs{
			Method: http.MethodDelete,
			Path:   cmn.JoinWords(cmn.Version, cmn.Cluster, cmn.Daemon, p.si.ID()),
		},
		timeout: cmn.DefaultTimeout,
	}
	res := p.call(args)
	return res.status, res.err
}

// stop gracefully
func (p *proxyrunner) Stop(err error) {
	var (
		smap      = p.owner.smap.get()
		isPrimary bool
	)
	if smap != nil { // in tests
		isPrimary = smap.isPrimary(p.si)
	}
	glog.Infof("Stopping %s (%s, primary=%t), err: %v", p.Name(), p.si, isPrimary, err)
	xreg.AbortAll()

	if isPrimary {
		p.metasyncer.stopping.Store(true)
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
	if !p.ClusterStarted() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
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
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}

	switch apiItems[0] {
	case cmn.AllBuckets:
		if err := p.checkPermissions(r.Header, nil, cmn.AccessBckLIST); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		query := r.URL.Query()
		bck, err := newBckFromQuery("", query)
		if err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
			return
		}
		p.listBuckets(w, r, cmn.QueryBcks(bck.Bck))
	default:
		p.invalmsghdlrf(w, r, "Invalid route /buckets/%s", apiItems[0])
	}
}

// GET /v1/objects/bucket-name/object-name
func (p *proxyrunner) httpobjget(w http.ResponseWriter, r *http.Request, origURLBck ...string) {
	started := time.Now()
	apiItems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objName := apiItems[0], apiItems[1]

	bckArgs := remBckAddArgs{p: p, w: w, r: r, query: r.URL.Query(), allowHTTPBck: true}
	bck, err := bckArgs.initAndTry(bucket, origURLBck...)
	if err != nil {
		return
	}

	if err := p.checkPermissions(r.Header, &bck.Bck, cmn.AccessGET); err != nil {
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
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s/%s => %s", r.Method, bucket, objName, si)
	}
	redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraData)
	http.Redirect(w, r, redirectURL, http.StatusMovedPermanently)
	p.statsT.Add(stats.GetCount, 1)
}

// PUT /v1/objects/bucket-name/object-name
func (p *proxyrunner) httpobjput(w http.ResponseWriter, r *http.Request) {
	var (
		started = time.Now()
		query   = r.URL.Query()
	)
	apiItems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objName := apiItems[0], apiItems[1]

	bckArgs := remBckAddArgs{p: p, w: w, r: r, query: query}
	bck, err := bckArgs.initAndTry(bucket)
	if err != nil {
		return
	}

	var (
		si       *cluster.Snode
		nodeID   string
		smap     = p.owner.smap.get()
		appendTy = query.Get(cmn.URLParamAppendType)
	)
	if appendTy == "" {
		if err := p.checkPermissions(r.Header, &bck.Bck, cmn.AccessPUT); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		err = bck.Allow(cmn.AccessPUT)
	} else {
		if err := p.checkPermissions(r.Header, &bck.Bck, cmn.AccessAPPEND); err != nil {
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
	apiItems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objName := apiItems[0], apiItems[1]
	bckArgs := remBckAddArgs{p: p, w: w, r: r, query: r.URL.Query()}
	bck, err := bckArgs.initAndTry(bucket)
	if err != nil {
		return
	}
	if err := p.checkPermissions(r.Header, &bck.Bck, cmn.AccessObjDELETE); err != nil {
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
	var (
		msg   cmn.ActionMsg
		query = r.URL.Query()
	)
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}
	bucket := apiItems[0]

	bckArgs := remBckAddArgs{p: p, w: w, r: r, query: query, msg: &msg}
	bck, err := bckArgs.initAndTry(bucket)
	if err != nil {
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
		if p.forwardCP(w, r, &msg, bucket) {
			return
		}
		if err := p.checkPermissions(r.Header, &bck.Bck, cmn.AccessBckDELETE); err != nil {
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
		if err := p.checkPermissions(r.Header, &bck.Bck, cmn.AccessObjDELETE); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		if msg.Action == cmn.ActEvictObjects && bck.IsAIS() {
			p.invalmsghdlrf(w, r, fmtNotCloud, bucket)
			return
		}
		var xactID string
		if xactID, err = p.doListRange(http.MethodDelete, bucket, &msg, query); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		w.Write([]byte(xactID))
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
		var (
			xact   = xreg.GetXactRunning(cmn.ActElection)
			msg    = p.si.String() + ": is primary, cannot be on the receiving side of metasync"
			detail string
		)
		if xact != nil {
			detail = fmt.Sprintf(" (%s)", xact)
		}
		p.invalmsghdlrf(w, r, msg, p.si, detail)
		return
	}
	payload := make(msPayload)
	if _, err := jsp.Decode(r.Body, &payload, jspMetasyncOpts, "metasync put"); err != nil {
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
			glog.Infof("%s: new %s from %s", p.si, newSmap, caller)
		}
		if err = p.owner.smap.synchronize(p.si, newSmap); err != nil {
			if !isErrDowngrade(err) {
				errs = append(errs, fmt.Errorf("failed to synch %s: %v", newSmap, err))
			}
			goto ExtractRMD
		}
		node := newSmap.GetNode(p.si.ID())
		p.si.Flags = node.Flags

		// When some node was removed from the cluster we need to clean up the
		// reverse proxy structure.
		p.rproxy.nodes.Range(func(key, _ interface{}) bool {
			nodeID := key.(string)
			if smap.containsID(nodeID) && !newSmap.containsID(nodeID) {
				p.rproxy.nodes.Delete(nodeID)
			}
			return true
		})
		p.syncNewICOwners(smap, newSmap)
	}

ExtractRMD:
	newRMD, msgRMD, err := p.extractRMD(payload)
	if err != nil {
		errs = append(errs, err)
	} else if newRMD != nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s: new %s from %s", p.si, newRMD, caller)
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
			glog.Infof("%s: new %s(%q) from %s", p.si, newBMD, msgBMD.Action, caller)
		}
		if err = p.receiveBMD(newBMD, caller); err != nil && !isErrDowngrade(err) {
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

func (p *proxyrunner) syncNewICOwners(smap, newSmap *smapX) {
	if !smap.IsIC(p.si) || !newSmap.IsIC(p.si) {
		return
	}

	for _, psi := range newSmap.Pmap {
		if p.si.ID() != psi.ID() && psi.IsIC() && !smap.IsIC(psi) {
			go func(psi *cluster.Snode) {
				if err := p.ic.sendOwnershipTbl(psi); err != nil {
					glog.Errorf("%s: failed to send ownership table to %s, err:%v", p.si, psi, err)
				}
			}(psi)
		}
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
		smap    = p.owner.smap.get()
		primary = smap != nil && smap.version() > 0 && smap.isPrimary(p.si)
		query   = r.URL.Query()
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
	cii.Smap.PrimaryURL = smap.Primary.IntraControlNet.DirectURL
}

// POST { action } /v1/buckets[/bucket-name]
func (p *proxyrunner) httpbckpost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	var msg cmn.ActionMsg
	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}
	switch len(apiItems) {
	case 0:
		p.hpostAllBuckets(w, r, &msg)
	case 1:
		bucket := apiItems[0]
		p.hpostBucket(w, r, &msg, bucket)
	default:
		p.invalmsghdlrf(w, r, "URL path %v is too long (action %s)", apiItems, msg.Action)
	}
}

func (p *proxyrunner) hpostBucket(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg, bucket string) {
	query := r.URL.Query()
	bck, err := newBckFromQuery(bucket, query)
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
			p.reverseReqRemote(w, r, msg, bck.Bck)
			return
		}
	}
	// 3. createlb
	if msg.Action == cmn.ActCreateLB {
		p.hpostCreateBucket(w, r, msg, bck)
		return
	}
	// only the primary can do metasync
	xactDtor := xaction.XactsDtor[msg.Action]
	if xactDtor.Metasync {
		if p.forwardCP(w, r, msg, bucket) {
			return
		}
	}
	if err = bck.Init(p.owner.bmd, p.si); err != nil {
		args := remBckAddArgs{p: p, w: w, r: r, queryBck: bck, err: err, msg: msg}
		if bck, err = args.try(); err != nil {
			return
		}
	}
	//
	// {action} on bucket
	//
	switch msg.Action {
	case cmn.ActRenameLB:
		if err := p.checkPermissions(r.Header, &bck.Bck, cmn.AccessBckRENAME); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		if !bck.IsAIS() {
			p.invalmsghdlrf(w, r, "cannot rename bucket %q, it is not a local bucket", bck)
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
		var xactID string
		if xactID, err = p.renameBucket(bckFrom, bckTo, msg); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		w.Write([]byte(xactID))
	case cmn.ActCopyBucket, cmn.ActETLBucket:
		if err := p.checkPermissions(r.Header, &bck.Bck, cmn.AccessGET); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}

		if msg.Value == nil {
			p.invalmsghdlr(w, r, "request body can't be empty", http.StatusBadRequest)
			return
		}

		var (
			internalMsg = &cmn.Bck2BckMsg{}
			bckTo       *cluster.Bck
		)

		switch msg.Action {
		case cmn.ActETLBucket:
			if err := cmn.MorphMarshal(msg.Value, internalMsg); err != nil {
				p.invalmsghdlr(w, r, "request body can't be empty", http.StatusBadRequest)
				return
			}

			if internalMsg.ID == "" {
				p.invalmsghdlr(w, r, etl.ErrMissingUUID.Error(), http.StatusBadRequest)
				return
			}
		case cmn.ActCopyBucket:
			cpyBckMsg := &cmn.CopyBckMsg{}
			if err = cmn.MorphMarshal(msg.Value, cpyBckMsg); err != nil {
				return
			}
			internalMsg.BckTo = cpyBckMsg.BckTo
			internalMsg.DryRun = cpyBckMsg.DryRun
			internalMsg.Prefix = cpyBckMsg.Prefix
		}

		// userBckTo is a bucket from a user - it means that it can be not initialized, missing provider, etc.
		userBckTo := cluster.NewBckEmbed(internalMsg.BckTo)

		if bck.Equal(userBckTo, false, true) {
			p.invalmsghdlrf(w, r, "cannot %s bucket %q onto itself", msg.Action, bucket)
			return
		}

		bckToArgs := remBckAddArgs{p: p, w: w, r: r, queryBck: userBckTo, allowBckNotExist: true}
		if bckTo, err = bckToArgs.initAndTry(userBckTo.Name); err != nil {
			return
		}

		if bckTo != nil {
			if err = bckTo.Allow(cmn.AccessSYNC); err != nil {
				p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
				return
			}

			if bckTo.IsHTTP() {
				p.invalmsghdlr(w, r, "copying to HTTP buckets not supported")
				return
			}
		} else {
			// It is a non existing ais bucket.
			bckTo = userBckTo
		}

		// bckTo is initialized by proxy, should be send to targets instead of userBck.
		internalMsg.BckTo = bckTo.Bck
		msg.Value = internalMsg

		glog.Infof("%s bucket %s => %s", msg.Action, bck, bckTo)

		var xactID string
		if xactID, err = p.bucketToBucketTxn(bck, bckTo, msg, internalMsg.DryRun); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}

		w.Write([]byte(xactID))
	case cmn.ActRegisterCB:
		// TODO: choose the best permission
		if err := p.checkPermissions(r.Header, &bck.Bck, cmn.AccessBckCREATE); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		if err := p.createBucket(msg, bck); err != nil {
			errCode := http.StatusInternalServerError
			if _, ok := err.(*cmn.ErrorBucketAlreadyExists); ok {
				errCode = http.StatusConflict
			}
			p.invalmsghdlr(w, r, err.Error(), errCode)
			return
		}
	case cmn.ActPrefetch:
		// TODO: GET vs SYNC?
		if err := p.checkPermissions(r.Header, &bck.Bck, cmn.AccessGET); err != nil {
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
		var xactID string
		if xactID, err = p.doListRange(http.MethodPost, bucket, msg, query); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		w.Write([]byte(xactID))
	case cmn.ActListObjects:
		begin := mono.NanoTime()
		if err := p.checkPermissions(r.Header, &bck.Bck, cmn.AccessObjLIST); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		if err = bck.Allow(cmn.AccessObjLIST); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
			return
		}
		p.listObjects(w, r, bck, msg, begin)
	case cmn.ActInvalListCache:
		if err = bck.Allow(cmn.AccessObjLIST); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
			return
		}
		p.qm.c.invalidate(bck.Bck)
	case cmn.ActSummaryBucket:
		if err := p.checkPermissions(r.Header, &bck.Bck, cmn.AccessObjLIST); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		if err = bck.Allow(cmn.AccessBckHEAD); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
			return
		}
		p.bucketSummary(w, r, bck, msg)
	case cmn.ActMakeNCopies:
		if err := p.checkPermissions(r.Header, &bck.Bck, cmn.AccessMAKENCOPIES); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		if err = bck.Allow(cmn.AccessMAKENCOPIES); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
			return
		}
		var xactID string
		if xactID, err = p.makeNCopies(msg, bck); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		w.Write([]byte(xactID))
	case cmn.ActECEncode:
		if err := p.checkPermissions(r.Header, &bck.Bck, cmn.AccessEC); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		if err = bck.Allow(cmn.AccessEC); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
			return
		}
		var xactID string
		if xactID, err = p.ecEncode(bck, msg); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		w.Write([]byte(xactID))
	default:
		p.invalmsghdlrf(w, r, fmtUnknownAct, msg)
	}
}

func (p *proxyrunner) hpostCreateBucket(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg, bck *cluster.Bck) {
	bucket := bck.Name
	err := p.checkPermissions(r.Header, nil, cmn.AccessBckCREATE)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
		return
	}
	if err = cmn.ValidateBckName(bucket); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if bck.Bck.Provider != "" && !bck.Bck.IsAIS() {
		p.invalmsghdlrf(w, r, fmtUnsupProv, msg.Action, bck.Provider)
		return
	}
	if p.forwardCP(w, r, msg, bucket) {
		return
	}
	bck.Provider = cmn.ProviderAIS
	if msg.Value != nil {
		propsToUpdate := cmn.BucketPropsToUpdate{}
		if err := cmn.MorphMarshal(msg.Value, &propsToUpdate); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		// make and validate nprops
		bck.Props = cmn.DefaultAISBckProps()
		bck.Props.Provider = bck.Provider
		bck.Props, err = p.makeNprops(bck, propsToUpdate, true /*creating*/)
		if err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		if bck.HasBackendBck() {
			// initialize backend
			backend := cluster.BackendBck(bck)
			if err = backend.InitNoBackend(p.owner.bmd, p.si); err != nil {
				if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); !ok {
					p.invalmsghdlrf(w, r,
						"cannot create %s: failing to initialize backend %s, err: %v",
						bck, backend, err)
					return
				}
				args := remBckAddArgs{p: p, w: w, r: r, queryBck: backend, err: err, msg: msg}
				if _, err = args.try(); err != nil {
					return
				}
			}
		}
	}
	if err := p.createBucket(msg, bck); err != nil {
		errCode := http.StatusInternalServerError
		if _, ok := err.(*cmn.ErrorBucketAlreadyExists); ok {
			errCode = http.StatusConflict
		}
		p.invalmsghdlr(w, r, err.Error(), errCode)
	}
}

func (p *proxyrunner) hpostAllBuckets(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg) {
	query := r.URL.Query()
	if err := p.checkPermissions(r.Header, nil, cmn.AccessBckLIST); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
		return
	}

	switch msg.Action {
	case cmn.ActRecoverBck:
		p.recoverBuckets(w, r, msg)
	case cmn.ActSyncLB:
		if p.forwardCP(w, r, msg, "") {
			return
		}
		bmd := p.owner.bmd.get()
		msg := p.newAisMsg(msg, nil, bmd)
		_ = p.metasyncer.sync(revsPair{bmd, msg})
	case cmn.ActSummaryBucket:
		bck, err := newBckFromQuery("", query)
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
		p.invalmsghdlrf(w, r, "all buckets: invalid action %q", msg.Action)
	}
}

func (p *proxyrunner) listObjects(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, amsg *cmn.ActionMsg, begin int64) {
	var (
		err     error
		bckList *cmn.BucketList
		smsg    = cmn.SelectMsg{}
		smap    = p.owner.smap.get()
	)
	if err := cmn.MorphMarshal(amsg.Value, &smsg); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if smap.CountActiveTargets() < 1 {
		p.invalmsghdlr(w, r, "No registered targets yet")
		return
	}

	// If props were not explicitly specified always return default ones.
	if smsg.Props == "" {
		smsg.AddProps(cmn.GetPropsDefault...)
	}

	// Vanilla HTTP buckets do not support remote listing
	if bck.IsHTTP() {
		smsg.SetFlag(cmn.SelectCached)
	}

	locationIsAIS := bck.IsAIS() || smsg.IsFlagSet(cmn.SelectCached)
	if smsg.UUID == "" {
		var nl nl.NotifListener
		smsg.UUID = cmn.GenUUID()
		if locationIsAIS || smsg.NeedLocalMD() {
			nl = xaction.NewXactNL(smsg.UUID,
				cmn.ActListObjects, &smap.Smap, nil, bck.Bck)
		} else {
			// random target to execute `list-objects` on a Cloud bucket
			si, _ := smap.GetRandTarget()
			nl = xaction.NewXactNL(smsg.UUID, cmn.ActListObjects,
				&smap.Smap, cluster.NodeMap{si.ID(): si}, bck.Bck)
		}
		nl.SetHrwOwner(&smap.Smap)
		p.ic.registerEqual(regIC{nl: nl, smap: smap, msg: amsg})
	}

	if p.ic.reverseToOwner(w, r, smsg.UUID, amsg) {
		return
	}

	if locationIsAIS {
		bckList, err = p.listObjectsAIS(bck, smsg)
	} else {
		bckList, err = p.listObjectsRemote(bck, smsg)
		// TODO: `status == http.StatusGone` At this point we know that this
		//  cloud bucket exists and is offline. We should somehow try to list
		//  cached objects. This isn't easy as we basically need to start a new
		//  xaction and return new `UUID`.
	}
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	cmn.Assert(bckList != nil)

	if strings.Contains(r.Header.Get(cmn.HeaderAccept), cmn.ContentMsgPack) {
		if !p.writeMsgPack(w, r, bckList, "list_objects") {
			return
		}
	} else if !p.writeJSON(w, r, bckList, "list_objects") {
		return
	}

	delta := mono.Since(begin)
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("LIST: bck: %q, token: %q, %s", bck, bckList.ContinuationToken, delta)
	}

	// Free memory allocated for temporary slice immediately as it can take up to a few GB
	bckList.Entries = bckList.Entries[:0]
	bckList.Entries = nil
	bckList = nil

	p.statsT.AddMany(
		stats.NamedVal64{Name: stats.ListCount, Value: 1},
		stats.NamedVal64{Name: stats.ListLatency, Value: int64(delta)},
	)
}

// bucket == "": all buckets for a given provider
func (p *proxyrunner) bucketSummary(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, amsg *cmn.ActionMsg) {
	var (
		err       error
		uuid      string
		summaries cmn.BucketsSummaries
		smsg      = cmn.BucketSummaryMsg{}
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

	if summaries, uuid, err = p.gatherBucketSummary(bck, &smsg); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	// uuid == "" means that async runner has completed and the result is available
	// otherwise it is an ID of a still running task
	if uuid != "" {
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(uuid))
		return
	}

	p.writeJSON(w, r, summaries, "bucket_summary")
}

func (p *proxyrunner) gatherBucketSummary(bck *cluster.Bck, msg *cmn.BucketSummaryMsg) (
	summaries cmn.BucketsSummaries, uuid string, err error) {
	var (
		isNew, q = p.initAsyncQuery(bck, msg, cmn.GenUUID())
		config   = cmn.GCO.Get()
	)
	var (
		smap   = p.owner.smap.get()
		aisMsg = p.newAisMsg(&cmn.ActionMsg{Action: cmn.ActSummaryBucket, Value: msg}, smap, nil)
		body   = cmn.MustMarshal(aisMsg)
		args   = bcastArgs{
			req: cmn.ReqArgs{
				Method: http.MethodPost,
				Path:   cmn.JoinWords(cmn.Version, cmn.Buckets, bck.Name),
				Query:  q,
				Body:   body,
			},
			smap:    smap,
			timeout: config.Timeout.MaxHostBusy + config.Timeout.MaxKeepalive,
		}
	)
	results := p.bcastToGroup(args)
	allOK, _, err := p.checkBckTaskResp(msg.UUID, results)
	if err != nil {
		return nil, "", err
	}

	// some targets are still executing their tasks or it is request to start
	// an async task. The proxy returns only uuid to a caller
	if !allOK || isNew {
		return nil, msg.UUID, nil
	}

	// all targets are ready, prepare the final result
	q = url.Values{}
	q = cmn.AddBckToQuery(q, bck.Bck)
	q.Set(cmn.URLParamTaskAction, cmn.TaskResult)
	q.Set(cmn.URLParamSilent, "true")
	args.req.Query = q
	args.fv = func() interface{} { return &cmn.BucketsSummaries{} }
	summaries = make(cmn.BucketsSummaries, 0)
	for result := range p.bcastToGroup(args) {
		if result.err != nil {
			return nil, "", result.err
		}

		targetSummary := result.v.(*cmn.BucketsSummaries)
		for _, bckSummary := range *targetSummary {
			summaries = summaries.Aggregate(bckSummary)
		}
	}

	return summaries, "", nil
}

// POST { action } /v1/objects/bucket-name[/object-name]
func (p *proxyrunner) httpobjpost(w http.ResponseWriter, r *http.Request) {
	var (
		msg   cmn.ActionMsg
		query = r.URL.Query()
	)
	apiItems, err := p.checkRESTItems(w, r, 1, true, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket := apiItems[0]
	bck, err := newBckFromQuery(bucket, query)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}

	// TODO: revisit versus cloud bucket not being present, see p.tryBckInit
	if err = bck.Init(p.owner.bmd, p.si); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case cmn.ActRenameObject:
		if err := p.checkPermissions(r.Header, &bck.Bck, cmn.AccessObjRENAME); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		if bck.IsRemote() {
			p.invalmsghdlrf(w, r, "%q is not supported for remote buckets (%s)", msg.Action, bck)
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
		if err := p.checkPermissions(r.Header, &bck.Bck, cmn.AccessPROMOTE); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
			return
		}
		if err = bck.Allow(cmn.AccessPROMOTE); err != nil {
			p.invalmsghdlr(w, r, err.Error(), http.StatusForbidden)
			return
		}
		if !filepath.IsAbs(msg.Name) {
			p.invalmsghdlr(w, r, "source must be an absolute path", http.StatusBadRequest)
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
	var (
		query     = r.URL.Query()
		hasLatest bool
	)
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apiItems[0]
	bck, err := newBckFromQuery(bucket, query)
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
		hasLatest = true
	}
	if err := p.checkPermissions(r.Header, &bck.Bck, cmn.AccessBckHEAD); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
		return
	}
	if bck.IsAIS() || hasLatest {
		p.bucketPropsToHdr(bck, w.Header())
		return
	}

	cloudProps, statusCode, err := p.headCloudBck(*bck.RemoteBck(), nil)
	if err != nil {
		// TODO -- FIXME: decide what needs to be done when HEAD fails - changes to BMD
		p.invalmsghdlr(w, r, err.Error(), statusCode)
		return
	}

	nprops := cmn.MergeCloudBckProps(bck.Props, cloudProps)
	if nprops.Equal(bck.Props) {
		p.bucketPropsToHdr(bck, w.Header())
		return
	}
	if p.forwardCP(w, r, nil, "httpheadbck") {
		return
	}
	glog.Warningf("%s: Cloud bucket %s properties have changed, resynchronizing...", p.si, bck)

	ctx := &bmdModifier{
		pre:        p._bckHeadPre,
		final:      p._syncBMDFinal,
		msg:        &cmn.ActionMsg{Action: cmn.ActResyncBprops},
		bcks:       []*cluster.Bck{bck},
		cloudProps: cloudProps,
	}

	_, err = p.owner.bmd.modify(ctx)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusNotFound)
		return
	}

	p.bucketPropsToHdr(bck, w.Header())
}

func (p *proxyrunner) _bckHeadPre(ctx *bmdModifier, clone *bucketMD) error {
	var (
		bck             = ctx.bcks[0]
		bprops, present = clone.Get(bck)
	)
	if !present {
		return cmn.NewErrorBucketDoesNotExist(bck.Bck, p.si.String())
	}
	nprops := cmn.MergeCloudBckProps(bprops, ctx.cloudProps)
	if nprops.Equal(bprops) {
		glog.Warningf("%s: Cloud bucket %s properties are already in-sync, nothing to do", p.si, bck)
		ctx.terminate = true
		return nil
	}
	clone.set(bck, nprops)
	return nil
}

// PATCH /v1/buckets/bucket-name
func (p *proxyrunner) httpbckpatch(w http.ResponseWriter, r *http.Request) {
	var (
		propsToUpdate cmn.BucketPropsToUpdate
		bucket        string
		bck           *cluster.Bck
		msg           *cmn.ActionMsg
		query         = r.URL.Query()
		apiItems, err = p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	)
	if err != nil {
		return
	}
	bucket = apiItems[0]
	if bck, err = newBckFromQuery(bucket, query); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	if p.forwardCP(w, r, msg, "httpbckpatch") {
		return
	}
	msg = &cmn.ActionMsg{Value: &propsToUpdate}
	if err = cmn.ReadJSON(w, r, msg); err != nil {
		return
	}
	if err = bck.InitNoBackend(p.owner.bmd, p.si); err != nil {
		args := remBckAddArgs{p: p, w: w, r: r, queryBck: bck, err: err, msg: msg}
		if bck, err = args.try(); err != nil {
			return
		}
	}
	if err := p.checkPermissions(r.Header, &bck.Bck, cmn.AccessPATCH); err != nil {
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

	var xactID string
	if xactID, err = p.setBucketProps(w, r, msg, bck, propsToUpdate); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	w.Write([]byte(xactID))
}

// HEAD /v1/objects/bucket-name/object-name
func (p *proxyrunner) httpobjhead(w http.ResponseWriter, r *http.Request, origURLBck ...string) {
	var (
		started       = time.Now()
		query         = r.URL.Query()
		checkExists   = cmn.IsParseBool(query.Get(cmn.URLParamCheckExists))
		apiItems, err = p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	)
	if err != nil {
		return
	}
	bucket, objName := apiItems[0], apiItems[1]

	bckArgs := remBckAddArgs{p: p, w: w, r: r, query: query, allowHTTPBck: true}
	bck, err := bckArgs.initAndTry(bucket, origURLBck...)
	if err != nil {
		return
	}
	if err := p.checkPermissions(r.Header, &bck.Bck, cmn.AccessObjHEAD); err != nil {
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
func (p *proxyrunner) forwardCP(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg, s string) (forf bool) {
	var (
		body []byte
		smap = p.owner.smap.get()
	)
	if smap == nil || !smap.isValid() {
		if msg != nil {
			s = fmt.Sprintf(`%s must be starting up: cannot execute "%s:%s"`, p.si, msg.Action, s)
		} else {
			s = fmt.Sprintf("%s must be starting up: cannot execute %q", p.si, s)
		}
		p.invalmsghdlr(w, r, s)
		return true
	}
	if p.inPrimaryTransition.Load() {
		p.invalmsghdlr(w, r, "primary is in transition, cannot process the request", http.StatusServiceUnavailable)
		return
	}
	if smap.isPrimary(p.si) {
		return
	}
	// We must **not** send any request body when doing HEAD request.
	// Otherwise, the request can be rejected and terminated.
	if r.Method != http.MethodHead && msg != nil {
		body = cmn.MustMarshal(msg)
	}
	primary := &p.rproxy.primary
	primary.Lock()
	if primary.url != smap.Primary.PublicNet.DirectURL {
		primary.url = smap.Primary.PublicNet.DirectURL
		uparsed, err := url.Parse(smap.Primary.PublicNet.DirectURL)
		cmn.AssertNoErr(err)
		cfg := cmn.GCO.Get()
		primary.rp = httputil.NewSingleHostReverseProxy(uparsed)
		primary.rp.Transport = cmn.NewTransport(cmn.TransportArgs{
			UseHTTPS:   cfg.Net.HTTP.UseHTTPS,
			SkipVerify: cfg.Net.HTTP.SkipVerify,
		})
		primary.rp.ErrorHandler = p.rpErrHandler
	}
	primary.Unlock()
	if len(body) > 0 {
		r.Body = ioutil.NopCloser(bytes.NewBuffer(body))
		r.ContentLength = int64(len(body)) // Directly setting `Content-Length` header.
	}
	if msg != nil {
		glog.Infof(`%s: forwarding "%s:%s" to the primary %s`, p.si, msg.Action, s, smap.Primary)
	} else {
		glog.Infof("%s: forwarding %q to the primary %s", p.si, s, smap.Primary)
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
	var rproxy *httputil.ReverseProxy

	val, ok := p.rproxy.nodes.Load(nodeID)
	if ok {
		rproxy = val.(*httputil.ReverseProxy)
	} else {
		cfg := cmn.GCO.Get()
		rproxy = httputil.NewSingleHostReverseProxy(parsedURL)
		rproxy.Transport = cmn.NewTransport(cmn.TransportArgs{
			UseHTTPS:   cfg.Net.HTTP.UseHTTPS,
			SkipVerify: cfg.Net.HTTP.SkipVerify,
		})
		rproxy.ErrorHandler = p.rpErrHandler
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

func (p *proxyrunner) initAsyncQuery(bck *cluster.Bck, msg *cmn.BucketSummaryMsg, newTaskID string) (bool, url.Values) {
	isNew := msg.UUID == ""
	q := url.Values{}
	if isNew {
		msg.UUID = newTaskID
		q.Set(cmn.URLParamTaskAction, cmn.TaskStart)
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("proxy: starting new async task %s", msg.UUID)
		}
	} else {
		// First request is always 'Status' to avoid wasting gigabytes of
		// traffic in case when few targets have finished their tasks.
		q.Set(cmn.URLParamTaskAction, cmn.TaskStatus)
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("proxy: reading async task %s result", msg.UUID)
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

// listObjectsAIS reads object list from all targets, combines, sorts and returns
// the final list. Excess of object entries from each target is remembered in the
// buffer (see: `queryBuffers`) so we won't request the same objects again.
func (p *proxyrunner) listObjectsAIS(bck *cluster.Bck, smsg cmn.SelectMsg) (allEntries *cmn.BucketList, err error) {
	if smsg.PageSize == 0 {
		smsg.PageSize = cmn.DefaultListPageSizeAIS
	}

	var (
		aisMsg *aisMsg
		args   bcastArgs
		smap   = p.owner.smap.get()

		hasEnough bool
		entries   []*cmn.BucketEntry
		cacheID   = cacheReqID{bck: bck.Bck, prefix: smsg.Prefix}
		token     = smsg.ContinuationToken
		pageSize  = smsg.PageSize
		props     = smsg.PropsSet()
	)

	// TODO: Before checking cache and buffer we should check if there is another
	//  request already in-flight that requests the same page as we do - if yes
	//  then we should just patiently wait for the cache to get populated.

	if smsg.UseCache {
		entries, hasEnough = p.qm.c.get(cacheID, token, pageSize)
		if hasEnough {
			goto end
		}
		// Request for all the props if (cache should always have all entries).
		smsg.AddProps(cmn.GetPropsAll...)
	}
	entries, hasEnough = p.qm.b.get(smsg.UUID, token, pageSize)
	if hasEnough {
		// We have enough in the buffer to fulfill the request.
		goto endWithCache
	}

	// User requested some page but we don't have enough (but we may have part
	// of the full page). Therefore, we must ask targets for page starting from
	// what we have locally, so we don't re-request the objects.
	smsg.ContinuationToken = p.qm.b.last(smsg.UUID, token)

	aisMsg = p.newAisMsg(&cmn.ActionMsg{Action: cmn.ActListObjects, Value: &smsg}, smap, nil)
	args = bcastArgs{
		req: cmn.ReqArgs{
			Method: http.MethodPost,
			Path:   cmn.JoinWords(cmn.Version, cmn.Buckets, bck.Name),
			Query:  cmn.AddBckToQuery(nil, bck.Bck),
			Body:   cmn.MustMarshal(aisMsg),
		},
		timeout: cmn.LongTimeout, // TODO: should it be `Client.ListObjects`?
		smap:    smap,
		fv:      func() interface{} { return &cmn.BucketList{} },
	}

	// Combine the results.
	for res := range p.bcastToGroup(args) {
		if res.err != nil {
			return nil, res.err
		}
		objList := res.v.(*cmn.BucketList)
		p.qm.b.set(smsg.UUID, res.si.ID(), objList.Entries, pageSize)
	}
	entries, hasEnough = p.qm.b.get(smsg.UUID, token, pageSize)
	cmn.Assert(hasEnough)

endWithCache:
	if smsg.UseCache {
		p.qm.c.set(cacheID, token, entries, pageSize)
	}
end:
	if smsg.UseCache && !props.All(cmn.GetPropsAll...) {
		// Since cache keeps entries with whole subset props we must create copy
		// of the entries with smaller subset of props (if we would change the
		// props of the `entries` it would also affect entries inside cache).
		propsEntries := make([]*cmn.BucketEntry, len(entries))
		for idx := range entries {
			propsEntries[idx] = entries[idx].CopyWithProps(props)
		}
		entries = propsEntries
	}

	allEntries = &cmn.BucketList{
		UUID:    smsg.UUID,
		Entries: entries,
	}
	if uint(len(entries)) >= pageSize {
		allEntries.ContinuationToken = entries[len(entries)-1].Name
	}
	return allEntries, nil
}

// listObjectsRemote returns the list of objects from requested remote bucket
// (cloud or remote AIS). If request requires local data then it is broadcast
// to all targets which perform traverse on the disks, otherwise random target
// is chosen to perform cloud listing.
func (p *proxyrunner) listObjectsRemote(bck *cluster.Bck, smsg cmn.SelectMsg) (allEntries *cmn.BucketList, err error) {
	if smsg.StartAfter != "" {
		return nil, fmt.Errorf("start after for cloud buckets is not yet supported")
	}

	var (
		smap       = p.owner.smap.get()
		reqTimeout = cmn.GCO.Get().Client.ListObjects

		aisMsg = p.newAisMsg(&cmn.ActionMsg{Action: cmn.ActListObjects, Value: &smsg}, smap, nil)
		args   = bcastArgs{
			req: cmn.ReqArgs{
				Method: http.MethodPost,
				Path:   cmn.JoinWords(cmn.Version, cmn.Buckets, bck.Name),
				Query:  cmn.AddBckToQuery(nil, bck.Bck),
				Body:   cmn.MustMarshal(aisMsg),
			},
			timeout: reqTimeout,
			smap:    smap,
			fv:      func() interface{} { return &cmn.BucketList{} },
		}
		results chan callResult
	)

	if smsg.NeedLocalMD() {
		results = p.bcastToGroup(args)
	} else {
		nl, exists := p.notifs.entry(smsg.UUID)
		// NOTE: We register a listobj xaction before starting the actual listing
		cmn.Assert(exists)

		for _, si := range nl.Notifiers() {
			res := p.call(callArgs{si: si, req: args.req, timeout: reqTimeout, v: &cmn.BucketList{}})
			results = make(chan callResult, 1)
			results <- res
			close(results)
			break
		}
	}

	// Combine the results.
	bckLists := make([]*cmn.BucketList, 0, len(results))
	for res := range results {
		if res.status == http.StatusNotFound { // TODO -- FIXME
			continue
		}
		if res.err != nil {
			return nil, res.err
		}
		bckLists = append(bckLists, res.v.(*cmn.BucketList))
	}

	// Maximum objects in the final result page. Take all objects in
	// case of Cloud and no limit is set by a user.
	allEntries = cmn.MergeObjLists(bckLists, 0)
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("Objects after merge: %d, token: %q", len(allEntries.Entries), allEntries.ContinuationToken)
	}

	if smsg.WantProp(cmn.GetTargetURL) {
		for _, e := range allEntries.Entries {
			si, err := cluster.HrwTarget(bck.MakeUname(e.Name), &smap.Smap)
			if err == nil {
				e.TargetURL = si.URL(cmn.NetworkPublic)
			}
		}
	}

	return allEntries, nil
}

func (p *proxyrunner) objRename(w http.ResponseWriter, r *http.Request, bck *cluster.Bck) {
	started := time.Now()
	apiItems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	objName := apiItems[1]
	smap := p.owner.smap.get()
	si, err := cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("RENAME %s %s/%s => %s", r.Method, bck.Name, objName, si)
	}

	// NOTE: Code 307 is the only way to http-redirect with the original JSON payload.
	redirectURL := p.redirectURL(r, si, started, cmn.NetworkIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)

	p.statsT.Add(stats.RenameCount, 1)
}

func (p *proxyrunner) promoteFQN(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, msg *cmn.ActionMsg) {
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}

	promoteArgs := cmn.ActValPromote{}
	if err := cmn.MorphMarshal(msg.Value, &promoteArgs); err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	var (
		started = time.Now()
		smap    = p.owner.smap.get()
		bucket  = apiItems[0]
	)

	// designated target ID
	if promoteArgs.Target != "" {
		tsi := smap.GetTarget(promoteArgs.Target)
		if tsi == nil {
			err = &errNodeNotFound{cmn.ActPromote + " failure", promoteArgs.Target, p.si, smap}
			p.invalmsghdlr(w, r, err.Error())
			return
		}

		// NOTE: Code 307 is the only way to http-redirect with the original JSON payload.
		redirectURL := p.redirectURL(r, tsi, started, cmn.NetworkIntraControl)
		http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
		return
	}

	// all targets
	//
	// TODO -- FIXME: 2phase begin to check space, validate params, and check vs running xactions
	//
	query := cmn.AddBckToQuery(nil, bck.Bck)
	results := p.callTargets(http.MethodPost, cmn.JoinWords(cmn.Version, cmn.Objects, bucket),
		cmn.MustMarshal(msg), query)

	for res := range results {
		if res.err != nil {
			p.invalmsghdlrf(w, r, "%s failed, err: %s", msg.Action, res.err)
			return
		}
	}
}

func (p *proxyrunner) doListRange(method, bucket string, msg *cmn.ActionMsg, query url.Values) (xactID string, err error) {
	var (
		timeout time.Duration
		results chan callResult
		wait    bool
	)
	var (
		smap   = p.owner.smap.get()
		aisMsg = p.newAisMsg(msg, smap, nil, cmn.GenUUID())
		body   = cmn.MustMarshal(aisMsg)
		path   = cmn.JoinWords(cmn.Version, cmn.Buckets, bucket)
	)
	if wait {
		timeout = cmn.GCO.Get().Client.ListObjects
	} else {
		timeout = cmn.DefaultTimeout
	}

	nlb := xaction.NewXactNL(aisMsg.UUID, aisMsg.Action, &smap.Smap, nil)
	nlb.SetOwner(equalIC)
	p.ic.registerEqual(regIC{smap: smap, query: query, nl: nlb})
	results = p.bcastToGroup(bcastArgs{
		req:     cmn.ReqArgs{Method: method, Path: path, Query: query, Body: body},
		smap:    smap,
		timeout: timeout,
	})
	for res := range results {
		if res.err != nil {
			err = fmt.Errorf("%s failed to %s List/Range: %v (%d: %s)",
				res.si, msg.Action, res.err, res.status, res.details)
		}
	}
	xactID = aisMsg.UUID
	return
}

func (p *proxyrunner) reverseHandler(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Reverse)
	if err != nil {
		return
	}

	// rewrite URL path (removing `cmn.Reverse`)
	r.URL.Path = cmn.JoinWords(cmn.Version, apiItems[0])

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
	case http.MethodPost:
		p.httpdaepost(w, r)
	default:
		p.invalmsghdlrf(w, r, "invalid method %s for /daemon path", r.Method)
	}
}

func (p *proxyrunner) handlePendingRenamedLB(renamedBucket string) {
	ctx := &bmdModifier{
		pre:   p._pendingRnPre,
		final: p._syncBMDFinal,
		msg:   &cmn.ActionMsg{Value: cmn.ActRenameLB},
		bcks:  []*cluster.Bck{cluster.NewBck(renamedBucket, cmn.ProviderAIS, cmn.NsGlobal)},
	}

	p.owner.bmd.modify(ctx)
}

func (p *proxyrunner) _pendingRnPre(ctx *bmdModifier, clone *bucketMD) error {
	var (
		bck            = ctx.bcks[0]
		props, present = clone.Get(bck)
	)
	if !present {
		ctx.terminate = true
		// Already removed via the the very first target calling here.
		return nil
	}
	if props.Renamed == "" {
		glog.Errorf("%s: renamed bucket %s: unexpected props %+v", p.si, bck.Name, *bck.Props)
		ctx.terminate = true
		return nil
	}
	clone.del(bck)
	return nil
}

func (p *proxyrunner) httpdaeget(w http.ResponseWriter, r *http.Request) {
	var (
		query = r.URL.Query()
		what  = query.Get(cmn.URLParamWhat)
	)
	switch what {
	case cmn.GetWhatBMD:
		if renamedBucket := query.Get(whatRenamedLB); renamedBucket != "" {
			p.handlePendingRenamedLB(renamedBucket)
		}
		fallthrough // fallthrough
	case cmn.GetWhatConfig, cmn.GetWhatSmapVote, cmn.GetWhatSnode:
		p.httprunner.httpdaeget(w, r)
	case cmn.GetWhatStats:
		ws := p.statsT.GetWhatStats()
		p.writeJSON(w, r, ws, what)
	case cmn.GetWhatSysInfo:
		p.writeJSON(w, r, sys.FetchSysInfo(), what)
	case cmn.GetWhatSmap:
		const max = 16
		var (
			smap  = p.owner.smap.get()
			sleep = cmn.GCO.Get().Timeout.CplaneOperation / 2
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
		msg := &stats.DaemonStatus{
			Snode:       p.httprunner.si,
			SmapVersion: p.owner.smap.get().Version,
			SysInfo:     sys.FetchSysInfo(),
			Stats:       p.statsT.CoreStats(),
		}
		p.writeJSON(w, r, msg, what)
	default:
		p.httprunner.httpdaeget(w, r)
	}
}

func (p *proxyrunner) httpdaeput(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Daemon)
	if err != nil {
		return
	}
	if len(apiItems) > 0 {
		switch apiItems[0] {
		case cmn.Proxy:
			p.httpdaesetprimaryproxy(w, r)
			return
		case cmn.SyncSmap:
			newsmap := &smapX{}
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
			if err := p.owner.smap.synchronize(p.si, newsmap); err != nil {
				p.invalmsghdlrf(w, r, "failed to synch %s: %v", newsmap, err)
				return
			}
			glog.Infof("%s: %s %s done", p.si, cmn.SyncSmap, newsmap)
			return
		case cmn.ActSetConfig: // setconfig #1 - via query parameters and "?n1=v1&n2=v2..."
			var (
				query = r.URL.Query()
				kvs   = cmn.NewSimpleKVsFromQuery(query)
			)
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
			if err := p.attachDetachRemoteAIS(query, apiItems[0]); err != nil {
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
			query = r.URL.Query()
			force = cmn.IsParseBool(query.Get(cmn.URLParamForce))
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
		p.keepalive.send(kaUnregisterMsg)
		return
	default:
		p.invalmsghdlrf(w, r, "unrecognized path: %q in /daemon DELETE", apiItems[0])
		return
	}
}

// register proxy
func (p *proxyrunner) httpdaepost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Daemon)
	if err != nil {
		return
	}

	if len(apiItems) == 0 || apiItems[0] != cmn.UserRegister {
		p.invalmsghdlr(w, r, "unrecognized path in /daemon POST")
		return
	}

	p.keepalive.send(kaRegisterMsg)
	body, err := cmn.ReadBytes(r)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}
	caller := r.Header.Get(cmn.HeaderCallerName)
	if err := p.applyRegMeta(body, caller); err != nil {
		p.invalmsghdlr(w, r, err.Error())
	}
}

func (p *proxyrunner) smapFromURL(baseURL string) (smap *smapX, err error) {
	var (
		req = cmn.ReqArgs{
			Method: http.MethodGet,
			Base:   baseURL,
			Path:   cmn.JoinWords(cmn.Version, cmn.Daemon),
			Query:  url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSmap}},
		}
		args = callArgs{req: req, timeout: cmn.DefaultTimeout, v: &smapX{}}
		res  = p.call(args)
	)
	if res.err != nil {
		return nil, fmt.Errorf("failed to get smap from %s: %v", baseURL, res.err)
	}
	smap = res.v.(*smapX)
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
	primary := newSmap.Primary
	if proxyID != primary.ID() {
		p.invalmsghdlrf(w, r, "%s: proxy %s is not the primary, current %s", p.si, proxyID, newSmap.pp())
		return
	}

	p.metasyncer.becomeNonPrimary() // metasync to stop syncing and cancel all pending requests
	p.owner.smap.put(newSmap)
	res := p.registerToURL(primary.IntraControlNet.DirectURL, primary, cmn.DefaultTimeout, nil, false)
	if res.err != nil {
		p.invalmsghdlr(w, r, res.err.Error())
	}
}

func (p *proxyrunner) httpdaesetprimaryproxy(w http.ResponseWriter, r *http.Request) {
	var (
		prepare bool
		query   = r.URL.Query()
	)
	apiItems, err := p.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Daemon)
	if err != nil {
		return
	}
	proxyID := apiItems[1]
	force := cmn.IsParseBool(query.Get(cmn.URLParamForce))
	// forceful primary change
	if force && apiItems[0] == cmn.Proxy {
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
	ctx := &smapModifier{pre: func(_ *smapModifier, clone *smapX) error { clone.Primary = psi; return nil }}
	err = p.owner.smap.modify(ctx)
	debug.AssertNoErr(err)
}

func (p *proxyrunner) becomeNewPrimary(proxyIDToRemove string) {
	ctx := &smapModifier{
		pre:   p._becomePre,
		final: p._becomeFinal,
		sid:   proxyIDToRemove,
	}
	err := p.owner.smap.modify(ctx)
	cmn.AssertNoErr(err)
}

func (p *proxyrunner) _becomePre(ctx *smapModifier, clone *smapX) error {
	ctx.smap = p.owner.smap.get()
	if !clone.isPresent(p.si) {
		cmn.Assertf(false, "%s must always be present in the %s", p.si, clone.pp())
	}

	if ctx.sid != "" && clone.containsID(ctx.sid) {
		// decision is made: going ahead to remove
		glog.Infof("%s: removing failed primary %s", p.si, ctx.sid)
		clone.delProxy(ctx.sid)
	}

	clone.Primary = clone.GetProxy(p.si.ID())
	clone.Version += 100
	clone.staffIC()
	return nil
}

func (p *proxyrunner) _becomeFinal(ctx *smapModifier, clone *smapX) {
	var (
		bmd   = p.owner.bmd.get()
		rmd   = p.owner.rmd.get()
		msg   = p.newAisMsgStr(cmn.ActNewPrimary, clone, bmd)
		pairs = []revsPair{{clone, msg}, {bmd, msg}, {rmd, msg}}
	)

	glog.Infof("%s: distributing (%s, %s, %s) with newly elected primary (self)", p.si, clone, bmd, rmd)
	_ = p.metasyncer.sync(pairs...)
	p.syncNewICOwners(ctx.smap, clone)
}

func (p *proxyrunner) httpclusetprimaryproxy(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Cluster, cmn.Proxy)
	if err != nil {
		return
	}
	proxyid := apiItems[0]
	s := "designate new primary proxy '" + proxyid + "'"
	if p.forwardCP(w, r, nil, s) {
		return
	}
	smap := p.owner.smap.get()
	psi := smap.GetProxy(proxyid)
	if psi == nil {
		p.invalmsghdlrf(w, r, "new primary proxy %s not present in the %s", proxyid, smap.pp())
		return
	}

	if proxyid == p.si.ID() {
		cmn.Assert(p.si.ID() == smap.Primary.ID()) // must be forwardCP-ed
		glog.Warningf("Request to set primary = %s = self: nothing to do", proxyid)
		return
	}

	if psi.InMaintenance() {
		p.invalmsghdlrf(w, r, "cannot set new primary, node %q under maintenance", psi)
		return
	}

	// (I.1) Prepare phase - inform other nodes.
	urlPath := cmn.JoinWords(cmn.Version, cmn.Daemon, cmn.Proxy, proxyid)
	q := url.Values{}
	q.Set(cmn.URLParamPrepare, "true")
	results := p.callAll(http.MethodPut, urlPath, nil, q)
	for res := range results {
		if res.err != nil {
			p.invalmsghdlrf(w, r, "Failed to set primary %s: err %v from %s in the prepare phase",
				proxyid, res.err, res.si)
			return
		}
	}

	// (I.2) Prepare phase - local changes.
	p.inPrimaryTransition.Store(true)
	defer p.inPrimaryTransition.Store(false)

	err = p.owner.smap.modify(&smapModifier{pre: func(_ *smapModifier, clone *smapX) error {
		clone.Primary = psi
		p.metasyncer.becomeNonPrimary()
		return nil
	}})
	debug.AssertNoErr(err)

	// (II) Commit phase.
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
	if !p.ClusterStarted() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	// TODO: separate permissions for dsort? xactions?
	if err := p.checkPermissions(r.Header, nil, cmn.AccessADMIN); err != nil {
		p.invalmsghdlr(w, r, err.Error(), http.StatusUnauthorized)
		return
	}
	dsort.ProxySortHandler(w, r)
}

// http reverse-proxy handler, to handle unmodified requests
// (not to confuse with p.rproxy)
func (p *proxyrunner) httpCloudHandler(w http.ResponseWriter, r *http.Request) {
	if !p.ClusterStarted() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if r.URL.Scheme == "" {
		p.invalmsghdlr(w, r, "invalid request", http.StatusBadRequest)
		return
	}
	baseURL := r.URL.Scheme + "://" + r.URL.Host
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("[HTTP CLOUD] RevProxy handler for: %s -> %s", baseURL, r.URL.Path)
	}

	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		// bck.IsHTTP()
		hbo := cmn.NewHTTPObj(r.URL)
		q := r.URL.Query()
		q.Set(cmn.URLParamOrigURL, r.URL.String())
		q.Set(cmn.URLParamProvider, cmn.ProviderHTTP)
		r.URL.Path = cmn.JoinWords(cmn.Version, cmn.Objects, hbo.Bck.Name, hbo.ObjName)
		r.URL.RawQuery = q.Encode()
		if r.Method == http.MethodGet {
			p.httpobjget(w, r, hbo.OrigURLBck)
		} else {
			p.httpobjhead(w, r, hbo.OrigURLBck)
		}
		return
	}
	p.invalmsghdlrf(w, r, "%q provider doesn't support %q", cmn.ProviderHTTP, r.Method)
}

///////////////////////////////////
// query target states and stats //
///////////////////////////////////

func (p *proxyrunner) httpcluget(w http.ResponseWriter, r *http.Request) {
	var (
		query = r.URL.Query()
		what  = query.Get(cmn.URLParamWhat)
	)
	switch what {
	case cmn.GetWhatStats:
		p.queryClusterStats(w, r, what)
	case cmn.GetWhatSysInfo:
		p.queryClusterSysinfo(w, r, what)
	case cmn.QueryXactStats:
		p.queryXaction(w, r, what)
	case cmn.GetWhatStatus:
		p.ic.writeStatus(w, r)
	case cmn.GetWhatMountpaths:
		p.queryClusterMountpaths(w, r, what)
	case cmn.GetWhatRemoteAIS:
		config := cmn.GCO.Get()
		smap := p.owner.smap.get()
		si, err := smap.GetRandTarget()
		if err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}

		args := callArgs{
			si: si,
			req: cmn.ReqArgs{
				Method: r.Method,
				Path:   cmn.JoinWords(cmn.Version, cmn.Daemon),
				Query:  query,
			},
			timeout: config.Timeout.CplaneOperation,
		}
		res := p.call(args)
		if res.err != nil {
			p.invalmsghdlr(w, r, res.err.Error())
			return
		}
		// TODO: figure out a way to switch to writeJSON
		p.writeJSONBytes(w, r, res.bytes, what)
	case cmn.GetWhatTargetIPs:
		// Return comma-separated IPs of the targets.
		// It can be used to easily fill the `--noproxy` parameter in cURL.
		var (
			smap = p.owner.smap.Get()
			buf  = bytes.NewBuffer(nil)
		)
		for _, si := range smap.Tmap {
			if buf.Len() > 0 {
				buf.WriteByte(',')
			}
			buf.WriteString(si.PublicNet.NodeIPAddr)
			buf.WriteByte(',')
			buf.WriteString(si.IntraControlNet.NodeIPAddr)
			buf.WriteByte(',')
			buf.WriteString(si.IntraDataNet.NodeIPAddr)
		}
		w.Write(buf.Bytes())
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
		var xactMsg xaction.XactReqMsg
		if err := cmn.ReadJSON(w, r, &xactMsg); err != nil {
			return
		}
		body = cmn.MustMarshal(xactMsg)
	default:
		p.invalmsghdlrstatusf(w, r, http.StatusBadRequest, "invalid `what`: %v", what)
		return
	}
	var (
		results       = p.callTargets(http.MethodGet, cmn.JoinWords(cmn.Version, cmn.Xactions), body, query)
		targetResults = p._queryResults(w, r, results)
	)
	if targetResults != nil {
		p.writeJSON(w, r, targetResults, what)
	}
}

func (p *proxyrunner) queryClusterSysinfo(w http.ResponseWriter, r *http.Request, what string) {
	fetchResults := func(broadcastType int) (cmn.JSONRawMsgs, string) {
		results := p.bcastToGroup(bcastArgs{
			req: cmn.ReqArgs{
				Method: r.Method,
				Path:   cmn.JoinWords(cmn.Version, cmn.Daemon),
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
			sysInfoMap[res.si.ID()] = res.bytes
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
	out.Proxy = p.statsT.CoreStats()
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
	results := p.bcastToGroup(bcastArgs{
		req: cmn.ReqArgs{
			Method: r.Method,
			Path:   cmn.JoinWords(cmn.Version, cmn.Daemon),
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
		targetResults[res.si.ID()] = res.bytes
	}
	return targetResults
}

// Commands: register|keepalive target|proxy
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

	if p.inPrimaryTransition.Load() {
		if apiItems[0] == cmn.Keepalive {
			// Ignore keepalive beat if the primary is in transition.
			return
		}
	}

	if p.forwardCP(w, r, nil, "httpclupost") {
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
	var (
		isProxy = nsi.IsProxy()
		msg     = &cmn.ActionMsg{Action: cmn.ActRegTarget}
	)
	if isProxy {
		msg = &cmn.ActionMsg{Action: cmn.ActRegProxy}

		s := r.URL.Query().Get(cmn.URLParamNonElectable)
		if nonElectable, err = cmn.ParseBool(s); err != nil {
			glog.Errorf("%s: failed to parse %s for non-electability: %v", p.si, s, err)
		}
	}

	if net.ParseIP(nsi.PublicNet.NodeIPAddr) == nil {
		p.invalmsghdlrf(w, r, "%s: failed to %s %s - invalid IPv4 %v", p.si, tag, nsi, nsi.PublicNet.NodeIPAddr)
		return
	}

	p.statsT.Add(stats.PostCount, 1)

	var flags cluster.SnodeFlags
	if nonElectable {
		flags = cluster.SnodeNonElectable
	}

	if userRegister {
		if errCode, err := p.userRegisterNode(nsi, tag); err != nil {
			p.invalmsghdlr(w, r, err.Error(), errCode)
			return
		}
	}

	p.owner.smap.Lock()
	smap, update, err := p.handleJoinKalive(nsi, regReq.Smap, tag, keepalive, flags)
	if !isProxy && p.NodeStarted() {
		// Short for `rebalance.Store(rebalance.Load() || regReq.Reb)`
		p.owner.rmd.rebalance.CAS(false, regReq.Reb)
	}
	p.owner.smap.Unlock()

	if err != nil {
		p.invalmsghdlr(w, r, err.Error())
		return
	}

	if !update {
		return
	}

	// send the current Smap and BMD to self-registering target
	if !isProxy && selfRegister {
		glog.Infof("%s: %s %s (%s)...", p.si, tag, nsi, regReq.Smap)
		bmd := p.owner.bmd.get()
		meta := &nodeRegMeta{smap, bmd, p.si, false}
		p.writeJSON(w, r, meta, path.Join(cmn.ActRegTarget, nsi.ID()) /* tag */)
	}

	// Return the rebalance ID when target is registered by user.
	if !isProxy && userRegister {
		rebID := p.updateAndDistribute(nsi, msg, flags)
		w.Write([]byte(rebID))
		return
	}

	go p.updateAndDistribute(nsi, msg, flags)
}

func (p *proxyrunner) userRegisterNode(nsi *cluster.Snode, tag string) (errCode int, err error) {
	var (
		smap = p.owner.smap.get()
		bmd  = p.owner.bmd.get()
		meta = &nodeRegMeta{smap, bmd, p.si, false}
	)

	//
	// join(node) => cluster via API
	//
	if glog.FastV(3, glog.SmoduleAIS) {
		glog.Infof("%s: %s %s => (%s)", p.si, tag, nsi, smap.StringEx())
	}

	// send the joining node the current BMD and Smap as well
	args := callArgs{
		si: nsi,
		req: cmn.ReqArgs{
			Method: http.MethodPost,
			Path:   cmn.JoinWords(cmn.Version, cmn.Daemon, cmn.UserRegister),
			Body:   cmn.MustMarshal(meta),
		},
		timeout: cmn.GCO.Get().Timeout.CplaneOperation,
	}

	res := p.call(args)
	if res.err == nil {
		return
	}

	if cmn.IsErrConnectionRefused(res.err) {
		err = fmt.Errorf("failed to reach %s at %s:%s",
			nsi, nsi.PublicNet.NodeIPAddr, nsi.PublicNet.DaemonPort)
	} else {
		err = fmt.Errorf("%s: failed to %s %s: %v, %s",
			p.si, tag, nsi, res.err, res.details)
	}
	errCode = res.status
	return
}

// NOTE: under lock
func (p *proxyrunner) handleJoinKalive(nsi *cluster.Snode, regSmap *smapX, tag string,
	keepalive bool, flags cluster.SnodeFlags) (smap *smapX, update bool, err error) {
	debug.AssertMutexLocked(&p.owner.smap.Mutex)
	smap = p.owner.smap.get()
	if !smap.isPrimary(p.si) {
		err = fmt.Errorf("%s is not the primary(%s, %s): cannot %s %s", p.si, smap.Primary, smap, tag, nsi)
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
	}
	// check for cluster integrity errors (cie)
	if err = smap.validateUUID(regSmap, p.si, nsi, ""); err != nil {
		return
	}
	// no further checks join when cluster's starting up
	if !p.ClusterStarted() {
		clone := smap.clone()
		clone.putNode(nsi, flags)
		p.owner.smap.put(clone)
		return
	}
	if _, err = smap.IsDuplicateURL(nsi); err != nil {
		err = errors.New(p.si.String() + ": " + err.Error())
	}
	update = err == nil
	return
}

func (p *proxyrunner) updateAndDistribute(nsi *cluster.Snode, msg *cmn.ActionMsg, flags cluster.SnodeFlags) (xactID string) {
	ctx := &smapModifier{
		pre:   p._updPre,
		post:  p._updPost,
		final: p._updFinal,
		nsi:   nsi,
		msg:   msg,
		flags: flags,
	}
	err := p.owner.smap.modify(ctx)
	if err != nil {
		glog.Errorf("FATAL: %v", err)
		return
	}
	if ctx.rmd != nil {
		return xaction.RebID(ctx.rmd.Version).String()
	}
	return
}

func (p *proxyrunner) _updPre(ctx *smapModifier, clone *smapX) error {
	ctx.smap = p.owner.smap.get()
	if !ctx.smap.isPrimary(p.si) {
		return fmt.Errorf("%s is not primary(%s, %s): cannot add %s", p.si, ctx.smap.Primary, ctx.smap, ctx.nsi)
	}
	ctx.exists = clone.putNode(ctx.nsi, ctx.flags)
	if ctx.nsi.IsTarget() {
		// Notify targets that they need to set up GFN
		aisMsg := p.newAisMsg(&cmn.ActionMsg{Action: cmn.ActStartGFN}, clone, nil)
		notifyPairs := revsPair{clone, aisMsg}
		_ = p.metasyncer.notify(true, notifyPairs)
	}
	clone.staffIC()
	return nil
}

func (p *proxyrunner) _updPost(ctx *smapModifier, clone *smapX) {
	if ctx.nsi.IsTarget() {
		// Trigger rebalance (in case target with given ID already exists
		// we must trigger the rebalance and assume it is a new target
		// with the same ID).
		if ctx.exists || p.requiresRebalance(ctx.smap, clone) {
			rmdCtx := &rmdModifier{
				pre: func(_ *rmdModifier, clone *rebMD) {
					clone.TargetIDs = []string{ctx.nsi.ID()}
					clone.inc()
				},
			}
			rmdClone := p.owner.rmd.modify(rmdCtx)
			ctx.rmd = rmdClone
		}
	}
}

func (p *proxyrunner) _updFinal(ctx *smapModifier, clone *smapX) {
	var (
		tokens = p.authn.revokedTokenList()
		bmd    = p.owner.bmd.get()
		aisMsg = p.newAisMsg(ctx.msg, clone, bmd)
		pairs  = []revsPair{{clone, aisMsg}, {bmd, aisMsg}}
	)

	if ctx.rmd != nil && ctx.nsi.IsTarget() {
		pairs = append(pairs, revsPair{ctx.rmd, aisMsg})
		nl := xaction.NewXactNL(xaction.RebID(ctx.rmd.version()).String(),
			cmn.ActRebalance, &clone.Smap, nil)
		nl.SetOwner(equalIC)
		// Rely on metasync to register rebalanace/resilver `nl` on all IC members.  See `p.receiveRMD`.
		err := p.notifs.add(nl)
		cmn.AssertNoErr(err)
	} else if ctx.nsi.IsProxy() {
		// Send RMD to proxies to make sure that they have
		// the latest one - newly joined can become primary in a second.
		rmd := p.owner.rmd.get()
		pairs = append(pairs, revsPair{rmd, aisMsg})
	}

	if len(tokens.Tokens) > 0 {
		pairs = append(pairs, revsPair{tokens, aisMsg})
	}
	_ = p.metasyncer.sync(pairs...)
	p.syncNewICOwners(ctx.smap, clone)
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
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Cluster, cmn.Daemon)
	if err != nil {
		return
	}
	var (
		msg  *cmn.ActionMsg
		sid  = apiItems[0]
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
	if p.forwardCP(w, r, msg, sid) {
		return
	}
	errCode, err := p.unregisterNode(msg, node, false /*skipReb*/)
	if err != nil {
		p.invalmsghdlr(w, r, err.Error(), errCode)
	}
}

func (p *proxyrunner) _unregNodePre(ctx *smapModifier, clone *smapX) (err error) {
	ctx.smap = p.owner.smap.get()
	sid := ctx.sid
	node := clone.GetNode(sid)
	if node == nil {
		err = &errNodeNotFound{"failed to unregister", sid, p.si, clone}
		ctx.status = http.StatusNotFound
		return err
	}
	if node.IsProxy() {
		clone.delProxy(sid)
		glog.Infof("unregistered %s (num proxies %d)", node, clone.CountProxies())
	} else {
		clone.delTarget(sid)
		glog.Infof("unregistered %s (num targets %d)", node, clone.CountTargets())
	}

	clone.staffIC()

	if !p.NodeStarted() {
		return
	}

	if isPrimary := clone.isPrimary(p.si); !isPrimary {
		ctx.status = http.StatusInternalServerError
		err = fmt.Errorf("%s: is not a primary", p.si)
	}
	return err
}

func (p *proxyrunner) _perfRebPost(ctx *smapModifier, clone *smapX) {
	if !ctx.skipReb && p.requiresRebalance(ctx.smap, clone) {
		rmdCtx := &rmdModifier{
			pre: func(_ *rmdModifier, clone *rebMD) {
				clone.inc()
			},
		}
		rmdClone := p.owner.rmd.modify(rmdCtx)
		ctx.rmd = rmdClone
	}
}

func (p *proxyrunner) _syncFinal(ctx *smapModifier, clone *smapX) {
	var (
		aisMsg = p.newAisMsg(ctx.msg, clone, nil)
		pairs  = []revsPair{{clone, aisMsg}}
	)

	if ctx.rmd != nil {
		nl := xaction.NewXactNL(xaction.RebID(ctx.rmd.version()).String(),
			cmn.ActRebalance, &clone.Smap, nil)
		nl.SetOwner(equalIC)
		// Rely on metasync to register rebalanace/resilver `nl` on all IC members.  See `p.receiveRMD`.
		err := p.notifs.add(nl)
		cmn.AssertNoErr(err)
		pairs = append(pairs, revsPair{ctx.rmd, aisMsg})
	}

	_ = p.metasyncer.sync(pairs...)
}

// '{"action": "shutdown"}' /v1/cluster => (proxy) =>
// '{"action": "syncsmap"}' /v1/cluster => (proxy) => PUT '{Smap}' /v1/daemon/syncsmap => target(s)
// '{"action": cmn.ActXactStart}' /v1/cluster
// '{"action": cmn.ActXactStop}' /v1/cluster
// '{"action": cmn.ActSendOwnershipTbl}' /v1/cluster
// '{"action": cmn.ActRebalance}' /v1/cluster => (proxy) => PUT '{Smap}' /v1/daemon/rebalance => target(s)
// '{"action": "setconfig"}' /v1/cluster => (proxy) =>
func (p *proxyrunner) httpcluput(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Cluster)
	if err != nil {
		return
	}
	if len(apiItems) == 0 {
		p.cluputJSON(w, r)
	} else {
		p.cluputQuery(w, r, apiItems[0])
	}
}

func (p *proxyrunner) unregisterNode(msg *cmn.ActionMsg, si *cluster.Snode, skipReb bool) (errCode int, err error) {
	smap := p.owner.smap.get()
	node := smap.GetNode(si.ID())
	if node == nil {
		err = &errNodeNotFound{"cannot unregister", si.ID(), p.si, smap}
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
				Path:   cmn.JoinWords(cmn.Version, cmn.Daemon, cmn.Unregister),
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
	ctx := &smapModifier{
		pre:     p._unregNodePre,
		post:    p._perfRebPost,
		final:   p._syncFinal,
		msg:     msg,
		sid:     si.ID(),
		skipReb: skipReb,
	}
	err = p.owner.smap.modify(ctx)
	return ctx.status, err
}

// Put a node under maintenance
func (p *proxyrunner) markMaintenance(msg *cmn.ActionMsg, si *cluster.Snode) error {
	var flags cluster.SnodeFlags
	switch msg.Action {
	case cmn.ActDecommission:
		flags = cluster.SnodeDecomission
	case cmn.ActStartMaintenance:
		flags = cluster.SnodeMaintenance
	default:
		return fmt.Errorf("invalid action: %s", msg.Action)
	}
	ctx := &smapModifier{
		pre:   p._markMaint,
		final: p._syncFinal,
		sid:   si.ID(),
		flags: flags,
		msg:   msg,
	}
	return p.owner.smap.modify(ctx)
}

func (p *proxyrunner) _markMaint(ctx *smapModifier, clone *smapX) error {
	clone.setNodeFlags(ctx.sid, ctx.flags)
	clone.staffIC()
	return nil
}

// Callback: remove the node from the cluster if rebalance finished successfully
func (p *proxyrunner) removeAfterRebalance(nl nl.NotifListener, msg *cmn.ActionMsg, si *cluster.Snode) {
	if err := nl.Err(false); err != nil || nl.Aborted() {
		glog.Errorf("Rebalance(%s) didn't finish successfully, err: %v, aborted: %v", nl.UUID(), err, nl.Aborted())
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("Rebalance(%s) finished. Removing node %s", nl.UUID(), si)
	}
	if _, err := p.unregisterNode(msg, si, true /*skipReb*/); err != nil {
		glog.Errorf("Failed to remove node (%s) after rebalance, err: %v", si, err)
	}
}

// Run rebalance and call a callback after the rebalance finishes
func (p *proxyrunner) finalizeMaintenance(msg *cmn.ActionMsg, si *cluster.Snode) (rebID xaction.RebID, err error) {
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("put %s under maintenance and rebalance: action %s", si, msg.Action)
	}

	if p.owner.smap.get().CountActiveTargets() == 0 {
		return
	}

	if err = p.canStartRebalance(true /*skip config*/); err == nil {
		goto updateRMD
	}

	if _, ok := err.(*errNotEnoughTargets); !ok {
		return
	}
	if msg.Action == cmn.ActDecommission {
		_, err = p.unregisterNode(msg, si, true /*skipReb*/)
		return
	}
	err = nil
	return

updateRMD:
	var cb nl.NotifCallback
	if msg.Action == cmn.ActDecommission {
		cb = func(nl nl.NotifListener) { p.removeAfterRebalance(nl, msg, si) }
	}
	rmdCtx := &rmdModifier{
		pre: func(_ *rmdModifier, clone *rebMD) {
			clone.inc()
		},
		final: p._syncRMDFinal,
		smap:  p.owner.smap.get(),
		msg:   msg,
		rebCB: cb,
		wait:  true,
	}
	rmdClone := p.owner.rmd.modify(rmdCtx)
	rebID = xaction.RebID(rmdClone.version())
	return
}

// Stops rebalance if needed, do cleanup, and get a node back to the cluster.
func (p *proxyrunner) cancelMaintenance(msg *cmn.ActionMsg, opts *cmn.ActValDecommision) (rebID xaction.RebID, err error) {
	ctx := &smapModifier{
		pre:     p._cancelMaintPre,
		post:    p._perfRebPost,
		final:   p._syncFinal,
		sid:     opts.DaemonID,
		skipReb: opts.SkipRebalance,
		msg:     msg,
		flags:   cluster.SnodeMaintenanceMask,
	}
	err = p.owner.smap.modify(ctx)
	if ctx.rmd != nil {
		rebID = xaction.RebID(ctx.rmd.Version)
	}
	return
}

func (p *proxyrunner) _cancelMaintPre(ctx *smapModifier, clone *smapX) error {
	ctx.smap = p.owner.smap.get()
	clone.clearNodeFlags(ctx.sid, ctx.flags)
	clone.staffIC()
	return nil
}

func (p *proxyrunner) cluputJSON(w http.ResponseWriter, r *http.Request) {
	var (
		err error
		msg = &cmn.ActionMsg{}
	)
	if cmn.ReadJSON(w, r, msg) != nil {
		return
	}
	if msg.Action != cmn.ActSendOwnershipTbl {
		if p.forwardCP(w, r, msg, "") {
			return
		}
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

		// same message -> all targets and proxies
		results := p.callAll(http.MethodPut, cmn.JoinWords(cmn.Version, cmn.Daemon), cmn.MustMarshal(msg))
		for res := range results {
			if res.err != nil {
				p.invalmsghdlrf(w, r, "%s: (%s = %s) failed, err: %s",
					msg.Action, msg.Name, value, res.details)
				p.keepalive.onerr(err, res.status)
				return
			}
		}
	case cmn.ActShutdown:
		glog.Infoln("Proxy-controlled cluster shutdown...")
		p.callAll(http.MethodPut, cmn.JoinWords(cmn.Version, cmn.Daemon), cmn.MustMarshal(msg))
		time.Sleep(time.Second)
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	case cmn.ActXactStart, cmn.ActXactStop:
		xactMsg := xaction.XactReqMsg{}
		if err := cmn.MorphMarshal(msg.Value, &xactMsg); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		if msg.Action == cmn.ActXactStart && xactMsg.Kind == cmn.ActRebalance {
			if err := p.canStartRebalance(true /*skip config*/); err != nil {
				p.invalmsghdlr(w, r, err.Error())
				return
			}
			rmdCtx := &rmdModifier{
				pre: func(_ *rmdModifier, clone *rebMD) {
					clone.inc()
				},
				final: p._syncRMDFinal,
				msg:   &cmn.ActionMsg{Action: cmn.ActRebalance},
				smap:  p.owner.smap.get(),
			}
			rmdClone := p.owner.rmd.modify(rmdCtx)
			w.Write([]byte(xaction.RebID(rmdClone.version()).String()))
			return
		}

		if msg.Action == cmn.ActXactStart {
			xactMsg.ID = cmn.GenUUID()
		}
		var (
			body    = cmn.MustMarshal(cmn.ActionMsg{Action: msg.Action, Value: xactMsg})
			results = p.callTargets(http.MethodPut, cmn.JoinWords(cmn.Version, cmn.Xactions), body)
		)
		for res := range results {
			if res.err != nil {
				p.invalmsghdlr(w, r, res.err.Error())
				return
			}
		}
		if msg.Action == cmn.ActXactStart {
			smap := p.owner.smap.get()
			nl := xaction.NewXactNL(xactMsg.ID, xactMsg.Kind, &smap.Smap, nil)
			p.ic.registerEqual(regIC{smap: smap, nl: nl})
			w.Write([]byte(xactMsg.ID))
		}
	case cmn.ActSendOwnershipTbl:
		var (
			smap  = p.owner.smap.get()
			dstID string
		)
		if err := cmn.MorphMarshal(msg.Value, &dstID); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		dst := smap.GetProxy(dstID)
		if dst == nil {
			p.invalmsghdlrf(w, r, "%s: unknown proxy node - p[%s]", p.si, dstID)
			return
		}

		if !smap.IsIC(dst) {
			p.invalmsghdlrf(w, r, "%s: not an IC member", dst)
			return
		}

		if smap.IsIC(p.si) && !p.si.Equals(dst) {
			// node has older version than dst node handle locally
			if err := p.ic.sendOwnershipTbl(dst); err != nil {
				p.invalmsghdlr(w, r, err.Error())
			}
			return
		}

		for pid, psi := range smap.Pmap {
			if !psi.IsIC() || pid == dstID {
				continue
			}
			result := p.call(
				callArgs{
					si: psi,
					req: cmn.ReqArgs{
						Method: http.MethodPut,
						Path:   cmn.JoinWords(cmn.Version, cmn.Cluster),
						Body:   cmn.MustMarshal(msg),
					}, timeout: cmn.LongTimeout,
				},
			)
			if result.err != nil {
				p.invalmsghdlr(w, r, result.err.Error())
			}
			break
		}
	case cmn.ActStartMaintenance, cmn.ActDecommission:
		var (
			rebID xaction.RebID
			smap  = p.owner.smap.get()
			opts  cmn.ActValDecommision
		)
		if err = cmn.MorphMarshal(msg.Value, &opts); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		si := smap.GetNode(opts.DaemonID)
		if si == nil {
			p.invalmsghdlrstatusf(w, r, http.StatusNotFound, "Node %q %s", opts.DaemonID, cmn.DoesNotExist)
			return
		}
		if si.InMaintenance() {
			p.invalmsghdlrf(w, r, "Node %q already in maintenance state", opts.DaemonID)
			return
		}

		if p.si.Equals(si) {
			p.invalmsghdlrf(w, r, "Node %q is primary, cannot perform %s", opts.DaemonID, msg.Action)
			return
		}

		if rebID, err = p.startMaintenance(si, msg, &opts); err != nil {
			p.invalmsghdlrf(w, r, "Failed to %s node %s: %v", msg.Action, opts.DaemonID, err)
			return
		}
		if rebID != 0 {
			w.Write([]byte(rebID.String()))
		}
	case cmn.ActStopMaintenance:
		var (
			opts cmn.ActValDecommision
			smap = p.owner.smap.get()
		)
		if err = cmn.MorphMarshal(msg.Value, &opts); err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		si := smap.GetNode(opts.DaemonID)
		if si == nil {
			p.invalmsghdlrstatusf(w, r, http.StatusNotFound, "Node %q %s", opts.DaemonID, cmn.DoesNotExist)
			return
		}
		if !si.InMaintenance() {
			p.invalmsghdlrf(w, r, "Node %q is not under maintenance", opts.DaemonID)
			return
		}

		rebID, err := p.cancelMaintenance(msg, &opts)
		if err != nil {
			p.invalmsghdlr(w, r, err.Error())
			return
		}
		if rebID != 0 {
			w.Write([]byte(rebID.String()))
		}
	default:
		p.invalmsghdlrf(w, r, fmtUnknownAct, msg)
	}
}

func (p *proxyrunner) _syncRMDFinal(ctx *rmdModifier, clone *rebMD) {
	wg := p.metasyncer.sync(revsPair{clone, p.newAisMsg(ctx.msg, nil, nil)})
	nl := xaction.NewXactNL(xaction.RebID(clone.Version).String(),
		cmn.ActRebalance, &ctx.smap.Smap, nil)
	nl.SetOwner(equalIC)
	if ctx.rebCB != nil {
		nl.F = ctx.rebCB
	}
	// Rely on metasync to register rebalanace/resilver `nl` on all IC members.  See `p.receiveRMD`.
	err := p.notifs.add(nl)
	cmn.AssertNoErr(err)
	if ctx.wait {
		wg.Wait()
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
		results := p.callAll(http.MethodPut,
			cmn.JoinWords(cmn.Version, cmn.Daemon, cmn.ActSetConfig), nil, query)
		for res := range results {
			if res.err != nil {
				p.invalmsghdlr(w, r, res.err.Error())
				p.keepalive.onerr(err, res.status)
				return
			}
		}
	case cmn.ActAttach, cmn.ActDetach:
		what := query.Get(cmn.URLParamWhat)
		if what != cmn.GetWhatRemoteAIS {
			cmn.InvalidHandlerWithMsg(w, r, fmt.Sprintf(fmtUnknownQue, what))
			return
		}
		if err := p.attachDetachRemoteAIS(query, action); err != nil {
			cmn.InvalidHandlerWithMsg(w, r, err.Error())
			return
		}
		results := p.callAll(http.MethodPut, cmn.JoinWords(cmn.Version, cmn.Daemon, action), nil, query)
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
			err = newErrDowngrade(p.si, rmd.String(), newRMD.String())
		}
		return
	}
	p.owner.rmd.put(newRMD)
	p.owner.rmd.Unlock()

	// Register `nl` for rebalance is metasynced.
	smap := p.owner.smap.get()
	if smap.IsIC(p.si) && smap.CountActiveTargets() > 0 {
		nl := xaction.NewXactNL(xaction.RebID(newRMD.Version).String(),
			cmn.ActRebalance, &smap.Smap, nil)
		nl.SetOwner(equalIC)
		err := p.notifs.add(nl)
		cmn.AssertNoErr(err)

		if newRMD.Resilver != "" {
			nl = xaction.NewXactNL(newRMD.Resilver, cmn.ActResilver, &smap.Smap, nil)
			nl.SetOwner(equalIC)
			err := p.notifs.add(nl)
			cmn.AssertNoErr(err)
		}
	}
	return
}

func (p *proxyrunner) _syncBMDFinal(ctx *bmdModifier, clone *bucketMD) {
	msg := p.newAisMsg(ctx.msg, ctx.smap, clone, ctx.txnID)
	wg := p.metasyncer.sync(revsPair{clone, msg})
	if ctx.wait {
		wg.Wait()
	}
}

func (p *proxyrunner) receiveBMD(newBMD *bucketMD, caller string) (err error) {
	p.owner.bmd.Lock()
	bmd := p.owner.bmd.get()
	if err = bmd.validateUUID(newBMD, p.si, nil, caller); err != nil {
		cmn.Assert(!p.owner.smap.get().isPrimary(p.si))
		// cluster integrity error: making exception for non-primary proxies
		glog.Errorf("%s (non-primary): %v - proceeding to override BMD", p.si, err)
	} else if newBMD.version() <= bmd.version() {
		p.owner.bmd.Unlock()
		return newErrDowngrade(p.si, bmd.String(), newBMD.String())
	}
	p.owner.bmd.put(newBMD)
	p.owner.bmd.Unlock()
	return
}

// detectDaemonDuplicate queries osi for its daemon info in order to determine if info has changed
// and is equal to nsi
func (p *proxyrunner) detectDaemonDuplicate(osi, nsi *cluster.Snode) bool {
	var (
		args = callArgs{
			si: osi,
			req: cmn.ReqArgs{
				Method: http.MethodGet,
				Path:   cmn.JoinWords(cmn.Version, cmn.Daemon),
				Query:  url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSnode}},
			},
			timeout: cmn.GCO.Get().Timeout.CplaneOperation,
			v:       &cluster.Snode{},
		}
		res = p.call(args)
	)
	if res.err != nil {
		return false
	}
	si := res.v.(*cluster.Snode)
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
		force, slowp bool
	)
	if p.forwardCP(w, r, msg, "recover-buckets") {
		return
	}
	var (
		path    = cmn.JoinWords(cmn.Version, cmn.Buckets, cmn.AllBuckets)
		results = p.bcastToGroup(bcastArgs{
			req:     cmn.ReqArgs{Method: http.MethodGet, Path: path},
			timeout: cmn.GCO.Get().Timeout.MaxKeepalive,
			fv:      func() interface{} { return &bucketMD{} },
		})
		bmds = make(map[*cluster.Snode]*bucketMD, len(results))
	)
	for res := range results {
		if res.err != nil {
			glog.Errorf("%v (%d: %s)", res.err, res.status, res.details)
			continue
		}
		bmd := res.v.(*bucketMD)
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

func (p *proxyrunner) canStartRebalance(skipConfigCheck ...bool) error {
	const minTargetCnt = 2
	var skipCfg bool
	if len(skipConfigCheck) != 0 {
		skipCfg = skipConfigCheck[0]
	}
	cfg := cmn.GCO.Get().Rebalance
	if !skipCfg && !cfg.Enabled {
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
	smap := p.owner.smap.get()
	if smap.CountActiveTargets() < minTargetCnt {
		return &errNotEnoughTargets{p.si, smap, minTargetCnt}
	}
	return nil
}

func (p *proxyrunner) requiresRebalance(prev, cur *smapX) bool {
	if err := p.canStartRebalance(); err != nil {
		return false
	}

	if cur.CountActiveTargets() == 0 {
		return false
	}

	if cur.CountActiveTargets() > prev.CountActiveTargets() {
		return true
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

func (p *proxyrunner) headCloudBck(bck cmn.Bck, q url.Values) (header http.Header, statusCode int, err error) {
	var (
		tsi   *cluster.Snode
		pname = p.si.String()
		path  = cmn.JoinWords(cmn.Version, cmn.Buckets, bck.Name)
	)
	if tsi, err = p.owner.smap.get().GetRandTarget(); err != nil {
		return
	}
	q = cmn.AddBckToQuery(q, bck)

	req := cmn.ReqArgs{Method: http.MethodHead, Base: tsi.URL(cmn.NetworkIntraData), Path: path, Query: q}
	res := p.call(callArgs{si: tsi, req: req, timeout: cmn.DefaultTimeout})
	if res.status == http.StatusNotFound {
		err = cmn.NewErrorRemoteBucketDoesNotExist(bck, pname)
	} else if res.status == http.StatusGone {
		err = cmn.NewErrorCloudBucketOffline(bck, pname)
	} else if res.err != nil {
		err = fmt.Errorf("%s: %s, target %s: %s", pname, bck, tsi, http.StatusText(res.status))
	} else {
		header = res.header
	}
	statusCode = res.status
	return
}

//////////////////
// reverseProxy //
//////////////////

func (rp *reverseProxy) init() {
	cfg := cmn.GCO.Get()
	rp.cloud = &httputil.ReverseProxy{
		Director: func(r *http.Request) {},
		Transport: cmn.NewTransport(cmn.TransportArgs{
			UseHTTPS:   cfg.Net.HTTP.UseHTTPS,
			SkipVerify: cfg.Net.HTTP.SkipVerify,
		}),
	}
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
		mlist[bmd.UUID] = append(mlist[bmd.UUID], nodeRegMeta{nil, bmd, si, false})

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
	uuid, l := "", 0
	for u, lst := range mlist {
		if l < len(lst) {
			uuid, l = u, len(lst)
		}
	}
	for u, lst := range mlist {
		if l == len(lst) && u != uuid {
			s := fmt.Sprintf("%s: BMDs have different uuids with no simple majority:\n%v",
				ciError(60), mlist)
			return nil, &errBmdUUIDSplit{s}
		}
	}
	var err error
	if len(mlist) > 1 {
		s := fmt.Sprintf("%s: BMDs have different uuids with simple majority: %s:\n%v",
			ciError(70), uuid, mlist)
		err = &errTgtBmdUUIDDiffer{s}
	}
	bmd := maxor[uuid]
	cmn.Assert(bmd.UUID != "")
	return bmd, err
}

func ciError(num int) string {
	const s = "[%s%d - for details, see %s/blob/master/docs/troubleshooting.md]"
	return fmt.Sprintf(s, ciePrefix, num, cmn.GithubHome)
}
