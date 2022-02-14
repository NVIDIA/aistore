// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2021, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"bytes"
	"errors"
	"fmt"
	"io"
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
	"github.com/NVIDIA/aistore/authn"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
	jsoniter "github.com/json-iterator/go"
)

const (
	whatRenamedLB = "renamedlb"
	ciePrefix     = "cluster integrity error cie#"
	listBuckets   = "listBuckets"
)

const (
	fmtNotRemote  = "%q appears to be ais bucket (expecting remote)"
	fmtUnknownQue = "unexpected query [what=%s]"
)

type (
	ClusterMountpathsRaw struct {
		Targets cos.JSONRawMsgs `json:"targets"`
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

	singleRProxy struct {
		rp *httputil.ReverseProxy
		u  *url.URL
	}

	// proxy runner
	proxy struct {
		htrun
		authn      *authManager
		metasyncer *metasyncer
		rproxy     reverseProxy
		notifs     notifs
		ic         ic
		reg        struct {
			mtx  sync.RWMutex
			pool nodeRegPool
		}
		qm lsobjMem
	}
)

// interface guard
var (
	_ cos.Runner = (*proxy)(nil)
	_ electable  = (*proxy)(nil)
)

func (p *proxy) initClusterCIDR() {
	if nodeCIDR := os.Getenv("AIS_CLUSTER_CIDR"); nodeCIDR != "" {
		_, network, err := net.ParseCIDR(nodeCIDR)
		p.si.LocalNet = network
		cos.AssertNoErr(err)
		glog.Infof("local network: %+v", *network)
	}
}

func (p *proxy) init(config *cmn.Config) {
	p.initNetworks()
	p.si.Init(initPID(config), cmn.Proxy)

	memsys.Init(p.si.ID(), p.si.ID())

	cos.InitShortID(p.si.Digest())

	p.initClusterCIDR()
	daemon.rg.add(p)

	ps := &stats.Prunner{}
	startedUp := ps.Init(p)
	daemon.rg.add(ps)
	p.statsT = ps

	k := newProxyKeepalive(p, ps, startedUp)
	daemon.rg.add(k)
	p.keepalive = k

	m := newMetasyncer(p)
	daemon.rg.add(m)
	p.metasyncer = m
}

func initPID(config *cmn.Config) (pid string) {
	if pid = envDaemonID(cmn.Proxy); pid != "" {
		if err := cos.ValidateDaemonID(pid); err != nil {
			glog.Errorf("Warning: %v", err)
		}
		return
	}
	// try to read ID
	if pid = readProxyID(config); pid != "" {
		glog.Infof("p[%s] from %q", pid, cmn.ProxyIDFname)
		return
	}
	pid = genDaemonID(cmn.Proxy, config)
	err := cos.ValidateDaemonID(pid)
	debug.AssertNoErr(err)

	// store ID on disk
	err = os.WriteFile(filepath.Join(config.ConfigDir, cmn.ProxyIDFname), []byte(pid), cos.PermRWR)
	debug.AssertNoErr(err)
	glog.Infof("p[%s] ID randomly generated", pid)
	return
}

func readProxyID(config *cmn.Config) (id string) {
	if b, err := os.ReadFile(filepath.Join(config.ConfigDir, cmn.ProxyIDFname)); err == nil {
		id = string(b)
	} else if !os.IsNotExist(err) {
		glog.Error(err)
	}
	return
}

// start proxy runner
func (p *proxy) Run() error {
	config := cmn.GCO.Get()
	p.htrun.init(config)
	p.htrun.electable = p
	p.owner.bmd = newBMDOwnerPrx(config)
	p.owner.etl = newEtlMDOwnerPrx(config)

	p.owner.bmd.init() // initialize owner and load BMD
	p.owner.etl.init() // initialize owner and load EtlMD

	cluster.Init(nil /*cluster.Target*/)

	p.statsT.RegMetrics(p.si) // reg target metrics to common; init Prometheus if used

	// startup sequence - see earlystart.go for the steps and commentary
	p.bootstrap()

	p.authn = &authManager{
		tokens:        make(map[string]*authn.Token),
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
		{r: cmn.Reverse, h: p.reverseHandler, net: accessNetPublic},

		// pubnet handlers: cluster must be started
		{r: cmn.Buckets, h: p.bucketHandler, net: accessNetPublic},
		{r: cmn.Objects, h: p.objectHandler, net: accessNetPublic},
		{r: cmn.Download, h: p.downloadHandler, net: accessNetPublic},
		{r: cmn.ETL, h: p.etlHandler, net: accessNetPublic},
		{r: cmn.Sort, h: p.dsortHandler, net: accessNetPublic},

		{r: cmn.IC, h: p.ic.handler, net: accessNetIntraControl},
		{r: cmn.Daemon, h: p.daemonHandler, net: accessNetPublicControl},
		{r: cmn.Cluster, h: p.clusterHandler, net: accessNetPublicControl},
		{r: cmn.Tokens, h: p.tokenHandler, net: accessNetPublic},

		{r: cmn.Metasync, h: p.metasyncHandler, net: accessNetIntraControl},
		{r: cmn.Health, h: p.healthHandler, net: accessNetPublicControl},
		{r: cmn.Vote, h: p.voteHandler, net: accessNetIntraControl},

		{r: cmn.Notifs, h: p.notifs.handler, net: accessNetIntraControl},

		{r: "/", h: p.httpCloudHandler, net: accessNetPublic},

		// S3 compatibility
		{r: "/" + cmn.S3, h: p.s3Handler, net: accessNetPublic},

		// "easy URL"
		{r: "/" + cmn.GSScheme, h: p.easyURLHandler, net: accessNetPublic},
		{r: "/" + cmn.AZScheme, h: p.easyURLHandler, net: accessNetPublic},
		{r: "/" + cmn.AISScheme, h: p.easyURLHandler, net: accessNetPublic},
	}
	p.registerNetworkHandlers(networkHandlers)

	glog.Infof("%s: [%s net] listening on: %s", p.si, cmn.NetPublic, p.si.PublicNet.DirectURL)
	if p.si.PublicNet.DirectURL != p.si.IntraControlNet.DirectURL {
		glog.Infof("%s: [%s net] listening on: %s", p.si, cmn.NetIntraControl, p.si.IntraControlNet.DirectURL)
	}
	if p.si.PublicNet.DirectURL != p.si.IntraDataNet.DirectURL {
		glog.Infof("%s: [%s net] listening on: %s", p.si, cmn.NetIntraData, p.si.IntraDataNet.DirectURL)
	}

	dsort.RegisterNode(p.owner.smap, p.owner.bmd, p.si, nil, p.statsT)
	return p.htrun.run()
}

func (p *proxy) sendKeepalive(timeout time.Duration) (status int, err error) {
	smap := p.owner.smap.get()
	if smap != nil && smap.isPrimary(p.si) {
		return
	}
	return p.htrun.sendKeepalive(timeout)
}

func (p *proxy) joinCluster(action string, primaryURLs ...string) (status int, err error) {
	var query url.Values
	if smap := p.owner.smap.get(); smap.isPrimary(p.si) {
		return 0, fmt.Errorf("%s should not be joining: is primary, %s", p.si, smap.StringEx())
	}
	if cmn.GCO.Get().Proxy.NonElectable {
		query = url.Values{cmn.URLParamNonElectable: []string{"true"}}
	}
	res := p.join(query, primaryURLs...)
	defer freeCR(res)
	if res.err != nil {
		status, err = res.status, res.err
		return
	}
	// not being sent at cluster startup and keepalive
	if len(res.bytes) == 0 {
		return
	}
	err = p.applyRegMeta(action, res.bytes, "")
	return
}

func (p *proxy) applyRegMeta(action string, body []byte, caller string) error {
	var regMeta cluMeta
	if err := jsoniter.Unmarshal(body, &regMeta); err != nil {
		return fmt.Errorf(cmn.FmtErrUnmarshal, p.si, "reg-meta", cmn.BytesHead(body), err)
	}
	return p.receiveCluMeta(&regMeta, action, caller)
}

func (p *proxy) receiveCluMeta(cluMeta *cluMeta, action, caller string) (err error) {
	msg := p.newAmsgStr(action, cluMeta.BMD)

	// Config
	debug.Assert(cluMeta.Config != nil)
	if err = p.receiveConfig(cluMeta.Config, msg, nil, caller); err != nil {
		if isErrDowngrade(err) {
			err = nil
		} else {
			glog.Error(err)
		}
		// Received outdated/invalid config in cluMeta, ignore by setting to `nil`.
		cluMeta.Config = nil
		// fall through
	}
	// Smap
	if err = p.receiveSmap(cluMeta.Smap, msg, nil /*ms payload*/, caller, p.smapOnUpdate); err != nil {
		if !isErrDowngrade(err) {
			glog.Errorf(cmn.FmtErrFailed, p.si, "sync", cluMeta.Smap, err)
		}
	} else {
		glog.Infof("%s: synch %s", p.si, cluMeta.Smap)
	}
	// BMD
	if err = p.receiveBMD(cluMeta.BMD, msg, nil, caller); err != nil {
		if !isErrDowngrade(err) {
			glog.Errorf(cmn.FmtErrFailed, p.si, "sync", cluMeta.BMD, err)
		}
	} else {
		glog.Infof("%s: synch %s", p.si, cluMeta.BMD)
	}
	// RMD
	if err = p.receiveRMD(cluMeta.RMD, msg, caller); err != nil {
		if !isErrDowngrade(err) {
			glog.Errorf(cmn.FmtErrFailed, p.si, "sync", cluMeta.RMD, err)
		}
	} else {
		glog.Infof("%s: synch %s", p.si, cluMeta.RMD)
	}
	// EtlMD
	if err = p.receiveEtlMD(cluMeta.EtlMD, msg, nil, caller, nil); err != nil {
		if !isErrDowngrade(err) {
			glog.Errorf(cmn.FmtErrFailed, p.si, "sync", cluMeta.EtlMD, err)
		}
	} else {
		glog.Infof("%s: synch %s", p.si, cluMeta.EtlMD)
	}
	return
}

// stop proxy runner and return => rungroup.run
func (p *proxy) Stop(err error) {
	var (
		s         string
		smap      = p.owner.smap.get()
		isPrimary = smap.isPrimary(p.si)
		f         = glog.Infof
	)
	if isPrimary {
		s = " (primary)"
	}
	if err != nil {
		f = glog.Warningf
	}
	f("Stopping %s%s, err: %v", p.si, s, err)
	xreg.AbortAll(errors.New("p-stop"))
	p.htrun.stop(!isPrimary && smap.isValid() && !isErrNoUnregister(err) /* rm from Smap*/)
}

////////////////////////////////////////
// http /bucket and /objects handlers //
////////////////////////////////////////

// parse request + init/lookup bucket (combo)
func (p *proxy) _parseReqTry(w http.ResponseWriter, r *http.Request, bckArgs *bckInitArgs) (bck *cluster.Bck, objName string, err error) {
	apireq := apiReqAlloc(2, cmn.URLPathObjects.L, false)
	if err = p.parseReq(w, r, apireq); err != nil {
		apiReqFree(apireq)
		return
	}
	bckArgs.bck, bckArgs.query = apireq.bck, apireq.query
	// both ais package caller  _and_ remote user (via `cmn.URLParamDontLookupRemoteBck`)
	bckArgs.lookupRemote = bckArgs.lookupRemote && lookupRemoteBck(apireq.query, apireq.dpq)

	bck, err = bckArgs.initAndTry(apireq.bck.Name)
	objName = apireq.items[1]

	apiReqFree(apireq)
	freeInitBckArgs(bckArgs) // caller does alloc
	return
}

// verb /v1/buckets/
func (p *proxy) bucketHandler(w http.ResponseWriter, r *http.Request) {
	if !p.ClusterStartedWithRetry() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	switch r.Method {
	case http.MethodGet:
		p.httpbckget(w, r)
	case http.MethodDelete:
		p.httpbckdelete(w, r)
	case http.MethodPut:
		p.httpbckput(w, r)
	case http.MethodPost:
		p.httpbckpost(w, r)
	case http.MethodHead:
		p.httpbckhead(w, r)
	case http.MethodPatch:
		p.httpbckpatch(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodHead,
			http.MethodPatch, http.MethodPost)
	}
}

// verb /v1/objects/
func (p *proxy) objectHandler(w http.ResponseWriter, r *http.Request) {
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
	case http.MethodPatch:
		p.httpobjpatch(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodHead,
			http.MethodPost, http.MethodPut)
	}
}

// "Easy URL" (feature) is a simple alternative mapping of the AIS API to handle
// URLs paths that look as follows:
//
// 	/gs/mybucket/myobject   - to access Google Cloud buckets
// 	/az/mybucket/myobject   - Azure Blob Storage
// 	/ais/mybucket/myobject  - AIS
//
// In other words, easy URL is a convenience feature that allows reading, writing,
// deleting, and listing objects as follows:
//
// # Example: GET
// $ curl -L -X GET 'http://aistore/gs/my-google-bucket/abc-train-0001.tar'
// # Example: PUT
// $ curl -L -X PUT 'http://aistore/gs/my-google-bucket/abc-train-9999.tar -T /tmp/9999.tar'
// # Example: LIST
// $ curl -L -X GET 'http://aistore/gs/my-google-bucket'
//
// NOTE:
//      Amazon S3 is missing in the list that includes GCP and Azure. The reason
//      for this is that AIS provides S3 compatibility layer via its "/s3" endpoint.
//      S3 compatibility (see https://github.com/NVIDIA/aistore/blob/master/docs/s3compat.md)
//      shall not be confused with a simple alternative URL Path mapping via easyURLHandler,
//      whereby a path (e.g.) "gs/mybucket/myobject" gets replaced with
//      "v1/objects/mybucket/myobject?provider=gcp" with _no_ other changes to the request
//      and response parameters and components.
//
func (p *proxy) easyURLHandler(w http.ResponseWriter, r *http.Request) {
	var provider, bucket, objName string
	apiItems, err := p.checkRESTItems(w, r, 2, true, nil)
	if err != nil {
		return
	}
	provider, bucket = apiItems[0], apiItems[1]
	if provider, err = cmn.NormalizeProvider(provider); err != nil {
		p.writeErr(w, r, err)
		return
	}
	bck := cmn.Bck{Name: bucket, Provider: provider}
	if err := bck.ValidateName(); err != nil {
		p.writeErr(w, r, err)
		return
	}
	if len(apiItems) > 2 {
		objName = apiItems[2]
		r.URL.Path = "/" + path.Join(cmn.Version, cmn.Objects, bucket, objName)
		r.URL.Path += path.Join(apiItems[3:]...)
	} else {
		r.URL.Path = "/" + path.Join(cmn.Version, cmn.Buckets, bucket)
	}
	if r.URL.RawQuery == "" {
		query := cmn.AddBckToQuery(nil, bck)
		r.URL.RawQuery = query.Encode()
	} else if !strings.Contains(r.URL.RawQuery, cmn.URLParamProvider) {
		r.URL.RawQuery += "&" + cmn.URLParamProvider + "=" + bck.Provider
	}
	// and finally
	if objName != "" {
		p.objectHandler(w, r)
	} else {
		p.bucketHandler(w, r)
	}
}

// GET /v1/buckets[/bucket-name]
func (p *proxy) httpbckget(w http.ResponseWriter, r *http.Request) {
	var (
		msg       *cmn.ActionMsg
		bckName   string
		queryBcks cmn.QueryBcks
	)
	apiItems, err := p.checkRESTItems(w, r, 0, true, cmn.URLPathBuckets.L)
	if err != nil {
		return
	}
	if len(apiItems) > 0 {
		bckName = apiItems[0]
	}
	if r.ContentLength == 0 && r.Header.Get(cmn.HdrContentType) != cmn.ContentJSON {
		// must be an "easy URL" request, e.g.: curl -L -X GET 'http://aistore/ais/abc'
		msg = &cmn.ActionMsg{Action: cmn.ActList, Value: &cmn.ListObjsMsg{}}
	} else if msg, err = p.readActionMsg(w, r); err != nil {
		return
	}
	dpq := dpqAlloc()
	defer dpqFree(dpq)
	if err := urlQuery(r.URL.RawQuery, dpq); err != nil {
		p.writeErr(w, r, err)
		return
	}
	if queryBcks, err = newQueryBcksFromQuery(bckName, nil, dpq); err != nil {
		p.writeErr(w, r, err)
		return
	}
	switch msg.Action {
	case cmn.ActList:
		// list buckets
		if queryBcks.Name == "" {
			if err := p.checkACL(w, r, nil, cmn.AceListBuckets); err == nil {
				p.listBuckets(w, r, queryBcks, msg)
			}
			return
		}
		// list objects
		var (
			err error
			bck = cluster.NewBckEmbed(cmn.Bck(queryBcks))
		)
		bckArgs := bckInitArgs{p: p, w: w, r: r, msg: msg, perms: cmn.AceObjLIST, bck: bck, dpq: dpq}
		bckArgs.createAIS = false
		bckArgs.lookupRemote = lookupRemoteBck(nil, dpq)
		if bck, err = bckArgs.initAndTry(queryBcks.Name); err == nil {
			begin := mono.NanoTime()
			p.listObjects(w, r, bck, msg, begin)
		}
	case cmn.ActSummaryBck:
		p.bucketSummary(w, r, queryBcks, msg, dpq)
	default:
		p.writeErrAct(w, r, msg.Action)
	}
}

// GET /v1/objects/bucket-name/object-name
func (p *proxy) httpobjget(w http.ResponseWriter, r *http.Request, origURLBck ...string) {
	// 1. request
	apireq := apiReqAlloc(2, cmn.URLPathObjects.L, true /*dpq*/)
	if err := p.parseReq(w, r, apireq); err != nil {
		apiReqFree(apireq)
		return
	}

	// 2. bucket
	bckArgs := allocInitBckArgs()
	{
		bckArgs.p = p
		bckArgs.w = w
		bckArgs.r = r
		bckArgs.bck = apireq.bck
		bckArgs.dpq = apireq.dpq
		bckArgs.perms = cmn.AceGET
		bckArgs.createAIS = false
		bckArgs.lookupRemote = lookupRemoteBck(apireq.query, apireq.dpq)
	}
	if len(origURLBck) > 0 {
		bckArgs.origURLBck = origURLBck[0]
	}
	bck, err := bckArgs.initAndTry(apireq.bck.Name)
	freeInitBckArgs(bckArgs)

	objName := apireq.items[1]
	apiReqFree(apireq)
	if err != nil {
		return
	}

	// 3. redirect
	smap := p.owner.smap.get()
	si, err := cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s/%s => %s", r.Method, bck.Name, objName, si)
	}
	redirectURL := p.redirectURL(r, si, time.Now() /*started*/, cmn.NetIntraData)
	http.Redirect(w, r, redirectURL, http.StatusMovedPermanently)

	// 4. stats
	p.statsT.Add(stats.GetCount, 1)
}

// PUT /v1/objects/bucket-name/object-name
func (p *proxy) httpobjput(w http.ResponseWriter, r *http.Request) {
	var (
		nodeID string
		perms  cmn.AccessAttrs
	)
	// 1. request
	apireq := apiReqAlloc(2, cmn.URLPathObjects.L, true /*dpq*/)
	if err := p.parseReq(w, r, apireq); err != nil {
		apiReqFree(apireq)
		return
	}
	appendTyProvided := apireq.dpq.appendTy != "" // cmn.URLParamAppendType
	if !appendTyProvided {
		perms = cmn.AcePUT
	} else {
		hi, err := parseAppendHandle(apireq.dpq.appendHdl) // cmn.URLParamAppendHandle
		if err != nil {
			apiReqFree(apireq)
			p.writeErr(w, r, err)
			return
		}
		nodeID = hi.nodeID
		perms = cmn.AceAPPEND
	}

	// 2. bucket
	bckArgs := allocInitBckArgs()
	{
		bckArgs.p = p
		bckArgs.w = w
		bckArgs.r = r
		bckArgs.perms = perms
		bckArgs.createAIS = false
		bckArgs.lookupRemote = true
	}
	bckArgs.lookupRemote = bckArgs.lookupRemote && lookupRemoteBck(nil, apireq.dpq)
	bckArgs.bck, bckArgs.dpq = apireq.bck, apireq.dpq
	bck, err := bckArgs.initAndTry(apireq.bck.Name)
	freeInitBckArgs(bckArgs)

	objName := apireq.items[1]
	apiReqFree(apireq)
	if err != nil {
		return
	}

	// 3. redirect
	var (
		si      *cluster.Snode
		smap    = p.owner.smap.get()
		started = time.Now()
	)
	if nodeID == "" {
		si, err = cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
	} else {
		si = smap.GetTarget(nodeID)
		if si == nil {
			err = &errNodeNotFound{"PUT failure", nodeID, p.si, smap}
			p.writeErr(w, r, err)
			return
		}
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s/%s => %s (append: %v)", r.Method, bck.Name, objName, si, appendTyProvided)
	}
	redirectURL := p.redirectURL(r, si, started, cmn.NetIntraData)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)

	// 4. stats
	if !appendTyProvided {
		p.statsT.Add(stats.PutCount, 1)
	} else {
		p.statsT.Add(stats.AppendCount, 1)
	}
}

// DELETE /v1/objects/bucket-name/object-name
func (p *proxy) httpobjdelete(w http.ResponseWriter, r *http.Request) {
	bckArgs := allocInitBckArgs()
	{
		bckArgs.p = p
		bckArgs.w = w
		bckArgs.r = r
		bckArgs.perms = cmn.AceObjDELETE
		bckArgs.createAIS = false
		bckArgs.lookupRemote = true
	}
	bck, objName, err := p._parseReqTry(w, r, bckArgs)
	if err != nil {
		return
	}
	smap := p.owner.smap.get()
	si, err := cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s/%s => %s", r.Method, bck.Name, objName, si)
	}
	redirectURL := p.redirectURL(r, si, time.Now() /*started*/, cmn.NetIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)

	p.statsT.Add(stats.DeleteCount, 1)
}

// DELETE { action } /v1/buckets
func (p *proxy) httpbckdelete(w http.ResponseWriter, r *http.Request) {
	// 1. request
	apireq := apiReqAlloc(1, cmn.URLPathBuckets.L, false)
	defer apiReqFree(apireq)
	if err := p.parseReq(w, r, apireq); err != nil {
		return
	}
	msg, err := p.readActionMsg(w, r)
	if err != nil {
		return
	}
	perms := cmn.AceDestroyBucket
	if msg.Action == cmn.ActDeleteObjects || msg.Action == cmn.ActEvictObjects {
		perms = cmn.AceObjDELETE
	}

	// 2. bucket
	bck := apireq.bck
	bckArgs := bckInitArgs{p: p, w: w, r: r, msg: msg, perms: perms, bck: bck, dpq: apireq.dpq, query: apireq.query}
	bckArgs.createAIS = false
	bckArgs.lookupRemote = true
	if msg.Action == cmn.ActEvictRemoteBck {
		var errCode int
		bckArgs.lookupRemote = false
		bck, errCode, err = bckArgs.init(bck.Name)
		if err != nil {
			if errCode != http.StatusNotFound && !cmn.IsErrRemoteBckNotFound(err) {
				p.writeErr(w, r, err, errCode)
			}
			return
		}
	} else if bck, err = bckArgs.initAndTry(bck.Name); err != nil {
		return
	}

	// 3. action
	switch msg.Action {
	case cmn.ActEvictRemoteBck:
		if !bck.IsRemote() {
			p.writeErrf(w, r, fmtNotRemote, bck.Name)
			return
		}
		keepMD := cos.IsParseBool(apireq.query.Get(cmn.URLParamKeepBckMD))
		// HDFS buckets will always keep metadata so they can re-register later
		if bck.IsHDFS() || keepMD {
			if err := p.destroyBucketData(msg, bck); err != nil {
				p.writeErr(w, r, err)
			}
			return
		}
		if p.forwardCP(w, r, msg, bck.Name) {
			return
		}
		if err := p.destroyBucket(msg, bck); err != nil {
			p.writeErr(w, r, err)
		}
	case cmn.ActDestroyBck:
		if p.forwardCP(w, r, msg, bck.Name) {
			return
		}
		if bck.IsRemoteAIS() {
			if err := p.destroyBucket(msg, bck); err != nil {
				if !cmn.IsErrBckNotFound(err) {
					p.writeErr(w, r, err)
					return
				}
			}
			// After successful removal of local copy of a bucket, remove the bucket from remote.
			p.reverseReqRemote(w, r, msg, bck.Bck)
			return
		}
		if err := p.destroyBucket(msg, bck); err != nil {
			if cmn.IsErrBckNotFound(err) {
				glog.Infof("%s: %s already %q-ed, nothing to do", p.si, bck, msg.Action)
			} else {
				p.writeErr(w, r, err)
			}
		}
	case cmn.ActDeleteObjects, cmn.ActEvictObjects:
		var xactID string
		if msg.Action == cmn.ActEvictObjects && bck.IsAIS() {
			p.writeErrf(w, r, fmtNotRemote, bck.Name)
			return
		}
		if xactID, err = p.doListRange(r.Method, bck.Name, msg, apireq.query); err != nil {
			p.writeErr(w, r, err)
			return
		}
		w.Write([]byte(xactID))
	default:
		p.writeErrAct(w, r, msg.Action)
	}
}

// PUT /v1/metasync
// NOTE: compare with receiveCluMeta()
// NOTE: compare with t.metasyncHandlerPut
func (p *proxy) metasyncHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err = &errMsync{}
		cii = &err.Cii
	)
	if r.Method != http.MethodPut {
		cmn.WriteErr405(w, r, http.MethodPut)
		return
	}
	smap := p.owner.smap.get()
	if smap.isPrimary(p.si) {
		const txt = "is primary, cannot be on the receiving side of metasync"
		cii.fill(&p.htrun)
		if xctn := voteInProgress(); xctn != nil {
			err.Message = fmt.Sprintf("%s: %s [%s, %s]", p.si, txt, smap, xctn)
		} else {
			err.Message = fmt.Sprintf("%s: %s, %s", p.si, txt, smap)
		}
		p.writeErr(w, r, errors.New(cos.MustMarshalToString(err)), http.StatusConflict)
		return
	}
	payload := make(msPayload)
	if errP := payload.unmarshal(r.Body, "metasync put"); errP != nil {
		cmn.WriteErr(w, r, errP)
		return
	}
	// 1. extract
	var (
		caller                       = r.Header.Get(cmn.HdrCallerName)
		newConf, msgConf, errConf    = p.extractConfig(payload, caller)
		newSmap, msgSmap, errSmap    = p.extractSmap(payload, caller)
		newBMD, msgBMD, errBMD       = p.extractBMD(payload, caller)
		newRMD, msgRMD, errRMD       = p.extractRMD(payload, caller)
		newEtlMD, msgEtlMD, errEtlMD = p.extractEtlMD(payload, caller)
		revokedTokens, errTokens     = p.extractRevokedTokenList(payload, caller)
	)
	// 2. apply
	if errConf == nil && newConf != nil {
		errConf = p.receiveConfig(newConf, msgConf, payload, caller)
	}
	if errSmap == nil && newSmap != nil {
		errSmap = p.receiveSmap(newSmap, msgSmap, payload, caller, p.smapOnUpdate)
	}
	if errBMD == nil && newBMD != nil {
		errBMD = p.receiveBMD(newBMD, msgBMD, payload, caller)
	}
	if errRMD == nil && newRMD != nil {
		errRMD = p.receiveRMD(newRMD, msgRMD, caller)
	}
	if errEtlMD == nil && newEtlMD != nil {
		errEtlMD = p.receiveEtlMD(newEtlMD, msgEtlMD, payload, caller, nil)
	}
	if errTokens == nil && revokedTokens != nil {
		p.authn.updateRevokedList(revokedTokens)
	}
	// 3. respond
	if errConf == nil && errSmap == nil && errBMD == nil && errRMD == nil && errTokens == nil && errEtlMD == nil {
		return
	}
	cii.fill(&p.htrun)
	err.message(errConf, errSmap, errBMD, errRMD, errEtlMD, errTokens)
	p.writeErr(w, r, errors.New(cos.MustMarshalToString(err)), http.StatusConflict)
}

func (p *proxy) syncNewICOwners(smap, newSmap *smapX) {
	if !smap.IsIC(p.si) || !newSmap.IsIC(p.si) {
		return
	}

	for _, psi := range newSmap.Pmap {
		if p.si.ID() != psi.ID() && newSmap.IsIC(psi) && !smap.IsIC(psi) {
			go func(psi *cluster.Snode) {
				if err := p.ic.sendOwnershipTbl(psi); err != nil {
					glog.Errorf("%s: failed to send ownership table to %s, err:%v", p.si, psi, err)
				}
			}(psi)
		}
	}
}

// GET /v1/health
func (p *proxy) healthHandler(w http.ResponseWriter, r *http.Request) {
	if !p.NodeStarted() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	query := r.URL.Query()
	prr := cos.IsParseBool(query.Get(cmn.URLParamPrimaryReadyReb))
	if !prr {
		if responded := p.healthByExternalWD(w, r); responded {
			return
		}
	}
	// piggy-backing cluster info on health
	getCii := cos.IsParseBool(query.Get(cmn.URLParamClusterInfo))
	if getCii {
		debug.Assert(!prr)
		cii := &clusterInfo{}
		cii.fill(&p.htrun)
		_ = p.writeJSON(w, r, cii, "cluster-info")
		return
	}
	smap := p.owner.smap.get()
	if err := smap.validate(); err != nil {
		p.writeErr(w, r, err, http.StatusServiceUnavailable)
		return
	}
	// primary
	if smap.isPrimary(p.si) {
		if prr {
			if a, b := p.ClusterStarted(), p.owner.rmd.startup.Load(); !a || b {
				err := fmt.Errorf(fmtErrPrimaryNotReadyYet, p.si, a, b)
				if glog.FastV(4, glog.SmoduleAIS) {
					p.writeErr(w, r, err, http.StatusServiceUnavailable)
				} else {
					p.writeErrSilent(w, r, err, http.StatusServiceUnavailable)
				}
				return
			}
		}
		w.WriteHeader(http.StatusOK)
		return
	}
	// non-primary
	if !p.ClusterStarted() {
		// keep returning 503 until cluster starts up
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if prr || cos.IsParseBool(query.Get(cmn.URLParamAskPrimary)) {
		caller := r.Header.Get(cmn.HdrCallerName)
		p.writeErrf(w, r, "%s (non-primary): misdirected health-of-primary request from %s, %s",
			p.si, caller, smap.StringEx())
		return
	}
	w.WriteHeader(http.StatusOK)
}

// PUT { action } /v1/buckets/bucket-name
func (p *proxy) httpbckput(w http.ResponseWriter, r *http.Request) {
	var (
		msg           *cmn.ActionMsg
		query         = r.URL.Query()
		apiItems, err = p.checkRESTItems(w, r, 1, true, cmn.URLPathBuckets.L)
	)
	if err != nil {
		return
	}
	bucket := apiItems[0]
	bck, err := newBckFromQuery(bucket, query, nil)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	if msg, err = p.readActionMsg(w, r); err != nil {
		return
	}
	bckArgs := bckInitArgs{p: p, w: w, r: r, bck: bck, msg: msg, query: query}
	bckArgs.createAIS = false
	bckArgs.lookupRemote = lookupRemoteBck(query, nil)
	if bck, err = bckArgs.initAndTry(bck.Name); err != nil {
		return
	}
	switch msg.Action {
	case cmn.ActArchive:
		var (
			bckFrom = bck
			archMsg = &cmn.ArchiveMsg{}
		)
		if err := cos.MorphMarshal(msg.Value, archMsg); err != nil {
			p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
			return
		}
		bckTo := cluster.NewBckEmbed(archMsg.ToBck)
		if bckTo.IsEmpty() {
			bckTo = bckFrom
		} else {
			bckToArgs := bckInitArgs{p: p, w: w, r: r, bck: bckTo, msg: msg, perms: cmn.AcePUT, query: query}
			bckToArgs.createAIS = false
			bckToArgs.lookupRemote = lookupRemoteBck(query, nil)
			if bckTo, err = bckToArgs.initAndTry(bckTo.Name); err != nil {
				p.writeErr(w, r, err)
				return
			}
		}
		if _, err := cos.Mime(archMsg.Mime, archMsg.ArchName); err != nil {
			p.writeErr(w, r, err)
			return
		}
		xactID, err := p.createArchMultiObj(bckFrom, bckTo, msg)
		if err == nil {
			w.Write([]byte(xactID))
		} else {
			p.writeErr(w, r, err)
		}
	default:
		p.writeErrAct(w, r, msg.Action)
	}
}

// POST { action } /v1/buckets[/bucket-name]
func (p *proxy) httpbckpost(w http.ResponseWriter, r *http.Request) {
	var msg *cmn.ActionMsg
	apiItems, err := p.checkRESTItems(w, r, 1, true, cmn.URLPathBuckets.L)
	if err != nil {
		return
	}
	if msg, err = p.readActionMsg(w, r); err != nil {
		return
	}
	bucket := apiItems[0]
	p.hpostBucket(w, r, msg, bucket)
}

func (p *proxy) hpostBucket(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg, bucket string) {
	query := r.URL.Query()
	bck, err := newBckFromQuery(bucket, query, nil)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	if bck.IsRemoteAIS() {
		// forward to remote AIS as is, with distinct exception
		switch msg.Action {
		case cmn.ActInvalListCache:
		default:
			p.reverseReqRemote(w, r, msg, bck.Bck)
			return
		}
	}
	if msg.Action == cmn.ActCreateBck {
		p.hpostCreateBucket(w, r, query, msg, bck)
		return
	}

	// only the primary can do metasync
	xactRecord := xact.Table[msg.Action]
	if xactRecord.Metasync {
		if p.forwardCP(w, r, msg, bucket) {
			return
		}
	}

	// Initialize bucket; if doesn't exist try creating it on the fly
	// but only if it's a remote bucket (and user did not explicitly disallowed).
	bckArgs := bckInitArgs{p: p, w: w, r: r, bck: bck, msg: msg, query: query}
	bckArgs.createAIS = false
	bckArgs.lookupRemote = lookupRemoteBck(query, nil)
	if bck, err = bckArgs.initAndTry(bck.Name); err != nil {
		return
	}

	//
	// {action} on bucket
	//
	switch msg.Action {
	case cmn.ActMoveBck:
		bckFrom := bck
		bckTo, err := newBckFromQueryUname(query, true /*required*/)
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		if !bckFrom.IsAIS() && !bckFrom.HasBackendBck() {
			p.writeErrf(w, r, "cannot rename bucket %q, it is not an AIS bucket", bckFrom)
			return
		}
		if bckTo.IsRemote() {
			p.writeErrf(w, r, "destination bucket %q must be an AIS bucket", bckTo)
			return
		}
		if bckFrom.Name == bckTo.Name {
			p.writeErrf(w, r, "cannot rename bucket %q as %q", bckFrom, bckTo)
			return
		}

		bckFrom.Provider = cmn.ProviderAIS
		bckTo.Provider = cmn.ProviderAIS

		if _, present := p.owner.bmd.get().Get(bckTo); present {
			err := cmn.NewErrBckAlreadyExists(bckTo.Bck)
			p.writeErr(w, r, err)
			return
		}
		glog.Infof("%s bucket %s => %s", msg.Action, bckFrom, bckTo)
		var xactID string
		if xactID, err = p.renameBucket(bckFrom, bckTo, msg); err != nil {
			p.writeErr(w, r, err)
			return
		}
		w.Write([]byte(xactID))
	case cmn.ActCopyBck, cmn.ActETLBck:
		var (
			xactID  string
			bckTo   *cluster.Bck
			tcbMsg  = &cmn.TCBMsg{}
			errCode int
		)
		switch msg.Action {
		case cmn.ActETLBck:
			if err := cos.MorphMarshal(msg.Value, tcbMsg); err != nil {
				p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
				return
			}
			if err := tcbMsg.Validate(); err != nil {
				p.writeErr(w, r, err)
				return
			}
		case cmn.ActCopyBck:
			if err = cos.MorphMarshal(msg.Value, &tcbMsg.CopyBckMsg); err != nil {
				p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
				return
			}
		}
		bckTo, err = newBckFromQueryUname(query, true /*required*/)
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		if bck.Equal(bckTo, false, true) {
			p.writeErrf(w, r, "cannot %s bucket %q onto itself", msg.Action, bck)
			return
		}
		// NOTE: not calling `initAndTry` - delegating bucket props cloning to the `p.tcb` below
		bckToArgs := bckInitArgs{p: p, w: w, r: r, bck: bckTo, perms: cmn.AcePUT, query: query}
		bckToArgs.createAIS = true
		bckArgs.lookupRemote = lookupRemoteBck(query, nil)
		if bckTo, errCode, err = bckToArgs.init(bckTo.Name); err != nil && errCode != http.StatusNotFound {
			p.writeErr(w, r, err, errCode)
			return
		}
		// NOTE: creating remote on the fly
		if errCode == http.StatusNotFound && bckTo.IsRemote() {
			if bckTo, err = bckToArgs.try(); err != nil {
				return
			}
		}
		if bckTo.IsHTTP() {
			p.writeErrf(w, r, "cannot %s to HTTP bucket %q", msg.Action, bckTo)
			return
		}
		glog.Infof("%s bucket %s => %s", msg.Action, bck, bckTo)
		if xactID, err = p.tcb(bck, bckTo, msg, tcbMsg.DryRun); err != nil {
			p.writeErr(w, r, err)
			return
		}
		w.Write([]byte(xactID))
	case cmn.ActCopyObjects, cmn.ActETLObjects:
		var (
			xactID string
			tcoMsg = &cmn.TCObjsMsg{}
			bckTo  *cluster.Bck
		)
		if err = cos.MorphMarshal(msg.Value, tcoMsg); err != nil {
			p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
			return
		}
		bckTo = cluster.NewBckEmbed(tcoMsg.ToBck)
		if err = bckTo.Init(p.owner.bmd); err != nil {
			p.writeErr(w, r, err)
			return
		}
		if bck.Equal(bckTo, true, true) {
			glog.Warningf("multi-obj %s within the same bucket %q", msg.Action, bck)
		}
		if bckTo.IsHTTP() {
			p.writeErrf(w, r, "cannot %s to HTTP bucket %q", msg.Action, bckTo)
			return
		}
		glog.Infof("multi-obj %s %s => %s", msg.Action, bck, bckTo)
		if xactID, err = p.tcobjs(bck, bckTo, msg); err != nil {
			p.writeErr(w, r, err)
			return
		}
		w.Write([]byte(xactID))
	case cmn.ActAddRemoteBck:
		// TODO: choose the best permission
		if err := p.checkACL(w, r, nil, cmn.AceCreateBucket); err != nil {
			return
		}
		if err := p.createBucket(msg, bck); err != nil {
			errCode := http.StatusInternalServerError
			if _, ok := err.(*cmn.ErrBucketAlreadyExists); ok {
				errCode = http.StatusConflict
			}
			p.writeErr(w, r, err, errCode)
			return
		}
	case cmn.ActPrefetchObjects:
		// TODO: GET vs SYNC?
		if bck.IsAIS() {
			p.writeErrf(w, r, fmtNotRemote, bucket)
			return
		}
		var xactID string
		if xactID, err = p.doListRange(r.Method, bucket, msg, query); err != nil {
			p.writeErr(w, r, err)
			return
		}
		w.Write([]byte(xactID))
	case cmn.ActInvalListCache:
		p.qm.c.invalidate(bck.Bck)
	case cmn.ActMakeNCopies:
		var xactID string
		if xactID, err = p.makeNCopies(msg, bck); err != nil {
			p.writeErr(w, r, err)
			return
		}
		w.Write([]byte(xactID))
	case cmn.ActECEncode:
		var xactID string
		if xactID, err = p.ecEncode(bck, msg); err != nil {
			p.writeErr(w, r, err)
			return
		}
		w.Write([]byte(xactID))
	default:
		p.writeErrAct(w, r, msg.Action)
	}
}

func (p *proxy) hpostCreateBucket(w http.ResponseWriter, r *http.Request, query url.Values, msg *cmn.ActionMsg, bck *cluster.Bck) {
	bucket := bck.Name
	err := p.checkACL(w, r, nil, cmn.AceCreateBucket)
	if err != nil {
		return
	}
	if err = bck.Validate(); err != nil {
		p.writeErr(w, r, err)
		return
	}
	if p.forwardCP(w, r, msg, bucket) {
		return
	}
	if bck.Provider == "" {
		bck.Provider = cmn.ProviderAIS
	}
	if bck.IsHDFS() && msg.Value == nil {
		p.writeErr(w, r,
			errors.New("property 'extra.hdfs.ref_directory' must be specified when creating HDFS bucket"))
		return
	}
	if msg.Value != nil {
		propsToUpdate := cmn.BucketPropsToUpdate{}
		if err := cos.MorphMarshal(msg.Value, &propsToUpdate); err != nil {
			p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
			return
		}
		// Make and validate new bucket props.
		bck.Props = defaultBckProps(bckPropsArgs{bck: bck})
		bck.Props, err = p.makeNewBckProps(bck, &propsToUpdate, true /*creating*/)
		if err != nil {
			p.writeErr(w, r, err)
			return
		}

		if bck.HasBackendBck() {
			// Initialize backend bucket.
			backend := cluster.BackendBck(bck)
			if err = backend.InitNoBackend(p.owner.bmd); err != nil {
				if !cmn.IsErrRemoteBckNotFound(err) {
					p.writeErrf(w, r,
						"cannot create %s: failing to initialize backend %s, err: %v",
						bck, backend, err)
					return
				}
				args := bckInitArgs{p: p, w: w, r: r, bck: backend, msg: msg, query: query}
				args.createAIS = false
				args.lookupRemote = true
				if _, err = args.try(); err != nil {
					return
				}
			}
		}

		// Send full props to the target. Required for HDFS provider.
		msg.Value = bck.Props
	}
	if err := p.createBucket(msg, bck); err != nil {
		errCode := http.StatusInternalServerError
		if _, ok := err.(*cmn.ErrBucketAlreadyExists); ok {
			errCode = http.StatusConflict
		}
		p.writeErr(w, r, err, errCode)
	}
}

func (p *proxy) listObjects(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, amsg *cmn.ActionMsg, begin int64) {
	var (
		err     error
		bckList *cmn.BucketList
		lsmsg   = cmn.ListObjsMsg{}
		smap    = p.owner.smap.get()
	)
	if err := cos.MorphMarshal(amsg.Value, &lsmsg); err != nil {
		p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, amsg.Action, amsg.Value, err)
		return
	}
	if smap.CountActiveTargets() < 1 {
		p.writeErrMsg(w, r, "no registered targets yet")
		return
	}

	// If props were not explicitly specified always return default ones.
	if lsmsg.Props == "" {
		lsmsg.AddProps(cmn.GetPropsDefault...)
	}

	// Vanilla HTTP buckets do not support remote listing.
	// LsArchDir needs files locally to read archive content.
	if bck.IsHTTP() || lsmsg.IsFlagSet(cmn.LsArchDir) {
		lsmsg.SetFlag(cmn.LsPresent)
	}

	locationIsAIS := bck.IsAIS() || lsmsg.IsFlagSet(cmn.LsPresent)
	if lsmsg.UUID == "" {
		var nl nl.NotifListener
		lsmsg.UUID = cos.GenUUID()
		if locationIsAIS || lsmsg.NeedLocalMD() {
			nl = xact.NewXactNL(lsmsg.UUID,
				cmn.ActList, &smap.Smap, nil, bck.Bck)
		} else {
			// random target to execute `list-objects` on a Cloud bucket
			si, _ := smap.GetRandTarget()
			nl = xact.NewXactNL(lsmsg.UUID, cmn.ActList,
				&smap.Smap, cluster.NodeMap{si.ID(): si}, bck.Bck)
		}
		nl.SetHrwOwner(&smap.Smap)
		p.ic.registerEqual(regIC{nl: nl, smap: smap, msg: amsg})
	}

	if p.ic.reverseToOwner(w, r, lsmsg.UUID, amsg) {
		return
	}

	if locationIsAIS {
		bckList, err = p.listObjectsAIS(bck, lsmsg)
	} else {
		bckList, err = p.listObjectsRemote(bck, lsmsg)
		// TODO: `status == http.StatusGone` At this point we know that this
		//       remote bucket exists and is offline. We should somehow try to list
		//       cached objects. This isn't easy as we basically need to start a new
		//       xaction and return new `UUID`.
	}
	if err != nil {
		p.writeErr(w, r, err)
		return
	}

	cos.Assert(bckList != nil)

	if strings.Contains(r.Header.Get(cmn.HdrAccept), cmn.ContentMsgPack) {
		if !p.writeMsgPack(w, r, bckList, "list_objects") {
			return
		}
	} else if !p.writeJSON(w, r, bckList, "list_objects") {
		return
	}

	// Free memory allocated for temporary slice immediately as it can take up to a few GB
	bckList.Entries = bckList.Entries[:0]
	bckList.Entries = nil
	bckList = nil

	delta := mono.SinceNano(begin)
	p.statsT.AddMany(
		cos.NamedVal64{Name: stats.ListCount, Value: 1},
		cos.NamedVal64{Name: stats.ListLatency, Value: delta},
	)
}

// bucket == "": all buckets for a given provider
func (p *proxy) bucketSummary(w http.ResponseWriter, r *http.Request, queryBcks cmn.QueryBcks, amsg *cmn.ActionMsg, dpq *dpq) {
	var lsmsg cmn.BckSummMsg
	if err := cos.MorphMarshal(amsg.Value, &lsmsg); err != nil {
		p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, amsg.Action, amsg.Value, err)
		return
	}
	if queryBcks.Name != "" {
		bck := cluster.NewBckEmbed(cmn.Bck(queryBcks))
		bckArgs := bckInitArgs{p: p, w: w, r: r, msg: amsg, perms: cmn.AceBckHEAD, bck: bck, dpq: dpq}
		bckArgs.createAIS = false
		bckArgs.lookupRemote = lookupRemoteBck(nil, dpq)
		if _, err := bckArgs.initAndTry(queryBcks.Name); err != nil {
			return
		}
	}
	if dpq.uuid != "" { // cmn.URLParamUUID
		lsmsg.UUID = dpq.uuid
	}
	summaries, uuid, err := p.gatherBckSumm(queryBcks, &lsmsg)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}

	// returned uuid == "" indicates that async runner has finished and the summary is done;
	// otherwise it's an ID of the still running task
	if uuid != "" {
		w.WriteHeader(http.StatusAccepted)
		w.Write([]byte(uuid))
		return
	}

	p.writeJSON(w, r, summaries, "bucket_summary")
}

func (p *proxy) gatherBckSumm(bck cmn.QueryBcks, msg *cmn.BckSummMsg) (
	summaries cmn.BckSummaries, uuid string, err error) {
	var (
		isNew, q = initAsyncQuery(cmn.Bck(bck), msg, cos.GenUUID())
		config   = cmn.GCO.Get()
		smap     = p.owner.smap.get()
		aisMsg   = p.newAmsgActVal(cmn.ActSummaryBck, msg)
	)
	args := allocBcArgs()
	args.req = cmn.HreqArgs{
		Method: http.MethodGet,
		Path:   cmn.URLPathBuckets.Join(bck.Name),
		Query:  q,
		Body:   cos.MustMarshal(aisMsg),
	}
	args.smap = smap
	args.timeout = config.Timeout.MaxHostBusy.D() + config.Timeout.MaxKeepalive.D()
	results := p.bcastGroup(args)
	allOK, _, err := p.checkBckTaskResp(msg.UUID, results)
	if err != nil {
		return nil, "", err
	}

	// some targets are still executing their tasks, or it is the request to start
	// an async task - return only uuid
	if !allOK || isNew {
		return nil, msg.UUID, nil
	}

	// all targets are ready, prepare the final result
	q = url.Values{}
	q = cmn.AddBckToQuery(q, cmn.Bck(bck))
	q.Set(cmn.URLParamTaskAction, cmn.TaskResult)
	q.Set(cmn.URLParamSilent, "true")
	args.req.Query = q
	args.fv = func() interface{} { return &cmn.BckSummaries{} }
	summaries = make(cmn.BckSummaries, 0)
	results = p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err != nil {
			err = res.toErr()
			freeBcastRes(results)
			return nil, "", err
		}
		targetSummary := res.v.(*cmn.BckSummaries)
		for _, bckSummary := range *targetSummary {
			summaries = summaries.Aggregate(bckSummary)
		}
	}
	freeBcastRes(results)
	return summaries, "", nil
}

// POST { action } /v1/objects/bucket-name[/object-name]
func (p *proxy) httpobjpost(w http.ResponseWriter, r *http.Request) {
	msg, err := p.readActionMsg(w, r)
	if err != nil {
		return
	}
	apireq := apiReqAlloc(1, cmn.URLPathObjects.L, false)
	defer apiReqFree(apireq)
	if msg.Action == cmn.ActRenameObject {
		apireq.after = 2
	}
	if err := p.parseReq(w, r, apireq); err != nil {
		return
	}

	// TODO: revisit versus remote bucket not being present, see p.tryBckInit
	bck := apireq.bck
	if err := bck.Init(p.owner.bmd); err != nil {
		p.writeErr(w, r, err)
		return
	}
	switch msg.Action {
	case cmn.ActRenameObject:
		if err := p.checkACL(w, r, bck, cmn.AceObjMOVE); err != nil {
			return
		}
		if bck.IsRemote() {
			p.writeErrActf(w, r, msg.Action, "not supported for remote buckets (%s)", bck)
			return
		}
		if bck.Props.EC.Enabled {
			p.writeErrActf(w, r, msg.Action, "not supported for erasure-coded buckets (%s)", bck)
			return
		}
		p.objMv(w, r, bck, apireq.items[1], msg)
		return
	case cmn.ActPromote:
		if err := p.checkACL(w, r, bck, cmn.AcePromote); err != nil {
			return
		}
		// ActionMsg.Name is the source
		if !filepath.IsAbs(msg.Name) {
			if msg.Name == "" {
				p.writeErrMsg(w, r, "promoted source pathname is empty")
			} else {
				p.writeErrf(w, r, "promoted source must be an absolute path (got %q)", msg.Name)
			}
			return
		}
		args := &cluster.PromoteArgs{}
		if err := cos.MorphMarshal(msg.Value, args); err != nil {
			p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
			return
		}
		var tsi *cluster.Snode
		if args.DaemonID != "" {
			smap := p.owner.smap.get()
			if tsi = smap.GetTarget(args.DaemonID); tsi == nil {
				err := &errNodeNotFound{cmn.ActPromote + " failure", args.DaemonID, p.si, smap}
				p.writeErr(w, r, err)
				return
			}
		}
		xactID, err := p.promote(bck, msg, tsi)
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		w.Write([]byte(xactID))
	default:
		p.writeErrAct(w, r, msg.Action)
	}
}

// HEAD /v1/buckets/bucket-name
func (p *proxy) httpbckhead(w http.ResponseWriter, r *http.Request) {
	apireq := apiReqAlloc(1, cmn.URLPathBuckets.L, false)
	defer apiReqFree(apireq)
	if err := p.parseReq(w, r, apireq); err != nil {
		return
	}
	bckArgs := bckInitArgs{p: p, w: w, r: r, bck: apireq.bck, perms: cmn.AceBckHEAD, dpq: apireq.dpq, query: apireq.query}
	bckArgs.createAIS = false
	bckArgs.lookupRemote = lookupRemoteBck(apireq.query, nil)
	bck, err := bckArgs.initAndTry(apireq.bck.Name)
	if err != nil {
		return
	}
	if bck.IsAIS() || !bckArgs.exists {
		_bpropsToHdr(bck, w.Header())
		return
	}
	cloudProps, statusCode, err := p.headRemoteBck(*bck.RemoteBck(), nil)
	if err != nil {
		// TODO: what if HEAD fails
		p.writeErr(w, r, err, statusCode)
		return
	}

	if p.forwardCP(w, r, nil, "httpheadbck") {
		return
	}

	ctx := &bmdModifier{
		pre:        p._bckHeadPre,
		final:      p._syncBMDFinal,
		msg:        &cmn.ActionMsg{Action: cmn.ActResyncBprops},
		bcks:       []*cluster.Bck{bck},
		cloudProps: cloudProps,
	}
	_, err = p.owner.bmd.modify(ctx)
	if err != nil {
		debug.AssertNoErr(err)
		p.writeErr(w, r, err, http.StatusNotFound)
		return
	}
	_bpropsToHdr(bck, w.Header())
}

func _bpropsToHdr(bck *cluster.Bck, hdr http.Header) {
	if bck.Props == nil {
		bck.Props = defaultBckProps(bckPropsArgs{bck: bck, hdr: hdr})
	}
	hdr.Set(cmn.HdrBucketProps, cos.MustMarshalToString(bck.Props))
}

func (p *proxy) _bckHeadPre(ctx *bmdModifier, clone *bucketMD) error {
	var (
		bck             = ctx.bcks[0]
		bprops, present = clone.Get(bck)
	)
	if !present {
		return cmn.NewErrBckNotFound(bck.Bck)
	}
	nprops := mergeRemoteBckProps(bprops, ctx.cloudProps)
	if nprops.Equal(bprops) {
		glog.Warningf("%s: Cloud bucket %s properties are already in-sync, nothing to do", p.si, bck)
		ctx.terminate = true
		return nil
	}
	clone.set(bck, nprops)
	return nil
}

// PATCH /v1/buckets/bucket-name
func (p *proxy) httpbckpatch(w http.ResponseWriter, r *http.Request) {
	var (
		err           error
		msg           *cmn.ActionMsg
		propsToUpdate cmn.BucketPropsToUpdate
		xactID        string
		nprops        *cmn.BucketProps // complete instance of bucket props with propsToUpdate changes
	)
	apireq := apiReqAlloc(1, cmn.URLPathBuckets.L, false)
	defer apiReqFree(apireq)
	if err = p.parseReq(w, r, apireq); err != nil {
		return
	}
	if msg, err = p.readActionMsg(w, r); err != nil {
		return
	}
	if err := cos.MorphMarshal(msg.Value, &propsToUpdate); err != nil {
		p.writeErrMsg(w, r, "invalid props-to-update value in apireq: "+msg.String())
		return
	}
	if p.forwardCP(w, r, msg, "httpbckpatch") {
		return
	}
	bck, perms := apireq.bck, cmn.AcePATCH
	if propsToUpdate.Access != nil {
		perms |= cmn.AceBckSetACL
	}
	bckArgs := bckInitArgs{p: p, w: w, r: r, bck: bck, msg: msg, skipBackend: true, perms: perms, dpq: apireq.dpq, query: apireq.query}
	bckArgs.createAIS = false
	bckArgs.lookupRemote = lookupRemoteBck(apireq.query, nil)
	if bck, err = bckArgs.initAndTry(bck.Name); err != nil {
		return
	}
	if err = _checkAction(msg, cmn.ActSetBprops, cmn.ActResetBprops); err != nil {
		p.writeErr(w, r, err)
		return
	}
	// make and validate new props
	if nprops, err = p.makeNewBckProps(bck, &propsToUpdate); err != nil {
		p.writeErr(w, r, err)
		return
	}
	if !nprops.BackendBck.IsEmpty() {
		// backend must exist
		backendBck := cluster.NewBckEmbed(nprops.BackendBck)
		args := bckInitArgs{p: p, w: w, r: r, bck: backendBck, msg: msg, dpq: apireq.dpq, query: apireq.query}
		args.createAIS = false
		args.lookupRemote = true
		if _, err = args.initAndTry(backendBck.Name); err != nil {
			return
		}
		// init and validate
		if err = p.initBackendProp(nprops); err != nil {
			p.writeErr(w, r, err)
			return
		}
	}
	if xactID, err = p.setBucketProps(msg, bck, nprops); err != nil {
		p.writeErr(w, r, err)
		return
	}
	w.Write([]byte(xactID))
}

// HEAD /v1/objects/bucket-name/object-name
func (p *proxy) httpobjhead(w http.ResponseWriter, r *http.Request, origURLBck ...string) {
	bckArgs := allocInitBckArgs()
	{
		bckArgs.p = p
		bckArgs.w = w
		bckArgs.r = r
		bckArgs.perms = cmn.AceObjHEAD
		bckArgs.createAIS = false
		bckArgs.lookupRemote = true
	}
	if len(origURLBck) > 0 {
		bckArgs.origURLBck = origURLBck[0]
	}
	bck, objName, err := p._parseReqTry(w, r, bckArgs)
	if err != nil {
		return
	}
	smap := p.owner.smap.get()
	si, err := cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
	if err != nil {
		p.writeErr(w, r, err, http.StatusInternalServerError)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s/%s => %s", r.Method, bck.Name, objName, si)
	}
	redirectURL := p.redirectURL(r, si, time.Now() /*started*/, cmn.NetIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

// PATCH /v1/objects/bucket-name/object-name
func (p *proxy) httpobjpatch(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	bckArgs := allocInitBckArgs()
	{
		bckArgs.p = p
		bckArgs.w = w
		bckArgs.r = r
		bckArgs.perms = cmn.AceObjHEAD
		bckArgs.createAIS = false
		bckArgs.lookupRemote = true
	}
	bck, objName, err := p._parseReqTry(w, r, bckArgs)
	if err != nil {
		return
	}
	smap := p.owner.smap.get()
	si, err := cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
	if err != nil {
		p.writeErr(w, r, err, http.StatusInternalServerError)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s/%s => %s", r.Method, bck.Name, objName, si)
	}
	redirectURL := p.redirectURL(r, si, started, cmn.NetIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

//============================
//
// supporting methods and misc
//
//============================
// forward control plane request to the current primary proxy
// return: forf (forwarded or failed) where forf = true means exactly that: forwarded or failed
func (p *proxy) forwardCP(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg,
	s string, origBody ...[]byte) (forf bool) {
	var (
		body []byte
		smap = p.owner.smap.get()
	)
	if !smap.isValid() {
		errmsg := fmt.Sprintf("%s must be starting up: cannot execute", p.si)
		if msg != nil {
			p.writeErrStatusf(w, r, http.StatusServiceUnavailable, "%s %s: %s", errmsg, msg.Action, s)
		} else {
			p.writeErrStatusf(w, r, http.StatusServiceUnavailable, "%s %q", errmsg, s)
		}
		return true
	}
	if p.inPrimaryTransition.Load() {
		p.writeErrStatusf(w, r, http.StatusServiceUnavailable,
			"%s is in transition, cannot process the request", p.si)
		return true
	}
	if smap.isPrimary(p.si) {
		return
	}
	// We must **not** send any request body when doing HEAD request.
	// Otherwise, the request can be rejected and terminated.
	if r.Method != http.MethodHead {
		if len(origBody) > 0 && len(origBody[0]) > 0 {
			body = origBody[0]
		} else if msg != nil {
			body = cos.MustMarshal(msg)
		}
	}
	primary := &p.rproxy.primary
	primary.Lock()
	if primary.url != smap.Primary.PublicNet.DirectURL {
		primary.url = smap.Primary.PublicNet.DirectURL
		uparsed, err := url.Parse(smap.Primary.PublicNet.DirectURL)
		cos.AssertNoErr(err)
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
		debug.AssertFunc(func() bool {
			l, _ := io.Copy(io.Discard, r.Body)
			return l == 0
		})

		r.Body = io.NopCloser(bytes.NewBuffer(body))
		r.ContentLength = int64(len(body)) // Directly setting `Content-Length` header.
	}
	if msg != nil {
		glog.Infof("%s: forwarding \"%s:%s\" to the primary %s", p.si, msg.Action, s, smap.Primary.StringEx())
	} else {
		glog.Infof("%s: forwarding %q to the primary %s", p.si, s, smap.Primary.StringEx())
	}
	primary.rp.ServeHTTP(w, r)
	return true
}

// Based on default error handler `defaultErrorHandler` in `httputil/reverseproxy.go`.
func (p *proxy) rpErrHandler(w http.ResponseWriter, r *http.Request, err error) {
	msg := fmt.Sprintf("%s: rproxy err %v, req: %s %s", p.si, err, r.Method, r.URL.Path)
	if caller := r.Header.Get(cmn.HdrCallerName); caller != "" {
		msg += " (from " + caller + ")"
	}
	glog.Errorln(msg)
	w.WriteHeader(http.StatusBadGateway)
}

// reverse-proxy request
func (p *proxy) reverseNodeRequest(w http.ResponseWriter, r *http.Request, si *cluster.Snode) {
	parsedURL, err := url.Parse(si.URL(cmn.NetPublic))
	debug.AssertNoErr(err)
	p.reverseRequest(w, r, si.ID(), parsedURL)
}

func (p *proxy) reverseRequest(w http.ResponseWriter, r *http.Request, nodeID string, parsedURL *url.URL) {
	rproxy := p.rproxy.loadOrStore(nodeID, parsedURL, p.rpErrHandler)
	rproxy.ServeHTTP(w, r)
}

func (p *proxy) reverseReqRemote(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg, bck cmn.Bck) (err error) {
	var (
		remoteUUID    = bck.Ns.UUID
		query         = r.URL.Query()
		v, configured = cmn.GCO.Get().Backend.ProviderConf(cmn.ProviderAIS)
	)

	if !configured {
		err = errors.New("ais remote cloud is not configured")
		p.writeErr(w, r, err)
		return err
	}

	aisConf := cmn.BackendConfAIS{}
	cos.MustMorphMarshal(v, &aisConf)
	urls, exists := aisConf[remoteUUID]
	if !exists {
		// TODO: this is a temp workaround - proxy should store UUIDs
		// and aliases, not just aliases (see #1136)
		aisInfo, err := p.getRemoteAISInfo()
		var remoteAlias *cmn.RemoteAISInfo
		if err == nil {
			remoteAlias = (*aisInfo)[remoteUUID]
		}
		if remoteAlias != nil {
			urls, exists = aisConf[remoteAlias.Alias]
		}
	}
	if !exists {
		err = cmn.NewErrNotFound("%s: remote UUID/alias %q", p.si, remoteUUID)
		p.writeErr(w, r, err)
		return err
	}

	cos.Assert(len(urls) > 0)
	u, err := url.Parse(urls[0])
	if err != nil {
		p.writeErr(w, r, err)
		return err
	}
	if msg != nil {
		body := cos.MustMarshal(msg)
		r.Body = io.NopCloser(bytes.NewReader(body))
	}

	bck.Ns.UUID = ""
	query = cmn.DelBckFromQuery(query)
	query = cmn.AddBckToQuery(query, bck)
	r.URL.RawQuery = query.Encode()
	p.reverseRequest(w, r, remoteUUID, u)
	return nil
}

func (p *proxy) listBuckets(w http.ResponseWriter, r *http.Request, query cmn.QueryBcks, msg *cmn.ActionMsg) {
	bmd := p.owner.bmd.get()
	if query.Provider != "" {
		if query.IsAIS() || query.IsHDFS() {
			bcks := selectBMDBuckets(bmd, query)
			p.writeJSON(w, r, bcks, listBuckets)
			return
		} else if query.IsCloud() {
			config := cmn.GCO.Get()
			if _, ok := config.Backend.Providers[query.Provider]; !ok {
				err := &cmn.ErrMissingBackend{Provider: query.Provider}
				p.writeErrf(w, r, "cannot list %q bucket: %v", query, err)
				return
			}
		}
	}
	si, err := p.owner.smap.get().GetRandTarget()
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	bodyCopy := cos.MustMarshal(msg)
	// NOTE: as the original r.Body has been already read and closed we always reset Content-Length;
	// (e.g. when it changes: curl -L -X GET -H 'Content-Type: application/json' -d '{"action": "list"}' ...)
	r.ContentLength = int64(len(bodyCopy))
	r.Body = io.NopCloser(bytes.NewBuffer(bodyCopy))
	p.reverseNodeRequest(w, r, si)
}

func (p *proxy) redirectURL(r *http.Request, si *cluster.Snode, ts time.Time, netName string) (redirect string) {
	var (
		nodeURL string
		query   = url.Values{}
	)
	if p.si.LocalNet == nil {
		nodeURL = si.URL(cmn.NetPublic)
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
			nodeURL = si.URL(cmn.NetPublic)
		}
	}
	redirect = nodeURL + r.URL.Path + "?"
	if r.URL.RawQuery != "" {
		redirect += r.URL.RawQuery + "&"
	}

	query.Set(cmn.URLParamProxyID, p.si.ID())
	query.Set(cmn.URLParamUnixTime, cos.UnixNano2S(ts.UnixNano()))
	redirect += query.Encode()
	return
}

func initAsyncQuery(bck cmn.Bck, msg *cmn.BckSummMsg, newTaskID string) (bool, url.Values) {
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

	q = cmn.AddBckToQuery(q, bck)
	return isNew, q
}

func (p *proxy) checkBckTaskResp(uuid string, results sliceResults) (allOK bool, status int, err error) {
	// check response codes of all targets
	// Target that has completed its async task returns 200, and 202 otherwise
	allOK = true
	allNotFound := true
	for _, res := range results {
		if res.status == http.StatusNotFound {
			continue
		}
		allNotFound = false
		if res.err != nil {
			allOK, status, err = false, res.status, res.err
			freeBcastRes(results)
			return
		}
		if res.status != http.StatusOK {
			allOK = false
			status = res.status
			break
		}
	}
	freeBcastRes(results)
	if allNotFound {
		err = cmn.NewErrNotFound("%s: task %q", p.si, uuid)
	}
	return
}

// listObjectsAIS reads object list from all targets, combines, sorts and returns
// the final list. Excess of object entries from each target is remembered in the
// buffer (see: `queryBuffers`) so we won't request the same objects again.
func (p *proxy) listObjectsAIS(bck *cluster.Bck, lsmsg cmn.ListObjsMsg) (allEntries *cmn.BucketList, err error) {
	var (
		aisMsg    *aisMsg
		args      *bcastArgs
		entries   []*cmn.BucketEntry
		results   sliceResults
		smap      = p.owner.smap.get()
		cacheID   = cacheReqID{bck: bck.Bck, prefix: lsmsg.Prefix}
		token     = lsmsg.ContinuationToken
		props     = lsmsg.PropsSet()
		hasEnough bool
		flags     uint32
	)
	if lsmsg.PageSize == 0 {
		lsmsg.PageSize = cmn.DefaultListPageSizeAIS
	}
	pageSize := lsmsg.PageSize

	// TODO: Before checking cache and buffer we should check if there is another
	//  request already in-flight that requests the same page as we do - if yes
	//  then we should just patiently wait for the cache to get populated.

	if lsmsg.IsFlagSet(cmn.UseListObjsCache) {
		entries, hasEnough = p.qm.c.get(cacheID, token, pageSize)
		if hasEnough {
			goto end
		}
		// Request for all the props if (cache should always have all entries).
		lsmsg.AddProps(cmn.GetPropsAll...)
	}
	entries, hasEnough = p.qm.b.get(lsmsg.UUID, token, pageSize)
	if hasEnough {
		// We have enough in the buffer to fulfill the request.
		goto endWithCache
	}

	// User requested some page but we don't have enough (but we may have part
	// of the full page). Therefore, we must ask targets for page starting from
	// what we have locally, so we don't re-request the objects.
	lsmsg.ContinuationToken = p.qm.b.last(lsmsg.UUID, token)

	aisMsg = p.newAmsgActVal(cmn.ActList, &lsmsg)
	args = allocBcArgs()
	args.req = cmn.HreqArgs{
		Method: http.MethodGet,
		Path:   cmn.URLPathBuckets.Join(bck.Name),
		Query:  cmn.AddBckToQuery(nil, bck.Bck),
		Body:   cos.MustMarshal(aisMsg),
	}
	args.timeout = cmn.LongTimeout // TODO: should it be `Client.ListObjects`?
	args.smap = smap
	args.fv = func() interface{} { return &cmn.BucketList{} }

	// Combine the results.
	results = p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err != nil {
			err = res.toErr()
			freeBcastRes(results)
			return nil, err
		}
		objList := res.v.(*cmn.BucketList)
		flags |= objList.Flags
		p.qm.b.set(lsmsg.UUID, res.si.ID(), objList.Entries, pageSize)
	}
	freeBcastRes(results)
	entries, hasEnough = p.qm.b.get(lsmsg.UUID, token, pageSize)
	cos.Assert(hasEnough)

endWithCache:
	if lsmsg.IsFlagSet(cmn.UseListObjsCache) {
		p.qm.c.set(cacheID, token, entries, pageSize)
	}
end:
	if lsmsg.IsFlagSet(cmn.UseListObjsCache) && !props.All(cmn.GetPropsAll...) {
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
		UUID:    lsmsg.UUID,
		Entries: entries,
		Flags:   flags,
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
func (p *proxy) listObjectsRemote(bck *cluster.Bck, lsmsg cmn.ListObjsMsg) (allEntries *cmn.BucketList, err error) {
	if lsmsg.StartAfter != "" {
		return nil, fmt.Errorf("list-objects %q option for remote buckets is not yet supported", lsmsg.StartAfter)
	}
	var (
		smap       = p.owner.smap.get()
		config     = cmn.GCO.Get()
		reqTimeout = config.Client.ListObjects.D()
		aisMsg     = p.newAmsgActVal(cmn.ActList, &lsmsg)
		args       = allocBcArgs()
		results    sliceResults
	)
	args.req = cmn.HreqArgs{
		Method: http.MethodGet,
		Path:   cmn.URLPathBuckets.Join(bck.Name),
		Query:  cmn.AddBckToQuery(nil, bck.Bck),
		Body:   cos.MustMarshal(aisMsg),
	}
	if lsmsg.NeedLocalMD() {
		args.timeout = reqTimeout
		args.smap = smap
		args.fv = func() interface{} { return &cmn.BucketList{} }
		results = p.bcastGroup(args)
	} else {
		nl, exists := p.notifs.entry(lsmsg.UUID)
		debug.Assert(exists) // NOTE: we register listobj xaction before starting to list
		for _, si := range nl.Notifiers() {
			cargs := allocCargs()
			{
				cargs.si = si
				cargs.req = args.req
				cargs.timeout = reqTimeout
				cargs.v = &cmn.BucketList{}
			}
			res := p.call(cargs)
			freeCargs(cargs)
			results = make(sliceResults, 1)
			results[0] = res
			break
		}
	}
	freeBcArgs(args)
	// Combine the results.
	bckLists := make([]*cmn.BucketList, 0, len(results))
	for _, res := range results {
		if res.status == http.StatusNotFound {
			continue
		}
		if res.err != nil {
			err = res.toErr()
			freeBcastRes(results)
			return nil, err
		}
		bckLists = append(bckLists, res.v.(*cmn.BucketList))
	}
	freeBcastRes(results)

	// Maximum objects in the final result page. Take all objects in
	// case of Cloud and no limit is set by a user.
	allEntries = cmn.MergeObjLists(bckLists, 0)
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("Objects after merge: %d, token: %q", len(allEntries.Entries), allEntries.ContinuationToken)
	}

	if lsmsg.WantProp(cmn.GetTargetURL) {
		for _, e := range allEntries.Entries {
			si, err := cluster.HrwTarget(bck.MakeUname(e.Name), &smap.Smap)
			if err == nil {
				e.TargetURL = si.URL(cmn.NetPublic)
			}
		}
	}

	return allEntries, nil
}

func (p *proxy) objMv(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string, msg *cmn.ActionMsg) {
	started := time.Now()
	if objName == msg.Name {
		p.writeErrMsg(w, r, "the new and the current name are the same")
		return
	}
	smap := p.owner.smap.get()
	si, err := cluster.HrwTarget(bck.MakeUname(objName), &smap.Smap)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%q %s/%s => %s", msg.Action, bck.Name, objName, si)
	}

	// NOTE: Code 307 is the only way to http-redirect with the original JSON payload.
	redirectURL := p.redirectURL(r, si, started, cmn.NetIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)

	p.statsT.Add(stats.RenameCount, 1)
}

func (p *proxy) doListRange(method, bucket string, msg *cmn.ActionMsg, query url.Values) (xactID string, err error) {
	var (
		smap   = p.owner.smap.get()
		aisMsg = p.newAmsg(msg, nil, cos.GenUUID())
		body   = cos.MustMarshal(aisMsg)
		path   = cmn.URLPathBuckets.Join(bucket)
	)
	nlb := xact.NewXactNL(aisMsg.UUID, aisMsg.Action, &smap.Smap, nil)
	nlb.SetOwner(equalIC)
	p.ic.registerEqual(regIC{smap: smap, query: query, nl: nlb})
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: method, Path: path, Query: query, Body: body}
	args.smap = smap
	args.timeout = cmn.DefaultTimeout
	results := p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err == nil {
			continue
		}
		err = res.errorf("%s failed to %q List/Range", res.si, msg.Action)
		break
	}
	freeBcastRes(results)
	xactID = aisMsg.UUID
	return
}

func (p *proxy) reverseHandler(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 1, false, cmn.URLPathReverse.L)
	if err != nil {
		return
	}

	// rewrite URL path (removing `cmn.Reverse`)
	r.URL.Path = cos.JoinWords(cmn.Version, apiItems[0])

	nodeID := r.Header.Get(cmn.HdrNodeID)
	if nodeID == "" {
		p.writeErrMsg(w, r, "missing node ID")
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
	nodeURL := r.Header.Get(cmn.HdrNodeURL)
	if nodeURL == "" {
		err = &errNodeNotFound{"cannot rproxy", nodeID, p.si, smap}
		p.writeErr(w, r, err, http.StatusNotFound)
		return
	}

	parsedURL, err := url.Parse(nodeURL)
	if err != nil {
		p.writeErrf(w, r, "%s: invalid URL %q for node %s", p.si, nodeURL, nodeID)
		return
	}

	p.reverseRequest(w, r, nodeID, parsedURL)
}

///////////////////////////
// http /daemon handlers //
///////////////////////////

// [METHOD] /v1/daemon
func (p *proxy) daemonHandler(w http.ResponseWriter, r *http.Request) {
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
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodPost, http.MethodPut)
	}
}

func (p *proxy) handlePendingRenamedLB(renamedBucket string) {
	ctx := &bmdModifier{
		pre:   p._pendingRnPre,
		final: p._syncBMDFinal,
		msg:   &cmn.ActionMsg{Value: cmn.ActMoveBck},
		bcks:  []*cluster.Bck{cluster.NewBck(renamedBucket, cmn.ProviderAIS, cmn.NsGlobal)},
	}
	_, err := p.owner.bmd.modify(ctx)
	debug.AssertNoErr(err)
}

func (p *proxy) _pendingRnPre(ctx *bmdModifier, clone *bucketMD) error {
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

func (p *proxy) httpdaeget(w http.ResponseWriter, r *http.Request) {
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
	case cmn.GetWhatConfig, cmn.GetWhatSmapVote, cmn.GetWhatSnode, cmn.GetWhatLog, cmn.GetWhatStats:
		p.htrun.httpdaeget(w, r)
	case cmn.GetWhatSysInfo:
		p.writeJSON(w, r, sys.FetchSysInfo(), what)
	case cmn.GetWhatSmap:
		const max = 16
		var (
			smap  = p.owner.smap.get()
			sleep = cmn.Timeout.CplaneOperation() / 2
		)
		for i := 0; smap.validate() != nil && i < max; i++ {
			if !p.NodeStarted() {
				time.Sleep(sleep)
				smap = p.owner.smap.get()
				if err := smap.validate(); err != nil {
					glog.Errorf("%s is starting up, cannot return %s yet: %v", p.si, smap, err)
				}
				break
			}
			smap = p.owner.smap.get()
			time.Sleep(sleep)
		}
		if err := smap.validate(); err != nil {
			glog.Errorf("%s: startup is taking unusually long time: %s (%v)", p.si, smap, err)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		p.writeJSON(w, r, smap, what)
	case cmn.GetWhatDaemonStatus:
		msg := &stats.DaemonStatus{
			Snode:       p.htrun.si,
			SmapVersion: p.owner.smap.get().Version,
			SysInfo:     sys.FetchSysInfo(),
			Stats:       p.statsT.CoreStats(),
			DeployedOn:  deploymentType(),
			Version:     daemon.version,
			BuildTime:   daemon.buildTime,
		}

		p.writeJSON(w, r, msg, what)
	default:
		p.htrun.httpdaeget(w, r)
	}
}

func (p *proxy) httpdaeput(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 0, true, cmn.URLPathDae.L)
	if err != nil {
		return
	}
	if err := p.checkACL(w, r, nil, cmn.AceAdmin); err != nil {
		return
	}
	// urlpath-based actions
	if len(apiItems) > 0 {
		action := apiItems[0]
		p.daePathAction(w, r, action)
		return
	}
	// message-based actions
	query := r.URL.Query()
	msg, err := p.readActionMsg(w, r)
	if err != nil {
		return
	}
	switch msg.Action {
	case cmn.ActSetConfig: // set-config #2 - via action message
		p.setDaemonConfigMsg(w, r, msg)
	case cmn.ActResetConfig:
		if err := p.owner.config.resetDaemonConfig(); err != nil {
			p.writeErr(w, r, err)
		}
	case cmn.ActDecommission:
		if !p.ensureIntraControl(w, r, true /* from primary */) {
			return
		}
		p.unreg(msg.Action)
	case cmn.ActShutdown:
		smap := p.owner.smap.get()
		isPrimary := smap.isPrimary(p.si)
		if !isPrimary {
			if !p.ensureIntraControl(w, r, true /* from primary */) {
				return
			}
			writeShutdownMarker()
			p.Stop(&errNoUnregister{msg.Action})
			return
		}
		force := cos.IsParseBool(query.Get(cmn.URLParamForce))
		if !force {
			p.writeErrf(w, r, "cannot shutdown primary %s (consider %s=true option)",
				p.si, cmn.URLParamForce)
			return
		}
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	default:
		p.writeErrAct(w, r, msg.Action)
	}
}

func (p *proxy) daePathAction(w http.ResponseWriter, r *http.Request, action string) {
	switch action {
	case cmn.Proxy:
		p.daeSetPrimary(w, r)
	case cmn.SyncSmap:
		newsmap := &smapX{}
		if cmn.ReadJSON(w, r, newsmap) != nil {
			return
		}
		if err := newsmap.validate(); err != nil {
			p.writeErrf(w, r, "%s: invalid %s: %v", p.si, newsmap, err)
			return
		}
		if err := p.owner.smap.synchronize(p.si, newsmap, nil /*ms payload*/); err != nil {
			p.writeErrf(w, r, cmn.FmtErrFailed, p.si, "sync", newsmap, err)
			return
		}
		glog.Infof("%s: %s %s done", p.si, cmn.SyncSmap, newsmap)
	case cmn.ActSetConfig: // set-config #1 - via query parameters and "?n1=v1&n2=v2..."
		p.setDaemonConfigQuery(w, r)
	default:
		p.writeErrAct(w, r, action)
	}
}

func (p *proxy) httpdaedelete(w http.ResponseWriter, r *http.Request) {
	// the path includes cmn.CallbackRmSelf (compare with t.httpdaedelete)
	_, err := p.checkRESTItems(w, r, 0, false, cmn.URLPathDaeRmSelf.L)
	if err != nil {
		return
	}
	_, action, err := p.parseUnregMsg(w, r)
	if err != nil {
		return
	}
	p.unreg(action)
}

func (p *proxy) unreg(action string) {
	// Stop keepaliving
	p.keepalive.send(kaSuspendMsg)

	// In case of maintenance, we only stop the keepalive daemon,
	// the HTTPServer is still active and accepts requests.
	if action == cmn.ActStartMaintenance {
		return
	}

	if action == cmn.ActDecommission || action == cmn.ActDecommissionNode {
		// When decommissioning always cleanup all system meta-data.
		err := cleanupConfigDir()
		if err != nil {
			glog.Errorf("%s: failed to cleanup config dir, err: %v", p.si, err)
		}
	}
	writeShutdownMarker()
	p.Stop(&errNoUnregister{action})
}

func (p *proxy) httpdaepost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 0, true, cmn.URLPathDae.L)
	if err != nil {
		return
	}
	if len(apiItems) == 0 || apiItems[0] != cmn.AdminJoin {
		p.writeErrURL(w, r)
		return
	}
	if !p.keepalive.paused() {
		glog.Warningf("%s: keepalive is already active - proceeding to resume (and reset) anyway", p.si)
	}
	p.keepalive.send(kaResumeMsg)
	body, err := cmn.ReadBytes(r)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	caller := r.Header.Get(cmn.HdrCallerName)
	if err := p.applyRegMeta(cmn.ActAdminJoinProxy, body, caller); err != nil {
		p.writeErr(w, r, err)
	}
}

func (p *proxy) smapFromURL(baseURL string) (smap *smapX, err error) {
	var (
		req = cmn.HreqArgs{
			Method: http.MethodGet,
			Base:   baseURL,
			Path:   cmn.URLPathDae.S,
			Query:  url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSmap}},
		}
		cargs = allocCargs()
	)
	{
		cargs.req = req
		cargs.timeout = cmn.DefaultTimeout
		cargs.v = &smapX{}
	}
	res := p.call(cargs)
	if res.err != nil {
		err = res.errorf("failed to get Smap from %s", baseURL)
	} else {
		smap = res.v.(*smapX)
		if err = smap.validate(); err != nil {
			err = fmt.Errorf("%s: invalid %s from %s: %v", p.si, smap, baseURL, err)
			smap = nil
		}
	}
	freeCargs(cargs)
	freeCR(res)
	return
}

// forceful primary change - is used when the original primary network is down
// for a while and the remained nodes selected a new primary. After the
// original primary is back it does not attach automatically to the new primary
// and the cluster gets into split-brain mode. This request makes original
// primary connect to the new primary
func (p *proxy) forcefulJoin(w http.ResponseWriter, r *http.Request, proxyID string) {
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
		p.writeErr(w, r, err, http.StatusNotFound)
		return
	}
	if newPrimaryURL == "" {
		newPrimaryURL = psi.IntraControlNet.DirectURL
	}
	if newPrimaryURL == "" {
		err := &errNodeNotFound{"failed to get new primary's direct URL", proxyID, p.si, smap}
		p.writeErr(w, r, err)
		return
	}
	newSmap, err := p.smapFromURL(newPrimaryURL)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	primary := newSmap.Primary
	if proxyID != primary.ID() {
		p.writeErrf(w, r, "%s: proxy %s is not the primary, current %s", p.si, proxyID, newSmap.pp())
		return
	}

	p.metasyncer.becomeNonPrimary() // metasync to stop syncing and cancel all pending requests
	p.owner.smap.put(newSmap)
	res := p.registerToURL(primary.IntraControlNet.DirectURL, primary, cmn.DefaultTimeout, nil, false)
	if res.err != nil {
		p.writeErr(w, r, res.toErr())
	}
}

func (p *proxy) daeSetPrimary(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.checkRESTItems(w, r, 2, false, cmn.URLPathDae.L)
	if err != nil {
		return
	}
	proxyID := apiItems[1]
	query := r.URL.Query()
	force := cos.IsParseBool(query.Get(cmn.URLParamForce))

	// forceful primary change
	if force && apiItems[0] == cmn.Proxy {
		if smap := p.owner.smap.get(); !smap.isPrimary(p.si) {
			p.writeErr(w, r, newErrNotPrimary(p.si, smap))
		}
		p.forcefulJoin(w, r, proxyID)
		return
	}
	prepare, err := cos.ParseBool(query.Get(cmn.URLParamPrepare))
	if err != nil {
		p.writeErrf(w, r, "failed to parse %s URL parameter: %v", cmn.URLParamPrepare, err)
		return
	}
	if p.owner.smap.get().isPrimary(p.si) {
		p.writeErr(w, r,
			errors.New("expecting 'cluster' (RESTful) resource when designating primary proxy via API"))
		return
	}
	if prepare {
		var cluMeta cluMeta
		if err := cmn.ReadJSON(w, r, &cluMeta); err != nil {
			return
		}
		if err := p.receiveCluMeta(&cluMeta, "set-primary", cluMeta.SI.String()); err != nil {
			p.writeErrf(w, r, "failed to receive clu-meta: %v", err)
			return
		}
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
		p.writeErr(w, r, err)
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

func (p *proxy) becomeNewPrimary(proxyIDToRemove string) {
	ctx := &smapModifier{
		pre:   p._becomePre,
		final: p._becomeFinal,
		sid:   proxyIDToRemove,
	}
	err := p.owner.smap.modify(ctx)
	cos.AssertNoErr(err)
}

func (p *proxy) _becomePre(ctx *smapModifier, clone *smapX) error {
	if !clone.isPresent(p.si) {
		cos.Assertf(false, "%s must always be present in the %s", p.si, clone.pp())
	}

	if ctx.sid != "" && clone.containsID(ctx.sid) {
		// decision is made: going ahead to remove
		glog.Infof("%s: removing failed primary %s", p.si, ctx.sid)
		clone.delProxy(ctx.sid)

		// Remove reverse proxy entry for the node.
		p.rproxy.nodes.Delete(ctx.sid)
	}

	clone.Primary = clone.GetProxy(p.si.ID())
	clone.Version += 100
	clone.staffIC()
	return nil
}

func (p *proxy) _becomeFinal(ctx *smapModifier, clone *smapX) {
	var (
		bmd   = p.owner.bmd.get()
		rmd   = p.owner.rmd.get()
		msg   = p.newAmsgStr(cmn.ActNewPrimary, bmd)
		pairs = []revsPair{{clone, msg}, {bmd, msg}, {rmd, msg}}
	)
	glog.Infof("%s: distributing (%s, %s, %s) with newly elected primary (self)", p.si, clone, bmd, rmd)
	config, err := p.ensureConfigPrimaryURL()
	if err != nil {
		glog.Error(err)
	}
	if config != nil {
		pairs = append(pairs, revsPair{config, msg})
		glog.Infof("%s: plus %s", p.si, config)
	}
	etl := p.owner.etl.get()
	if etl != nil && etl.version() > 0 {
		pairs = append(pairs, revsPair{etl, msg})
		glog.Infof("%s: plus %s", p.si, etl)
	}
	debug.Assert(clone._sgl != nil)
	_ = p.metasyncer.sync(pairs...)
	p.syncNewICOwners(ctx.smap, clone)
}

func (p *proxy) ensureConfigPrimaryURL() (config *globalConfig, err error) {
	config, err = p.owner.config.modify(&configModifier{pre: p._primaryURLPre})
	if err != nil {
		err = fmt.Errorf("failed to update primary URL, err: %v", err)
	}
	return
}

func (p *proxy) _primaryURLPre(_ *configModifier, clone *globalConfig) (updated bool, err error) {
	smap := p.owner.smap.get()
	debug.Assert(smap.isPrimary(p.si))
	if newURL := smap.Primary.URL(cmn.NetPublic); clone.Proxy.PrimaryURL != newURL {
		clone.Proxy.PrimaryURL = smap.Primary.URL(cmn.NetPublic)
		updated = true
	}
	return
}

// [METHOD] /v1/tokens
func (p *proxy) tokenHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodDelete:
		p.httpTokenDelete(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete)
	}
}

// [METHOD] /v1/dsort
func (p *proxy) dsortHandler(w http.ResponseWriter, r *http.Request) {
	if !p.ClusterStartedWithRetry() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if err := p.checkACL(w, r, nil, cmn.AceAdmin); err != nil {
		return
	}

	apiItems, err := cmn.MatchRESTItems(r.URL.Path, 0, true, cmn.URLPathdSort.L)
	if err != nil {
		p.writeErrURL(w, r)
		return
	}

	switch r.Method {
	case http.MethodPost:
		p.proxyStartSortHandler(w, r)
	case http.MethodGet:
		dsort.ProxyGetHandler(w, r)
	case http.MethodDelete:
		if len(apiItems) == 1 && apiItems[0] == cmn.Abort {
			dsort.ProxyAbortSortHandler(w, r)
		} else if len(apiItems) == 0 {
			dsort.ProxyRemoveSortHandler(w, r)
		} else {
			p.writeErrURL(w, r)
		}
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodPost)
	}
}

// http reverse-proxy handler, to handle unmodified requests
// (not to confuse with p.rproxy)
func (p *proxy) httpCloudHandler(w http.ResponseWriter, r *http.Request) {
	if !p.ClusterStartedWithRetry() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if r.URL.Scheme == "" {
		p.writeErrURL(w, r)
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
		r.URL.Path = cmn.URLPathObjects.Join(hbo.Bck.Name, hbo.ObjName)
		r.URL.RawQuery = q.Encode()
		if r.Method == http.MethodGet {
			p.httpobjget(w, r, hbo.OrigURLBck)
		} else {
			p.httpobjhead(w, r, hbo.OrigURLBck)
		}
		return
	}
	p.writeErrf(w, r, "%q provider doesn't support %q", cmn.ProviderHTTP, r.Method)
}

/////////////////
// METASYNC RX //
/////////////////

func (p *proxy) receiveRMD(newRMD *rebMD, msg *aisMsg, caller string) (err error) {
	rmd := p.owner.rmd.get()
	glog.Infof("receive %s%s", newRMD, _msdetail(rmd.Version, msg, caller))
	p.owner.rmd.Lock()
	rmd = p.owner.rmd.get()
	if newRMD.version() <= rmd.version() {
		p.owner.rmd.Unlock()
		if newRMD.version() < rmd.version() {
			err = newErrDowngrade(p.si, rmd.String(), newRMD.String())
		}
		return
	}
	p.owner.rmd.put(newRMD)
	p.owner.rmd.Unlock()

	// Register `nl` for rebalance/resilver
	smap := p.owner.smap.get()
	if smap.IsIC(p.si) && smap.CountActiveTargets() > 0 {
		nl := xact.NewXactNL(xact.RebID2S(newRMD.Version), cmn.ActRebalance, &smap.Smap, nil)
		nl.SetOwner(equalIC)
		err := p.notifs.add(nl)
		cos.AssertNoErr(err)

		if newRMD.Resilver != "" {
			nl = xact.NewXactNL(newRMD.Resilver, cmn.ActResilver, &smap.Smap, nil)
			nl.SetOwner(equalIC)
			err := p.notifs.add(nl)
			cos.AssertNoErr(err)
		}
	}
	return
}

func (p *proxy) smapOnUpdate(newSmap, oldSmap *smapX) {
	// When some node was removed from the cluster we need to clean up the
	// reverse proxy structure.
	p.rproxy.nodes.Range(func(key, _ interface{}) bool {
		nodeID := key.(string)
		if oldSmap.containsID(nodeID) && !newSmap.containsID(nodeID) {
			p.rproxy.nodes.Delete(nodeID)
		}
		return true
	})
	p.syncNewICOwners(oldSmap, newSmap)
}

func (p *proxy) receiveBMD(newBMD *bucketMD, msg *aisMsg, payload msPayload, caller string) (err error) {
	bmd := p.owner.bmd.get()
	glog.Infof("receive %s%s", newBMD, _msdetail(bmd.Version, msg, caller))

	p.owner.bmd.Lock()
	bmd = p.owner.bmd.get()
	if err = bmd.validateUUID(newBMD, p.si, nil, caller); err != nil {
		cos.Assert(!p.owner.smap.get().isPrimary(p.si))
		// cluster integrity error: making exception for non-primary proxies
		glog.Errorf("%s (non-primary): %v - proceeding to override BMD", p.si, err)
	} else if newBMD.version() <= bmd.version() {
		p.owner.bmd.Unlock()
		return newErrDowngrade(p.si, bmd.String(), newBMD.String())
	}
	err = p.owner.bmd.putPersist(newBMD, payload)
	debug.AssertNoErr(err)
	p.owner.bmd.Unlock()
	return
}

// detectDaemonDuplicate queries osi for its daemon info in order to determine if info has changed
// and is equal to nsi
func (p *proxy) detectDaemonDuplicate(osi, nsi *cluster.Snode) (bool, error) {
	si, err := p.getDaemonInfo(osi)
	if err != nil {
		return false, err
	}
	return !nsi.Equals(si), nil
}

// getDaemonInfo queries osi for its daemon info and returns it.
func (p *proxy) getDaemonInfo(osi *cluster.Snode) (si *cluster.Snode, err error) {
	cargs := allocCargs()
	{
		cargs.si = osi
		cargs.req = cmn.HreqArgs{
			Method: http.MethodGet,
			Path:   cmn.URLPathDae.S,
			Query:  url.Values{cmn.URLParamWhat: []string{cmn.GetWhatSnode}},
		}
		cargs.timeout = cmn.Timeout.CplaneOperation()
		cargs.v = &cluster.Snode{}
	}
	res := p.call(cargs)
	if res.err != nil {
		err = res.err
	} else {
		si = res.v.(*cluster.Snode)
	}
	freeCargs(cargs)
	freeCR(res)
	return
}

func (p *proxy) headRemoteBck(bck cmn.Bck, q url.Values) (header http.Header, statusCode int, err error) {
	var (
		tsi  *cluster.Snode
		path = cmn.URLPathBuckets.Join(bck.Name)
	)
	if tsi, err = p.owner.smap.get().GetRandTarget(); err != nil {
		return
	}
	if bck.IsCloud() {
		config := cmn.GCO.Get()
		if _, ok := config.Backend.Providers[bck.Provider]; !ok {
			err = &cmn.ErrMissingBackend{Provider: bck.Provider}
			err = fmt.Errorf("cannot lookup cloud bucket %q: %v", bck, err)
			statusCode = http.StatusNotFound
			return
		}
	}
	q = cmn.AddBckToQuery(q, bck)
	cargs := allocCargs()
	{
		cargs.si = tsi
		cargs.req = cmn.HreqArgs{Method: http.MethodHead, Base: tsi.URL(cmn.NetIntraData), Path: path, Query: q}
		cargs.timeout = cmn.DefaultTimeout
	}
	res := p.call(cargs)
	if res.status == http.StatusNotFound {
		err = cmn.NewErrRemoteBckNotFound(bck)
	} else if res.status == http.StatusGone {
		err = cmn.NewErrRemoteBckOffline(bck)
	} else {
		err = res.err
		header = res.header
	}
	statusCode = res.status
	freeCargs(cargs)
	freeCR(res)
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

func (rp *reverseProxy) loadOrStore(uuid string, u *url.URL,
	errHdlr func(w http.ResponseWriter, r *http.Request, err error)) *httputil.ReverseProxy {
	revProxyIf, exists := rp.nodes.Load(uuid)
	if exists {
		shrp := revProxyIf.(*singleRProxy)
		if shrp.u.Host == u.Host {
			return shrp.rp
		}
	}
	cfg := cmn.GCO.Get()
	rproxy := httputil.NewSingleHostReverseProxy(u)
	rproxy.Transport = cmn.NewTransport(cmn.TransportArgs{
		UseHTTPS:   cfg.Net.HTTP.UseHTTPS,
		SkipVerify: cfg.Net.HTTP.SkipVerify,
	})
	rproxy.ErrorHandler = errHdlr
	// NOTE: races are rare probably happen only when storing an entry for the first time or when URL changes.
	// Also, races don't impact the correctness as we always have latest entry for `uuid`, `URL` pair (see: L3917).
	rp.nodes.Store(uuid, &singleRProxy{rproxy, u})
	return rproxy
}

////////////////
// misc utils //
////////////////

func resolveUUIDBMD(bmds bmds) (*bucketMD, error) {
	var (
		mlist = make(map[string][]cluMeta) // uuid => list(targetRegMeta)
		maxor = make(map[string]*bucketMD) // uuid => max-ver BMD
	)
	// results => (mlist, maxor)
	for si, bmd := range bmds {
		if bmd.Version == 0 {
			continue
		}
		mlist[bmd.UUID] = append(mlist[bmd.UUID], cluMeta{BMD: bmd, SI: si})

		if rbmd, ok := maxor[bmd.UUID]; !ok {
			maxor[bmd.UUID] = bmd
		} else if rbmd.Version < bmd.Version {
			maxor[bmd.UUID] = bmd
		}
	}
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
			s := fmt.Sprintf("%s: BMDs have different UUIDs with no simple majority:\n%v",
				ciError(60), mlist)
			return nil, &errBmdUUIDSplit{s}
		}
	}
	var err error
	if len(mlist) > 1 {
		s := fmt.Sprintf("%s: BMDs have different UUIDs with simple majority: %s:\n%v",
			ciError(70), uuid, mlist)
		err = &errTgtBmdUUIDDiffer{s}
	}
	bmd := maxor[uuid]
	cos.Assert(cos.IsValidUUID(bmd.UUID))
	return bmd, err
}

func ciError(num int) string {
	return fmt.Sprintf(cmn.FmtErrIntegrity, ciePrefix, num, cmn.GitHubHome)
}
