// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
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
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/ext/dsort"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"
	jsoniter "github.com/json-iterator/go"
)

const (
	fmtNotRemote = "%q is AIS (ais://) bucket - expecting remote bucket"
	lsotag       = "list-objects"
)

type (
	ClusterMountpathsRaw struct {
		Targets cos.JSONRawMsgs `json:"targets"`
	}
	reverseProxy struct {
		cloud   *httputil.ReverseProxy // unmodified GET requests => storage.googleapis.com
		nodes   sync.Map               // map of reverse proxies keyed by node DaemonIDs
		primary struct {
			rp  *httputil.ReverseProxy
			url string
			sync.Mutex
		}
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
		ic         ic
		qm         lsobjMem
		rproxy     reverseProxy
		notifs     notifs
		reg        struct {
			pool nodeRegPool
			mu   sync.RWMutex
		}
		remais struct {
			cluster.Remotes
			old []*cluster.RemAis // to facilitate a2u resultion (and, therefore, offline access)
			mu  sync.RWMutex
			in  atomic.Bool
		}
		inPrimaryTransition atomic.Bool
	}
)

// interface guard
var _ cos.Runner = (*proxy)(nil)

func (*proxy) Name() string     { return apc.Proxy } // as cos.Runner
func (p *proxy) String() string { return p.si.String() }

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
	p.si.Init(initPID(config), apc.Proxy)

	memsys.Init(p.si.ID(), p.si.ID(), config)

	cos.InitShortID(p.si.Digest())

	p.initClusterCIDR()
	daemon.rg.add(p)

	ps := &stats.Prunner{}
	startedUp := ps.Init(p)
	daemon.rg.add(ps)
	p.statsT = ps

	k := newPalive(p, ps, startedUp)
	daemon.rg.add(k)
	p.keepalive = k

	m := newMetasyncer(p)
	daemon.rg.add(m)
	p.metasyncer = m
}

func initPID(config *cmn.Config) (pid string) {
	if pid = envDaemonID(apc.Proxy); pid != "" {
		if err := cos.ValidateDaemonID(pid); err != nil {
			glog.Errorf("Warning: %v", err)
		}
		return
	}
	// try to read ID
	if pid = readProxyID(config); pid != "" {
		glog.Infof("p[%s] from %q", pid, fname.ProxyID)
		return
	}
	pid = genDaemonID(apc.Proxy, config)
	err := cos.ValidateDaemonID(pid)
	debug.AssertNoErr(err)

	// store ID on disk
	err = os.WriteFile(filepath.Join(config.ConfigDir, fname.ProxyID), []byte(pid), cos.PermRWR)
	debug.AssertNoErr(err)
	glog.Infof("p[%s] ID randomly generated", pid)
	return
}

func readProxyID(config *cmn.Config) (id string) {
	if b, err := os.ReadFile(filepath.Join(config.ConfigDir, fname.ProxyID)); err == nil {
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
	p.owner.bmd = newBMDOwnerPrx(config)
	p.owner.etl = newEtlMDOwnerPrx(config)

	p.owner.bmd.init() // initialize owner and load BMD
	p.owner.etl.init() // initialize owner and load EtlMD

	cluster.Init(nil /*cluster.Target*/)

	p.statsT.RegMetrics(p.si) // reg target metrics to common; init Prometheus if used

	// startup sequence - see earlystart.go for the steps and commentary
	p.bootstrap()

	p.authn = newAuthManager()

	p.rproxy.init()

	p.notifs.init(p)
	p.ic.init(p)
	p.qm.init()

	//
	// REST API: register proxy handlers and start listening
	//
	networkHandlers := []networkHandler{
		{r: apc.Reverse, h: p.reverseHandler, net: accessNetPublic},

		// pubnet handlers: cluster must be started
		{r: apc.Buckets, h: p.bucketHandler, net: accessNetPublic},
		{r: apc.Objects, h: p.objectHandler, net: accessNetPublic},
		{r: apc.Download, h: p.downloadHandler, net: accessNetPublic},
		{r: apc.ETL, h: p.etlHandler, net: accessNetPublic},
		{r: apc.Sort, h: p.dsortHandler, net: accessNetPublic},

		{r: apc.IC, h: p.ic.handler, net: accessNetIntraControl},
		{r: apc.Daemon, h: p.daemonHandler, net: accessNetPublicControl},
		{r: apc.Cluster, h: p.clusterHandler, net: accessNetPublicControl},
		{r: apc.Tokens, h: p.tokenHandler, net: accessNetPublic},

		{r: apc.Metasync, h: p.metasyncHandler, net: accessNetIntraControl},
		{r: apc.Health, h: p.healthHandler, net: accessNetPublicControl},
		{r: apc.Vote, h: p.voteHandler, net: accessNetIntraControl},

		{r: apc.Notifs, h: p.notifs.handler, net: accessNetIntraControl},

		// S3 compatibility
		{r: "/" + apc.S3, h: p.s3Handler, net: accessNetPublic},

		// "easy URL"
		{r: "/" + apc.GSScheme, h: p.easyURLHandler, net: accessNetPublic},
		{r: "/" + apc.AZScheme, h: p.easyURLHandler, net: accessNetPublic},
		{r: "/" + apc.AISScheme, h: p.easyURLHandler, net: accessNetPublic},

		// ht:// _or_ S3 compatibility, depending on feature flag
		{r: "/", h: p.rootHandler, net: accessNetPublic},
	}
	p.registerNetworkHandlers(networkHandlers)

	glog.Infof("%s: [%s net] listening on: %s", p, cmn.NetPublic, p.si.PubNet.URL)
	if p.si.PubNet.URL != p.si.ControlNet.URL {
		glog.Infof("%s: [%s net] listening on: %s", p, cmn.NetIntraControl, p.si.ControlNet.URL)
	}
	if p.si.PubNet.URL != p.si.DataNet.URL {
		glog.Infof("%s: [%s net] listening on: %s", p, cmn.NetIntraData, p.si.DataNet.URL)
	}

	dsort.RegisterNode(p.owner.smap, p.owner.bmd, p.si, nil, p.statsT)
	return p.htrun.run()
}

func (p *proxy) joinCluster(action string, primaryURLs ...string) (status int, err error) {
	var query url.Values
	if smap := p.owner.smap.get(); smap.isPrimary(p.si) {
		return 0, fmt.Errorf("%s should not be joining: is primary, %s", p, smap.StringEx())
	}
	if cmn.GCO.Get().Proxy.NonElectable {
		query = url.Values{apc.QparamNonElectable: []string{"true"}}
	}
	res := p.join(query, nil /*htext*/, primaryURLs...)
	defer freeCR(res)
	if res.err != nil {
		status, err = res.status, res.err
		return
	}
	// not being sent at cluster startup and keepalive
	if len(res.bytes) == 0 {
		return
	}
	err = p.recvCluMetaBytes(action, res.bytes, "")
	return
}

func (p *proxy) recvCluMetaBytes(action string, body []byte, caller string) error {
	var cm cluMeta
	if err := jsoniter.Unmarshal(body, &cm); err != nil {
		return fmt.Errorf(cmn.FmtErrUnmarshal, p, "reg-meta", cos.BHead(body), err)
	}
	return p.recvCluMeta(&cm, action, caller)
}

// TODO: unify w/ t.recvCluMetaBytes
func (p *proxy) recvCluMeta(cm *cluMeta, action, caller string) error {
	var (
		msg  = p.newAmsgStr(action, cm.BMD)
		errs []error
	)
	// Config
	debug.Assert(cm.Config != nil)
	if err := p.receiveConfig(cm.Config, msg, nil, caller); err != nil {
		if !isErrDowngrade(err) {
			errs = append(errs, err)
			glog.Error(err)
		}
	} else {
		glog.Infof("%s: recv-clumeta %s %s", p, action, cm.Config)
	}
	// Smap
	if err := p.receiveSmap(cm.Smap, msg, nil /*ms payload*/, caller, p.smapOnUpdate); err != nil {
		if !isErrDowngrade(err) {
			errs = append(errs, err)
			glog.Error(err)
		}
	} else if cm.Smap != nil {
		glog.Infof("%s: recv-clumeta %s %s", p, action, cm.Smap)
	}
	// BMD
	if err := p.receiveBMD(cm.BMD, msg, nil, caller); err != nil {
		if !isErrDowngrade(err) {
			errs = append(errs, err)
			glog.Error(err)
		}
	} else {
		glog.Infof("%s: recv-clumeta %s %s", p, action, cm.BMD)
	}
	// RMD
	if err := p.receiveRMD(cm.RMD, msg, caller); err != nil {
		if !isErrDowngrade(err) {
			errs = append(errs, err)
			glog.Error(err)
		}
	} else {
		glog.Infof("%s: recv-clumeta %s %s", p, action, cm.RMD)
	}
	// EtlMD
	if err := p.receiveEtlMD(cm.EtlMD, msg, nil, caller, nil); err != nil {
		if !isErrDowngrade(err) {
			errs = append(errs, err)
			glog.Error(err)
		}
	} else if cm.EtlMD != nil {
		glog.Infof("%s: recv-clumeta %s %s", p, action, cm.EtlMD)
	}

	switch {
	case errs == nil:
		return nil
	case len(errs) == 1:
		return errs[0]
	default:
		s := fmt.Sprintf("%v", errs)
		return cmn.NewErrFailedTo(p, action, "clumeta", errors.New(s))
	}
}

// stop proxy runner and return => rungroup.run
// TODO: write shutdown-marker
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

// parse request + init/lookup bucket (combo)
func (p *proxy) _parseReqTry(w http.ResponseWriter, r *http.Request, bckArgs *bckInitArgs) (bck *cluster.Bck,
	objName string, err error) {
	apireq := apiReqAlloc(2, apc.URLPathObjects.L, false /*dpq*/)
	if err = p.parseReq(w, r, apireq); err != nil {
		apiReqFree(apireq)
		return
	}
	bckArgs.bck, bckArgs.query = apireq.bck, apireq.query
	bck, err = bckArgs.initAndTry()
	objName = apireq.items[1]

	apiReqFree(apireq)
	freeInitBckArgs(bckArgs) // caller does alloc
	return
}

// verb /v1/buckets/
func (p *proxy) bucketHandler(w http.ResponseWriter, r *http.Request) {
	if !p.cluStartedWithRetry() {
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
//	/gs/mybucket/myobject   - to access Google Cloud buckets
//	/az/mybucket/myobject   - Azure Blob Storage
//	/ais/mybucket/myobject  - AIS
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
//
//	Amazon S3 is missing in the list that includes GCP and Azure. The reason
//	for this is that AIS provides S3 compatibility layer via its "/s3" endpoint.
//	S3 compatibility (see https://github.com/NVIDIA/aistore/blob/master/docs/s3compat.md)
//	shall not be confused with a simple alternative URL Path mapping via easyURLHandler,
//	whereby a path (e.g.) "gs/mybucket/myobject" gets replaced with
//	"v1/objects/mybucket/myobject?provider=gcp" with _no_ other changes to the request
//	and response parameters and components.
func (p *proxy) easyURLHandler(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.apiItems(w, r, 1, true, nil)
	if err != nil {
		return
	}
	provider := apiItems[0]
	if provider, err = cmn.NormalizeProvider(provider); err != nil {
		p.writeErr(w, r, err)
		return
	}
	// num items: 1
	if len(apiItems) == 1 {
		// list buckets for a given provider
		// NOTE two differences between this implementation and `p.bckNamesFromBMD` (s3 API):
		// - `/s3` is an API endpoint rather than a namesake provider
		//   (the API must "cover" all providers)
		// - `/s3` and its subordinate URL paths can only "see" buckets that are already present
		//   in the BMD, while native API, when given sufficient permissions, can immediately
		//   access (read, write, list) any remote buckets, while adding them to the BMD "on the fly".
		r.URL.Path = apc.URLPathBuckets.S
		if r.URL.RawQuery == "" {
			qbck := cmn.QueryBcks{Provider: provider}
			query := qbck.AddToQuery(nil)
			r.URL.RawQuery = query.Encode()
		} else if !strings.Contains(r.URL.RawQuery, apc.QparamProvider) {
			r.URL.RawQuery += "&" + apc.QparamProvider + "=" + provider
		}
		p.bucketHandler(w, r)
		return
	}
	// num items: 2
	bucket := apiItems[1]
	bck := cmn.Bck{Name: bucket, Provider: provider}
	if err := bck.ValidateName(); err != nil {
		p.writeErr(w, r, err)
		return
	}

	var objName string
	if len(apiItems) > 2 {
		// num items: 3
		objName = apiItems[2]
		r.URL.Path = apc.URLPathObjects.Join(bucket, objName)
		r.URL.Path += path.Join(apiItems[3:]...)
	} else {
		if r.Method == http.MethodPut {
			p.writeErrMsg(w, r, "missing destination object name in the \"easy URL\"")
			return
		}
		r.URL.Path = apc.URLPathBuckets.Join(bucket)
	}

	if r.URL.RawQuery == "" {
		query := bck.AddToQuery(nil)
		r.URL.RawQuery = query.Encode()
	} else if !strings.Contains(r.URL.RawQuery, apc.QparamProvider) {
		r.URL.RawQuery += "&" + apc.QparamProvider + "=" + bck.Provider
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
		msg     *apc.ActMsg
		bckName string
		qbck    *cmn.QueryBcks
	)
	apiItems, err := p.apiItems(w, r, 0, true, apc.URLPathBuckets.L)
	if err != nil {
		return
	}
	if len(apiItems) > 0 {
		bckName = apiItems[0]
	}
	ctype := r.Header.Get(cos.HdrContentType)
	if r.ContentLength == 0 && !strings.HasPrefix(ctype, cos.ContentJSON) {
		// must be an "easy URL" request, e.g.: curl -L -X GET 'http://aistore/ais/abc'
		msg = &apc.ActMsg{Action: apc.ActList, Value: &apc.LsoMsg{}}
	} else if msg, err = p.readActionMsg(w, r); err != nil {
		return
	}
	dpq := dpqAlloc()
	defer dpqFree(dpq)
	if err := dpq.fromRawQ(r.URL.RawQuery); err != nil {
		p.writeErr(w, r, err)
		return
	}
	if qbck, err = newQbckFromQ(bckName, nil, dpq); err != nil {
		p.writeErr(w, r, err)
		return
	}
	switch msg.Action {
	case apc.ActList:
		// list buckets if `qbck` indicates a bucket-type query
		// (see api.ListBuckets and the line below)
		if !qbck.IsBucket() {
			qbck.Name = msg.Name
			if qbck.IsRemoteAIS() {
				qbck.Ns.UUID = p.a2u(qbck.Ns.UUID)
			}
			if err := p.checkAccess(w, r, nil, apc.AceListBuckets); err == nil {
				p.listBuckets(w, r, qbck, msg, dpq)
			}
			return
		}

		// list objects
		// NOTE #1: TODO: currently, always primary - hrw redirect vs scenarios***
		if p.forwardCP(w, r, msg, lsotag+" "+qbck.String()) {
			return
		}
		var (
			err   error
			lsmsg apc.LsoMsg
			bck   = cluster.CloneBck((*cmn.Bck)(qbck))
		)
		if err = cos.MorphMarshal(msg.Value, &lsmsg); err != nil {
			p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
			return
		}
		bckArgs := bckInitArgs{p: p, w: w, r: r, msg: msg, perms: apc.AceObjLIST, bck: bck, dpq: dpq}
		bckArgs.createAIS = false

		// mutually exclusive
		debug.Assert(!(lsmsg.IsFlagSet(apc.LsDontHeadRemote) && lsmsg.IsFlagSet(apc.LsTryHeadRemote)))
		//
		// TODO -- FIXME: the capability to list remote buckets without adding them to BMD
		//
		debug.Assert(!lsmsg.IsFlagSet(apc.LsDontAddRemote), "not implemented yet")

		if bckArgs.dontHeadRemote = lsmsg.IsFlagSet(apc.LsDontHeadRemote); !bckArgs.dontHeadRemote {
			bckArgs.tryHeadRemote = lsmsg.IsFlagSet(apc.LsTryHeadRemote)
		}
		if bck, err = bckArgs.initAndTry(); err == nil {
			begin := mono.NanoTime()
			p.listObjects(w, r, bck, msg /*amsg*/, &lsmsg, begin)
		}
	case apc.ActSummaryBck:
		p.bucketSummary(w, r, qbck, msg, dpq)
	default:
		p.writeErrAct(w, r, msg.Action)
	}
}

// GET /v1/objects/bucket-name/object-name
func (p *proxy) httpobjget(w http.ResponseWriter, r *http.Request, origURLBck ...string) {
	// 1. request
	apireq := apiReqAlloc(2, apc.URLPathObjects.L, true /*dpq*/)
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
		bckArgs.perms = apc.AceGET
		bckArgs.createAIS = false
	}
	if len(origURLBck) > 0 {
		bckArgs.origURLBck = origURLBck[0]
	}
	bck, err := bckArgs.initAndTry()
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
		glog.Infof("%s %s => %s", r.Method, bck.Cname(objName), si)
	}
	redirectURL := p.redirectURL(r, si, time.Now() /*started*/, cmn.NetIntraData)
	http.Redirect(w, r, redirectURL, http.StatusMovedPermanently)

	// 4. stats
	p.statsT.Inc(stats.GetCount)
}

// PUT /v1/objects/bucket-name/object-name
func (p *proxy) httpobjput(w http.ResponseWriter, r *http.Request) {
	var (
		nodeID string
		perms  apc.AccessAttrs
	)
	// 1. request
	apireq := apiReqAlloc(2, apc.URLPathObjects.L, true /*dpq*/)
	if err := p.parseReq(w, r, apireq); err != nil {
		apiReqFree(apireq)
		return
	}
	appendTyProvided := apireq.dpq.appendTy != "" // apc.QparamAppendType
	if !appendTyProvided {
		perms = apc.AcePUT
	} else {
		hi, err := parseAppendHandle(apireq.dpq.appendHdl) // apc.QparamAppendHandle
		if err != nil {
			apiReqFree(apireq)
			p.writeErr(w, r, err)
			return
		}
		nodeID = hi.nodeID
		perms = apc.AceAPPEND
	}

	// 2. bucket
	bckArgs := allocInitBckArgs()
	{
		bckArgs.p = p
		bckArgs.w = w
		bckArgs.r = r
		bckArgs.perms = perms
		bckArgs.createAIS = false
	}
	bckArgs.bck, bckArgs.dpq = apireq.bck, apireq.dpq
	bck, err := bckArgs.initAndTry()
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
		glog.Infof("%s %s => %s (append: %v)", r.Method, bck.Cname(objName), si, appendTyProvided)
	}
	redirectURL := p.redirectURL(r, si, started, cmn.NetIntraData)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)

	// 4. stats
	if !appendTyProvided {
		p.statsT.Inc(stats.PutCount)
	} else {
		p.statsT.Inc(stats.AppendCount)
	}
}

// DELETE /v1/objects/bucket-name/object-name
func (p *proxy) httpobjdelete(w http.ResponseWriter, r *http.Request) {
	bckArgs := allocInitBckArgs()
	{
		bckArgs.p = p
		bckArgs.w = w
		bckArgs.r = r
		bckArgs.perms = apc.AceObjDELETE
		bckArgs.createAIS = false
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
		glog.Infof("%s %s => %s", r.Method, bck.Cname(objName), si)
	}
	redirectURL := p.redirectURL(r, si, time.Now() /*started*/, cmn.NetIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)

	p.statsT.Inc(stats.DeleteCount)
}

// DELETE { action } /v1/buckets
func (p *proxy) httpbckdelete(w http.ResponseWriter, r *http.Request) {
	// 1. request
	apireq := apiReqAlloc(1, apc.URLPathBuckets.L, false /*dpq*/)
	defer apiReqFree(apireq)
	if err := p.parseReq(w, r, apireq); err != nil {
		return
	}
	msg, err := p.readActionMsg(w, r)
	if err != nil {
		return
	}
	perms := apc.AceDestroyBucket
	if msg.Action == apc.ActDeleteObjects || msg.Action == apc.ActEvictObjects {
		perms = apc.AceObjDELETE
	}

	// 2. bucket
	bck := apireq.bck
	bckArgs := bckInitArgs{p: p, w: w, r: r, msg: msg, perms: perms, bck: bck, dpq: apireq.dpq, query: apireq.query}
	bckArgs.createAIS = false
	if msg.Action == apc.ActEvictRemoteBck {
		var errCode int
		bckArgs.dontHeadRemote = true // unconditionally
		errCode, err = bckArgs.init()
		if err != nil {
			if errCode != http.StatusNotFound && !cmn.IsErrRemoteBckNotFound(err) {
				p.writeErr(w, r, err, errCode)
			}
			return
		}
	} else if bck, err = bckArgs.initAndTry(); err != nil {
		return
	}

	// 3. action
	switch msg.Action {
	case apc.ActEvictRemoteBck:
		if !bck.IsRemote() {
			p.writeErrf(w, r, fmtNotRemote, bck.Name)
			return
		}
		keepMD := cos.IsParseBool(apireq.query.Get(apc.QparamKeepRemote))
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
	case apc.ActDestroyBck:
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
			p.reverseReqRemote(w, r, msg, bck.Bucket(), apireq.query)
			return
		}
		if err := p.destroyBucket(msg, bck); err != nil {
			if cmn.IsErrBckNotFound(err) {
				glog.Infof("%s: %s already %q-ed, nothing to do", p, bck, msg.Action)
			} else {
				p.writeErr(w, r, err)
			}
		}
	case apc.ActDeleteObjects, apc.ActEvictObjects:
		var xid string
		if msg.Action == apc.ActEvictObjects && bck.IsAIS() {
			p.writeErrf(w, r, fmtNotRemote, bck.Name)
			return
		}
		if xid, err = p.listrange(r.Method, bck.Name, msg, apireq.query); err != nil {
			p.writeErr(w, r, err)
			return
		}
		w.Write([]byte(xid))
	default:
		p.writeErrAct(w, r, msg.Action)
	}
}

// PUT /v1/metasync
// (compare with p.recvCluMeta and t.metasyncHandlerPut)
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
			err.Message = fmt.Sprintf("%s: %s [%s, %s]", p, txt, smap, xctn)
		} else {
			err.Message = fmt.Sprintf("%s: %s, %s", p, txt, smap)
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
		caller                       = r.Header.Get(apc.HdrCallerName)
		newConf, msgConf, errConf    = p.extractConfig(payload, caller)
		newSmap, msgSmap, errSmap    = p.extractSmap(payload, caller, false /*skip validation*/)
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
		_ = p.authn.updateRevokedList(revokedTokens)
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
					glog.Errorf("%s: failed to send ownership table to %s, err:%v", p, psi, err)
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

	p.uptime2hdr(w.Header())

	query := r.URL.Query()
	prr := cos.IsParseBool(query.Get(apc.QparamPrimaryReadyReb))
	if !prr {
		if responded := p.healthByExternalWD(w, r); responded {
			return
		}
	}
	// piggy-backing cluster info on health
	getCii := cos.IsParseBool(query.Get(apc.QparamClusterInfo))
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
				err := fmt.Errorf(fmtErrPrimaryNotReadyYet, p, a, b)
				if glog.FastV(4, glog.SmoduleAIS) {
					p.writeErr(w, r, err, http.StatusServiceUnavailable)
				} else {
					p.writeErr(w, r, err, http.StatusServiceUnavailable, Silent)
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
	if prr || cos.IsParseBool(query.Get(apc.QparamAskPrimary)) {
		caller := r.Header.Get(apc.HdrCallerName)
		p.writeErrf(w, r, "%s (non-primary): misdirected health-of-primary request from %s, %s",
			p.si, caller, smap.StringEx())
		return
	}
	w.WriteHeader(http.StatusOK)
}

// PUT { action } /v1/buckets/bucket-name
func (p *proxy) httpbckput(w http.ResponseWriter, r *http.Request) {
	var (
		msg           *apc.ActMsg
		query         = r.URL.Query()
		apiItems, err = p.apiItems(w, r, 1, true, apc.URLPathBuckets.L)
	)
	if err != nil {
		return
	}
	bucket := apiItems[0]
	bck, err := newBckFromQ(bucket, query, nil)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	if msg, err = p.readActionMsg(w, r); err != nil {
		return
	}
	bckArgs := bckInitArgs{p: p, w: w, r: r, bck: bck, msg: msg, query: query}
	bckArgs.createAIS = false
	if bck, err = bckArgs.initAndTry(); err != nil {
		return
	}
	switch msg.Action {
	case apc.ActArchive:
		var (
			bckFrom = bck
			archMsg = &cmn.ArchiveMsg{}
		)
		if err := cos.MorphMarshal(msg.Value, archMsg); err != nil {
			p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
			return
		}
		bckTo := cluster.CloneBck(&archMsg.ToBck)
		if bckTo.IsEmpty() {
			bckTo = bckFrom
		} else {
			bckToArgs := bckInitArgs{p: p, w: w, r: r, bck: bckTo, msg: msg, perms: apc.AcePUT, query: query}
			bckToArgs.createAIS = false
			if bckTo, err = bckToArgs.initAndTry(); err != nil {
				p.writeErr(w, r, err)
				return
			}
		}
		if _, err := cos.Mime(archMsg.Mime, archMsg.ArchName); err != nil {
			p.writeErr(w, r, err)
			return
		}
		xid, err := p.createArchMultiObj(bckFrom, bckTo, msg)
		if err == nil {
			w.Write([]byte(xid))
		} else {
			p.writeErr(w, r, err)
		}
	default:
		p.writeErrAct(w, r, msg.Action)
	}
}

// POST { action } /v1/buckets[/bucket-name]
func (p *proxy) httpbckpost(w http.ResponseWriter, r *http.Request) {
	var msg *apc.ActMsg
	apiItems, err := p.apiItems(w, r, 1, true, apc.URLPathBuckets.L)
	if err != nil {
		return
	}
	if msg, err = p.readActionMsg(w, r); err != nil {
		return
	}
	bucket := apiItems[0]
	p._bckpost(w, r, msg, bucket)
}

func (p *proxy) _bckpost(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg, bucket string) {
	var (
		query    = r.URL.Query()
		bck, err = newBckFromQ(bucket, query, nil)
	)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}

	if msg.Action == apc.ActCreateBck {
		if bck.IsRemoteAIS() {
			// create bucket (remais)
			p.reverseReqRemote(w, r, msg, bck.Bucket(), query)
			return
		}
		// create bucket (this cluster)
		p._bcr(w, r, query, msg, bck)
		return
	}

	// only the primary can do metasync
	dtor := xact.Table[msg.Action]
	if dtor.Metasync {
		if p.forwardCP(w, r, msg, bucket) {
			return
		}
	}

	// POST /bucket operations (this cluster)

	// Initialize bucket; if doesn't exist try creating it on the fly
	// but only if it's a remote bucket (and user did not explicitly disallowed).
	bckArgs := bckInitArgs{p: p, w: w, r: r, bck: bck, msg: msg, query: query}
	bckArgs.createAIS = false
	if bck, err = bckArgs.initAndTry(); err != nil {
		return
	}

	//
	// {action} on bucket
	//
	switch msg.Action {
	case apc.ActMoveBck:
		bckFrom := bck
		bckTo, err := newBckFromQuname(query, true /*required*/)
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		if !bckFrom.IsAIS() && bckFrom.Backend() == nil {
			p.writeErrf(w, r, "can only rename AIS ('ais://') bucket (%q is not)", bckFrom)
			return
		}
		if bckTo.IsRemote() {
			p.writeErrf(w, r, "can only rename to AIS ('ais://') bucket (%q is remote)", bckTo)
			return
		}
		if bckFrom.Equal(bckTo, false, false) {
			p.writeErrf(w, r, "cannot rename bucket %q to itself (%q)", bckFrom, bckTo)
			return
		}

		bckFrom.Provider = apc.AIS
		bckTo.Provider = apc.AIS

		if _, present := p.owner.bmd.get().Get(bckTo); present {
			err := cmn.NewErrBckAlreadyExists(bckTo.Bucket())
			p.writeErr(w, r, err)
			return
		}
		glog.Infof("%s bucket %s => %s", msg.Action, bckFrom, bckTo)
		var xid string
		if xid, err = p.renameBucket(bckFrom, bckTo, msg); err != nil {
			p.writeErr(w, r, err)
			return
		}
		w.Write([]byte(xid))
	case apc.ActCopyBck, apc.ActETLBck:
		var (
			bckTo       *cluster.Bck
			tcbmsg      = &apc.TCBMsg{}
			xid         string
			errCode     int
			fltPresence = apc.FltPresent
		)
		switch msg.Action {
		case apc.ActETLBck:
			if err := cos.MorphMarshal(msg.Value, tcbmsg); err != nil {
				p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
				return
			}
			if err := tcbmsg.Validate(true); err != nil {
				p.writeErr(w, r, err)
				return
			}
		case apc.ActCopyBck:
			if err = cos.MorphMarshal(msg.Value, &tcbmsg.CopyBckMsg); err != nil {
				p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
				return
			}
		}
		bckTo, err = newBckFromQuname(query, true /*required*/)
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		if bck.Equal(bckTo, false, true) {
			p.writeErrf(w, r, "cannot %s bucket %q onto itself", msg.Action, bck)
			return
		}

		bckTo, errCode, err = p.initBckTo(w, r, query, bckTo)
		if err != nil {
			return
		}
		if errCode == http.StatusNotFound {
			if p.forwardCP(w, r, msg, bucket) { // to create
				return
			}
			glog.Warningf("%s: dst %s doesn't exist and will be created with the src (%s) props", p, bckTo, bck)
		}

		// start x-tcb or x-tco
		if v := query.Get(apc.QparamFltPresence); v != "" {
			fltPresence, _ = strconv.Atoi(v)
		}
		if !apc.IsFltPresent(fltPresence) && bck.IsRemote() && !bck.IsHTTP() {
			if fltPresence == apc.FltExistsOutside {
				// TODO: upon request
				err = fmt.Errorf("(flt %d=\"outside\") not implemented yet", fltPresence)
				p.writeErr(w, r, err, http.StatusNotImplemented)
				return
			}
			lstcx := &lstcx{
				p:       p,
				bckFrom: bck,
				bckTo:   bckTo,
				amsg:    msg,
				tcbmsg:  tcbmsg,
			}
			xid, err = lstcx.do()
		} else {
			glog.Infof("%s bucket %s => %s", msg.Action, bck, bckTo)
			xid, err = p.tcb(bck, bckTo, msg, tcbmsg.DryRun)
		}
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		w.Write([]byte(xid))
	case apc.ActCopyObjects, apc.ActETLObjects:
		var (
			xid     string
			tcomsg  = &cmn.TCObjsMsg{}
			bckTo   *cluster.Bck
			errCode int
			eq      bool
		)
		if err = cos.MorphMarshal(msg.Value, tcomsg); err != nil {
			p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
			return
		}
		bckTo = cluster.CloneBck(&tcomsg.ToBck)

		if bck.Equal(bckTo, true, true) {
			eq = true
			glog.Warningf("multi-obj %s within the same bucket %q", msg.Action, bck)
		}
		if bckTo.IsHTTP() {
			p.writeErrf(w, r, "cannot %s to HTTP bucket %q", msg.Action, bckTo)
			return
		}
		if !eq {
			bckTo, errCode, err = p.initBckTo(w, r, query, bckTo)
			if err != nil {
				return
			}
			if errCode == http.StatusNotFound {
				if p.forwardCP(w, r, msg, bucket) { // to create
					return
				}
				glog.Warningf("%s: dst %s doesn't exist and will be created with src %s props", p, bck, bckTo)
			}
		}

		glog.Infof("multi-obj %s %s => %s", msg.Action, bck, bckTo)
		if xid, err = p.tcobjs(bck, bckTo, msg); err != nil {
			p.writeErr(w, r, err)
			return
		}
		w.Write([]byte(xid))
	case apc.ActAddRemoteBck:
		if err := p.checkAccess(w, r, nil, apc.AceCreateBucket); err != nil {
			return
		}
		if err := p.createBucket(msg, bck, nil); err != nil {
			p.writeErr(w, r, err, crerrStatus(err))
			return
		}
	case apc.ActPrefetchObjects:
		// TODO: GET vs SYNC?
		if bck.IsAIS() {
			p.writeErrf(w, r, fmtNotRemote, bucket)
			return
		}
		var xid string
		if xid, err = p.listrange(r.Method, bucket, msg, query); err != nil {
			p.writeErr(w, r, err)
			return
		}
		w.Write([]byte(xid))
	case apc.ActInvalListCache:
		p.qm.c.invalidate(bck.Bucket())
	case apc.ActMakeNCopies:
		var xid string
		if xid, err = p.makeNCopies(msg, bck); err != nil {
			p.writeErr(w, r, err)
			return
		}
		w.Write([]byte(xid))
	case apc.ActECEncode:
		var xid string
		if xid, err = p.ecEncode(bck, msg); err != nil {
			p.writeErr(w, r, err)
			return
		}
		w.Write([]byte(xid))
	default:
		p.writeErrAct(w, r, msg.Action)
	}
}

// init existing or create remote
// not calling `initAndTry` - delegating ais:from// props cloning to the separate method
func (p *proxy) initBckTo(w http.ResponseWriter, r *http.Request, query url.Values,
	bckTo *cluster.Bck) (*cluster.Bck, int, error) {
	bckToArgs := bckInitArgs{p: p, w: w, r: r, bck: bckTo, perms: apc.AcePUT, query: query}
	bckToArgs.createAIS = true
	errCode, err := bckToArgs.init()
	if err != nil && errCode != http.StatusNotFound {
		p.writeErr(w, r, err, errCode)
		return nil, 0, err
	}
	// creating (BMD-wise) remote destination on the fly
	if errCode == http.StatusNotFound && bckTo.IsRemote() {
		if bckTo, err = bckToArgs.try(); err != nil {
			return nil, 0, err
		}
		errCode = 0
	}
	return bckTo, errCode, nil
}

// POST { apc.ActCreateBck } /v1/buckets/bucket-name
func (p *proxy) _bcr(w http.ResponseWriter, r *http.Request, query url.Values, msg *apc.ActMsg, bck *cluster.Bck) {
	var (
		remoteHdr http.Header
		bucket    = bck.Name
	)
	if err := p.checkAccess(w, r, nil, apc.AceCreateBucket); err != nil {
		return
	}
	if err := bck.Validate(); err != nil {
		p.writeErr(w, r, err)
		return
	}
	if p.forwardCP(w, r, msg, bucket) {
		return
	}
	if bck.Provider == "" {
		bck.Provider = apc.AIS
	}
	if bck.IsHDFS() && msg.Value == nil {
		p.writeErr(w, r,
			errors.New("property 'extra.hdfs.ref_directory' must be specified when creating HDFS bucket"))
		return
	}
	// remote: check existence and get (cloud) props
	if bck.IsRemote() {
		rhdr, statusCode, err := p.headRemoteBck(bck.RemoteBck(), nil)
		if err != nil {
			if bck.IsCloud() {
				statusCode = http.StatusNotImplemented
				err = cmn.NewErrNotImpl("create", bck.Provider+"(cloud) bucket")
			} else if !bck.IsRemoteAIS() {
				err = cmn.NewErrUnsupp("create", bck.Provider+":// bucket")
			}
			p.writeErr(w, r, err, statusCode)
			return
		}
		remoteHdr = rhdr
		// NOTE: substituting action in the message
		msg.Action = apc.ActAddRemoteBck
	}
	// props-to-update at creation time
	if msg.Value != nil {
		propsToUpdate := cmn.BucketPropsToUpdate{}
		if err := cos.MorphMarshal(msg.Value, &propsToUpdate); err != nil {
			p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
			return
		}
		// Make and validate new bucket props.
		bck.Props = defaultBckProps(bckPropsArgs{bck: bck})
		nprops, err := p.makeNewBckProps(bck, &propsToUpdate, true /*creating*/)
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		bck.Props = nprops
		if backend := bck.Backend(); backend != nil {
			if err := backend.Validate(); err != nil {
				p.writeErrf(w, r, "cannot create %s: invalid backend %s, err: %v", bck, backend, err)
				return
			}
			// Initialize backend bucket.
			if err := backend.InitNoBackend(p.owner.bmd); err != nil {
				if !cmn.IsErrRemoteBckNotFound(err) {
					p.writeErrf(w, r, "cannot create %s: failing to initialize backend %s, err: %v",
						bck, backend, err)
					return
				}
				args := bckInitArgs{p: p, w: w, r: r, bck: backend, msg: msg, query: query}
				args.createAIS = false
				if _, err = args.try(); err != nil {
					return
				}
			}
		}
		// Send all props to the target (required for HDFS).
		msg.Value = bck.Props
	}
	if err := p.createBucket(msg, bck, remoteHdr); err != nil {
		p.writeErr(w, r, err, crerrStatus(err))
	}
}

func crerrStatus(err error) (errCode int) {
	switch err.(type) {
	case *cmn.ErrBucketAlreadyExists:
		errCode = http.StatusConflict
	case *cmn.ErrNotImpl:
		errCode = http.StatusNotImplemented
	}
	return
}

func (p *proxy) listObjects(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, amsg *apc.ActMsg,
	lsmsg *apc.LsoMsg, beg int64) {
	smap := p.owner.smap.get()
	if smap.CountActiveTs() < 1 {
		p.writeErr(w, r, cmn.NewErrNoNodes(apc.Target, smap.CountTargets()))
		return
	}

	// default props & flags => user-provided message
	switch {
	case lsmsg.Props == "" && lsmsg.IsFlagSet(apc.LsObjCached):
		lsmsg.AddProps(apc.GetPropsDefaultAIS...)
	case lsmsg.Props == "":
		lsmsg.AddProps(apc.GetPropsMinimal...)
		lsmsg.SetFlag(apc.LsNameSize)
	case lsmsg.WantOnlyName():
		lsmsg.SetFlag(apc.LsNameOnly)
	}
	// ht:// backend doesn't have `ListObjects`; can only locally list archived content
	if bck.IsHTTP() || lsmsg.IsFlagSet(apc.LsArchDir) {
		lsmsg.SetFlag(apc.LsObjCached)
	}

	var (
		err                        error
		tsi                        *cluster.Snode
		lst                        *cmn.LsoResult
		newls                      bool
		listRemote, wantOnlyRemote bool
	)
	if lsmsg.UUID == "" {
		lsmsg.UUID = cos.GenUUID()
		newls = true
	}
	tsi, listRemote, wantOnlyRemote, err = p._lsofc(bck, lsmsg, smap)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	var nl nl.Listener
	if newls {
		if wantOnlyRemote {
			nl = xact.NewXactNL(lsmsg.UUID, apc.ActList, &smap.Smap, cluster.NodeMap{tsi.ID(): tsi}, bck.Bucket())
		} else {
			// bcast
			nl = xact.NewXactNL(lsmsg.UUID, apc.ActList, &smap.Smap, nil, bck.Bucket())
		}
		// NOTE #2: TODO: currently, always primary - hrw redirect vs scenarios***
		nl.SetOwner(smap.Primary.ID())
		p.ic.registerEqual(regIC{nl: nl, smap: smap, msg: amsg})
	}

	if p.ic.reverseToOwner(w, r, lsmsg.UUID, amsg) {
		debug.Assert(p.si.ID() != smap.Primary.ID())
		if newls {
			// cleanup right away
			p.notifs.del(nl, false)
		}
		return
	}

	if listRemote {
		if lsmsg.StartAfter != "" {
			// TODO: remote AIS first, then Cloud
			err = fmt.Errorf("%s option --start_after (%s) not yet supported for remote buckets (%s)",
				lsotag, lsmsg.StartAfter, bck)
			p.writeErr(w, r, err, http.StatusNotImplemented)
			return
		}
		lst, err = p.lsObjsR(bck, lsmsg, smap, tsi, wantOnlyRemote)
		// TODO: `status == http.StatusGone`: at this point we know that this
		// remote bucket exists and is offline. We should somehow try to list
		// cached objects. This isn't easy as we basically need to start a new
		// xaction and return a new `UUID`.
	} else {
		lst, err = p.lsObjsA(bck, lsmsg)
	}
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	debug.Assert(lst != nil)

	if strings.Contains(r.Header.Get(cos.HdrAccept), cos.ContentMsgPack) {
		if !p.writeMsgPack(w, lst, lsotag) {
			return
		}
	} else if !p.writeJSON(w, r, lst, lsotag) {
		return
	}

	// Free memory allocated for temporary slice immediately as it can take up to a few GB
	lst.Entries = lst.Entries[:0]
	lst.Entries = nil
	lst = nil

	p.statsT.AddMany(
		cos.NamedVal64{Name: stats.ListCount, Value: 1},
		cos.NamedVal64{Name: stats.ListLatency, Value: mono.SinceNano(beg)},
	)
}

// list-objects flow control helper
func (p *proxy) _lsofc(bck *cluster.Bck, lsmsg *apc.LsoMsg, smap *smapX) (tsi *cluster.Snode, listRemote, wantOnlyRemote bool,
	err error) {
	listRemote = bck.IsRemote() && !lsmsg.IsFlagSet(apc.LsObjCached)
	if !listRemote {
		return
	}
	wantOnlyRemote = lsmsg.WantOnlyRemoteProps() // system default unless set by user

	// designate one target to carry-out backend.list-objects
	if lsmsg.SID != "" {
		tsi = smap.GetTarget(lsmsg.SID)
		if tsi == nil || tsi.InMaintOrDecomm() {
			err = &errNodeNotFound{lsotag + " failure", lsmsg.SID, p.si, smap}
			if smap.CountActiveTs() == 1 {
				// (walk an extra mile)
				orig := err
				tsi, err = cluster.HrwTargetTask(lsmsg.UUID, &smap.Smap)
				if err == nil {
					glog.Warningf("ignoring [%v] - utilizing the last (or the only) active target %s", orig, tsi)
					lsmsg.SID = tsi.ID()
				}
			}
		}
		return
	}

	if tsi, err = cluster.HrwTargetTask(lsmsg.UUID, &smap.Smap); err == nil {
		lsmsg.SID = tsi.ID()
	}
	return
}

// POST { action } /v1/objects/bucket-name[/object-name]
func (p *proxy) httpobjpost(w http.ResponseWriter, r *http.Request) {
	msg, err := p.readActionMsg(w, r)
	if err != nil {
		return
	}
	apireq := apiReqAlloc(1, apc.URLPathObjects.L, false /*dpq*/)
	defer apiReqFree(apireq)
	if msg.Action == apc.ActRenameObject {
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
	case apc.ActRenameObject:
		if err := p.checkAccess(w, r, bck, apc.AceObjMOVE); err != nil {
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
	case apc.ActPromote:
		if err := p.checkAccess(w, r, bck, apc.AcePromote); err != nil {
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
				err := &errNodeNotFound{apc.ActPromote + " failure", args.DaemonID, p.si, smap}
				p.writeErr(w, r, err)
				return
			}
		}
		xid, err := p.promote(bck, msg, tsi)
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		w.Write([]byte(xid))
	default:
		p.writeErrAct(w, r, msg.Action)
	}
}

// HEAD /v1/buckets/bucket-name
func (p *proxy) httpbckhead(w http.ResponseWriter, r *http.Request) {
	var (
		info           *cmn.BsummResult
		hdr            = w.Header()
		apireq         = apiReqAlloc(1, apc.URLPathBuckets.L, true /*dpq*/)
		fltPresence    int  // operate on present-only or otherwise (as per apc.Flt* enum)
		wantBckSummary bool // whether to include bucket summary in the response
	)
	defer apiReqFree(apireq)

	if err := p.parseReq(w, r, apireq); err != nil {
		return
	}

	bckArgs := bckInitArgs{p: p, w: w, r: r, bck: apireq.bck, perms: apc.AceBckHEAD,
		dpq: apireq.dpq, query: apireq.query}

	bckArgs.dontAddRemote = cos.IsParseBool(apireq.dpq.dontAddRemote) // QparamDontAddRemote

	// present-only [+ bucket summary]
	if apireq.dpq.fltPresence != "" {
		wantBckSummary = true
		fltPresence, _ = strconv.Atoi(apireq.dpq.fltPresence)
		// bckArgs.dontHeadRemote == false will have an effect of adding an (existing)
		// bucket to the cluster's BMD - right here and now, ie., on the fly.
		// This could be easily prevented but then again, it is currently impossible
		// to run xactions on buckets that are not present.
		// See also: comments to `QparamFltPresence` and `apc.Flt*` enum
		bckArgs.dontHeadRemote = bckArgs.dontHeadRemote || apc.IsFltPresent(fltPresence)
	}
	bckArgs.createAIS = false

	bck, err := bckArgs.initAndTry()
	if err != nil {
		return
	}
	if wantBckSummary {
		info = &cmn.BsummResult{IsBckPresent: false}
	}
	if bckArgs.isPresent {
		if wantBckSummary {
			if !apc.IsFltNoProps(fltPresence) {
				// get runtime bucket info (aka /summary/):
				// broadcast to all targets, collect, and summarize
				if err := p.bsummDoWait(bck, info, fltPresence); err != nil {
					p.writeErr(w, r, err)
					return
				}
			}
			info.IsBckPresent = true
		}
		toHdr(bck, hdr, info)
		return
	}

	debug.Assert(bck.IsRemote())

	// [filter] when the bucket that must be present is not
	if apc.IsFltPresent(fltPresence) {
		toHdr(bck, hdr, info)
		return
	}
	if bckArgs.dontAddRemote {
		debug.Assert(bckArgs.exists)
		toHdr(bck, hdr, info)
		return
	}

	bmd := p.owner.bmd.get()
	bck.Props, bckArgs.isPresent = bmd.Get(bck)

	if wantBckSummary {
		// NOTE: summarizing freshly added remote
		runtime.Gosched()
		debug.Assert(bckArgs.isPresent)
		if !apc.IsFltNoProps(fltPresence) {
			if err := p.bsummDoWait(bck, info, fltPresence); err != nil {
				p.writeErr(w, r, err)
				return
			}
		}
		info.IsBckPresent = true
	}
	toHdr(bck, hdr, info)
}

func toHdr(bck *cluster.Bck, hdr http.Header, info *cmn.BsummResult) {
	if bck.Props == nil {
		hdr.Set(apc.HdrBucketProps, cos.MustMarshalToString(&cmn.BucketProps{}))
	} else {
		hdr.Set(apc.HdrBucketProps, cos.MustMarshalToString(bck.Props))
	}
	if info != nil {
		hdr.Set(apc.HdrBucketSumm, cos.MustMarshalToString(info))
	}
}

// PATCH /v1/buckets/bucket-name
func (p *proxy) httpbckpatch(w http.ResponseWriter, r *http.Request) {
	var (
		err           error
		msg           *apc.ActMsg
		propsToUpdate cmn.BucketPropsToUpdate
		xid           string
		nprops        *cmn.BucketProps // complete instance of bucket props with propsToUpdate changes
	)
	apireq := apiReqAlloc(1, apc.URLPathBuckets.L, false /*dpq*/)
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
	bck := apireq.bck
	if p.forwardCP(w, r, msg, "patch "+bck.String()) {
		return
	}
	perms := apc.AcePATCH
	if propsToUpdate.Access != nil {
		perms |= apc.AceBckSetACL
	}
	bckArgs := bckInitArgs{p: p, w: w, r: r, bck: bck, msg: msg, skipBackend: true,
		perms: perms, dpq: apireq.dpq, query: apireq.query}
	bckArgs.createAIS = false
	if bck, err = bckArgs.initAndTry(); err != nil {
		return
	}
	if err = _checkAction(msg, apc.ActSetBprops, apc.ActResetBprops); err != nil {
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
		backendBck := cluster.CloneBck(&nprops.BackendBck)
		args := bckInitArgs{p: p, w: w, r: r, bck: backendBck, msg: msg, dpq: apireq.dpq, query: apireq.query}
		args.createAIS = false
		if _, err = args.initAndTry(); err != nil {
			return
		}
		// init and validate
		if err = p.initBackendProp(nprops); err != nil {
			p.writeErr(w, r, err)
			return
		}
	}
	if xid, err = p.setBucketProps(msg, bck, nprops); err != nil {
		p.writeErr(w, r, err)
		return
	}
	w.Write([]byte(xid))
}

// HEAD /v1/objects/bucket-name/object-name
func (p *proxy) httpobjhead(w http.ResponseWriter, r *http.Request, origURLBck ...string) {
	bckArgs := allocInitBckArgs()
	{
		bckArgs.p = p
		bckArgs.w = w
		bckArgs.r = r
		bckArgs.perms = apc.AceObjHEAD
		bckArgs.createAIS = false
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
		glog.Infof("%s %s => %s", r.Method, bck.Cname(objName), si)
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
		bckArgs.perms = apc.AceObjHEAD
		bckArgs.createAIS = false
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
		glog.Infof("%s %s => %s", r.Method, bck.Cname(objName), si)
	}
	redirectURL := p.redirectURL(r, si, started, cmn.NetIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

// ============================
//
// supporting methods and misc
//
// ============================
// forward control plane request to the current primary proxy
// return: forf (forwarded or failed) where forf = true means exactly that: forwarded or failed
func (p *proxy) forwardCP(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg, s string, origBody ...[]byte) (forf bool) {
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
	if primary.url != smap.Primary.PubNet.URL {
		primary.url = smap.Primary.PubNet.URL
		uparsed, err := url.Parse(smap.Primary.PubNet.URL)
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
	pname := smap.Primary.StringEx()
	if msg != nil {
		glog.Infof("%s: forwarding \"%s:%s\" to the primary %s", p, msg.Action, s, pname)
	} else {
		glog.Infof("%s: forwarding %q to the primary %s", p, s, pname)
	}
	primary.rp.ServeHTTP(w, r)
	return true
}

// Based on default error handler `defaultErrorHandler` in `httputil/reverseproxy.go`.
func (p *proxy) rpErrHandler(w http.ResponseWriter, r *http.Request, err error) {
	msg := fmt.Sprintf("%s: rproxy err %v, req: %s %s", p, err, r.Method, r.URL.Path)
	if caller := r.Header.Get(apc.HdrCallerName); caller != "" {
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

func (p *proxy) reverseReqRemote(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg, bck *cmn.Bck, query url.Values) (err error) {
	var (
		backend     = cmn.BackendConfAIS{}
		aliasOrUUID = bck.Ns.UUID
		config      = cmn.GCO.Get()
		v           = config.Backend.Get(apc.AIS)
	)
	if v == nil {
		p.writeErrMsg(w, r, "no remote ais clusters attached")
		return err
	}

	cos.MustMorphMarshal(v, &backend)
	urls, exists := backend[aliasOrUUID]
	if !exists {
		var refreshed bool
		if p.remais.Ver == 0 {
			p._remais(&config.ClusterConfig, true)
			refreshed = true
		}
	ml:
		p.remais.mu.RLock()
		for _, remais := range p.remais.A {
			if remais.Alias == aliasOrUUID || remais.UUID == aliasOrUUID {
				urls = []string{remais.URL}
				exists = true
				break
			}
		}
		p.remais.mu.RUnlock()
		if !exists && !refreshed {
			p._remais(&config.ClusterConfig, true)
			refreshed = true
			goto ml
		}
	}
	if !exists {
		err = cmn.NewErrNotFound("%s: remote UUID/alias %q (%s)", p.si, aliasOrUUID, backend)
		p.writeErr(w, r, err)
		return err
	}

	debug.Assert(len(urls) > 0)
	u, err := url.Parse(urls[0])
	if err != nil {
		p.writeErr(w, r, err)
		return err
	}
	if msg != nil {
		body := cos.MustMarshal(msg)
		r.ContentLength = int64(len(body))
		r.Body = io.NopCloser(bytes.NewReader(body))
	}

	bck.Ns.UUID = ""
	query = cmn.DelBckFromQuery(query)
	query = bck.AddToQuery(query)
	r.URL.RawQuery = query.Encode()
	p.reverseRequest(w, r, aliasOrUUID, u)
	return nil
}

func (p *proxy) listBuckets(w http.ResponseWriter, r *http.Request, qbck *cmn.QueryBcks, msg *apc.ActMsg, dpq *dpq) {
	var (
		bmd     = p.owner.bmd.get()
		present bool
	)
	if qbck.IsAIS() || qbck.IsHTTP() || qbck.IsHDFS() {
		bcks := bmd.Select(qbck)
		p.writeJSON(w, r, bcks, "list-buckets")
		return
	}

	// present-only filtering
	if dpq.fltPresence != "" {
		if v, err := strconv.Atoi(dpq.fltPresence); err == nil {
			present = apc.IsFltPresent(v)
		}
	}
	if present {
		bcks := bmd.Select(qbck)
		p.writeJSON(w, r, bcks, "list-buckets")
		return
	}

	// via random target
	si, err := p.owner.smap.get().GetRandTarget()
	if err != nil {
		p.writeErr(w, r, err)
		return
	}

	cargs := allocCargs()
	{
		cargs.si = si
		cargs.req = cmn.HreqArgs{
			Method:   r.Method,
			Path:     r.URL.Path,
			RawQuery: r.URL.RawQuery,
			Header:   r.Header,
			Body:     cos.MustMarshal(msg),
		}
		cargs.timeout = apc.DefaultTimeout
	}
	res := p.call(cargs)
	freeCargs(cargs)

	if res.err != nil {
		err = res.toErr()
		p.writeErr(w, r, err, res.status)
		return
	}

	hdr := w.Header()
	hdr.Set(cos.HdrContentType, res.header.Get(cos.HdrContentType))
	hdr.Set(cos.HdrContentLength, strconv.Itoa(len(res.bytes)))
	_, err = w.Write(res.bytes)
	debug.AssertNoErr(err)
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

	query.Set(apc.QparamProxyID, p.si.ID())
	query.Set(apc.QparamUnixTime, cos.UnixNano2S(ts.UnixNano()))
	redirect += query.Encode()
	return
}

// lsObjsA reads object list from all targets, combines, sorts and returns
// the final list. Excess of object entries from each target is remembered in the
// buffer (see: `queryBuffers`) so we won't request the same objects again.
func (p *proxy) lsObjsA(bck *cluster.Bck, lsmsg *apc.LsoMsg) (allEntries *cmn.LsoResult, err error) {
	var (
		aisMsg    *aisMsg
		args      *bcastArgs
		entries   cmn.LsoEntries
		results   sliceResults
		smap      = p.owner.smap.get()
		cacheID   = cacheReqID{bck: bck.Bucket(), prefix: lsmsg.Prefix}
		token     = lsmsg.ContinuationToken
		props     = lsmsg.PropsSet()
		hasEnough bool
		flags     uint32
	)
	if lsmsg.PageSize == 0 {
		lsmsg.PageSize = apc.DefaultPageSizeAIS
	}
	pageSize := lsmsg.PageSize

	// TODO: Before checking cache and buffer we should check if there is another
	// request in-flight that asks for the same page - if true wait for the cache
	// to get populated.

	if lsmsg.IsFlagSet(apc.UseListObjsCache) {
		entries, hasEnough = p.qm.c.get(cacheID, token, pageSize)
		if hasEnough {
			goto end
		}
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

	aisMsg = p.newAmsgActVal(apc.ActList, &lsmsg)
	args = allocBcArgs()
	args.req = cmn.HreqArgs{
		Method: http.MethodGet,
		Path:   apc.URLPathBuckets.Join(bck.Name),
		Query:  bck.AddToQuery(nil),
		Body:   cos.MustMarshal(aisMsg),
	}
	args.timeout = apc.LongTimeout
	args.smap = smap
	args.cresv = cresLso{} // -> cmn.LsoResult

	// Combine the results.
	results = p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err != nil {
			err = res.toErr()
			freeBcastRes(results)
			return nil, err
		}
		objList := res.v.(*cmn.LsoResult)
		flags |= objList.Flags
		p.qm.b.set(lsmsg.UUID, res.si.ID(), objList.Entries, pageSize)
	}
	freeBcastRes(results)
	entries, hasEnough = p.qm.b.get(lsmsg.UUID, token, pageSize)
	debug.Assert(hasEnough)

endWithCache:
	if lsmsg.IsFlagSet(apc.UseListObjsCache) {
		p.qm.c.set(cacheID, token, entries, pageSize)
	}
end:
	if lsmsg.IsFlagSet(apc.UseListObjsCache) && !props.All(apc.GetPropsAll...) {
		// Since cache keeps entries with whole subset props we must create copy
		// of the entries with smaller subset of props (if we would change the
		// props of the `entries` it would also affect entries inside cache).
		propsEntries := make(cmn.LsoEntries, len(entries))
		for idx := range entries {
			propsEntries[idx] = entries[idx].CopyWithProps(props)
		}
		entries = propsEntries
	}

	allEntries = &cmn.LsoResult{
		UUID:    lsmsg.UUID,
		Entries: entries,
		Flags:   flags,
	}
	if uint(len(entries)) >= pageSize {
		allEntries.ContinuationToken = entries[len(entries)-1].Name
	}
	// By default, recursion is always enabled. When disabled the result will include
	// directories. It is then possible that multiple targets return the same directory
	// in their respective `cmn.LsoResult` responses - which is why:
	if lsmsg.IsFlagSet(apc.LsNoRecursion) {
		allEntries.Entries, _ = cmn.DedupLso(allEntries.Entries, uint(len(entries)))
	}
	return allEntries, nil
}

func (p *proxy) lsObjsR(bck *cluster.Bck, lsmsg *apc.LsoMsg, smap *smapX, tsi *cluster.Snode, wantOnlyRemote bool) (*cmn.LsoResult, error) {
	var (
		config     = cmn.GCO.Get()
		reqTimeout = config.Client.ListObjTimeout.D()
		aisMsg     = p.newAmsgActVal(apc.ActList, &lsmsg)
	)
	args := allocBcArgs()
	args.req = cmn.HreqArgs{
		Method: http.MethodGet,
		Path:   apc.URLPathBuckets.Join(bck.Name),
		Query:  bck.AddToQuery(nil),
		Body:   cos.MustMarshal(aisMsg),
	}

	var results sliceResults
	if wantOnlyRemote {
		cargs := allocCargs()
		{
			cargs.si = tsi
			cargs.req = args.req
			cargs.timeout = reqTimeout
			cargs.cresv = cresLso{} // -> cmn.LsoResult
		}
		res := p.call(cargs)
		freeCargs(cargs)
		results = make(sliceResults, 1)
		results[0] = res
	} else {
		args.timeout = reqTimeout
		args.smap = smap
		args.cresv = cresLso{} // -> cmn.LsoResult
		results = p.bcastGroup(args)
	}

	freeBcArgs(args)

	// Combine the results.
	resLists := make([]*cmn.LsoResult, 0, len(results))
	for _, res := range results {
		if res.status == http.StatusNotFound {
			continue
		}
		if res.err != nil {
			err := res.toErr()
			freeBcastRes(results)
			return nil, err
		}
		resLists = append(resLists, res.v.(*cmn.LsoResult))
	}
	freeBcastRes(results)

	return cmn.MergeLso(resLists, 0), nil
}

func (p *proxy) objMv(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string, msg *apc.ActMsg) {
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
		glog.Infof("%q %s => %s", msg.Action, bck.Cname(objName), si)
	}

	// NOTE: Code 307 is the only way to http-redirect with the original JSON payload.
	redirectURL := p.redirectURL(r, si, started, cmn.NetIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)

	p.statsT.Inc(stats.RenameCount)
}

func (p *proxy) listrange(method, bucket string, msg *apc.ActMsg, query url.Values) (xid string, err error) {
	var (
		smap   = p.owner.smap.get()
		aisMsg = p.newAmsg(msg, nil, cos.GenUUID())
		body   = cos.MustMarshal(aisMsg)
		path   = apc.URLPathBuckets.Join(bucket)
	)
	nlb := xact.NewXactNL(aisMsg.UUID, aisMsg.Action, &smap.Smap, nil)
	nlb.SetOwner(equalIC)
	p.ic.registerEqual(regIC{smap: smap, query: query, nl: nlb})
	args := allocBcArgs()
	args.req = cmn.HreqArgs{Method: method, Path: path, Query: query, Body: body}
	args.smap = smap
	args.timeout = apc.DefaultTimeout
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
	xid = aisMsg.UUID
	return
}

func (p *proxy) reverseHandler(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.apiItems(w, r, 1, false, apc.URLPathReverse.L)
	if err != nil {
		return
	}

	// rewrite URL path (removing `apc.Reverse`)
	r.URL.Path = cos.JoinWords(apc.Version, apiItems[0])

	nodeID := r.Header.Get(apc.HdrNodeID)
	if nodeID == "" {
		p.writeErrMsg(w, r, "missing node ID")
		return
	}
	smap := p.owner.smap.get()
	si := smap.GetNode(nodeID)
	if si != nil && si.InMaintOrDecomm() {
		daeStatus := "inactive"
		switch {
		case si.Flags.IsSet(cluster.NodeFlagMaint):
			daeStatus = apc.NodeMaintenance
		case si.Flags.IsSet(cluster.NodeFlagDecomm):
			daeStatus = apc.NodeDecommission
		}
		if r.Method == http.MethodGet {
			what := r.URL.Query().Get(apc.QparamWhat)
			if what == apc.WhatNodeStatsAndStatus {
				// skip reversing, return status as per Smap
				msg := &stats.NodeStatus{
					Node:   stats.Node{Snode: si},
					Status: daeStatus,
				}
				p.writeJSON(w, r, msg, what)
				return
			}
		}
		// otherwise, warn and go ahead
		// (e.g. scenario: shutdown when transitioning through states)
		glog.Warningf("%s: %s status is: %s", p.si, si.StringEx(), daeStatus)
	}

	// access control
	switch r.Method {
	case http.MethodGet:
		// must be consistent with httpdaeget, httpcluget
		err = p.checkAccess(w, r, nil, apc.AceShowCluster)
	case http.MethodPost:
		// (ditto) httpdaepost, httpclupost
		err = p.checkAccess(w, r, nil, apc.AceAdmin)
	case http.MethodPut, http.MethodDelete:
		// (ditto) httpdaeput/delete and httpcluput/delete
		err = p.checkAccess(w, r, nil, apc.AceAdmin)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodPost, http.MethodPut)
		return
	}
	if err != nil {
		return
	}

	// do
	if si != nil {
		p.reverseNodeRequest(w, r, si)
		return
	}
	// special case when the target self-removed itself from cluster map
	// after having lost all mountpaths.
	nodeURL := r.Header.Get(apc.HdrNodeURL)
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
		pre:   p.bmodPostMv,
		final: p.bmodSync,
		msg:   &apc.ActMsg{Value: apc.ActMoveBck},
		bcks:  []*cluster.Bck{cluster.NewBck(renamedBucket, apc.AIS, cmn.NsGlobal)},
	}
	_, err := p.owner.bmd.modify(ctx)
	debug.AssertNoErr(err)
}

func (p *proxy) bmodPostMv(ctx *bmdModifier, clone *bucketMD) error {
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
		glog.Errorf("%s: renamed bucket %s: unexpected props %+v", p, bck.Name, *bck.Props)
		ctx.terminate = true
		return nil
	}
	clone.del(bck)
	return nil
}

func (p *proxy) httpdaeget(w http.ResponseWriter, r *http.Request) {
	var (
		query = r.URL.Query()
		what  = query.Get(apc.QparamWhat)
	)
	if err := p.checkAccess(w, r, nil, apc.AceShowCluster); err != nil {
		return
	}
	switch what {
	case apc.WhatBMD:
		if renamedBucket := query.Get(whatRenamedLB); renamedBucket != "" {
			p.handlePendingRenamedLB(renamedBucket)
		}
		fallthrough // fallthrough
	case apc.WhatConfig, apc.WhatSmapVote, apc.WhatSnode, apc.WhatLog,
		apc.WhatNodeStats, apc.WhatMetricNames:
		p.htrun.httpdaeget(w, r, query, nil /*htext*/)
	case apc.WhatSysInfo:
		p.writeJSON(w, r, apc.GetMemCPU(), what)
	case apc.WhatSmap:
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
					glog.Errorf("%s is starting up, cannot return %s yet: %v", p, smap, err)
				}
				break
			}
			smap = p.owner.smap.get()
			time.Sleep(sleep)
		}
		if err := smap.validate(); err != nil {
			glog.Errorf("%s: startup is taking unusually long time: %s (%v)", p, smap, err)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		p.writeJSON(w, r, smap, what)
	case apc.WhatNodeStatsAndStatus:
		smap := p.owner.smap.get()
		msg := &stats.NodeStatus{
			Node: stats.Node{
				Snode: p.htrun.si,
			},
			SmapVersion:    smap.Version,
			MemCPUInfo:     apc.GetMemCPU(),
			DeploymentType: deploymentType(),
			Version:        daemon.version,
			BuildTime:      daemon.buildTime,
			K8sPodName:     os.Getenv(env.AIS.K8sPod),
			Status:         p._status(smap),
		}
		daeStats := p.statsT.GetStats()
		msg.Tracker = daeStats.Tracker

		p.writeJSON(w, r, msg, what)
	default:
		p.htrun.httpdaeget(w, r, query, nil /*htext*/)
	}
}

func (p *proxy) httpdaeput(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.apiItems(w, r, 0, true, apc.URLPathDae.L)
	if err != nil {
		return
	}
	if err := p.checkAccess(w, r, nil, apc.AceAdmin); err != nil {
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
	case apc.ActSetConfig: // set-config #2 - via action message
		p.setDaemonConfigMsg(w, r, msg, query)
	case apc.ActResetConfig:
		if err := p.owner.config.resetDaemonConfig(); err != nil {
			p.writeErr(w, r, err)
		}
	case apc.ActResetStats:
		errorsOnly := msg.Value.(bool)
		p.statsT.ResetStats(errorsOnly)
	case apc.ActDecommission:
		if !p.ensureIntraControl(w, r, true /* from primary */) {
			return
		}
		p.unreg(w, r, msg)
	case apc.ActShutdown:
		smap := p.owner.smap.get()
		isPrimary := smap.isPrimary(p.si)
		if !isPrimary {
			if !p.ensureIntraControl(w, r, true /* from primary */) {
				return
			}
			p.Stop(&errNoUnregister{msg.Action})
			return
		}
		force := cos.IsParseBool(query.Get(apc.QparamForce))
		if !force {
			p.writeErrf(w, r, "cannot shutdown primary %s (consider %s=true option)",
				p.si, apc.QparamForce)
			return
		}
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	default:
		p.writeErrAct(w, r, msg.Action)
	}
}

func (p *proxy) daePathAction(w http.ResponseWriter, r *http.Request, action string) {
	switch action {
	case apc.Proxy:
		p.daeSetPrimary(w, r)
	case apc.SyncSmap:
		newsmap := &smapX{}
		if cmn.ReadJSON(w, r, newsmap) != nil {
			return
		}
		if err := newsmap.validate(); err != nil {
			p.writeErrf(w, r, "%s: invalid %s: %v", p.si, newsmap, err)
			return
		}
		if err := p.owner.smap.synchronize(p.si, newsmap, nil /*ms payload*/); err != nil {
			p.writeErr(w, r, cmn.NewErrFailedTo(p, "synchronize", newsmap, err))
			return
		}
		glog.Infof("%s: %s %s done", p, apc.SyncSmap, newsmap)
	case apc.ActSetConfig: // set-config #1 - via query parameters and "?n1=v1&n2=v2..."
		p.setDaemonConfigQuery(w, r)
	default:
		p.writeErrAct(w, r, action)
	}
}

func (p *proxy) httpdaedelete(w http.ResponseWriter, r *http.Request) {
	// the path includes cmn.CallbackRmSelf (compare with t.httpdaedelete)
	_, err := p.apiItems(w, r, 0, false, apc.URLPathDaeRmSelf.L)
	if err != nil {
		return
	}
	if err := p.checkAccess(w, r, nil, apc.AceAdmin); err != nil {
		return
	}
	var msg apc.ActMsg
	if err := readJSON(w, r, &msg); err != nil {
		return
	}
	p.unreg(w, r, &msg)
}

func (p *proxy) unreg(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg) {
	// Stop keepaliving
	p.keepalive.ctrl(kaSuspendMsg)

	// In case of maintenance, we only stop the keepalive daemon,
	// the HTTPServer is still active and accepts requests.
	if msg.Action == apc.ActStartMaintenance {
		return
	}
	if msg.Action == apc.ActDecommission || msg.Action == apc.ActDecommissionNode {
		var (
			opts              apc.ActValRmNode
			keepInitialConfig bool
		)
		if msg.Value != nil {
			if err := cos.MorphMarshal(msg.Value, &opts); err != nil {
				p.writeErr(w, r, err)
				return
			}
			keepInitialConfig = opts.KeepInitialConfig
		}
		cleanupConfigDir(p.Name(), keepInitialConfig)
	}
	p.Stop(&errNoUnregister{msg.Action})
}

func (p *proxy) httpdaepost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.apiItems(w, r, 0, true, apc.URLPathDae.L)
	if err != nil {
		return
	}
	if len(apiItems) == 0 || apiItems[0] != apc.AdminJoin {
		p.writeErrURL(w, r)
		return
	}
	if err := p.checkAccess(w, r, nil, apc.AceAdmin); err != nil {
		return
	}
	if !p.keepalive.paused() {
		glog.Warningf("%s: keepalive is already active - proceeding to resume (and reset) anyway", p.si)
	}
	p.keepalive.ctrl(kaResumeMsg)
	body, err := cmn.ReadBytes(r)
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	caller := r.Header.Get(apc.HdrCallerName)
	if err := p.recvCluMetaBytes(apc.ActAdminJoinProxy, body, caller); err != nil {
		p.writeErr(w, r, err)
	}
}

func (p *proxy) smapFromURL(baseURL string) (smap *smapX, err error) {
	cargs := allocCargs()
	{
		cargs.req = cmn.HreqArgs{
			Method: http.MethodGet,
			Base:   baseURL,
			Path:   apc.URLPathDae.S,
			Query:  url.Values{apc.QparamWhat: []string{apc.WhatSmap}},
		}
		cargs.timeout = apc.DefaultTimeout
		cargs.cresv = cresSM{} // -> smapX
	}
	res := p.call(cargs)
	if res.err != nil {
		err = res.errorf("failed to get Smap from %s", baseURL)
	} else {
		smap = res.v.(*smapX)
		if err = smap.validate(); err != nil {
			err = fmt.Errorf("%s: invalid %s from %s: %v", p, smap, baseURL, err)
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
	newPrimaryURL := r.URL.Query().Get(apc.QparamPrimaryCandidate)
	glog.Infof("%s: force new primary %s (URL: %s)", p, proxyID, newPrimaryURL)

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
		newPrimaryURL = psi.ControlNet.URL
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
	res := p.regTo(primary.ControlNet.URL, primary, apc.DefaultTimeout, nil, nil, false /*keepalive*/)
	if res.err != nil {
		p.writeErr(w, r, res.toErr())
	}
}

func (p *proxy) daeSetPrimary(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.apiItems(w, r, 2, false, apc.URLPathDae.L)
	if err != nil {
		return
	}
	proxyID := apiItems[1]
	query := r.URL.Query()
	force := cos.IsParseBool(query.Get(apc.QparamForce))

	// forceful primary change
	if force && apiItems[0] == apc.Proxy {
		if smap := p.owner.smap.get(); !smap.isPrimary(p.si) {
			p.writeErr(w, r, newErrNotPrimary(p.si, smap))
		}
		p.forcefulJoin(w, r, proxyID)
		return
	}
	prepare, err := cos.ParseBool(query.Get(apc.QparamPrepare))
	if err != nil {
		p.writeErrf(w, r, "failed to parse URL query %q: %v", apc.QparamPrepare, err)
		return
	}
	if p.owner.smap.get().isPrimary(p.si) {
		p.writeErrf(w, r, "%s: am PRIMARY, expecting '/v1/cluster/...' when designating a new one", p)
		return
	}
	if prepare {
		var cluMeta cluMeta
		if err := cmn.ReadJSON(w, r, &cluMeta); err != nil {
			return
		}
		if err := p.recvCluMeta(&cluMeta, "set-primary", cluMeta.SI.String()); err != nil {
			p.writeErrf(w, r, "%s: failed to receive clu-meta: %v", p, err)
			return
		}
	}

	// self
	if p.si.ID() == proxyID {
		smap := p.owner.smap.get()
		if smap.GetActiveNode(proxyID) == nil {
			p.writeErrf(w, r, "%s: in maintenance or decommissioned", p)
			return
		}
		if !prepare {
			p.becomeNewPrimary("")
		}
		return
	}

	// other
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
	if ctx.sid != "" && clone.GetNode(ctx.sid) != nil {
		// decision is made: going ahead to remove
		glog.Infof("%s: removing failed primary %s", p, ctx.sid)
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
		msg   = p.newAmsgStr(apc.ActNewPrimary, bmd)
		pairs = []revsPair{{clone, msg}, {bmd, msg}, {rmd, msg}}
	)
	glog.Infof("%s: distributing (%s, %s, %s) with newly elected primary (self)", p, clone, bmd, rmd)
	config, err := p.ensureConfigPrimaryURL()
	if err != nil {
		glog.Error(err)
	}
	if config != nil {
		pairs = append(pairs, revsPair{config, msg})
		glog.Infof("%s: plus %s", p, config)
	}
	etl := p.owner.etl.get()
	if etl != nil && etl.version() > 0 {
		pairs = append(pairs, revsPair{etl, msg})
		glog.Infof("%s: plus %s", p, etl)
	}
	debug.Assert(clone._sgl != nil)
	_ = p.metasyncer.sync(pairs...)
	p.syncNewICOwners(ctx.smap, clone)
}

func (p *proxy) ensureConfigPrimaryURL() (config *globalConfig, err error) {
	config, err = p.owner.config.modify(&configModifier{pre: p._primaryURLPre})
	if err != nil {
		err = cmn.NewErrFailedTo(p, "update primary URL", config, err)
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

// [METHOD] /v1/sort
func (p *proxy) dsortHandler(w http.ResponseWriter, r *http.Request) {
	if !p.cluStartedWithRetry() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if err := p.checkAccess(w, r, nil, apc.AceAdmin); err != nil {
		return
	}

	apiItems, err := cmn.MatchItems(r.URL.Path, 0, true, apc.URLPathdSort.L)
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
		if len(apiItems) == 1 && apiItems[0] == apc.Abort {
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

func (p *proxy) rootHandler(w http.ResponseWriter, r *http.Request) {
	const fs3 = "/" + apc.S3
	if !p.cluStartedWithRetry() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	// by default, s3 is serviced at `/s3`
	// with `/` root reserved for vanilla http locations via ht:// mechanism
	config := cmn.GCO.Get()
	cmn.Features = config.Features
	if !cmn.Features.IsSet(feat.ProvideS3APIViaRoot) {
		p.htHandler(w, r)
		return
	}

	// prepend /s3 and handle
	switch {
	case r.URL.Path == "" || r.URL.Path == "/":
		r.URL.Path = fs3
	case r.URL.Path[0] == '/':
		r.URL.Path = fs3 + r.URL.Path
	default:
		r.URL.Path = fs3 + "/" + r.URL.Path
	}
	p.s3Handler(w, r)
}

// GET | HEAD vanilla http(s) location via `ht://` bucket with the corresponding `OrigURLBck`
func (p *proxy) htHandler(w http.ResponseWriter, r *http.Request) {
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
		q.Set(apc.QparamOrigURL, r.URL.String())
		q.Set(apc.QparamProvider, apc.HTTP)
		r.URL.Path = apc.URLPathObjects.Join(hbo.Bck.Name, hbo.ObjName)
		r.URL.RawQuery = q.Encode()
		if r.Method == http.MethodGet {
			p.httpobjget(w, r, hbo.OrigURLBck)
		} else {
			p.httpobjhead(w, r, hbo.OrigURLBck)
		}
		return
	}
	p.writeErrf(w, r, "%q provider doesn't support %q", apc.HTTP, r.Method)
}

//
// metasync Rx
//

// compare w/ t.receiveConfig
func (p *proxy) receiveConfig(newConfig *globalConfig, msg *aisMsg, payload msPayload, caller string) (err error) {
	oldConfig := cmn.GCO.Get()
	if err = p._recvCfg(newConfig, msg, payload, caller); err != nil {
		return
	}

	if !p.NodeStarted() {
		if msg.Action == apc.ActAttachRemAis || msg.Action == apc.ActDetachRemAis {
			glog.Warningf("%s: cannot handle %s (%s => %s) - starting up...", p, msg, oldConfig, newConfig)
		}
		return
	}

	if msg.Action != apc.ActAttachRemAis && msg.Action != apc.ActDetachRemAis &&
		newConfig.Backend.EqualRemAIS(&oldConfig.Backend) {
		return // nothing to do
	}

	go p._remais(&newConfig.ClusterConfig, false)
	return
}

// refresh local p.remais cache via intra-cluster call to a random target
func (p *proxy) _remais(newConfig *cmn.ClusterConfig, blocking bool) {
	if !p.remais.in.CAS(false, true) {
		return
	}
	var (
		sleep      = newConfig.Timeout.CplaneOperation.D()
		retries    = 5
		over, nver int64
	)
	if blocking {
		retries = 1
	} else {
		maxsleep := newConfig.Timeout.MaxKeepalive.D()
		clutime := mono.Since(p.startup.cluster.Load())
		if clutime < maxsleep {
			sleep = 4 * maxsleep
		} else if clutime < newConfig.Timeout.Startup.D() {
			sleep = 2 * maxsleep
		}
	}
	for ; retries > 0; retries-- {
		time.Sleep(sleep)
		all, err := p.getRemAises(false /*refresh*/)
		if err != nil {
			glog.Errorf("%s: failed to get remais (%d attempts)", p, retries-1)
			continue
		}
		p.remais.mu.Lock()
		if over <= 0 {
			over = p.remais.Ver
		}
		if p.remais.Ver < all.Ver {
			// keep old/detached clusters to support access to existing ("cached") buckets
			// i.e., the ability to resolve remote alias to Ns.UUID (see p.a2u)
			for _, a := range p.remais.Remotes.A {
				var found bool
				for _, b := range p.remais.old {
					if b.UUID == a.UUID {
						*b = *a
						found = true
						break
					}
					if b.Alias == a.Alias {
						glog.Errorf("duplicated remais alias: (%q, %q) vs (%q, %q)", a.UUID, a.Alias, b.UUID, b.Alias)
					}
				}
				if !found {
					p.remais.old = append(p.remais.old, a)
				}
			}

			p.remais.Remotes = *all
			nver = p.remais.Ver
			p.remais.mu.Unlock()
			break
		}
		p.remais.mu.Unlock()
		glog.Errorf("%s: retrying remais ver=%d (%d attempts)", p, all.Ver, retries-1)
		sleep = newConfig.Timeout.CplaneOperation.D()
	}

	p.remais.in.Store(false)
	glog.Infof("%s: remais v%d => v%d", p, over, nver)
}

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
	if smap.IsIC(p.si) && smap.CountActiveTs() > 0 {
		nl := xact.NewXactNL(xact.RebID2S(newRMD.Version), apc.ActRebalance, &smap.Smap, nil)
		nl.SetOwner(equalIC)
		err := p.notifs.add(nl)
		debug.AssertNoErr(err)

		if newRMD.Resilver != "" {
			nl = xact.NewXactNL(newRMD.Resilver, apc.ActResilver, &smap.Smap, nil)
			nl.SetOwner(equalIC)
			err := p.notifs.add(nl)
			debug.AssertNoErr(err)
		}
	}
	return
}

func (p *proxy) smapOnUpdate(newSmap, oldSmap *smapX) {
	// When some node was removed from the cluster we need to clean up the
	// reverse proxy structure.
	p.rproxy.nodes.Range(func(key, _ any) bool {
		nodeID := key.(string)
		if oldSmap.GetNode(nodeID) != nil && newSmap.GetNode(nodeID) == nil {
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
		glog.Errorf("%s (non-primary): %v - proceeding to override BMD", p, err)
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
			Path:   apc.URLPathDae.S,
			Query:  url.Values{apc.QparamWhat: []string{apc.WhatSnode}},
		}
		cargs.timeout = cmn.Timeout.CplaneOperation()
		cargs.cresv = cresND{} // -> cluster.Snode
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

func (p *proxy) headRemoteBck(bck *cmn.Bck, q url.Values) (header http.Header, statusCode int, err error) {
	var (
		tsi  *cluster.Snode
		path = apc.URLPathBuckets.Join(bck.Name)
	)
	if tsi, err = p.owner.smap.get().GetRandTarget(); err != nil {
		return
	}
	if bck.IsCloud() {
		config := cmn.GCO.Get()
		if config.Backend.Get(bck.Provider) == nil {
			err = &cmn.ErrMissingBackend{Provider: bck.Provider}
			statusCode = http.StatusNotFound
			err = cmn.NewErrFailedTo(p, "lookup Cloud bucket", bck, err, statusCode)
			return
		}
	}
	q = bck.AddToQuery(q)
	cargs := allocCargs()
	{
		cargs.si = tsi
		cargs.req = cmn.HreqArgs{Method: http.MethodHead, Path: path, Query: q}
		cargs.timeout = apc.DefaultTimeout
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
