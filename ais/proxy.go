// Package ais provides AIStore's proxy and target nodes.
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */

//go:generate go run ../tools/gendocs/
package ais

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/api/env"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/k8s"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/dsort"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/xact"
	"github.com/NVIDIA/aistore/xact/xreg"

	jsoniter "github.com/json-iterator/go"
)

const (
	lsotag = "list-objects"
)

type (
	ClusterMountpathsRaw struct {
		Targets cos.JSONRawMsgs `json:"targets"`
	}

	// proxy runner
	proxy struct {
		htrun
		authn      *authManager
		metasyncer *metasyncer
		ic         ic
		rproxy     reverseProxy
		notifs     notifs
		lstca      lstca
		reg        struct {
			pool nodeRegPool
			mu   sync.RWMutex
		}
		remais struct {
			old []*meta.RemAis // to facilitate a2u resolution (and, therefore, offline access)
			meta.RemAisVec
			mu sync.RWMutex
			in atomic.Bool
		}
		ec                ecToggle
		dm                dmToggle
		settingNewPrimary atomic.Bool // primary executing "set new primary" request (state)
		readyToFastKalive atomic.Bool // primary can accept fast keepalives
	}
)

// interface guard
var _ cos.Runner = (*proxy)(nil)

func (*proxy) Name() string { return apc.Proxy } // as cos.Runner

func (p *proxy) init(config *cmn.Config) {
	p.initSnode(config)

	// (a) get node ID from command-line or env var (see envDaemonID())
	// (b) load existing ID from config file stored under local config `confdir` (compare w/ target)
	// (c) generate a new one (genDaemonID())
	// - in that sequence
	p.si.Init(initPID(config), apc.Proxy)

	memsys.Init(p.SID(), p.SID(), config)

	debug.Assert(p.si.IDDigest != 0)
	cos.InitShortID(p.si.IDDigest)

	if network, err := _parseCIDR(env.AisLocalRedirectCIDR, ""); err != nil {
		cos.ExitLog(err) // FATAL
	} else {
		p.si.LocalNet = network
		nlog.Infoln("using local redirect CIDR:", network.String())
	}

	daemon.rg.add(p)

	ps := &stats.Prunner{}
	startedUp := ps.Init(p) // (+ reg common metrics)
	daemon.rg.add(ps)
	p.statsT = ps

	k := newPalive(p, ps, startedUp)
	daemon.rg.add(k)
	p.keepalive = k

	m := newMetasyncer(p)
	daemon.rg.add(m)
	p.metasyncer = m

	// shared streams
	p.ec.init()
	p.dm.init()
}

func initPID(config *cmn.Config) string {
	// 1. ID from env
	if pid := envDaemonID(apc.Proxy); pid != "" {
		if err := cos.ValidateDaemonID(pid); err != nil {
			nlog.Errorln("Daemon ID loaded from env is invalid:", err)
		}
		nlog.Infoln("Initialized", meta.Pname(pid), "from env")
		return pid
	}

	// 2. ID from Kubernetes Node name.
	if k8s.IsK8s() {
		pid := cos.HashK8sProxyID(k8s.NodeName)
		nlog.Infoln("Initialized", meta.Pname(pid), "from K8s node nam", k8s.NodeName)
		return pid
	}

	// 3. Read ID from persistent file.
	if pid := readProxyID(config); pid != "" {
		nlog.Infof("Initialized %s from file %q", meta.Pname(pid), fname.ProxyID)
		return pid
	}

	// 4. initial deployment
	pid := genDaemonID(apc.Proxy, config)
	err := cos.ValidateDaemonID(pid)
	debug.AssertNoErr(err)

	// store ID on disk
	if err := os.WriteFile(filepath.Join(config.ConfigDir, fname.ProxyID), []byte(pid), cos.PermRWR); err != nil {
		cos.ExitLog("failed to write PID:", err, "[", meta.Pname(pid), "]")
	}
	nlog.Infoln("Initialized", meta.Pname(pid), "from randomly generated ID")
	return pid
}

func readProxyID(config *cmn.Config) (pid string) {
	if b, err := os.ReadFile(filepath.Join(config.ConfigDir, fname.ProxyID)); err == nil {
		pid = string(b)
	} else if !cos.IsNotExist(err) {
		nlog.Errorln(err)
	}
	return pid
}

func (p *proxy) pready(smap *smapX, withRR bool /* also check readiness to rebalance */) error {
	const msg = "%s primary: not ready yet "
	debug.Assert(smap == nil || smap.IsPrimary(p.si))

	if !p.ClusterStarted() {
		return fmt.Errorf(msg+"(cluster is starting up)", p)
	}
	if withRR && p.owner.rmd.starting.Load() {
		return fmt.Errorf(msg+"(finalizing global rebalancing state)", p)
	}
	return nil
}

// start proxy runner
func (p *proxy) Run() error {
	config := cmn.GCO.Get()
	p.htrun.init(config)
	p.setusr1()
	p.owner.bmd = newBMDOwnerPrx(config)
	p.owner.etl = newEtlMDOwnerPrx(config)

	p.owner.bmd.init() // initialize owner and load BMD
	p.owner.etl.init() // initialize owner and load EtlMD

	core.Pinit()

	// startup sequence - see earlystart.go for the steps and commentary
	p.bootstrap()

	p.authn = newAuthManager(config)

	p.rproxy.init()

	p.notifs.init(p)
	p.ic.init(p)

	//
	// REST API: register proxy handlers and start listening
	//
	networkHandlers := []networkHandler{
		{r: apc.Reverse, h: p.reverseHandler, net: accessNetPublicControl},

		// pubnet handlers: cluster must be started
		{r: apc.Buckets, h: p.bucketHandler, net: accessNetPublic},
		{r: apc.Objects, h: p.objectHandler, net: accessNetPublic},
		{r: apc.Download, h: p.dloadHandler, net: accessNetPublic},
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
		{r: apc.EC, h: p.ecHandler, net: accessNetIntraControl},

		// machine learning
		{r: apc.ML, h: p.mlHandler, net: accessNetPublic},

		// S3 compatibility
		{r: "/" + apc.S3, h: p.s3Handler, net: accessNetPublic},

		// "easy URL"
		{r: "/" + apc.GSScheme, h: p.easyURLHandler, net: accessNetPublic},
		{r: "/" + apc.AZScheme, h: p.easyURLHandler, net: accessNetPublic},
		{r: "/" + apc.AISScheme, h: p.easyURLHandler, net: accessNetPublic},

		// S3 compatibility, depending on feature flag
		{r: "/", h: p.rootHandler, net: accessNetPublic},
	}
	p.regNetHandlers(networkHandlers)

	nlog.Infoln(cmn.NetPublic+":", "\t\t", p.si.PubNet.URL)
	if p.si.PubNet.URL != p.si.ControlNet.URL {
		nlog.Infoln(cmn.NetIntraControl+":", "\t", p.si.ControlNet.URL)
	}
	if p.si.PubNet.URL != p.si.DataNet.URL {
		nlog.Infoln(cmn.NetIntraData+":", "\t", p.si.DataNet.URL)
	}

	dsort.Pinit(p, config)

	return p.htrun.run(config)
}

func (p *proxy) joinCluster(action string, primaryURLs ...string) (status int, err error) {
	res, err := p.join(nil /*htext*/, primaryURLs...)
	if err != nil {
		return status, err
	}
	defer freeCR(res)
	if res.err != nil {
		return res.status, res.err
	}
	// not being sent at cluster startup and keepalive
	if len(res.bytes) == 0 {
		return
	}
	err = p.recvCluMetaBytes(action, res.bytes, "")
	return
}

// apart from minor, albeit subtle, differences between `t.joinCluster` vs `p.joinCluster`
// this method is otherwise identical to t.gojoin (TODO: unify)
func (p *proxy) gojoin(config *cmn.Config) {
	var (
		smap      = p.owner.smap.get()
		pub, ctrl string
	)
	if smap.Primary != nil && smap.Version > 0 {
		pub = smap.Primary.URL(cmn.NetPublic)
		ctrl = smap.Primary.URL(cmn.NetIntraControl)
	}
	nsti := p.pollClusterStarted(config, smap.Primary)
	if nlog.Stopping() {
		return
	}

	if nsti != nil {
		// (primary changed)
		pub, ctrl = nsti.Smap.Primary.PubURL, nsti.Smap.Primary.CtrlURL
		if status, err := p.joinCluster(apc.ActSelfJoinProxy, ctrl, pub); err != nil {
			nlog.Errorf(fmtFailedRejoin, p, err, status)
			return
		}
	}

	// normally, immediately return with "is ready";
	// otherwise, handle: (not present in cluster map | net-info changed)
	i, sleep, total := 2, config.Timeout.MaxKeepalive.D(), config.Timeout.MaxHostBusy.D()
	for total >= 0 {
		smap = p.owner.smap.get()
		si := smap.GetNode(p.SID())
		if si == nil {
			nlog.Errorf(fmtSelfNotPresent, p, smap.StringEx())
		} else {
			nerr := si.NetEq(p.si)
			if nerr == nil {
				p.markClusterStarted()
				nlog.Infoln(p.String(), "is ready")
				return // ok ---
			}
			nlog.Warningln(p.String(), "- trying to rejoin and, simultaneously, have the primary to update net-info:")
			nlog.Warningln("\t", nerr, smap.StringEx())
		}

		if nlog.Stopping() {
			return
		}
		time.Sleep(sleep)
		i++
		total -= sleep
		smap = p.owner.smap.get()
		if ctrl == "" && smap.Primary != nil && smap.Version > 0 {
			pub = smap.Primary.URL(cmn.NetPublic)
			ctrl = smap.Primary.URL(cmn.NetIntraControl)
		}
		nlog.Warningln(p.String(), "- attempt number", i, "to join")
		if status, err := p.joinCluster(apc.ActSelfJoinProxy, ctrl, pub); err != nil {
			nlog.Errorf(fmtFailedRejoin, p, err, status)
			return
		}
	}

	p.markClusterStarted()
	nlog.Infoln(p.String(), "is ready(?)")
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
		self = p.String() + ":"
		errs []error
	)
	if cm.PrimeTime != 0 {
		xreg.PrimeTime.Store(cm.PrimeTime)
		xreg.MyTime.Store(time.Now().UnixNano())
	}
	// Config
	if cm.Config == nil {
		err := fmt.Errorf(self+" invalid %T (nil config): %+v", cm, cm)
		nlog.Errorln(err)
		return err
	}
	if err := p.receiveConfig(cm.Config, msg, nil, caller); err != nil {
		if !isErrDowngrade(err) {
			errs = append(errs, err)
			nlog.Errorln(err)
		}
	} else {
		nlog.Infoln(self, tagCM, action, cm.Config.String())
	}
	// Smap
	if err := p.receiveSmap(cm.Smap, msg, nil /*ms payload*/, caller, p.smapOnUpdate); err != nil {
		if !isErrDowngrade(err) {
			errs = append(errs, err)
			nlog.Errorln(err)
		}
	} else if cm.Smap != nil {
		nlog.Infoln(self, tagCM, action, cm.Smap.String())
	}
	// BMD
	if err := p.receiveBMD(cm.BMD, msg, nil, caller); err != nil {
		if !isErrDowngrade(err) {
			errs = append(errs, err)
			nlog.Errorln(err)
		}
	} else {
		nlog.Infoln(self, tagCM, action, cm.BMD.String())
	}
	// RMD
	if err := p.receiveRMD(cm.RMD, msg, caller); err != nil {
		if !isErrDowngrade(err) {
			errs = append(errs, err)
			nlog.Errorln(err)
		}
	} else {
		nlog.Infoln(self, tagCM, action, cm.RMD.String())
	}
	// EtlMD
	if err := p.receiveEtlMD(cm.EtlMD, msg, nil, caller, nil); err != nil {
		if !isErrDowngrade(err) {
			errs = append(errs, err)
			nlog.Errorln(err)
		}
	} else if cm.EtlMD != nil {
		nlog.Infoln(self, tagCM, action, cm.EtlMD.String())
	}

	switch {
	case errs == nil:
		return nil
	case len(errs) == 1:
		return errs[0]
	default:
		s := fmt.Sprintf("%v", errs)
		return cmn.NewErrFailedTo(p, action, tagCM, errors.New(s))
	}
}

// parse request + init/lookup bucket (combo)
func (p *proxy) _parseReqTry(w http.ResponseWriter, r *http.Request, bckArgs *bctx) (bck *meta.Bck,
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
	freeBctx(bckArgs) // caller does alloc
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
		dpq := dpqAlloc()
		p.httpbckget(w, r, dpq)
		dpqFree(dpq)
	case http.MethodDelete:
		apireq := apiReqAlloc(1, apc.URLPathBuckets.L, false /*dpq*/)
		p.httpbckdelete(w, r, apireq)
		apiReqFree(apireq)
	case http.MethodPut:
		p.httpbckput(w, r)
	case http.MethodPost:
		p.httpbckpost(w, r)
	case http.MethodHead:
		apireq := apiReqAlloc(1, apc.URLPathBuckets.L, true /*dpq*/)
		p.httpbckhead(w, r, apireq)
		apiReqFree(apireq)
	case http.MethodPatch:
		apireq := apiReqAlloc(1, apc.URLPathBuckets.L, false /*dpq*/)
		p.httpbckpatch(w, r, apireq)
		apiReqFree(apireq)
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
		apireq := apiReqAlloc(2, apc.URLPathObjects.L, true /*dpq*/)
		p.httpobjput(w, r, apireq)
		apiReqFree(apireq)
	case http.MethodDelete:
		p.httpobjdelete(w, r)
	case http.MethodPost:
		apireq := apiReqAlloc(1, apc.URLPathObjects.L, false /*dpq*/)
		p.httpobjpost(w, r, apireq)
		apiReqFree(apireq)
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
//	S3 compatibility (see https://github.com/NVIDIA/aistore/blob/main/docs/s3compat.md)
//	shall not be confused with a simple alternative URL Path mapping via easyURLHandler,
//	whereby a path (e.g.) "gs/mybucket/myobject" gets replaced with
//	"v1/objects/mybucket/myobject?provider=gcp" with _no_ other changes to the request
//	and response parameters and components.
func (p *proxy) easyURLHandler(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.parseURL(w, r, nil, 1, true)
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
			query := make(url.Values, 1)
			qbck.SetQuery(query)
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
		query := make(url.Values, 1)
		bck.SetQuery(query)
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

// +gen:endpoint GET /v1/buckets/{bucket-name}[apc.QparamProvider=string,apc.QparamNamespace=string]
// List buckets or list objects within a bucket
func (p *proxy) httpbckget(w http.ResponseWriter, r *http.Request, dpq *dpq) {
	var (
		msg     *apc.ActMsg
		bckName string
	)
	apiItems, err := p.parseURL(w, r, apc.URLPathBuckets.L, 0, true)
	if err != nil {
		return
	}
	if len(apiItems) > 0 {
		bckName = apiItems[0]
	}
	ctype := r.Header.Get(cos.HdrContentType)
	if r.ContentLength == 0 && !strings.HasPrefix(ctype, cos.ContentJSON) {
		// e.g. "easy URL" request: curl -L -X GET 'http://aistore/ais/abc'
		msg = &apc.ActMsg{Action: apc.ActList, Value: &apc.LsoMsg{}}
	} else if msg, err = p.readActionMsg(w, r); err != nil {
		return
	}
	if err := dpq.parse(r.URL.RawQuery); err != nil {
		p.writeErr(w, r, err)
		return
	}

	qbck, errV := newQbckFromQ(bckName, nil, dpq)
	if errV != nil {
		p.writeErr(w, r, errV)
		return
	}

	// switch (I) through (IV) --------------------------

	// (I) summarize buckets
	if msg.Action == apc.ActSummaryBck {
		var summMsg apc.BsummCtrlMsg
		if err := cos.MorphMarshal(msg.Value, &summMsg); err != nil {
			p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
			return
		}
		summMsg.Prefix = cos.TrimPrefix(summMsg.Prefix)
		if qbck.IsBucket() {
			bck := (*meta.Bck)(qbck)
			bckArgs := bctx{p: p, w: w, r: r, msg: msg, perms: apc.AceBckHEAD, bck: bck, dpq: dpq}
			bckArgs.createAIS = false
			bckArgs.dontHeadRemote = summMsg.BckPresent
			if _, err := bckArgs.initAndTry(); err != nil {
				return
			}
		}
		p.bsummact(w, r, qbck, &summMsg)
		return
	}

	// (II) invalid action
	if msg.Action != apc.ActList {
		p.writeErrAct(w, r, msg.Action)
		return
	}

	// (III) list buckets
	if msg.Value == nil {
		if qbck.Name != "" && qbck.Name != msg.Name {
			p.writeErrf(w, r, "bad list-buckets request: %q vs %q (%+v, %+v)", qbck.Name, msg.Name, qbck, msg)
			return
		}
		qbck.Name = msg.Name
		if qbck.IsRemoteAIS() {
			qbck.Ns.UUID = p.a2u(qbck.Ns.UUID)
		}
		if err := p.checkAccess(w, r, nil, apc.AceListBuckets); err == nil {
			p.listBuckets(w, r, qbck, msg, dpq)
		}
		return
	}

	// (IV) list objects (NOTE -- TODO: currently, always forwarding)
	if !qbck.IsBucket() {
		p.writeErrf(w, r, "bad list-objects request: %q is not a bucket (is a bucket query?)", qbck.String())
		return
	}
	if p.forwardCP(w, r, msg, lsotag+" "+qbck.String()) {
		return
	}

	// lsmsg
	var (
		lsmsg apc.LsoMsg
		bck   = meta.CloneBck((*cmn.Bck)(qbck))
	)
	if err := cos.MorphMarshal(msg.Value, &lsmsg); err != nil {
		p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
		return
	}
	lsmsg.Prefix = cos.TrimPrefix(lsmsg.Prefix)
	if err := cos.ValidatePrefix("bad list-objects request", lsmsg.Prefix); err != nil {
		p.statsT.IncBck(stats.ErrListCount, bck.Bucket())
		p.writeErr(w, r, err)
		return
	}
	bckArgs := bctx{p: p, w: w, r: r, msg: msg, perms: apc.AceObjLIST, bck: bck, dpq: dpq}
	bckArgs.createAIS = false

	if lsmsg.IsFlagSet(apc.LsBckPresent) {
		bckArgs.dontHeadRemote = true
		bckArgs.dontAddRemote = true
	} else {
		bckArgs.tryHeadRemote = lsmsg.IsFlagSet(apc.LsDontHeadRemote)
		bckArgs.dontAddRemote = lsmsg.IsFlagSet(apc.LsDontAddRemote)
	}

	// do
	bck, errN := bckArgs.initAndTry()
	if errN != nil {
		return
	}
	p.listObjects(w, r, bck, msg /*amsg*/, &lsmsg)
}

// +gen:endpoint GET /v1/objects/{bucket-name}/{object-name}[apc.QparamProvider=string,apc.QparamNamespace=string,apc.QparamOrigURL=string,apc.QparamLatestVer=bool]
// Retrieve the object content with the given uname
func (p *proxy) httpobjget(w http.ResponseWriter, r *http.Request, origURLBck ...string) {
	// 1. request
	apireq := apiReqAlloc(2, apc.URLPathObjects.L, true /*dpq*/)
	if err := p.parseReq(w, r, apireq); err != nil {
		apiReqFree(apireq)
		return
	}

	// 2. bucket
	bckArgs := allocBctx()
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
	freeBctx(bckArgs)

	objName := apireq.items[1]
	apiReqFree(apireq)
	if err != nil {
		return
	}

	if err := cos.ValidOname(objName); err != nil {
		p.statsT.IncBck(stats.ErrGetCount, bck.Bucket())
		p.writeErr(w, r, err)
		return
	}

	started := time.Now()

	// 3. rate limit
	smap := p.owner.smap.get()
	if err := p.ratelimit(bck, http.MethodGet, smap); err != nil {
		p.writeErr(w, r, err, http.StatusTooManyRequests, Silent)
		return
	}

	// 4. redirect
	tsi, netPub, err := smap.HrwMultiHome(bck.MakeUname(objName))
	if err != nil {
		p.statsT.IncBck(stats.ErrGetCount, bck.Bucket())
		p.writeErr(w, r, err)
		return
	}
	if cmn.Rom.FastV(5, cos.SmoduleAIS) {
		nlog.Infoln("GET", bck.Cname(objName), "=>", tsi.StringEx())
	}

	redirectURL := p.redirectURL(r, tsi, started, cmn.NetIntraData, netPub)
	http.Redirect(w, r, redirectURL, http.StatusMovedPermanently)

	// 5. stats
	p.statsT.IncBck(stats.GetCount, bck.Bucket())
}

// +gen:endpoint PUT /v1/objects/{bucket-name}/{object-name}[apc.QparamAppendType=string,apc.QparamAppendHandle=string]
// Create a new object with the given uname
func (p *proxy) httpobjput(w http.ResponseWriter, r *http.Request, apireq *apiRequest) {
	var (
		nodeID string
		verb   = "PUT"
		errcnt = stats.ErrPutCount
		scnt   = stats.PutCount
		perms  = apc.AcePUT
		vlabs  = map[string]string{stats.VlabBucket: "", stats.VlabXkind: ""}
	)
	// 1. request
	if err := p.parseReq(w, r, apireq); err != nil {
		return
	}
	if apireq.dpq.etl.name != "" { // apc.QparamEtlName
		if err := p.etlExists(apireq.dpq.etl.name); err != nil {
			p.writeErr(w, r, err)
			return
		}
	}
	appendTyProvided := apireq.dpq.apnd.ty != "" // apc.QparamAppendType
	if appendTyProvided {
		verb = "APPEND"
		perms = apc.AceAPPEND
		errcnt = stats.ErrAppendCount
		scnt = stats.AppendCount
		vlabs = map[string]string{stats.VlabBucket: ""}
		if apireq.dpq.apnd.hdl != "" {
			items, err := preParse(apireq.dpq.apnd.hdl) // apc.QparamAppendHandle
			if err != nil {
				p.writeErr(w, r, err)
				return
			}
			nodeID = items[0] // nodeID; compare w/ apndOI.parse
		}
	}

	// 2. bucket
	bckArgs := allocBctx()
	{
		bckArgs.p = p
		bckArgs.w = w
		bckArgs.r = r
		bckArgs.perms = perms
		bckArgs.createAIS = false
	}
	bckArgs.bck, bckArgs.dpq = apireq.bck, apireq.dpq
	bck, err := bckArgs.initAndTry()
	freeBctx(bckArgs)
	if err != nil {
		return
	}
	vlabs[stats.VlabBucket] = bck.Cname("")

	// 3. rate limit
	smap := p.owner.smap.get()
	if err := p.ratelimit(bck, http.MethodPut, smap); err != nil {
		p.writeErr(w, r, err, http.StatusTooManyRequests, Silent)
		return
	}

	// 4. redirect
	var (
		tsi     *meta.Snode
		started = time.Now()
		objName = apireq.items[1]
		netPub  = cmn.NetPublic
	)
	if err := cos.ValidOname(objName); err != nil {
		p.statsT.IncWith(errcnt, vlabs)
		p.writeErr(w, r, err)
		return
	}

	if nodeID == "" {
		tsi, netPub, err = smap.HrwMultiHome(bck.MakeUname(objName))
		if err != nil {
			p.statsT.IncWith(errcnt, vlabs)
			p.writeErr(w, r, err)
			return
		}
	} else {
		if tsi = smap.GetTarget(nodeID); tsi == nil {
			p.statsT.IncWith(errcnt, vlabs)
			err = &errNodeNotFound{p.si, smap, verb + " failure:", nodeID}
			p.writeErr(w, r, err)
			return
		}
	}

	// verbose
	if cmn.Rom.FastV(5, cos.SmoduleAIS) {
		nlog.Infoln(verb, bck.Cname(objName), "=>", tsi.StringEx())
	}

	redirectURL := p.redirectURL(r, tsi, started, cmn.NetIntraData, netPub)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)

	// 5. stats
	p.statsT.IncWith(scnt, vlabs)
}

// +gen:endpoint DELETE /v1/objects/{bucket-name}/{object-name}[apc.QparamProvider=string,apc.QparamNamespace=string]
// Delete an object with the given uname
func (p *proxy) httpobjdelete(w http.ResponseWriter, r *http.Request) {
	bckArgs := allocBctx()
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
	if err := cos.ValidOname(objName); err != nil {
		p.statsT.IncBck(stats.ErrDeleteCount, bck.Bucket())
		p.writeErr(w, r, err)
		return
	}

	// rate limit
	smap := p.owner.smap.get()
	if err := p.ratelimit(bck, http.MethodDelete, smap); err != nil {
		p.writeErr(w, r, err, http.StatusTooManyRequests, Silent)
		return
	}

	tsi, err := smap.HrwName2T(bck.MakeUname(objName))
	if err != nil {
		p.statsT.IncBck(stats.ErrDeleteCount, bck.Bucket())
		p.writeErr(w, r, err)
		return
	}
	if cmn.Rom.FastV(5, cos.SmoduleAIS) {
		nlog.Infoln("DELETE", bck.Cname(objName), "=>", tsi.StringEx())
	}
	redirectURL := p.redirectURL(r, tsi, time.Now() /*started*/, cmn.NetIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)

	p.statsT.IncBck(stats.DeleteCount, bck.Bucket())
}

// +gen:endpoint DELETE /v1/buckets/{bucket-name}[apc.QparamProvider=string,apc.QparamNamespace=string,apc.QparamKeepRemote=bool]
// Delete a bucket or delete/evict objects within a bucket
func (p *proxy) httpbckdelete(w http.ResponseWriter, r *http.Request, apireq *apiRequest) {
	// 1. request
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
	bckArgs := bctx{p: p, w: w, r: r, msg: msg, perms: perms, bck: bck, dpq: apireq.dpq, query: apireq.query}
	bckArgs.createAIS = false
	if msg.Action == apc.ActEvictRemoteBck {
		var ecode int
		bckArgs.dontHeadRemote = true // unconditionally
		ecode, err = bckArgs.init()
		if err != nil {
			if ecode != http.StatusNotFound && !cmn.IsErrRemoteBckNotFound(err) {
				p.writeErr(w, r, err, ecode)
			}
			return
		}
	} else if bck, err = bckArgs.initAndTry(); err != nil {
		return
	}

	// 3. action
	switch msg.Action {
	case apc.ActEvictRemoteBck:
		if err := cmn.ValidateRemoteBck(apc.ActEvictRemoteBck, bck.Bucket()); err != nil {
			p.writeErr(w, r, err)
			return
		}
		keepMD := cos.IsParseBool(apireq.query.Get(apc.QparamKeepRemote))
		if keepMD {
			if err := p.evictRemoteKeepMD(msg, bck); err != nil {
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
			// having removed bucket from BMD ask remote to do the same
			p.reverseRemAis(w, r, msg, bck.Bucket(), apireq.query)
			return
		}
		if err := p.destroyBucket(msg, bck); err != nil {
			if cmn.IsErrBckNotFound(err) {
				// TODO: return http.StatusNoContent
				nlog.Infof("%s: %s already %q-ed, nothing to do", p, bck.String(), msg.Action)
			} else {
				p.writeErr(w, r, err)
			}
		}
	case apc.ActDeleteObjects, apc.ActEvictObjects:
		if msg.Action == apc.ActEvictObjects {
			if err := cmn.ValidateRemoteBck(apc.ActEvictRemoteBck, bck.Bucket()); err != nil {
				p.writeErr(w, r, err)
				return
			}
		}
		xid, err := p.bcastMultiobj(r.Method, bck.Name, msg, apireq.query)
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		writeXid(w, xid)
	default:
		p.writeErrAct(w, r, msg.Action)
	}
}

// +gen:endpoint PUT /v1/metasync
// Internal metadata synchronization between cluster nodes
// (compare with p.recvCluMeta and t.metasyncHandlerPut)
func (p *proxy) metasyncHandler(w http.ResponseWriter, r *http.Request) {
	var (
		err  = &errMsync{}
		nsti = &err.Cii
	)
	if r.Method != http.MethodPut {
		cmn.WriteErr405(w, r, http.MethodPut)
		return
	}
	smap := p.owner.smap.get()

	if smap.isPrimary(p.si) {
		const txt = "cannot be on the receiving side of metasync"
		xctn := voteInProgress()
		maps := smap.StringEx()
		p.fillNsti(nsti)
		switch {
		case !p.ClusterStarted():
			err.Message = fmt.Sprintf("%s(self) %s, %s", p, "is starting up as primary, "+txt, maps)
		case xctn != nil:
			err.Message = fmt.Sprintf("%s(self) %s, %s", p, "is still primary while voting is in progress, "+txt, maps)
		default:
			err.Message = fmt.Sprintf("%s(self) %s, %s", p, "is primary, "+txt, maps)
		}
		nlog.Errorln(err.Message)
		// marshal along with nsti
		p.writeErr(w, r, errors.New(cos.MustMarshalToString(err)), http.StatusConflict, Silent)
		return
	}

	p.warnMsync(r, smap)

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
	p.fillNsti(nsti)
	retErr := err.message(errConf, errSmap, errBMD, errRMD, errEtlMD, errTokens)
	p.writeErr(w, r, retErr, http.StatusConflict)
}

func (p *proxy) syncNewICOwners(smap, newSmap *smapX) {
	if !smap.IsIC(p.si) || !newSmap.IsIC(p.si) {
		return
	}
	// async - not waiting
	for _, psi := range newSmap.Pmap {
		if p.SID() != psi.ID() && newSmap.IsIC(psi) && !smap.IsIC(psi) {
			go func(psi *meta.Snode) {
				if err := p.ic.sendOwnershipTbl(psi, newSmap); err != nil {
					nlog.Errorln(p.String()+": failed to send ownership table to", psi.String()+":", err)
				}
			}(psi)
		}
	}
}

// +gen:endpoint GET /v1/health[apc.QparamPrimaryReadyReb=bool,apc.QparamClusterInfo=bool,apc.QparamAskPrimary=bool]
// Get cluster and node health status
func (p *proxy) healthHandler(w http.ResponseWriter, r *http.Request) {
	if !p.NodeStarted() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}

	p.uptime2hdr(w.Header())

	var (
		prr, getCii, askPrimary bool
	)
	if r.URL.RawQuery != "" {
		query := r.URL.Query()
		prr = cos.IsParseBool(query.Get(apc.QparamPrimaryReadyReb))
		getCii = cos.IsParseBool(query.Get(apc.QparamClusterInfo))
		askPrimary = cos.IsParseBool(query.Get(apc.QparamAskPrimary))
	}

	if !prr {
		if responded := p.externalWD(w, r); responded {
			return
		}
	}
	// piggy-backing cluster info on health
	if getCii {
		if prr {
			err := fmt.Errorf("invalid query parameters: %q (internal use only) and %q", apc.QparamClusterInfo, apc.QparamPrimaryReadyReb)
			p.writeErr(w, r, err)
			return
		}
		nsti := &cos.NodeStateInfo{}
		p.fillNsti(nsti)
		p.writeJSON(w, r, nsti, "cluster-info")
		return
	}
	smap := p.owner.smap.get()
	if err := smap.validate(); err != nil {
		if !p.ClusterStarted() {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			p.writeErr(w, r, err, http.StatusServiceUnavailable)
		}
		return
	}

	callerID := r.Header.Get(apc.HdrCallerID)
	if callerID != "" && smap.GetProxy(callerID) != nil {
		p.keepalive.heardFrom(callerID)
	}

	// primary
	if smap.isPrimary(p.si) {
		if prr {
			if err := p.pready(smap, true); err != nil {
				if cmn.Rom.FastV(5, cos.SmoduleAIS) {
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
	if prr {
		if !p.forwardCP(w, r, nil /*msg*/, "health(prr)") {
			// (unlikely)
			p.writeErrMsg(w, r, "failing to forward health(prr) => primary", http.StatusServiceUnavailable)
		}
		return
	}
	if askPrimary {
		p.writeErrf(w, r, "%s (non-primary): misdirected health-of-primary request from %q, %s",
			p, callerID, smap.StringEx())
		return
	}
	w.WriteHeader(http.StatusOK)
}

// +gen:endpoint PUT /v1/buckets/{bucket-name}[apc.QparamProvider=string,apc.QparamNamespace=string]
// Perform actions on a bucket (like archiving)
func (p *proxy) httpbckput(w http.ResponseWriter, r *http.Request) {
	var (
		msg           *apc.ActMsg
		query         = r.URL.Query()
		apiItems, err = p.parseURL(w, r, apc.URLPathBuckets.L, 1, true)
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
	bckArgs := bctx{p: p, w: w, r: r, bck: bck, msg: msg, query: query}
	bckArgs.createAIS = false
	if bck, err = bckArgs.initAndTry(); err != nil {
		return
	}
	switch msg.Action {
	case apc.ActArchive:
		var (
			bckFrom = bck
			archMsg = &cmn.ArchiveBckMsg{}
		)
		if err := cos.MorphMarshal(msg.Value, archMsg); err != nil {
			p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
			return
		}
		bckTo := meta.CloneBck(&archMsg.ToBck)
		if bckTo.IsEmpty() {
			bckTo = bckFrom
		} else {
			bckToArgs := bctx{p: p, w: w, r: r, bck: bckTo, msg: msg, perms: apc.AcePUT, query: query}
			bckToArgs.createAIS = false
			if bckTo, err = bckToArgs.initAndTry(); err != nil {
				return
			}
		}
		//
		// NOTE: strict enforcement of the standard & supported file extensions
		//
		if _, err := archive.Strict(archMsg.Mime, archMsg.ArchName); err != nil {
			p.writeErr(w, r, err)
			return
		}
		xid, err := p.createArchMultiObj(bckFrom, bckTo, msg)
		if err == nil {
			writeXid(w, xid)
		} else {
			p.writeErr(w, r, err)
		}
	default:
		p.writeErrAct(w, r, msg.Action)
	}
}

// +gen:endpoint POST /v1/buckets/{bucket-name}[apc.QparamProvider=string,apc.QparamNamespace=string,apc.QparamBckTo=string,apc.QparamDontHeadRemote=bool] action=[apc.ActCopyBck=apc.TCBMsg|apc.ActETLBck=apc.TCBMsg]
// +gen:payload apc.ActCopyBck={"action": "copy-bck"}
// +gen:payload apc.ActETLBck={"action": "etl-bck", "value": {"id": "ETL_NAME"}}
// Create buckets or perform bucket operations (copy, move, etc.)
func (p *proxy) httpbckpost(w http.ResponseWriter, r *http.Request) {
	var msg *apc.ActMsg
	apiItems, err := p.parseURL(w, r, apc.URLPathBuckets.L, 1, true)
	if err != nil {
		return
	}
	if msg, err = p.readActionMsg(w, r); err != nil {
		return
	}
	bucket := apiItems[0]
	if len(apiItems) > 1 {
		err := cmn.InitErrHTTP(r, fmt.Errorf("invalid request URI %q", r.URL.Path), 0)
		p.writeErr(w, r, err)
		return
	}
	p._bckpost(w, r, msg, bucket)
}

func (p *proxy) _bckpost(w http.ResponseWriter, r *http.Request, msg *apc.ActMsg, bucket string) {
	const (
		warnDstNotExist = "%s: destination %s doesn't exist and will be created with the %s (source bucket) props"
		errPrependSync  = "prepend option (%q) is incompatible with the request to synchronize buckets"
	)
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
			p.reverseRemAis(w, r, msg, bck.Bucket(), query)
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

	bckArgs := bctx{p: p, w: w, r: r, bck: bck, perms: apc.AccessNone /* access checked below */, msg: msg, query: query}
	bckArgs.createAIS = false
	if bck, err = bckArgs.initAndTry(); err != nil {
		return
	}

	//
	// POST {action} on bucket
	//
	var xid string
	switch msg.Action {
	case apc.ActMoveBck:
		bckFrom := bck
		bckTo, err := newBckFromQuname(query, true /*required*/)
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		if !bckFrom.IsAIS() && bckFrom.Backend() == nil {
			p.writeErrf(w, r, "can only rename AIS ('ais://') bucket (%q is not)", bckFrom.Cname(""))
			return
		}
		if bckTo.IsRemote() {
			p.writeErrf(w, r, "can only rename to AIS ('ais://') bucket (%q is remote)", bckTo.Cname(""))
			return
		}
		if bckFrom.Equal(bckTo, false, false) {
			p.writeErrf(w, r, "cannot rename bucket %q to itself (%q)", bckFrom.Cname(""), bckTo.Cname(""))
			return
		}
		bckFrom.Provider, bckTo.Provider = apc.AIS, apc.AIS
		if _, present := p.owner.bmd.get().Get(bckTo); present {
			err := cmn.NewErrBckAlreadyExists(bckTo.Bucket())
			p.writeErr(w, r, err)
			return
		}
		if err := p.checkAccess(w, r, nil, apc.AceMoveBucket); err != nil {
			return
		}
		nlog.Infof("%s bucket %s => %s", msg.Action, bckFrom.String(), bckTo.String())
		if xid, err = p.renameBucket(bckFrom, bckTo, msg); err != nil {
			p.writeErr(w, r, err)
			return
		}
	case apc.ActCopyBck, apc.ActETLBck:
		var (
			bckFrom     = bck
			bckTo       *meta.Bck
			tcbmsg      = &apc.TCBMsg{}
			ecode       int
			fltPresence = apc.FltPresent
		)
		if err := cos.MorphMarshal(msg.Value, tcbmsg); err != nil {
			p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
			return
		}
		if msg.Action == apc.ActETLBck {
			if err := p.etlExists(tcbmsg.Transform.Name); err != nil {
				p.writeErr(w, r, err, http.StatusNotFound)
				return
			}
		}
		if tcbmsg.Sync && tcbmsg.Prepend != "" {
			p.writeErrf(w, r, errPrependSync, tcbmsg.Prepend)
			return
		}
		bckTo, err = newBckFromQuname(query, true /*required*/)
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		tcbmsg.Prefix = cos.TrimPrefix(tcbmsg.Prefix)
		if bckFrom.Equal(bckTo, true, true) {
			if !bckFrom.IsRemote() {
				p.writeErrf(w, r, "cannot %s bucket %q onto itself", msg.Action, bckFrom.Cname(""))
				return
			}
			nlog.Infoln("proceeding to copy remote", bckFrom.String())
		}

		bckTo, ecode, err = p.initBckTo(w, r, query, bckTo)
		if err != nil {
			return
		}
		if ecode == http.StatusNotFound {
			if p.forwardCP(w, r, msg, bucket) { // to create
				return
			}
			if err := p.checkAccess(w, r, nil, apc.AceCreateBucket); err != nil {
				return
			}
			nlog.Infof(warnDstNotExist, p, bckTo, bckFrom)
		}

		// start x-tcb or x-tco
		if v := query.Get(apc.QparamFltPresence); v != "" {
			fltPresence, _ = strconv.Atoi(v)
		}
		debug.Assertf(fltPresence != apc.FltExistsOutside, "(flt %d=\"outside\") not implemented yet", fltPresence)

		// [NOTE]
		// - when an "entire bucket" x-tcb job suddenly becomes a "multi-object" x-tco (next case in the switch)
		// - pros: lists remote bucket only once, and distributes relevant (listed) content to each target
		// - cons: given tcbmsg.Sync (--sync) option, x-tco that is from-starters "x-tco" deletes deleted content -
		//         this one does not

		if !apc.IsFltPresent(fltPresence) && (bckFrom.IsCloud() || bckFrom.IsRemoteAIS()) {
			lstcx := &lstcx{
				p:       p,
				bckFrom: bckFrom,
				bckTo:   bckTo,
				amsg:    msg,
				config:  cmn.GCO.Get(),
			}
			lstcx.tcomsg.TCBMsg = *tcbmsg
			nlog.Infoln("x-tco:", bckFrom.String(), "=>", bckTo.String(), "[", tcbmsg.Prefix, tcbmsg.LatestVer, tcbmsg.Sync, "]")
			xid, err = lstcx.do()
		} else {
			nlog.Infoln("x-tcb:", bckFrom.String(), "=>", bckTo.String(), "[", tcbmsg.Prefix, tcbmsg.LatestVer, tcbmsg.Sync, "]")
			xid, err = p.tcb(bckFrom, bckTo, msg, tcbmsg.DryRun)
		}
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
	case apc.ActCopyObjects, apc.ActETLObjects:
		var (
			tcomsg = &cmn.TCOMsg{}
			bckTo  *meta.Bck
			ecode  int
			eq     bool
		)
		if err = cos.MorphMarshal(msg.Value, tcomsg); err != nil {
			p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
			return
		}
		if msg.Action == apc.ActETLBck {
			if err := p.etlExists(tcomsg.Transform.Name); err != nil {
				p.writeErr(w, r, err, http.StatusNotFound)
				return
			}
		}
		if tcomsg.Sync && tcomsg.Prepend != "" {
			p.writeErrf(w, r, errPrependSync, tcomsg.Prepend)
			return
		}
		tcomsg.Prefix = cos.TrimPrefix(tcomsg.Prefix) // trim trailing wildcard
		bckTo = meta.CloneBck(&tcomsg.ToBck)

		if bck.Equal(bckTo, true, true) {
			eq = true
			nlog.Warningf("multi-object operation %q within the same bucket %q", msg.Action, bck)
		}
		if bckTo.IsHT() {
			p.writeErrf(w, r, "cannot %s to HTTP bucket %q", msg.Action, bckTo.Cname(""))
			return
		}
		if !eq {
			bckTo, ecode, err = p.initBckTo(w, r, query, bckTo)
			if err != nil {
				return
			}
			if ecode == http.StatusNotFound {
				if p.forwardCP(w, r, msg, bucket) { // to create
					return
				}
				if err := p.checkAccess(w, r, nil, apc.AceCreateBucket); err != nil {
					return
				}
				nlog.Infof(warnDstNotExist, p, bckTo, bck)
			}
		}

		xid, err = p.tcobjs(bck, bckTo, cmn.GCO.Get(), msg, tcomsg)
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
	case apc.ActAddRemoteBck:
		if err := p.checkAccess(w, r, nil, apc.AceCreateBucket); err != nil {
			return
		}
		if err := p.createBucket(msg, bck, nil); err != nil {
			p.writeErr(w, r, err, crerrStatus(err))
		}
		return
	case apc.ActPrefetchObjects:
		// TODO: GET vs SYNC?
		if err := cmn.ValidateRemoteBck(apc.ActPrefetchObjects, bck.Bucket()); err != nil {
			p.writeErr(w, r, err)
			return
		}
		if xid, err = p.bcastMultiobj(r.Method, bucket, msg, query); err != nil {
			p.writeErr(w, r, err)
			return
		}
	case apc.ActMakeNCopies:
		if xid, err = p.makeNCopies(msg, bck); err != nil {
			p.writeErr(w, r, err)
			return
		}
	case apc.ActECEncode:
		if cmn.Rom.EcStreams() > 0 {
			if err = p.ec.on(p, p.ec.timeout()); err != nil {
				p.writeErr(w, r, err)
				return
			}
		}
		if xid, err = p.ecEncode(bck, msg); err != nil {
			p.writeErr(w, r, err)
			return
		}
	default:
		p.writeErrAct(w, r, msg.Action)
		return
	}

	debug.Assertf(xact.IsValidUUID(xid) || strings.IndexByte(xid, ',') > 0, "%q: %q", msg.Action, xid)
	writeXid(w, xid)
}

// init existing or create remote
// not calling `initAndTry` - delegating ais:from// props cloning to the separate method
func (p *proxy) initBckTo(w http.ResponseWriter, r *http.Request, query url.Values, bckTo *meta.Bck) (*meta.Bck, int, error) {
	bckToArgs := bctx{p: p, w: w, r: r, bck: bckTo, perms: apc.AcePUT, query: query}
	bckToArgs.createAIS = true

	ecode, err := bckToArgs.init()
	if err == nil {
		return bckTo, 0, p.onEC(bckTo) // compare with `initAndTry`
	}

	if ecode != http.StatusNotFound {
		p.writeErr(w, r, err, ecode)
		return nil, 0, err
	}

	// remote bucket: create it (BMD-wise) on the fly
	if ecode == http.StatusNotFound && bckTo.IsRemote() {
		if bckTo, err = bckToArgs.try(); err != nil {
			return nil, 0, err
		}
		ecode = 0
	}
	return bckTo, ecode, nil
}

func (p *proxy) _bcr(w http.ResponseWriter, r *http.Request, query url.Values, msg *apc.ActMsg, bck *meta.Bck) {
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

	if bck.IsRemote() {
		// (feature) add Cloud bucket to BMD, to further set its `Props.Extra`
		// with alternative access profile and/or endpoint
		// TODO:
		// change bucket props - and the BMD meta-version - to have Flags int64 for
		// the bits that'll include "renamed" (instead of the current `Props.Renamed`)
		// and "added-with-no-head"; use the latter to synchronize Cloud props once
		// connected
		if cos.IsParseBool(query.Get(apc.QparamDontHeadRemote)) {
			if !bck.IsCloud() {
				p.writeErr(w, r, cmn.NewErrUnsupp("skip lookup for the", bck.Provider+":// bucket"))
				return
			}
			msg.Action = apc.ActAddRemoteBck // NOTE: substituting action in the message

			// NOTE: inherit cluster defaults
			config := cmn.GCO.Get()
			bprops := bck.Bucket().DefaultProps(&config.ClusterConfig)
			bprops.SetProvider(bck.Provider)

			if err := p._createBucketWithProps(msg, bck, bprops); err != nil {
				p.writeErr(w, r, err, crerrStatus(err))
			}
			return
		}

		// remote: check existence and get (cloud) props
		rhdr, code, err := p.headRemoteBck(bck.RemoteBck(), nil)
		if err != nil {
			if msg.Action == apc.ActCreateBck && (code == http.StatusNotFound || code == http.StatusBadRequest) {
				code = http.StatusNotImplemented
				err = cmn.NewErrNotImpl("create", bck.Provider+" bucket")
			}
			p.writeErr(w, r, err, code)
			return
		}
		remoteHdr = rhdr
		msg.Action = apc.ActAddRemoteBck // ditto
	}
	// props-to-update at creation time
	if msg.Value != nil {
		propsToUpdate := cmn.BpropsToSet{}
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
				p.writeErrf(w, r, "cannot create %s: invalid backend %s, err: %v", bck.Cname(""), backend.Cname(""), err)
				return
			}
			// Initialize backend bucket.
			if err := backend.InitNoBackend(p.owner.bmd); err != nil {
				if !cmn.IsErrRemoteBckNotFound(err) {
					p.writeErrf(w, r, "cannot create %s: failing to initialize backend %s, err: %v",
						bck.Cname(""), backend.Cname(""), err)
					return
				}
				args := bctx{p: p, w: w, r: r, bck: backend, msg: msg, query: query}
				args.createAIS = false
				if _, err = args.try(); err != nil {
					return
				}
			}
		}
		// Send all props to the target
		msg.Value = bck.Props
	}
	if err := p.createBucket(msg, bck, remoteHdr); err != nil {
		p.writeErr(w, r, err, crerrStatus(err))
	}
}

func crerrStatus(err error) (ecode int) {
	switch err.(type) {
	case *cmn.ErrBucketAlreadyExists:
		ecode = http.StatusConflict
	case *cmn.ErrNotImpl:
		ecode = http.StatusNotImplemented
	}
	return
}

// +gen:endpoint POST /v1/objects/{bucket-name}/{object-name}[apc.QparamProvider=string,apc.QparamNamespace=string] action=[apc.ActPromote=apc.PromoteArgs|apc.ActBlobDl=apc.BlobMsg]
// +gen:payload apc.ActPromote={"action": "promote", "name": "/user/dir", "value": {"target": "234ed78", "trim_prefix": "/user/", "recurs": true, "keep": true}}
// +gen:payload apc.ActBlobDl={"action": "blob-download", "value": {"chunk-size": 10485760, "num-workers": 4}}
// Perform actions on objects (rename, promote, blob download, check lock)
func (p *proxy) httpobjpost(w http.ResponseWriter, r *http.Request, apireq *apiRequest) {
	msg, err := p.readActionMsg(w, r)
	if err != nil {
		return
	}
	if msg.Action == apc.ActRenameObject || msg.Action == apc.ActCheckLock {
		apireq.after = 2
	}
	if err := p.parseReq(w, r, apireq); err != nil {
		return
	}

	bck := apireq.bck
	bckArgs := bctx{p: p, w: w, r: r, msg: msg, perms: apc.AccessNone /* access checked below */, bck: bck}
	bckArgs.createAIS = false
	bckArgs.dontHeadRemote = true
	if _, err := bckArgs.initAndTry(); err != nil {
		return
	}

	switch msg.Action {
	case apc.ActRenameObject:
		if err := p.checkAccess(w, r, bck, apc.AceObjMOVE); err != nil {
			p.statsT.IncBck(stats.ErrRenameCount, bck.Bucket())
			return
		}
		if err := _checkObjMv(bck, msg, apireq); err != nil {
			p.statsT.IncBck(stats.ErrRenameCount, bck.Bucket())
			p.writeErr(w, r, err)
		}
		p.redirectAction(w, r, bck, apireq.items[1], msg)
		p.statsT.IncBck(stats.RenameCount, bck.Bucket())
	case apc.ActPromote:
		if err := p.checkAccess(w, r, bck, apc.AcePromote); err != nil {
			p.statsT.IncBck(stats.ErrRenameCount, bck.Bucket())
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
		args := &apc.PromoteArgs{}
		if err := cos.MorphMarshal(msg.Value, args); err != nil {
			p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
			return
		}
		var tsi *meta.Snode
		if args.DaemonID != "" {
			smap := p.owner.smap.get()
			if tsi = smap.GetTarget(args.DaemonID); tsi == nil {
				err := &errNodeNotFound{p.si, smap, apc.ActPromote + " failure:", args.DaemonID}
				p.writeErr(w, r, err)
				return
			}
		}
		xid, err := p.promote(bck, msg, tsi)
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		if xid != "" {
			writeXid(w, xid)
		}
	case apc.ActBlobDl:
		if err := p.checkAccess(w, r, bck, apc.AccessRW); err != nil {
			return
		}
		if err := cmn.ValidateRemoteBck(apc.ActBlobDl, bck.Bucket()); err != nil {
			p.writeErr(w, r, err)
			return
		}
		objName := msg.Name
		p.redirectAction(w, r, bck, objName, msg)
	case apc.ActCheckLock:
		if err := p.checkAccess(w, r, bck, apc.AccessRO); err != nil {
			return
		}
		p.redirectAction(w, r, bck, apireq.items[1], msg)
	default:
		p.writeErrAct(w, r, msg.Action)
	}
}

func _checkObjMv(bck *meta.Bck, msg *apc.ActMsg, apireq *apiRequest) error {
	if bck.IsRemote() {
		err := fmt.Errorf("invalid action %q: not supported for remote buckets (%s)", msg.Action, bck.String())
		return cmn.NewErrUnsuppErr(err)
	}
	if bck.Props.EC.Enabled {
		err := fmt.Errorf("invalid action %q: not supported for erasure-coded buckets (%s)", msg.Action, bck.String())
		return cmn.NewErrUnsuppErr(err)
	}
	objName, objNameTo := apireq.items[1], msg.Name
	if err := cos.ValidOname(objName); err != nil {
		return err
	}
	if err := cos.ValidateOname(objNameTo); err != nil {
		return err
	}
	if objName == objNameTo {
		return fmt.Errorf("cannot rename %s to self, nothing to do", bck.Cname(objName))
	}
	return nil
}

// +gen:endpoint HEAD /v1/buckets/{bucket-name}/[apc.QparamFltPresence=string,apc.QparamBinfoWithOrWithoutRemote=string,apc.QparamDontAddRemote=bool]
// Get bucket metadata and properties
// with additional preparsing step to support api.GetBucketInfo prefix
// (e.g. 'ais ls ais://nnn --summary --prefix=aaa/bbb')
func (p *proxy) httpbckhead(w http.ResponseWriter, r *http.Request, apireq *apiRequest) {
	var prefix string

	// preparse
	{
		items, err := p.parseURL(w, r, apireq.prefix, apireq.after, false)
		if err != nil {
			return
		}
		if i := strings.IndexByte(items[0], '/'); i > 0 {
			prefix = cos.TrimPrefix(items[0][i+1:])
			apireq.after = 2
		}
	}

	err := p.parseReq(w, r, apireq)
	if err != nil {
		return
	}
	bckArgs := bctx{p: p, w: w, r: r, bck: apireq.bck, perms: apc.AceBckHEAD, dpq: apireq.dpq, query: apireq.query}
	bckArgs.dontAddRemote = apireq.dpq.dontAddRemote // QparamDontAddRemote

	var (
		info        *cmn.BsummResult
		dpq         = apireq.dpq
		msg         apc.BsummCtrlMsg
		fltPresence int
		status      int
	)
	if dpq.fltPresence != "" {
		fltPresence, err = strconv.Atoi(dpq.fltPresence)
		if err != nil {
			p.writeErrf(w, r, "%s: parse 'flt-presence': %w", p, err)
			return
		}
		bckArgs.dontHeadRemote = bckArgs.dontHeadRemote || apc.IsFltPresent(fltPresence)
	}
	if dpq.binfo != "" { // QparamBinfoWithOrWithoutRemote
		msg = apc.BsummCtrlMsg{
			UUID:          dpq.uuid,
			Prefix:        prefix,
			ObjCached:     !cos.IsParseBool(dpq.binfo),
			BckPresent:    apc.IsFltPresent(fltPresence),
			DontAddRemote: dpq.dontAddRemote,
		}
		bckArgs.dontAddRemote = msg.DontAddRemote
	} else if prefix != "" {
		err := fmt.Errorf("invalid URL '%s': expecting just the bucket name, got '%s' and '%s'",
			r.URL.Path, apireq.bck.Name, prefix)
		p.writeErr(w, r, err)
		return
	}
	bckArgs.createAIS = false

	bck, err := bckArgs.initAndTry()
	if err != nil {
		return
	}

	// 1. bucket is present (and was present prior to this call), and we are done with it here
	if bckArgs.isPresent {
		if fltPresence == apc.FltExistsOutside {
			nlog.Warningf("bucket %s is present, flt %d=\"outside\" not implemented yet", bck.Cname(""), fltPresence)
		}
		if dpq.binfo != "" {
			info, status, err = p.bsummhead(bck, &msg)
			if err != nil {
				p.writeErr(w, r, err)
				return
			}
			if info != nil {
				info.IsBckPresent = true
			}
		}
		toHdr(w, bck, info, status, msg.UUID)
		return
	}

	// 2. bucket is remote and does exist
	debug.Assert(bck.IsRemote(), bck.String())
	debug.Assert(bckArgs.exists)

	// [filtering] when the bucket that must be present is not
	if apc.IsFltPresent(fltPresence) {
		toHdr(w, bck, nil, 0, "")
		return
	}

	var (
		bprops *cmn.Bprops
		bmd    = p.owner.bmd.get()
	)
	bprops, bckArgs.isPresent = bmd.Get(bck)
	if bprops != nil {
		// just added via bckArgs.initAndTry() above, with dontAdd == false
		bck.Props = bprops
	} // otherwise, keep bck.Props as per (#18995)

	if dpq.binfo != "" {
		info, status, err = p.bsummhead(bck, &msg)
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		if info != nil {
			info.IsBckPresent = true
		}
	}
	toHdr(w, bck, info, status, msg.UUID)
}

func toHdr(w http.ResponseWriter, bck *meta.Bck, info *cmn.BsummResult, status int, xid string) {
	hdr := w.Header()
	if bck.Props != nil {
		hdr.Set(apc.HdrBucketProps, cos.MustMarshalToString(bck.Props))
	}
	if info != nil {
		hdr.Set(apc.HdrBucketSumm, cos.MustMarshalToString(info))
	}
	if xid != "" {
		hdr.Set(apc.HdrXactionID, xid)
	}
	if status > 0 {
		w.WriteHeader(status)
	}
}

// +gen:endpoint PATCH /v1/buckets/{bucket-name}[apc.QparamProvider=string,apc.QparamNamespace=string]
// Update bucket properties and settings
func (p *proxy) httpbckpatch(w http.ResponseWriter, r *http.Request, apireq *apiRequest) {
	var (
		err           error
		msg           *apc.ActMsg
		propsToUpdate cmn.BpropsToSet
		xid           string
		nprops        *cmn.Bprops // complete instance of bucket props with propsToUpdate changes
	)
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
	bckArgs := bctx{p: p, w: w, r: r, bck: bck, msg: msg, skipBackend: true,
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
		// backend must exist, must init itself
		backendBck := meta.CloneBck(&nprops.BackendBck)
		backendBck.Props = nil

		args := bctx{p: p, w: w, r: r, bck: backendBck, msg: msg, dpq: apireq.dpq, query: apireq.query}
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
	if xid, err = p.setBprops(msg, bck, nprops); err != nil {
		p.writeErr(w, r, err)
		return
	}
	if xid != "" {
		writeXid(w, xid)
	}
}

// +gen:endpoint HEAD /v1/objects/{bucket-name}/{object-name}[apc.QparamProvider=string,apc.QparamNamespace=string,apc.QparamSilent=bool]
// Get object metadata and properties
func (p *proxy) httpobjhead(w http.ResponseWriter, r *http.Request, origURLBck ...string) {
	bckArgs := allocBctx()
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

	// TODO: control plane multihoming: return LRU data plane interface - here and elsewhere (bcast)

	smap := p.owner.smap.get()
	si, err := smap.HrwName2T(bck.MakeUname(objName))
	if err != nil {
		p.writeErr(w, r, err, http.StatusInternalServerError)
		return
	}
	if cmn.Rom.FastV(5, cos.SmoduleAIS) {
		nlog.Infoln(r.Method, bck.Cname(objName), "=>", si.StringEx())
	}
	redirectURL := p.redirectURL(r, si, time.Now() /*started*/, cmn.NetIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

// +gen:endpoint PATCH /v1/objects/{bucket-name}/{object-name}[apc.QparamProvider=string,apc.QparamNamespace=string]
// Update object metadata and custom properties
func (p *proxy) httpobjpatch(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	bckArgs := allocBctx()
	{
		bckArgs.p = p
		bckArgs.w = w
		bckArgs.r = r
		bckArgs.perms = apc.AceObjUpdate
		bckArgs.createAIS = false
	}
	bck, objName, err := p._parseReqTry(w, r, bckArgs)
	if err != nil {
		return
	}
	if err := cos.ValidOname(objName); err != nil {
		p.writeErr(w, r, err)
		return
	}
	smap := p.owner.smap.get()
	si, err := smap.HrwName2T(bck.MakeUname(objName))
	if err != nil {
		p.writeErr(w, r, err, http.StatusInternalServerError)
		return
	}
	if cmn.Rom.FastV(5, cos.SmoduleAIS) {
		nlog.Infoln(r.Method, bck.Cname(objName), "=>", si.StringEx())
	}
	redirectURL := p.redirectURL(r, si, started, cmn.NetIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

func (p *proxy) listBuckets(w http.ResponseWriter, r *http.Request, qbck *cmn.QueryBcks, msg *apc.ActMsg, dpq *dpq) {
	var (
		bmd     = p.owner.bmd.get()
		present bool
	)
	if qbck.IsAIS() || qbck.IsHT() {
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
	smap := p.owner.smap.get()
	si, err := smap.GetRandTarget()
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
	res := p.call(cargs, smap)
	freeCargs(cargs)

	if res.err != nil {
		err = res.toErr()
		p.writeErr(w, r, err, res.status, Silent) // always silent
		return
	}

	hdr := w.Header()
	hdr.Set(cos.HdrContentType, res.header.Get(cos.HdrContentType))
	hdr.Set(cos.HdrContentLength, strconv.Itoa(len(res.bytes)))
	if _, err := w.Write(res.bytes); err != nil {
		nlog.Warningln(err) // (broken pipe; benign)
	}
}

func (p *proxy) redirectURL(r *http.Request, si *meta.Snode, ts time.Time, netIntra string, netPubs ...string) string {
	var (
		nodeURL string
		netPub  = cmn.NetPublic
	)
	if len(netPubs) > 0 {
		netPub = netPubs[0]
	}
	if p.si.LocalNet == nil {
		nodeURL = si.URL(netPub)
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
			nodeURL = si.URL(netIntra)
		} else {
			nodeURL = si.URL(netPub)
		}
	}
	// fast path
	if !cmn.HasSpecialSymbols(r.URL.Path) {
		var (
			q = url.Values{
				apc.QparamPID:      []string{p.SID()},
				apc.QparamUnixTime: []string{cos.UnixNano2S(ts.UnixNano())},
			}
		)
		debug.Assertf(!strings.Contains(r.URL.Path, "%"), "path %q contains %%", r.URL.Path)
		if r.URL.RawQuery != "" {
			return nodeURL + r.URL.Path + "?" + r.URL.RawQuery + "&" + q.Encode()
		}
		return nodeURL + r.URL.Path + "?" + q.Encode()
	}

	// slow path
	// it is conceivable that at some future point we may need to url.Parse nodeURL
	// and then use both scheme and host from the parsed result; not now though (NOTE)
	var (
		scheme = "http"
		host   string
	)
	if strings.HasPrefix(nodeURL, "https://") {
		scheme = "https"
		host = strings.TrimPrefix(nodeURL, "https://")
	} else {
		host = strings.TrimPrefix(nodeURL, "http://")
	}
	q := r.URL.Query()
	q.Set(apc.QparamPID, p.SID())
	q.Set(apc.QparamUnixTime, cos.UnixNano2S(ts.UnixNano()))
	u := url.URL{
		Scheme:   scheme,
		Host:     host,
		Path:     r.URL.Path,
		RawQuery: q.Encode(),
	}
	return u.String()
}

// http-redirect(with-json-message)
func (p *proxy) redirectAction(w http.ResponseWriter, r *http.Request, bck *meta.Bck, objName string, msg *apc.ActMsg) {
	started := time.Now()
	smap := p.owner.smap.get()
	si, err := smap.HrwName2T(bck.MakeUname(objName))
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	if cmn.Rom.FastV(5, cos.SmoduleAIS) {
		nlog.Infoln(msg.Action, bck.Cname(objName), "=>", si.StringEx())
	}

	// 307 is the only way to http-redirect with the original JSON payload
	redirectURL := p.redirectURL(r, si, started, cmn.NetIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

func (p *proxy) bcastMultiobj(method, bucket string, msg *apc.ActMsg, query url.Values) (xid string, err error) {
	var (
		smap      = p.owner.smap.get()
		actMsgExt = p.newAmsg(msg, nil, cos.GenUUID())
		body      = cos.MustMarshal(actMsgExt)
		path      = apc.URLPathBuckets.Join(bucket)
	)
	nlb := xact.NewXactNL(actMsgExt.UUID, actMsgExt.Action, &smap.Smap, nil)
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
	xid = actMsgExt.UUID
	return
}

//
// /daemon handlers
//

// [METHOD] /v1/daemon
func (p *proxy) daemonHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		p.httpdaeget(w, r)
	case http.MethodPut:
		p.httpdaeput(w, r)
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
		bcks:  []*meta.Bck{meta.NewBck(renamedBucket, apc.AIS, cmn.NsGlobal)},
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
		// Already removed via the very first target calling here.
		return nil
	}
	if props.Renamed == "" {
		nlog.Errorf("%s: renamed bucket %s: unexpected props %+v", p, bck.Name, *bck.Props)
		ctx.terminate = true
		return nil
	}
	clone.del(bck)
	return nil
}

// +gen:endpoint GET /v1/daemon[apc.QparamWhat=string]
// Retrieve various cluster and node information based on the 'what' query parameter.
// Supports multiple types: BMD (bucket metadata), NodeStatsAndStatus, SysInfo, Smap (cluster map), and more.
// (compare w/ httpcluget)
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
	case apc.WhatNodeConfig, apc.WhatSmapVote, apc.WhatSnode, apc.WhatLog, apc.WhatNodeStats, apc.WhatMetricNames:
		p.htrun.httpdaeget(w, r, query, nil /*htext*/)

	case apc.WhatNodeStatsAndStatus:
		ds := p.statsAndStatus()
		daeStats := p.statsT.GetStats()
		ds.Tracker = daeStats.Tracker
		p.fillNsti(&ds.Cluster)
		p.writeJSON(w, r, ds, what)

	case apc.WhatSysInfo:
		p.writeJSON(w, r, apc.GetMemCPU(), what)

	case apc.WhatSmap:
		const retries = 16
		var (
			smap  = p.owner.smap.get()
			sleep = cmn.Rom.CplaneOperation() / 2
		)
		for i := 0; smap.validate() != nil && i < retries; i++ {
			if !p.NodeStarted() {
				time.Sleep(sleep)
				smap = p.owner.smap.get()
				if err := smap.validate(); err != nil {
					nlog.Errorf("%s is starting up, cannot return %s yet: %v", p, smap, err)
				}
				break
			}
			smap = p.owner.smap.get()
			time.Sleep(sleep)
		}
		if err := smap.validate(); err != nil {
			nlog.Errorf("%s: startup is taking unusually long time: %s (%v)", p, smap, err)
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		if checkReady := r.Header.Get(apc.HdrReadyToJoinClu); checkReady != "" {
			if err := p.readyToJoinClu(smap); err != nil {
				p.writeErr(w, r, err)
				return
			}
		}
		p.writeJSON(w, r, smap, what)
	default:
		p.htrun.httpdaeget(w, r, query, nil /*htext*/)
	}
}

func (p *proxy) readyToJoinClu(smap *smapX) error {
	switch {
	case !smap.IsPrimary(p.si):
		return newErrNotPrimary(p.si, smap)
	case cmn.GCO.Get().Rebalance.Enabled && smap.CountTargets() > 1:
		return errors.New(p.String() + ": please disable global rebalance for the duration of the critical (force-join) operation")
	case nlog.Stopping():
		return p.errStopping()
	}
	return p.pready(smap, true)
}

// +gen:endpoint PUT /v1/daemon[apc.QparamForce=bool]
// Configure daemon settings and perform daemon operations
func (p *proxy) httpdaeput(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.parseURL(w, r, apc.URLPathDae.L, 0, true)
	if err != nil {
		return
	}
	if err := p.checkAccess(w, r, nil, apc.AceAdmin); err != nil {
		return
	}
	// urlpath items
	if len(apiItems) > 0 {
		p.daeputItems(w, r, apiItems)
		return
	}
	// action-message
	query := r.URL.Query()
	msg, err := p.readActionMsg(w, r)
	if err != nil {
		return
	}

	// primary?
	switch msg.Action {
	case apc.ActStartMaintenance, apc.ActDecommissionCluster, apc.ActDecommissionNode, apc.ActShutdownNode, apc.ActShutdownCluster:
		smap := p.owner.smap.get()
		if !smap.isPrimary(p.si) {
			break
		}
		if msg.Action == apc.ActShutdownCluster {
			force := cos.IsParseBool(query.Get(apc.QparamForce))
			if force {
				break
			}
		}
		err = fmt.Errorf("primary %s: invalid action %q (node-level operation on primary?), %s", p, msg.Action, smap.StringEx())
		p.writeErr(w, r, err)
		return
	}

	switch msg.Action {
	case apc.ActSetConfig: // set-config #2 - via action message
		p.setDaemonConfigMsg(w, r, msg, query)
	case apc.ActResetConfig:
		if err := p.owner.config.resetDaemonConfig(); err != nil {
			p.writeErr(w, r, err)
		}
	case apc.ActRotateLogs:
		nlog.Flush(nlog.ActRotate)
	case apc.ActResetStats:
		errorsOnly := msg.Value.(bool)
		p.statsT.ResetStats(errorsOnly)

	case apc.ActStartMaintenance:
		if !p.ensureIntraControl(w, r, true /* from primary */) {
			return
		}
		p.termKalive(msg.Action)
	case apc.ActDecommissionCluster, apc.ActDecommissionNode:
		if !p.ensureIntraControl(w, r, true /* from primary */) {
			return
		}
		var opts apc.ActValRmNode
		if err := cos.MorphMarshal(msg.Value, &opts); err != nil {
			p.writeErr(w, r, err)
			return
		}
		p.termKalive(msg.Action)
		p.decommission(msg.Action, &opts)
	case apc.ActShutdownNode:
		if !p.ensureIntraControl(w, r, true /* from primary */) {
			return
		}
		p.termKalive(msg.Action)
		p.shutdown(msg.Action)
	case apc.ActShutdownCluster:
		smap := p.owner.smap.get()
		isPrimary := smap.isPrimary(p.si)
		if !isPrimary {
			if !p.ensureIntraControl(w, r, true /* from primary */) {
				return
			}
			p.Stop(&errNoUnregister{msg.Action})
			return
		}
		// (see "force" above)
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	case apc.LoadX509:
		p.daeLoadX509(w, r)
	default:
		p.writeErrAct(w, r, msg.Action)
	}
}

func (p *proxy) daeputItems(w http.ResponseWriter, r *http.Request, apiItems []string) {
	switch apiItems[0] {
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
		if err := p.owner.smap.synchronize(p.si, newsmap, nil /*ms payload*/, p.htrun.smapUpdatedCB); err != nil {
			p.writeErr(w, r, cmn.NewErrFailedTo(p, "synchronize", newsmap, err))
			return
		}
		nlog.Infof("%s: %s %s done", p, apc.SyncSmap, newsmap)
	case apc.ActSetConfig: // set-config #1 - via query parameters and "?n1=v1&n2=v2..."
		p.setDaemonConfigQuery(w, r)
	case apc.LoadX509:
		p.daeLoadX509(w, r)
	default:
		p.writeErrAct(w, r, apiItems[0])
	}
}

// +gen:endpoint POST /v1/daemon[apc.QparamPrimaryCandidate=string,apc.QparamPrepare=bool]
// Admin operations like joining cluster or forcing primary selection
func (p *proxy) httpdaepost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.parseURL(w, r, apc.URLPathDae.L, 0, true)
	if err != nil {
		return
	}
	if len(apiItems) == 0 {
		p.writeErrURL(w, r)
		return
	}
	if err := p.checkAccess(w, r, nil, apc.AceAdmin); err != nil {
		return
	}
	act := apiItems[0]
	if act == apc.ActPrimaryForce {
		p.daeForceJoin(w, r)
		return
	}
	if act != apc.AdminJoin {
		p.writeErrURL(w, r)
		return
	}
	if !p.keepalive.paused() {
		nlog.Warningf("%s: keepalive is already active - proceeding to resume (and reset) anyway", p)
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

func (p *proxy) ensureConfigURLs() (config *globalConfig, err error) {
	config, err = p.owner.config.modify(&configModifier{pre: p._configURLs})
	if err != nil {
		err = cmn.NewErrFailedTo(p, "update config (primary, original, discovery) URLs", config, err)
	}
	return config, err
}

// using cmn.NetIntraControl network for all three: PrimaryURL, OriginalURL, and DiscoveryURL
func (p *proxy) _configURLs(_ *configModifier, clone *globalConfig) (updated bool, _ error) {
	smap := p.owner.smap.get()
	debug.Assert(smap.isPrimary(p.si))

	if prim := smap.Primary.URL(cmn.NetIntraControl); clone.Proxy.PrimaryURL != prim {
		clone.Proxy.PrimaryURL = prim
		updated = true
	}
	orig, disc := smap.configURLsIC(clone.Proxy.OriginalURL, clone.Proxy.DiscoveryURL)
	if orig != "" && orig != clone.Proxy.OriginalURL {
		clone.Proxy.OriginalURL = orig
		updated = true
	}
	if disc != "" && disc != clone.Proxy.DiscoveryURL {
		clone.Proxy.DiscoveryURL = disc
		updated = true
	}
	return updated, nil
}

// +gen:endpoint POST /v1/sort model=[dsort.RequestSpec]
// +gen:endpoint GET /v1/sort
// +gen:endpoint DELETE /v1/sort/abort
// +gen:endpoint DELETE /v1/sort
// +gen:payload dsort.RequestSpec={"input_bck":{"name":"<input-bucket-name>","provider":"<provider>"},"input_format":{"template":"<input-template>"},"output_format":"<output-template>","output_shard_size":"<shard-size>","input_extension":"<input-ext>","output_extension":"<output-ext>","description":"<description>","algorithm":{"kind":"<algorithm-kind>"}}
// Start, monitor, abort, or remove distributed sort (dsort) jobs
func (p *proxy) dsortHandler(w http.ResponseWriter, r *http.Request) {
	if !p.cluStartedWithRetry() {
		w.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if err := p.checkAccess(w, r, nil, apc.AceAdmin); err != nil {
		return
	}
	apiItems, err := cmn.ParseURL(r.URL.Path, apc.URLPathdSort.L, 0, true)
	if err != nil {
		p.writeErrURL(w, r)
		return
	}

	switch r.Method {
	case http.MethodPost:
		// - validate request, check input_bck and output_bck
		// - start dsort
		body, err := cos.ReadAllN(r.Body, r.ContentLength)
		if err != nil {
			p.writeErrStatusf(w, r, http.StatusInternalServerError, "failed to receive dsort request: %v", err)
			return
		}
		rs := &dsort.RequestSpec{}
		if err := jsoniter.Unmarshal(body, rs); err != nil {
			err = fmt.Errorf(cmn.FmtErrUnmarshal, p, "dsort request", cos.BHead(body), err)
			p.writeErr(w, r, err)
			return
		}
		parsc, err := rs.ParseCtx()
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		bck := meta.CloneBck(&parsc.InputBck)
		args := bctx{p: p, w: w, r: r, bck: bck, perms: apc.AceObjLIST | apc.AceGET}
		if _, err = args.initAndTry(); err != nil {
			return
		}
		if !parsc.OutputBck.Equal(&parsc.InputBck) {
			bckTo := meta.CloneBck(&parsc.OutputBck)
			bckTo, ecode, err := p.initBckTo(w, r, nil /*query*/, bckTo)
			if err != nil {
				return
			}
			if ecode == http.StatusNotFound {
				if err := p.checkAccess(w, r, nil, apc.AceCreateBucket); err != nil {
					return
				}
				naction := "dsort-create-output-bck"
				warnfmt := "%s: %screate 'output_bck' %s with the 'input_bck' (%s) props"
				if p.forwardCP(w, r, nil /*msg*/, naction, body /*orig body*/) { // to create
					return
				}
				ctx := &bmdModifier{
					pre:   bmodCpProps,
					final: p.bmodSync,
					msg:   &apc.ActMsg{Action: naction},
					txnID: "",
					bcks:  []*meta.Bck{bck, bckTo},
					wait:  true,
				}
				if _, err = p.owner.bmd.modify(ctx); err != nil {
					debug.AssertNoErr(err)
					err = fmt.Errorf(warnfmt+": %w", p, "failed to ", bckTo.String(), bck.String(), err)
					p.writeErr(w, r, err)
					return
				}
				nlog.Warningf(warnfmt, p, "", bckTo.String(), bck.String())
			}
		}
		dsort.PstartHandler(w, r, parsc)
	case http.MethodGet:
		dsort.PgetHandler(w, r)
	case http.MethodDelete:
		switch {
		case len(apiItems) == 1 && apiItems[0] == apc.Abort:
			dsort.PabortHandler(w, r)
		case len(apiItems) == 0:
			dsort.PremoveHandler(w, r)
		default:
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
	if !cmn.Rom.Features().IsSet(feat.S3APIviaRoot) {
		config := cmn.GCO.Get()
		if config.Backend.Get(apc.HT) == nil {
			p.writeErrURL(w, r)
			return
		}

		// `/` root reserved for vanilla http locations via ht:// mechanism
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
	if cmn.Rom.FastV(5, cos.SmoduleAIS) {
		nlog.Infoln("[HTTP CLOUD] RevProxy handler:", baseURL, "-->", r.URL.Path)
	}
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		// bck.IsHT()
		hbo := cmn.NewHTTPObj(r.URL)
		q := r.URL.Query()
		q.Set(apc.QparamOrigURL, r.URL.String())
		q.Set(apc.QparamProvider, apc.HT)
		r.URL.Path = apc.URLPathObjects.Join(hbo.Bck.Name, hbo.ObjName)
		r.URL.RawQuery = q.Encode()
		if r.Method == http.MethodGet {
			p.httpobjget(w, r, hbo.OrigURLBck)
		} else {
			p.httpobjhead(w, r, hbo.OrigURLBck)
		}
		return
	}
	p.writeErrf(w, r, "%q provider doesn't support %q", apc.HT, r.Method)
}

//
// metasync Rx
//

// compare w/ t.receiveConfig
func (p *proxy) receiveConfig(newConfig *globalConfig, msg *actMsgExt, payload msPayload, caller string) (err error) {
	oldConfig := cmn.GCO.Get()
	logmsync(oldConfig.Version, newConfig, msg, caller, newConfig.String(), oldConfig.UUID)

	p.owner.config.Lock()
	err = p._recvCfg(newConfig, msg, payload)
	p.owner.config.Unlock()
	if err != nil {
		return
	}

	if !p.NodeStarted() {
		if msg.Action == apc.ActAttachRemAis || msg.Action == apc.ActDetachRemAis {
			nlog.Warningf("%s: cannot handle %s (%s => %s) - starting up...", p, msg, oldConfig, newConfig)
		}
		return
	}

	if msg.Action != apc.ActAttachRemAis && msg.Action != apc.ActDetachRemAis &&
		newConfig.Backend.EqualRemAIS(&oldConfig.Backend, p.String()) {
		return // nothing to do
	}

	go p._remais(&newConfig.ClusterConfig, false)
	return
}

// refresh local p.remais cache via intra-cluster call to a random target
func (p *proxy) _remais(newConfig *cmn.ClusterConfig, blocking bool) {
	const maxretries = 5
	if !p.remais.in.CAS(false, true) {
		return
	}
	var (
		sleep      = newConfig.Timeout.CplaneOperation.D()
		retries    = maxretries
		over, nver int64
	)
	if blocking {
		retries = 1
	} else {
		maxsleep := newConfig.Timeout.MaxKeepalive.D()
		if uptime := p.keepalive.cluUptime(mono.NanoTime()); uptime < maxsleep {
			sleep = 2 * maxsleep
		}
	}
	for ; retries > 0; retries-- {
		time.Sleep(sleep)
		all, err := p.getRemAisVec(false /*refresh*/)
		if err != nil {
			if retries < maxretries {
				nlog.Errorf("%s: failed to get remais (%d attempts)", p, retries-1)
			}
			continue
		}
		p.remais.mu.Lock()
		if over <= 0 {
			over = p.remais.Ver
		}
		if p.remais.Ver < all.Ver {
			// keep old/detached clusters to support access to existing ("cached") buckets
			// i.e., the ability to resolve remote alias to Ns.UUID (see p.a2u)
			for _, a := range p.remais.RemAisVec.A {
				var found bool
				for _, b := range p.remais.old {
					if b.UUID == a.UUID {
						*b = *a
						found = true
						break
					}
					if b.Alias == a.Alias {
						nlog.Errorf("duplicated remais alias: (%q, %q) vs (%q, %q)", a.UUID, a.Alias, b.UUID, b.Alias)
					}
				}
				if !found {
					p.remais.old = append(p.remais.old, a)
				}
			}

			p.remais.RemAisVec = *all
			nver = p.remais.Ver
			p.remais.mu.Unlock()
			break
		}
		p.remais.mu.Unlock()
		nlog.Errorf("%s: retrying remais ver=%d (%d attempts)", p, all.Ver, retries-1)
		sleep = newConfig.Timeout.CplaneOperation.D()
	}

	p.remais.in.Store(false)
	nlog.Infof("%s: remais v%d => v%d", p, over, nver)
}

func (p *proxy) receiveRMD(newRMD *rebMD, msg *actMsgExt, caller string) error {
	rmd := p.owner.rmd.get()
	logmsync(rmd.Version, newRMD, msg, caller, newRMD.String(), rmd.CluID)

	if msg.Action == apc.ActPrimaryForce {
		return p.owner.rmd.synch(newRMD, false)
	}

	p.owner.rmd.Lock()
	rmd = p.owner.rmd.get()
	if newRMD.version() <= rmd.version() {
		p.owner.rmd.Unlock()
		if newRMD.version() < rmd.version() {
			return newErrDowngrade(p.si, rmd.String(), newRMD.String())
		}
		return nil
	}
	p.owner.rmd.put(newRMD)
	err := p.owner.rmd.persist(newRMD)
	p.owner.rmd.Unlock()

	if err != nil {
		nlog.Errorln("FATAL:", err)
		debug.AssertNoErr(err)
	}

	// Register `nl` for rebalance/resilver
	smap := p.owner.smap.get()
	if smap.IsIC(p.si) && smap.CountActiveTs() > 0 && (smap.IsPrimary(p.si) || p.ClusterStarted()) {
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

	return nil
}

func (p *proxy) smapOnUpdate(newSmap, oldSmap *smapX, nfl, ofl cos.BitFlags) {
	// upon node's removal cleanup associated reverse-proxy state
	p.rproxy.nodes.Range(func(key, _ any) bool {
		nodeID := key.(string)
		if oldSmap.GetNode(nodeID) != nil && newSmap.GetNode(nodeID) == nil {
			p.rproxy.nodes.Delete(nodeID)
		}
		return true
	})
	p.syncNewICOwners(oldSmap, newSmap)

	p.htrun.smapUpdatedCB(newSmap, oldSmap, nfl, ofl)
}

func (p *proxy) receiveBMD(newBMD *bucketMD, msg *actMsgExt, payload msPayload, caller string) (err error) {
	bmd := p.owner.bmd.get()
	logmsync(bmd.Version, newBMD, msg, caller, newBMD.String(), bmd.UUID)

	p.owner.bmd.Lock()
	bmd = p.owner.bmd.get()

	if msg.Action == apc.ActPrimaryForce {
		goto skip
	}

	if err = bmd.validateUUID(newBMD, p.si, nil, caller); err != nil {
		cos.Assert(!p.owner.smap.get().isPrimary(p.si))
		// cluster integrity error: making exception for non-primary proxies
		nlog.Errorf("%s (non-primary): %v - proceeding to override BMD", p, err)
	} else if newBMD.version() <= bmd.version() {
		p.owner.bmd.Unlock()
		return newErrDowngrade(p.si, bmd.String(), newBMD.String())
	}

skip:
	err = p.owner.bmd.putPersist(newBMD, payload)
	debug.AssertNoErr(err)
	p.owner.bmd.Unlock()
	return
}

// getDaemonInfo queries osi for its daemon info and returns it.
func (p *proxy) _getSI(osi *meta.Snode) (si *meta.Snode, err error) {
	cargs := allocCargs()
	{
		cargs.si = osi
		cargs.req = cmn.HreqArgs{
			Method: http.MethodGet,
			Path:   apc.URLPathDae.S,
			Query:  url.Values{apc.QparamWhat: []string{apc.WhatSnode}},
		}
		cargs.timeout = cmn.Rom.CplaneOperation()
		cargs.cresv = cresjGeneric[meta.Snode]{}
	}
	res := p.call(cargs, p.owner.smap.get())
	if res.err != nil {
		err = res.err
	} else {
		si = res.v.(*meta.Snode)
	}
	freeCargs(cargs)
	freeCR(res)
	return
}

func (p *proxy) headRemoteBck(bck *cmn.Bck, query url.Values) (http.Header, int, error) {
	var (
		path = apc.URLPathBuckets.Join(bck.Name)
		smap = p.owner.smap.get()
	)
	tsi, err := smap.GetRandTarget()
	if err != nil {
		return nil, 0, err
	}
	if bck.IsBuiltTagged() {
		config := cmn.GCO.Get()
		if config.Backend.Get(bck.Provider) == nil {
			err := &cmn.ErrMissingBackend{Provider: bck.Provider}
			ecode := http.StatusNotFound
			return nil, ecode, cmn.NewErrFailedTo(p, "lookup bucket", bck, err, ecode)
		}
	}

	// call
	var (
		hdr   http.Header
		q     = bck.AddToQuery(query)
		cargs = allocCargs()
	)
	{
		cargs.si = tsi
		cargs.req = cmn.HreqArgs{Method: http.MethodHead, Path: path, Query: q}
		cargs.timeout = apc.DefaultTimeout
	}

	res := p.call(cargs, smap)
	ecode := res.status

	switch ecode {
	case http.StatusNotFound:
		if err = res.err; err == nil {
			err = cmn.NewErrRemBckNotFound(bck)
		}
	case http.StatusGone:
		err = cmn.NewErrRemoteBckOffline(bck)
	default:
		err = res.err
		hdr = res.header
	}

	freeCargs(cargs)
	freeCR(res)
	return hdr, ecode, err
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

//
// termination(s)
//

func (p *proxy) termKalive(action string) {
	p.keepalive.ctrl(kaSuspendMsg)

	err := fmt.Errorf("%s: term-kalive by %q", p, action)
	xreg.AbortAll(err)
}

func (p *proxy) shutdown(action string) {
	p.Stop(&errNoUnregister{action})
}

func (p *proxy) decommission(action string, opts *apc.ActValRmNode) {
	cleanupConfigDir(p.Name(), opts.KeepInitialConfig)
	if !opts.NoShutdown {
		p.Stop(&errNoUnregister{action})
	}
}

// and return from rungroup.run
func (p *proxy) Stop(err error) {
	var (
		s         = "Stopping " + p.String()
		smap      = p.owner.smap.get()
		isPrimary = smap.isPrimary(p.si)
		e, isEnu  = err.(*errNoUnregister)
	)
	if isPrimary {
		s += "(primary)"
		if !isEnu || e.action != apc.ActShutdownCluster {
			if npsi, err := smap.HrwProxy(p.SID()); err == nil {
				p.notifyCandidate(npsi, smap)
			}
		}
	}
	if err == nil {
		nlog.Infoln(s)
	} else {
		nlog.Warningln(s, "[", err, "]")
	}
	xreg.AbortAll(errors.New("p-stop"))

	p.htrun.stop(&sync.WaitGroup{}, !isPrimary && smap.isValid() && !isEnu /*rmFromSmap*/)
}

// on a best-effort basis, ignoring errors and bodyclose
func (p *proxy) notifyCandidate(npsi *meta.Snode, smap *smapX) {
	cargs := allocCargs()
	cargs.si = npsi
	cargs.req = cmn.HreqArgs{Method: http.MethodPut, Base: npsi.URL(cmn.NetIntraControl), Path: apc.URLPathVotePriStop.S}
	req, err := cargs.req.Req()
	if err != nil {
		return
	}
	req.Header.Set(apc.HdrCallerID, p.SID())
	req.Header.Set(apc.HdrCallerSmapVer, smap.vstr)
	g.client.control.Do(req) //nolint:bodyclose // exiting
	cmn.HreqFree(req)
}

//
// rate limit on the front
//

func (p *proxy) ratelimit(bck *meta.Bck, verb string, smap *smapX) error {
	if !bck.Props.RateLimit.Frontend.Enabled {
		return nil
	}
	var (
		brl   *cos.BurstRateLim // bursty
		rl    = &p.ratelim
		uhash = bck.HashUname(verb)
		v, ok = rl.Load(uhash)
	)
	if ok {
		brl = v.(*cos.BurstRateLim)
	} else {
		brl = bck.NewFrontendRateLim(smap.CountActivePs())
		rl.Store(uhash, brl)
	}
	if !brl.TryAcquire() {
		return cmn.NewErrRateLimitFrontend()
	}
	return nil
}
