// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
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

	"github.com/NVIDIA/aistore/ais/s3"
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
	"github.com/NVIDIA/aistore/nl"
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
		qm         lsobjMem
		rproxy     reverseProxy
		notifs     notifs
		lstca      lstca
		reg        struct {
			pool nodeRegPool
			mu   sync.RWMutex
		}
		remais struct {
			meta.RemAisVec
			old []*meta.RemAis // to facilitate a2u resolution (and, therefore, offline access)
			mu  sync.RWMutex
			in  atomic.Bool
		}
		lastEC atomic.Int64 // last active EC via apc.HdrActiveEC (mono time)

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

	cos.InitShortID(p.si.Digest())

	if network, err := _parseCIDR(env.AIS.LocalRedirectCIDR, ""); err != nil {
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
}

func initPID(config *cmn.Config) (pid string) {
	// 1. ID from env
	if pid = envDaemonID(apc.Proxy); pid != "" {
		if err := cos.ValidateDaemonID(pid); err != nil {
			nlog.Errorf("Warning: %v", err)
		}
		return
	}

	// 2. proxy, K8s
	if k8s.IsK8s() {
		// NOTE: always generate i.e., compute
		if net.ParseIP(k8s.NodeName) != nil { // does not parse as IP
			nlog.Warningf("using K8s node name %q, an IP addr, to compute _persistent_ proxy ID", k8s.NodeName)
		}
		return cos.HashK8sProxyID(k8s.NodeName)
	}

	// 3. try to read ID
	if pid = readProxyID(config); pid != "" {
		nlog.Infof("p[%s] from %q", pid, fname.ProxyID)
		return
	}

	// 4. initial deployment
	pid = genDaemonID(apc.Proxy, config)
	err := cos.ValidateDaemonID(pid)
	debug.AssertNoErr(err)

	// store ID on disk
	err = os.WriteFile(filepath.Join(config.ConfigDir, fname.ProxyID), []byte(pid), cos.PermRWR)
	debug.AssertNoErr(err)
	nlog.Infof("p[%s] ID randomly generated", pid)
	return
}

func readProxyID(config *cmn.Config) (id string) {
	if b, err := os.ReadFile(filepath.Join(config.ConfigDir, fname.ProxyID)); err == nil {
		id = string(b)
	} else if !os.IsNotExist(err) {
		nlog.Errorln(err)
	}
	return
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
	p.qm.init()

	//
	// REST API: register proxy handlers and start listening
	//
	networkHandlers := []networkHandler{
		{r: apc.Reverse, h: p.reverseHandler, net: accessNetPublic},

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

		// S3 compatibility
		{r: "/" + apc.S3, h: p.s3Handler, net: accessNetPublic},

		// "easy URL"
		{r: "/" + apc.GSScheme, h: p.easyURLHandler, net: accessNetPublic},
		{r: "/" + apc.AZScheme, h: p.easyURLHandler, net: accessNetPublic},
		{r: "/" + apc.AISScheme, h: p.easyURLHandler, net: accessNetPublic},

		// ht:// _or_ S3 compatibility, depending on feature flag
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
	var query url.Values
	if smap := p.owner.smap.get(); smap.isPrimary(p.si) {
		return 0, fmt.Errorf("%s should not be joining: is primary, %s", p, smap.StringEx())
	}
	if cmn.GCO.Get().Proxy.NonElectable {
		query = url.Values{apc.QparamNonElectable: []string{"true"}}
	}
	res, err := p.join(query, nil /*htext*/, primaryURLs...)
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
			query := qbck.NewQuery()
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
		query := bck.NewQuery()
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
func (p *proxy) httpbckget(w http.ResponseWriter, r *http.Request, dpq *dpq) {
	var (
		msg     *apc.ActMsg
		bckName string
		qbck    *cmn.QueryBcks
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
	if qbck, err = newQbckFromQ(bckName, nil, dpq); err != nil {
		p.writeErr(w, r, err)
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
		p.writeErrf(w, r, "bad list-objects request: %q is not a bucket (is a bucket query?)", qbck)
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
	if err = cos.MorphMarshal(msg.Value, &lsmsg); err != nil {
		p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
		return
	}
	if lsmsg.Prefix != "" && strings.Contains(lsmsg.Prefix, "../") {
		p.writeErrf(w, r, "bad list-objects request: invalid prefix %q", lsmsg.Prefix)
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
	if bck, err = bckArgs.initAndTry(); err == nil {
		p.listObjects(w, r, bck, msg /*amsg*/, &lsmsg)
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

	// 3. redirect
	smap := p.owner.smap.get()
	tsi, netPub, err := smap.HrwMultiHome(bck.MakeUname(objName))
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	if cmn.Rom.FastV(5, cos.SmoduleAIS) {
		nlog.Infoln("GET " + bck.Cname(objName) + " => " + tsi.String())
	}
	redirectURL := p.redirectURL(r, tsi, time.Now() /*started*/, cmn.NetIntraData, netPub)
	http.Redirect(w, r, redirectURL, http.StatusMovedPermanently)

	// 4. stats
	p.statsT.Inc(stats.GetCount)
}

// PUT /v1/objects/bucket-name/object-name
func (p *proxy) httpobjput(w http.ResponseWriter, r *http.Request, apireq *apiRequest) {
	var (
		nodeID string
		perms  apc.AccessAttrs
	)
	// 1. request
	if err := p.parseReq(w, r, apireq); err != nil {
		return
	}
	appendTyProvided := apireq.dpq.apnd.ty != "" // apc.QparamAppendType
	if !appendTyProvided {
		perms = apc.AcePUT
	} else {
		perms = apc.AceAPPEND
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

	// 3. redirect
	var (
		tsi     *meta.Snode
		smap    = p.owner.smap.get()
		started = time.Now()
		objName = apireq.items[1]
		netPub  = cmn.NetPublic
	)
	if nodeID == "" {
		tsi, netPub, err = smap.HrwMultiHome(bck.MakeUname(objName))
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
	} else {
		if tsi = smap.GetTarget(nodeID); tsi == nil {
			err = &errNodeNotFound{"PUT failure:", nodeID, p.si, smap}
			p.writeErr(w, r, err)
			return
		}
	}

	// verbose
	if cmn.Rom.FastV(5, cos.SmoduleAIS) {
		verb, s := "PUT", ""
		if appendTyProvided {
			verb = "APPEND"
		}
		if bck.Props.Mirror.Enabled {
			s = " (put-mirror)"
		}
		nlog.Infof("%s %s => %s%s", verb, bck.Cname(objName), tsi.StringEx(), s)
	}

	redirectURL := p.redirectURL(r, tsi, started, cmn.NetIntraData, netPub)
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
	smap := p.owner.smap.get()
	tsi, err := smap.HrwName2T(bck.MakeUname(objName))
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	if cmn.Rom.FastV(5, cos.SmoduleAIS) {
		nlog.Infoln("DELETE " + bck.Cname(objName) + " => " + tsi.StringEx())
	}
	redirectURL := p.redirectURL(r, tsi, time.Now() /*started*/, cmn.NetIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)

	p.statsT.Inc(stats.DeleteCount)
}

// DELETE { action } /v1/buckets
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
			// having removed bucket from BMD ask remote to do the same
			p.reverseRemAis(w, r, msg, bck.Bucket(), apireq.query)
			return
		}
		if err := p.destroyBucket(msg, bck); err != nil {
			if cmn.IsErrBckNotFound(err) {
				// TODO: return http.StatusNoContent
				nlog.Infof("%s: %s already %q-ed, nothing to do", p, bck, msg.Action)
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
		xid, err := p.listrange(r.Method, bck.Name, msg, apireq.query)
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		w.Header().Set(cos.HdrContentLength, strconv.Itoa(len(xid)))
		w.Write([]byte(xid))
	default:
		p.writeErrAct(w, r, msg.Action)
	}
}

// PUT /v1/metasync
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

// GET /v1/health
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
		debug.Assert(!prr)
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
	if smap.GetProxy(callerID) != nil {
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
	if prr || askPrimary {
		caller := r.Header.Get(apc.HdrCallerName)
		p.writeErrf(w, r, "%s (non-primary): misdirected health-of-primary request from %s, %s",
			p, caller, smap.StringEx())
		return
	}
	w.WriteHeader(http.StatusOK)
}

// PUT { action } /v1/buckets/bucket-name
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
			w.Header().Set(cos.HdrContentLength, strconv.Itoa(len(xid)))
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
		bckFrom.Provider, bckTo.Provider = apc.AIS, apc.AIS
		if _, present := p.owner.bmd.get().Get(bckTo); present {
			err := cmn.NewErrBckAlreadyExists(bckTo.Bucket())
			p.writeErr(w, r, err)
			return
		}
		if err := p.checkAccess(w, r, nil, apc.AceMoveBucket); err != nil {
			return
		}
		nlog.Infof("%s bucket %s => %s", msg.Action, bckFrom, bckTo)
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
		if tcbmsg.Sync && tcbmsg.Prepend != "" {
			p.writeErrf(w, r, errPrependSync, tcbmsg.Prepend)
			return
		}
		bckTo, err = newBckFromQuname(query, true /*required*/)
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
		if bckFrom.Equal(bckTo, true, true) {
			if !bckFrom.IsRemote() {
				p.writeErrf(w, r, "cannot %s bucket %q onto itself", msg.Action, bckFrom)
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
		if !apc.IsFltPresent(fltPresence) && (bckFrom.IsCloud() || bckFrom.IsRemoteAIS()) {
			lstcx := &lstcx{
				p:       p,
				bckFrom: bckFrom,
				bckTo:   bckTo,
				amsg:    msg,
				config:  cmn.GCO.Get(),
			}
			lstcx.tcomsg.TCBMsg = *tcbmsg
			xid, err = lstcx.do()
		} else {
			nlog.Infoln("x-tcb:", bckFrom.String(), "=>", bckTo.String())
			xid, err = p.tcb(bckFrom, bckTo, msg, tcbmsg.DryRun)
		}
		if err != nil {
			p.writeErr(w, r, err)
			return
		}
	case apc.ActCopyObjects, apc.ActETLObjects:
		var (
			tcomsg = &cmn.TCObjsMsg{}
			bckTo  *meta.Bck
			ecode  int
			eq     bool
		)
		if err = cos.MorphMarshal(msg.Value, tcomsg); err != nil {
			p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
			return
		}
		if tcomsg.Sync && tcomsg.Prepend != "" {
			p.writeErrf(w, r, errPrependSync, tcomsg.Prepend)
			return
		}
		bckTo = meta.CloneBck(&tcomsg.ToBck)

		if bck.Equal(bckTo, true, true) {
			eq = true
			nlog.Warningf("multi-object operation %q within the same bucket %q", msg.Action, bck)
		}
		if bckTo.IsHT() {
			p.writeErrf(w, r, "cannot %s to HTTP bucket %q", msg.Action, bckTo)
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
		if xid, err = p.listrange(r.Method, bucket, msg, query); err != nil {
			p.writeErr(w, r, err)
			return
		}
	case apc.ActInvalListCache:
		p.qm.c.invalidate(bck.Bucket())
		return
	case apc.ActMakeNCopies:
		if xid, err = p.makeNCopies(msg, bck); err != nil {
			p.writeErr(w, r, err)
			return
		}
	case apc.ActECEncode:
		if xid, err = p.ecEncode(bck, msg); err != nil {
			p.writeErr(w, r, err)
			return
		}
	default:
		p.writeErrAct(w, r, msg.Action)
		return
	}

	debug.Assertf(xact.IsValidUUID(xid) || strings.IndexByte(xid, ',') > 0, "%q: %q", msg.Action, xid)
	w.Header().Set(cos.HdrContentLength, strconv.Itoa(len(xid)))
	w.Write([]byte(xid))
}

// init existing or create remote
// not calling `initAndTry` - delegating ais:from// props cloning to the separate method
func (p *proxy) initBckTo(w http.ResponseWriter, r *http.Request, query url.Values, bckTo *meta.Bck) (*meta.Bck, int, error) {
	bckToArgs := bctx{p: p, w: w, r: r, bck: bckTo, perms: apc.AcePUT, query: query}
	bckToArgs.createAIS = true

	ecode, err := bckToArgs.init()
	if err != nil && ecode != http.StatusNotFound {
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

// POST { apc.ActCreateBck } /v1/buckets/bucket-name
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

// one page => msgpack rsp
func (p *proxy) listObjects(w http.ResponseWriter, r *http.Request, bck *meta.Bck, amsg *apc.ActMsg, lsmsg *apc.LsoMsg) {
	// LsVerChanged a.k.a. '--check-versions' limitations
	if lsmsg.IsFlagSet(apc.LsVerChanged) {
		const a = "cannot perform remote versions check"
		if !bck.HasVersioningMD() {
			p.writeErrMsg(w, r, a+": bucket "+bck.Cname("")+" does not provide (remote) versioning info")
			return
		}
		if lsmsg.IsFlagSet(apc.LsNameOnly) || lsmsg.IsFlagSet(apc.LsNameSize) {
			p.writeErrMsg(w, r, a+": flag 'LsVerChanged' is incompatible with 'LsNameOnly', 'LsNameSize'")
			return
		}
		if !lsmsg.WantProp(apc.GetPropsCustom) {
			p.writeErrf(w, r, a+" without listing %q (object property)", apc.GetPropsCustom)
			return
		}
	}

	// default props & flags => user-provided message
	switch {
	case lsmsg.Props == "":
		if lsmsg.IsFlagSet(apc.LsObjCached) {
			lsmsg.AddProps(apc.GetPropsDefaultAIS...)
		} else {
			lsmsg.AddProps(apc.GetPropsMinimal...)
			lsmsg.SetFlag(apc.LsNameSize)
		}
	case lsmsg.Props == apc.GetPropsName:
		lsmsg.SetFlag(apc.LsNameOnly)
	case lsmsg.Props == apc.GetPropsNameSize:
		lsmsg.SetFlag(apc.LsNameSize)
	}
	if bck.IsHT() || lsmsg.IsFlagSet(apc.LsArchDir) {
		lsmsg.SetFlag(apc.LsObjCached)
	}

	// do page
	beg := mono.NanoTime()
	lst, err := p.lsPage(bck, amsg, lsmsg, r.Header, p.owner.smap.get())
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	p.statsT.AddMany(
		cos.NamedVal64{Name: stats.ListCount, Value: 1},
		cos.NamedVal64{Name: stats.ListLatency, Value: mono.SinceNano(beg)},
	)

	var ok bool
	if strings.Contains(r.Header.Get(cos.HdrAccept), cos.ContentMsgPack) {
		ok = p.writeMsgPack(w, lst, lsotag)
	} else {
		ok = p.writeJS(w, r, lst, lsotag)
	}
	if !ok && cmn.Rom.FastV(4, cos.SmoduleAIS) {
		nlog.Errorln("failed to transmit list-objects page (TCP RST?)")
	}

	// GC
	clear(lst.Entries)
	lst.Entries = lst.Entries[:0]
	lst.Entries = nil
	lst = nil
}

// one page; common code (native, s3 api)
func (p *proxy) lsPage(bck *meta.Bck, amsg *apc.ActMsg, lsmsg *apc.LsoMsg, hdr http.Header, smap *smapX) (*cmn.LsoRes, error) {
	var (
		nl             nl.Listener
		err            error
		tsi            *meta.Snode
		lst            *cmn.LsoRes
		newls          bool
		listRemote     bool
		wantOnlyRemote bool
	)
	if lsmsg.UUID == "" {
		lsmsg.UUID = cos.GenUUID()
		newls = true
	}
	tsi, listRemote, wantOnlyRemote, err = p._lsofc(bck, lsmsg, smap)
	if err != nil {
		return nil, err
	}
	if newls {
		if wantOnlyRemote {
			nl = xact.NewXactNL(lsmsg.UUID, apc.ActList, &smap.Smap, meta.NodeMap{tsi.ID(): tsi}, bck.Bucket())
		} else {
			// bcast
			nl = xact.NewXactNL(lsmsg.UUID, apc.ActList, &smap.Smap, nil, bck.Bucket())
		}
		// NOTE #2: TODO: currently, always primary - hrw redirect vs scenarios***
		nl.SetOwner(smap.Primary.ID())
		p.ic.registerEqual(regIC{nl: nl, smap: smap, msg: amsg})
	}

	if listRemote {
		if lsmsg.StartAfter != "" {
			// TODO: remote AIS first, then Cloud
			return nil, fmt.Errorf("%s option --start_after (%s) not yet supported for remote buckets (%s)",
				lsotag, lsmsg.StartAfter, bck)
		}
		// verbose log
		if cmn.Rom.FastV(4, cos.SmoduleAIS) {
			var s string
			if lsmsg.ContinuationToken != "" {
				s = " cont=" + lsmsg.ContinuationToken
			}
			if lsmsg.SID != "" {
				s += " via " + tsi.StringEx()
			}
			nlog.Infof("%s[%s] %s%s", amsg.Action, lsmsg.UUID, bck.Cname(""), s)
		}

		config := cmn.GCO.Get()
		lst, err = p.lsObjsR(bck, lsmsg, hdr, smap, tsi, config, wantOnlyRemote)

		// TODO: `status == http.StatusGone`: at this point we know that this
		// remote bucket exists and is offline. We should somehow try to list
		// cached objects. This isn't easy as we basically need to start a new
		// xaction and return a new `UUID`.
	} else {
		lst, err = p.lsObjsA(bck, lsmsg)
	}

	return lst, err
}

// list-objects flow control helper
func (p *proxy) _lsofc(bck *meta.Bck, lsmsg *apc.LsoMsg, smap *smapX) (tsi *meta.Snode, listRemote, wantOnlyRemote bool, err error) {
	listRemote = bck.IsRemote() && !lsmsg.IsFlagSet(apc.LsObjCached)
	if !listRemote {
		return
	}
	if bck.Props.BID == 0 {
		// remote bucket outside cluster (not in BMD) that hasn't been added ("on the fly") by the caller
		// (lsmsg flag below)
		debug.Assert(bck.IsRemote())
		debug.Assert(lsmsg.IsFlagSet(apc.LsDontAddRemote))
		wantOnlyRemote = true
		if !lsmsg.WantOnlyRemoteProps() {
			err = fmt.Errorf("cannot list remote not-in-cluster bucket %s for not-only-remote object properties: %q",
				bck.Cname(""), lsmsg.Props)
			return
		}
	} else {
		// default
		wantOnlyRemote = lsmsg.WantOnlyRemoteProps()
	}

	// designate one target to carry-out backend.list-objects
	if lsmsg.SID != "" {
		tsi = smap.GetTarget(lsmsg.SID)
		if tsi == nil || tsi.InMaintOrDecomm() {
			err = &errNodeNotFound{lsotag + " failure:", lsmsg.SID, p.si, smap}
			nlog.Errorln(err)
			if smap.CountActiveTs() == 1 {
				// (walk an extra mile)
				orig := err
				tsi, err = smap.HrwTargetTask(lsmsg.UUID)
				if err == nil {
					nlog.Warningf("ignoring [%v] - utilizing the last (or the only) active target %s", orig, tsi)
					lsmsg.SID = tsi.ID()
				}
			}
		}
		return
	}
	// if listing using bucket inventory (`apc.HdrInventory`) is requested
	// target selection can change - see lsObjsR below
	if tsi, err = smap.HrwTargetTask(lsmsg.UUID); err == nil {
		lsmsg.SID = tsi.ID()
	}
	return
}

// POST { action } /v1/objects/bucket-name[/object-name]
func (p *proxy) httpobjpost(w http.ResponseWriter, r *http.Request, apireq *apiRequest) {
	msg, err := p.readActionMsg(w, r)
	if err != nil {
		return
	}
	if msg.Action == apc.ActRenameObject {
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
		objName, objNameTo := apireq.items[1], msg.Name
		if objName == objNameTo {
			p.writeErrMsg(w, r, "cannot rename "+bck.Cname(objName)+" to self, nothing to do")
			return
		}
		if !p.isValidObjname(w, r, objNameTo) {
			return
		}
		p.redirectObjAction(w, r, bck, apireq.items[1], msg)
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
		args := &apc.PromoteArgs{}
		if err := cos.MorphMarshal(msg.Value, args); err != nil {
			p.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, p.si, msg.Action, msg.Value, err)
			return
		}
		var tsi *meta.Snode
		if args.DaemonID != "" {
			smap := p.owner.smap.get()
			if tsi = smap.GetTarget(args.DaemonID); tsi == nil {
				err := &errNodeNotFound{apc.ActPromote + " failure:", args.DaemonID, p.si, smap}
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
	case apc.ActBlobDl:
		if err := p.checkAccess(w, r, bck, apc.AccessRW); err != nil {
			return
		}
		if err := cmn.ValidateRemoteBck(apc.ActBlobDl, bck.Bucket()); err != nil {
			p.writeErr(w, r, err)
			return
		}
		objName := msg.Name
		p.redirectObjAction(w, r, bck, objName, msg)
	default:
		p.writeErrAct(w, r, msg.Action)
	}
}

// HEAD /v1/buckets/bucket-name[/prefix]
// with additional preparsing step to support api.GetBucketInfo prefix (as in: ais ls --summary)
func (p *proxy) httpbckhead(w http.ResponseWriter, r *http.Request, apireq *apiRequest) {
	var prefix string

	// preparse
	{
		items, err := p.parseURL(w, r, apireq.prefix, apireq.after, true)
		if err != nil {
			return
		}
		debug.Assert(apireq.bckIdx == 0, "expecting bucket name at index 0 (zero)")
		if len(items) > 1 {
			prefix = items[1]
			for _, s := range items[2:] {
				prefix += "/" + s
			}
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

// PATCH /v1/buckets/bucket-name
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
	w.Write([]byte(xid))
}

// HEAD /v1/objects/bucket-name/object-name
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
	smap := p.owner.smap.get()
	si, err := smap.HrwName2T(bck.MakeUname(objName))
	if err != nil {
		p.writeErr(w, r, err, http.StatusInternalServerError)
		return
	}
	if cmn.Rom.FastV(5, cos.SmoduleAIS) {
		nlog.Infof("%s %s => %s", r.Method, bck.Cname(objName), si.StringEx())
	}
	redirectURL := p.redirectURL(r, si, time.Now() /*started*/, cmn.NetIntraControl)
	http.Redirect(w, r, redirectURL, http.StatusTemporaryRedirect)
}

// PATCH /v1/objects/bucket-name/object-name
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
	smap := p.owner.smap.get()
	si, err := smap.HrwName2T(bck.MakeUname(objName))
	if err != nil {
		p.writeErr(w, r, err, http.StatusInternalServerError)
		return
	}
	if cmn.Rom.FastV(5, cos.SmoduleAIS) {
		nlog.Infof("%s %s => %s", r.Method, bck.Cname(objName), si.StringEx())
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
		p.writeErr(w, r, err, res.status)
		return
	}

	hdr := w.Header()
	hdr.Set(cos.HdrContentType, res.header.Get(cos.HdrContentType))
	hdr.Set(cos.HdrContentLength, strconv.Itoa(len(res.bytes)))
	_, err = w.Write(res.bytes)
	debug.AssertNoErr(err)
}

func (p *proxy) redirectURL(r *http.Request, si *meta.Snode, ts time.Time, netIntra string, netPubs ...string) (redirect string) {
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
	redirect = nodeURL + r.URL.Path + "?"
	if r.URL.RawQuery != "" {
		redirect += r.URL.RawQuery + "&"
	}

	query := url.Values{
		apc.QparamProxyID:  []string{p.SID()},
		apc.QparamUnixTime: []string{cos.UnixNano2S(ts.UnixNano())},
	}
	redirect += query.Encode()
	return
}

// lsObjsA reads object list from all targets, combines, sorts and returns
// the final list. Excess of object entries from each target is remembered in the
// buffer (see: `queryBuffers`) so we won't request the same objects again.
func (p *proxy) lsObjsA(bck *meta.Bck, lsmsg *apc.LsoMsg) (allEntries *cmn.LsoRes, err error) {
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
		lsmsg.PageSize = apc.MaxPageSizeAIS
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
		Query:  bck.NewQuery(),
		Body:   cos.MustMarshal(aisMsg),
	}
	args.timeout = apc.LongTimeout
	args.smap = smap
	args.cresv = cresLso{} // -> cmn.LsoRes

	// Combine the results.
	results = p.bcastGroup(args)
	freeBcArgs(args)
	for _, res := range results {
		if res.err != nil {
			if res.details == "" || res.details == dfltDetail {
				res.details = xact.Cname(apc.ActList, lsmsg.UUID)
			}
			err = res.toErr()
			freeBcastRes(results)
			return nil, err
		}
		objList := res.v.(*cmn.LsoRes)
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

	allEntries = &cmn.LsoRes{
		UUID:    lsmsg.UUID,
		Entries: entries,
		Flags:   flags,
	}
	if len(entries) >= int(pageSize) {
		allEntries.ContinuationToken = entries[len(entries)-1].Name
	}

	// when recursion is disabled (i.e., lsmsg.IsFlagSet(apc.LsNoRecursion))
	// the (`cmn.LsoRes`) result _may_ include duplicated names of the virtual subdirectories
	// - that's why:
	if lsmsg.IsFlagSet(apc.LsNoRecursion) {
		allEntries.Entries = cmn.DedupLso(allEntries.Entries, len(entries), false /*no-dirs*/)
	}

	return allEntries, nil
}

func (p *proxy) lsObjsR(bck *meta.Bck, lsmsg *apc.LsoMsg, hdr http.Header, smap *smapX, tsi *meta.Snode, config *cmn.Config,
	wantOnlyRemote bool) (*cmn.LsoRes, error) {
	var (
		results sliceResults
		aisMsg  = p.newAmsgActVal(apc.ActList, &lsmsg)
		args    = allocBcArgs()
		timeout = config.Client.ListObjTimeout.D()
	)
	if cos.IsParseBool(hdr.Get(apc.HdrInventory)) {
		// TODO: extend to other Clouds or, more precisely, other list-objects supporting backends
		if !bck.IsRemoteS3() {
			return nil, cmn.NewErrUnsupp("list (via bucket inventory) non-S3 bucket", bck.Cname(""))
		}
		if lsmsg.ContinuationToken == "" /*first page*/ {
			// override _lsofc selection (see above)
			var (
				err        error
				_, objName = s3.InvPrefObjname(bck.Bucket(), hdr.Get(apc.HdrInvName), hdr.Get(apc.HdrInvID))
			)
			tsi, err = smap.HrwName2T(bck.MakeUname(objName))
			if err != nil {
				return nil, err
			}
			lsmsg.SID = tsi.ID()

			timeout = config.Client.TimeoutLong.D()
		}
	}
	args.req = cmn.HreqArgs{
		Method: http.MethodGet,
		Path:   apc.URLPathBuckets.Join(bck.Name),
		Header: hdr,
		Query:  bck.NewQuery(),
		Body:   cos.MustMarshal(aisMsg),
	}
	if wantOnlyRemote {
		cargs := allocCargs()
		{
			cargs.si = tsi
			cargs.req = args.req
			cargs.timeout = timeout
			cargs.cresv = cresLso{} // -> cmn.LsoRes
		}
		// duplicate via query to have target ignoring an (early) failure to initialize bucket
		if lsmsg.IsFlagSet(apc.LsDontHeadRemote) {
			cargs.req.Query.Set(apc.QparamDontHeadRemote, "true")
		}
		if lsmsg.IsFlagSet(apc.LsDontAddRemote) {
			cargs.req.Query.Set(apc.QparamDontAddRemote, "true")
		}
		res := p.call(cargs, smap)
		freeCargs(cargs)
		results = make(sliceResults, 1)
		results[0] = res
	} else {
		args.timeout = timeout
		args.smap = smap
		args.cresv = cresLso{} // -> cmn.LsoRes
		results = p.bcastGroup(args)
	}

	freeBcArgs(args)

	// Combine the results.
	resLists := make([]*cmn.LsoRes, 0, len(results))
	for _, res := range results {
		if res.err != nil {
			if res.details == "" || res.details == dfltDetail {
				res.details = xact.Cname(apc.ActList, lsmsg.UUID)
			}
			err := res.toErr()
			freeBcastRes(results)
			return nil, err
		}
		resLists = append(resLists, res.v.(*cmn.LsoRes))
	}
	freeBcastRes(results)

	return cmn.MergeLso(resLists, lsmsg, 0), nil
}

func (p *proxy) redirectObjAction(w http.ResponseWriter, r *http.Request, bck *meta.Bck, objName string, msg *apc.ActMsg) {
	started := time.Now()
	smap := p.owner.smap.get()
	si, err := smap.HrwName2T(bck.MakeUname(objName))
	if err != nil {
		p.writeErr(w, r, err)
		return
	}
	if cmn.Rom.FastV(5, cos.SmoduleAIS) {
		nlog.Infof("%q %s => %s", msg.Action, bck.Cname(objName), si.StringEx())
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
	apiItems, err := p.parseURL(w, r, apc.URLPathReverse.L, 1, false)
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
		case si.Flags.IsSet(meta.SnodeMaint):
			daeStatus = apc.NodeMaintenance
		case si.Flags.IsSet(meta.SnodeDecomm):
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
		nlog.Warningln(p.String()+":", si.StringEx(), "status is:", daeStatus)
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
	if si == nil {
		// TODO: if not primary, restore the original request (including URL.Path but not only)
		// and forwardCP
		v := &p.rproxy.removed
		v.mu.Lock()
		si = v.m[nodeID]
		v.mu.Unlock()

		if si == nil {
			// when failing to find in Smap and (self)removed, both
			var s string
			if !smap.IsPrimary(p.si) {
				s = "non-primary, " // TODO above
			}
			err = &errNodeNotFound{s + "cannot forward request to node", nodeID, p.si, smap}
			p.writeErr(w, r, err, http.StatusNotFound)
			return
		}

		// cleanup in place
		go p._clremoved(nodeID)
	}

	// do
	p.reverseNodeRequest(w, r, si)
}

func (p *proxy) _clremoved(sid string) {
	time.Sleep(cmn.Rom.MaxKeepalive())
	smap := p.owner.smap.get()
	if smap.GetNode(sid) == nil {
		return
	}
	v := &p.rproxy.removed
	v.mu.Lock()
	delete(v.m, sid)
	v.mu.Unlock()
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
		// Already removed via the the very first target calling here.
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
	case apc.WhatNodeConfig, apc.WhatSmapVote, apc.WhatSnode, apc.WhatLog,
		apc.WhatNodeStats, apc.WhatNodeStatsV322, apc.WhatMetricNames,
		apc.WhatNodeStatsAndStatusV322:
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
		p.writeJSON(w, r, smap, what)
	default:
		p.htrun.httpdaeget(w, r, query, nil /*htext*/)
	}
}

func (p *proxy) httpdaeput(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.parseURL(w, r, apc.URLPathDae.L, 0, true)
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
		force := cos.IsParseBool(query.Get(apc.QparamForce))
		if !force {
			p.writeErrf(w, r, "cannot shutdown primary %s (consider %s=true option)",
				p.si, apc.QparamForce)
			return
		}
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	case apc.LoadX509:
		p.daeLoadX509(w, r)
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
		if err := p.owner.smap.synchronize(p.si, newsmap, nil /*ms payload*/, p.htrun.smapUpdatedCB); err != nil {
			p.writeErr(w, r, cmn.NewErrFailedTo(p, "synchronize", newsmap, err))
			return
		}
		nlog.Infof("%s: %s %s done", p, apc.SyncSmap, newsmap)
	case apc.ActSetConfig: // set-config #1 - via query parameters and "?n1=v1&n2=v2..."
		p.setDaemonConfigQuery(w, r)
	default:
		p.writeErrAct(w, r, action)
	}
}

func (p *proxy) httpdaepost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := p.parseURL(w, r, apc.URLPathDae.L, 0, true)
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
	res := p.call(cargs, p.owner.smap.get())
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
	nlog.Infof("%s: force new primary %s (URL: %s)", p, proxyID, newPrimaryURL)

	if p.SID() == proxyID {
		nlog.Warningf("%s is already primary", p)
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
	apiItems, err := p.parseURL(w, r, apc.URLPathDae.L, 2, false)
	if err != nil {
		return
	}
	proxyID := apiItems[1]
	query := r.URL.Query()
	force := cos.IsParseBool(query.Get(apc.QparamForce))

	// force primary change
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
	if p.SID() == proxyID {
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
		if cmn.Rom.FastV(4, cos.SmoduleAIS) {
			nlog.Infoln("Preparation step: do nothing")
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
		nlog.Infof("%s: removing failed primary %s", p, ctx.sid)
		clone.delProxy(ctx.sid)

		// Remove reverse proxy entry for the node.
		p.rproxy.nodes.Delete(ctx.sid)
	}

	clone.Primary = clone.GetProxy(p.SID())
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
	nlog.Infof("%s: distributing (%s, %s, %s) with newly elected primary (self)", p, clone, bmd, rmd)
	config, err := p.ensureConfigURLs()
	if err != nil {
		nlog.Errorln(err)
	}
	if config != nil {
		pairs = append(pairs, revsPair{config, msg})
		nlog.Infof("%s: plus %s", p, config)
	}
	etl := p.owner.etl.get()
	if etl != nil && etl.version() > 0 {
		pairs = append(pairs, revsPair{etl, msg})
		nlog.Infof("%s: plus %s", p, etl)
	}
	// metasync
	debug.Assert(clone._sgl != nil)
	_ = p.metasyncer.sync(pairs...)

	// synchronize IC tables
	p.syncNewICOwners(ctx.smap, clone)
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

// [METHOD] /v1/sort
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
					err = fmt.Errorf(warnfmt+": %w", p, "failed to ", bckTo, bck, err)
					p.writeErr(w, r, err)
					return
				}
				nlog.Warningf(warnfmt, p, "", bckTo, bck)
			}
		}
		dsort.PstartHandler(w, r, parsc)
	case http.MethodGet:
		dsort.PgetHandler(w, r)
	case http.MethodDelete:
		if len(apiItems) == 1 && apiItems[0] == apc.Abort {
			dsort.PabortHandler(w, r)
		} else if len(apiItems) == 0 {
			dsort.PremoveHandler(w, r)
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
	if !cmn.Rom.Features().IsSet(feat.S3APIviaRoot) {
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
		nlog.Infof("[HTTP CLOUD] RevProxy handler for: %s -> %s", baseURL, r.URL.Path)
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
func (p *proxy) receiveConfig(newConfig *globalConfig, msg *aisMsg, payload msPayload, caller string) (err error) {
	oldConfig := cmn.GCO.Get()
	logmsync(oldConfig.Version, newConfig, msg, caller)

	p.owner.config.Lock()
	err = p._recvCfg(newConfig, payload)
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

func (p *proxy) receiveRMD(newRMD *rebMD, msg *aisMsg, caller string) (err error) {
	rmd := p.owner.rmd.get()
	logmsync(rmd.Version, newRMD, msg, caller)

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
	err = p.owner.rmd.persist(newRMD)
	debug.AssertNoErr(err)
	p.owner.rmd.Unlock()

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
	return
}

func (p *proxy) smapOnUpdate(newSmap, oldSmap *smapX, nfl, ofl cos.BitFlags) {
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

	p.htrun.smapUpdatedCB(newSmap, oldSmap, nfl, ofl)
}

func (p *proxy) receiveBMD(newBMD *bucketMD, msg *aisMsg, payload msPayload, caller string) (err error) {
	bmd := p.owner.bmd.get()
	logmsync(bmd.Version, newBMD, msg, caller)

	p.owner.bmd.Lock()
	bmd = p.owner.bmd.get()
	if err = bmd.validateUUID(newBMD, p.si, nil, caller); err != nil {
		cos.Assert(!p.owner.smap.get().isPrimary(p.si))
		// cluster integrity error: making exception for non-primary proxies
		nlog.Errorf("%s (non-primary): %v - proceeding to override BMD", p, err)
	} else if newBMD.version() <= bmd.version() {
		p.owner.bmd.Unlock()
		return newErrDowngrade(p.si, bmd.String(), newBMD.String())
	}
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
		cargs.cresv = cresND{} // -> meta.Snode
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

func (p *proxy) headRemoteBck(bck *cmn.Bck, q url.Values) (header http.Header, statusCode int, err error) {
	var (
		tsi  *meta.Snode
		path = apc.URLPathBuckets.Join(bck.Name)
		smap = p.owner.smap.get()
	)
	if tsi, err = smap.GetRandTarget(); err != nil {
		return
	}
	if bck.IsBuiltTagged() {
		config := cmn.GCO.Get()
		if config.Backend.Get(bck.Provider) == nil {
			err = &cmn.ErrMissingBackend{Provider: bck.Provider}
			statusCode = http.StatusNotFound
			err = cmn.NewErrFailedTo(p, "lookup bucket", bck, err, statusCode)
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
	res := p.call(cargs, smap)
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
		nlog.Warningf("%s: %v", s, err)
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
}
