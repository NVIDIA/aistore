// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/mirror"
	"github.com/NVIDIA/aistore/reb"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xaction"
	jsoniter "github.com/json-iterator/go"
)

const (
	bucketMDFixup    = "fixup"
	bucketMDReceive  = "receive"
	bucketMDRegister = "register"

	evictCBOp = "evict-cb"
)

type (
	regstate struct {
		sync.Mutex
		disabled bool // target was unregistered by internal event (e.g, all mountpaths are down)
	}
	baseGFN struct {
		lookup atomic.Bool
		tag    string
	}
	// The state that may influence GET logic when mountpath is added/enabled
	localGFN struct {
		baseGFN
	}
	// The state that may influence GET logic when new target joins cluster
	globalGFN struct {
		baseGFN

		mtx         sync.Mutex
		timedLookup atomic.Bool
		refCount    uint32
	}
	capUsed struct {
		sync.RWMutex
		used int32
		oos  bool
	}
	// main
	targetrunner struct {
		httprunner
		cloud         cloudProvider // multi-cloud backend
		prefetchQueue chan filesWithDeadline
		authn         *authManager
		fsprg         fsprungroup
		rebManager    *reb.Manager
		capUsed       capUsed
		transactions  transactions
		gfn           struct {
			local  localGFN
			global globalGFN
		}
		regstate       regstate // the state of being registered with the primary, can be (en/dis)abled via API
		clusterStarted atomic.Bool
	}
)

//////////////
// base gfn //
//////////////

func (gfn *baseGFN) active() bool { return gfn.lookup.Load() }
func (gfn *baseGFN) Activate() bool {
	previous := gfn.lookup.Swap(true)
	glog.Infoln(gfn.tag, "activated")
	return previous
}
func (gfn *baseGFN) Deactivate() { gfn.lookup.Store(false); glog.Infoln(gfn.tag, "deactivated") }

////////////////
// global gfn //
////////////////

func (gfn *globalGFN) active() bool {
	return gfn.lookup.Load() || gfn.timedLookup.Load()
}

func (gfn *globalGFN) Activate() bool {
	gfn.mtx.Lock()
	previous := gfn.lookup.Swap(true)
	gfn.timedLookup.Store(false)
	gfn.refCount = 0
	gfn.mtx.Unlock()

	glog.Infoln(gfn.tag, "activated")
	return previous
}

func (gfn *globalGFN) activateTimed() {
	timedInterval := cmn.GCO.Get().Timeout.Startup

	gfn.mtx.Lock()
	// If gfn is already activated we should not start timed.
	if gfn.lookup.Load() {
		gfn.mtx.Unlock()
		return
	}

	if active := gfn.timedLookup.Swap(true); active {
		// There is no need to start goroutine since we know that one is already
		// running at it should take care about deactivating.
		gfn.refCount++
		gfn.mtx.Unlock()
		glog.Infoln(gfn.tag, "updated timed")
		return
	}
	gfn.mtx.Unlock()
	glog.Infoln(gfn.tag, "activated timed")

	go func() {
		for {
			time.Sleep(timedInterval)

			gfn.mtx.Lock()
			// If we woke up after defined schedule we are safe to deactivate.
			// Otherwise it means that someone updated the schedule and we need
			// to sleep again.
			if gfn.refCount == 0 {
				gfn.timedLookup.Store(false)
				gfn.mtx.Unlock()
				glog.Infoln(gfn.tag, "deactivated timed")
				return
			}
			gfn.refCount = 0
			gfn.mtx.Unlock()
		}
	}()
}

///////////////////
// target runner //
///////////////////

func (t *targetrunner) Run() error {
	var (
		config = cmn.GCO.Get()
		ereg   error
	)
	if err := t.si.Validate(); err != nil {
		cmn.ExitLogf("%v", err)
	}
	t.httprunner.init(getstorstatsrunner(), config)
	t.owner.bmd = newBMDOwnerTgt()

	t.registerStats()
	t.httprunner.keepalive = gettargetkeepalive()

	dryRunInit()
	t.gfn.local.tag, t.gfn.global.tag = "local GFN", "global GFN"

	// init meta-owners and load local instances
	t.owner.bmd.init() // BMD
	smap := newSmap()  // Smap
	if err := t.owner.smap.load(smap, config); err == nil {
		smap.Tmap[t.si.ID()] = t.si
	} else if !os.IsNotExist(err) {
		glog.Errorf("%s: cannot load Smap (- corruption?), err: %v", t.si, err)
	}
	//
	// join cluster
	//
	t.owner.smap.put(smap)
	for i := 0; i < maxRetryCnt; i++ {
		var status int
		if status, ereg = t.register(false, cmn.DefaultTimeout); ereg != nil {
			if cmn.IsErrConnectionRefused(ereg) || status == http.StatusRequestTimeout {
				glog.Errorf("%s: retrying registration...", t.si)
				time.Sleep(time.Second)
				continue
			}
		}
		break
	}
	if ereg != nil {
		glog.Errorf("%s failed to register, err: %v", t.si, ereg)
		glog.Errorf("%s is terminating", t.si)
		return ereg
	}

	go t.pollClusterStarted(config.Timeout.CplaneOperation)

	// register object type and workfile type
	if err := fs.CSM.RegisterContentType(fs.ObjectType, &fs.ObjectContentResolver{}); err != nil {
		cmn.ExitLogf("%v", err)
	}
	if err := fs.CSM.RegisterContentType(fs.WorkfileType, &fs.WorkfileContentResolver{}); err != nil {
		cmn.ExitLogf("%v", err)
	}

	t.detectMpathChanges()

	// cloud provider (empty stubs that may get populated via build tags)
	var (
		err          error
		providerConf = config.Cloud.ProviderConf()
	)
	if config.Cloud.Supported {
		switch config.Cloud.Provider {
		case cmn.ProviderAIS:
			var (
				clusterConf = providerConf.(cmn.CloudConfAIS)
			)
			t.cloud, err = newAISCloudProvider(t, clusterConf)
		case cmn.ProviderAmazon:
			t.cloud, err = newAWSProvider(t)
		case cmn.ProviderGoogle:
			t.cloud, err = newGCPProvider(t)
		default:
			cmn.AssertMsg(false, fmt.Sprintf("unsupported cloud provider: %s", config.Cloud.Provider))
		}
	} else {
		t.cloud, err = newEmptyCloud() // mock
	}
	if err != nil {
		cmn.ExitLogf("%v", err)
	}

	// prefetch
	t.prefetchQueue = make(chan filesWithDeadline, prefetchChanSize)

	t.authn = &authManager{
		tokens:        make(map[string]*authRec),
		revokedTokens: make(map[string]bool),
		version:       1,
	}

	// transactions
	t.transactions.init(t)

	//
	// REST API: register storage target's handler(s) and start listening
	//
	transport.SetMux(cmn.NetworkPublic, t.publicServer.mux)
	if config.Net.UseIntraControl {
		transport.SetMux(cmn.NetworkIntraControl, t.intraControlServer.mux)
	}
	if config.Net.UseIntraData {
		transport.SetMux(cmn.NetworkIntraData, t.intraDataServer.mux)
	}
	networkHandlers := []networkHandler{
		{r: cmn.Buckets, h: t.bucketHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl, cmn.NetworkIntraData}},
		{r: cmn.Objects, h: t.objectHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraData}},
		{r: cmn.Daemon, h: t.daemonHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl}},
		{r: cmn.Tokens, h: t.tokenHandler, net: []string{cmn.NetworkPublic}},

		{r: cmn.Download, h: t.downloadHandler, net: []string{cmn.NetworkIntraControl}},
		{r: cmn.Metasync, h: t.metasyncHandler, net: []string{cmn.NetworkIntraControl}},
		{r: cmn.Health, h: t.healthHandler, net: []string{cmn.NetworkIntraControl}},
		{r: cmn.Vote, h: t.voteHandler, net: []string{cmn.NetworkIntraControl}},
		{r: cmn.Sort, h: dsort.SortHandler, net: []string{cmn.NetworkIntraControl, cmn.NetworkIntraData}},
		{r: cmn.Rebalance, h: t.rebalanceHandler, net: []string{cmn.NetworkIntraData}},

		{r: cmn.Txn, h: t.txnHandler, net: []string{cmn.NetworkIntraControl}}, // control plane transactions

		{r: "/", h: cmn.InvalidHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl, cmn.NetworkIntraData}},
	}
	t.registerNetworkHandlers(networkHandlers)

	t.rebManager = reb.NewManager(t, config, getstorstatsrunner())
	ec.Init(t, xaction.Registry)

	aborted, _ := reb.IsRebalancing(cmn.ActLocalReb)
	if aborted {
		go func() {
			glog.Infoln("resuming local rebalance...")
			t.rebManager.RunLocalReb(false /*skipGlobMisplaced*/)
		}()
	}

	dsort.RegisterNode(t.owner.smap, t.owner.bmd, t.si, daemon.gmm, t, t.statsT)
	if err := t.httprunner.run(); err != nil {
		return err
	}
	return nil
}

// target-only stats
func (t *targetrunner) registerStats() {
	t.statsT.Register(stats.PutLatency, stats.KindLatency)
	t.statsT.Register(stats.AppendLatency, stats.KindLatency)
	t.statsT.Register(stats.GetColdCount, stats.KindCounter)
	t.statsT.Register(stats.GetColdSize, stats.KindCounter)
	t.statsT.Register(stats.GetThroughput, stats.KindThroughput)
	t.statsT.Register(stats.LruEvictSize, stats.KindCounter)
	t.statsT.Register(stats.LruEvictCount, stats.KindCounter)
	t.statsT.Register(stats.TxRebCount, stats.KindCounter)
	t.statsT.Register(stats.TxRebSize, stats.KindCounter)
	t.statsT.Register(stats.RxRebCount, stats.KindCounter)
	t.statsT.Register(stats.RxRebSize, stats.KindCounter)
	t.statsT.Register(stats.PrefetchCount, stats.KindCounter)
	t.statsT.Register(stats.PrefetchSize, stats.KindCounter)
	t.statsT.Register(stats.VerChangeCount, stats.KindCounter)
	t.statsT.Register(stats.VerChangeSize, stats.KindCounter)
	t.statsT.Register(stats.ErrCksumCount, stats.KindCounter)
	t.statsT.Register(stats.ErrCksumSize, stats.KindCounter)
	t.statsT.Register(stats.ErrMetadataCount, stats.KindCounter)
	t.statsT.Register(stats.ErrIOCount, stats.KindCounter)
	t.statsT.Register(stats.GetRedirLatency, stats.KindLatency)
	t.statsT.Register(stats.PutRedirLatency, stats.KindLatency)
	// download
	t.statsT.Register(stats.DownloadSize, stats.KindCounter)
	t.statsT.Register(stats.DownloadLatency, stats.KindLatency)
	// dsort
	t.statsT.Register(stats.DSortCreationReqCount, stats.KindCounter)
	t.statsT.Register(stats.DSortCreationReqLatency, stats.KindLatency)
	t.statsT.Register(stats.DSortCreationRespCount, stats.KindCounter)
	t.statsT.Register(stats.DSortCreationRespLatency, stats.KindLatency)
}

// stop gracefully
func (t *targetrunner) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", t.GetRunName(), err)
	sleep := xaction.Registry.AbortAll()
	if t.publicServer.s != nil {
		t.unregister() // ignore errors
	}

	t.httprunner.stop(err)
	if sleep {
		time.Sleep(time.Second)
	}
}

//
// http handlers
//

// verb /v1/buckets
func (t *targetrunner) bucketHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t.httpbckget(w, r)
	case http.MethodDelete:
		t.httpbckdelete(w, r)
	case http.MethodPost:
		t.httpbckpost(w, r)
	case http.MethodHead:
		t.httpbckhead(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /buckets path")
	}
}

// verb /v1/objects
func (t *targetrunner) objectHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t.httpobjget(w, r)
	case http.MethodPut:
		t.httpobjput(w, r)
	case http.MethodDelete:
		t.httpobjdelete(w, r)
	case http.MethodPost:
		t.httpobjpost(w, r)
	case http.MethodHead:
		t.httpobjhead(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /objects path")
	}
}

// GET /v1/buckets/bucket-name
func (t *targetrunner) httpbckget(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	switch apiItems[0] {
	case cmn.AllBuckets:
		query := r.URL.Query()
		what := query.Get(cmn.URLParamWhat)
		if what == cmn.GetWhatBMD {
			body := cmn.MustMarshal(t.owner.bmd.get())
			t.writeJSON(w, r, body, "get-what-bmd")
		} else {
			provider := r.URL.Query().Get(cmn.URLParamProvider)
			t.getBucketNames(w, r, provider)
		}
	default:
		s := fmt.Sprintf("Invalid route /buckets/%s", apiItems[0])
		t.invalmsghdlr(w, r, s)
	}
}

// verifyProxyRedirection returns if the http request was redirected from a proxy
func (t *targetrunner) verifyProxyRedirection(w http.ResponseWriter, r *http.Request, action ...string) bool {
	var (
		query  = r.URL.Query()
		pid    = query.Get(cmn.URLParamProxyID)
		method = r.Method
	)
	if len(action) > 0 {
		method = action[0]
	}
	if pid == "" {
		s := fmt.Sprintf("%s: %s is expected to be redirected", t.si, method)
		t.invalmsghdlr(w, r, s)
		return false
	}
	smap := t.owner.smap.get()
	if smap.GetProxy(pid) == nil {
		s := fmt.Sprintf("%s: %s from unknown [%s], %s", t.si, method, pid, smap.StringEx())
		t.invalmsghdlr(w, r, s)
		return false
	}
	return true
}

// GET /v1/objects/bucket[+"/"+objname]
// Checks if the object exists locally (if not, downloads it) and sends it back
// If the bucket is in the Cloud one and ValidateWarmGet is enabled there is an extra
// check whether the object exists locally. Version is checked as well if configured.
func (t *targetrunner) httpobjget(w http.ResponseWriter, r *http.Request) {
	var (
		config          = cmn.GCO.Get()
		query           = r.URL.Query()
		isGFNRequest, _ = cmn.ParseBool(query.Get(cmn.URLParamIsGFNRequest))
	)

	apiItems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objName := apiItems[0], apiItems[1]
	started := time.Now()
	if redirDelta := t.redirectLatency(started, query); redirDelta != 0 {
		t.statsT.Add(stats.GetRedirLatency, redirDelta)
	}
	rangeOff, rangeLen, err := t.offsetAndLength(query)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	bck, err := newBckFromQuery(bucket, r.URL.Query())
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	lom := &cluster.LOM{T: t, Objname: objName}
	if err = lom.Init(bck.Bck, config); err != nil {
		if _, ok := err.(*cmn.ErrorCloudBucketDoesNotExist); ok {
			t.BMDVersionFixup(r, cmn.Bck{}, true /* sleep */)
			err = lom.Init(bck.Bck, config)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	}
	if err = lom.AllowGET(); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	goi := &getObjInfo{
		started: started,
		t:       t,
		lom:     lom,
		w:       w,
		ctx:     t.contextWithAuth(r.Header),
		offset:  rangeOff,
		length:  rangeLen,
		isGFN:   isGFNRequest,
		chunked: config.Net.HTTP.Chunked,
	}
	if err, errCode := goi.getObject(); err != nil {
		if cmn.IsErrConnectionReset(err) {
			glog.Errorf("GET %s: %v", lom, err)
		} else {
			t.invalmsghdlr(w, r, err.Error(), errCode)
		}
	}
}

func (t *targetrunner) offsetAndLength(query url.Values) (offset, length int64, err error) {
	offsetStr := query.Get(cmn.URLParamOffset)
	lengthStr := query.Get(cmn.URLParamLength)
	if offsetStr == "" && lengthStr == "" {
		return
	}
	if offsetStr == "" || lengthStr == "" {
		err = fmt.Errorf("invalid offset [%s] and/or length [%s]", offsetStr, lengthStr)
		return
	}
	o, err1 := strconv.ParseInt(url.QueryEscape(offsetStr), 10, 64)
	l, err2 := strconv.ParseInt(url.QueryEscape(lengthStr), 10, 64)
	if err1 != nil || err2 != nil || o < 0 || l <= 0 {
		err = fmt.Errorf("invalid offset [%s] and/or length [%s]", offsetStr, lengthStr)
		return
	}
	offset, length = o, l
	return
}

// PUT /v1/objects/bucket-name/object-name
func (t *targetrunner) httpobjput(w http.ResponseWriter, r *http.Request) {
	var (
		query    = r.URL.Query()
		appendTy = query.Get(cmn.URLParamAppendType)
	)
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	started := time.Now()
	if redelta := t.redirectLatency(started, query); redelta != 0 {
		t.statsT.Add(stats.PutRedirLatency, redelta)
	}
	// PUT
	if !t.verifyProxyRedirection(w, r) {
		return
	}
	config := cmn.GCO.Get()
	if capInfo := t.AvgCapUsed(config); capInfo.OOS {
		t.invalmsghdlr(w, r, capInfo.Err.Error())
		return
	}
	bck, err := newBckFromQuery(bucket, query)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	lom := &cluster.LOM{T: t, Objname: objname}
	if err = lom.Init(bck.Bck, config); err != nil {
		if _, ok := err.(*cmn.ErrorCloudBucketDoesNotExist); ok {
			t.BMDVersionFixup(r, cmn.Bck{}, true /* sleep */)
			err = lom.Init(bck.Bck, config)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
		return
	}
	if appendTy == "" {
		err = lom.AllowPUT()
	} else {
		err = lom.AllowAPPEND()
	}
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if lom.Bck().IsAIS() && lom.VerConf().Enabled {
		lom.Load() // need to know the current version if versioning enabled
	}
	lom.SetAtimeUnix(started.UnixNano())
	if appendTy == "" {
		if err, errCode := t.doPut(r, lom, started); err != nil {
			t.invalmsghdlr(w, r, err.Error(), errCode)
		}
	} else {
		if filePath, err, errCode := t.doAppend(r, lom, started); err != nil {
			t.invalmsghdlr(w, r, err.Error(), errCode)
		} else {
			handle := combineAppendHandle(t.si.ID(), filePath)
			w.Header().Set(cmn.HeaderAppendHandle, handle)
		}
	}
}

// DELETE { action } /v1/buckets/bucket-name
func (t *targetrunner) httpbckdelete(w http.ResponseWriter, r *http.Request) {
	var (
		msgInt = &actionMsgInternal{}
	)
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	bck, err := newBckFromQuery(bucket, r.URL.Query())
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	if err = bck.Init(t.owner.bmd, t.si); err != nil {
		if _, ok := err.(*cmn.ErrorCloudBucketDoesNotExist); !ok { // is ais
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	}
	if err := bck.AllowDELETE(); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	b, err := ioutil.ReadAll(r.Body)
	if err == nil && len(b) > 0 {
		err = jsoniter.Unmarshal(b, msgInt)
	}
	if err != nil {
		s := fmt.Sprintf("Failed to read %s body, err: %v", r.Method, err)
		if err == io.EOF {
			trailer := r.Trailer.Get("Error")
			if trailer != "" {
				s = fmt.Sprintf("Failed to read %s request, err: %v, trailer: %s", r.Method, err, trailer)
			}
		}
		t.invalmsghdlr(w, r, s)
		return
	}
	t.ensureLatestMD(msgInt, r)

	switch msgInt.Action {
	case cmn.ActEvictCB:
		cluster.EvictLomCache(bck)
		if err := fs.Mountpaths.DestroyBuckets(evictCBOp, bck.Bck); err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	case cmn.ActDelete, cmn.ActEvictObjects:
		if len(b) == 0 { // must be a List/Range request
			s := fmt.Sprintf("Invalid API request: no message body")
			t.invalmsghdlr(w, r, s)
			return
		}
		var (
			rangeMsg = &cmn.RangeMsg{}
			listMsg  = &cmn.ListMsg{}
			err      error
		)

		args := &xaction.EvictDeleteArgs{
			Ctx:   t.contextWithAuth(r.Header),
			Evict: msgInt.Action == cmn.ActEvictObjects,
		}
		if err = cmn.TryUnmarshal(msgInt.Value, &rangeMsg); err == nil {
			args.RangeMsg = rangeMsg
			if args.RangeMsg.Range != "" && args.RangeMsg.Regex == "" {
				t.invalmsghdlr(w, r, "Range operation requires 'regex' parameter")
				return
			}
		} else if err = cmn.TryUnmarshal(msgInt.Value, &listMsg); err == nil {
			args.ListMsg = listMsg
		} else {
			details := fmt.Sprintf("invalid %s action message: %s, %T", msgInt.Action, msgInt.Name, msgInt.Value)
			t.invalmsghdlr(w, r, details)
			return
		}
		if err != nil {
			t.invalmsghdlr(w, r, "Failed to parse request")
			return
		}

		var xact *xaction.EvictDelete
		xact, err = xaction.Registry.RenewEvictDelete(t, bck, args)
		if err != nil {
			glog.Error(err) // must not happen at commit time
			break
		}

		go xact.Run(args)
	default:
		t.invalmsghdlr(w, r, fmt.Sprintf("Unsupported Action: %s", msgInt.Action))
	}
}

// DELETE [ { action } ] /v1/objects/bucket-name/object-name
func (t *targetrunner) httpobjdelete(w http.ResponseWriter, r *http.Request) {
	var (
		msg     cmn.ActionMsg
		evict   bool
		started = time.Now()
		query   = r.URL.Query()
	)
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	b, err := cmn.ReadBytes(r)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if len(b) > 0 {
		if err = jsoniter.Unmarshal(b, &msg); err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
		evict = msg.Action == cmn.ActEvictObjects
	}

	bck, err := newBckFromQuery(bucket, query)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	lom := &cluster.LOM{T: t, Objname: objname}
	if err = lom.Init(bck.Bck); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if err = lom.AllowDELETE(); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	err = t.objDelete(t.contextWithAuth(r.Header), lom, evict)
	if err != nil {
		if cmn.IsObjNotExist(err) {
			t.invalmsghdlrsilent(w, r, fmt.Sprintf("object %s/%s doesn't exist", lom.Bck(), lom.Objname), http.StatusNotFound)
		} else {
			t.invalmsghdlr(w, r, fmt.Sprintf("error deleting %s: %v", lom, err))
		}
		return
	}
	// EC cleanup if EC is enabled
	ec.ECM.CleanupObject(lom)
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("DELETE: %s, %d µs", lom, int64(time.Since(started)/time.Microsecond))
	}
}

// POST /v1/buckets/bucket-name
func (t *targetrunner) httpbckpost(w http.ResponseWriter, r *http.Request) {
	var (
		bucket string

		msgInt  = &actionMsgInternal{}
		started = time.Now()
	)
	if cmn.ReadJSON(w, r, msgInt) != nil {
		return
	}
	apiItems, err := t.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}

	t.ensureLatestMD(msgInt, r)

	if len(apiItems) == 0 {
		switch msgInt.Action {
		case cmn.ActSummaryBucket:
			bck, err := newBckFromQuery("", r.URL.Query())
			if err != nil {
				t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
				return
			}
			if ok := t.bucketSummary(w, r, bck, msgInt); !ok {
				return
			}
		default:
			t.invalmsghdlr(w, r, "url path is too short: bucket name is missing", http.StatusBadRequest)
		}
		return
	}

	bucket = apiItems[0]
	bck, err := newBckFromQuery(bucket, r.URL.Query())
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	if err = bck.Init(t.owner.bmd, t.si); err != nil {
		if _, ok := err.(*cmn.ErrorCloudBucketDoesNotExist); ok {
			t.BMDVersionFixup(r, cmn.Bck{}, true /* sleep */)
			err = bck.Init(t.owner.bmd, t.si)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	}
	switch msgInt.Action {
	case cmn.ActPrefetch:
		if err := t.listRangeOperation(r, bck, msgInt); err != nil {
			t.invalmsghdlr(w, r, fmt.Sprintf("Failed to prefetch, err: %v", err))
			return
		}
	case cmn.ActCopyBucket, cmn.ActRenameLB:
		var (
			phase = apiItems[1]
		)
		bckFrom := bck
		// TODO: Currently `copybck` is only supported if the destination is AIS bucket.
		bckTo := cluster.NewBck(msgInt.Name, cmn.ProviderAIS, cmn.NsGlobal)
		switch phase {
		case cmn.ActBegin:
			err = t.beginCopyRenameLB(bckFrom, bckTo, msgInt.Action)
		case cmn.ActAbort:
			t.abortCopyRenameLB(bckFrom, bckTo, msgInt.Action)
		case cmn.ActCommit:
			err = t.commitCopyRenameLB(bckFrom, bckTo, msgInt)
		default:
			err = fmt.Errorf("invalid phase %s: %s %s => %s", phase, msgInt.Action, bckFrom, bckTo)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
		glog.Infof("%s %s bucket %s => %s", phase, msgInt.Action, bckFrom, bckTo)
	case cmn.ActListObjects:
		// list the bucket and return
		if ok := t.listbucket(w, r, bck, msgInt); !ok {
			return
		}

		delta := time.Since(started)
		t.statsT.AddMany(
			stats.NamedVal64{Name: stats.ListCount, Value: 1},
			stats.NamedVal64{Name: stats.ListLatency, Value: int64(delta)},
		)
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("LIST: %s, %d µs", bucket, int64(delta/time.Microsecond))
		}
	case cmn.ActSummaryBucket:
		if ok := t.bucketSummary(w, r, bck, msgInt); !ok {
			return
		}
	case cmn.ActECEncode:
		phase := apiItems[1]
		switch phase {
		case cmn.ActBegin:
			err = t.beginECEncode(bck)
		case cmn.ActAbort:
			break // nothing to do
		case cmn.ActCommit:
			err = t.commitECEncode(bck)
		default:
			cmn.Assert(false)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
		glog.Infof("%s %s bucket %s", phase, msgInt.Action, bck)
	case cmn.ActSetProps:
		phase := apiItems[1]
		switch phase {
		case cmn.ActBegin:
			if err := t.validateBckProps(bck, msgInt); err != nil {
				t.invalmsghdlr(w, r, err.Error())
				return
			}
		case cmn.ActCommit, cmn.ActAbort:
			// not required
		}
	default:
		s := fmt.Sprintf(fmtUnknownAct, msgInt)
		t.invalmsghdlr(w, r, s)
	}
}

// POST /v1/objects/bucket-name/object-name
func (t *targetrunner) httpobjpost(w http.ResponseWriter, r *http.Request) {
	var msg cmn.ActionMsg
	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case cmn.ActRename:
		t.renameObject(w, r, &msg)
	case cmn.ActPromote:
		t.promoteFQN(w, r, &msg)
	default:
		s := fmt.Sprintf(fmtUnknownAct, msg)
		t.invalmsghdlr(w, r, s)
	}
}

// HEAD /v1/buckets/bucket-name
func (t *targetrunner) httpbckhead(w http.ResponseWriter, r *http.Request) {
	var (
		bucketProps cmn.SimpleKVs
		hdr         = w.Header()
		query       = r.URL.Query()
		config      = cmn.GCO.Get()
		code        int
		inBMD       = true
	)
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	bck, err := newBckFromQuery(bucket, query)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	if err = bck.Init(t.owner.bmd, t.si); err != nil {
		if _, ok := err.(*cmn.ErrorCloudBucketDoesNotExist); !ok { // is ais
			t.invalmsghdlr(w, r, err.Error())
			return
		}
		inBMD = false
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, bck, pid)
	}
	if bck.IsAIS() {
		t.bucketPropsToHdr(bck, hdr, config, "")
		return
	}
	// + cloud
	bucketProps, err, code = t.cloud.headBucket(t.contextWithAuth(r.Header), bucket)
	if err != nil {
		if !inBMD {
			if code == http.StatusNotFound {
				err = cmn.NewErrorCloudBucketDoesNotExist(bck.Bck, t.si.String())
			} else {
				err = fmt.Errorf("%s: bucket %s, err: %v", t.si, bucket, err)
			}
			t.invalmsghdlr(w, r, err.Error(), code)
			return
		}
		bucketProps = make(cmn.SimpleKVs)
		glog.Warningf("%s: %s bucket %s, err: %v(%d)", t.si, bck.Provider, bucket, err, code)
		bucketProps[cmn.HeaderCloudProvider] = bck.Provider
		bucketProps[cmn.HeaderCloudOffline] = strconv.FormatBool(bck.IsCloud())
	}
	for k, v := range bucketProps {
		hdr.Set(k, v)
	}
	t.bucketPropsToHdr(bck, hdr, config, bucketProps[cmn.HeaderBucketVerEnabled])
}

// HEAD /v1/objects/bucket-name/object-name
func (t *targetrunner) httpobjhead(w http.ResponseWriter, r *http.Request) {
	var (
		errCode int
		exists  bool

		query             = r.URL.Query()
		checkExists, _    = cmn.ParseBool(query.Get(cmn.URLParamCheckExists))
		checkExistsAny, _ = cmn.ParseBool(query.Get(cmn.URLParamCheckExistsAny))
		checkEC, _        = cmn.ParseBool(query.Get(cmn.URLParamECMeta))
		silent, _         = cmn.ParseBool(query.Get(cmn.URLParamSilent))
	)

	apiItems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	var (
		bucket, objName = apiItems[0], apiItems[1]
		invalidHandler  = t.invalmsghdlr
		hdr             = w.Header()
	)
	if silent {
		invalidHandler = t.invalmsghdlrsilent
	}

	if checkEC {
		// should do it before LOM is initialized because the target may
		// have a slice instead of object/replica
		bck, err := newBckFromQuery(bucket, query)
		if err != nil {
			t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
			return
		}
		if err = bck.Init(t.owner.bmd, t.si); err != nil {
			if _, ok := err.(*cmn.ErrorCloudBucketDoesNotExist); !ok { // is ais
				t.invalmsghdlr(w, r, err.Error())
				return
			}
		}
		md, err := ec.ObjectMetadata(bck, objName)
		if err != nil {
			invalidHandler(w, r, err.Error(), http.StatusNotFound)
			return
		}
		hdr.Set(cmn.HeaderObjECMeta, ec.MetaToString(md))
		return
	}

	bck, err := newBckFromQuery(bucket, query)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	lom := &cluster.LOM{T: t, Objname: objName}
	if err = lom.Init(bck.Bck); err != nil {
		invalidHandler(w, r, err.Error())
		return
	}

	lom.Lock(false)
	if err = lom.Load(true); err != nil && !cmn.IsObjNotExist(err) { // (doesnotexist -> ok, other)
		lom.Unlock(false)
		invalidHandler(w, r, err.Error())
		return
	}
	lom.Unlock(false)

	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, lom, pid)
	}

	exists = err == nil

	// NOTE:
	// * checkExists and checkExistsAny establish local presence of the object by looking up all mountpaths
	// * checkExistsAny does it *even* if the object *may* not have local copies
	// * see also: GFN
	// TODO -- FIXME: Bprops.Copies vs actual
	if !exists && (checkExists && lom.Bprops().Mirror.Copies > 1 || checkExistsAny) {
		exists = lom.RestoreObjectFromAny() // lookup and restore the object to its default location
	}

	if lom.Bck().IsAIS() || checkExists {
		if !exists {
			invalidHandler(w, r, fmt.Sprintf("%s/%s %s", bucket, objName, cmn.DoesNotExist), http.StatusNotFound)
			return
		}
		if checkExists {
			return
		}
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s(%s), ver=%s", lom, cmn.B2S(lom.Size(), 1), lom.Version())
		}
	} else {
		var objMeta cmn.SimpleKVs
		objMeta, err, errCode = t.cloud.headObj(t.contextWithAuth(r.Header), lom)
		if err != nil {
			errMsg := fmt.Sprintf("%s: failed to head metadata, err: %v", lom, err)
			invalidHandler(w, r, errMsg, errCode)
			return
		}
		for k, v := range objMeta {
			hdr.Set(k, v)
		}
	}

	if exists {
		hdr.Set(cmn.HeaderObjSize, strconv.FormatInt(lom.Size(), 10))
		hdr.Set(cmn.HeaderObjVersion, lom.Version())
		if lom.AtimeUnix() != 0 {
			hdr.Set(cmn.HeaderObjAtime, cmn.FormatUnixNano(lom.AtimeUnix(), time.RFC822)) // formatted for API caller
		}
		hdr.Set(cmn.HeaderObjNumCopies, strconv.Itoa(lom.NumCopies()))
		if cksum := lom.Cksum(); cksum != nil {
			hdr.Set(cmn.HeaderObjCksumVal, cksum.Value())
		}
		if lom.Bck().Props.EC.Enabled {
			if md, err := ec.ObjectMetadata(lom.Bck(), objName); err == nil {
				hdr.Set(cmn.HeaderObjECMeta, ec.MetaToString(md))
			}
		}
	}
	hdr.Set(cmn.HeaderObjProvider, lom.Bck().Provider)
	hdr.Set(cmn.HeaderObjPresent, strconv.FormatBool(exists))
}

// GET /v1/rebalance (cmn.Rebalance)
// Handles internal data requests while rebalance is running (pull notifications):
//   - request collected slice list for a given target
//     Returns: StatusAccepted if this target has not finished collecting local
//     data yet; StatusNoContent if this target finished collecting local data but
//     it found no slices that belong to a given target. Response body is empty.
//     In case of StatusOK it returns a JSON with all found slices for a given target.
func (t *targetrunner) rebalanceHandler(w http.ResponseWriter, r *http.Request) {
	var (
		caller = r.Header.Get(cmn.HeaderCallerName)
		query  = r.URL.Query()
	)
	getRebData, _ := cmn.ParseBool(query.Get(cmn.URLParamRebData))
	if !getRebData {
		t.invalmsghdlr(w, r, "invalid request", http.StatusBadRequest)
		return
	}

	body, status := t.rebManager.GlobECDataStatus()
	if status != http.StatusOK {
		w.WriteHeader(status)
		return
	}

	if ok := t.writeJSON(w, r, body, "rebalance-data"); !ok {
		glog.Errorf("Failed to send data to %s", caller)
	}
}

//
// supporting methods and misc
//

// checkCloudVersion returns (vchanged=) true if object versions differ between Cloud and local cache;
// should be called only if the local copy exists
func (t *targetrunner) checkCloudVersion(ctx context.Context, lom *cluster.LOM) (vchanged bool, err error, errCode int) {
	var objMeta cmn.SimpleKVs
	objMeta, err, errCode = t.cloud.headObj(ctx, lom)
	if err != nil {
		err = fmt.Errorf("%s: failed to head metadata, err: %v", lom, err)
		return
	}
	if cloudVersion, ok := objMeta[cmn.HeaderObjVersion]; ok {
		if lom.Version() != cloudVersion {
			glog.Infof("%s: version changed from %s to %s", lom, lom.Version(), cloudVersion)
			vchanged = true
		}
	}
	return
}

func (t *targetrunner) getBucketNames(w http.ResponseWriter, r *http.Request, provider string) {
	var (
		bmd         = t.owner.bmd.get()
		bucketNames = &cmn.BucketNames{}
		all         = provider == "" /*all providers*/
		bck         = cmn.Bck{Provider: provider, Ns: cmn.NsGlobal}
	)

	if all || cmn.IsProviderAIS(bck) {
		bucketNames = t.getBucketNamesAIS(bmd)
	}
	if all || cmn.IsProviderCloud(bck, true /*acceptAnon*/) {
		buckets, err, errcode := t.cloud.getBucketNames(t.contextWithAuth(r.Header))
		if err != nil {
			errMsg := fmt.Sprintf("failed to list all buckets, err: %v", err)
			t.invalmsghdlr(w, r, errMsg, errcode)
			return
		}
		bucketNames.Cloud = buckets
		sort.Strings(bucketNames.Cloud) // sort by name
	}

	body := cmn.MustMarshal(bucketNames)
	t.writeJSON(w, r, body, "getbucketnames")
}

func (t *targetrunner) doAppend(r *http.Request, lom *cluster.LOM, started time.Time) (filePath string, err error, errCode int) {
	_, userFilePath := parseAppendHandle(r.URL.Query().Get(cmn.URLParamAppendHandle))
	aoi := &appendObjInfo{
		started:  started,
		t:        t,
		lom:      lom,
		r:        r.Body,
		op:       r.URL.Query().Get(cmn.URLParamAppendType),
		filePath: userFilePath,
	}
	if sizeStr := r.Header.Get("Content-Length"); sizeStr != "" {
		if size, ers := strconv.ParseInt(sizeStr, 10, 64); ers == nil {
			aoi.size = size
		}
	}
	return aoi.appendObject()
}

// PUT new version and update object metadata
// ais bucket:
//  - if bucket versioning is enabled, the version is autoincremented
// Cloud bucket:
//  - returned version ID is the version
// In both cases, new checksum is also generated and stored along with the new version.
func (t *targetrunner) doPut(r *http.Request, lom *cluster.LOM, started time.Time) (err error, errCode int) {
	var (
		header     = r.Header
		cksumType  = header.Get(cmn.HeaderObjCksumType)
		cksumValue = header.Get(cmn.HeaderObjCksumVal)
		cksum      = cmn.NewCksum(cksumType, cksumValue)
	)
	poi := &putObjInfo{
		started:      started,
		t:            t,
		lom:          lom,
		r:            r.Body,
		cksumToCheck: cksum,
		ctx:          t.contextWithAuth(header),
		workFQN:      fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfilePut),
	}
	sizeStr := header.Get("Content-Length")
	if sizeStr != "" {
		if size, ers := strconv.ParseInt(sizeStr, 10, 64); ers == nil {
			poi.size = size
		}
	}
	return poi.putObject()
}

func (t *targetrunner) putMirror(lom *cluster.LOM) {
	const retries = 2
	var (
		err      error
		mirrConf = lom.MirrorConf()
		nmp      = fs.Mountpaths.NumAvail()
	)
	if !mirrConf.Enabled {
		return
	}
	if nmp < 2 {
		glog.Errorf("%s: insufficient mountpaths (%d)", lom, nmp)
		return
	}
	for i := 0; i < retries; i++ {
		xputlrep := xaction.Registry.RenewPutLocReplicas(lom)
		if xputlrep == nil {
			return
		}
		err = xputlrep.Repl(lom)
		if cmn.IsErrXactExpired(err) {
			break
		}
		// retry upon race vs (just finished/timed_out)
	}
	if err != nil {
		glog.Errorf("%s: unexpected failure to initiate local mirroring, err: %v", lom, err)
	}
}

func (t *targetrunner) objDelete(ctx context.Context, lom *cluster.LOM, evict bool) error {
	var (
		cloudErr   error
		errRet     error
		delFromAIS bool
	)
	lom.Lock(true)
	defer lom.Unlock(true)

	delFromCloud := lom.Bck().IsCloud() && !evict
	if err := lom.Load(false); err == nil {
		delFromAIS = true
	} else if !cmn.IsObjNotExist(err) || (!delFromCloud && cmn.IsObjNotExist(err)) {
		return err
	}

	if delFromCloud {
		if err, _ := t.cloud.DeleteObj(ctx, lom); err != nil {
			cloudErr = fmt.Errorf("%s: DELETE failed, err: %v", lom, err)
			t.statsT.Add(stats.DeleteCount, 1)
		}
	}
	if delFromAIS {
		errRet = lom.Remove()
		if errRet != nil {
			if !os.IsNotExist(errRet) {
				if cloudErr != nil {
					glog.Errorf("%s: failed to delete from cloud: %v", lom, cloudErr)
				}
				return errRet
			}
		}
		if evict {
			cmn.Assert(lom.Bck().IsCloud())
			t.statsT.AddMany(
				stats.NamedVal64{Name: stats.LruEvictCount, Value: 1},
				stats.NamedVal64{Name: stats.LruEvictSize, Value: lom.Size()},
			)
		}
	}
	if cloudErr != nil {
		return fmt.Errorf("%s: failed to delete from cloud: %v", lom, cloudErr)
	}
	return errRet
}

///////////////////
// RENAME OBJECT //
///////////////////

func (t *targetrunner) renameObject(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg) {
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objnameFrom := apitems[0], apitems[1]
	bck, err := newBckFromQuery(bucket, r.URL.Query())
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	lom := &cluster.LOM{T: t, Objname: objnameFrom}
	if err = lom.Init(bck.Bck); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if lom.Bck().IsCloud() {
		t.invalmsghdlr(w, r, fmt.Sprintf("%s: cannot rename object from Cloud bucket", lom))
		return
	}
	// TODO -- FIXME: cannot rename erasure-coded
	if err = lom.AllowRENAME(); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	buf, slab := daemon.gmm.Alloc()
	ri := &replicInfo{smap: t.owner.smap.get(), t: t, bckTo: lom.Bck(), buf: buf, localOnly: false, finalize: true}
	copied, err := ri.copyObject(lom, msg.Name /* new objname */)
	slab.Free(buf)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if copied {
		lom.Lock(true)
		if err = lom.Remove(); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
		lom.Unlock(true)
	}
}

///////////////////////////////////////
// PROMOTE local file(s) => objects  //
///////////////////////////////////////

func (t *targetrunner) promoteFQN(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg) {
	const fmtErr = "%s: %s failed: "
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket := apiItems[0]

	params := cmn.ActValPromote{}
	if err := cmn.TryUnmarshal(msg.Value, &params); err != nil {
		return
	}

	if params.Target != "" && params.Target != t.si.ID() {
		glog.Errorf("%s: unexpected target ID %s mismatch", t.si, params.Target)
	}

	// 2. init & validate
	srcFQN := msg.Name
	if srcFQN == "" {
		loghdr := fmt.Sprintf(fmtErr, t.si, msg.Action)
		t.invalmsghdlr(w, r, loghdr+"missing source filename")
		return
	}
	if err = cmn.ValidatePromoteTrimPrefix(srcFQN, params.TrimPrefix); err != nil {
		loghdr := fmt.Sprintf(fmtErr, t.si, msg.Action)
		t.invalmsghdlr(w, r, loghdr+err.Error())
		return
	}
	finfo, err := os.Stat(srcFQN)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	bck, err := newBckFromQuery(bucket, r.URL.Query())
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	if err = bck.Init(t.owner.bmd, t.si); err != nil {
		if _, ok := err.(*cmn.ErrorCloudBucketDoesNotExist); ok {
			t.BMDVersionFixup(r, cmn.Bck{}, true /* sleep */)
			err = bck.Init(t.owner.bmd, t.si)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	}

	// 3a. promote dir
	if finfo.IsDir() {
		if params.Objname != "" {
			t.invalmsghdlr(w, r, "object name not supported for promoting directories")
			return
		}
		if params.Verbose {
			glog.Infof("%s: promote %+v", t.si, params)
		}
		var xact *mirror.XactDirPromote
		xact, err = xaction.Registry.RenewDirPromote(srcFQN, bck, t, &params)
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
		go xact.Run()
		return
	}
	// 3b. promote file
	objName := params.Objname
	if objName == "" {
		objName = strings.TrimPrefix(srcFQN, string(os.PathSeparator))
	}
	if err = t.PromoteFile(srcFQN, bck, objName, params.Overwrite, true /*safe*/, params.Verbose); err != nil {
		loghdr := fmt.Sprintf(fmtErr, t.si, msg.Action)
		t.invalmsghdlr(w, r, loghdr+err.Error())
	}
}

//====================== common for both cold GET and PUT ======================================
//
// on err: closes and removes the file; otherwise closes and returns the size;
// empty omd5 or oxxhash: not considered an exception even when the configuration says otherwise;
// xxhash is always preferred over md5
//
//==============================================================================================

func (t *targetrunner) redirectLatency(started time.Time, query url.Values) (redelta int64) {
	s := query.Get(cmn.URLParamUnixTime)
	if s == "" {
		return
	}
	pts, err := cmn.S2UnixNano(s)
	if err != nil {
		glog.Errorf("unexpected: failed to convert %s to int, err: %v", s, err)
		return
	}
	redelta = started.UnixNano() - pts
	return
}

// fshc wakes up FSHC and makes it to run filesystem check immediately if err != nil
func (t *targetrunner) fshc(err error, filepath string) {
	if !cmn.GCO.Get().FSHC.Enabled {
		return
	}
	if !cmn.IsIOError(err) {
		return
	}
	glog.Errorf("FSHC: fqn %s, err %v", filepath, err)
	mpathInfo, _ := fs.Mountpaths.Path2MpathInfo(filepath)
	if mpathInfo == nil {
		return
	}
	keyName := mpathInfo.Path
	// keyName is the mountpath is the fspath - counting IO errors on a per basis..
	t.statsT.AddMany(stats.NamedVal64{Name: stats.ErrIOCount, NameSuffix: keyName, Value: 1})
	getfshealthchecker().OnErr(filepath)
}
