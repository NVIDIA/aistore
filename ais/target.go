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
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/filter"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/mirror"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/stats/statsd"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xaction"
	jsoniter "github.com/json-iterator/go"
)

const (
	bucketMDFixup    = "fixup"
	bucketMDReceive  = "receive"
	bucketMDRegister = "register"
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

	targetrunner struct {
		httprunner
		cloud         cloudProvider // multi-cloud backend
		prefetchQueue chan filesWithDeadline
		authn         *authManager
		fsprg         fsprungroup
		readahead     readaheader
		ecmanager     *ecManager
		rebManager    *rebManager
		capUsed       capUsed
		gfn           struct {
			local  localGFN
			global globalGFN
		}
		regstate       regstate // the state of being registered with the primary (can be en/disabled via API)
		clusterStarted atomic.Bool
	}
)

// BASE GFN

func (gfn *baseGFN) active() bool { return gfn.lookup.Load() }
func (gfn *baseGFN) activate() bool {
	previous := gfn.lookup.Swap(true)
	glog.Infoln(gfn.tag, "activated")
	return previous
}
func (gfn *baseGFN) deactivate() { gfn.lookup.Store(false); glog.Infoln(gfn.tag, "deactivated") }

// GLOBAL GFN

func (gfn *globalGFN) active() bool {
	return gfn.lookup.Load() || gfn.timedLookup.Load()
}

func (gfn *globalGFN) activate() bool {
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

//
// target runner
//
func (t *targetrunner) Run() error {
	var (
		config = cmn.GCO.Get()
		ereg   error
	)
	t.httprunner.init(getstorstatsrunner(), config)
	t.bmdowner = newBMDOwnerTgt()

	t.registerStats()
	t.httprunner.keepalive = gettargetkeepalive()

	dryinit()
	t.gfn.local.tag, t.gfn.global.tag = "local GFN", "global GFN"

	t.bmdowner.init() // initialize owner and load BMD

	smap := newSmap()
	smap.Tmap[t.si.DaemonID] = t.si
	t.smapowner.put(smap)

	if err := t.si.Validate(); err != nil {
		cmn.ExitLogf("%v", err)
	}
	for i := 0; i < maxRetrySeconds; i++ {
		var status int
		if status, ereg = t.register(false, defaultTimeout); ereg != nil {
			if cmn.IsErrConnectionRefused(ereg) || status == http.StatusRequestTimeout {
				glog.Errorf("%s: retrying registration...", t.si.Name())
				time.Sleep(time.Second)
				continue
			}
		}
		break
	}
	if ereg != nil {
		glog.Errorf("%s failed to register, err: %v", t.si.Name(), ereg)
		glog.Errorf("%s is terminating", t.si.Name())
		return ereg
	}

	go t.pollClusterStarted(config.Timeout.CplaneOperation)

	// register object type and workfile type
	if err := fs.CSM.RegisterFileType(fs.ObjectType, &fs.ObjectContentResolver{}); err != nil {
		cmn.ExitLogf("%v", err)
	}
	if err := fs.CSM.RegisterFileType(fs.WorkfileType, &fs.WorkfileContentResolver{}); err != nil {
		cmn.ExitLogf("%v", err)
	}

	if err := fs.Mountpaths.CreateBucketDir(cmn.AIS); err != nil {
		cmn.ExitLogf("%v", err)
	}
	if err := fs.Mountpaths.CreateBucketDir(cmn.Cloud); err != nil {
		cmn.ExitLogf("%v", err)
	}
	t.detectMpathChanges()

	// cloud provider (empty stubs that may get populated via build tags)
	if config.CloudProvider == cmn.ProviderAmazon {
		t.cloud = newAWSProvider(t)
	} else if config.CloudProvider == cmn.ProviderGoogle {
		t.cloud = newGCPProvider(t)
	} else {
		t.cloud = newEmptyCloud() // mock
	}

	// prefetch
	t.prefetchQueue = make(chan filesWithDeadline, prefetchChanSize)

	t.authn = &authManager{
		tokens:        make(map[string]*authRec),
		revokedTokens: make(map[string]bool),
		version:       1,
	}

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

		{r: "/", h: cmn.InvalidHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl, cmn.NetworkIntraData}},
	}
	t.registerNetworkHandlers(networkHandlers)

	t.initRebManager(config)

	ec.Init()
	t.ecmanager = newECM(t)

	aborted, _ := xaction.Registry.IsRebalancing(cmn.ActLocalReb)
	if aborted {
		go func() {
			glog.Infoln("resuming local rebalance...")
			t.rebManager.RunLocalReb(false /*skipGlobMisplaced*/)
		}()
	}

	dsort.RegisterNode(t.smapowner, t.bmdowner, t.si, t, t.statsif)
	if err := t.httprunner.run(); err != nil {
		return err
	}
	return nil
}

func (t *targetrunner) initRebManager(config *cmn.Config) {
	reb := &rebManager{t: t, filterGFN: filter.NewDefaultFilter(), nodeStages: make(map[string]uint32)}
	reb.ecReb = newECRebalancer(t, reb)
	// Rx endpoints
	reb.netd, reb.netc = cmn.NetworkPublic, cmn.NetworkPublic
	if config.Net.UseIntraData {
		reb.netd = cmn.NetworkIntraData
	}
	if config.Net.UseIntraControl {
		reb.netc = cmn.NetworkIntraControl
	}
	if _, err := transport.Register(reb.netd, rebalanceStreamName, reb.recvObj); err != nil {
		cmn.ExitLogf("%v", err)
	}
	if _, err := transport.Register(reb.netc, rebalanceAcksName, reb.recvAck); err != nil {
		cmn.ExitLogf("%v", err)
	}
	if _, err := transport.Register(reb.netc, rebalancePushName, reb.recvPush); err != nil {
		cmn.ExitLogf("%v", err)
	}
	if _, err := transport.Register(reb.netc, dataECRebStreamName, reb.ecReb.OnData); err != nil {
		cmn.ExitLogf("%v", err)
	}
	// serialization: one at a time
	reb.semaCh = make(chan struct{}, 1)
	reb.semaCh <- struct{}{}

	t.rebManager = reb
}

// target-only stats
func (t *targetrunner) registerStats() {
	t.statsif.Register(stats.PutLatency, stats.KindLatency)
	t.statsif.Register(stats.AppendLatency, stats.KindLatency)
	t.statsif.Register(stats.GetColdCount, stats.KindCounter)
	t.statsif.Register(stats.GetColdSize, stats.KindCounter)
	t.statsif.Register(stats.GetThroughput, stats.KindThroughput)
	t.statsif.Register(stats.LruEvictSize, stats.KindCounter)
	t.statsif.Register(stats.LruEvictCount, stats.KindCounter)
	t.statsif.Register(stats.TxRebCount, stats.KindCounter)
	t.statsif.Register(stats.TxRebSize, stats.KindCounter)
	t.statsif.Register(stats.RxRebCount, stats.KindCounter)
	t.statsif.Register(stats.RxRebSize, stats.KindCounter)
	t.statsif.Register(stats.PrefetchCount, stats.KindCounter)
	t.statsif.Register(stats.PrefetchSize, stats.KindCounter)
	t.statsif.Register(stats.VerChangeCount, stats.KindCounter)
	t.statsif.Register(stats.VerChangeSize, stats.KindCounter)
	t.statsif.Register(stats.ErrCksumCount, stats.KindCounter)
	t.statsif.Register(stats.ErrCksumSize, stats.KindCounter)
	t.statsif.Register(stats.ErrMetadataCount, stats.KindCounter)
	t.statsif.Register(stats.GetRedirLatency, stats.KindLatency)
	t.statsif.Register(stats.PutRedirLatency, stats.KindLatency)
	// download
	t.statsif.Register(stats.DownloadSize, stats.KindCounter)
	t.statsif.Register(stats.DownloadLatency, stats.KindLatency)
	// dsort
	t.statsif.Register(stats.DSortCreationReqCount, stats.KindCounter)
	t.statsif.Register(stats.DSortCreationReqLatency, stats.KindLatency)
	t.statsif.Register(stats.DSortCreationRespCount, stats.KindCounter)
	t.statsif.Register(stats.DSortCreationRespLatency, stats.KindLatency)
}

// stop gracefully
func (t *targetrunner) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", t.Getname(), err)
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
// http handlers: data and metadata
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
			body := cmn.MustMarshal(t.bmdowner.get())
			t.writeJSON(w, r, body, "get-what-bmd")
		} else {
			provider := r.URL.Query().Get(cmn.URLParamProvider)
			normalizedProvider, err := cmn.ProviderFromStr(provider)
			if err != nil {
				t.invalmsghdlr(w, r, err.Error())
				return
			}
			t.getbucketnames(w, r, normalizedProvider)
		}
	default:
		s := fmt.Sprintf("Invalid route /buckets/%s", apiItems[0])
		t.invalmsghdlr(w, r, s)
	}
}

// verifyProxyRedirection returns if the http request was redirected from a proxy
func (t *targetrunner) verifyProxyRedirection(w http.ResponseWriter, r *http.Request, action string) bool {
	query := r.URL.Query()
	pid := query.Get(cmn.URLParamProxyID)
	if pid == "" {
		t.invalmsghdlr(w, r, fmt.Sprintf("%s %s requests are expected to be redirected", r.Method, action))
		return false
	}
	if t.smapowner.get().GetProxy(pid) == nil {
		t.invalmsghdlr(w, r,
			fmt.Sprintf("%s %s request from an unknown proxy/gateway ID '%s' - Smap out of sync?", r.Method, action, pid))
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
	provider := query.Get(cmn.URLParamProvider)
	started := time.Now()
	if redirDelta := t.redirectLatency(started, query); redirDelta != 0 {
		t.statsif.Add(stats.GetRedirLatency, redirDelta)
	}
	rangeOff, rangeLen, err := t.offsetAndLength(query)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	lom := &cluster.LOM{T: t, Objname: objName}
	if err = lom.Init(bucket, provider, config); err != nil {
		if _, ok := err.(*cmn.ErrorCloudBucketDoesNotExist); ok {
			t.bmdVersionFixup("", true /* sleep */)
			err = lom.Init(bucket, provider, config)
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
	provider := query.Get(cmn.URLParamProvider)
	started := time.Now()
	if redelta := t.redirectLatency(started, query); redelta != 0 {
		t.statsif.Add(stats.PutRedirLatency, redelta)
	}
	// PUT
	if !t.verifyProxyRedirection(w, r, r.Method) {
		return
	}
	config := cmn.GCO.Get()
	if capInfo := t.AvgCapUsed(config); capInfo.OOS {
		t.invalmsghdlr(w, r, capInfo.Err.Error())
		return
	}
	lom := &cluster.LOM{T: t, Objname: objname}
	if err = lom.Init(bucket, provider, config); err != nil {
		t.invalmsghdlr(w, r, err.Error())
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
	if lom.IsAIS() && lom.VerConf().Enabled {
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
			handle := combineAppendHandle(t.si.DaemonID, filePath)
			w.Header().Set(cmn.HeaderAppendHandle, handle)
		}
	}
}

// DELETE { action } /v1/buckets/bucket-name
func (t *targetrunner) httpbckdelete(w http.ResponseWriter, r *http.Request) {
	var (
		msgInt  = &actionMsgInternal{}
		started = time.Now()
	)
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	provider := r.URL.Query().Get(cmn.URLParamProvider)
	bck := &cluster.Bck{Name: bucket, Provider: provider}
	if err = bck.Init(t.bmdowner); err != nil {
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
	t.ensureLatestMD(msgInt)

	switch msgInt.Action {
	case cmn.ActEvictCB:
		cluster.EvictCache(bucket)
		fs.Mountpaths.EvictCloudBucket(bucket) // validation handled in proxy.go
	case cmn.ActDelete, cmn.ActEvictObjects:
		if len(b) > 0 { // must be a List/Range request
			err := t.listRangeOperation(r, apitems, provider, msgInt)
			if err != nil {
				t.invalmsghdlr(w, r, fmt.Sprintf("Failed to delete/evict objects: %v", err))
			} else if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("DELETE list|range: %s, %d µs", bucket, int64(time.Since(started)/time.Microsecond))
			}
			return
		}
		s := fmt.Sprintf("Invalid API request: no message body")
		t.invalmsghdlr(w, r, s)
	default:
		t.invalmsghdlr(w, r, fmt.Sprintf("Unsupported Action: %s", msgInt.Action))
	}
}

// DELETE [ { action } ] /v1/objects/bucket-name/object-name
func (t *targetrunner) httpobjdelete(w http.ResponseWriter, r *http.Request) {
	var (
		msg     cmn.ActionMsg
		started = time.Now()
		evict   bool
	)
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	provider := r.URL.Query().Get(cmn.URLParamProvider)
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

	lom := &cluster.LOM{T: t, Objname: objname}
	if err = lom.Init(bucket, provider); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if err = lom.AllowDELETE(); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	err = t.objDelete(t.contextWithAuth(r.Header), lom, evict)
	if err != nil {
		s := fmt.Sprintf("Error deleting %s: %v", lom.StringEx(), err)
		t.invalmsghdlr(w, r, s)
		return
	}
	// EC cleanup if EC is enabled
	t.ecmanager.CleanupObject(lom)
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("DELETE: %s, %d µs", lom.StringEx(), int64(time.Since(started)/time.Microsecond))
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

	t.ensureLatestMD(msgInt)

	provider := r.URL.Query().Get(cmn.URLParamProvider)
	if len(apiItems) == 0 {
		switch msgInt.Action {
		case cmn.ActSummaryBucket:
			bck := &cluster.Bck{Name: "", Provider: provider}
			if ok := t.bucketSummary(w, r, bck, msgInt); !ok {
				return
			}
		default:
			t.invalmsghdlr(w, r, "path is too short: bucket name expected")
		}
		return
	}

	bucket = apiItems[0]
	bck := &cluster.Bck{Name: bucket, Provider: provider}
	if err = bck.Init(t.bmdowner); err != nil {
		if _, ok := err.(*cmn.ErrorCloudBucketDoesNotExist); ok {
			t.bmdVersionFixup("", true /* sleep */)
			err = bck.Init(t.bmdowner)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	}
	switch msgInt.Action {
	case cmn.ActPrefetch:
		if err := t.listRangeOperation(r, apiItems, provider, msgInt); err != nil {
			t.invalmsghdlr(w, r, fmt.Sprintf("Failed to prefetch, err: %v", err))
			return
		}
	case cmn.ActCopyBucket, cmn.ActRenameLB:
		var (
			phase                = apiItems[1]
			bucketFrom, bucketTo = bucket, msgInt.Name
		)
		bckFrom := &cluster.Bck{Name: bucketFrom, Provider: provider}
		if err := bckFrom.Init(t.bmdowner); err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
		switch phase {
		case cmn.ActBegin:
			err = t.beginCopyRenameLB(bckFrom, bucketTo, msgInt.Action)
		case cmn.ActAbort:
			t.abortCopyRenameLB(bckFrom, bucketTo, msgInt.Action)
		case cmn.ActCommit:
			err = t.commitCopyRenameLB(bckFrom, bucketTo, msgInt)
		default:
			err = fmt.Errorf("invalid phase %s: %s %s => %s", phase, msgInt.Action, bucketFrom, bucketTo)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
		glog.Infof("%s %s bucket %s => %s", phase, msgInt.Action, bucketFrom, bucketTo)
	case cmn.ActListObjects:
		// list the bucket and return
		if ok := t.listbucket(w, r, bck, msgInt); !ok {
			return
		}

		delta := time.Since(started)
		t.statsif.AddMany(
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
	case cmn.ActMakeNCopies:
		phase := apiItems[1]
		switch phase {
		case cmn.ActBegin:
			err = t.beginMakeNCopies(bck, msgInt)
			if err != nil {
				t.invalmsghdlr(w, r, err.Error())
				return
			}
		case cmn.ActAbort:
			break // nothing to do
		case cmn.ActCommit:
			copies, _ := t.parseValidateNCopies(msgInt.Value)
			xaction.Registry.AbortBucketXact(cmn.ActPutCopies, bucket)
			xaction.Registry.RenewBckMakeNCopies(bck, t, copies)
		default:
			cmn.Assert(false)
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

func (t *targetrunner) beginMakeNCopies(bck *cluster.Bck, msgInt *actionMsgInternal) (err error) {
	copies, err := t.parseValidateNCopies(msgInt.Value)
	if err == nil {
		err = mirror.ValidateNCopies(t.si.Name(), copies)
	}
	if err != nil {
		return
	}
	if bck.Props.Mirror.Copies < int64(copies) {
		capInfo := t.AvgCapUsed(nil)
		err = capInfo.Err
	}
	return
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
		query       = r.URL.Query()
		tname       = t.si.Name()
		inBMD       = true
	)
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	provider := r.URL.Query().Get(cmn.URLParamProvider)
	bck := &cluster.Bck{Name: bucket, Provider: provider}
	if err = bck.Init(t.bmdowner); err != nil {
		if _, ok := err.(*cmn.ErrorCloudBucketDoesNotExist); !ok { // is ais
			t.invalmsghdlr(w, r, err.Error())
			return
		}
		inBMD = false
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, bucket, pid)
	}
	config := cmn.GCO.Get()
	if bck.IsCloud() {
		var code int
		bucketProps, err, code = t.cloud.headBucket(t.contextWithAuth(r.Header), bucket)
		if err != nil {
			if !inBMD {
				if code == http.StatusNotFound {
					err = cmn.NewErrorCloudBucketDoesNotExist(bucket)
				} else {
					err = fmt.Errorf("%s: bucket %s, err: %v", t.si.Name(), bucket, err)
				}
				t.invalmsghdlr(w, r, err.Error(), code)
				return
			}
			bucketProps = make(cmn.SimpleKVs)
			if config.CloudProvider == "" {
				glog.Warningf("%s: %v(%d) - provider is not configured", tname, err, code)
				bucketProps[cmn.HeaderCloudProvider] = cmn.Cloud
			} else {
				glog.Warningf("%s: %s bucket %s, err: %v(%d)", tname, config.CloudProvider, bucket, err, code)
				bucketProps[cmn.HeaderCloudProvider] = config.CloudProvider
			}
			bucketProps[cmn.HeaderCloudOffline] = strconv.FormatBool(bck.IsCloud())
		}
	} else {
		bucketProps = make(cmn.SimpleKVs)
		bucketProps[cmn.HeaderCloudProvider] = cmn.ProviderAIS
	}
	hdr := w.Header()
	for k, v := range bucketProps {
		hdr.Set(k, v)
	}
	// include bucket's own config override
	props := bck.Props
	cksumConf := &config.Cksum // FIXME: must be props.CksumConf w/o conditions, here and elsewhere
	if props.Cksum.Type != cmn.PropInherit {
		cksumConf = &props.Cksum
	}
	verConf := &props.Versioning
	// transfer bucket props via http header
	// (it is ok for Cloud buckets not to have locally cached props)
	hdr.Set(cmn.HeaderReadPolicy, props.Tiering.ReadPolicy)
	hdr.Set(cmn.HeaderWritePolicy, props.Tiering.WritePolicy)
	hdr.Set(cmn.HeaderBucketChecksumType, cksumConf.Type)
	hdr.Set(cmn.HeaderBucketValidateColdGet, strconv.FormatBool(cksumConf.ValidateColdGet))
	hdr.Set(cmn.HeaderBucketValidateWarmGet, strconv.FormatBool(cksumConf.ValidateWarmGet))
	hdr.Set(cmn.HeaderBucketValidateObjMove, strconv.FormatBool(cksumConf.ValidateObjMove))
	hdr.Set(cmn.HeaderBucketEnableReadRange, strconv.FormatBool(cksumConf.EnableReadRange))
	hdr.Set(cmn.HeaderBucketVerValidateWarm, strconv.FormatBool(verConf.ValidateWarmGet))
	// for Cloud buckets, correct Versioning.Enabled is combination of
	// local and cloud settings and it is true only if versioning
	// is enabled at both sides: Cloud and for local usage
	if bck.IsAIS() || !verConf.Enabled {
		hdr.Set(cmn.HeaderBucketVerEnabled, strconv.FormatBool(verConf.Enabled))
	} else if enabled, err := cmn.ParseBool(bucketProps[cmn.HeaderBucketVerEnabled]); !enabled && err == nil {
		hdr.Set(cmn.HeaderBucketVerEnabled, strconv.FormatBool(false))
	}
	hdr.Set(cmn.HeaderBucketLRULowWM, strconv.FormatInt(props.LRU.LowWM, 10))
	hdr.Set(cmn.HeaderBucketLRUHighWM, strconv.FormatInt(props.LRU.HighWM, 10))
	hdr.Set(cmn.HeaderBucketLRUOOS, strconv.FormatInt(props.LRU.OOS, 10))
	hdr.Set(cmn.HeaderBucketDontEvictTime, props.LRU.DontEvictTimeStr)
	hdr.Set(cmn.HeaderBucketCapUpdTime, props.LRU.CapacityUpdTimeStr)
	hdr.Set(cmn.HeaderBucketMirrorEnabled, strconv.FormatBool(props.Mirror.Enabled))
	hdr.Set(cmn.HeaderBucketMirrorThresh, strconv.FormatInt(props.Mirror.UtilThresh, 10))
	hdr.Set(cmn.HeaderBucketLRUEnabled, strconv.FormatBool(props.LRU.Enabled))
	if props.Mirror.Enabled {
		hdr.Set(cmn.HeaderBucketCopies, strconv.FormatInt(props.Mirror.Copies, 10))
	} else {
		hdr.Set(cmn.HeaderBucketCopies, "0")
	}
	hdr.Set(cmn.HeaderBucketECEnabled, strconv.FormatBool(props.EC.Enabled))
	hdr.Set(cmn.HeaderBucketECObjSizeLimit, strconv.FormatUint(uint64(props.EC.ObjSizeLimit), 10))
	hdr.Set(cmn.HeaderBucketECData, strconv.FormatUint(uint64(props.EC.DataSlices), 10))
	hdr.Set(cmn.HeaderBucketECParity, strconv.FormatUint(uint64(props.EC.ParitySlices), 10))
	hdr.Set(cmn.HeaderBucketAccessAttrs, strconv.FormatUint(props.AccessAttrs, 10))
}

// HEAD /v1/objects/bucket-name/object-name
func (t *targetrunner) httpobjhead(w http.ResponseWriter, r *http.Request) {
	var (
		errCode int
		exists  bool

		query             = r.URL.Query()
		checkExists, _    = cmn.ParseBool(query.Get(cmn.URLParamCheckExists))
		checkExistsAny, _ = cmn.ParseBool(query.Get(cmn.URLParamCheckExistsAny))
		silent, _         = cmn.ParseBool(query.Get(cmn.URLParamSilent))
	)

	apiItems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objName := apiItems[0], apiItems[1]
	provider := r.URL.Query().Get(cmn.URLParamProvider)
	invalidHandler := t.invalmsghdlr
	if silent {
		invalidHandler = t.invalmsghdlrsilent
	}

	lom := &cluster.LOM{T: t, Objname: objName}
	if err = lom.Init(bucket, provider); err != nil {
		invalidHandler(w, r, err.Error())
		return
	}

	lom.Lock(false)
	if err = lom.Load(true); err != nil { // (doesnotexist -> ok, other)
		lom.Unlock(false)
		invalidHandler(w, r, err.Error())
		return
	}
	lom.Unlock(false)

	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, lom.StringEx(), pid)
	}

	exists = lom.Exists()

	// NOTE: DEFINITION
	// * checkExists and checkExistsAny establish local presence of the object by looking up all mountpaths
	// * checkExistsAny does it *even* if the object *may* not have local copies
	// * see also: GFN
	// TODO -- FIXME: Bprops.Copies vs actual
	if !exists && (checkExists && lom.Bprops().Mirror.Copies > 1 || checkExistsAny) {
		exists = lom.RestoreObjectFromAny() // lookup and restore the object to its default location
	}

	hdr := w.Header()
	if lom.IsAIS() || checkExists {
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
			hdr.Set(cmn.HeaderObjAtime, lom.Atime().Format(time.RFC822))
		}
		hdr.Set(cmn.HeaderObjNumCopies, strconv.Itoa(lom.NumCopies()))
		if cksum := lom.Cksum(); cksum != nil {
			hdr.Set(cmn.HeaderObjCksumVal, cksum.Value())
		}
	}
	hdr.Set(cmn.HeaderObjBckIsAIS, strconv.FormatBool(lom.IsAIS()))
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
		callerID = r.Header.Get(cmn.HeaderCallerID)
		query    = r.URL.Query()
	)

	getRebData, _ := cmn.ParseBool(query.Get(cmn.URLParamRebData))
	if !getRebData {
		t.invalmsghdlr(w, r, "invalid request", http.StatusBadRequest)
		return
	}

	status := &rebStatus{}
	t.rebManager.getGlobStatus(status)

	// the target is still collecting the data, reply that the result is not ready
	if status.Stage < rebStageECDetect {
		w.WriteHeader(http.StatusAccepted)
		return
	}

	// ask rebalance manager the list of all local slices
	slices, ok := t.rebManager.ecReb.nodeData(t.si.DaemonID)
	// no local slices found. It is possible if the number of object is small
	if !ok {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	body := cmn.MustMarshal(slices)
	if ok := t.writeJSON(w, r, body, "rebalance-data"); !ok {
		glog.Errorf("Failed to send data to %s", callerID)
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

func (t *targetrunner) getbucketnames(w http.ResponseWriter, r *http.Request, provider string) {
	var (
		bmd         = t.bmdowner.get()
		bucketNames = &cmn.BucketNames{
			AIS:   make([]string, 0, len(bmd.LBmap)),
			Cloud: make([]string, 0, 64),
		}
	)

	if provider != cmn.Cloud {
		for bucket := range bmd.LBmap {
			bucketNames.AIS = append(bucketNames.AIS, bucket)
		}
		sort.Strings(bucketNames.AIS) // sort by name
	}

	buckets, err, errcode := t.cloud.getBucketNames(t.contextWithAuth(r.Header))
	if err != nil {
		errMsg := fmt.Sprintf("failed to list all buckets, err: %v", err)
		t.invalmsghdlr(w, r, errMsg, errcode)
		return
	}
	bucketNames.Cloud = buckets
	sort.Strings(bucketNames.Cloud) // sort by name

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
func (t *targetrunner) doPut(r *http.Request, lom *cluster.LOM, started time.Time) (err error, errcode int) {
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
	var err error

	mirrConf := lom.MirrorConf()
	if !mirrConf.Enabled {
		return
	}
	if nmp := fs.Mountpaths.NumAvail(); nmp < int(mirrConf.Copies) {
		glog.Warningf("insufficient ## mountpaths %d (bucket %s, ## copies %d)", nmp, lom.Bucket(), mirrConf.Copies)
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
		glog.Errorf("%s: unexpected failure to initiate local mirroring, err: %v", lom.StringEx(), err)
	}
}

func (t *targetrunner) objDelete(ctx context.Context, lom *cluster.LOM, evict bool) error {
	var (
		cloudErr error
		errRet   error
	)

	lom.Lock(true)
	defer lom.Unlock(true)

	delFromCloud := !lom.IsAIS() && !evict
	if err := lom.Load(false); err != nil {
		return err
	}
	delFromAIS := lom.Exists()

	if delFromCloud {
		if err, _ := t.cloud.deleteObj(ctx, lom); err != nil {
			cloudErr = fmt.Errorf("%s: DELETE failed, err: %v", lom, err)
			t.statsif.Add(stats.DeleteCount, 1)
		}
	}
	if delFromAIS {
		errRet = lom.Remove()
		if errRet != nil {
			if !os.IsNotExist(errRet) {
				if cloudErr != nil {
					glog.Errorf("%s: failed to delete from cloud: %v", lom.StringEx(), cloudErr)
				}
				return errRet
			}
		}
		if evict {
			cmn.Assert(!lom.IsAIS())
			t.statsif.AddMany(
				stats.NamedVal64{Name: stats.LruEvictCount, Value: 1},
				stats.NamedVal64{Name: stats.LruEvictSize, Value: lom.Size()},
			)
		}
	}
	if cloudErr != nil {
		return fmt.Errorf("%s: failed to delete from cloud: %v", lom.StringEx(), cloudErr)
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
	provider := r.URL.Query().Get(cmn.URLParamProvider)

	lom := &cluster.LOM{T: t, Objname: objnameFrom}
	if err = lom.Init(bucket, provider); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if !lom.IsAIS() {
		t.invalmsghdlr(w, r, fmt.Sprintf("%s: cannot rename object from Cloud bucket", lom))
		return
	}
	// TODO -- FIXME: cannot rename erasure-coded
	if err = lom.AllowRENAME(); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	buf, slab := nodeCtx.mm.AllocDefault()
	ri := &replicInfo{smap: t.smapowner.get(), t: t, bckTo: lom.Bck(), buf: buf, localOnly: false, finalize: true}
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
	tname := t.si.Name()
	if err != nil {
		return
	}
	bucket := apiItems[0]

	params := cmn.ActValPromote{}
	if err := cmn.TryUnmarshal(msg.Value, &params); err != nil {
		return
	}

	if params.Target != "" && params.Target != t.si.DaemonID {
		glog.Errorf("%s: unexpected target ID %s mismatch", tname, params.Target)
	}

	// 2. init & validate
	provider := r.URL.Query().Get(cmn.URLParamProvider)
	srcFQN := msg.Name
	if srcFQN == "" {
		loghdr := fmt.Sprintf(fmtErr, tname, msg.Action)
		t.invalmsghdlr(w, r, loghdr+"missing source filename")
		return
	}
	if err = cmn.ValidateOmitBase(srcFQN, params.OmitBase); err != nil {
		loghdr := fmt.Sprintf(fmtErr, tname, msg.Action)
		t.invalmsghdlr(w, r, loghdr+err.Error())
		return
	}
	finfo, err := os.Stat(srcFQN)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	bck := &cluster.Bck{Name: bucket, Provider: provider}
	if err = bck.Init(t.bmdowner); err != nil {
		if _, ok := err.(*cmn.ErrorCloudBucketDoesNotExist); ok {
			t.bmdVersionFixup("", true /* sleep */)
			err = bck.Init(t.bmdowner)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	}

	// 3a. promote dir
	if finfo.IsDir() {
		if params.Verbose {
			glog.Infof("%s: promote %+v", tname, params)
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
	if err = t.PromoteFile(srcFQN, bck, params.Objname, params.Overwrite, true /*safe*/, params.Verbose); err != nil {
		loghdr := fmt.Sprintf(fmtErr, tname, msg.Action)
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
	pts, err := strconv.ParseInt(s, 0, 64)
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
	t.statsdC.Send(keyName+".io.errors", 1, metric{Type: statsd.Counter, Name: "count", Value: 1})
	getfshealthchecker().OnErr(filepath)
}
