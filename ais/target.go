// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/ais/backend"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/dbdriver"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/etl"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/health"
	"github.com/NVIDIA/aistore/mirror"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/reb"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
)

const (
	bucketMDFixup    = "fixup"
	bucketMDReceive  = "receive"
	bucketMDRegister = "register"
	dbName           = "ais.db"
)

const (
	clusterClockDrift = 5 * time.Millisecond // is expected to be bounded by
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
	clouds map[string]cluster.BackendProvider
	// main
	targetrunner struct {
		httprunner
		cloud        clouds
		fshc         *health.FSHC
		authn        *authManager
		fsprg        fsprungroup
		rebManager   *reb.Manager
		dbDriver     dbdriver.Driver
		transactions transactions
		gfn          struct {
			local  localGFN
			global globalGFN
		}
		regstate regstate // the state of being registered with the primary, can be (en/dis)abled via API
	}
)

// interface guard
var _ cmn.Runner = (*targetrunner)(nil)

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
		const sleep = 5 * time.Second
		for {
			for tm := time.Duration(0); tm < timedInterval; tm += sleep {
				time.Sleep(sleep)
				if !gfn.timedLookup.Load() {
					return
				}
			}

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

// Deactivates timed GFN only if timed GFN has been activated only once before.
func (gfn *globalGFN) abortTimed() {
	gfn.mtx.Lock()
	defer gfn.mtx.Unlock()
	if gfn.refCount == 1 {
		glog.Infoln(gfn.tag, "aborted timed")
		gfn.timedLookup.Store(false)
		gfn.refCount = 0
	}
}

///////////////////
// target runner //
///////////////////

func (t *targetrunner) Run() error {
	config := cmn.GCO.Get()
	if err := t.si.Validate(); err != nil {
		cmn.ExitLogf("%v", err)
	}
	t.httprunner.init(config)

	cluster.Init(t)

	t.statsT.RegisterAll()

	t.checkRestarted()

	// register object type and workfile type
	if err := fs.CSM.RegisterContentType(fs.ObjectType, &fs.ObjectContentResolver{}); err != nil {
		cmn.ExitLogf("%v", err)
	}
	if err := fs.CSM.RegisterContentType(fs.WorkfileType, &fs.WorkfileContentResolver{}); err != nil {
		cmn.ExitLogf("%v", err)
	}

	dryRunInit()

	// Init meta-owners and load local instances
	t.owner.bmd.init()

	smap, reliable := t.tryLoadSmap()
	if !reliable {
		smap = newSmap()
	}
	// Insert self and always proceed starting up.
	smap.Tmap[t.si.ID()] = t.si
	t.owner.smap.put(smap)

	// Try joining the cluster.
	if status, err := t.joinCluster(); err != nil {
		glog.Errorf("%s failed to join cluster (status: %d, err: %v)", t.si, status, err)
		glog.Errorf("%s is terminating", t.si)
		return err
	}

	t.markNodeStarted()

	go func() {
		t.pollClusterStarted(config.Timeout.CplaneOperation)
		t.markClusterStarted()
	}()

	t.detectMpathChanges()

	// init cloud
	t.cloud.init(t)

	t.authn = &authManager{
		tokens:        make(map[string]*cmn.AuthToken),
		revokedTokens: make(map[string]bool),
		version:       1,
	}
	driver, err := dbdriver.NewBuntDB(filepath.Join(config.Confdir, dbName))
	if err != nil {
		glog.Errorf("Failed to initialize DB: %v", err)
		return err
	}
	t.dbDriver = driver
	defer cmn.Close(driver)

	// transactions
	t.transactions.init(t)

	t.rebManager = reb.NewManager(t, config, t.statsT)

	// register storage target's handler(s) and start listening
	t.initRecvHandlers()

	ec.Init(t)

	marked := xreg.GetResilverMarked()
	if marked.Interrupted {
		go func() {
			glog.Infoln("resuming resilver...")
			t.runResilver("", false /*skipGlobMisplaced*/)
		}()
	}

	dsort.InitManagers(driver)
	dsort.RegisterNode(t.owner.smap, t.owner.bmd, t.si, t, t.statsT)

	defer etl.StopAll(t) // Always try to stop running ETLs.

	err = t.httprunner.run()
	// NOTE: This must be done *after* `t.httprunner.run()` so we don't remove marker on panic.
	fs.RemoveMarker(fs.NodeRestartedMarker)
	return err
}

func (c clouds) init(t *targetrunner) {
	config := cmn.GCO.Get()
	ais := backend.NewAIS(t)
	c[cmn.ProviderAIS] = ais // ais cloud is always present
	if aisConf, ok := config.Backend.ProviderConf(cmn.ProviderAIS); ok {
		if err := ais.Apply(aisConf, "init"); err != nil {
			glog.Errorf("%s: %v - proceeding to start anyway...", t.si, err)
		}
	}

	c[cmn.ProviderHTTP], _ = backend.NewHTTP(t, config)
	if err := c.initExt(t); err != nil {
		cmn.ExitLogf("%v", err)
	}
}

// 3rd part cloud: empty stubs unless populated via build tags
func (c clouds) initExt(t *targetrunner) (err error) {
	config := cmn.GCO.Get()
	for provider := range config.Backend.Providers {
		switch provider {
		case cmn.ProviderAmazon:
			c[provider], err = backend.NewAWS(t)
		case cmn.ProviderAzure:
			c[provider], err = backend.NewAzure(t)
		case cmn.ProviderGoogle:
			c[provider], err = backend.NewGCP(t)
		case cmn.ProviderHDFS:
			c[provider], err = backend.NewHDFS(t)
		default:
			err = fmt.Errorf("unknown backend provider: %q", provider)
		}
		if err != nil {
			return
		}
	}
	return
}

func (t *targetrunner) initRecvHandlers() {
	networkHandlers := []networkHandler{
		{r: cmn.Buckets, h: t.bucketHandler, net: accessNetAll},
		{r: cmn.Objects, h: t.objectHandler, net: accessNetPublicData},
		{r: cmn.Daemon, h: t.daemonHandler, net: accessNetPublicControl},
		{r: cmn.Metasync, h: t.metasyncHandler, net: accessNetIntraControl},
		{r: cmn.Health, h: t.healthHandler, net: accessNetPublicControl},
		{r: cmn.Xactions, h: t.xactHandler, net: accessNetIntraControl},
		{r: cmn.Rebalance, h: t.rebManager.RespHandler, net: accessNetIntraData},
		{r: cmn.EC, h: t.ecHandler, net: accessNetIntraData},
		{r: cmn.Vote, h: t.voteHandler, net: accessNetIntraControl},
		{r: cmn.Txn, h: t.txnHandler, net: accessNetIntraControl},
		{r: cmn.ObjStream, h: transport.RxAnyStream, net: accessNetAll},
		{r: cmn.Tokens, h: t.tokenHandler, net: accessNetPublic},

		{r: cmn.Download, h: t.downloadHandler, net: accessNetIntraControl},
		{r: cmn.Sort, h: dsort.SortHandler, net: accessControlData},
		{r: cmn.ETL, h: t.etlHandler, net: accessNetPublicControl},
		{r: cmn.Query, h: t.queryHandler, net: accessNetPublicControl},

		{r: "/" + cmn.S3, h: t.s3Handler, net: accessNetPublicData},
		{r: "/", h: cmn.InvalidHandler, net: accessNetAll},
	}
	t.registerNetworkHandlers(networkHandlers)
}

// stop gracefully
func (t *targetrunner) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", t.Name(), err)
	xreg.AbortAll()
	if t.netServ.pub.s != nil {
		t.unregister() // ignore errors
	}
	t.httprunner.stop(err)
}

func (t *targetrunner) checkRestarted() {
	if fs.MarkerExists(fs.NodeRestartedMarker) {
		t.statsT.Add(stats.RestartCount, 1)
	} else if err := fs.PersistMarker(fs.NodeRestartedMarker); err != nil {
		glog.Error(err)
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
	case http.MethodHead:
		t.httpobjhead(w, r)
	case http.MethodPut:
		t.httpobjput(w, r)
	case http.MethodDelete:
		t.httpobjdelete(w, r)
	case http.MethodPost:
		t.httpobjpost(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /objects path")
	}
}

// verb /v1/slices
// Non-public inerface
func (t *targetrunner) ecHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t.httpecget(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /slices path")
	}
}

///////////////////////
// httpbck* handlers //
///////////////////////

// GET /v1/buckets/bucket-name
func (t *targetrunner) httpbckget(w http.ResponseWriter, r *http.Request) {
	request := apiRequest{after: 1, prefix: cmn.URLPathBuckets.L}
	if err := t.parseAPIRequest(w, r, &request); err != nil {
		return
	}
	switch request.items[0] {
	case cmn.AllBuckets:
		var (
			query = r.URL.Query()
			what  = query.Get(cmn.URLParamWhat)
		)
		if what == cmn.GetWhatBMD {
			t.writeJSON(w, r, t.owner.bmd.get(), "get-what-bmd")
		} else {
			t.listBuckets(w, r, cmn.QueryBcks(request.bck.Bck))
		}
	default:
		t.invalmsghdlrf(w, r, "Invalid route /buckets/%s", request.items[0])
	}
}

// DELETE { action } /v1/buckets/bucket-name
// (evict | delete) (list | range)
func (t *targetrunner) httpbckdelete(w http.ResponseWriter, r *http.Request) {
	msg := aisMsg{}
	if err := cmn.ReadJSON(w, r, &msg, true); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	request := &apiRequest{after: 1, prefix: cmn.URLPathBuckets.L}
	if err := t.parseAPIRequest(w, r, request); err != nil {
		return
	}
	if err := request.bck.Init(t.owner.bmd); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); ok {
			t.BMDVersionFixup(r, cmn.Bck{}, true /* sleep */)
			err = request.bck.Init(t.owner.bmd)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	}

	switch msg.Action {
	case cmn.ActDelete, cmn.ActEvictObjects:
		var (
			rangeMsg = &cmn.RangeMsg{}
			listMsg  = &cmn.ListMsg{}
		)
		args := &xreg.DeletePrefetchArgs{
			Ctx:   context.Background(),
			UUID:  msg.UUID,
			Evict: msg.Action == cmn.ActEvictObjects,
		}
		if err := cmn.MorphMarshal(msg.Value, &rangeMsg); err == nil {
			args.RangeMsg = rangeMsg
		} else if err := cmn.MorphMarshal(msg.Value, &listMsg); err == nil {
			args.ListMsg = listMsg
		} else {
			t.invalmsghdlrf(w, r, "invalid %s action message: %s, %T", msg.Action, msg.Name, msg.Value)
			return
		}
		xact, err := xreg.RenewEvictDelete(t, request.bck, args)
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}

		xact.AddNotif(&xaction.NotifXact{
			NotifBase: nl.NotifBase{
				When: cluster.UponTerm,
				Dsts: []string{equalIC},
				F:    t.callerNotifyFin,
			},
			Xact: xact,
		})
		go xact.Run()
	default:
		t.invalmsghdlrf(w, r, fmtUnknownAct, msg)
	}
}

func (t *targetrunner) httpbcksummary(w http.ResponseWriter, r *http.Request, msg *aisMsg) {
	apiItems, err := t.checkRESTItems(w, r, 0, true, cmn.URLPathBuckets.L)
	if err != nil {
		return
	}
	var (
		bck   *cluster.Bck
		query = r.URL.Query()
	)
	t.ensureLatestSmap(msg, r)
	if len(apiItems) == 0 {
		bck, err = newBckFromQuery("", query)
	} else {
		bck, err = newBckFromQuery(apiItems[0], query)
		if err != nil {
			if err = bck.Init(t.owner.bmd); err != nil {
				if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); ok {
					t.BMDVersionFixup(r, cmn.Bck{}, true /* sleep */)
					err = bck.Init(t.owner.bmd)
				}
			}
		}
	}
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	t.bucketSummary(w, r, bck, msg)
}

// POST /v1/buckets/bucket-name
func (t *targetrunner) httpbckpost(w http.ResponseWriter, r *http.Request) {
	msg := &aisMsg{}
	if err := cmn.ReadJSON(w, r, msg); err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	if msg.Action == cmn.ActSummaryBucket {
		t.httpbcksummary(w, r, msg)
		return
	}

	request := &apiRequest{prefix: cmn.URLPathBuckets.L, after: 1}
	if err := t.parseAPIRequest(w, r, request); err != nil {
		return
	}

	t.ensureLatestSmap(msg, r)

	if err := request.bck.Init(t.owner.bmd); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); ok {
			t.BMDVersionFixup(r, cmn.Bck{}, true /* sleep */)
			err = request.bck.Init(t.owner.bmd)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	}
	switch msg.Action {
	case cmn.ActPrefetch:
		if !request.bck.IsRemote() {
			t.invalmsghdlrf(w, r, "%s: expecting remote bucket, got %s, action=%s", t.si, request.bck, msg.Action)
			return
		}
		var (
			err      error
			rangeMsg = &cmn.RangeMsg{}
			listMsg  = &cmn.ListMsg{}
			args     = &xreg.DeletePrefetchArgs{Ctx: context.Background()}
		)
		if err = cmn.MorphMarshal(msg.Value, &rangeMsg); err == nil {
			args.RangeMsg = rangeMsg
		} else if err = cmn.MorphMarshal(msg.Value, &listMsg); err == nil {
			args.ListMsg = listMsg
		} else {
			t.invalmsghdlrf(w, r, "invalid %s action message: %s, %T", msg.Action, msg.Name, msg.Value)
			return
		}
		args.UUID = msg.UUID
		xact := xreg.RenewPrefetch(t, request.bck, args)
		go xact.Run()
	case cmn.ActListObjects:
		// list the bucket and return
		begin := mono.NanoTime()
		if ok := t.listObjects(w, r, request.bck, msg); !ok {
			return
		}

		delta := mono.Since(begin)
		t.statsT.AddMany(
			stats.NamedVal64{Name: stats.ListCount, Value: 1},
			stats.NamedVal64{Name: stats.ListLatency, Value: int64(delta)},
		)
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("LIST: %s, %s", request.bck, delta)
		}
	default:
		t.invalmsghdlrf(w, r, fmtUnknownAct, msg)
	}
}

// HEAD /v1/buckets/bucket-name
func (t *targetrunner) httpbckhead(w http.ResponseWriter, r *http.Request) {
	var (
		bucketProps cmn.SimpleKVs
		code        int
		inBMD       = true
		ctx         = context.Background()
		hdr         = w.Header()
		query       = r.URL.Query()
		request     = &apiRequest{after: 1, prefix: cmn.URLPathBuckets.L}
		err         error
	)
	if err = t.parseAPIRequest(w, r, request); err != nil {
		return
	}
	if err = request.bck.Init(t.owner.bmd); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); !ok { // is ais
			t.invalmsghdlr(w, r, err.Error())
			return
		}
		inBMD = false
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, request.bck, pid)
	}

	cmn.Assert(!request.bck.IsAIS())

	if request.bck.IsHTTP() {
		originalURL := query.Get(cmn.URLParamOrigURL)
		ctx = context.WithValue(ctx, cmn.CtxOriginalURL, originalURL)
		if !inBMD && originalURL == "" {
			err = cmn.NewErrorRemoteBucketDoesNotExist(request.bck.Bck)
			t.invalmsghdlrsilent(w, r, err.Error(), http.StatusNotFound)
			return
		}
	}
	// + cloud
	bucketProps, code, err = t.Backend(request.bck).HeadBucket(ctx, request.bck)
	if err != nil {
		if !inBMD {
			if code == http.StatusNotFound {
				err = cmn.NewErrorRemoteBucketDoesNotExist(request.bck.Bck)
				t.invalmsghdlrsilent(w, r, err.Error(), code)
			} else {
				err = fmt.Errorf("%s: bucket %s, err: %v", t.si, request.bck, err)
				t.invalmsghdlr(w, r, err.Error(), code)
			}
			return
		}
		glog.Warningf("%s: bucket %s, err: %v(%d)", t.si, request.bck, err, code)
		bucketProps = make(cmn.SimpleKVs)
		bucketProps[cmn.HeaderCloudProvider] = request.bck.Provider
		// TODO: what about `HTTP` cloud?
		bucketProps[cmn.HeaderCloudOffline] = strconv.FormatBool(request.bck.IsCloud())
		bucketProps[cmn.HeaderRemoteAISOffline] = strconv.FormatBool(request.bck.IsRemoteAIS())
	}
	for k, v := range bucketProps {
		if k == cmn.HeaderBucketVerEnabled && request.bck.Props != nil {
			if curr := strconv.FormatBool(request.bck.VersionConf().Enabled); curr != v {
				// e.g., change via vendor-provided CLI and similar
				glog.Errorf("%s: %s versioning got out of sync: %s != %s", t.si, request.bck, v, curr)
			}
		}
		hdr.Set(k, v)
	}
}

///////////////////////
// httpobj* handlers //
///////////////////////

// GET /v1/objects/<bucket-name>/<object-name>
//
// Initially validates if the request is internal request (either from proxy
// or target) and calls getObject.
//
// Checks if the object exists locally (if not, downloads it) and sends it back
// If the bucket is in the Cloud one and ValidateWarmGet is enabled there is an extra
// check whether the object exists locally. Version is checked as well if configured.
func (t *targetrunner) httpobjget(w http.ResponseWriter, r *http.Request) {
	var (
		query    = r.URL.Query()
		ptime    = isRedirect(query)
		features = cmn.GCO.Get().Client.Features
		request  = &apiRequest{after: 2, prefix: cmn.URLPathObjects.L}
	)

	if !isIntraCall(r.Header) && ptime == "" && !features.IsSet(cmn.FeatureDirectAccess) {
		t.invalmsghdlrf(w, r, "%s: %s(obj) is expected to be redirected (remaddr=%s)",
			t.si, r.Method, r.RemoteAddr)
		return
	}
	if err := t.parseAPIRequest(w, r, request); err != nil {
		return
	}

	t.getObject(w, r, query, request.bck, request.items[1])
}

// getObject is main function to get the object. It doesn't check request origin,
// so it must be done by the caller (if necessary).
func (t *targetrunner) getObject(w http.ResponseWriter, r *http.Request, query url.Values, bck *cluster.Bck, objName string) {
	var (
		ptime        = isRedirect(query)
		config       = cmn.GCO.Get()
		isGFNRequest = cmn.IsParseBool(query.Get(cmn.URLParamIsGFNRequest))
		started      = time.Now()
	)

	if ptime != "" {
		if redelta := requestLatency(started, ptime); redelta != 0 {
			t.statsT.Add(stats.GetRedirLatency, redelta)
		}
	}
	lom := cluster.AllocLOM(objName, "")
	defer cluster.FreeLOM(lom)
	if err := lom.Init(bck.Bck); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); ok {
			t.BMDVersionFixup(r, cmn.Bck{}, true /* sleep */)
			err = lom.Init(bck.Bck)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	}

	if isETLRequest(query) {
		t.doETL(w, r, query.Get(cmn.URLParamUUID), bck, objName)
		return
	}
	goi := &getObjInfo{
		started: started,
		t:       t,
		lom:     lom,
		w:       w,
		ctx:     context.Background(),
		ranges:  cmn.RangesQuery{Range: r.Header.Get(cmn.HeaderRange), Size: 0},
		isGFN:   isGFNRequest,
		chunked: config.Net.HTTP.Chunked,
	}
	if bck.IsHTTP() {
		originalURL := query.Get(cmn.URLParamOrigURL)
		goi.ctx = context.WithValue(goi.ctx, cmn.CtxOriginalURL, originalURL)
	}
	if errCode, err := goi.getObject(); err != nil {
		if cmn.IsErrConnectionReset(err) {
			glog.Errorf("GET %s: %v", lom, err)
		} else {
			t.invalmsghdlr(w, r, err.Error(), errCode)
		}
	}
}

// PUT /v1/objects/bucket-name/object-name
func (t *targetrunner) httpobjput(w http.ResponseWriter, r *http.Request) {
	var (
		query   = r.URL.Query()
		ptime   string
		request = &apiRequest{after: 2, prefix: cmn.URLPathObjects.L}
	)

	if ptime = isRedirect(query); ptime == "" && !isIntraPut(r.Header) {
		// TODO: send TCP RST?
		t.invalmsghdlrf(w, r, "%s: %s(obj) is expected to be redirected or replicated", t.si, r.Method)
		return
	}
	if err := t.parseAPIRequest(w, r, request); err != nil {
		return
	}
	objName := request.items[1]

	started := time.Now()
	if ptime != "" {
		if redelta := requestLatency(started, ptime); redelta != 0 {
			t.statsT.Add(stats.PutRedirLatency, redelta)
		}
	}
	if cs := fs.GetCapStatus(); cs.Err != nil {
		go t.RunLRU("" /*uuid*/, false)
		if cs.OOS {
			t.invalmsghdlr(w, r, cs.Err.Error())
			return
		}
	}
	lom := cluster.AllocLOM(objName, "")
	defer cluster.FreeLOM(lom)

	if err := lom.Init(request.bck.Bck); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); ok {
			t.BMDVersionFixup(r, cmn.Bck{}, true /* sleep */)
			err = lom.Init(request.bck.Bck)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	}
	if lom.Load() == nil { // if exists, check custom md
		srcProvider, hasSrc := lom.GetCustomMD(cluster.SourceObjMD)
		if hasSrc && srcProvider != cluster.SourceWebObjMD {
			bck := lom.Bck()
			if bck.IsAIS() {
				t.invalmsghdlrf(w, r,
					"bucket %s: cannot override %s-downloaded object", bck, srcProvider)
				return
			}
			if b := bck.RemoteBck(); b != nil && b.Provider != srcProvider {
				t.invalmsghdlrf(w, r,
					"bucket %s: cannot override %s-downloaded object", b, srcProvider)
				return
			}
		}
	}
	lom.SetAtimeUnix(started.UnixNano())
	appendTy := query.Get(cmn.URLParamAppendType)
	if appendTy == "" {
		if errCode, err := t.doPut(r, lom, started); err != nil {
			t.fsErr(err, lom.FQN)
			t.invalmsghdlr(w, r, err.Error(), errCode)
		}
	} else {
		if handle, errCode, err := t.doAppend(r, lom, started); err != nil {
			t.invalmsghdlr(w, r, err.Error(), errCode)
		} else {
			w.Header().Set(cmn.HeaderAppendHandle, handle)
		}
	}
}

// DELETE [ { action } ] /v1/objects/bucket-name/object-name
func (t *targetrunner) httpobjdelete(w http.ResponseWriter, r *http.Request) {
	var (
		msg     aisMsg
		evict   bool
		query   = r.URL.Query()
		request = &apiRequest{after: 2, prefix: cmn.URLPathObjects.L}
	)
	if err := cmn.ReadJSON(w, r, &msg, true); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if isRedirect(query) == "" {
		t.invalmsghdlrf(w, r, "%s: %s(obj) is expected to be redirected", t.si, r.Method)
		return
	}
	if err := t.parseAPIRequest(w, r, request); err != nil {
		return
	}

	evict = msg.Action == cmn.ActEvictObjects
	lom := cluster.AllocLOM(request.items[1], "")
	defer cluster.FreeLOM(lom)
	if err := lom.Init(request.bck.Bck); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	errCode, err := t.DeleteObject(context.Background(), lom, evict)
	if err != nil {
		if errCode == http.StatusNotFound {
			t.invalmsghdlrsilent(w, r,
				fmt.Sprintf("object %s/%s doesn't exist", lom.Bucket(), lom.ObjName),
				http.StatusNotFound,
			)
		} else {
			t.invalmsghdlr(w, r, err.Error(), errCode)
		}
		return
	}
	// EC cleanup if EC is enabled
	ec.ECM.CleanupObject(lom)
}

// POST /v1/objects/bucket-name/object-name
func (t *targetrunner) httpobjpost(w http.ResponseWriter, r *http.Request) {
	var (
		msg   cmn.ActionMsg
		query = r.URL.Query()
	)
	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case cmn.ActRenameObject:
		if isRedirect(query) == "" {
			t.invalmsghdlrf(w, r, "%s: %s-%s(obj) is expected to be redirected", t.si, r.Method, msg.Action)
			return
		}
		t.renameObject(w, r, &msg)
	case cmn.ActPromote:
		if isRedirect(query) == "" && !isIntraCall(r.Header) {
			t.invalmsghdlrf(w, r, "%s: %s-%s(obj) is expected to be redirected or intra-called",
				t.si, r.Method, msg.Action)
			return
		}
		t.promoteFQN(w, r, &msg)
	default:
		t.invalmsghdlrf(w, r, fmtUnknownAct, msg)
	}
}

// HEAD /v1/objects/<bucket-name>/<object-name>
//
// Initially validates if the request is internal request (either from proxy
// or target) and calls headObject.
func (t *targetrunner) httpobjhead(w http.ResponseWriter, r *http.Request) {
	var (
		features = cmn.GCO.Get().Client.Features
		query    = r.URL.Query()
		request  = &apiRequest{after: 2, prefix: cmn.URLPathObjects.L}
	)
	if isRedirect(query) == "" && !isIntraCall(r.Header) && !features.IsSet(cmn.FeatureDirectAccess) {
		t.invalmsghdlrf(w, r, "%s: %s(obj) is expected to be redirected (remaddr=%s)",
			t.si, r.Method, r.RemoteAddr)
		return
	}
	if err := t.parseAPIRequest(w, r, request); err != nil {
		return
	}
	t.headObject(w, r, query, request.bck, request.items[1])
}

// headObject is main function to head the object. It doesn't check request origin,
// so it must be done by the caller (if necessary).
func (t *targetrunner) headObject(w http.ResponseWriter, r *http.Request, query url.Values, bck *cluster.Bck, objName string) {
	var (
		err            error
		invalidHandler = t.invalmsghdlr
		hdr            = w.Header()
		checkExists    = cmn.IsParseBool(query.Get(cmn.URLParamCheckExists))
		checkExistsAny = cmn.IsParseBool(query.Get(cmn.URLParamCheckExistsAny))
		silent         = cmn.IsParseBool(query.Get(cmn.URLParamSilent))
	)
	if silent {
		invalidHandler = t.invalmsghdlrsilent
	}

	lom := cluster.AllocLOM(objName, "")
	defer cluster.FreeLOM(lom)
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

	exists := err == nil

	// * checkExists and checkExistsAny establish local presence of the object by looking up all mountpaths
	// * checkExistsAny does it *even* if the object *may* not have local copies
	// * see also: GFN
	if !exists {
		// lookup and restore the object to its proper location
		if (checkExists && lom.HasCopies()) || checkExistsAny {
			exists = lom.RestoreObjectFromAny()
		}
	}

	if checkExists || checkExistsAny {
		if !exists {
			invalidHandler(w, r, fmt.Sprintf("%s/%s %s", bck.Name, objName, cmn.DoesNotExist),
				http.StatusNotFound)
		}
		return
	}
	if lom.Bck().IsAIS() || exists { // && !lom.VerConf().Enabled) {
		if !exists {
			invalidHandler(w, r, fmt.Sprintf("%s/%s %s", bck.Name, objName, cmn.DoesNotExist),
				http.StatusNotFound)
			return
		}
		lom.ToHTTPHdr(hdr)
	} else {
		objMeta, errCode, err := t.Backend(lom.Bck()).HeadObj(context.Background(), lom)
		if err != nil {
			errMsg := fmt.Sprintf("%s: HEAD request failed, err: %v", lom, err)
			invalidHandler(w, r, errMsg, errCode)
			return
		}
		for k, v := range objMeta {
			hdr.Set(k, v)
		}
	}
	objProps := cmn.ObjectProps{
		Name:    objName,
		Bck:     lom.Bucket(),
		Present: exists,
	}
	if exists {
		objProps.Size = lom.Size()
		objProps.NumCopies = lom.NumCopies()
		if lom.Bck().Props.EC.Enabled {
			if md, err := ec.ObjectMetadata(lom.Bck(), objName); err == nil {
				hdr.Set(cmn.HeaderObjECMeta, ec.MetaToString(md))
			}
		}
	}
	err = cmn.IterFields(objProps, func(tag string, field cmn.IterField) (err error, b bool) {
		if hdr.Get(tag) == "" {
			hdr.Set(tag, fmt.Sprintf("%v", field.Value()))
		}
		return nil, false
	})
	cmn.AssertNoErr(err)

	if isETLRequest(query) {
		// We don't know neither length of on-the-fly transformed object, nor checksum.
		hdr.Del(cmn.HeaderContentLength)
		hdr.Del(cmn.GetPropsChecksum)
		hdr.Del(cmn.HeaderObjCksumVal)
		hdr.Del(cmn.HeaderObjCksumType)
	}
}

//////////////////////
// httpec* handlers //
//////////////////////

// Returns a slice. Does not use GFN.
func (t *targetrunner) httpecget(w http.ResponseWriter, r *http.Request) {
	request := &apiRequest{after: 3, prefix: cmn.URLPathEC.L, bckIdx: 1}
	if err := t.parseAPIRequest(w, r, request); err != nil {
		return
	}

	switch request.items[0] {
	case ec.URLMeta:
		t.sendECMetafile(w, r, request.bck, request.items[2])
	case ec.URLCT:
		t.sendECCT(w, r, request.bck, request.items[2])
	default:
		t.invalmsghdlrf(w, r, "invalid EC URL path %s", request.items[0])
	}
}

// Returns a CT's metadata.
func (t *targetrunner) sendECMetafile(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) {
	if err := bck.Init(t.owner.bmd); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); !ok { // is ais
			t.invalmsghdlrsilent(w, r, err.Error())
			return
		}
	}
	md, err := ec.ObjectMetadata(bck, objName)
	if err != nil {
		if os.IsNotExist(err) {
			t.invalmsghdlrsilent(w, r, err.Error(), http.StatusNotFound)
		} else {
			t.invalmsghdlrsilent(w, r, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	w.Write(md.Marshal())
}

func (t *targetrunner) sendECCT(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) {
	lom := cluster.AllocLOM(objName, "")
	defer cluster.FreeLOM(lom)
	if err := lom.Init(bck.Bck); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); ok {
			t.BMDVersionFixup(r, cmn.Bck{}, true /* sleep */)
			err = lom.Init(bck.Bck)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	}
	sliceFQN := lom.MpathInfo().MakePathFQN(bck.Bck, ec.SliceType, objName)
	finfo, err := os.Stat(sliceFQN)
	if err != nil {
		t.invalmsghdlrsilent(w, r, err.Error(), http.StatusNotFound)
		return
	}
	file, err := os.Open(sliceFQN)
	if err != nil {
		t.fsErr(err, sliceFQN)
		t.invalmsghdlr(w, r, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Length", strconv.FormatInt(finfo.Size(), 10))
	_, err = io.Copy(w, file) // No need for `io.CopyBuffer` as `sendfile` syscall will be used.
	cmn.Close(file)
	if err != nil {
		glog.Errorf("Failed to send slice %s/%s: %v", bck, objName, err)
	}
}

//
// supporting methods
//

// CheckRemoteVersion sets `vchanged` to true if object versions differ between
// remote object and local cache.
// NOTE: Should be called only if the local copy exists.
func (t *targetrunner) CheckRemoteVersion(ctx context.Context, lom *cluster.LOM) (vchanged bool, errCode int, err error) {
	var objMeta cmn.SimpleKVs
	objMeta, errCode, err = t.Backend(lom.Bck()).HeadObj(ctx, lom)
	if err != nil {
		err = fmt.Errorf("%s: failed to head metadata, err: %v", lom, err)
		return
	}
	if remoteVersion, ok := objMeta[cmn.HeaderObjVersion]; ok {
		if lom.Version() != remoteVersion {
			glog.Infof("%s: version changed from %s to %s", lom, lom.Version(), remoteVersion)
			vchanged = true
		}
	}
	return
}

func (t *targetrunner) listBuckets(w http.ResponseWriter, r *http.Request, query cmn.QueryBcks) {
	var (
		bucketNames cmn.BucketNames
		code        int
		err         error
		config      = cmn.GCO.Get()
	)

	if query.Provider != "" {
		bucketNames, code, err = t._listBcks(query, config)
		if err != nil {
			t.invalmsghdlrstatusf(w, r, code, "failed to list buckets for %q, err: %v", query, err)
			return
		}
	} else /* all providers */ {
		for provider := range cmn.Providers {
			var buckets cmn.BucketNames
			query.Provider = provider
			buckets, code, err = t._listBcks(query, config)
			if err != nil {
				t.invalmsghdlrstatusf(w, r, code, "failed to list buckets for %q, err: %v", query, err)
				return
			}
			bucketNames = append(bucketNames, buckets...)
		}
	}
	sort.Sort(bucketNames)
	t.writeJSON(w, r, bucketNames, listBuckets)
}

func (t *targetrunner) _listBcks(query cmn.QueryBcks, cfg *cmn.Config) (names cmn.BucketNames, errCode int, err error) {
	_, ok := cfg.Backend.Providers[query.Provider]
	// HDFS doesn't support listing remote buckets (there are no remote buckets).
	if (!ok && !query.IsRemoteAIS()) || query.IsHDFS() {
		names = t.selectBMDBuckets(t.owner.bmd.get(), query)
	} else {
		bck := cluster.NewBck("", query.Provider, query.Ns)
		names, errCode, err = t.Backend(bck).ListBuckets(context.Background(), query)
		sort.Sort(names)
	}
	return
}

func (t *targetrunner) doAppend(r *http.Request, lom *cluster.LOM, started time.Time) (newHandle string, errCode int, err error) {
	var (
		cksumValue    = r.Header.Get(cmn.HeaderObjCksumVal)
		cksumType     = r.Header.Get(cmn.HeaderObjCksumType)
		contentLength = r.Header.Get("Content-Length")
		query         = r.URL.Query()
		handle        = query.Get(cmn.URLParamAppendHandle)
	)

	hi, err := parseAppendHandle(handle)
	if err != nil {
		return "", http.StatusBadRequest, err
	}

	aoi := &appendObjInfo{
		started: started,
		t:       t,
		lom:     lom,
		r:       r.Body,
		op:      query.Get(cmn.URLParamAppendType),
		hi:      hi,
	}
	if contentLength != "" {
		if size, ers := strconv.ParseInt(contentLength, 10, 64); ers == nil {
			aoi.size = size
		}
	}
	if cksumValue != "" {
		aoi.cksum = cmn.NewCksum(cksumType, cksumValue)
	}
	return aoi.appendObject()
}

// PUT new version and update object metadata
// ais bucket:
//  - if ais bucket versioning is enabled, the version is auto-incremented
// Cloud bucket:
//  - returned version ID is the version
// In both cases, new checksum is also generated and stored along with the new version.
func (t *targetrunner) doPut(r *http.Request, lom *cluster.LOM, started time.Time) (errCode int, err error) {
	var (
		header     = r.Header
		cksumType  = header.Get(cmn.HeaderObjCksumType)
		cksumValue = header.Get(cmn.HeaderObjCksumVal)
		recvType   = r.URL.Query().Get(cmn.URLParamRecvType)
	)
	lom.FromHTTPHdr(header) // TODO: check that values parsed here are not coming from the user
	poi := &putObjInfo{
		started:    started,
		t:          t,
		lom:        lom,
		r:          r.Body,
		cksumToUse: cmn.NewCksum(cksumType, cksumValue),
		ctx:        context.Background(),
		workFQN:    fs.CSM.GenContentFQN(lom, fs.WorkfileType, fs.WorkfilePut),
	}
	if recvType != "" {
		n, err := strconv.Atoi(recvType)
		if err != nil {
			return http.StatusBadRequest, fmt.Errorf("failed to parse receive type, err: %v", err)
		}
		poi.recvType = cluster.RecvType(n)
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
	if !lom.MirrorConf().Enabled {
		return
	}
	if mpathCnt := fs.NumAvail(); mpathCnt < 2 {
		glog.Errorf("%s: insufficient mountpaths (%d)", lom, mpathCnt)
		return
	}
	var err error
	for i := 0; i < retries; i++ {
		xputlrep := xreg.RenewPutMirror(t, lom)
		if xputlrep == nil {
			return
		}
		err = xputlrep.(*mirror.XactPut).Repl(lom)
		if xaction.IsErrXactExpired(err) {
			break
		}
		// retry upon race vs (just finished/timed_out)
	}
	if err != nil {
		glog.Errorf("%s: unexpected failure to initiate local mirroring, err: %v", lom, err)
	}
}

func (t *targetrunner) DeleteObject(ctx context.Context, lom *cluster.LOM, evict bool) (int, error) {
	var (
		cloudErr     error
		cloudErrCode int
		errRet       error
		delFromAIS   bool
	)
	lom.Lock(true)
	defer lom.Unlock(true)

	delFromCloud := lom.Bck().IsRemote() && !evict
	if err := lom.Load(false); err == nil {
		delFromAIS = true
	} else if !cmn.IsObjNotExist(err) {
		return 0, err
	} else if !delFromCloud && cmn.IsObjNotExist(err) {
		return http.StatusNotFound, err
	}

	if delFromCloud {
		if errCode, err := t.Backend(lom.Bck()).DeleteObj(ctx, lom); err != nil {
			cloudErr = err
			cloudErrCode = errCode
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
				return 0, errRet
			}
		}
		if evict {
			cmn.Assert(lom.Bck().IsRemote())
			t.statsT.AddMany(
				stats.NamedVal64{Name: stats.LruEvictCount, Value: 1},
				stats.NamedVal64{Name: stats.LruEvictSize, Value: lom.Size()},
			)
		}
	}
	if cloudErr != nil {
		return cloudErrCode, cloudErr
	}
	return 0, errRet
}

///////////////////
// RENAME OBJECT //
///////////////////

// TODO: unify with PromoteFile (refactor)
func (t *targetrunner) renameObject(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg) {
	request := &apiRequest{after: 2, prefix: cmn.URLPathObjects.L}
	if err := t.parseAPIRequest(w, r, request); err != nil {
		return
	}
	lom := cluster.AllocLOM(request.items[1], "")
	defer cluster.FreeLOM(lom)
	if err := lom.Init(request.bck.Bck); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	if lom.Bck().IsRemote() {
		t.invalmsghdlrf(w, r, "%s: cannot rename object %s from a remote bucket", t.si, lom)
		return
	}
	if lom.Bck().Props.EC.Enabled {
		t.invalmsghdlrf(w, r, "%s: cannot rename erasure-coded object %s", t.si, lom)
		return
	}
	buf, slab := t.gmm.Alloc()
	coi := &copyObjInfo{
		CopyObjectParams: cluster.CopyObjectParams{
			BckTo: lom.Bck(),
			Buf:   buf,
		},
		t:         t,
		localOnly: false,
		finalize:  true,
	}
	copied, err := coi.copyObject(lom, msg.Name /* new object name */)
	slab.Free(buf)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if copied {
		lom.Lock(true)
		if err = lom.Remove(); err != nil {
			glog.Warningf("%s: failed to delete renamed object source %s: %v", t.si, lom, err)
		}
		lom.Unlock(true)
	}
}

///////////////////////////////////////
// PROMOTE local file(s) => objects  //
///////////////////////////////////////

func (t *targetrunner) promoteFQN(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg) {
	const fmtErr = "%s: %s failed: "
	request := &apiRequest{after: 1, prefix: cmn.URLPathObjects.L}
	if err := t.parseAPIRequest(w, r, request); err != nil {
		return
	}

	promoteArgs := cmn.ActValPromote{}
	if err := cmn.MorphMarshal(msg.Value, &promoteArgs); err != nil {
		return
	}

	if promoteArgs.Target != "" && promoteArgs.Target != t.si.ID() {
		glog.Errorf("%s: unexpected target ID %s mismatch", t.si, promoteArgs.Target)
	}

	// 2. init & validate
	srcFQN := msg.Name
	if srcFQN == "" {
		t.invalmsghdlrf(w, r, fmtErr+"missing source filename", t.si, msg.Action)
		return
	}

	finfo, err := os.Stat(srcFQN)
	if err != nil {
		if os.IsNotExist(err) {
			errMsg := fmt.Sprintf("%s: %q %s", t.si.ID(), srcFQN, cmn.DoesNotExist)
			t.invalmsghdlr(w, r, errMsg, http.StatusNotFound)
			return
		}
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if err = request.bck.Init(t.owner.bmd); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); ok {
			t.BMDVersionFixup(r, cmn.Bck{}, true /* sleep */)
			err = request.bck.Init(t.owner.bmd)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	}

	// 3a. promote dir
	if finfo.IsDir() {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s: promote %+v", t.si, promoteArgs)
		}
		xact, err := xreg.RenewDirPromote(t, request.bck, srcFQN, &promoteArgs)
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
		go xact.Run()
		return
	}
	// 3b. promote file
	objName := promoteArgs.ObjName
	if objName == "" || objName[len(objName)-1] == os.PathSeparator {
		objName += filepath.Base(srcFQN)
	}
	params := cluster.PromoteFileParams{
		SrcFQN:    srcFQN,
		Bck:       request.bck,
		ObjName:   objName,
		Overwrite: promoteArgs.Overwrite,
		KeepOrig:  promoteArgs.KeepOrig,
	}
	if _, err = t.PromoteFile(params); err != nil {
		t.invalmsghdlrf(w, r, fmtErr+" %s", t.si, msg.Action, err.Error())
	}
	// TODO: inc stats
}

// fshc wakes up FSHC and makes it to run filesystem check immediately if err != nil
func (t *targetrunner) fsErr(err error, filepath string) {
	if !cmn.GCO.Get().FSHC.Enabled {
		return
	}
	if !cmn.IsIOError(err) {
		return
	}
	glog.Errorf("FSHC: fqn %s, err %v", filepath, err)
	mpathInfo, _ := fs.Path2MpathInfo(filepath)
	if mpathInfo == nil {
		return
	}
	keyName := mpathInfo.Path
	// keyName is the mountpath is the fspath - counting IO errors on a per basis..
	t.statsT.AddMany(stats.NamedVal64{Name: stats.ErrIOCount, NameSuffix: keyName, Value: 1})
	t.fshc.OnErr(filepath)
}

func (t *targetrunner) runResilver(id string, skipGlobMisplaced bool, notifs ...*xaction.NotifXact) {
	if id == "" {
		id = cmn.GenUUID()
		regMsg := xactRegMsg{UUID: id, Kind: cmn.ActResilver, Srcs: []string{t.si.ID()}}
		msg := t.newAisMsg(&cmn.ActionMsg{Action: cmn.ActRegGlobalXaction, Value: regMsg}, nil, nil)
		t.bcastAsyncIC(msg)
	}
	t.rebManager.RunResilver(id, skipGlobMisplaced, notifs...)
}
