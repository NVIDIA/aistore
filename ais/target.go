// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
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
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/ais/cloud"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/dbdriver"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/etl"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/health"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/mirror"
	"github.com/NVIDIA/aistore/nl"
	"github.com/NVIDIA/aistore/reb"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xaction"
	"github.com/NVIDIA/aistore/xaction/xreg"
	jsoniter "github.com/json-iterator/go"
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
	clouds map[string]cluster.CloudProvider
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
		regstate regstate     // the state of being registered with the primary, can be (en/dis)abled via API
		gmm      *memsys.MMSA // system pagesize-based memory manager and slab allocator
		smm      *memsys.MMSA // system MMSA for small-size allocations
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

	cluster.Init()
	cluster.InitTarget()

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

	var (
		contactURLs    []string
		smap, reliable = t.tryLoadSmap()
	)
	if !reliable {
		if smap.isValid() {
			// As we cannot fully rely on the loaded smap we can just gather
			// a couple of random contact URLs to increase our join chances.
			var (
				candidatesCnt = cmn.Min(int(cmn.FastLog2(uint64(len(smap.Pmap)))), 5)
				candidates    = cmn.NewStringSet(smap.Primary.URL(cmn.NetworkIntraControl))
			)
			for _, proxy := range smap.Pmap {
				candidates.Add(proxy.URL(cmn.NetworkIntraControl))
				if len(candidates) >= candidatesCnt {
					break
				}
			}
			contactURLs = candidates.Keys()
		}
		smap = newSmap()
	}
	// Insert self and always proceed starting up.
	smap.Tmap[t.si.ID()] = t.si
	t.owner.smap.put(smap)

	// Try joining the cluster.
	if err := t.withRetry(t.joinCluster, "join", true /*backoff*/, contactURLs...); err != nil {
		if reliable {
			var (
				smapMaxVer int64
				primaryURL string
			)
			if smapMaxVer, primaryURL = t.bcastHealth(smap); smapMaxVer > smap.version() {
				glog.Infof("%s: local copy of %s is older than v%d - retrying via %s",
					t.si, smap, smapMaxVer, primaryURL)
				err = t.withRetry(t.joinCluster, "join", true, primaryURL)
			}
		}
		if err != nil {
			glog.Errorf("%s failed to join cluster, err: %v", t.si, err)
			glog.Errorf("%s is terminating", t.si)
			return err
		}
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
	ais := cloud.NewAIS(t)
	c[cmn.ProviderAIS] = ais // ais cloud is always present
	if aisConf, ok := config.Cloud.ProviderConf(cmn.ProviderAIS); ok {
		if err := ais.Apply(aisConf, "init"); err != nil {
			glog.Errorf("%s: %v - proceeding to start anyway...", t.si, err)
		}
	}

	c[cmn.ProviderHTTP], _ = cloud.NewHTTP(t, config)
	if err := c.initExt(t); err != nil {
		cmn.ExitLogf("%v", err)
	}
}

// 3rd part cloud: empty stubs unless populated via build tags
func (c clouds) initExt(t *targetrunner) (err error) {
	config := cmn.GCO.Get()
	for provider := range config.Cloud.Providers {
		switch provider {
		case cmn.ProviderAmazon:
			c[provider], err = cloud.NewAWS(t)
		case cmn.ProviderGoogle:
			c[provider], err = cloud.NewGCP(t)
		case cmn.ProviderAzure:
			c[provider], err = cloud.NewAzure(t)
		default:
			err = fmt.Errorf("unknown cloud provider: %q", provider)
		}
		if err != nil {
			return
		}
	}
	return
}

func (t *targetrunner) initRecvHandlers() {
	networkHandlers := []networkHandler{
		{
			r: cmn.Buckets, h: t.bucketHandler,
			net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl, cmn.NetworkIntraData},
		},
		{r: cmn.Objects, h: t.objectHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraData}},
		{r: cmn.Daemon, h: t.daemonHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl}},
		{r: cmn.Metasync, h: t.metasyncHandler, net: []string{cmn.NetworkIntraControl}},
		{r: cmn.Health, h: t.healthHandler, net: []string{cmn.NetworkIntraControl}},
		{r: cmn.Xactions, h: t.xactHandler, net: []string{cmn.NetworkIntraControl}},
		{r: cmn.Rebalance, h: t.rebManager.RespHandler, net: []string{cmn.NetworkIntraData}},
		{r: cmn.EC, h: t.ecHandler, net: []string{cmn.NetworkIntraData}},
		{r: cmn.Vote, h: t.voteHandler, net: []string{cmn.NetworkIntraControl}},
		{r: cmn.Txn, h: t.txnHandler, net: []string{cmn.NetworkIntraControl}},
		{
			r: cmn.ObjStream, h: transport.RxAnyStream,
			net: []string{cmn.NetworkPublic, cmn.NetworkIntraData, cmn.NetworkIntraControl},
		},
		{r: cmn.Tokens, h: t.tokenHandler, net: []string{cmn.NetworkPublic}},

		{r: cmn.Download, h: t.downloadHandler, net: []string{cmn.NetworkIntraControl}},
		{
			r: cmn.Sort, h: dsort.SortHandler,
			net: []string{cmn.NetworkIntraControl, cmn.NetworkIntraData},
		},
		{
			r: cmn.ETL, h: t.etlHandler,
			net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl},
		},

		{r: cmn.Query, h: t.queryHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl}},

		{r: "/" + cmn.S3, h: t.s3Handler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraData}},

		{
			r: "/", h: cmn.InvalidHandler,
			net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl, cmn.NetworkIntraData},
		},
	}
	t.registerNetworkHandlers(networkHandlers)
}

// stop gracefully
func (t *targetrunner) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", t.Name(), err)
	xreg.AbortAll()
	if t.publicServer.s != nil {
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
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	switch apiItems[0] {
	case cmn.AllBuckets:
		var (
			query = r.URL.Query()
			what  = query.Get(cmn.URLParamWhat)
		)
		if what == cmn.GetWhatBMD {
			t.writeJSON(w, r, t.owner.bmd.get(), "get-what-bmd")
		} else {
			bck, err := newBckFromQuery("", query)
			if err != nil {
				t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
				return
			}
			t.listBuckets(w, r, cmn.QueryBcks(bck.Bck))
		}
	default:
		t.invalmsghdlrf(w, r, "Invalid route /buckets/%s", apiItems[0])
	}
}

// DELETE { action } /v1/buckets/bucket-name
// (evict | delete) (list | range)
func (t *targetrunner) httpbckdelete(w http.ResponseWriter, r *http.Request) {
	var (
		msg           = &aisMsg{}
		apiItems, err = t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	)
	if err != nil {
		return
	}
	bucket := apiItems[0]
	bck, err := newBckFromQuery(bucket, r.URL.Query())
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	if err = bck.Init(t.owner.bmd, t.si); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); ok {
			t.BMDVersionFixup(r, cmn.Bck{}, true /* sleep */)
			err = bck.Init(t.owner.bmd, t.si)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	}
	b, err := ioutil.ReadAll(r.Body)
	if err == nil && len(b) > 0 {
		err = jsoniter.Unmarshal(b, msg)
	}
	if err != nil {
		s := fmt.Sprintf("failed to read %s body, err: %v", r.Method, err)
		if err == io.EOF {
			trailer := r.Trailer.Get("Error")
			if trailer != "" {
				s = fmt.Sprintf("failed to read %s request, err: %v, trailer: %s", r.Method, err, trailer)
			}
		}
		t.invalmsghdlr(w, r, s)
		return
	}

	switch msg.Action {
	case cmn.ActDelete, cmn.ActEvictObjects:
		if len(b) == 0 { // must be a List/Range request
			t.invalmsghdlr(w, r, "invalid API request: no message body")
			return
		}
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
		xact, err := xreg.RenewEvictDelete(t, bck, args)
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

// POST /v1/buckets/bucket-name
func (t *targetrunner) httpbckpost(w http.ResponseWriter, r *http.Request) {
	var (
		bucket string
		msg    = &aisMsg{}
		query  = r.URL.Query()
	)
	if cmn.ReadJSON(w, r, msg) != nil {
		return
	}
	apiItems, err := t.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}

	t.ensureLatestSmap(msg, r)

	if len(apiItems) == 0 {
		switch msg.Action {
		case cmn.ActSummaryBucket:
			bck, err := newBckFromQuery("", query)
			if err != nil {
				t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
				return
			}
			if !t.bucketSummary(w, r, bck, msg) {
				return
			}
		default:
			t.invalmsghdlr(w, r, "url path is too short: bucket name is missing", http.StatusBadRequest)
		}
		return
	}

	bucket = apiItems[0]
	bck, err := newBckFromQuery(bucket, query)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}

	if err = bck.Init(t.owner.bmd, t.si); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); ok {
			t.BMDVersionFixup(r, cmn.Bck{}, true /* sleep */)
			err = bck.Init(t.owner.bmd, t.si)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	}
	switch msg.Action {
	case cmn.ActPrefetch:
		if !bck.IsRemote() {
			t.invalmsghdlrf(w, r, "%s: expecting remote bucket, got %s, action=%s", t.si, bck, msg.Action)
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
		xact := xreg.RenewPrefetch(t, bck, args)
		go xact.Run()
	case cmn.ActListObjects:
		// list the bucket and return
		begin := mono.NanoTime()
		if ok := t.listObjects(w, r, bck, msg); !ok {
			return
		}

		delta := mono.Since(begin)
		t.statsT.AddMany(
			stats.NamedVal64{Name: stats.ListCount, Value: 1},
			stats.NamedVal64{Name: stats.ListLatency, Value: int64(delta)},
		)
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("LIST: %s, %s", bucket, delta)
		}
	case cmn.ActSummaryBucket:
		if !t.bucketSummary(w, r, bck, msg) {
			return
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
	)
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apiItems[0]
	bck, err := newBckFromQuery(bucket, query)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	if err = bck.Init(t.owner.bmd, t.si); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); !ok { // is ais
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
		t.bucketPropsToHdr(bck, hdr)
		return
	}
	if bck.IsHTTP() {
		originalURL := query.Get(cmn.URLParamOrigURL)
		ctx = context.WithValue(ctx, cmn.CtxOriginalURL, originalURL)
		if !inBMD && originalURL == "" {
			err = cmn.NewErrorRemoteBucketDoesNotExist(bck.Bck, t.si.String())
			t.invalmsghdlrsilent(w, r, err.Error(), http.StatusNotFound)
			return
		}
	}
	// + cloud
	bucketProps, code, err = t.Cloud(bck).HeadBucket(ctx, bck)
	if err != nil {
		if !inBMD {
			if code == http.StatusNotFound {
				err = cmn.NewErrorRemoteBucketDoesNotExist(bck.Bck, t.si.String())
				t.invalmsghdlrsilent(w, r, err.Error(), code)
			} else {
				err = fmt.Errorf("%s: bucket %s, err: %v", t.si, bck, err)
				t.invalmsghdlr(w, r, err.Error(), code)
			}
			return
		}
		bucketProps = make(cmn.SimpleKVs)
		glog.Warningf("%s: bucket %s, err: %v(%d)", t.si, bck, err, code)
		bucketProps[cmn.HeaderCloudProvider] = bck.Provider
		// TODO: what about `HTTP` cloud?
		bucketProps[cmn.HeaderCloudOffline] = strconv.FormatBool(bck.IsCloud())
		bucketProps[cmn.HeaderRemoteAisOffline] = strconv.FormatBool(bck.IsRemoteAIS())
	}
	for k, v := range bucketProps {
		if k == cmn.HeaderBucketVerEnabled && bck.Props != nil {
			if curr := strconv.FormatBool(bck.VersionConf().Enabled); curr != v {
				// e.g., change via vendor-provided CLI and similar
				glog.Errorf("%s: %s versioning got out of sync: %s != %s", t.si, bck, v, curr)
			}
		}
		hdr.Set(k, v)
	}
	t.bucketPropsToHdr(bck, hdr)
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
	apiItems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}

	var (
		query    = r.URL.Query()
		ptime    = isRedirect(query)
		features = cmn.GCO.Get().Client.Features

		bucket  = apiItems[0]
		objName = apiItems[1]
	)

	if !isIntraCall(r.Header) && ptime == "" && !features.IsSet(cmn.FeatureDirectAccess) {
		t.invalmsghdlrf(w, r, "%s: %s(obj) is expected to be redirected (remaddr=%s)",
			t.si, r.Method, r.RemoteAddr)
		return
	}

	t.getObject(w, r, query, bucket, objName)
}

// getObject is main function to get the object. It doesn't check request origin,
// so it must be done by the caller (if necessary).
func (t *targetrunner) getObject(w http.ResponseWriter, r *http.Request, query url.Values, bucket, objName string) {
	var (
		ptime        = isRedirect(query)
		config       = cmn.GCO.Get()
		isGFNRequest = cmn.IsParseBool(query.Get(cmn.URLParamIsGFNRequest))
		started      = time.Now()
	)

	// TODO: return TCP RST here and elsewhere

	if ptime != "" {
		if redelta := redirectLatency(started, ptime); redelta != 0 {
			t.statsT.Add(stats.GetRedirLatency, redelta)
		}
	}
	bck, err := newBckFromQuery(bucket, query)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	lom := &cluster.LOM{T: t, ObjName: objName}
	if err = lom.Init(bck.Bck, config); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); ok {
			t.BMDVersionFixup(r, cmn.Bck{}, true /* sleep */)
			err = lom.Init(bck.Bck, config)
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
		config = cmn.GCO.Get()
		query  = r.URL.Query()
		ptime  string
	)
	if ptime = isRedirect(query); ptime == "" && !isIntraPut(r.Header) {
		// TODO: send TCP RST?
		t.invalmsghdlrf(w, r, "%s: %s(obj) is expected to be redirected or replicated", t.si, r.Method)
		return
	}
	apiItems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objName := apiItems[0], apiItems[1]

	started := time.Now()
	if ptime != "" {
		if redelta := redirectLatency(started, ptime); redelta != 0 {
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
	bck, err := newBckFromQuery(bucket, query)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	lom := &cluster.LOM{T: t, ObjName: objName}
	if err = lom.Init(bck.Bck, config); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); ok {
			t.BMDVersionFixup(r, cmn.Bck{}, true /* sleep */)
			err = lom.Init(bck.Bck, config)
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
		msg   cmn.ActionMsg
		evict bool
		query = r.URL.Query()
	)
	if isRedirect(query) == "" {
		t.invalmsghdlrf(w, r, "%s: %s(obj) is expected to be redirected", t.si, r.Method)
		return
	}
	apiItems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objName := apiItems[0], apiItems[1]
	if err := cmn.ReadJSON(w, r, &msg, true /*optional=allow empty body*/); err != nil {
		return
	}
	evict = msg.Action == cmn.ActEvictObjects

	bck, err := newBckFromQuery(bucket, query)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	lom := &cluster.LOM{T: t, ObjName: objName}
	if err = lom.Init(bck.Bck); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	errCode, err := t.DeleteObject(context.Background(), lom, evict)
	if err != nil {
		if errCode == http.StatusNotFound {
			t.invalmsghdlrsilent(w, r,
				fmt.Sprintf("object %s/%s doesn't exist", lom.Bck(), lom.ObjName),
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
	)
	if isRedirect(query) == "" && !isIntraCall(r.Header) && !features.IsSet(cmn.FeatureDirectAccess) {
		t.invalmsghdlrf(w, r, "%s: %s(obj) is expected to be redirected (remaddr=%s)",
			t.si, r.Method, r.RemoteAddr)
		return
	}
	apiItems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}

	var (
		bucket  = apiItems[0]
		objName = apiItems[1]
	)

	t.headObject(w, r, query, bucket, objName)
}

// headObject is main function to head the object. It doesn't check request origin,
// so it must be done by the caller (if necessary).
func (t *targetrunner) headObject(w http.ResponseWriter, r *http.Request, query url.Values, bucket, objName string) {
	var (
		invalidHandler = t.invalmsghdlr
		hdr            = w.Header()
		checkExists    = cmn.IsParseBool(query.Get(cmn.URLParamCheckExists))
		checkExistsAny = cmn.IsParseBool(query.Get(cmn.URLParamCheckExistsAny))
		silent         = cmn.IsParseBool(query.Get(cmn.URLParamSilent))
	)
	if silent {
		invalidHandler = t.invalmsghdlrsilent
	}

	bck, err := newBckFromQuery(bucket, query)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	lom := &cluster.LOM{T: t, ObjName: objName}
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
			invalidHandler(w, r, fmt.Sprintf("%s/%s %s", bucket, objName, cmn.DoesNotExist),
				http.StatusNotFound)
		}
		return
	}
	if lom.Bck().IsAIS() || exists { // && !lom.VerConf().Enabled) {
		if !exists {
			invalidHandler(w, r, fmt.Sprintf("%s/%s %s", bucket, objName, cmn.DoesNotExist),
				http.StatusNotFound)
			return
		}
		lom.ToHTTPHdr(hdr)
	} else {
		objMeta, errCode, err := t.Cloud(lom.Bck()).HeadObj(context.Background(), lom)
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
		Bck:     lom.Bck().Bck,
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
	apiItems, err := t.checkRESTItems(w, r, 3, false, cmn.Version, cmn.EC)
	if err != nil {
		return
	}

	if apiItems[0] == ec.URLMeta {
		t.sendECMetafile(w, r, apiItems[1:])
	} else if apiItems[0] == ec.URLCT {
		t.sendECCT(w, r, apiItems[1:])
	} else {
		t.invalmsghdlrf(w, r, "invalid EC URL path %s", apiItems[0])
	}
}

// Returns a CT's metadata.
func (t *targetrunner) sendECMetafile(w http.ResponseWriter, r *http.Request, apiItems []string) {
	bucket, objName := apiItems[0], apiItems[1]
	bck, err := newBckFromQuery(bucket, r.URL.Query())
	if err != nil {
		t.invalmsghdlrsilent(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	if err = bck.Init(t.owner.bmd, t.si); err != nil {
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

func (t *targetrunner) sendECCT(w http.ResponseWriter, r *http.Request, apiItems []string) {
	config := cmn.GCO.Get()
	bucket, objName := apiItems[0], apiItems[1]
	bck, err := newBckFromQuery(bucket, r.URL.Query())
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	lom := &cluster.LOM{T: t, ObjName: objName}
	if err = lom.Init(bck.Bck, config); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); ok {
			t.BMDVersionFixup(r, cmn.Bck{}, true /* sleep */)
			err = lom.Init(bck.Bck, config)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	}
	sliceFQN := lom.ParsedFQN.MpathInfo.MakePathFQN(bck.Bck, ec.SliceType, objName)
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

// checkCloudVersion returns (vchanged=) true if object versions differ between Cloud and local cache;
// should be called only if the local copy exists
func (t *targetrunner) CheckCloudVersion(ctx context.Context, lom *cluster.LOM) (vchanged bool, errCode int, err error) {
	var objMeta cmn.SimpleKVs
	objMeta, errCode, err = t.Cloud(lom.Bck()).HeadObj(ctx, lom)
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
	// 3rd party cloud or remote ais
	if _, ok := cfg.Cloud.Providers[query.Provider]; ok || query.IsRemoteAIS() {
		bck := cluster.NewBck("", query.Provider, query.Ns)
		names, errCode, err = t.Cloud(bck).ListBuckets(context.Background(), query)
		sort.Sort(names)
	} else { // BMD
		names = t.selectBMDBuckets(t.owner.bmd.get(), query)
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
		workFQN:    fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfilePut),
	}
	if recvType != "" {
		n, err := strconv.Atoi(recvType)
		if err != nil {
			return http.StatusBadRequest, err
		}
		poi.migrated = cluster.RecvType(n) == cluster.Migrated
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
		nmp      = fs.NumAvail()
	)
	if !mirrConf.Enabled {
		return
	}
	if nmp < 2 {
		glog.Errorf("%s: insufficient mountpaths (%d)", lom, nmp)
		return
	}
	for i := 0; i < retries; i++ {
		xputlrep := xreg.RenewPutMirror(lom)
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
		if errCode, err := t.Cloud(lom.Bck()).DeleteObj(ctx, lom); err != nil {
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
	apiItems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objNameFrom := apiItems[0], apiItems[1]
	bck, err := newBckFromQuery(bucket, r.URL.Query())
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	lom := &cluster.LOM{T: t, ObjName: objNameFrom}
	if err = lom.Init(bck.Bck); err != nil {
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
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket := apiItems[0]

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
			// Nothing to promote, return immediately.
			return
		}
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	bck, err := newBckFromQuery(bucket, r.URL.Query())
	if err != nil {
		t.invalmsghdlr(w, r, err.Error(), http.StatusBadRequest)
		return
	}
	if err = bck.Init(t.owner.bmd, t.si); err != nil {
		if _, ok := err.(*cmn.ErrorRemoteBucketDoesNotExist); ok {
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
		if promoteArgs.Verbose {
			glog.Infof("%s: promote %+v", t.si, promoteArgs)
		}
		xact, err := xreg.RenewDirPromote(t, bck, srcFQN, &promoteArgs)
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
		Bck:       bck,
		ObjName:   objName,
		Overwrite: promoteArgs.Overwrite,
		KeepOrig:  promoteArgs.KeepOrig,
		Verbose:   promoteArgs.Verbose,
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
		t.bcastToIC(msg, false /*wait*/)
	}
	t.rebManager.RunResilver(id, skipGlobMisplaced, notifs...)
}
