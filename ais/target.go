// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/ais/backend"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
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
	"github.com/NVIDIA/aistore/xreg"
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
	backends map[string]cluster.BackendProvider
	// main
	targetrunner struct {
		httprunner
		backend      backends
		fshc         *health.FSHC
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
var _ cos.Runner = (*targetrunner)(nil)

//////////////
// backends //
//////////////

func (b backends) init(t *targetrunner, starting bool) {
	backend.Init()

	ais := backend.NewAIS(t)
	b[cmn.ProviderAIS] = ais // ais cloud is always present

	config := cmn.GCO.Get()
	if aisConf, ok := config.Backend.ProviderConf(cmn.ProviderAIS); ok {
		if err := ais.Apply(aisConf, "init"); err != nil {
			glog.Errorf("%s: %v - proceeding to start anyway...", t.si, err)
		}
	}

	b[cmn.ProviderHTTP], _ = backend.NewHTTP(t, config)
	if err := b.initExt(t, starting); err != nil {
		cos.ExitLogf("%v", err)
	}
}

// 3rd part cloud: empty stubs unless populated via build tags
// NOTE: write access to backends - other than target startup is also invoked
//       via primary startup (see earlystart for "choosing remote backends")
func (b backends) initExt(t *targetrunner, starting bool) (err error) {
	config := cmn.GCO.Get()
	for provider := range b {
		if provider == cmn.ProviderHTTP || provider == cmn.ProviderAIS { // always present
			continue
		}
		if _, ok := config.Backend.Providers[provider]; !ok {
			if !starting {
				glog.Errorf("Warning: %s: delete %q backend", t.si, provider)
			}
			delete(b, provider)
		}
	}
	for provider := range config.Backend.Providers {
		var add string
		switch provider {
		case cmn.ProviderAmazon:
			if _, ok := b[provider]; !ok {
				b[provider], err = backend.NewAWS(t)
				add = provider
			}
		case cmn.ProviderAzure:
			if _, ok := b[provider]; !ok {
				b[provider], err = backend.NewAzure(t)
				add = provider
			}
		case cmn.ProviderGoogle:
			if _, ok := b[provider]; !ok {
				b[provider], err = backend.NewGCP(t)
				add = provider
			}
		case cmn.ProviderHDFS:
			if _, ok := b[provider]; !ok {
				b[provider], err = backend.NewHDFS(t)
				add = provider
			}
		default:
			err = fmt.Errorf(cmn.FmtErrUnknown, t.si, "backend provider", provider)
		}
		if err != nil {
			err = _overback(err)
		}
		if err != nil {
			return
		}
		if add != "" && !starting {
			glog.Errorf("Warning: %s: add %q backend", t.si, add)
		}
	}
	return
}

func _overback(err error) error {
	if _, ok := err.(*cmn.ErrInitBackend); !ok {
		return err
	}
	if !daemon.cli.overrideBackends {
		return fmt.Errorf("%v - consider using '-override_backends' command-line", err)
	}
	glog.Warningf("%v - overriding, proceeding anyway", err)
	return nil
}

///////////////////
// target runner //
///////////////////

func (t *targetrunner) init(config *cmn.Config) {
	t.initNetworks()
	t.si.Init(initTID(config), cmn.Target)

	cos.InitShortID(t.si.Digest())

	t.initFs()

	t.initHostIP()
	daemon.rg.add(t)

	ts := &stats.Trunner{T: t} // iostat below
	startedUp := ts.Init(t)
	daemon.rg.add(ts)
	t.statsT = ts

	k := newTargetKeepalive(t, ts, startedUp)
	daemon.rg.add(k)
	t.keepalive = k

	t.fsprg.init(t) // subgroup of the daemon.rg rungroup

	// Stream Collector - a singleton object with responsibilities that include:
	sc := transport.Init()
	daemon.rg.add(sc)

	fshc := health.NewFSHC(t)
	daemon.rg.add(fshc)
	t.fshc = fshc

	if err := ts.InitCapacity(); err != nil { // goes after fs.Init
		cos.ExitLogf("%s", err)
	}
}

func initTID(config *cmn.Config) (tid string) {
	var err error
	if tid = envDaemonID(cmn.Target); tid != "" {
		return
	}
	if tid, err = fs.LoadDaemonID(config.FSpaths.Paths); err != nil {
		cos.ExitLogf("%v", err)
	}
	if tid != "" {
		return
	}
	tid = generateDaemonID(cmn.Target, config)
	cos.Assert(tid != "")
	glog.Infof("t[%s] ID randomly generated", tid)
	return
}

func (t *targetrunner) initFs() {
	// Initialize filesystem/mountpaths manager.
	fs.Init()

	// fs.Mountpaths must be inited prior to all runners that utilize them.
	// For mountpath definition, see `fs/mountfs.go`.
	config := cmn.GCO.Get()
	if config.TestingEnv() {
		glog.Warningf("Using %d mpaths for testing with disabled FsID check", config.TestFSP.Count)
		fs.DisableFsIDCheck()
	}

	if changed, err := fs.InitMpaths(t.si.ID()); err != nil {
		cos.ExitLogf("%v", err)
	} else if changed {
		daemon.resilver.required = true
		daemon.resilver.reason = "mountpaths differ from last run"
	}
}

func regDiskMetrics(tstats *stats.Trunner, mpi fs.MPI) {
	for _, mi := range mpi {
		for _, disk := range mi.Disks {
			tstats.RegDiskMetrics(disk)
		}
	}
}

func (t *targetrunner) Run() error {
	if err := t.si.Validate(); err != nil {
		cos.ExitLogf("%v", err)
	}
	config := cmn.GCO.Get()
	t.httprunner.init(config)

	cluster.Init(t)

	// metrics, disks first
	tstats := t.statsT.(*stats.Trunner)
	availablePaths, disabledPaths := fs.Get()
	if len(availablePaths) == 0 {
		cos.ExitLogf("%v", fs.ErrNoMountpaths)
	}
	regDiskMetrics(tstats, availablePaths)
	regDiskMetrics(tstats, disabledPaths)
	t.statsT.RegMetrics(t.si) // + Prometheus, if configured

	fatalErr, writeErr := t.checkRestarted()
	if fatalErr != nil {
		cos.ExitLogf("%v", fatalErr)
	}
	if writeErr != nil {
		glog.Errorln("")
		glog.Error(writeErr)
		glog.Errorln("")
	}

	// register object type and workfile type
	if err := fs.CSM.RegisterContentType(fs.ObjectType, &fs.ObjectContentResolver{}); err != nil {
		cos.ExitLogf("%v", err)
	}
	if err := fs.CSM.RegisterContentType(fs.WorkfileType, &fs.WorkfileContentResolver{}); err != nil {
		cos.ExitLogf("%v", err)
	}

	// Init meta-owners and load local instances
	t.owner.bmd.init()

	smap, reliable := t.tryLoadSmap()
	if !reliable {
		smap = newSmap()
	}

	// Add self to cluster map
	smap.Tmap[t.si.ID()] = t.si
	t.owner.smap.put(smap)

	// Join cluster
	debug.Assert(!t.NodeStarted())
	if status, err := t.joinCluster(); err != nil {
		glog.Errorf("%s failed to join cluster (status: %d, err: %v)", t.si, status, err)
		glog.Errorf("%s is terminating", t.si)
		return err
	}

	t.markNodeStarted()

	go func() {
		smap := t.owner.smap.get()
		cii := t.pollClusterStarted(config, smap.Primary)
		if daemon.stopping.Load() {
			return
		}
		if cii != nil {
			if status, err := t.joinCluster(cii.Smap.Primary.CtrlURL, cii.Smap.Primary.PubURL); err != nil {
				glog.Errorf("%s failed to re-join cluster (status: %d, err: %v)", t.si, status, err)
				return
			}
		}
		t.markClusterStarted()
	}()

	t.backend.init(t, true /*starting*/)

	driver, err := dbdriver.NewBuntDB(filepath.Join(config.ConfigDir, dbName))
	if err != nil {
		glog.Errorf("Failed to initialize DB: %v", err)
		return err
	}
	t.dbDriver = driver
	defer cos.Close(driver)

	// transactions
	t.transactions.init(t)

	t.rebManager = reb.NewManager(t, config, t.statsT)

	// register storage target's handler(s) and start listening
	t.initRecvHandlers()

	ec.Init(t)

	marked := xreg.GetResilverMarked()
	if marked.Interrupted || daemon.resilver.required {
		go func() {
			if marked.Interrupted {
				glog.Info("Resuming resilver...")
			} else if daemon.resilver.required {
				glog.Infof("Starting resilver, reason: %s", daemon.resilver.reason)
			}
			t.runResilver("" /*uuid*/, nil /*wg*/, false /*skipGlobMisplaced*/)
		}()
	}

	dsort.InitManagers(driver)
	dsort.RegisterNode(t.owner.smap, t.owner.bmd, t.si, t, t.statsT)

	defer etl.StopAll(t) // Always try to stop running ETLs.

	err = t.httprunner.run()

	// do it after the `run()` to retain `restarted` marker on panic
	fs.RemoveMarker(cmn.NodeRestartedMarker)
	return err
}

func (t *targetrunner) initRecvHandlers() {
	networkHandlers := []networkHandler{
		{r: cmn.Buckets, h: t.bucketHandler, net: accessNetAll},
		{r: cmn.Objects, h: t.objectHandler, net: accessNetAll},
		{r: cmn.Daemon, h: t.daemonHandler, net: accessNetPublicControl},
		{r: cmn.Metasync, h: t.metasyncHandler, net: accessNetIntraControl},
		{r: cmn.Health, h: t.healthHandler, net: accessNetPublicControl},
		{r: cmn.Xactions, h: t.xactHandler, net: accessNetIntraControl},
		{r: cmn.EC, h: t.ecHandler, net: accessNetIntraData},
		{r: cmn.Vote, h: t.voteHandler, net: accessNetIntraControl},
		{r: cmn.Txn, h: t.txnHandler, net: accessNetIntraControl},
		{r: cmn.ObjStream, h: transport.RxAnyStream, net: accessControlData},

		{r: cmn.Download, h: t.downloadHandler, net: accessNetIntraControl},
		{r: cmn.Sort, h: dsort.SortHandler, net: accessControlData},
		{r: cmn.ETL, h: t.etlHandler, net: accessNetAll},
		{r: cmn.Query, h: t.queryHandler, net: accessNetPublicControl},

		{r: "/" + cmn.S3, h: t.s3Handler, net: accessNetPublicData},
		{r: "/", h: t.writeErrURL, net: accessNetAll},
	}
	t.registerNetworkHandlers(networkHandlers)
}

// stop gracefully
func (t *targetrunner) Stop(err error) {
	// NOTE: vs metasync
	t.regstate.Lock()
	daemon.stopping.Store(true)
	t.regstate.Unlock()

	f := glog.Infof
	if err != nil {
		f = glog.Warningf
	}
	f("Stopping %s, err: %v", t.si, err)
	xreg.AbortAll()
	t.httprunner.stop(t.netServ.pub.s != nil && !isErrNoUnregister(err) /*rm from Smap*/)
}

func (t *targetrunner) checkRestarted() (fatalErr, writeErr error) {
	if fs.MarkerExists(cmn.NodeRestartedMarker) {
		t.statsT.Add(stats.RestartCount, 1)
	} else {
		fatalErr, writeErr = fs.PersistMarker(cmn.NodeRestartedMarker)
	}
	return
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
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodHead, http.MethodPost)
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
	case http.MethodPatch:
		t.httpobjpatch(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodHead,
			http.MethodPost, http.MethodPut)
	}
}

// verb /v1/slices
// Non-public inerface
func (t *targetrunner) ecHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t.httpecget(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodGet)
	}
}

///////////////////////
// httpbck* handlers //
///////////////////////

// GET /v1/buckets[/bucket-name]
func (t *targetrunner) httpbckget(w http.ResponseWriter, r *http.Request) {
	var (
		bckName   string
		queryBcks cmn.QueryBcks
	)
	apiItems, err := t.checkRESTItems(w, r, 0, true, cmn.URLPathBuckets.L)
	if err != nil {
		return
	}
	msg := &aisMsg{}
	if err := cmn.ReadJSON(w, r, &msg); err != nil {
		return
	}
	if len(apiItems) > 0 {
		bckName = apiItems[0]
	}
	if queryBcks, err = newQueryBcksFromQuery(bckName, r.URL.Query()); err != nil {
		t.writeErr(w, r, err)
		return
	}
	t.ensureLatestBMD(msg, r)
	switch msg.Action {
	case cmn.ActList:
		t.handleList(w, r, queryBcks, msg)
	case cmn.ActSummary:
		t.handleSummary(w, r, queryBcks, msg)
	default:
		t.writeErrAct(w, r, msg.Action)
	}
}

func (t *targetrunner) handleList(w http.ResponseWriter, r *http.Request, queryBcks cmn.QueryBcks, msg *aisMsg) {
	if queryBcks.Name == "" {
		t.listBuckets(w, r, queryBcks)
	} else {
		bck := cluster.NewBckEmbed(cmn.Bck(queryBcks))
		if err := bck.Init(t.owner.bmd); err != nil {
			if cmn.IsErrRemoteBckNotFound(err) {
				t.BMDVersionFixup(r)
				err = bck.Init(t.owner.bmd)
			}
			if err != nil {
				t.writeErr(w, r, err)
				return
			}
		}
		begin := mono.NanoTime()
		if ok := t.listObjects(w, r, bck, msg); !ok {
			return
		}

		delta := mono.SinceNano(begin)
		t.statsT.AddMany(
			stats.NamedVal64{Name: stats.ListCount, Value: 1},
			stats.NamedVal64{Name: stats.ListLatency, Value: delta},
		)
	}
}

func (t *targetrunner) handleSummary(w http.ResponseWriter, r *http.Request, queryBcks cmn.QueryBcks, msg *aisMsg) {
	bck := cluster.NewBckEmbed(cmn.Bck(queryBcks))
	if bck.Name != "" {
		// Ensure that the bucket exists.
		if err := bck.Init(t.owner.bmd); err != nil {
			if cmn.IsErrRemoteBckNotFound(err) {
				t.BMDVersionFixup(r)
				err = bck.Init(t.owner.bmd)
			}
			if err != nil {
				t.writeErr(w, r, err)
				return
			}
		}
	}

	t.bucketSummary(w, r, bck, msg)
}

// DELETE { action } /v1/buckets/bucket-name
// (evict | delete) (list | range)
func (t *targetrunner) httpbckdelete(w http.ResponseWriter, r *http.Request) {
	msg := aisMsg{}
	if err := cmn.ReadJSON(w, r, &msg, true); err != nil {
		return
	}
	request := &apiRequest{after: 1, prefix: cmn.URLPathBuckets.L}
	if err := t.parseAPIRequest(w, r, request); err != nil {
		return
	}
	if err := request.bck.Init(t.owner.bmd); err != nil {
		if cmn.IsErrRemoteBckNotFound(err) {
			t.BMDVersionFixup(r)
			err = request.bck.Init(t.owner.bmd)
		}
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
	}

	switch msg.Action {
	case cmn.ActEvictRemoteBck:
		keepMD := cos.IsParseBool(r.URL.Query().Get(cmn.URLParamKeepBckMD))
		// HDFS buckets will always keep metadata so they can re-register later
		if request.bck.IsHDFS() || keepMD {
			nlp := request.bck.GetNameLockPair()
			nlp.Lock()
			defer nlp.Unlock()

			err := fs.DestroyBucket(msg.Action, request.bck.Bck, request.bck.Props.BID)
			if err != nil {
				t.writeErr(w, r, err)
				return
			}
			// Recreate bucket directories (now empty), since bck is still in BMD
			errs := fs.CreateBucket(msg.Action, request.bck.Bck, false /*nilbmd*/)
			if len(errs) > 0 {
				debug.AssertNoErr(errs[0])
				t.writeErr(w, r, errs[0]) // only 1 err is possible for 1 bck
			}
		}
	case cmn.ActDeleteObjects, cmn.ActEvictObjects:
		lrMsg := &cmn.ListRangeMsg{}
		if err := cos.MorphMarshal(msg.Value, lrMsg); err != nil {
			t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, msg.Value, err)
			return
		}
		rns := xreg.RenewEvictDelete(msg.UUID, t, msg.Action /*xaction kind*/, request.bck, lrMsg)
		if rns.Err != nil {
			t.writeErr(w, r, rns.Err)
			return
		}
		xact := rns.Entry.Get()
		xact.AddNotif(&xaction.NotifXact{
			NotifBase: nl.NotifBase{
				When: cluster.UponTerm,
				Dsts: []string{equalIC},
				F:    t.callerNotifyFin,
			},
			Xact: xact,
		})
		go xact.Run(nil)
	default:
		t.writeErrAct(w, r, msg.Action)
	}
}

// POST /v1/buckets/bucket-name
func (t *targetrunner) httpbckpost(w http.ResponseWriter, r *http.Request) {
	msg := &aisMsg{}
	if err := cmn.ReadJSON(w, r, msg); err != nil {
		return
	}
	request := &apiRequest{prefix: cmn.URLPathBuckets.L, after: 1}
	if err := t.parseAPIRequest(w, r, request); err != nil {
		return
	}

	t.ensureLatestBMD(msg, r)

	if err := request.bck.Init(t.owner.bmd); err != nil {
		if cmn.IsErrRemoteBckNotFound(err) {
			t.BMDVersionFixup(r)
			err = request.bck.Init(t.owner.bmd)
		}
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
	}

	switch msg.Action {
	case cmn.ActPrefetchObjects:
		var (
			err   error
			lrMsg = &cmn.ListRangeMsg{}
		)
		if !request.bck.IsRemote() {
			t.writeErrf(w, r, "%s: expecting remote bucket, got %s, action=%s",
				t.si, request.bck, msg.Action)
			return
		}
		if err = cos.MorphMarshal(msg.Value, lrMsg); err != nil {
			t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, msg.Value, err)
			return
		}
		rns := xreg.RenewPrefetch(msg.UUID, t, request.bck, lrMsg)
		xact := rns.Entry.Get()
		go xact.Run(nil)
	default:
		t.writeErrAct(w, r, msg.Action)
	}
}

// HEAD /v1/buckets/bucket-name
func (t *targetrunner) httpbckhead(w http.ResponseWriter, r *http.Request) {
	var (
		bucketProps cos.SimpleKVs
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
		if !cmn.IsErrRemoteBckNotFound(err) { // is ais
			t.writeErr(w, r, err)
			return
		}
		inBMD = false
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, request.bck, pid)
	}

	cos.Assert(!request.bck.IsAIS())

	if request.bck.IsHTTP() {
		originalURL := query.Get(cmn.URLParamOrigURL)
		ctx = context.WithValue(ctx, cmn.CtxOriginalURL, originalURL)
		if !inBMD && originalURL == "" {
			err = cmn.NewErrRemoteBckNotFound(request.bck.Bck)
			t.writeErrSilent(w, r, err, http.StatusNotFound)
			return
		}
	}
	// + cloud
	bucketProps, code, err = t.Backend(request.bck).HeadBucket(ctx, request.bck)
	if err != nil {
		if !inBMD {
			if code == http.StatusNotFound {
				err = cmn.NewErrRemoteBckNotFound(request.bck.Bck)
				t.writeErrSilent(w, r, err, code)
			} else {
				err = fmt.Errorf("failed to locate bucket %q, err: %v", request.bck, err)
				t.writeErr(w, r, err, code)
			}
			return
		}
		glog.Warningf("%s: bucket %s, err: %v(%d)", t.si, request.bck, err, code)
		bucketProps = make(cos.SimpleKVs)
		bucketProps[cmn.HdrBackendProvider] = request.bck.Provider
		bucketProps[cmn.HdrRemoteOffline] = strconv.FormatBool(request.bck.IsRemote())
	}
	for k, v := range bucketProps {
		if k == cmn.HdrBucketVerEnabled && request.bck.Props != nil {
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

	if !t.isIntraCall(r.Header) && ptime == "" && !features.IsSet(cmn.FeatureDirectAccess) {
		t.writeErrf(w, r, "%s: %s(obj) is expected to be redirected (remaddr=%s)",
			t.si, r.Method, r.RemoteAddr)
		return
	}
	if err := t.parseAPIRequest(w, r, request); err != nil {
		return
	}

	lom := cluster.AllocLOM(request.items[1])
	t.getObject(w, r, query, request.bck, lom)
	cluster.FreeLOM(lom)
}

// getObject is main function to get the object. It doesn't check request origin,
// so it must be done by the caller (if necessary).
func (t *targetrunner) getObject(w http.ResponseWriter, r *http.Request, query url.Values, bck *cluster.Bck, lom *cluster.LOM) {
	var (
		ptime   = isRedirect(query)
		config  = cmn.GCO.Get()
		started = time.Now()
		nanotim = mono.NanoTime()
	)
	if nanotim&0x5 == 5 {
		if redelta := ptLatency(time.Now(), ptime); redelta != 0 {
			t.statsT.Add(stats.GetRedirLatency, redelta)
		}
	}
	if err := lom.Init(bck.Bck); err != nil {
		if cmn.IsErrRemoteBckNotFound(err) {
			t.BMDVersionFixup(r)
			err = lom.Init(bck.Bck)
		}
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
	}
	if isETLRequest(query) {
		t.doETL(w, r, query.Get(cmn.URLParamUUID), bck, lom.ObjName)
		return
	}
	filename := query.Get(cmn.URLParamArchpath)
	if strings.HasPrefix(filename, lom.ObjName) {
		if rel, err := filepath.Rel(lom.ObjName, filename); err == nil {
			filename = rel
		}
	}
	goi := allocGetObjInfo()
	{
		goi.started = started
		goi.nanotim = nanotim
		goi.t = t
		goi.lom = lom
		goi.w = w
		goi.ctx = context.Background()
		goi.ranges = rangesQuery{Range: r.Header.Get(cmn.HdrRange), Size: 0}
		goi.archive = archiveQuery{
			filename: filename,
			mime:     query.Get(cmn.URLParamArchmime),
		}
		goi.isGFN = cos.IsParseBool(query.Get(cmn.URLParamIsGFNRequest))
		goi.chunked = config.Net.HTTP.Chunked
	}
	if bck.IsHTTP() {
		originalURL := query.Get(cmn.URLParamOrigURL)
		goi.ctx = context.WithValue(goi.ctx, cmn.CtxOriginalURL, originalURL)
	}
	if errCode, err := goi.getObject(); err != nil && err != errSendingResp {
		t.writeErr(w, r, err, errCode)
	}
	freeGetObjInfo(goi)
}

// PUT /v1/objects/bucket-name/object-name
func (t *targetrunner) httpobjput(w http.ResponseWriter, r *http.Request) {
	var (
		query   = r.URL.Query()
		ptime   string
		request = &apiRequest{after: 2, prefix: cmn.URLPathObjects.L}
	)

	if ptime = isRedirect(query); ptime == "" && !isIntraPut(r.Header) {
		t.writeErrf(w, r, "%s: %s(obj) is expected to be redirected or replicated", t.si, r.Method)
		return
	}
	if err := t.parseAPIRequest(w, r, request); err != nil {
		return
	}
	objName := request.items[1]

	started := time.Now()
	if ptime != "" {
		if redelta := ptLatency(started, ptime); redelta != 0 {
			t.statsT.Add(stats.PutRedirLatency, redelta)
		}
	}
	if cs := fs.GetCapStatus(); cs.Err != nil {
		go t.RunLRU("" /*uuid*/, nil /*wg*/, false)
		if cs.OOS {
			t.writeErr(w, r, cs.Err)
			return
		}
	}
	lom := cluster.AllocLOM(objName)
	defer cluster.FreeLOM(lom)

	if err := lom.Init(request.bck.Bck); err != nil {
		if cmn.IsErrRemoteBckNotFound(err) {
			t.BMDVersionFixup(r)
			err = lom.Init(request.bck.Bck)
		}
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
	}
	if lom.Load(true /*cache it*/, false /*locked*/) == nil { // if exists, check custom md
		srcProvider, hasSrc := lom.GetCustomKey(cmn.SourceObjMD)
		if hasSrc && srcProvider != cmn.WebObjMD {
			bck := lom.Bck()
			if bck.IsAIS() {
				t.writeErrf(w, r,
					"bucket %s: cannot override %s-downloaded object", bck, srcProvider)
				return
			}
			if b := bck.RemoteBck(); b != nil && b.Provider != srcProvider {
				t.writeErrf(w, r,
					"bucket %s: cannot override %s-downloaded object", b, srcProvider)
				return
			}
		}
	}
	lom.SetAtimeUnix(started.UnixNano())

	var (
		handle  string
		err     error
		errCode int

		archPathProvided = query.Has(cmn.URLParamArchpath)
		appendTyProvided = query.Has(cmn.URLParamAppendType)
	)
	if archPathProvided {
		errCode, err = t.doAppendArch(r, lom, started)
	} else if appendTyProvided {
		handle, errCode, err = t.doAppend(r, lom, started)
		if err == nil {
			w.Header().Set(cmn.HdrAppendHandle, handle)
			return
		}
	} else {
		errCode, err = t.doPut(r, lom, started)
	}
	if err != nil {
		t.fsErr(err, lom.FQN)
		t.writeErr(w, r, err, errCode)
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
		return
	}
	if isRedirect(query) == "" {
		t.writeErrf(w, r, "%s: %s(obj) is expected to be redirected", t.si, r.Method)
		return
	}
	if err := t.parseAPIRequest(w, r, request); err != nil {
		return
	}

	evict = msg.Action == cmn.ActEvictObjects
	lom := cluster.AllocLOM(request.items[1])
	defer cluster.FreeLOM(lom)
	if err := lom.Init(request.bck.Bck); err != nil {
		t.writeErr(w, r, err)
		return
	}

	errCode, err := t.DeleteObject(lom, evict)
	if err != nil {
		if errCode == http.StatusNotFound {
			t.writeErrSilentf(w, r, http.StatusNotFound, "object %s/%s doesn't exist",
				lom.Bucket(), lom.ObjName)
		} else {
			t.writeErr(w, r, err, errCode)
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
			t.writeErrf(w, r, "%s: %s-%s(obj) is expected to be redirected", t.si, r.Method, msg.Action)
			return
		}
		t.objMv(w, r, &msg)
	case cmn.ActPromote:
		if isRedirect(query) == "" && !t.isIntraCall(r.Header) {
			t.writeErrf(w, r, "%s: %s-%s(obj) is expected to be redirected or intra-called",
				t.si, r.Method, msg.Action)
			return
		}
		t.promoteFQN(w, r, &msg)
	default:
		t.writeErrAct(w, r, msg.Action)
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
	if isRedirect(query) == "" && !t.isIntraCall(r.Header) && !features.IsSet(cmn.FeatureDirectAccess) {
		t.writeErrf(w, r, "%s: %s(obj) is expected to be redirected (remaddr=%s)",
			t.si, r.Method, r.RemoteAddr)
		return
	}
	if err := t.parseAPIRequest(w, r, request); err != nil {
		return
	}
	lom := cluster.AllocLOM(request.items[1] /*objName*/)
	t.headObject(w, r, query, request.bck, lom)
	cluster.FreeLOM(lom)
}

// headObject is main function to head the object. It doesn't check request origin,
// so it must be done by the caller (if necessary).
func (t *targetrunner) headObject(w http.ResponseWriter, r *http.Request, query url.Values, bck *cluster.Bck, lom *cluster.LOM) {
	var (
		invalidHandler = t.writeErr
		hdr            = w.Header()
		checkExists    = cos.IsParseBool(query.Get(cmn.URLParamCheckExists))
		checkExistsAny = cos.IsParseBool(query.Get(cmn.URLParamCheckExistsAny))
		silent         = cos.IsParseBool(query.Get(cmn.URLParamSilent))
		exists         = true
		addedEC        bool
	)
	if silent {
		invalidHandler = t.writeErrSilent
	}
	if err := lom.Init(bck.Bck); err != nil {
		invalidHandler(w, r, err)
		return
	}
	if err := lom.Load(true /*cache it*/, false /*locked*/); err != nil {
		if !cmn.IsObjNotExist(err) {
			invalidHandler(w, r, err)
			return
		}
		exists = false
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s(exists=%t) <= %s", r.Method, lom, exists, pid)
	}

	// * `checkExists` and `checkExistsAny` can be used to:
	//    a) establish local object's presence on any of the local mountpaths, and
	//    b) if located, restore the object to its default location.
	// * `checkExistsAny` tries to perform the b) even if the object does not have copies.
	// * see also: GFN
	if !exists {
		// lookup and restore the object to its proper location
		if (checkExists && lom.HasCopies()) || checkExistsAny {
			exists = lom.RestoreObjectFromAny()
		}
	}
	if checkExists || checkExistsAny {
		if !exists {
			err := cmn.NewErrNotFound("%s: object %s", t.si, lom.FullName())
			invalidHandler(w, r, err, http.StatusNotFound)
		}
		return
	}

	// NOTE: compare with `api.HeadObject()`
	// fill-in
	op := cmn.ObjectProps{Name: lom.ObjName, Bck: lom.Bucket(), Present: exists}
	if lom.Bck().IsAIS() {
		if !exists {
			err := cmn.NewErrNotFound("%s: object %s", t.si, lom.FullName())
			invalidHandler(w, r, err, http.StatusNotFound)
			return
		}
		op.ObjAttrs = *lom.ObjAttrs()
	} else if exists {
		op.ObjAttrs = *lom.ObjAttrs()
	} else {
		// cold HEAD
		objAttrs, errCode, err := t.Backend(lom.Bck()).HeadObj(context.Background(), lom)
		if err != nil {
			err = fmt.Errorf(cmn.FmtErrFailed, t.si, "HEAD", lom, err)
			invalidHandler(w, r, err, errCode)
			return
		}
		op.ObjAttrs = *objAttrs
	}
	if exists {
		op.NumCopies = lom.NumCopies()
		if lom.Bck().Props.EC.Enabled {
			if md, err := ec.ObjectMetadata(lom.Bck(), lom.ObjName); err == nil {
				addedEC = true
				op.EC.DataSlices = md.Data
				op.EC.ParitySlices = md.Parity
				op.EC.IsECCopy = md.IsCopy
				op.EC.Generation = md.Generation
			}
		}
	}

	// to header
	op.ObjAttrs.ToHeader(hdr)
	err := cmn.IterFields(op, func(tag string, field cmn.IterField) (err error, b bool) {
		if !addedEC && strings.HasPrefix(tag, "ec-") {
			return nil, false
		}
		v := field.String()
		if v == "" {
			return nil, false
		}
		headerName := cmn.PropToHeader(tag)
		hdr.Set(headerName, v)
		return nil, false
	})
	debug.AssertNoErr(err)
}

// PATCH /v1/objects/<bucket-name>/<object-name>
// By default, adds or updates existing custom keys. Will remove all existing keys and
// replace them with the specified ones _iff_ `cmn.URLParamNewCustom` is set.
func (t *targetrunner) httpobjpatch(w http.ResponseWriter, r *http.Request) {
	var (
		msg      cmn.ActionMsg
		features = cmn.GCO.Get().Client.Features
		query    = r.URL.Query()
		request  = &apiRequest{after: 2, prefix: cmn.URLPathObjects.L}
	)
	if isRedirect(query) == "" && !t.isIntraCall(r.Header) && !features.IsSet(cmn.FeatureDirectAccess) {
		t.writeErrf(w, r, "%s: %s(obj) is expected to be redirected (remaddr=%s)",
			t.si, r.Method, r.RemoteAddr)
		return
	}
	if err := t.parseAPIRequest(w, r, request); err != nil {
		return
	}
	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}
	custom := cos.SimpleKVs{}
	if err := cos.MorphMarshal(msg.Value, &custom); err != nil {
		t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, "set-custom", msg.Value, err)
		return
	}
	lom := cluster.AllocLOM(request.items[1] /*objName*/)
	defer cluster.FreeLOM(lom)
	if err := lom.Init(request.bck.Bck); err != nil {
		t.writeErr(w, r, err)
		return
	}
	if err := lom.Load(true /*cache it*/, false /*locked*/); err != nil {
		if cmn.IsObjNotExist(err) {
			t.writeErr(w, r, err, http.StatusNotFound)
		} else {
			t.writeErr(w, r, err)
		}
		return
	}
	delOldSetNew := cos.IsParseBool(query.Get(cmn.URLParamNewCustom))
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s: %s, custom=%+v, del-old-set-new=%t", t.si, lom, msg.Value, delOldSetNew)
	}
	if delOldSetNew {
		lom.SetCustomMD(custom)
	} else {
		for key, val := range custom {
			lom.SetCustomKey(key, val)
		}
	}
	lom.Persist()
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
		t.writeErrURL(w, r)
	}
}

// Returns a CT's metadata.
func (t *targetrunner) sendECMetafile(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) {
	if err := bck.Init(t.owner.bmd); err != nil {
		if !cmn.IsErrRemoteBckNotFound(err) { // is ais
			t.writeErrSilent(w, r, err)
			return
		}
	}
	md, err := ec.ObjectMetadata(bck, objName)
	if err != nil {
		if os.IsNotExist(err) {
			t.writeErrSilent(w, r, err, http.StatusNotFound)
		} else {
			t.writeErrSilent(w, r, err, http.StatusInternalServerError)
		}
		return
	}
	w.Write(md.NewPack())
}

func (t *targetrunner) sendECCT(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) {
	lom := cluster.AllocLOM(objName)
	defer cluster.FreeLOM(lom)
	if err := lom.Init(bck.Bck); err != nil {
		if cmn.IsErrRemoteBckNotFound(err) {
			t.BMDVersionFixup(r)
			err = lom.Init(bck.Bck)
		}
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
	}
	sliceFQN := lom.MpathInfo().MakePathFQN(bck.Bck, fs.ECSliceType, objName)
	finfo, err := os.Stat(sliceFQN)
	if err != nil {
		t.writeErrSilent(w, r, err, http.StatusNotFound)
		return
	}
	file, err := os.Open(sliceFQN)
	if err != nil {
		t.fsErr(err, sliceFQN)
		t.writeErr(w, r, err, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Length", strconv.FormatInt(finfo.Size(), 10))
	_, err = io.Copy(w, file) // No need for `io.CopyBuffer` as `sendfile` syscall will be used.
	cos.Close(file)
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
func (t *targetrunner) CompareObjects(ctx context.Context, lom *cluster.LOM) (equal bool, errCode int, err error) {
	var objAttrs *cmn.ObjAttrs
	objAttrs, errCode, err = t.Backend(lom.Bck()).HeadObj(ctx, lom)
	if err != nil {
		err = fmt.Errorf(cmn.FmtErrFailed, t.si, "head metadata of", lom, err)
		return
	}
	if lom.Bck().IsHDFS() {
		equal = true // no versioning in HDFS
		return
	}
	equal = lom.Equal(objAttrs)
	return
}

func (t *targetrunner) listBuckets(w http.ResponseWriter, r *http.Request, query cmn.QueryBcks) {
	const fmterr = "failed to list %q buckets: [%v]"
	var (
		bcks   cmn.Bcks
		code   int
		err    error
		config = cmn.GCO.Get()
	)
	if query.Provider != "" {
		bcks, code, err = t._listBcks(query, config)
		if err != nil {
			t.writeErrStatusf(w, r, code, fmterr, query, err)
			return
		}
	} else /* all providers */ {
		for provider := range cmn.Providers {
			var buckets cmn.Bcks
			query.Provider = provider
			buckets, code, err = t._listBcks(query, config)
			if err != nil {
				if provider == cmn.ProviderAIS {
					t.writeErrStatusf(w, r, code, fmterr, query, err)
					return
				}
				if glog.FastV(4, glog.SmoduleAIS) {
					glog.Warningf(fmterr, query, err)
				}
			}
			bcks = append(bcks, buckets...)
		}
	}
	sort.Sort(bcks)
	t.writeJSON(w, r, bcks, listBuckets)
}

func (t *targetrunner) _listBcks(query cmn.QueryBcks, cfg *cmn.Config) (names cmn.Bcks, errCode int, err error) {
	_, ok := cfg.Backend.Providers[query.Provider]
	// HDFS doesn't support listing remote buckets (there are no remote buckets).
	if (!ok && !query.IsRemoteAIS()) || query.IsHDFS() {
		names = selectBMDBuckets(t.owner.bmd.get(), query)
	} else {
		bck := cluster.NewBck("", query.Provider, query.Ns)
		names, errCode, err = t.Backend(bck).ListBuckets(query)
		sort.Sort(names)
	}
	return
}

func (t *targetrunner) doAppend(r *http.Request, lom *cluster.LOM, started time.Time) (newHandle string, errCode int, err error) {
	var (
		cksumValue    = r.Header.Get(cmn.HdrObjCksumVal)
		cksumType     = r.Header.Get(cmn.HdrObjCksumType)
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
		aoi.cksum = cos.NewCksum(cksumType, cksumValue)
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
		header   = r.Header
		query    = r.URL.Query()
		recvType = query.Get(cmn.URLParamRecvType)
	)
	// TODO: oa.Size vs "Content-Length" vs actual, similar to checksum
	cksumToUse := lom.ObjAttrs().FromHeader(header)
	poi := allocPutObjInfo()
	{
		poi.started = started
		poi.t = t
		poi.lom = lom
		poi.r = r.Body
		poi.workFQN = fs.CSM.GenContentFQN(lom, fs.WorkfileType, fs.WorkfilePut)
		poi.cksumToUse = cksumToUse
	}
	if recvType != "" {
		n, err := strconv.Atoi(recvType)
		if err != nil {
			return http.StatusBadRequest, fmt.Errorf(cmn.FmtErrFailed, t.si, "parse", "receive type", err)
		}
		poi.recvType = cluster.RecvType(n)
	}
	sizeStr := header.Get("Content-Length")
	if sizeStr != "" {
		if size, ers := strconv.ParseInt(sizeStr, 10, 64); ers == nil {
			poi.size = size
		}
	}
	errCode, err = poi.putObject()
	freePutObjInfo(poi)
	return
}

func (t *targetrunner) doAppendArch(r *http.Request, lom *cluster.LOM, started time.Time) (errCode int, err error) {
	var (
		sizeStr  = r.Header.Get("Content-Length")
		query    = r.URL.Query()
		mime     = query.Get(cmn.URLParamArchmime)
		filename = query.Get(cmn.URLParamArchpath)
	)
	if strings.HasPrefix(filename, lom.ObjName) {
		if rel, err := filepath.Rel(lom.ObjName, filename); err == nil {
			filename = rel
		}
	}
	lom.Lock(true)
	defer lom.Unlock(true)
	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		if os.IsNotExist(err) {
			return http.StatusNotFound, err
		}
		return http.StatusInternalServerError, err
	}
	aaoi := &appendArchObjInfo{
		started:  started,
		t:        t,
		lom:      lom,
		r:        r.Body,
		filename: filename,
		mime:     mime,
	}
	if sizeStr != "" {
		if size, ers := strconv.ParseInt(sizeStr, 10, 64); ers == nil {
			aaoi.size = size
		}
	}
	if aaoi.size == 0 {
		return http.StatusBadRequest, errors.New("size is not defined")
	}
	return aaoi.appendObject()
}

func (t *targetrunner) putMirror(lom *cluster.LOM) {
	mconfig := lom.MirrorConf()
	if !mconfig.Enabled {
		return
	}
	if mpathCnt := fs.NumAvail(); mpathCnt < int(mconfig.Copies) {
		t.statsT.Add(stats.ErrPutCount, 1) // TODO: differentiate put err metrics
		nanotim := mono.NanoTime()
		if nanotim&0x7 == 7 {
			if mpathCnt == 0 {
				glog.Errorf("%s: %v", t.si, fs.ErrNoMountpaths)
			} else {
				glog.Errorf(fmtErrInsuffMpaths2, t.si, mpathCnt, lom, mconfig.Copies)
			}
		}
		return
	}
	rns := xreg.RenewPutMirror(t, lom)
	xact := rns.Entry.Get()
	xputlrep := xact.(*mirror.XactPut)
	xputlrep.Repl(lom)
}

func (t *targetrunner) DeleteObject(lom *cluster.LOM, evict bool) (int, error) {
	var (
		aisErr, backendErr         error
		aisErrCode, backendErrCode int
		delFromAIS, delFromBackend bool
	)
	lom.Lock(true)
	defer lom.Unlock(true)

	delFromBackend = lom.Bck().IsRemote() && !evict
	if err := lom.Load(false /*cache it*/, true /*locked*/); err == nil {
		delFromAIS = true
	} else if !cmn.IsObjNotExist(err) {
		return 0, err
	} else {
		aisErrCode = http.StatusNotFound
		if !delFromBackend {
			return http.StatusNotFound, err
		}
	}

	if delFromBackend {
		backendErrCode, backendErr = t.Backend(lom.Bck()).DeleteObj(lom)
		if backendErr == nil {
			t.statsT.Add(stats.DeleteCount, 1)
		}
	}
	if delFromAIS {
		size := lom.SizeBytes()
		aisErr = lom.Remove()
		if aisErr != nil {
			if !os.IsNotExist(aisErr) {
				if backendErr != nil {
					glog.Errorf("failed to delete %s from %s: %v", lom, lom.Bck(), backendErr)
				}
				return 0, aisErr
			}
		} else if evict {
			cos.Assert(lom.Bck().IsRemote())
			t.statsT.AddMany(
				stats.NamedVal64{Name: stats.LruEvictCount, Value: 1},
				stats.NamedVal64{Name: stats.LruEvictSize, Value: size},
			)
		}
	}
	if backendErr != nil {
		return backendErrCode, backendErr
	}
	return aisErrCode, aisErr
}

///////////////////
// RENAME OBJECT //
///////////////////

// TODO: unify with PromoteFile (refactor)
func (t *targetrunner) objMv(w http.ResponseWriter, r *http.Request, msg *cmn.ActionMsg) {
	request := &apiRequest{after: 2, prefix: cmn.URLPathObjects.L}
	if err := t.parseAPIRequest(w, r, request); err != nil {
		return
	}
	lom := cluster.AllocLOM(request.items[1])
	defer cluster.FreeLOM(lom)
	if err := lom.Init(request.bck.Bck); err != nil {
		t.writeErr(w, r, err)
		return
	}

	if lom.Bck().IsRemote() {
		t.writeErrf(w, r, "%s: cannot rename object %s from a remote bucket", t.si, lom)
		return
	}
	if lom.Bck().Props.EC.Enabled {
		t.writeErrf(w, r, "%s: cannot rename erasure-coded object %s", t.si, lom)
		return
	}
	if msg.Name == lom.ObjName {
		t.writeErrf(w, r, "%s: cannot rename/move object %s onto itself", t.si, lom)
		return
	}
	buf, slab := t.gmm.Alloc()
	coi := allocCopyObjInfo()
	{
		coi.CopyObjectParams = cluster.CopyObjectParams{BckTo: lom.Bck(), Buf: buf}
		coi.t = t
		coi.localOnly = false
		coi.finalize = true
	}
	_, err := coi.copyObject(lom, msg.Name /* new object name */)
	slab.Free(buf)
	freeCopyObjInfo(coi)
	if err != nil {
		t.writeErr(w, r, err)
		return
	}
	// TODO: combine copy+delete under a single write lock
	lom.Lock(true)
	if err = lom.Remove(); err != nil {
		glog.Warningf("%s: failed to delete renamed object %s (new name %s): %v", t.si, lom, msg.Name, err)
	}
	lom.Unlock(true)
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
	if err := cos.MorphMarshal(msg.Value, &promoteArgs); err != nil {
		t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, msg.Action, msg.Value, err)
		return
	}
	if promoteArgs.Target != "" && promoteArgs.Target != t.si.ID() {
		glog.Errorf("%s: unexpected target ID %s mismatch", t.si, promoteArgs.Target)
	}

	// 2. init & validate
	srcFQN := msg.Name
	if srcFQN == "" {
		t.writeErrf(w, r, fmtErr+"missing source filename", t.si, msg.Action)
		return
	}

	finfo, err := os.Stat(srcFQN)
	if err != nil {
		if os.IsNotExist(err) {
			err := cmn.NewErrNotFound("%s: file %q", t.si, srcFQN)
			t.writeErr(w, r, err, http.StatusNotFound)
			return
		}
		t.writeErr(w, r, err)
		return
	}
	if err = request.bck.Init(t.owner.bmd); err != nil {
		if cmn.IsErrRemoteBckNotFound(err) {
			t.BMDVersionFixup(r)
			err = request.bck.Init(t.owner.bmd)
		}
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
	}

	// 3a. promote dir
	if finfo.IsDir() {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s: promote %+v", t.si, promoteArgs)
		}
		rns := xreg.RenewDirPromote(t, request.bck, srcFQN, &promoteArgs)
		if rns.Err != nil {
			t.writeErr(w, r, rns.Err)
			return
		}
		xact := rns.Entry.Get()
		go xact.Run(nil)
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
		t.writeErrf(w, r, fmtErr+" %v", t.si, msg.Action, err)
	}
	// TODO: inc stats
}

// fshc wakes up FSHC and makes it to run filesystem check immediately if err != nil
func (t *targetrunner) fsErr(err error, filepath string) {
	if !cmn.GCO.Get().FSHC.Enabled {
		return
	}
	if !cos.IsIOError(err) {
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

func (t *targetrunner) runResilver(id string, wg *sync.WaitGroup, skipGlobMisplaced bool, notifs ...*xaction.NotifXact) {
	if id == "" {
		id = cos.GenUUID()
		regMsg := xactRegMsg{UUID: id, Kind: cmn.ActResilver, Srcs: []string{t.si.ID()}}
		msg := t.newAmsgActVal(cmn.ActRegGlobalXaction, regMsg)
		t.bcastAsyncIC(msg)
	}
	if wg != nil {
		wg.Done() // compare w/ xaction.GoRunW(()
	}
	t.rebManager.RunResilver(id, skipGlobMisplaced, notifs...)
}
