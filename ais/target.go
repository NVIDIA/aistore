// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"crypto/md5"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/filter"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/lru"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/mirror"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/stats/statsd"
	"github.com/NVIDIA/aistore/transport"
	"github.com/OneOfOne/xxhash"
	jsoniter "github.com/json-iterator/go"
)

const (
	maxPageSize     = 64 * 1024     // max number of objects in a page (warn when req. size exceeds this limit)
	maxBytesInMem   = 256 * cmn.KiB // objects with smaller size than this will be read to memory when checksumming
	maxBMDXattrSize = 128 * 1024

	// GET-from-neighbors tunables
	getFromNeighRetries   = 10
	getFromNeighSleep     = 300 * time.Millisecond
	getFromNeighAfterJoin = time.Second * 30

	bucketMDFixup    = "fixup"
	bucketMDReceive  = "receive"
	bucketMDRegister = "register"
)

type (
	uxprocess struct {
		starttime time.Time
		spid      string
		pid       int64
	}
	renamectx struct {
		bucketFrom string
		bucketTo   string
		t          *targetrunner
		pid        string
	}
	regstate struct {
		sync.Mutex
		disabled bool // target was unregistered by internal event (e.g, all mountpaths are down)
	}
	recvObjInfo struct {
		started time.Time // started time of receiving - used to calculate the recv duration
		t       *targetrunner
		lom     *cluster.LOM
		// Reader with the content of the object.
		r io.ReadCloser
		// Checksum which needs to be checked on receive. It is only checked
		// on specific occasions: see `writeToFile` method.
		cksumToCheck cmn.Cksummer
		// Context used when receiving object which is contained in cloud bucket.
		// It usually contains credentials to access the cloud.
		ctx context.Context
		// FQN which is used only temporarily for receiving file. After
		// successful receive is renamed to actual FQN.
		workFQN string
		// Determines if the object was already in cluster and was received
		// because some kind of migration.
		migrated bool
		// Determines if the recv is cold recv: either from another cluster or cloud.
		cold bool
	}

	// The state that may influence GET logic when mountpath is added/enabled
	localGFN struct {
		lookup atomic.Bool
	}

	// The state that may influence GET logic when new target joins cluster
	globalGFN struct {
		lookup       atomic.Bool
		stopDeadline atomic.Int64   // (reg-time + const) when we stop trying to GET from neighbors
		smap         atomic.Pointer // new smap which will be soon live
	}

	capUsed struct {
		sync.RWMutex
		used int32
		oos  bool
	}

	targetrunner struct {
		httprunner
		cloudif        cloudif // multi-cloud backend
		uxprocess      *uxprocess
		prefetchQueue  chan filesWithDeadline
		authn          *authManager
		clusterStarted atomic.Bool
		fsprg          fsprungroup
		readahead      readaheader
		xputlrep       *mirror.XactPutLRepl
		ecmanager      *ecManager
		rebManager     *rebManager
		capUsed        capUsed
		gfn            struct {
			local  localGFN
			global globalGFN
		}
		regstate regstate // the state of being registered with the primary (can be en/disabled via API)
	}
)

func (gfn *localGFN) active() bool {
	return gfn.lookup.Load()
}

func (gfn *localGFN) activate() {
	gfn.lookup.Store(true)
	glog.Infof("global GFN has been activated")
}

func (gfn *localGFN) deactivate() {
	gfn.lookup.Store(false)
	glog.Infof("local GFN has been deactivated")
}

func (gfn *globalGFN) active() (bool, *smapX) {
	if !gfn.lookup.Load() {
		return false, nil
	}

	// Deadline exceeded - probably primary proxy notified about new smap
	// but did not update it due to some failures.
	if time.Now().UnixNano() > gfn.stopDeadline.Load() {
		gfn.deactivate()
		return false, nil
	}

	return true, (*smapX)(gfn.smap.Load())
}

func (gfn *globalGFN) activate(smap *smapX) {
	gfn.smap.Store(unsafe.Pointer(smap))
	gfn.stopDeadline.Store(time.Now().UnixNano() + getFromNeighAfterJoin.Nanoseconds())
	gfn.lookup.Store(true)
	glog.Infof("global GFN has been activated")
}

func (gfn *globalGFN) deactivate() {
	gfn.lookup.Store(false)
	glog.Infof("global GFN has been deactivated")
}

//
// target runner
//
func (t *targetrunner) Run() error {
	config := cmn.GCO.Get()

	var ereg error
	t.httprunner.init(getstorstatsrunner())
	t.registerStats()
	t.httprunner.keepalive = gettargetkeepalive()

	dryinit()

	bmd := newBucketMD()
	t.bmdowner.put(bmd)

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

	if err := fs.Mountpaths.CreateBucketDir(cmn.LocalBs); err != nil {
		cmn.ExitLogf("%v", err)
	}
	if err := fs.Mountpaths.CreateBucketDir(cmn.CloudBs); err != nil {
		cmn.ExitLogf("%v", err)
	}
	t.detectMpathChanges()

	// cloud provider (empty stubs that may get populated via build tags)
	if config.CloudProvider == cmn.ProviderAmazon {
		t.cloudif = newAWSProvider(t)
	} else if config.CloudProvider == cmn.ProviderGoogle {
		t.cloudif = newGCPProvider(t)
	} else {
		t.cloudif = newEmptyCloud() // mock
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

		{r: "/", h: cmn.InvalidHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl, cmn.NetworkIntraData}},
	}
	t.registerNetworkHandlers(networkHandlers)

	// rebalance Rx endpoints
	t.setupRebalanceRx(config)

	pid := int64(os.Getpid())
	t.uxprocess = &uxprocess{time.Now(), strconv.FormatInt(pid, 16), pid}

	getfshealthchecker().SetDispatcher(t)

	ec.Init()
	t.ecmanager = newECM(t)

	aborted, _ := t.xactions.isRebalancing(cmn.ActLocalReb)
	if aborted {
		go func() {
			glog.Infoln("resuming local rebalance...")
			t.rebManager.runLocalReb()
		}()
	}

	dsort.RegisterNode(t.smapowner, t.bmdowner, t.si, t)
	if err := t.httprunner.run(); err != nil {
		return err
	}
	glog.Infof("%s is ready to handle requests", t.si.Name())
	glog.Flush()
	return nil
}

func (t *targetrunner) setupRebalanceRx(config *cmn.Config) {
	reb := &rebManager{t: t, filterGFN: filter.NewDefaultFilter()}
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
	t.rebManager = reb
}

// target-only stats
func (t *targetrunner) registerStats() {
	t.statsif.Register(stats.PutLatency, stats.KindLatency)
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
}

// stop gracefully
func (t *targetrunner) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", t.Getname(), err)
	sleep := t.xactions.abortAll()
	if t.publicServer.s != nil {
		t.unregister() // ignore errors
	}

	t.httprunner.stop(err)
	if sleep {
		time.Sleep(time.Second)
	}
}

//===========================================================================================
//
// targetrunner's API for external packages
//
//===========================================================================================

// implements cluster.Target interfaces
var _ cluster.Target = &targetrunner{}

func (t *targetrunner) AvgCapUsed(config *cmn.Config, used ...int32) (avgCapUsed int32, oos bool) {
	if len(used) > 0 {
		t.capUsed.Lock()
		t.capUsed.used = used[0]
		if t.capUsed.oos && t.capUsed.used < int32(config.LRU.HighWM) {
			t.capUsed.oos = false
		} else if !t.capUsed.oos && t.capUsed.used > int32(config.LRU.OOS) {
			t.capUsed.oos = true
		}
		avgCapUsed, oos = t.capUsed.used, t.capUsed.oos
		t.capUsed.Unlock()
	} else {
		t.capUsed.RLock()
		avgCapUsed, oos = t.capUsed.used, t.capUsed.oos
		t.capUsed.RUnlock()
	}
	return
}

func (t *targetrunner) IsRebalancing() bool {
	_, running := t.xactions.isRebalancing(cmn.ActGlobalReb)
	_, runningLocal := t.xactions.isRebalancing(cmn.ActLocalReb)
	return running || runningLocal
}

// gets triggered by the stats evaluation of a remaining capacity
// and then runs in a goroutine - see stats package, target_stats.go
func (t *targetrunner) RunLRU() {
	if t.IsRebalancing() {
		glog.Infoln("Warning: rebalancing (local or global) is in progress, skipping LRU run")
		return
	}
	xlru := t.xactions.renewLRU()
	if xlru == nil {
		return
	}
	ini := lru.InitLRU{
		Xlru:                xlru,
		Statsif:             t.statsif,
		T:                   t,
		GetFSUsedPercentage: ios.GetFSUsedPercentage,
		GetFSStats:          ios.GetFSStats,
	}
	lru.InitAndRun(&ini) // blocking

	xlru.EndTime(time.Now())
}

func (t *targetrunner) PrefetchQueueLen() int { return len(t.prefetchQueue) }

func (t *targetrunner) Prefetch() {
	xpre := t.xactions.renewPrefetch(getstorstatsrunner())

	if xpre == nil {
		return
	}
loop:
	for {
		select {
		case fwd := <-t.prefetchQueue:
			if !fwd.deadline.IsZero() && time.Now().After(fwd.deadline) {
				continue
			}
			bckIsLocal, _ := t.bmdowner.get().ValidateBucket(fwd.bucket, fwd.bckProvider)
			if bckIsLocal {
				glog.Errorf("prefetch: bucket %s is local, nothing to do", fwd.bucket)
			} else {
				for _, objname := range fwd.objnames {
					t.prefetchMissing(fwd.ctx, objname, fwd.bucket, fwd.bckProvider)
				}
			}
			// Signal completion of prefetch
			if fwd.done != nil {
				fwd.done <- struct{}{}
			}
		default:
			// When there is nothing left to fetch, the prefetch routine ends
			break loop

		}
	}
	xpre.EndTime(time.Now())
}

func (t *targetrunner) GetBowner() cluster.Bowner   { return t.bmdowner }
func (t *targetrunner) FSHC(err error, path string) { t.fshc(err, path) }
func (t *targetrunner) GetMem2() *memsys.Mem2       { return gmem2 }
func (t *targetrunner) GetFSPRG() fs.PathRunGroup   { return &t.fsprg }

//===========================================================================================
//
// http handlers: data and metadata
//
//===========================================================================================

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
	bucket := apiItems[0]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)

	normalizedBckProvider, err := cmn.BckProviderFromStr(bckProvider)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	// list bucket names
	if bucket == cmn.ListAll {
		query := r.URL.Query()
		what := query.Get(cmn.URLParamWhat)
		if what == cmn.GetWhatBucketMetaX {
			t.bucketsFromXattr(w, r)
		} else {
			t.getbucketnames(w, r, normalizedBckProvider)
		}
		return
	}
	s := fmt.Sprintf("Invalid route /buckets/%s", bucket)
	t.invalmsghdlr(w, r, s)
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
		lom       *cluster.LOM
		started   time.Time
		config    = cmn.GCO.Get()
		ct        = t.contextWithAuth(r.Header)
		query     = r.URL.Query()
		errcode   int
		retried   bool
		fromCache bool
	)
	//
	// 1. start, init lom, ...readahead
	//
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	bckProvider := query.Get(cmn.URLParamBckProvider)
	started = time.Now()
	if redirDelta := t.redirectLatency(started, query); redirDelta != 0 {
		t.statsif.Add(stats.GetRedirLatency, redirDelta)
	}
	rangeOff, rangeLen, errstr := t.offsetAndLength(query)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}
	lom, errstr = cluster.LOM{T: t, Bucket: bucket, Objname: objname, BucketProvider: bckProvider}.Init(config)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}
	if err = lom.AllowGET(); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	// 2. under lock: lom init, restore from cluster
	cluster.ObjectLocker.Lock(lom.Uname(), false)
do:
	// all the next checks work with disks - skip all if dryRun.disk=true
	coldGet := false
	if dryRun.disk {
		goto get
	}

	fromCache, errstr = lom.Load(true)
	if errstr != "" {
		cluster.ObjectLocker.Unlock(lom.Uname(), false)
		t.invalmsghdlr(w, r, errstr)
		return
	}

	coldGet = !lom.Exists()
	if coldGet && lom.BckIsLocal {
		// does not exist in the local bucket: restore from neighbors
		if errstr, errcode = t.restoreObjLBNeigh(lom, r); errstr != "" {
			cluster.ObjectLocker.Unlock(lom.Uname(), false)
			t.invalmsghdlr(w, r, errstr, errcode)
			return
		}
		goto get
	}
	if !coldGet && !lom.BckIsLocal { // exists && cloud-bucket : check ver if requested
		if lom.Version() != "" && lom.VerConf().ValidateWarmGet {
			if coldGet, errstr, errcode = t.checkCloudVersion(ct, lom); errstr != "" {
				lom.Uncache()
				cluster.ObjectLocker.Unlock(lom.Uname(), false)
				t.invalmsghdlr(w, r, errstr, errcode)
				return
			}
		}
	}

	// checksum validation, if requested
	if !coldGet && lom.CksumConf().ValidateWarmGet {
		if fromCache {
			errstr = lom.ValidateChecksum(true)
		} else {
			errstr = lom.ValidateDiskChecksum()
		}

		if errstr != "" {
			if lom.BadCksum {
				glog.Errorln(errstr)
				if lom.BckIsLocal {
					if err := os.Remove(lom.FQN); err != nil {
						glog.Warningf("%s - failed to remove, err: %v", errstr, err)
					}
					cluster.ObjectLocker.Unlock(lom.Uname(), false)
					t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
					return
				}
				coldGet = true
			} else {
				cluster.ObjectLocker.Unlock(lom.Uname(), false)
				t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
				return
			}
		}
	}

	// 3. coldget
	if coldGet {
		cluster.ObjectLocker.Unlock(lom.Uname(), false)
		if err = lom.AllowColdGET(); err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
		lom.SetAtimeUnix(started.UnixNano())
		if errstr, errcode := t.GetCold(ct, lom, false); errstr != "" {
			t.invalmsghdlr(w, r, errstr, errcode)
			return
		}
		t.putMirror(lom)
	}

	// 4. get locally and stream back
get:
	retry, errstr := t.objGetComplete(w, r, lom, started, rangeOff, rangeLen, coldGet)
	if retry && !retried {
		glog.Warningf("GET %s: uncaching and retrying...", lom)
		retried = true
		lom.Uncache()
		goto do
	}
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
	}
	cluster.ObjectLocker.Unlock(lom.Uname(), false)
}

//
// 3a. attempt to restore an object that is missing in the LOCAL BUCKET - from:
//     1) local FS, 2) this cluster, 3) other tiers in the DC 4) from other
//		targets using erasure coding(if it is on)
// FIXME: must be done => (getfqn, and under write lock)
//
func (t *targetrunner) restoreObjLBNeigh(lom *cluster.LOM, r *http.Request) (errstr string, errcode int) {
	// check FS-wide if local rebalance is running
	aborted, running := t.xactions.isRebalancing(cmn.ActLocalReb)
	gfnActive := t.gfn.local.active()
	if aborted || running || gfnActive {
		// FIXME: move this part to lom
		oldFQN, oldSize := getFromOtherLocalFS(lom)
		if oldFQN != "" {
			lom.FQN = oldFQN
			lom.SetSize(oldSize)
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("restored from LFS %s (%s)", lom, cmn.B2S(oldSize, 1))
			}
			lom.ReCache()
			return
		}
	}

	// HACK: if there's not enough EC targets to restore an sliced object, we might be able to restore it
	// if it was replicated. In this case even just one additional target might be sufficient
	// This won't succeed if an object was sliced, neither will ecmanager.RestoreObject(lom)
	enoughECRestoreTargets := lom.BckProps.EC.RequiredRestoreTargets() <= t.smapowner.Get().CountTargets()

	// check cluster-wide ("ask neighbors")
	aborted, running = t.xactions.isRebalancing(cmn.ActGlobalReb)
	gfnActive, smap := t.gfn.global.active()
	if aborted || running || gfnActive || !enoughECRestoreTargets {
		if !gfnActive {
			smap = t.smapowner.get()
		}
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("neighbor lookup: aborted=%t, running=%t, lookup=%t", aborted, running, gfnActive)
		}
		// retry in case the object is being moved right now
		for retry := 0; retry < getFromNeighRetries; retry++ {
			if err := t.getFromNeighbor(r, lom, smap); err != nil {
				if glog.FastV(4, glog.SmoduleAIS) {
					glog.Infof("Unsuccessful GFN: %v.", err)
				}
				time.Sleep(getFromNeighSleep)
				continue
			}
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("restored from a neighbor: %s (%s)", lom, cmn.B2S(lom.Size(), 1))
			}
			return
		}
	}
	// restore from existing EC slices if possible
	if ecErr := t.ecmanager.RestoreObject(lom); ecErr == nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s/%s is restored successfully", lom.Bucket, lom.Objname)
		}
		lom.Load(true)
		return
	} else if ecErr != ec.ErrorECDisabled {
		errstr = fmt.Sprintf("Failed to restore object %s/%s: %v", lom.Bucket, lom.Objname, ecErr)
	}

	s := fmt.Sprintf("GET local: %s(%s) %s", lom, lom.FQN, cmn.DoesNotExist)
	if errstr != "" {
		errstr = s + " => [" + errstr + "]"
	} else {
		errstr = s
	}
	if errcode == 0 {
		errcode = http.StatusNotFound
	}
	return
}

//
// 4. read local, write http (note: coldGet() keeps the read lock if successful)
//
func (t *targetrunner) objGetComplete(w http.ResponseWriter, r *http.Request, lom *cluster.LOM, started time.Time,
	rangeOff, rangeLen int64, coldGet bool) (retry bool, errstr string) {
	var (
		file            *os.File
		sgl             *memsys.SGL
		slab            *memsys.Slab2
		buf             []byte
		rangeReader     io.ReadSeeker
		reader          io.Reader
		written         int64
		err             error
		isGFNRequest, _ = cmn.ParseBool(r.URL.Query().Get(cmn.URLParamIsGFNRequest))
	)
	defer func() {
		// rahfcacher.got()
		if file != nil {
			file.Close()
		}
		if buf != nil {
			slab.Free(buf)
		}
		if sgl != nil {
			sgl.Free()
		}
	}()
	ckConf := lom.CksumConf()
	cksumRange := ckConf.Type != cmn.ChecksumNone && rangeLen > 0 && ckConf.EnableReadRange
	hdr := w.Header()

	if lom.Cksum() != nil && !cksumRange {
		cksumType, cksumValue := lom.Cksum().Get()
		hdr.Set(cmn.HeaderObjCksumType, cksumType)
		hdr.Set(cmn.HeaderObjCksumVal, cksumValue)
	}
	if lom.Version() != "" {
		hdr.Set(cmn.HeaderObjVersion, lom.Version())
	}
	hdr.Set(cmn.HeaderObjSize, strconv.FormatInt(lom.Size(), 10))

	timeInt := lom.Atime().UnixNano()
	if lom.Atime().IsZero() {
		timeInt = 0
	}
	hdr.Set(cmn.HeaderObjAtime, strconv.FormatInt(timeInt, 10))

	// loopback if disk IO is disabled
	if dryRun.disk {
		rd := newDryReader(dryRun.size)
		if _, err = io.Copy(w, rd); err != nil {
			// NOTE: Cannot call invalid handler because it would be double header write
			errstr = fmt.Sprintf("dry-run: failed to send random response, err: %v", err)
			glog.Error(errstr)
			t.statsif.Add(stats.ErrGetCount, 1)
			return
		}
		delta := time.Since(started)
		t.statsif.AddMany(stats.NamedVal64{stats.GetCount, 1}, stats.NamedVal64{stats.GetLatency, int64(delta)})
		return
	}

	if lom.Size() == 0 {
		glog.Warningf("%s size=0(zero)", lom) // TODO: optimize out much of the below
		return
	}
	fqn := lom.LoadBalanceGET() // coldGet => len(CopyFQN) == 0
	file, err = os.Open(fqn)
	if err != nil {
		if os.IsNotExist(err) {
			errstr = err.Error()
			retry = true // (!lom.BckIsLocal || lom.ECEnabled() || GFN...)
		} else {
			t.fshc(err, fqn)
			errstr = fmt.Sprintf("%s: err: %v", lom, err)
		}
		return
	}
	if rangeLen == 0 {
		reader = file
		// No need to allocate buffer for whole object (it might be very large).
		buf, slab = gmem2.AllocFromSlab2(cmn.MinI64(lom.Size(), 8*cmn.MiB))
	} else {
		buf, slab = gmem2.AllocFromSlab2(cmn.MinI64(rangeLen, 8*cmn.MiB))
		if cksumRange {
			var cksum string
			cksum, sgl, rangeReader, errstr = t.rangeCksum(file, fqn, rangeOff, rangeLen, buf)
			if errstr != "" {
				return
			}
			reader = rangeReader
			hdr.Set(cmn.HeaderObjCksumType, ckConf.Type)
			hdr.Set(cmn.HeaderObjCksumVal, cksum)
		} else {
			reader = io.NewSectionReader(file, rangeOff, rangeLen)
		}
	}

	written, err = io.CopyBuffer(w, reader, buf)
	if err != nil {
		errstr = fmt.Sprintf("Failed to GET %s, err: %v", fqn, err)
		glog.Error(errstr)
		t.fshc(err, fqn)
		t.statsif.Add(stats.ErrGetCount, 1)
		return
	}

	// GFN: atime must be already set by the getFromNeighbor
	if !coldGet && !isGFNRequest {
		lom.SetAtimeUnix(started.UnixNano())
		lom.ReCache() // GFN and cold GETs already did this
	}

	// Update objects which were sent during GFN. Thanks to this we will not
	// have to resend them in global rebalance. In case of race between rebalance
	// and GFN, the former wins and it will result in double send.
	if isGFNRequest {
		t.rebManager.filterGFN.Insert([]byte(lom.Uname()))
	}

	if glog.FastV(4, glog.SmoduleAIS) {
		s := fmt.Sprintf("GET: %s(%s), %d µs", lom, cmn.B2S(written, 1), int64(time.Since(started)/time.Microsecond))
		if coldGet {
			s += " (cold)"
		}
		glog.Infoln(s)
	}
	delta := time.Since(started)
	t.statsif.AddMany(
		stats.NamedVal64{Name: stats.GetThroughput, Val: written},
		stats.NamedVal64{Name: stats.GetLatency, Val: int64(delta)},
		stats.NamedVal64{Name: stats.GetCount, Val: 1},
	)
	return
}

func (t *targetrunner) rangeCksum(r io.ReaderAt, fqn string, offset, length int64, buf []byte) (
	cksumValue string, sgl *memsys.SGL, rangeReader io.ReadSeeker, errstr string) {
	var (
		err error
	)
	rangeReader = io.NewSectionReader(r, offset, length)
	if length <= maxBytesInMem {
		sgl = gmem2.NewSGL(length)
		if _, cksumValue, err = cmn.WriteWithHash(sgl, rangeReader, buf); err != nil {
			errstr = fmt.Sprintf("failed to read byte range, offset:%d, length:%d from %s, err: %v", offset, length, fqn, err)
			t.fshc(err, fqn)
			return
		}
		// overriding rangeReader here to read from the sgl
		rangeReader = memsys.NewReader(sgl)
	}

	if _, err = rangeReader.Seek(0, io.SeekStart); err != nil {
		errstr = fmt.Sprintf("failed to seek file %s to beginning, err: %v", fqn, err)
		t.fshc(err, fqn)
		return
	}

	return
}

func (t *targetrunner) offsetAndLength(query url.Values) (offset, length int64, errstr string) {
	offsetStr := query.Get(cmn.URLParamOffset)
	lengthStr := query.Get(cmn.URLParamLength)
	if offsetStr == "" && lengthStr == "" {
		return
	}
	s := fmt.Sprintf("Invalid offset [%s] and/or length [%s]", offsetStr, lengthStr)
	if offsetStr == "" || lengthStr == "" {
		errstr = s
		return
	}
	o, err1 := strconv.ParseInt(url.QueryEscape(offsetStr), 10, 64)
	l, err2 := strconv.ParseInt(url.QueryEscape(lengthStr), 10, 64)
	if err1 != nil || err2 != nil || o < 0 || l <= 0 {
		errstr = s
		return
	}
	offset, length = o, l
	return
}

// PUT /v1/objects/bucket-name/object-name
func (t *targetrunner) httpobjput(w http.ResponseWriter, r *http.Request) {
	var (
		query = r.URL.Query()
	)
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	bckProvider := query.Get(cmn.URLParamBckProvider)
	started := time.Now()
	if redelta := t.redirectLatency(started, query); redelta != 0 {
		t.statsif.Add(stats.PutRedirLatency, redelta)
	}
	// PUT
	if !t.verifyProxyRedirection(w, r, r.Method) {
		return
	}
	if _, oos := t.AvgCapUsed(nil); oos {
		t.invalmsghdlr(w, r, "OOS")
		return
	}
	lom, errstr := cluster.LOM{T: t, Bucket: bucket, Objname: objname, BucketProvider: bckProvider}.Init()
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}
	if err = lom.AllowPUT(); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if lom.BckIsLocal && lom.VerConf().Enabled {
		lom.Load(true) // need to know the current version if versionig enabled
	}
	lom.SetAtimeUnix(started.UnixNano())
	if err, errCode := t.doPut(r, lom, started); err != nil {
		t.invalmsghdlr(w, r, err.Error(), errCode)
	}
}

// DELETE { action } /v1/buckets/bucket-name
func (t *targetrunner) httpbckdelete(w http.ResponseWriter, r *http.Request) {
	var (
		bucket  string
		msgInt  actionMsgInternal
		started = time.Now()
	)
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket = apitems[0]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	bmd, bckIsLocal := t.validateBucket(w, r, bucket, bckProvider)
	if bmd == nil {
		return
	}
	if err := bmd.AllowDELETE(bucket, bckIsLocal); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	b, err := ioutil.ReadAll(r.Body)

	if err == nil && len(b) > 0 {
		err = jsoniter.Unmarshal(b, &msgInt)
	}
	t.ensureLatestMD(msgInt)
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

	switch msgInt.Action {
	case cmn.ActEvictCB:
		cluster.EvictCache(bucket)
		fs.Mountpaths.EvictCloudBucket(bucket) // validation handled in proxy.go
	case cmn.ActDelete, cmn.ActEvictObjects:
		if len(b) > 0 { // must be a List/Range request
			err := t.listRangeOperation(r, apitems, bckProvider, &msgInt)
			if err != nil {
				t.invalmsghdlr(w, r, fmt.Sprintf("Failed to delete/evict objects: %v", err))
			} else if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("DELETE list|range: %s, %d µs",
					bmd.Bstring(bucket, bckIsLocal), int64(time.Since(started)/time.Microsecond))
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
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	b, errstr, err := cmn.ReadBytes(r)
	if err != nil {
		t.invalmsghdlr(w, r, errstr)
		return
	}
	if len(b) > 0 {
		if err = jsoniter.Unmarshal(b, &msg); err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
		evict = (msg.Action == cmn.ActEvictObjects)
	}

	lom, errstr := cluster.LOM{T: t, Bucket: bucket, Objname: objname, BucketProvider: bckProvider}.Init()
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
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
		started = time.Now()
		msgInt  actionMsgInternal
		bmd     *bucketMD
	)
	if cmn.ReadJSON(w, r, &msgInt) != nil {
		return
	}
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}

	bucket := apitems[0]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	bmd, bckIsLocal := t.validateBucket(w, r, bucket, bckProvider)
	if bmd == nil {
		return
	}
	t.ensureLatestMD(msgInt)
	switch msgInt.Action {
	case cmn.ActPrefetch:
		// validation done in proxy.go
		if err := t.listRangeOperation(r, apitems, bckProvider, &msgInt); err != nil {
			t.invalmsghdlr(w, r, fmt.Sprintf("Failed to prefetch files: %v", err))
			return
		}
	case cmn.ActRenameLB:
		bucketFrom, bucketTo := bucket, msgInt.Name

		t.bmdowner.Lock() // lock#1 begin

		bmd = t.bmdowner.get()
		props, ok := bmd.Get(bucketFrom, true)
		if !ok {
			t.bmdowner.Unlock()
			s := fmt.Sprintf("bucket %s %s", bmd.Bstring(bucketFrom, true), cmn.DoesNotExist)
			t.invalmsghdlr(w, r, s)
			return
		}
		clone := bmd.clone()
		clone.LBmap[bucketTo] = props
		t.bmdowner.put(clone) // bmd updated with an added bucket, lock#1 end
		t.bmdowner.Unlock()

		if errstr := t.renameLB(bucketFrom, bucketTo); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
		}
		glog.Infof("renamed bucket %s => %s, %s v%d", bucketFrom, bucketTo, bmdTermName, clone.version())
	case cmn.ActListObjects:
		// list the bucket and return
		tag, ok := t.listbucket(w, r, bucket, bckIsLocal, &msgInt)
		if ok {
			delta := time.Since(started)
			t.statsif.AddMany(stats.NamedVal64{stats.ListCount, 1}, stats.NamedVal64{stats.ListLatency, int64(delta)})
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("LIST %s: %s, %d µs", tag, bmd.Bstring(bucket, bckIsLocal), int64(delta/time.Microsecond))
			}
		}
	case cmn.ActMakeNCopies:
		copies, err := t.parseValidateNCopies(msgInt.Value)
		if err == nil {
			err = mirror.ValidateNCopies(copies)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
		t.xactions.abortBucketXact(cmn.ActPutCopies, bucket)
		t.xactions.renewBckMakeNCopies(bucket, t, copies, bckIsLocal)
	default:
		t.invalmsghdlr(w, r, "Unexpected action "+msgInt.Action)
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
		t.renameObject(w, r, msg, t.smapowner.get().ProxySI.DaemonID /*to force thru proxy-redirection check*/)
	default:
		t.invalmsghdlr(w, r, "Unexpected action "+msg.Action)
	}
}

// HEAD /v1/buckets/bucket-name
func (t *targetrunner) httpbckhead(w http.ResponseWriter, r *http.Request) {
	var (
		bucketProps cmn.SimpleKVs
		query       = r.URL.Query()
		errCode     int
	)
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	bmd, bckIsLocal := t.validateBucket(w, r, bucket, bckProvider)
	if bmd == nil {
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, bucket, pid)
	}
	config := cmn.GCO.Get()
	if !bckIsLocal {
		bucketProps, err, errCode = getcloudif().headbucket(t.contextWithAuth(r.Header), bucket)
		if err != nil {
			errMsg := fmt.Sprintf("bucket %s either %s or is not accessible, err: %v", bucket, cmn.DoesNotExist, err)
			t.invalmsghdlr(w, r, errMsg, errCode)
			return
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
	props, ok := bmd.Get(bucket, bckIsLocal)
	if props == nil {
		return
	}
	cksumConf := &config.Cksum // FIXME: must be props.CksumConf w/o conditions, here and elsewhere
	if ok && props.Cksum.Type != cmn.PropInherit {
		cksumConf = &props.Cksum
	}
	verConf := &config.Ver
	if ok {
		verConf = &props.Versioning
	}
	// transfer bucket props via http header
	// (it is ok for Cloud buckets not to have locally cached props)
	hdr.Set(cmn.HeaderReadPolicy, props.Tiering.ReadPolicy)
	hdr.Set(cmn.HeaderWritePolicy, props.Tiering.WritePolicy)
	hdr.Set(cmn.HeaderBucketChecksumType, cksumConf.Type)
	hdr.Set(cmn.HeaderBucketValidateColdGet, strconv.FormatBool(cksumConf.ValidateColdGet))
	hdr.Set(cmn.HeaderBucketValidateWarmGet, strconv.FormatBool(cksumConf.ValidateWarmGet))
	hdr.Set(cmn.HeaderBucketValidateObjMove, strconv.FormatBool(cksumConf.ValidateObjMove))
	hdr.Set(cmn.HeaderBucketEnableReadRange, strconv.FormatBool(cksumConf.EnableReadRange))

	hdr.Set(cmn.HeaderBucketVerEnabled, strconv.FormatBool(verConf.Enabled))
	hdr.Set(cmn.HeaderBucketVerValidateWarm, strconv.FormatBool(verConf.ValidateWarmGet))

	hdr.Set(cmn.HeaderBucketLRULowWM, strconv.FormatInt(props.LRU.LowWM, 10))
	hdr.Set(cmn.HeaderBucketLRUHighWM, strconv.FormatInt(props.LRU.HighWM, 10))
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
	hdr.Set(cmn.HeaderBucketECMinSize, strconv.FormatUint(uint64(props.EC.ObjSizeLimit), 10))
	hdr.Set(cmn.HeaderBucketECData, strconv.FormatUint(uint64(props.EC.DataSlices), 10))
	hdr.Set(cmn.HeaderBucketECParity, strconv.FormatUint(uint64(props.EC.ParitySlices), 10))
	hdr.Set(cmn.HeaderBucketAccessAttrs, strconv.FormatUint(props.AccessAttrs, 10))
}

// HEAD /v1/objects/bucket-name/object-name
func (t *targetrunner) httpobjhead(w http.ResponseWriter, r *http.Request) {
	var (
		bucket, objname, errstr string
		objmeta                 cmn.SimpleKVs
		query                   = r.URL.Query()

		errcode        int
		exists         bool
		checkCached, _ = cmn.ParseBool(query.Get(cmn.URLParamCheckCached)) // establish local presence, ignore obj attrs
		silent, _      = cmn.ParseBool(query.Get(cmn.URLParamSilent))
	)

	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname = apitems[0], apitems[1]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	invalidHandler := t.invalmsghdlr
	if silent {
		invalidHandler = t.invalmsghdlrsilent
	}

	lom, errstr := cluster.LOM{T: t, Bucket: bucket, Objname: objname, BucketProvider: bckProvider}.Init()
	if errstr != "" {
		invalidHandler(w, r, errstr)
		return
	}
	cluster.ObjectLocker.Lock(lom.Uname(), false)
	defer cluster.ObjectLocker.Unlock(lom.Uname(), false)

	if _, errstr = lom.Load(true); errstr != "" { // (doesnotexist -> ok, other)
		invalidHandler(w, r, errstr)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, lom.StringEx(), pid)
	}

	exists = lom.Exists()
	if lom.BckIsLocal || checkCached {
		if !exists {
			invalidHandler(w, r, fmt.Sprintf("no such object %s in bucket %s", objname, bucket), http.StatusNotFound)
			return
		}
		if checkCached {
			return
		}
		objmeta = make(cmn.SimpleKVs)
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s(%s), ver=%s", lom, cmn.B2S(lom.Size(), 1), lom.Version())
		}
	} else {
		objmeta, err, errcode = getcloudif().headobject(t.contextWithAuth(r.Header), lom)
		if err != nil {
			errMsg := fmt.Sprintf("%s: failed to head metadata, err: %v", lom, err)
			invalidHandler(w, r, errMsg, errcode)
			return
		}
	}

	if exists {
		objmeta[cmn.HeaderObjSize] = strconv.FormatInt(lom.Size(), 10)
		objmeta[cmn.HeaderObjVersion] = lom.Version()
		if lom.AtimeUnix() != 0 {
			objmeta[cmn.HeaderObjAtime] = lom.Atime().Format(time.RFC822)
		}
		objmeta[cmn.HeaderObjNumCopies] = strconv.Itoa(lom.NumCopies())
		_, ckSum := lom.Cksum().Get()
		objmeta[cmn.HeaderObjCksumVal] = ckSum
	}
	objmeta[cmn.HeaderObjIsBckLocal] = strconv.FormatBool(lom.BckIsLocal)
	objmeta[cmn.HeaderObjPresent] = strconv.FormatBool(exists)

	hdr := w.Header()
	for k, v := range objmeta {
		hdr.Set(k, v)
	}
}

//====================================================================================
//
// supporting methods and misc
//
//====================================================================================
func (t *targetrunner) renameLB(bucketFrom, bucketTo string) (errstr string) {
	// ready to receive migrated obj-s _after_ that point
	// insert directly w/o incrementing the version (metasyncer will do at the end of the operation)
	wg := &sync.WaitGroup{}

	pid := t.smapowner.get().ProxySI.DaemonID
	availablePaths, _ := fs.Mountpaths.Get()
	ch := make(chan string, len(fs.CSM.RegisteredContentTypes)*len(availablePaths))
	for contentType := range fs.CSM.RegisteredContentTypes {
		for _, mpathInfo := range availablePaths {
			// Create directory for new local bucket
			toDir := mpathInfo.MakePathBucket(contentType, bucketTo, true /*bucket is local*/)
			if err := cmn.CreateDir(toDir); err != nil {
				ch <- fmt.Sprintf("Failed to create dir %s, error: %v", toDir, err)
				continue
			}

			wg.Add(1)
			fromDir := mpathInfo.MakePathBucket(contentType, bucketFrom, true /*bucket is local*/)
			go func(fromDir string) {
				time.Sleep(time.Millisecond * 100) // FIXME: 2-phase for the targets to 1) prep (above) and 2) rebalance
				ch <- t.renameOne(fromDir, bucketFrom, bucketTo, pid)
				wg.Done()
			}(fromDir)
		}
	}
	wg.Wait()
	close(ch)
	for errstr = range ch {
		if errstr != "" {
			return
		}
	}
	return
}

func (t *targetrunner) renameOne(fromdir, bucketFrom, bucketTo, pid string) (errstr string) {
	renctx := &renamectx{bucketFrom: bucketFrom, bucketTo: bucketTo, t: t, pid: pid}

	if err := filepath.Walk(fromdir, renctx.walkf); err != nil {
		errstr = fmt.Sprintf("Failed to rename %s, err: %v", fromdir, err)
	}
	return
}

func (renctx *renamectx) walkf(fqn string, osfi os.FileInfo, err error) error {
	if err != nil {
		if errstr := cmn.PathWalkErr(err); errstr != "" {
			glog.Errorf(errstr)
			return err
		}
		return nil
	}
	if osfi.Mode().IsDir() {
		return nil
	}
	// FIXME: workfiles indicate work in progress. Renaming could break ongoing
	// operations and not renaming it probably result in having file in wrong directory.
	if !fs.CSM.PermToProcess(fqn) {
		return nil
	}
	// FIXME: ignoring "misplaced" (non-error) and errors that ResolveFQN may return
	parsedFQN, _, errstr := cluster.ResolveFQN(fqn, nil, true /* bucket is local */)
	contentType, bucket, objname := parsedFQN.ContentType, parsedFQN.Bucket, parsedFQN.Objname
	if errstr == "" {
		if bucket != renctx.bucketFrom {
			return fmt.Errorf("unexpected: bucket %s != %s bucketFrom", bucket, renctx.bucketFrom)
		}
	}
	if errstr := renctx.t.renameBucketObject(contentType, bucket, objname, renctx.bucketTo, objname, renctx.pid); errstr != "" {
		return fmt.Errorf(errstr)
	}
	return nil
}

// checkCloudVersion returns (vchanged=) true if object versions differ between Cloud and local cache;
// should be called only if the local copy exists
func (t *targetrunner) checkCloudVersion(ct context.Context, lom *cluster.LOM) (vchanged bool, errstr string, errcode int) {
	var objmeta cmn.SimpleKVs
	objmeta, err, errcode := t.cloudif.headobject(ct, lom)
	if err != nil {
		errstr = fmt.Sprintf("%s: failed to head metadata, err: %v", lom, err)
		return
	}
	if cloudVersion, ok := objmeta[cmn.HeaderObjVersion]; ok {
		if lom.Version() != cloudVersion {
			glog.Infof("%s: version changed from %s to %s", lom, lom.Version(), cloudVersion)
			vchanged = true
		}
	}
	return
}

func (t *targetrunner) getFromNeighbor(r *http.Request, lom *cluster.LOM, smap *smapX) (err error) {
	neighsi := t.lookupRemotely(lom, smap)
	if neighsi == nil {
		err = fmt.Errorf("failed cluster-wide lookup %s", lom)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("Found %s at %s", lom, neighsi)
	}

	query := url.Values{}
	query.Add(cmn.URLParamBckProvider, lom.BucketProvider)
	query.Add(cmn.URLParamIsGFNRequest, "true")
	reqArgs := cmn.ReqArgs{
		Method: http.MethodGet,
		Base:   neighsi.URL(cmn.NetworkIntraData),
		Path:   r.URL.Path,
		Query:  query,
	}

	req, _, cancel, err := reqArgs.ReqWithTimeout(lom.Config().Timeout.SendFile)
	if err != nil {
		return fmt.Errorf("failed to create request, err: %v", err)
	}
	defer cancel()

	response, err := t.httpclientLongTimeout.Do(req)
	if err != nil {
		return fmt.Errorf("failed to GET redirect URL %q, err: %v", reqArgs.URL(), err)
	}
	var (
		cksumValue = response.Header.Get(cmn.HeaderObjCksumVal)
		cksumType  = response.Header.Get(cmn.HeaderObjCksumType)
		cksum      = cmn.NewCksum(cksumType, cksumValue)
		version    = response.Header.Get(cmn.HeaderObjVersion)
		workFQN    = fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfileRemote)
		atimeStr   = response.Header.Get(cmn.HeaderObjAtime)
	)

	// The string in the header is an int represented as a string, NOT a formatted date string.
	atime, err := cmn.S2TimeUnix(atimeStr)
	if err != nil {
		return
	}
	lom.SetCksum(cksum)
	lom.SetVersion(version)
	lom.SetAtimeUnix(atime)
	roi := &recvObjInfo{
		t:        t,
		lom:      lom,
		workFQN:  workFQN,
		r:        response.Body,
		migrated: true,
	}
	if err = roi.writeToFile(); err != nil {
		return
	}
	// commit
	if err = cmn.MvFile(workFQN, lom.FQN); err != nil {
		return
	}
	if err = lom.Persist(); err != nil {
		return
	}
	lom.ReCache()
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("Success: %s (%s, %s) from %s", lom, cmn.B2S(lom.Size(), 1), lom.Cksum(), neighsi)
	}
	return
}

// FIXME: recomputes checksum if called with a bad one (optimize)
func (t *targetrunner) GetCold(ct context.Context, lom *cluster.LOM, prefetch bool) (errstr string, errcode int) {
	if prefetch {
		if !cluster.ObjectLocker.TryLock(lom.Uname(), true) {
			glog.Infof("prefetch: cold GET race: %s - skipping", lom)
			return "skip", 0
		}
	} else {
		cluster.ObjectLocker.Lock(lom.Uname(), true) // one cold-GET at a time
	}
	var (
		err             error
		vchanged, crace bool
		workFQN         = fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfileColdget)
	)
	if err, errcode = getcloudif().getobj(ct, workFQN, lom); err != nil {
		errstr = fmt.Sprintf("%s: GET failed, err: %v", lom, err)
		cluster.ObjectLocker.Unlock(lom.Uname(), true)
		return
	}
	defer func() {
		if errstr != "" {
			cluster.ObjectLocker.Unlock(lom.Uname(), true)
			if errRemove := os.Remove(workFQN); errRemove != nil {
				glog.Errorf("Nested error %s => (remove %s => err: %v)", errstr, workFQN, errRemove)
				t.fshc(errRemove, workFQN)
			}
		}
	}()
	if err = cmn.MvFile(workFQN, lom.FQN); err != nil {
		errstr = fmt.Sprintf("Unexpected failure to rename %s => %s, err: %v", workFQN, lom.FQN, err)
		t.fshc(err, lom.FQN)
		return
	}
	if err = lom.Persist(); err != nil {
		errstr = err.Error()
		return
	}
	lom.ReCache()

	// NOTE: GET - downgrade and keep the lock, PREFETCH - unlock
	if prefetch {
		cluster.ObjectLocker.Unlock(lom.Uname(), true)
	} else {
		if vchanged {
			t.statsif.AddMany(stats.NamedVal64{stats.GetColdCount, 1},
				stats.NamedVal64{stats.GetColdSize, lom.Size()},
				stats.NamedVal64{stats.VerChangeSize, lom.Size()},
				stats.NamedVal64{stats.VerChangeCount, 1})
		} else if !crace {
			t.statsif.AddMany(stats.NamedVal64{stats.GetColdCount, 1}, stats.NamedVal64{stats.GetColdSize, lom.Size()})
		}
		cluster.ObjectLocker.DowngradeLock(lom.Uname())
	}
	return
}

func (t *targetrunner) lookupRemotely(lom *cluster.LOM, smap *smapX) *cluster.Snode {
	query := make(url.Values)
	query.Add(cmn.URLParamSilent, "true")
	res := t.broadcastTo(
		cmn.URLPath(cmn.Version, cmn.Objects, lom.Bucket, lom.Objname),
		query,
		http.MethodHead,
		nil,
		smap,
		lom.Config().Timeout.MaxKeepalive,
		cmn.NetworkIntraControl,
		cluster.Targets,
	)

	for r := range res {
		if r.err == nil {
			return r.si
		}
	}

	return nil
}

func (t *targetrunner) bucketsFromXattr(w http.ResponseWriter, r *http.Request) {
	bmdXattr := &bucketMD{}
	if err := bmdXattr.LoadFromFS(); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	body := cmn.MustMarshal(bmdXattr)
	t.writeJSON(w, r, body, "getbucketsxattr")
}

func (t *targetrunner) getbucketnames(w http.ResponseWriter, r *http.Request, bckProvider string) {
	var (
		bmd         = t.bmdowner.get()
		bucketNames = &cmn.BucketNames{
			Local: make([]string, 0, len(bmd.LBmap)),
			Cloud: make([]string, 0, 64),
		}
	)

	if bckProvider != cmn.CloudBs {
		for bucket := range bmd.LBmap {
			bucketNames.Local = append(bucketNames.Local, bucket)
		}
	}

	buckets, err, errcode := getcloudif().getbucketnames(t.contextWithAuth(r.Header))
	if err != nil {
		errMsg := fmt.Sprintf("failed to list all buckets, err: %v", err)
		t.invalmsghdlr(w, r, errMsg, errcode)
		return
	}
	bucketNames.Cloud = buckets

	body := cmn.MustMarshal(bucketNames)
	t.writeJSON(w, r, body, "getbucketnames")
}

func (t *targetrunner) newFileWalk(bucket string, msg *cmn.SelectMsg) *allfinfos {
	// Marker is always a file name, so we need to strip filename from path
	markerDir := ""
	if msg.PageMarker != "" {
		markerDir = filepath.Dir(msg.PageMarker)
	}

	// A small optimization: set boolean variables need* to avoid
	// doing string search(strings.Contains) for every entry.
	ci := &allfinfos{
		t:            t, // targetrunner
		files:        make([]*cmn.BucketEntry, 0, cmn.DefaultPageSize),
		prefix:       msg.Prefix,
		marker:       msg.PageMarker,
		markerDir:    markerDir,
		msg:          msg,
		lastFilePath: "",
		bucket:       bucket,
		fileCount:    0,
		rootLength:   0,
		limit:        cmn.DefaultPageSize, // maximum number files to return
		needAtime:    strings.Contains(msg.Props, cmn.GetPropsAtime),
		needCtime:    strings.Contains(msg.Props, cmn.GetPropsCtime),
		needChkSum:   strings.Contains(msg.Props, cmn.GetPropsChecksum),
		needVersion:  strings.Contains(msg.Props, cmn.GetPropsVersion),
		needStatus:   strings.Contains(msg.Props, cmn.GetPropsStatus),
		needCopies:   strings.Contains(msg.Props, cmn.GetPropsCopies),
	}

	if msg.PageSize != 0 {
		ci.limit = msg.PageSize
	}

	return ci
}

// After putting a new version it updates xattr attributes for the object
// Local bucket:
//  - if bucket versioning is enable("all" or "local") then the version is autoincremented
// Cloud bucket:
//  - if the Cloud returns a new version id then save it to xattr
// In both case a new checksum is saved to xattrs
// compare with t.Receive()
func (t *targetrunner) doPut(r *http.Request, lom *cluster.LOM, started time.Time) (err error, errcode int) {
	var (
		header     = r.Header
		cksumType  = header.Get(cmn.HeaderObjCksumType)
		cksumValue = header.Get(cmn.HeaderObjCksumVal)
		cksum      = cmn.NewCksum(cksumType, cksumValue)
	)
	roi := &recvObjInfo{
		started:      started,
		t:            t,
		lom:          lom,
		r:            r.Body,
		cksumToCheck: cksum,
		ctx:          t.contextWithAuth(header),
		workFQN:      fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfilePut),
	}
	return roi.recv()
}

// slight variation vs t.doPut() above
func (t *targetrunner) Receive(workFQN string, reader io.ReadCloser, lom *cluster.LOM,
	recvType cluster.RecvType, cksum cmn.Cksummer, started time.Time) error {
	roi := &recvObjInfo{
		started: started,
		t:       t,
		lom:     lom,
		r:       reader,
		workFQN: workFQN,
		ctx:     context.Background(),
	}
	if recvType == cluster.ColdGet {
		roi.cold = true
		roi.cksumToCheck = cksum
	}
	err, _ := roi.recv()
	return err
}

func (roi *recvObjInfo) recv() (err error, errCode int) {
	lom := roi.lom
	// optimize out if the checksums do match
	if roi.cksumToCheck != nil {
		if cmn.EqCksum(lom.Cksum(), roi.cksumToCheck) {
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("%s is valid %s: PUT is a no-op", lom, roi.cksumToCheck)
			}
			io.Copy(ioutil.Discard, roi.r) // drain the reader
			return nil, 0
		}
	}

	if !dryRun.disk {
		if err := roi.writeToFile(); err != nil {
			return err, http.StatusInternalServerError
		}

		if errstr, errCode := roi.commit(); errstr != "" {
			return errors.New(errstr), errCode
		}
	}

	delta := time.Since(roi.started)
	roi.t.statsif.AddMany(stats.NamedVal64{stats.PutCount, 1}, stats.NamedVal64{stats.PutLatency, int64(delta)})
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("PUT %s: %d µs", lom, int64(delta/time.Microsecond))
	}
	return nil, 0
}

func (roi *recvObjInfo) commit() (errstr string, errCode int) {
	if errstr, errCode = roi.tryCommit(); errstr != "" {
		if _, err := os.Stat(roi.workFQN); err == nil || !os.IsNotExist(err) {
			if err == nil {
				err = errors.New(errstr)
			}
			roi.t.fshc(err, roi.workFQN)
			if err = os.Remove(roi.workFQN); err != nil {
				glog.Errorf("Nested error: %s => (remove %s => err: %v)", errstr, roi.workFQN, err)
			}
		}
		roi.lom.Uncache()
		return
	}
	if err := roi.t.ecmanager.EncodeObject(roi.lom); err != nil && err != ec.ErrorECDisabled {
		errstr = err.Error()
	}
	roi.t.putMirror(roi.lom)
	return
}

func (roi *recvObjInfo) tryCommit() (errstr string, errCode int) {
	var (
		ver string
		lom = roi.lom
	)
	if !lom.BckIsLocal && !roi.migrated {
		file, err := os.Open(roi.workFQN)
		if err != nil {
			errstr = fmt.Sprintf("failed to open %s err: %v", roi.workFQN, err)
			return
		}
		cmn.Assert(lom.Cksum() != nil)
		ver, err, errCode = getcloudif().putobj(roi.ctx, file, lom)
		file.Close()
		if err != nil {
			errstr = fmt.Sprintf("%s: PUT failed, err: %v", lom, err)
			return
		}
		lom.SetVersion(ver)
	}

	cluster.ObjectLocker.Lock(lom.Uname(), true)
	defer cluster.ObjectLocker.Unlock(lom.Uname(), true)

	if lom.BckIsLocal && lom.VerConf().Enabled {
		if ver, errstr = lom.IncObjectVersion(); errstr != "" {
			return
		}
		lom.SetVersion(ver)
	}
	// Don't persist meta, it will be persisted after move
	if errstr = lom.DelAllCopies(); errstr != "" {
		return
	}
	if err := cmn.MvFile(roi.workFQN, lom.FQN); err != nil {
		errstr = fmt.Sprintf("MvFile failed => %s: %v", lom, err)
		return
	}
	if err := lom.Persist(); err != nil {
		errstr = err.Error()
		glog.Errorf("failed to persist %s: %s", lom, errstr)
	}
	lom.ReCache()
	return
}

func (t *targetrunner) putMirror(lom *cluster.LOM) {
	mirrConf := lom.MirrorConf()
	if !mirrConf.Enabled {
		return
	}
	if nmp := fs.Mountpaths.NumAvail(); nmp < int(mirrConf.Copies) {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Warningf("insufficient ## mountpaths %d (bucket %s, ## copies %d)",
				nmp, lom.Bucket, mirrConf.Copies)
		}
		return
	}
	if t.xputlrep == nil || t.xputlrep.Finished() || !t.xputlrep.SameBucket(lom) {
		t.xputlrep = t.xactions.renewPutLocReplicas(lom)
	}
	if t.xputlrep == nil {
		return
	}
	err := t.xputlrep.Repl(lom)
	// retry upon race vs (just finished/timedout)
	if _, ok := err.(*cmn.ErrXpired); ok {
		t.xputlrep = t.xactions.renewPutLocReplicas(lom)
		if t.xputlrep != nil {
			err = t.xputlrep.Repl(lom)
		}
	}
	if err != nil {
		glog.Errorf("%s: unexpected failure to post for copying, err: %v", lom.StringEx(), err)
	}
}

func (t *targetrunner) objDelete(ct context.Context, lom *cluster.LOM, evict bool) error {
	var (
		cloudErr error
		errRet   error
	)

	cluster.ObjectLocker.Lock(lom.Uname(), true)
	defer cluster.ObjectLocker.Unlock(lom.Uname(), true)

	delFromCloud := !lom.BckIsLocal && !evict
	if _, errstr := lom.Load(false); errstr != "" {
		return errors.New(errstr)
	}
	delFromAIS := lom.Exists()

	if delFromCloud {
		if err, _ := getcloudif().deleteobj(ct, lom); err != nil {
			cloudErr = fmt.Errorf("%s: DELETE failed, err: %v", lom, err)
			t.statsif.Add(stats.DeleteCount, 1)
		}
	}
	if delFromAIS {
		// Don't persist meta as object will be removed soon anyway
		if errs := lom.DelAllCopies(); errs != "" {
			glog.Errorf("%s: %s", lom, errs)
		}
		errRet = os.Remove(lom.FQN)
		if errRet != nil {
			if !os.IsNotExist(errRet) {
				if cloudErr != nil {
					glog.Errorf("%s: failed to delete from cloud: %v", lom.StringEx(), cloudErr)
				}
				return errRet
			}
		}
		if evict {
			cmn.Assert(!lom.BckIsLocal)
			t.statsif.AddMany(
				stats.NamedVal64{stats.LruEvictCount, 1},
				stats.NamedVal64{stats.LruEvictSize, lom.Size()})
		}
	}
	if cloudErr != nil {
		return fmt.Errorf("%s: failed to delete from cloud: %v", lom.StringEx(), cloudErr)
	}
	return errRet
}

func (t *targetrunner) renameObject(w http.ResponseWriter, r *http.Request, msg cmn.ActionMsg, pid string) {
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objnameFrom := apitems[0], apitems[1]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	if bmd, _ := t.validateBucket(w, r, bucket, bckProvider); bmd == nil {
		return
	}
	objnameTo := msg.Name
	uname := cluster.Bo2Uname(bucket, objnameFrom)
	cluster.ObjectLocker.Lock(uname, true)

	if errstr := t.renameBucketObject(fs.ObjectType, bucket, objnameFrom, bucket, objnameTo, pid); errstr != "" {
		t.invalmsghdlr(w, r, errstr)
	}
	cluster.ObjectLocker.Unlock(uname, true)
}

func (t *targetrunner) renameBucketObject(contentType, bucketFrom, objnameFrom, bucketTo, objnameTo, pid string) (errstr string) {
	var (
		file                  *cmn.FileHandle
		si                    *cluster.Snode
		newFQN                string
		cksumType, cksumValue string
		err                   error
	)
	if si, errstr = hrwTarget(bucketTo, objnameTo, t.smapowner.get()); errstr != "" {
		return
	}
	bmd := t.bmdowner.get()
	bckIsLocalFrom := bmd.IsLocal(bucketFrom)
	fqn, _, errstr := cluster.HrwFQN(contentType, bucketFrom, objnameFrom, bckIsLocalFrom)
	if errstr != "" {
		return
	}
	if _, err = os.Stat(fqn); err != nil {
		errstr = fmt.Sprintf("failed to fstat %s (%s/%s), err: %v", fqn, bucketFrom, objnameFrom, err)
		return
	}
	// local rename
	if si.DaemonID == t.si.DaemonID {
		bckIsLocalTo := bmd.IsLocal(bucketTo)
		newFQN, _, errstr = cluster.HrwFQN(contentType, bucketTo, objnameTo, bckIsLocalTo)
		if errstr != "" {
			return
		}
		if err := cmn.MvFile(fqn, newFQN); err != nil {
			errstr = fmt.Sprintf("Rename object %s/%s: %v", bucketFrom, objnameFrom, err)
		} else {
			t.statsif.Add(stats.RenameCount, 1)
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("Renamed %s => %s", fqn, newFQN)
			}
		}
		return
	}

	// migrate to another target
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("Migrating %s/%s at %s => %s/%s at %s",
			bucketFrom, objnameFrom, t.si.Name(), bucketTo, objnameTo, si.Name())
	}

	if file, err = cmn.NewFileHandle(fqn); err != nil {
		return fmt.Sprintf("failed to open %s, err: %v", fqn, err)
	}
	defer file.Close()

	lom, errstr := cluster.LOM{T: t, FQN: fqn}.Init()
	if errstr != "" {
		return errstr
	}
	if _, errstr := lom.Load(false); errstr != "" {
		return errstr
	}
	if lom.Cksum() != nil {
		cksumType, cksumValue = lom.Cksum().Get()
	}

	// PUT object into different target
	query := url.Values{}
	query.Add(cmn.URLParamBckProvider, lom.BucketProvider)
	query.Add(cmn.URLParamProxyID, pid)
	reqArgs := cmn.ReqArgs{
		Method: http.MethodPut,
		Base:   si.URL(cmn.NetworkIntraData),
		Path:   cmn.URLPath(cmn.Version, cmn.Objects, bucketTo, objnameTo),
		Query:  query,
		BodyR:  file,
	}
	req, _, cancel, err := reqArgs.ReqWithTimeout(lom.Config().Timeout.SendFile)
	if err != nil {
		errstr = fmt.Sprintf("unexpected failure to create request, err: %v", err)
		return
	}
	defer cancel()
	req.Header.Set(cmn.HeaderObjCksumType, cksumType)
	req.Header.Set(cmn.HeaderObjCksumVal, cksumValue)
	req.Header.Set(cmn.HeaderObjVersion, lom.Version())

	timeInt := lom.Atime().UnixNano()
	if lom.Atime().IsZero() {
		timeInt = 0
	}
	req.Header.Set(cmn.HeaderObjAtime, strconv.FormatInt(timeInt, 10))

	_, err = t.httpclientLongTimeout.Do(req)
	if err != nil {
		errstr = fmt.Sprintf("failed to PUT to %s, err: %v", reqArgs.URL(), err)
	}
	return
}

func (t *targetrunner) checkCacheQueryParameter(r *http.Request) (useCache bool, errstr string, errcode int) {
	useCacheStr := r.URL.Query().Get(cmn.URLParamCached)
	var err error
	if useCache, err = cmn.ParseBool(useCacheStr); err != nil {
		errstr = fmt.Sprintf("Invalid URL query parameter: %s=%s (expecting: '' | true | false)",
			cmn.URLParamCached, useCacheStr)
		errcode = http.StatusInternalServerError
	}
	return
}

//====================== common for both cold GET and PUT ======================================
//
// on err: closes and removes the file; otherwise closes and returns the size;
// empty omd5 or oxxhash: not considered an exception even when the configuration says otherwise;
// xxhash is always preferred over md5
//
//==============================================================================================

// NOTE: LOM is updated on the end of the call with proper size and checksum.
// NOTE: `roi.r` is closed on the end of the call.
func (roi *recvObjInfo) writeToFile() (err error) {
	var (
		file   *os.File
		reader = roi.r
	)

	if dryRun.disk {
		return
	}

	if file, err = cmn.CreateFile(roi.workFQN); err != nil {
		roi.t.fshc(err, roi.workFQN)
		return fmt.Errorf("failed to create %s, err: %s", roi.workFQN, err)
	}

	buf, slab := gmem2.AllocFromSlab2(0)
	defer func() { // free & cleanup on err
		slab.Free(buf)
		reader.Close()

		if err != nil {
			if nestedErr := file.Close(); nestedErr != nil {
				glog.Errorf("Nested (%v): failed to close received object %s, err: %v", err, roi.workFQN, nestedErr)
			}
			if nestedErr := os.Remove(roi.workFQN); nestedErr != nil {
				glog.Errorf("Nested (%v): failed to remove %s, err: %v", err, roi.workFQN, nestedErr)
			}
		}
	}()

	// receive and checksum
	var (
		written int64

		checkCksumType      string
		expectedCksum       cmn.Cksummer
		saveHash, checkHash hash.Hash
		hashes              []hash.Hash
	)

	roiCkConf := roi.lom.CksumConf()
	if !roi.cold && roiCkConf.Type != cmn.ChecksumNone {
		checkCksumType = roiCkConf.Type
		cmn.AssertMsg(checkCksumType == cmn.ChecksumXXHash, checkCksumType)

		if !roi.migrated || roiCkConf.ValidateObjMove {
			saveHash = xxhash.New64()
			hashes = []hash.Hash{saveHash}

			// if sender provided checksum we need to ensure that it is correct
			if expectedCksum = roi.cksumToCheck; expectedCksum != nil {
				checkHash = saveHash
			}
		} else {
			// if migration validation is not configured we can just take
			// the checksum that has arrived with the object (and compute it if not present)
			roi.lom.SetCksum(roi.cksumToCheck)
			if roi.cksumToCheck == nil {
				saveHash = xxhash.New64()
				hashes = []hash.Hash{saveHash}
			}
		}
	} else if roi.cold {
		// compute xxhash (the default checksum) and save it as part of the object metadata
		saveHash = xxhash.New64()
		hashes = []hash.Hash{saveHash}

		// if validate-cold-get and the cksum is provied we should also check md5 hash (aws, gcp)
		if roiCkConf.ValidateColdGet && roi.cksumToCheck != nil {
			expectedCksum = roi.cksumToCheck
			checkCksumType, _ = expectedCksum.Get()
			cmn.AssertMsg(checkCksumType == cmn.ChecksumMD5 || checkCksumType == cmn.ChecksumCRC32C, checkCksumType)

			checkHash = md5.New()
			if checkCksumType == cmn.ChecksumCRC32C {
				checkHash = cmn.NewCRC32C()
			}

			hashes = append(hashes, checkHash)
		}
	}

	if written, err = cmn.ReceiveAndChecksum(file, reader, buf, hashes...); err != nil {
		return
	}

	if checkHash != nil {
		computedCksum := cmn.NewCksum(checkCksumType, cmn.HashToStr(checkHash))
		if !cmn.EqCksum(expectedCksum, computedCksum) {
			s := cmn.BadCksum(expectedCksum, computedCksum) + ", " + roi.lom.StringEx() + "[" + roi.workFQN + "]"
			err = fmt.Errorf(s)
			roi.t.statsif.AddMany(stats.NamedVal64{stats.ErrCksumCount, 1}, stats.NamedVal64{stats.ErrCksumSize, written})
			return
		}
	}
	roi.lom.SetSize(written)
	if saveHash != nil {
		roi.lom.SetCksum(cmn.NewCksum(cmn.ChecksumXXHash, cmn.HashToStr(saveHash)))
	}
	if err = file.Close(); err != nil {
		return fmt.Errorf("failed to close received file %s, err: %v", roi.workFQN, err)
	}
	return nil
}

func (t *targetrunner) redirectLatency(started time.Time, query url.Values) (redelta int64) {
	s := query.Get(cmn.URLParamUnixTime)
	if s == "" {
		return
	}
	pts, err := strconv.ParseInt(s, 0, 64)
	if err != nil {
		glog.Errorf("Unexpected: failed to convert %s to int, err: %v", s, err)
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
	t.statsdC.Send(keyName+".io.errors", 1, metric{statsd.Counter, "count", 1})
	getfshealthchecker().OnErr(filepath)
}

func (t *targetrunner) HRWTarget(bucket, objname string) (si *cluster.Snode, errstr string) {
	return hrwTarget(bucket, objname, t.smapowner.get())
}

func (t *targetrunner) Snode() *cluster.Snode {
	return t.si
}

func getFromOtherLocalFS(lom *cluster.LOM) (fqn string, size int64) {
	availablePaths, _ := fs.Mountpaths.Get()
	for _, mpathInfo := range availablePaths {
		filePath := mpathInfo.MakePathBucketObject(fs.ObjectType, lom.Bucket, lom.Objname, lom.BckIsLocal)
		stat, err := os.Stat(filePath)
		if err == nil {
			return filePath, stat.Size()
		}
	}
	return
}
