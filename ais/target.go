// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
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
	"github.com/karrick/godirwalk"
)

const (
	maxPageSize     = 64 * 1024     // max number of objects in a page (warn when req. size exceeds this limit)
	maxBytesInMem   = 256 * cmn.KiB // objects with smaller size than this will be read to memory when checksumming
	maxBMDXattrSize = 128 * 1024

	// GET-from-neighbors tunables
	getFromNeighRetries   = 10
	getFromNeighSleep     = 300 * time.Millisecond
	getFromNeighAfterJoin = time.Second * 30

	rebalanceStreamName = "rebalance"

	bucketMDFixup    = "fixup"
	bucketMDReceive  = "receive"
	bucketMDRegister = "register"
)

type (
	allfinfos struct {
		t            *targetrunner
		files        []*cmn.BucketEntry
		prefix       string
		marker       string
		markerDir    string
		msg          *cmn.SelectMsg
		lastFilePath string
		bucket       string
		fileCount    int
		rootLength   int
		limit        int
		needAtime    bool
		needCtime    bool
		needChkSum   bool
		needVersion  bool
		needStatus   bool
		needCopies   bool
	}
	uxprocess struct {
		starttime time.Time
		spid      string
		pid       int64
	}
	thealthstatus struct {
		IsRebalancing bool `json:"is_rebalancing"`
		// NOTE: include core stats and other info as needed
	}
	renamectx struct {
		bucketFrom string
		bucketTo   string
		t          *targetrunner
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
		rtnamemap      *rtnamemap
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

	t.rtnamemap = newrtnamemap()

	bucketmd := newBucketMD()
	t.bmdowner.put(bucketmd)

	smap := newSmap()
	smap.Tmap[t.si.DaemonID] = t.si
	t.smapowner.put(smap)

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
		cmn.ExitLogf("%s", err)
	}
	if err := fs.CSM.RegisterFileType(fs.WorkfileType, &fs.WorkfileContentResolver{}); err != nil {
		cmn.ExitLogf("%s", err)
	}

	if err := fs.Mountpaths.CreateBucketDir(cmn.LocalBs); err != nil {
		cmn.ExitLogf("%s", err)
	}
	if err := fs.Mountpaths.CreateBucketDir(cmn.CloudBs); err != nil {
		cmn.ExitLogf("%s", err)
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

	if err := t.setupRebalanceManager(); err != nil {
		cmn.ExitLogf("%s", err)
	}

	pid := int64(os.Getpid())
	t.uxprocess = &uxprocess{time.Now(), strconv.FormatInt(pid, 16), pid}

	getfshealthchecker().SetDispatcher(t)

	ec.Init()
	t.ecmanager = newECM(t)

	aborted, _ := t.xactions.localRebStatus()
	if aborted {
		go func() {
			glog.Infoln("resuming local rebalance...")
			t.rebManager.runLocalReb()
		}()
	}

	dsort.RegisterNode(t.smapowner, t.bmdowner, t.si, t, t.rtnamemap)
	if err := t.httprunner.run(); err != nil {
		return err
	}
	glog.Infof("%s is ready to handle requests", t.si.Name())
	glog.Flush()
	return nil
}

func (t *targetrunner) setupRebalanceManager() error {
	var (
		network = cmn.NetworkIntraData
	)

	// fallback to network public if intra data is not available
	if !cmn.GCO.Get().Net.UseIntraData {
		network = cmn.NetworkPublic
	}

	t.rebManager = &rebManager{
		t:           t,
		objectsSent: filter.NewDefaultFilter(),
	}

	if _, err := transport.Register(network, "rebalance", t.rebManager.recvRebalanceObj); err != nil {
		return err
	}

	client := transport.NewDefaultClient()

	// TODO: stream bundle multiplier (currently default) should be adjustable at runtime (#253)
	sbArgs := transport.SBArgs{
		ManualResync: true,
		Multiplier:   4,
		Network:      network,
		Trname:       rebalanceStreamName,
	}

	t.rebManager.streams = transport.NewStreamBundle(t.smapowner, t.si, client, sbArgs)
	return nil
}

// target-only stats
func (t *targetrunner) registerStats() {
	t.statsif.Register(stats.PutLatency, stats.KindLatency)
	t.statsif.Register(stats.GetColdCount, stats.KindCounter)
	t.statsif.Register(stats.GetColdSize, stats.KindCounter)
	t.statsif.Register(stats.GetThroughput, stats.KindThroughput)
	t.statsif.Register(stats.LruEvictSize, stats.KindCounter)
	t.statsif.Register(stats.LruEvictCount, stats.KindCounter)
	t.statsif.Register(stats.TxCount, stats.KindCounter)
	t.statsif.Register(stats.TxSize, stats.KindCounter)
	t.statsif.Register(stats.RxCount, stats.KindCounter)
	t.statsif.Register(stats.RxSize, stats.KindCounter)
	t.statsif.Register(stats.PrefetchCount, stats.KindCounter)
	t.statsif.Register(stats.PrefetchSize, stats.KindCounter)
	t.statsif.Register(stats.VerChangeCount, stats.KindCounter)
	t.statsif.Register(stats.VerChangeSize, stats.KindCounter)
	t.statsif.Register(stats.ErrCksumCount, stats.KindCounter)
	t.statsif.Register(stats.ErrCksumSize, stats.KindCounter)
	t.statsif.Register(stats.ErrMetadataCount, stats.KindCounter)
	t.statsif.Register(stats.GetRedirLatency, stats.KindLatency)
	t.statsif.Register(stats.PutRedirLatency, stats.KindLatency)
	// rebalance
	t.statsif.Register(stats.RebGlobalCount, stats.KindCounter)
	t.statsif.Register(stats.RebLocalCount, stats.KindCounter)
	t.statsif.Register(stats.RebGlobalSize, stats.KindCounter)
	t.statsif.Register(stats.RebLocalSize, stats.KindCounter)
	// replication
	t.statsif.Register(stats.ReplPutCount, stats.KindCounter)
	t.statsif.Register(stats.ReplPutLatency, stats.KindLatency)
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
	_, running := t.xactions.globalRebStatus()
	_, runningLocal := t.xactions.localRebStatus()
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
		Namelocker:          t.rtnamemap,
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
	xpre := t.xactions.renewPrefetch()

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
	started = time.Now()
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	bckProvider := query.Get(cmn.URLParamBckProvider)
	if _, ok := t.validateBucket(w, r, bucket, bckProvider); !ok {
		return
	}
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

	// 2. under lock: lom init, restore from cluster
	t.rtnamemap.Lock(lom.Uname(), false)
do:
	// all the next checks work with disks - skip all if dryRun.disk=true
	coldGet := false
	if dryRun.disk {
		goto get
	}

	fromCache, errstr = lom.Load(true)
	if errstr != "" {
		t.rtnamemap.Unlock(lom.Uname(), false)
		t.invalmsghdlr(w, r, errstr)
		return
	}

	coldGet = !lom.Exists()
	if coldGet && lom.BckIsLocal {
		// does not exist in the local bucket: restore from neighbors
		if errstr, errcode = t.restoreObjLBNeigh(lom, r); errstr != "" {
			t.rtnamemap.Unlock(lom.Uname(), false)
			t.invalmsghdlr(w, r, errstr, errcode)
			return
		}
		goto get
	}
	if !coldGet && !lom.BckIsLocal { // exists && cloud-bucket : check ver if requested
		if lom.Version() != "" && lom.VerConf().ValidateWarmGet {
			if coldGet, errstr, errcode = t.checkCloudVersion(ct, lom); errstr != "" {
				lom.Uncache()
				t.rtnamemap.Unlock(lom.Uname(), false)
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
					t.rtnamemap.Unlock(lom.Uname(), false)
					t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
					return
				}
				coldGet = true
			} else {
				t.rtnamemap.Unlock(lom.Uname(), false)
				t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
				return
			}
		}
	}

	// 3. coldget
	if coldGet {
		t.rtnamemap.Unlock(lom.Uname(), false)
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
	t.rtnamemap.Unlock(lom.Uname(), false)
}

//
// 3a. attempt to restore an object that is missing in the LOCAL BUCKET - from:
//     1) local FS, 2) this cluster, 3) other tiers in the DC 4) from other
//		targets using erasure coding(if it is on)
// FIXME: must be done => (getfqn, and under write lock)
//
func (t *targetrunner) restoreObjLBNeigh(lom *cluster.LOM, r *http.Request) (errstr string, errcode int) {
	// check FS-wide if local rebalance is running
	aborted, running := t.xactions.localRebStatus()
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
	aborted, running = t.xactions.globalRebStatus()
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
		hdr.Add(cmn.HeaderObjCksumType, cksumType)
		hdr.Add(cmn.HeaderObjCksumVal, cksumValue)
	}
	if lom.Version() != "" {
		hdr.Add(cmn.HeaderObjVersion, lom.Version())
	}
	hdr.Add(cmn.HeaderObjSize, strconv.FormatInt(lom.Size(), 10))

	timeInt := lom.Atime().UnixNano()
	if lom.Atime().IsZero() {
		timeInt = 0
	}
	hdr.Add(cmn.HeaderObjAtime, strconv.FormatInt(timeInt, 10))

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
			hdr.Add(cmn.HeaderObjCksumType, ckConf.Type)
			hdr.Add(cmn.HeaderObjCksumVal, cksum)
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
	}

	// Update objects which were sent during GFN. Thanks to this we will not
	// have to resend them in global rebalance. In case of race between rebalance
	// and GFN, the former wins and it will result in double send.
	if isGFNRequest {
		t.rebManager.objectsSent.Insert([]byte(lom.Uname()))
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
	if _, ok := t.validateBucket(w, r, bucket, bckProvider); !ok {
		return
	}
	if redelta := t.redirectLatency(time.Now(), query); redelta != 0 {
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
	if lom.BckIsLocal && lom.VerConf().Enabled {
		lom.Load(true) // need to know the current version if versionig enabled
	}
	if err, errCode := t.doPut(r, lom); err != nil {
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
	bckIsLocal, ok := t.validateBucket(w, r, bucket, bckProvider)
	if !ok {
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
				bmd := t.bmdowner.get()
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
	if _, ok := t.validateBucket(w, r, bucket, bckProvider); !ok {
		return
	}

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
		started        = time.Now()
		msgInt         actionMsgInternal
		bckIsLocal, ok bool
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
	bckIsLocal, ok = t.validateBucket(w, r, bucket, bckProvider)
	if !ok {
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

		bmd := t.bmdowner.get()
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

		t.bmdowner.Lock() // lock#2 begin
		clone = t.bmdowner.get().clone()
		clone.del(bucketFrom, true) // TODO: resolve conflicts
		t.bmdowner.put(clone)
		t.bmdowner.Unlock()

		glog.Infof("renamed bucket %s => %s, %s v%d", bucketFrom, bucketTo, bmdTermName, clone.version())
	case cmn.ActListObjects:
		// list the bucket and return
		tag, ok := t.listbucket(w, r, bucket, bckIsLocal, &msgInt)
		if ok {
			delta := time.Since(started)
			t.statsif.AddMany(stats.NamedVal64{stats.ListCount, 1}, stats.NamedVal64{stats.ListLatency, int64(delta)})
			if glog.FastV(4, glog.SmoduleAIS) {
				bmd := t.bmdowner.get()
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
		bckIsLocal := t.bmdowner.get().IsLocal(bucket)
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
		t.renameObject(w, r, msg)
	default:
		t.invalmsghdlr(w, r, "Unexpected action "+msg.Action)
	}
}

// HEAD /v1/buckets/bucket-name
func (t *targetrunner) httpbckhead(w http.ResponseWriter, r *http.Request) {
	var (
		bucketProps    cmn.SimpleKVs
		errCode        int
		bckIsLocal, ok bool
		query          = r.URL.Query()
		bucketmd       = t.bmdowner.get()
	)

	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	if bckIsLocal, ok = t.validateBucket(w, r, bucket, bckProvider); !ok {
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
			errMsg := fmt.Sprintf("the bucket %s either %s or is not accessible, err: %v", bucket, cmn.DoesNotExist, err)
			t.invalmsghdlr(w, r, errMsg, errCode)
			return
		}
	} else {
		bucketProps = make(cmn.SimpleKVs)
		bucketProps[cmn.HeaderCloudProvider] = cmn.ProviderAIS
	}

	hdr := w.Header()
	for k, v := range bucketProps {
		hdr.Add(k, v)
	}

	// include bucket's own config override
	props, ok := bucketmd.Get(bucket, bckIsLocal)
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
	// transfer bucket props via http header;
	// (it is totally legal for Cloud buckets to not have locally cached props)
	hdr.Add(cmn.HeaderReadPolicy, props.ReadPolicy)
	hdr.Add(cmn.HeaderWritePolicy, props.WritePolicy)
	hdr.Add(cmn.HeaderBucketChecksumType, cksumConf.Type)
	hdr.Add(cmn.HeaderBucketValidateColdGet, strconv.FormatBool(cksumConf.ValidateColdGet))
	hdr.Add(cmn.HeaderBucketValidateWarmGet, strconv.FormatBool(cksumConf.ValidateWarmGet))
	hdr.Add(cmn.HeaderBucketValidateRange, strconv.FormatBool(cksumConf.EnableReadRange))

	hdr.Add(cmn.HeaderBucketVerEnabled, strconv.FormatBool(verConf.Enabled))
	hdr.Add(cmn.HeaderBucketVerValidateWarm, strconv.FormatBool(verConf.ValidateWarmGet))

	hdr.Add(cmn.HeaderBucketLRULowWM, strconv.FormatInt(props.LRU.LowWM, 10))
	hdr.Add(cmn.HeaderBucketLRUHighWM, strconv.FormatInt(props.LRU.HighWM, 10))
	hdr.Add(cmn.HeaderBucketDontEvictTime, props.LRU.DontEvictTimeStr)
	hdr.Add(cmn.HeaderBucketCapUpdTime, props.LRU.CapacityUpdTimeStr)
	hdr.Add(cmn.HeaderBucketMirrorEnabled, strconv.FormatBool(props.Mirror.Enabled))
	hdr.Add(cmn.HeaderBucketMirrorThresh, strconv.FormatInt(props.Mirror.UtilThresh, 10))
	hdr.Add(cmn.HeaderBucketLRUEnabled, strconv.FormatBool(props.LRU.Enabled))
	if props.Mirror.Enabled {
		hdr.Add(cmn.HeaderBucketCopies, strconv.FormatInt(props.Mirror.Copies, 10))
	} else {
		hdr.Add(cmn.HeaderBucketCopies, "0")
	}
	hdr.Add(cmn.HeaderRebalanceEnabled, strconv.FormatBool(props.Rebalance.Enabled))

	hdr.Add(cmn.HeaderBucketECEnabled, strconv.FormatBool(props.EC.Enabled))
	hdr.Add(cmn.HeaderBucketECMinSize, strconv.FormatUint(uint64(props.EC.ObjSizeLimit), 10))
	hdr.Add(cmn.HeaderBucketECData, strconv.FormatUint(uint64(props.EC.DataSlices), 10))
	hdr.Add(cmn.HeaderBucketECParity, strconv.FormatUint(uint64(props.EC.ParitySlices), 10))
}

// HEAD /v1/objects/bucket-name/object-name
func (t *targetrunner) httpobjhead(w http.ResponseWriter, r *http.Request) {
	var (
		bucket, objname, errstr string
		checkCached             bool
		errcode                 int
		objmeta                 cmn.SimpleKVs
	)
	query := r.URL.Query()
	checkCached, _ = cmn.ParseBool(query.Get(cmn.URLParamCheckCached))
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname = apitems[0], apitems[1]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	if _, ok := t.validateBucket(w, r, bucket, bckProvider); !ok {
		return
	}
	lom, errstr := cluster.LOM{T: t, Bucket: bucket, Objname: objname, BucketProvider: bckProvider}.Init()
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}
	if _, errstr = lom.Load(true); errstr != "" { // (doesnotexist -> ok, other)
		t.invalmsghdlr(w, r, errstr)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, lom.StringEx(), pid)
	}
	if lom.BckIsLocal || checkCached {
		if !lom.Exists() {
			status := http.StatusNotFound
			http.Error(w, http.StatusText(status), status)
			return
		} else if checkCached {
			return
		}
		objmeta = make(cmn.SimpleKVs)
		objmeta[cmn.HeaderObjSize] = strconv.FormatInt(lom.Size(), 10)
		objmeta[cmn.HeaderObjVersion] = lom.Version()
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s(%s), ver=%s", lom, cmn.B2S(lom.Size(), 1), lom.Version())
		}
	} else {
		objmeta, err, errcode = getcloudif().headobject(t.contextWithAuth(r.Header), lom)
		if err != nil {
			errMsg := fmt.Sprintf("%s: failed to head metadata, err: %v", lom, err)
			t.invalmsghdlr(w, r, errMsg, errcode)
			return
		}
	}
	hdr := w.Header()
	for k, v := range objmeta {
		hdr.Add(k, v)
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
				ch <- t.renameOne(fromDir, bucketFrom, bucketTo)
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
	fs.Mountpaths.CreateDestroyLocalBuckets("rename", false /*false=destroy*/, bucketFrom)
	return
}

func (t *targetrunner) renameOne(fromdir, bucketFrom, bucketTo string) (errstr string) {
	renctx := &renamectx{bucketFrom: bucketFrom, bucketTo: bucketTo, t: t}

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
	if errstr := renctx.t.renameBucketObject(contentType, bucket, objname, renctx.bucketTo, objname); errstr != "" {
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

	// FIXME: For now, need to re-translate lom.BckIsLocal to appropriate value ("local"|"cloud")
	// FIXME: this code below looks like a general code for sending request
	geturl := fmt.Sprintf("%s%s?%s=%s&%s=%s",
		neighsi.PublicNet.DirectURL, r.URL.Path, cmn.URLParamBckProvider, lom.BucketProvider, cmn.URLParamIsGFNRequest, "true")
	//
	// http request
	//
	newr, err := http.NewRequest(http.MethodGet, geturl, nil)
	if err != nil {
		err = fmt.Errorf("unexpected failure to create %s request %s, err: %v", http.MethodGet, geturl, err)
		return
	}

	// Do
	contextwith, cancel := context.WithTimeout(context.Background(), lom.Config().Timeout.SendFile)
	defer cancel()
	newrequest := newr.WithContext(contextwith)

	response, err := t.httpclientLongTimeout.Do(newrequest)
	if err != nil {
		err = fmt.Errorf("failed to GET redirect URL %q, err: %v", geturl, err)
		return
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
		if !t.rtnamemap.TryLock(lom.Uname(), true) {
			glog.Infof("prefetch: cold GET race: %s - skipping", lom)
			return "skip", 0
		}
	} else {
		t.rtnamemap.Lock(lom.Uname(), true) // one cold-GET at a time
	}
	var (
		err             error
		vchanged, crace bool
		workFQN         = fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfileColdget)
	)
	if err, errcode = getcloudif().getobj(ct, workFQN, lom); err != nil {
		errstr = fmt.Sprintf("%s: GET failed, err: %v", lom, err)
		t.rtnamemap.Unlock(lom.Uname(), true)
		return
	}
	defer func() {
		if errstr != "" {
			t.rtnamemap.Unlock(lom.Uname(), true)
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
		t.rtnamemap.Unlock(lom.Uname(), true)
	} else {
		if vchanged {
			t.statsif.AddMany(stats.NamedVal64{stats.GetColdCount, 1},
				stats.NamedVal64{stats.GetColdSize, lom.Size()},
				stats.NamedVal64{stats.VerChangeSize, lom.Size()},
				stats.NamedVal64{stats.VerChangeCount, 1})
		} else if !crace {
			t.statsif.AddMany(stats.NamedVal64{stats.GetColdCount, 1}, stats.NamedVal64{stats.GetColdSize, lom.Size()})
		}
		t.rtnamemap.DowngradeLock(lom.Uname())
	}
	return
}

func (t *targetrunner) lookupRemotely(lom *cluster.LOM, smap *smapX) *cluster.Snode {
	res := t.broadcastTo(
		cmn.URLPath(cmn.Version, cmn.Objects, lom.Bucket, lom.Objname),
		nil, // query
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

// should not be called for local buckets
func (t *targetrunner) listCachedObjects(bucket string, msg *cmn.SelectMsg,
	bckIsLocal bool) (outbytes []byte, errstr string) {
	reslist, err := t.prepareLocalObjectList(bucket, msg, bckIsLocal)
	if err != nil {
		return nil, err.Error()
	}

	outbytes, err = jsoniter.Marshal(reslist)
	if err != nil {
		return nil, err.Error()
	}
	return
}

func (t *targetrunner) prepareLocalObjectList(bucket string, msg *cmn.SelectMsg,
	bckIsLocal bool) (*cmn.BucketList, error) {
	type mresp struct {
		infos      *allfinfos
		failedPath string
		err        error
	}

	availablePaths, _ := fs.Mountpaths.Get()
	ch := make(chan *mresp, len(fs.CSM.RegisteredContentTypes)*len(availablePaths))
	wg := &sync.WaitGroup{}

	// function to traverse one mountpoint
	walkMpath := func(dir string) {
		r := &mresp{t.newFileWalk(bucket, msg), "", nil}
		if msg.Fast {
			r.infos.limit = 1<<63 - 1 // return all objects in one response
		}
		if _, err := os.Stat(dir); err != nil {
			if !os.IsNotExist(err) {
				r.failedPath = dir
				r.err = err
			}
			ch <- r // not an error, just skip the path
			wg.Done()
			return
		}
		r.infos.rootLength = len(dir) + 1 // +1 for separator between bucket and filename
		if msg.Fast {
			// return all object names and sizes (and only names and sizes)
			err := godirwalk.Walk(dir, &godirwalk.Options{
				Callback: r.infos.listwalkfFast,
				Unsorted: true,
			})
			if err != nil {
				glog.Errorf("Failed to traverse path %q, err: %v", dir, err)
				r.failedPath = dir
				r.err = err
			}
		} else if err := filepath.Walk(dir, r.infos.listwalkf); err != nil {
			glog.Errorf("Failed to traverse path %q, err: %v", dir, err)
			r.failedPath = dir
			r.err = err
		}
		ch <- r
		wg.Done()
	}

	// Traverse all mountpoints in parallel.
	// If any mountpoint traversing fails others keep running until they complete.
	// But in this case all collected data is thrown away because the partial result
	// makes paging inconsistent
	for contentType, contentResolver := range fs.CSM.RegisteredContentTypes {
		if !contentResolver.PermToProcess() {
			continue
		}
		for _, mpathInfo := range availablePaths {
			wg.Add(1)
			dir := mpathInfo.MakePathBucket(contentType, bucket, bckIsLocal)
			go walkMpath(dir)
		}
	}
	wg.Wait()
	close(ch)

	// combine results into one long list
	// real size of page is set in newFileWalk, so read it from any of results inside loop
	pageSize := cmn.DefaultPageSize
	bckEntries := make([]*cmn.BucketEntry, 0)
	fileCount := 0
	for r := range ch {
		if r.err != nil {
			if !os.IsNotExist(r.err) {
				t.fshc(r.err, r.failedPath)
				return nil, fmt.Errorf("failed to read %s", r.failedPath)
			}
			continue
		}

		pageSize = r.infos.limit
		bckEntries = append(bckEntries, r.infos.files...)
		fileCount += r.infos.fileCount
	}

	// determine whether the fast-listed bucket is /cold/, and load lom cache if it is
	if msg.Fast {
		const minloaded = 10 // check that many randomly-selected
		if fileCount > minloaded {
			go func(bckEntries []*cmn.BucketEntry) {
				var (
					bckProvider = cmn.BckProviderFromLocal(bckIsLocal)
					l           = len(bckEntries)
					m           = l / minloaded
					loaded      int
				)
				if l < minloaded {
					return
				}
				for i := 0; i < l; i += m {
					lom, errstr := cluster.LOM{T: t, Bucket: bucket,
						Objname: bckEntries[i].Name, BucketProvider: bckProvider}.Init()
					if errstr == "" && lom.IsLoaded() { // loaded?
						loaded++
					}
				}
				renew := loaded < minloaded/2
				if glog.FastV(4, glog.SmoduleAIS) {
					glog.Errorf("%s: loaded %d/%d, renew=%t", t.si, loaded, minloaded, renew)
				}
				if renew {
					t.xactions.renewBckLoadLomCache(bucket, t, bckIsLocal)
				}
			}(bckEntries)
		}
	}

	marker := ""
	// - sort the result but only if the set is greater than page size;
	// - return the page size
	// - do not sort fast list (see Unsorted above)
	if !msg.Fast && fileCount > pageSize {
		ifLess := func(i, j int) bool {
			return bckEntries[i].Name < bckEntries[j].Name
		}
		sort.Slice(bckEntries, ifLess)
		// set extra infos to nil to avoid memory leaks
		// see NOTE on https://github.com/golang/go/wiki/SliceTricks
		for i := pageSize; i < fileCount; i++ {
			bckEntries[i] = nil
		}
		bckEntries = bckEntries[:pageSize]
		marker = bckEntries[pageSize-1].Name
	}

	bucketList := &cmn.BucketList{
		Entries:    bckEntries,
		PageMarker: marker,
	}

	if strings.Contains(msg.Props, cmn.GetTargetURL) {
		for _, e := range bucketList.Entries {
			e.TargetURL = t.si.PublicNet.DirectURL
		}
	}

	return bucketList, nil
}

func (t *targetrunner) bucketsFromXattr(w http.ResponseWriter, r *http.Request) {
	bmdXattr := &bucketMD{}
	slab, err := gmem2.GetSlab2(maxBMDXattrSize)
	if err != nil {
		t.invalmsghdlr(w, r, "Failed to read BMD from xattrs: "+err.Error())
		return
	}

	buf := slab.Alloc()
	if err := bmdXattr.Load(buf); err != nil {
		slab.Free(buf)
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	slab.Free(buf)

	jsbytes, err := jsoniter.Marshal(bmdXattr)
	cmn.AssertNoErr(err)
	t.writeJSON(w, r, jsbytes, "getbucketsxattr")
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

	jsbytes, err := jsoniter.Marshal(bucketNames)
	cmn.AssertNoErr(err)
	t.writeJSON(w, r, jsbytes, "getbucketnames")
}

func (t *targetrunner) doLocalBucketList(w http.ResponseWriter, r *http.Request,
	bucket string, msg *cmn.SelectMsg) (errstr string, ok bool) {
	reslist, err := t.prepareLocalObjectList(bucket, msg, true)
	if err != nil {
		errstr = fmt.Sprintf("List local bucket %s failed, err: %v", bucket, err)
		return
	}
	jsbytes, err := jsoniter.Marshal(reslist)
	cmn.AssertNoErr(err)
	ok = t.writeJSON(w, r, jsbytes, "listbucket")
	return
}

// List bucket returns a list of objects in a bucket (with optional prefix)
// Special case:
// If URL contains cachedonly=true then the function returns the list of
// locally cached objects. Paging is used to return a long list of objects
func (t *targetrunner) listbucket(w http.ResponseWriter, r *http.Request, bucket string,
	bckIsLocal bool, actionMsg *actionMsgInternal) (tag string, ok bool) {
	var (
		jsbytes []byte
		errstr  string
		errcode int
	)
	query := r.URL.Query()
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.URLParamProxyID)
		bmd := t.bmdowner.get()
		glog.Infof("%s %s <= (%s)", r.Method, bmd.Bstring(bucket, bckIsLocal), pid)
	}
	useCache, errstr, errcode := t.checkCacheQueryParameter(r)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}

	getMsgJSON, err := jsoniter.Marshal(actionMsg.Value)
	if err != nil {
		errstr := fmt.Sprintf("Unable to marshal 'value' in request: %v", actionMsg.Value)
		t.invalmsghdlr(w, r, errstr)
		return
	}

	var msg cmn.SelectMsg
	err = jsoniter.Unmarshal(getMsgJSON, &msg)
	if err != nil {
		errstr := fmt.Sprintf("Unable to unmarshal 'value' in request to a cmn.SelectMsg: %v", actionMsg.Value)
		t.invalmsghdlr(w, r, errstr)
		return
	}
	if bckIsLocal {
		tag = "local"
		if errstr, ok = t.doLocalBucketList(w, r, bucket, &msg); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
		}
		return // ======================================>
	}
	// cloud bucket
	msg.Fast = false // fast mode does not apply to Cloud buckets
	if useCache {
		tag = "cloud cached"
		jsbytes, errstr = t.listCachedObjects(bucket, &msg, false /* local */)
	} else {
		tag = "cloud"
		jsbytes, err, errcode = getcloudif().listbucket(t.contextWithAuth(r.Header), bucket, &msg)
		if err != nil {
			errstr = fmt.Sprintf("error listing cloud bucket %s: %d(%v)", bucket, errcode, err)
		}
	}
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}
	ok = t.writeJSON(w, r, jsbytes, "listbucket")
	return
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

// Checks if the directory should be processed by cache list call
// Does checks:
//  - Object name must start with prefix (if it is set)
//  - Object name is not in early processed directories by the previous call:
//    paging support
func (ci *allfinfos) processDir(fqn string) error {
	if len(fqn) <= ci.rootLength {
		return nil
	}

	// every directory has to either:
	// - start with prefix (for levels higher than prefix: prefix="ab", directory="abcd/def")
	// - or include prefix (for levels deeper than prefix: prefix="a/", directory="a/b/")
	relname := fqn[ci.rootLength:]
	if ci.prefix != "" && !strings.HasPrefix(ci.prefix, relname) && !strings.HasPrefix(relname, ci.prefix) {
		return filepath.SkipDir
	}

	// When markerDir = "b/c/d/" we should skip directories: "a/", "b/a/",
	// "b/b/" etc. but should not skip entire "b/" or "b/c/" since it is our
	// parent which we want to traverse (see that: "b/" < "b/c/d/").
	if ci.markerDir != "" && relname < ci.markerDir && !strings.HasPrefix(ci.markerDir, relname) {
		return filepath.SkipDir
	}

	return nil
}

// Adds an info about cached object to the list if:
//  - its name starts with prefix (if prefix is set)
//  - it has not been already returned by previous page request
//  - this target responses getobj request for the object
func (ci *allfinfos) lsObject(lom *cluster.LOM, osfi os.FileInfo, objStatus string) error {
	relname := lom.FQN[ci.rootLength:]
	if ci.prefix != "" && !strings.HasPrefix(relname, ci.prefix) {
		return nil
	}
	if ci.marker != "" && relname <= ci.marker {
		return nil
	}
	// add the obj to the page
	ci.fileCount++
	fileInfo := &cmn.BucketEntry{
		Name:     relname,
		Atime:    "",
		IsCached: true,
		Status:   objStatus,
		Copies:   1,
	}
	_, _ = lom.Load(true) // FIXME: handle errors
	if ci.needAtime {
		fileInfo.Atime = cmn.FormatTime(lom.Atime(), ci.msg.TimeFormat)
	}
	if ci.needCtime {
		fileInfo.Ctime = cmn.FormatTime(osfi.ModTime(), ci.msg.TimeFormat)
	}
	if ci.needChkSum && lom.Cksum() != nil {
		_, storedCksum := lom.Cksum().Get()
		fileInfo.Checksum = hex.EncodeToString([]byte(storedCksum))
	}
	if ci.needVersion {
		fileInfo.Version = lom.Version()
	}
	if ci.needCopies {
		fileInfo.Copies = int16(lom.NumCopies())
	}
	fileInfo.Size = osfi.Size()
	ci.files = append(ci.files, fileInfo)
	ci.lastFilePath = lom.FQN
	return nil
}

// fast alternative of generic listwalk: do not fetch any object information
// Always returns all objects - no paging required. But the result may have
// 'ghost' or duplicated  objects.
// The only supported SelectMsg feature is 'Prefix' - it does not slow down.
func (ci *allfinfos) listwalkfFast(fqn string, de *godirwalk.Dirent) error {
	if de.IsDir() {
		return ci.processDir(fqn)
	}

	relname := fqn[ci.rootLength:]
	if ci.prefix != "" && !strings.HasPrefix(relname, ci.prefix) {
		return nil
	}
	ci.fileCount++
	fileInfo := &cmn.BucketEntry{
		Name:   relname,
		Status: cmn.ObjStatusOK,
	}
	ci.files = append(ci.files, fileInfo)
	return nil
}

func (ci *allfinfos) listwalkf(fqn string, osfi os.FileInfo, err error) error {
	if err != nil {
		if errstr := cmn.PathWalkErr(err); errstr != "" {
			glog.Errorf(errstr)
			return err
		}
		return nil
	}
	if ci.fileCount >= ci.limit {
		return filepath.SkipDir
	}
	if osfi.IsDir() {
		return ci.processDir(fqn)
	}
	// FIXME: check the logic vs local/global rebalance
	var (
		objStatus = cmn.ObjStatusOK
	)
	lom, errstr := cluster.LOM{T: ci.t, FQN: fqn}.Init()
	if errstr != "" {
		glog.Errorf("%s: %s", lom, errstr) // proceed to list this object anyway
	}
	_, errstr = lom.Load(true)
	if !lom.Exists() {
		return nil
	}
	if lom.IsCopy() {
		return nil
	}
	if lom.Misplaced() {
		objStatus = cmn.ObjStatusMoved
	} else {
		if errstr != "" {
			glog.Errorf("%s: %s", lom, errstr) // proceed to list this object anyway
		}
		si, errstr := hrwTarget(lom.Bucket, lom.Objname, ci.t.smapowner.get())
		if errstr != "" {
			glog.Errorf("%s: %s", lom, errstr)
		}
		if ci.t.si.DaemonID != si.DaemonID {
			objStatus = cmn.ObjStatusMoved
		}
	}
	return ci.lsObject(lom, osfi, objStatus)
}

// After putting a new version it updates xattr attributes for the object
// Local bucket:
//  - if bucket versioning is enable("all" or "local") then the version is autoincremented
// Cloud bucket:
//  - if the Cloud returns a new version id then save it to xattr
// In both case a new checksum is saved to xattrs
// compare with t.Receive()
func (t *targetrunner) doPut(r *http.Request, lom *cluster.LOM) (err error, errcode int) {
	var (
		header     = r.Header
		cksumType  = header.Get(cmn.HeaderObjCksumType)
		cksumValue = header.Get(cmn.HeaderObjCksumVal)
		cksum      = cmn.NewCksum(cksumType, cksumValue)
	)
	roi := &recvObjInfo{
		t:            t,
		lom:          lom,
		r:            r.Body,
		cksumToCheck: cksum,
		ctx:          t.contextWithAuth(header),
	}
	roi.init()
	return roi.recv()
}

// slight variation vs t.doPut() above
func (t *targetrunner) Receive(workFQN string, reader io.ReadCloser, lom *cluster.LOM, recvType cluster.RecvType, cksum cmn.Cksummer) error {
	roi := &recvObjInfo{
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

func (roi *recvObjInfo) init() {
	roi.started = time.Now()
	roi.workFQN = fs.CSM.GenContentParsedFQN(roi.lom.ParsedFQN, fs.WorkfileType, fs.WorkfilePut)
	cmn.Assert(roi.lom != nil)
	cmn.Assert(!roi.migrated || roi.cksumToCheck != nil)
}

func (roi *recvObjInfo) recv() (err error, errCode int) {
	cmn.Assert(roi.lom != nil)
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
		if err = roi.writeToFile(); err != nil {
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

	roi.t.rtnamemap.Lock(lom.Uname(), true)
	defer roi.t.rtnamemap.Unlock(lom.Uname(), true)

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
		t.xputlrep = t.xactions.renewPutLocReplicas(lom, t.rtnamemap)
	}
	if t.xputlrep == nil {
		return
	}
	err := t.xputlrep.Repl(lom)
	// retry upon race vs (just finished/timedout)
	if _, ok := err.(*cmn.ErrXpired); ok {
		t.xputlrep = t.xactions.renewPutLocReplicas(lom, t.rtnamemap)
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

	t.rtnamemap.Lock(lom.Uname(), true)
	defer t.rtnamemap.Unlock(lom.Uname(), true)

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
		// Don't persis meta as object will be soon removed anyway
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

func (t *targetrunner) renameObject(w http.ResponseWriter, r *http.Request, msg cmn.ActionMsg) {
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objnameFrom := apitems[0], apitems[1]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	if _, ok := t.validateBucket(w, r, bucket, bckProvider); !ok {
		return
	}

	objnameTo := msg.Name
	uname := cluster.Bo2Uname(bucket, objnameFrom)
	t.rtnamemap.Lock(uname, true)

	if errstr := t.renameBucketObject(fs.ObjectType, bucket, objnameFrom, bucket, objnameTo); errstr != "" {
		t.invalmsghdlr(w, r, errstr)
	}
	t.rtnamemap.Unlock(uname, true)
}

func (t *targetrunner) renameBucketObject(contentType, bucketFrom, objnameFrom, bucketTo,
	objnameTo string) (errstr string) {
	var (
		fi                    os.FileInfo
		file                  *cmn.FileHandle
		si                    *cluster.Snode
		newFQN                string
		cksumType, cksumValue string
		err                   error
	)
	if si, errstr = hrwTarget(bucketTo, objnameTo, t.smapowner.get()); errstr != "" {
		return
	}
	bucketmd := t.bmdowner.get()
	bckIsLocalFrom := bucketmd.IsLocal(bucketFrom)
	fqn, _, errstr := cluster.HrwFQN(contentType, bucketFrom, objnameFrom, bckIsLocalFrom)
	if errstr != "" {
		return
	}
	if fi, err = os.Stat(fqn); err != nil {
		errstr = fmt.Sprintf("failed to fstat %s (%s/%s), err: %v", fqn, bucketFrom, objnameFrom, err)
		return
	}
	// local rename
	if si.DaemonID == t.si.DaemonID {
		bckIsLocalTo := bucketmd.IsLocal(bucketTo)
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

	// TODO: fill and send should be general function in `rebManager`: from, to, object
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
	hdr := transport.Header{
		Bucket:  bucketTo,
		Objname: objnameTo,
		IsLocal: lom.BckIsLocal,
		Opaque:  []byte(t.si.DaemonID),
		ObjAttrs: transport.ObjectAttrs{
			Size:       fi.Size(),
			Atime:      lom.Atime().UnixNano(),
			CksumType:  cksumType,
			CksumValue: cksumValue,
			Version:    lom.Version(),
		},
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	cb := func(hdr transport.Header, r io.ReadCloser, cbErr error) {
		err = cbErr
		wg.Done()
	}
	t.rebManager.streams.Resync()
	if err := t.rebManager.streams.SendV(hdr, file, cb, si); err != nil {
		glog.Errorf("failed to migrate: %s, err: %v", lom.FQN, err)
		return err.Error()
	}
	wg.Wait()

	if err != nil {
		return err.Error()
	}

	t.statsif.AddMany(stats.NamedVal64{stats.TxCount, 1}, stats.NamedVal64{stats.TxSize, fi.Size()})
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

		if !roi.migrated || roiCkConf.ValidateClusterMigration {
			saveHash = xxhash.New64()
			hashes = []hash.Hash{saveHash}

			// if sender provided checksum we need to ensure that it is correct
			if expectedCksum = roi.cksumToCheck; expectedCksum != nil {
				checkHash = saveHash
			}
		} else {
			// If migration validation is not required we can just take
			// calculated checksum by some other node (from which we received
			// the object). If not present we need to calculate it.
			roi.lom.SetCksum(roi.cksumToCheck)
			if roi.cksumToCheck == nil {
				saveHash = xxhash.New64()
				hashes = []hash.Hash{saveHash}
			}
		}
	} else if roi.cold {
		// by default we should calculate xxhash and save it together with file
		saveHash = xxhash.New64()
		hashes = []hash.Hash{saveHash}

		// if configured and the cksum is provied we should also check md5 hash (aws, gcp)
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
			err = fmt.Errorf("bad checksum - expected %s, got: %s; workFQN: %q",
				expectedCksum.String(), computedCksum.String(), roi.workFQN)
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
	glog.Errorf("FSHC: fqn %s, err %v", filepath, err)
	if !cmn.IsIOError(err) {
		return
	}
	mpathInfo, _ := fs.Mountpaths.Path2MpathInfo(filepath)
	if mpathInfo == nil {
		return
	}

	keyName := mpathInfo.Path
	// keyName is the mountpath is the fspath - counting IO errors on a per basis..
	t.statsdC.Send(keyName+".io.errors", metric{statsd.Counter, "count", 1})
	getfshealthchecker().OnErr(filepath)
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
