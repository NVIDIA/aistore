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
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/atime"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
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
	maxPageSize   = 64 * 1024     // max number of objects in a page (warn when req. size exceeds this limit)
	maxBytesInMem = 256 * cmn.KiB // objects with smaller size than this will be read to memory when checksumming

	// GET-from-neighbors tunables
	getFromNeighRetries   = 10
	getFromNeighSleep     = 300 * time.Millisecond
	getFromNeighAfterJoin = time.Second * 30
)

type (
	allfinfos struct {
		t            *targetrunner
		files        []*cmn.BucketEntry
		prefix       string
		marker       string
		markerDir    string
		msg          *cmn.GetMsg
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
		atimeRespCh  chan *atime.Response
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
		t       *targetrunner
		objname string
		bucket  string
		// Determines if the object was already in cluster and was received
		// because some kind of migration.
		migrated bool
		// Determines if the recv is cold recv: either from another cluster or cloud.
		cold bool

		// Reader with the content of the object.
		r io.ReadCloser
		// Checksum which needs to be checked on receive. It is only checked
		// on specific occasions: see `writeToFile` method.
		cksumToCheck cmn.CksumProvider
		// Context used when receiving object which is contained in cloud bucket.
		// It usually contains credentials to access the cloud.
		ctx context.Context

		// User specified parameter to determine which bucket to put object
		bucketProvider string

		// INTERNAL

		// FQN which is used only temporarily for receiving file. After
		// successful receive is renamed to actual FQN.
		workFQN string
		started time.Time // started time of receiving - used to calculate the recv duration
		lom     *cluster.LOM
	}
	// the state that may influence GET logic for the first (getFromNeighAfterJoin) seconds after joining
	getFromNeighbors struct {
		stopts time.Time // (reg-time + const) when we stop trying to GET from neighbors
		lookup bool
	}

	targetrunner struct {
		httprunner
		cloudif        cloudif // multi-cloud backend
		uxprocess      *uxprocess
		rtnamemap      *rtnamemap
		prefetchQueue  chan filesWithDeadline
		authn          *authManager
		clusterStarted int32
		oos            int32 // out of space
		fsprg          fsprungroup
		readahead      readaheader
		xcopy          *mirror.XactCopy
		ecmanager      *ecManager
		streams        struct {
			rebalance *transport.StreamBundle
		}
		gfn      getFromNeighbors
		regstate regstate // the state of being registered with the primary (can be en/disabled via API)
	}
)

//
// target runner
//
func (t *targetrunner) Run() error {
	config := cmn.GCO.Get()

	var ereg error
	t.httprunner.init(getstorstatsrunner(), false)
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
				glog.Errorf("%s: retrying registration...", tname(t.si))
				time.Sleep(time.Second)
				continue
			}
		}
		break
	}
	if ereg != nil {
		glog.Errorf("%s failed to register, err: %v", tname(t.si), ereg)
		glog.Errorf("%s is terminating", tname(t.si))
		return ereg
	}

	go t.pollClusterStarted(config.Timeout.CplaneOperation)

	// register object type and workfile type
	if err := fs.CSM.RegisterFileType(fs.ObjectType, &fs.ObjectContentResolver{}); err != nil {
		glog.Error(err)
		os.Exit(1)
	}
	if err := fs.CSM.RegisterFileType(fs.WorkfileType, &fs.WorkfileContentResolver{}); err != nil {
		glog.Error(err)
		os.Exit(1)
	}

	if err := fs.Mountpaths.CreateBucketDir(cmn.LocalBs); err != nil {
		glog.Error(err)
		os.Exit(1)
	}
	if err := fs.Mountpaths.CreateBucketDir(cmn.CloudBs); err != nil {
		glog.Error(err)
		os.Exit(1)
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
		networkHandler{r: cmn.Buckets, h: t.bucketHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl, cmn.NetworkIntraData}},
		networkHandler{r: cmn.Objects, h: t.objectHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraData}},
		networkHandler{r: cmn.Daemon, h: t.daemonHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl}},
		networkHandler{r: cmn.Tokens, h: t.tokenHandler, net: []string{cmn.NetworkPublic}},
		networkHandler{r: cmn.Push, h: t.pushHandler, net: []string{cmn.NetworkPublic}},

		networkHandler{r: cmn.Download, h: t.downloadHandler, net: []string{cmn.NetworkIntraControl}},
		networkHandler{r: cmn.Metasync, h: t.metasyncHandler, net: []string{cmn.NetworkIntraControl}},
		networkHandler{r: cmn.Health, h: t.healthHandler, net: []string{cmn.NetworkIntraControl}},
		networkHandler{r: cmn.Vote, h: t.voteHandler, net: []string{cmn.NetworkIntraControl}},
		networkHandler{r: cmn.Sort, h: dsort.SortHandler, net: []string{cmn.NetworkIntraControl, cmn.NetworkIntraData}},

		networkHandler{r: "/", h: cmn.InvalidHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl, cmn.NetworkIntraData}},
	}
	t.registerNetworkHandlers(networkHandlers)

	if err := t.setupStreams(); err != nil {
		glog.Error(err)
		os.Exit(1)
	}

	pid := int64(os.Getpid())
	t.uxprocess = &uxprocess{time.Now(), strconv.FormatInt(pid, 16), pid}

	getfshealthchecker().SetDispatcher(t)

	ec.Init()
	t.ecmanager = newECM(t)

	aborted, _ := t.xactions.isAbortedOrRunningLocalRebalance()
	if aborted {
		// resume local rebalance
		f := func() {
			glog.Infoln("resuming local rebalance...")
			t.runLocalRebalance()
		}
		go runLocalRebalanceOnce.Do(f) // only once at startup
	}

	dsort.RegisterNode(t.smapowner, t.si, t, t.rtnamemap)
	if err := t.httprunner.run(); err != nil {
		return err
	}
	glog.Infof("%s is ready to handle requests", tname(t.si))
	glog.Flush()
	return nil
}

func (t *targetrunner) setupStreams() error {
	var (
		network = cmn.NetworkIntraData
	)

	// fallback to network public if intra data is not available
	if !cmn.GCO.Get().Net.UseIntraData {
		network = cmn.NetworkPublic
	}

	if _, err := transport.Register(network, "rebalance", t.recvRebalanceObj); err != nil {
		return err
	}

	client := transport.NewDefaultClient()
	// TODO: stream bundle multiplier (currently default) should be adjustable at runtime (#253)
	t.streams.rebalance = transport.NewStreamBundle(t.smapowner, t.si, client, network, "rebalance", nil, cluster.Targets, 4)
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
	t.statsif.Register(stats.RebalGlobalCount, stats.KindCounter)
	t.statsif.Register(stats.RebalLocalCount, stats.KindCounter)
	t.statsif.Register(stats.RebalGlobalSize, stats.KindCounter)
	t.statsif.Register(stats.RebalLocalSize, stats.KindCounter)
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

// target registration with proxy
func (t *targetrunner) register(keepalive bool, timeout time.Duration) (status int, err error) {
	var (
		res  callResult
		meta targetRegMeta
	)
	if !keepalive {
		res = t.join(false, nil)
	} else { // keepalive
		url, psi := t.getPrimaryURLAndSI()
		res = t.registerToURL(url, psi, timeout, false, nil, keepalive)
	}
	if res.err != nil {
		return res.status, res.err
	}
	// not being sent at cluster startup and keepalive
	if len(res.outjson) == 0 {
		return
	}
	// There's a window of time between:
	// a) target joining existing cluster and b) cluster starting to rebalance itself
	// The latter is driven by metasync (see metasync.go) distributing the new Smap.
	// To handle incoming GETs within this window (which would typically take a few seconds or less)
	// we need to have the current cluster-wide metadata and the temporary gfn state:
	err = jsoniter.Unmarshal(res.outjson, &meta)
	cmn.AssertNoErr(err)
	t.gfn.lookup = true
	t.gfn.stopts = time.Now().Add(getFromNeighAfterJoin)
	// bmd
	msgInt := t.newActionMsgInternalStr(cmn.ActRegTarget, meta.Smap, meta.Bmd)
	if errstr := t.receiveBucketMD(meta.Bmd, msgInt, "register"); errstr != "" {
		t.gfn.lookup = false
		glog.Errorf("registered %s: %s", tname(t.si), errstr)
	} else {
		glog.Infof("registered %s: %s v%d", tname(t.si), bmdTermName, t.bmdowner.get().version())
	}
	// smap
	if errstr := t.smapowner.synchronize(meta.Smap, false /*saveSmap*/, true /* lesserIsErr */); errstr != "" {
		t.gfn.lookup = false
		glog.Errorf("registered %s: %s", tname(t.si), errstr)
	} else {
		glog.Infof("registered %s: Smap v%d", tname(t.si), t.smapowner.get().version())
	}
	return
}

func (t *targetrunner) unregister() (int, error) {
	smap := t.smapowner.get()
	if smap == nil || !smap.isValid() {
		return 0, nil
	}
	args := callArgs{
		si: smap.ProxySI,
		req: reqArgs{
			method: http.MethodDelete,
			path:   cmn.URLPath(cmn.Version, cmn.Cluster, cmn.Daemon, t.si.DaemonID),
		},
		timeout: defaultTimeout,
	}
	res := t.call(args)
	return res.status, res.err
}

//===========================================================================================
//
// targetrunner's API for external packages
//
//===========================================================================================

// implements cluster.Target interfaces
var _ cluster.Target = &targetrunner{}

func (t *targetrunner) OOS(oos ...bool) bool {
	if len(oos) > 0 {
		var v int32
		if oos[0] {
			v = 2019
		}
		atomic.StoreInt32(&t.oos, v)
		return v != 0
	}
	return atomic.LoadInt32(&t.oos) != 0
}

func (t *targetrunner) IsRebalancing() bool {
	_, running := t.xactions.isAbortedOrRunningRebalance()
	_, runningLocal := t.xactions.isAbortedOrRunningLocalRebalance()
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
		Xlru:       xlru,
		Namelocker: t.rtnamemap,
		Statsif:    t.statsif,
		T:          t,
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
			bckIsLocal, _ := t.validateBucketProvider(fwd.bucketProvider, fwd.bucket)
			if bckIsLocal {
				glog.Errorf("prefetch: bucket %s is local, nothing to do", fwd.bucket)
			} else {
				for _, objname := range fwd.objnames {
					t.prefetchMissing(fwd.ctx, objname, fwd.bucket, fwd.bucketProvider)
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

func (t *targetrunner) GetBowner() cluster.Bowner     { return t.bmdowner }
func (t *targetrunner) FSHC(err error, path string)   { t.fshc(err, path) }
func (t *targetrunner) GetAtimeRunner() *atime.Runner { return getatimerunner() }
func (t *targetrunner) GetMem2() *memsys.Mem2         { return gmem2 }
func (t *targetrunner) GetFSPRG() fs.PathRunGroup     { return &t.fsprg }

func (t *targetrunner) Receive(workFQN string, reader io.ReadCloser, lom *cluster.LOM) error {
	roi := &recvObjInfo{
		t:       t,
		r:       reader,
		workFQN: workFQN,
		lom:     lom,
	}
	err, _ := roi.recv()
	return err
}

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
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	// all cloud bucket names
	if bucket == "*" {
		t.getbucketnames(w, r)
		return
	}
	s := fmt.Sprintf("Invalid route /buckets/%s", bucket)
	t.invalmsghdlr(w, r, s)
}

// verifyProxyRedirection returns if the http request was redirected from a proxy
func (t *targetrunner) verifyProxyRedirection(w http.ResponseWriter, r *http.Request, bucket, objname, action string) bool {
	query := r.URL.Query()
	pid := query.Get(cmn.URLParamProxyID)
	if pid == "" {
		t.invalmsghdlr(w, r, fmt.Sprintf("%s %s requests are expected to be redirected", r.Method, action))
		return false
	}
	if t.smapowner.get().GetProxy(pid) == nil {
		t.invalmsghdlr(w, r, fmt.Sprintf("%s %s request from an unknown proxy/gateway ID '%s' - Smap out of sync?", r.Method, action, pid))
		return false
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s %s/%s <= %s", r.Method, action, bucket, objname, pid)
	}
	return true
}

// GET /v1/objects/bucket[+"/"+objname]
// Checks if the object exists locally (if not, downloads it) and sends it back
// If the bucket is in the Cloud one and ValidateWarmGet is enabled there is an extra
// check whether the object exists locally. Version is checked as well if configured.
func (t *targetrunner) httpobjget(w http.ResponseWriter, r *http.Request) {
	var (
		errcode    int
		started    time.Time
		config     = cmn.GCO.Get()
		versioncfg = &config.Ver
		ct         = t.contextWithAuth(r)
	)
	//
	// 1. start, init lom, readahead
	//
	started = time.Now()
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	query := r.URL.Query()
	bucketProvider := query.Get(cmn.URLParamBucketProvider)
	if redirDelta := t.redirectLatency(started, query); redirDelta != 0 {
		t.statsif.Add(stats.GetRedirLatency, redirDelta)
	}

	rangeOff, rangeLen, errstr := t.offsetAndLength(query)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}
	lom := &cluster.LOM{T: t, Bucket: bucket, Objname: objname}
	if errstr = lom.Fill(bucketProvider, cluster.LomFstat, config); errstr != "" { // (does_not_exist|misplaced -> ok, other)
		t.invalmsghdlr(w, r, errstr)
		return
	}
	if lom.Mirror.MirrorEnabled {
		if errstr = lom.Fill(bucketProvider, cluster.LomCopy); errstr != "" {
			// Log error but don't abort get operation, it is not critical.
			glog.Error(errstr)
		}
	}
	// FIXME:
	// if !dryRun.disk && lom.Exists() {
	// 	if x := query.Get(cmn.URLParamReadahead); x != "" {

	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, lom, pid)
	}

	// 2. under lock: versioning, checksum, restore from cluster
	t.rtnamemap.Lock(lom.Uname, false)
	coldGet := !lom.Exists()

	if !coldGet {
		if errstr = lom.Fill(bucketProvider, cluster.LomVersion|cluster.LomCksum); errstr != "" {
			_ = lom.Fill(bucketProvider, cluster.LomFstat)
			if lom.Exists() {
				t.rtnamemap.Unlock(lom.Uname, false)
				t.invalmsghdlr(w, r, errstr)
				return
			}
			glog.Warningf("%s delete race - proceeding to execute cold GET...", lom)
		}
	}

	// check if dryrun is enabled to stop from getting LOM
	if coldGet && lom.BckIsLocal && !dryRun.disk && !dryRun.network {
		// does not exist in the local bucket: restore from neighbors
		if errstr, errcode = t.restoreObjLBNeigh(lom, r, started); errstr != "" {
			t.rtnamemap.Unlock(lom.Uname, false)
			t.invalmsghdlr(w, r, errstr, errcode)
			return
		}
		t.objGetComplete(w, r, lom, started, rangeOff, rangeLen, false)
		t.rtnamemap.Unlock(lom.Uname, false)
		return
	}

	if !coldGet && !lom.BckIsLocal { // exists && cloud-bucket : check ver if requested
		if versioncfg.ValidateWarmGet && (lom.Version != "" && versioningConfigured(lom.BckIsLocal)) {
			if coldGet, errstr, errcode = t.checkCloudVersion(ct, bucket, objname, lom.Version); errstr != "" {
				t.rtnamemap.Unlock(lom.Uname, false)
				t.invalmsghdlr(w, r, errstr, errcode)
				return
			}
		}
	}

	// checksum validation, if requested
	if !coldGet && lom.Cksumcfg.ValidateWarmGet {
		errstr = lom.Fill(bucketProvider, cluster.LomCksum|cluster.LomCksumPresentRecomp|cluster.LomCksumMissingRecomp)
		if lom.BadCksum {
			glog.Errorln(errstr)
			if lom.BckIsLocal {
				if err := os.Remove(lom.FQN); err != nil {
					glog.Warningf("%s - failed to remove, err: %v", errstr, err)
				}
				t.rtnamemap.Unlock(lom.Uname, false)
				t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
				return
			}
			coldGet = true
		} else if errstr != "" {
			t.rtnamemap.Unlock(lom.Uname, false)
			t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
			return
		}
	}

	// 3. coldget
	if coldGet && !dryRun.disk && !dryRun.network {
		t.rtnamemap.Unlock(lom.Uname, false)
		if errstr, errcode := t.getCold(ct, lom, false); errstr != "" {
			t.invalmsghdlr(w, r, errstr, errcode)
			return
		}
	}

	// 4. finally
	t.objGetComplete(w, r, lom, started, rangeOff, rangeLen, coldGet)
	t.rtnamemap.Unlock(lom.Uname, false)
}

//
// 3a. attempt to restore an object that is missing in the LOCAL BUCKET - from:
//     1) local FS, 2) this cluster, 3) other tiers in the DC 4) from other
//		targets using erasure coding(if it is on)
// FIXME: must be done => (getfqn, and under write lock)
//
func (t *targetrunner) restoreObjLBNeigh(lom *cluster.LOM, r *http.Request, started time.Time) (errstr string, errcode int) {
	bucketProvider := r.URL.Query().Get(cmn.URLParamBucketProvider)
	// check FS-wide if local rebalance is running
	aborted, running := t.xactions.isAbortedOrRunningLocalRebalance()
	if aborted || running {
		// FIXME: move this part to lom
		oldFQN, oldSize := t.getFromOtherLocalFS(lom)
		if oldFQN != "" {
			lom.FQN = oldFQN
			lom.Size = oldSize
			lom.SetExists(true)
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("restored from LFS %s (%s)", lom, cmn.B2S(oldSize, 1))
			}
			return
		}
	}
	// check cluster-wide ("ask neighbors")
	aborted, running = t.xactions.isAbortedOrRunningRebalance()
	if t.gfn.lookup {
		if aborted || running {
			t.gfn.lookup = false
		} else if started.After(t.gfn.stopts) {
			t.gfn.lookup = false
		}
	}
	if aborted || running || t.gfn.lookup {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("neighbor lookup: aborted=%t, running=%t, lookup=%t", aborted, running, t.gfn.lookup)
		}
		// retry in case the object is being moved right now
		for retry := 0; retry < getFromNeighRetries; retry++ {
			props, errs := t.getFromNeighbor(r, lom)
			if errs != "" {
				time.Sleep(getFromNeighSleep)
				continue
			}
			lom.RestoredReceived(props)
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("restored from a neighbor: %s (%s)", lom, cmn.B2S(lom.Size, 1))
			}
			return
		}
	} else if t.getFromTier(lom) {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("restored from tier: %s (%s)", lom, cmn.B2S(lom.Size, 1))
		}
		return
	}

	// restore from existing EC slices if possible
	if ecErr := t.ecmanager.RestoreObject(lom); ecErr == nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s/%s is restored successfully", lom.Bucket, lom.Objname)
		}
		lom.Fill(bucketProvider, cluster.LomFstat|cluster.LomAtime)
		lom.SetExists(true)
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
	rangeOff, rangeLen int64, coldGet bool) {
	var (
		file        *os.File
		sgl         *memsys.SGL
		slab        *memsys.Slab2
		buf         []byte
		rangeReader io.ReadSeeker
		reader      io.Reader
		written     int64
		err         error
		errstr      string
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

	cksumRange := lom.Cksumcfg.Checksum != cmn.ChecksumNone && rangeLen > 0 && lom.Cksumcfg.EnableReadRangeChecksum
	hdr := w.Header()

	if lom.Cksum != nil && !cksumRange {
		cksumType, cksumValue := lom.Cksum.Get()
		hdr.Add(cmn.HeaderObjCksumType, cksumType)
		hdr.Add(cmn.HeaderObjCksumVal, cksumValue)
	}
	if lom.Version != "" {
		hdr.Add(cmn.HeaderObjVersion, lom.Version)
	}
	hdr.Add(cmn.HeaderObjSize, strconv.FormatInt(lom.Size, 10))

	// loopback if disk IO is disabled
	if dryRun.disk {
		if !dryRun.network {
			rd := newDryReader(dryRun.size)
			if _, err = io.Copy(w, rd); err != nil {
				// NOTE: Cannot return invalid handler because it will be double header write
				errstr = fmt.Sprintf("dry-run: failed to send random response, err: %v", err)
				glog.Error(errstr)
				t.statsif.Add(stats.ErrGetCount, 1)
				return
			}
		}

		delta := time.Since(started)
		t.statsif.AddMany(stats.NamedVal64{stats.GetCount, 1}, stats.NamedVal64{stats.GetLatency, int64(delta)})
		return
	}
	if lom.Size == 0 {
		glog.Warningf("%s size=0(zero)", lom) // TODO: optimize out much of the below
		return
	}
	fqn := lom.ChooseMirror()
	file, err = os.Open(fqn)
	if err != nil {
		if os.IsPermission(err) {
			errstr = fmt.Sprintf("Permission to access %s denied, err: %v", fqn, err)
			t.invalmsghdlr(w, r, errstr, http.StatusForbidden)
		} else {
			errstr = fmt.Sprintf("Failed to open %s, err: %v", fqn, err)
			t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
		}
		t.fshc(err, fqn)
		return
	}
	if rangeLen == 0 {
		reader = file
		// No need to allocate buffer for whole object (it might be very large).
		buf, slab = gmem2.AllocFromSlab2(cmn.MinI64(lom.Size, cmn.MiB))
	} else {
		buf, slab = gmem2.AllocFromSlab2(cmn.MinI64(rangeLen, cmn.MiB))
		if cksumRange {
			var cksum string
			cksum, sgl, rangeReader, errstr = t.rangeCksum(file, fqn, rangeOff, rangeLen, buf)
			if errstr != "" {
				t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
				return

			}
			reader = rangeReader
			hdr.Add(cmn.HeaderObjCksumType, lom.Cksumcfg.Checksum)
			hdr.Add(cmn.HeaderObjCksumVal, cksum)
		} else {
			reader = io.NewSectionReader(file, rangeOff, rangeLen)
		}
	}

	if !dryRun.network {
		written, err = io.CopyBuffer(w, reader, buf)
	} else {
		written, err = io.CopyBuffer(ioutil.Discard, reader, buf)
	}
	if err != nil {
		if !dryRun.network {
			errstr = fmt.Sprintf("Failed to GET %s, err: %v", fqn, err)
		} else {
			errstr = fmt.Sprintf("dry-run: failed to read/discard %s, err: %v", fqn, err)
		}
		glog.Error(errstr)
		t.fshc(err, fqn)
		t.statsif.Add(stats.ErrGetCount, 1)
		return
	}

	if !coldGet {
		lom.UpdateAtime(started)
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
}

func (t *targetrunner) rangeCksum(file *os.File, fqn string, offset, length int64, buf []byte) (
	cksumValue string, sgl *memsys.SGL, rangeReader io.ReadSeeker, errstr string) {
	var (
		err error
	)
	rangeReader = io.NewSectionReader(file, offset, length)
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
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	query := r.URL.Query()
	if redelta := t.redirectLatency(time.Now(), query); redelta != 0 {
		t.statsif.Add(stats.PutRedirLatency, redelta)
	}
	// PUT
	if !t.verifyProxyRedirection(w, r, bucket, objname, cmn.Objects) {
		return
	}
	if t.OOS() {
		t.invalmsghdlr(w, r, "OOS")
		return
	}

	if err, errCode := t.doPut(r, bucket, objname); err != nil {
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
	if !t.validatebckname(w, r, bucket) {
		return
	}
	bucketProvider := r.URL.Query().Get(cmn.URLParamBucketProvider)
	if _, errstr := t.validateBucketProvider(bucketProvider, bucket); errstr != "" {
		t.invalmsghdlr(w, r, errstr)
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
		// Validation handled in proxy.go
		fs.Mountpaths.EvictCloudBucket(bucket)
	case cmn.ActDelete, cmn.ActEvictObjects:
		if len(b) > 0 { // must be a List/Range request
			err := t.listRangeOperation(r, apitems, bucketProvider, msgInt)
			if err != nil {
				t.invalmsghdlr(w, r, fmt.Sprintf("Failed to delete files: %v", err))
			} else {
				if glog.FastV(4, glog.SmoduleAIS) {
					glog.Infof("DELETE list|range: %s, %d µs", bucket, int64(time.Since(started)/time.Microsecond))
				}
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
	bucketProvider := r.URL.Query().Get(cmn.URLParamBucketProvider)
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	if objname == "" {
		t.invalmsghdlr(w, r, "no object name")
		return
	}
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		s := err.Error()
		if err == io.EOF {
			if trailer := r.Trailer.Get("Error"); trailer != "" {
				s += " [trailer: " + trailer + "]"
			}
		}
		t.invalmsghdlr(w, r, s)
		return
	}
	if len(b) > 0 {
		if err = jsoniter.Unmarshal(b, &msg); err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
		evict = (msg.Action == cmn.ActEvictObjects)
	}
	lom := &cluster.LOM{T: t, Bucket: bucket, Objname: objname}
	if errstr := lom.Fill(bucketProvider, cluster.LomFstat|cluster.LomCopy); errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}
	if !lom.Exists() {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s/%s %s, nothing to do", bucket, objname, cmn.DoesNotExist)
		}
		return
	}
	err = t.objDelete(t.contextWithAuth(r), lom, evict)
	if err != nil {
		s := fmt.Sprintf("Error deleting %s/%s: %v", bucket, objname, err)
		t.invalmsghdlr(w, r, s)
		return
	}
	// EC cleanup if EC is enabled
	t.ecmanager.CleanupObject(lom)
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("DELETE: %s/%s, %d µs", bucket, objname, int64(time.Since(started)/time.Microsecond))
	}
}

// POST /v1/buckets/bucket-name
func (t *targetrunner) httpbckpost(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	var msgInt actionMsgInternal
	if cmn.ReadJSON(w, r, &msgInt) != nil {
		return
	}
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}

	bucket := apitems[0]
	bucketProvider := r.URL.Query().Get(cmn.URLParamBucketProvider)
	if _, errstr := t.validateBucketProvider(bucketProvider, bucket); errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}

	t.ensureLatestMD(msgInt)

	switch msgInt.Action {
	case cmn.ActPrefetch:
		// validation done in proxy.go
		if err := t.listRangeOperation(r, apitems, bucketProvider, msgInt); err != nil {
			t.invalmsghdlr(w, r, fmt.Sprintf("Failed to prefetch files: %v", err))
			return
		}
	case cmn.ActRenameLB:
		if !t.validatebckname(w, r, bucket) {
			return
		}
		bucketFrom, bucketTo := bucket, msgInt.Name

		t.bmdowner.Lock() // lock#1 begin

		bucketmd := t.bmdowner.get()
		props, ok := bucketmd.Get(bucketFrom, true)
		if !ok {
			t.bmdowner.Unlock()
			s := fmt.Sprintf("Local bucket %s %s", bucketFrom, cmn.DoesNotExist)
			t.invalmsghdlr(w, r, s)
			return
		}
		clone := bucketmd.clone()
		clone.LBmap[bucketTo] = props
		t.bmdowner.put(clone) // bmd updated with an added bucket, lock#1 end
		t.bmdowner.Unlock()

		if errstr := t.renameLB(bucketFrom, bucketTo); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
		}

		t.bmdowner.Lock() // lock#2 begin
		bucketmd = t.bmdowner.get()
		bucketmd.del(bucketFrom, true) // TODO: resolve conflicts
		t.bmdowner.Unlock()

		glog.Infof("renamed local bucket %s => %s, %s v%d", bucketFrom, bucketTo, bmdTermName, clone.version())
	case cmn.ActListObjects:
		if !t.validatebckname(w, r, bucket) {
			return
		}
		// list the bucket and return
		tag, ok := t.listbucket(w, r, bucket, bucketProvider, &msgInt)
		if ok {
			delta := time.Since(started)
			t.statsif.AddMany(stats.NamedVal64{stats.ListCount, 1}, stats.NamedVal64{stats.ListLatency, int64(delta)})
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("LIST %s: %s, %d µs", tag, bucket, int64(delta/time.Microsecond))
			}
		}
	case cmn.ActRechecksum:
		bucket := apitems[0]
		if !t.validatebckname(w, r, bucket) {
			return
		}
		// re-checksum the bucket and return
		t.runRechecksumBucket(bucket)
	case cmn.ActEraseCopies:
		bucket := apitems[0]
		if !t.validatebckname(w, r, bucket) {
			return
		}
		t.xactions.abortPutCopies(bucket)
		bucketmd := t.bmdowner.get()
		bckIsLocal := bucketmd.IsLocal(bucket)
		t.xactions.renewEraseCopies(bucket, t, bckIsLocal)
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
	case cmn.ActReplicate:
		t.replicate(w, r, msg)
	default:
		t.invalmsghdlr(w, r, "Unexpected action "+msg.Action)
	}
}

// HEAD /v1/buckets/bucket-name
func (t *targetrunner) httpbckhead(w http.ResponseWriter, r *http.Request) {
	var bucketprops cmn.SimpleKVs
	var errcode int
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	query := r.URL.Query()
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, bucket, pid)
	}
	bucketmd := t.bmdowner.get()
	bucketProvider := query.Get(cmn.URLParamBucketProvider)
	bckIsLocal, errstr := t.validateBucketProvider(bucketProvider, bucket)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}

	if !bckIsLocal {
		bucketprops, errstr, errcode = getcloudif().headbucket(t.contextWithAuth(r), bucket)
		if errstr != "" {
			t.invalmsghdlr(w, r, errstr, errcode)
			return
		}
	} else {
		bucketprops = make(cmn.SimpleKVs)
		bucketprops[cmn.HeaderCloudProvider] = cmn.ProviderAIS
		bucketprops[cmn.HeaderVersioning] = cmn.VersionLocal
	}
	// double check if we support versioning internally for the bucket
	if !versioningConfigured(bckIsLocal) {
		bucketprops[cmn.HeaderVersioning] = cmn.VersionNone
	}

	hdr := w.Header()
	for k, v := range bucketprops {
		hdr.Add(k, v)
	}

	// include bucket's own config override
	props, ok := bucketmd.Get(bucket, bckIsLocal)
	if props == nil {
		return
	}
	config := cmn.GCO.Get()
	cksumcfg := &config.Cksum // FIXME: must be props.CksumConf w/o conditions, here and elsewhere
	if ok && props.Checksum != cmn.ChecksumInherit {
		cksumcfg = &props.CksumConf
	}
	// transfer bucket props via http header;
	// (it is totally legal for Cloud buckets to not have locally cached props)
	hdr.Add(cmn.HeaderNextTierURL, props.NextTierURL)
	hdr.Add(cmn.HeaderReadPolicy, props.ReadPolicy)
	hdr.Add(cmn.HeaderWritePolicy, props.WritePolicy)
	hdr.Add(cmn.HeaderBucketChecksumType, cksumcfg.Checksum)
	hdr.Add(cmn.HeaderBucketValidateColdGet, strconv.FormatBool(cksumcfg.ValidateColdGet))
	hdr.Add(cmn.HeaderBucketValidateWarmGet, strconv.FormatBool(cksumcfg.ValidateWarmGet))
	hdr.Add(cmn.HeaderBucketValidateRange, strconv.FormatBool(cksumcfg.EnableReadRangeChecksum))
	hdr.Add(cmn.HeaderBucketLRULowWM, strconv.FormatInt(props.LowWM, 10))
	hdr.Add(cmn.HeaderBucketLRUHighWM, strconv.FormatInt(props.HighWM, 10))
	hdr.Add(cmn.HeaderBucketAtimeCacheMax, strconv.FormatInt(props.AtimeCacheMax, 10))
	hdr.Add(cmn.HeaderBucketDontEvictTime, props.DontEvictTimeStr)
	hdr.Add(cmn.HeaderBucketCapUpdTime, props.CapacityUpdTimeStr)
	hdr.Add(cmn.HeaderBucketMirrorEnabled, strconv.FormatBool(props.MirrorEnabled))
	hdr.Add(cmn.HeaderBucketMirrorThresh, strconv.FormatInt(props.MirrorUtilThresh, 10))
	hdr.Add(cmn.HeaderBucketLRUEnabled, strconv.FormatBool(props.LRUEnabled))
	if props.MirrorEnabled {
		hdr.Add(cmn.HeaderBucketCopies, strconv.FormatInt(props.MirrorConf.Copies, 10))
	} else {
		hdr.Add(cmn.HeaderBucketCopies, "0")
	}

	hdr.Add(cmn.HeaderBucketECEnabled, strconv.FormatBool(props.ECEnabled))
	hdr.Add(cmn.HeaderBucketECMinSize, strconv.FormatUint(uint64(props.ECObjSizeLimit), 10))
	hdr.Add(cmn.HeaderBucketECData, strconv.FormatUint(uint64(props.DataSlices), 10))
	hdr.Add(cmn.HeaderBucketECParity, strconv.FormatUint(uint64(props.ParitySlices), 10))
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
	checkCached, _ = parsebool(query.Get(cmn.URLParamCheckCached))
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname = apitems[0], apitems[1]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	bucketProvider := query.Get(cmn.URLParamBucketProvider)
	lom := &cluster.LOM{T: t, Bucket: bucket, Objname: objname}
	if errstr = lom.Fill(bucketProvider, cluster.LomFstat|cluster.LomVersion); errstr != "" { // (doesnotexist -> ok, other)
		t.invalmsghdlr(w, r, errstr)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, lom, pid)
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
		objmeta[cmn.HeaderObjSize] = strconv.FormatInt(lom.Size, 10)
		objmeta[cmn.HeaderObjVersion] = lom.Version
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s(%s), ver=%s", lom, cmn.B2S(lom.Size, 1), lom.Version)
		}
	} else {
		objmeta, errstr, errcode = getcloudif().headobject(t.contextWithAuth(r), bucket, objname)
		if errstr != "" {
			t.invalmsghdlr(w, r, errstr, errcode)
			return
		}
	}
	hdr := w.Header()
	for k, v := range objmeta {
		hdr.Add(k, v)
	}
}

// handler for: /v1/tokens
func (t *targetrunner) tokenHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodDelete:
		t.httpTokenDelete(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /tokens path")
	}
}

// [METHOD] /v1/metasync
func (t *targetrunner) metasyncHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		t.metasyncHandlerPut(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /metasync path")
	}
}

// PUT /v1/metasync
func (t *targetrunner) metasyncHandlerPut(w http.ResponseWriter, r *http.Request) {
	var payload = make(cmn.SimpleKVs)
	if err := cmn.ReadJSON(w, r, &payload); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	newsmap, actionsmap, errstr := t.extractSmap(payload)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}

	if newsmap != nil {
		errstr = t.receiveSmap(newsmap, actionsmap)
		if errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
		}
	}

	newbucketmd, actionlb, errstr := t.extractbucketmd(payload)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}

	if newbucketmd != nil {
		if errstr = t.receiveBucketMD(newbucketmd, actionlb, "receive"); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
		}
	}

	revokedTokens, errstr := t.extractRevokedTokenList(payload)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}
	t.authn.updateRevokedList(revokedTokens)
}

// GET /v1/health
func (t *targetrunner) healthHandler(w http.ResponseWriter, r *http.Request) {
	aborted, running := t.xactions.isAbortedOrRunningRebalance()
	if !aborted && !running {
		aborted, running = t.xactions.isAbortedOrRunningLocalRebalance()
	}
	status := &thealthstatus{IsRebalancing: aborted || running}

	jsbytes, err := jsoniter.Marshal(status)
	cmn.AssertNoErr(err)
	if ok := t.writeJSON(w, r, jsbytes, "thealthstatus"); !ok {
		return
	}

	query := r.URL.Query()
	from := query.Get(cmn.URLParamFromID)
	smap := t.smapowner.get()
	if smap.GetProxy(from) != nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s: health-ping from p[%s]", pname(t.si), from)
		}
		t.keepalive.heardFrom(from, false)
	} else if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s: health-ping from %s", tname(t.si), smap.printname(from))
	}
}

// [METHOD] /v1/push/bucket-name
func (t *targetrunner) pushHandler(w http.ResponseWriter, r *http.Request) {
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Push)
	if err != nil {
		return
	}
	bucket := apitems[0]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	if pusher, ok := w.(http.Pusher); ok {
		objnamebytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			s := fmt.Sprintf("Could not read Request Body: %v", err)
			if err == io.EOF {
				trailer := r.Trailer.Get("Error")
				if trailer != "" {
					s = fmt.Sprintf("pushhdlr: Failed to read %s request, err: %v, trailer: %s",
						r.Method, err, trailer)
				}
			}
			t.invalmsghdlr(w, r, s)
			return
		}
		objnames := make([]string, 0)
		err = jsoniter.Unmarshal(objnamebytes, &objnames)
		if err != nil {
			s := fmt.Sprintf("Could not unmarshal objnames: %v", err)
			t.invalmsghdlr(w, r, s)
			return
		}

		for _, objname := range objnames {
			err := pusher.Push(cmn.URLPath(cmn.Version, cmn.Objects, bucket, objname), nil)
			if err != nil {
				t.invalmsghdlr(w, r, "Error Pushing "+bucket+"/"+objname+": "+err.Error())
				return
			}
		}
	} else {
		t.invalmsghdlr(w, r, "Pusher Unavailable")
		return
	}

	if _, err = w.Write([]byte("Pushed Object List")); err != nil {
		s := fmt.Sprintf("Error writing response: %v", err)
		glog.Error(s)
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
	parsedFQN, _, err := cluster.ResolveFQN(fqn, nil, true /* bucket is local */)
	contentType, bucket, objname := parsedFQN.ContentType, parsedFQN.Bucket, parsedFQN.Objname
	if err == nil {
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
func (t *targetrunner) checkCloudVersion(ct context.Context, bucket, objname, version string) (vchanged bool, errstr string, errcode int) {
	var objmeta cmn.SimpleKVs
	if objmeta, errstr, errcode = t.cloudif.headobject(ct, bucket, objname); errstr != "" {
		return
	}
	if cloudVersion, ok := objmeta[cmn.HeaderObjVersion]; ok {
		if version != cloudVersion {
			glog.Infof("Object %s/%s version changed, current version %s (old/local %s)",
				bucket, objname, cloudVersion, version)
			vchanged = true
		}
	}
	return
}

func (t *targetrunner) getFromNeighbor(r *http.Request, lom *cluster.LOM) (remoteLOM *cluster.LOM, errstr string) {
	neighsi := t.lookupRemotely(lom)
	if neighsi == nil {
		errstr = fmt.Sprintf("Failed cluster-wide lookup %s", lom)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("Found %s at %s", lom, neighsi)
	}

	// FIXME: For now, need to re-translate lom.BckIsLocal to appropriate value ("local"|"cloud")
	// FIXME: this code below looks like a general code for sending request
	bucketProvider := cmn.LocalBs
	if !lom.BckIsLocal {
		bucketProvider = cmn.CloudBs
	}
	geturl := fmt.Sprintf("%s%s?%s=%s", neighsi.PublicNet.DirectURL, r.URL.Path, cmn.URLParamBucketProvider, bucketProvider)
	//
	// http request
	//
	newr, err := http.NewRequest(http.MethodGet, geturl, nil)
	if err != nil {
		errstr = fmt.Sprintf("Unexpected failure to create %s request %s, err: %v", http.MethodGet, geturl, err)
		return
	}
	// Do
	contextwith, cancel := context.WithTimeout(context.Background(), lom.Config.Timeout.SendFile)
	defer cancel()
	newrequest := newr.WithContext(contextwith)

	response, err := t.httpclientLongTimeout.Do(newrequest)
	if err != nil {
		errstr = fmt.Sprintf("Failed to GET redirect URL %q, err: %v", geturl, err)
		return
	}
	var (
		cksumValue = response.Header.Get(cmn.HeaderObjCksumVal)
		cksumType  = response.Header.Get(cmn.HeaderObjCksumType)
		cksum      = cmn.NewCksum(cksumType, cksumValue)
		version    = response.Header.Get(cmn.HeaderObjVersion)
		workFQN    = lom.GenFQN(fs.WorkfileType, fs.WorkfileRemote)
	)

	remoteLOM = lom.Copy(cluster.LOMCopyProps{Cksum: cksum, Version: version})

	roi := &recvObjInfo{
		t:        t,
		workFQN:  workFQN,
		migrated: true,
		r:        response.Body,
		lom:      remoteLOM,
	}

	if err = roi.writeToFile(); err != nil { // FIXME: transfer atime as well
		errstr = err.Error()
		return
	}
	// commit
	if err = cmn.MvFile(workFQN, remoteLOM.FQN); err != nil {
		errstr = err.Error()
		return
	}
	if errstr = remoteLOM.Persist(); errstr != "" {
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("Success: %s (%s, %s) from %s", remoteLOM, cmn.B2S(remoteLOM.Size, 1), remoteLOM.Cksum, neighsi)
	}
	return
}

// TODO
func (t *targetrunner) getFromTier(lom *cluster.LOM) (ok bool) {
	if lom.Bprops == nil || lom.Bprops.NextTierURL == "" {
		return
	}
	inNextTier, _, _ := t.objectInNextTier(lom.Bprops.NextTierURL, lom.Bucket, lom.Objname)
	if !inNextTier {
		return
	}
	props, errstr, _ := t.getObjectNextTier(lom.Bprops.NextTierURL, lom.Bucket, lom.Objname, lom.FQN)
	if errstr == "" {
		lom.RestoredReceived(props)
		ok = true
	}
	return
}

// FIXME: recomputes checksum if called with a bad one (optimize)
func (t *targetrunner) getCold(ct context.Context, lom *cluster.LOM, prefetch bool) (errstr string, errcode int) {
	var (
		versioncfg      = &lom.Config.Ver
		errv            string
		vchanged, crace bool
		err             error
		props           *cluster.LOM
	)
	if prefetch {
		if !t.rtnamemap.TryLock(lom.Uname, true) {
			glog.Infof("prefetch: cold GET race: %s - skipping", lom)
			return "skip", 0
		}
	} else {
		t.rtnamemap.Lock(lom.Uname, true) // one cold-GET at a time
	}

	// refill
	if errstr = lom.Fill("", cluster.LomFstat|cluster.LomCksum|cluster.LomVersion); errstr != "" {
		glog.Warning(errstr)
		lom.SetExists(false) // NOTE: in an attempt to fix it
	}

	// existence, access & versioning
	coldGet := !lom.Exists()
	if !coldGet {
		if versioncfg.ValidateWarmGet && lom.Version != "" && versioningConfigured(lom.BckIsLocal) {
			vchanged, errv, _ = t.checkCloudVersion(ct, lom.Bucket, lom.Objname, lom.Version)
			if errv == "" {
				coldGet = vchanged
			}
		}
		if !coldGet && lom.Cksumcfg.ValidateWarmGet {
			errstr := lom.Fill("", cluster.LomCksum|cluster.LomCksumPresentRecomp|cluster.LomCksumMissingRecomp)
			if lom.BadCksum {
				coldGet = true
			} else if errstr == "" {
				if lom.Cksum == nil {
					coldGet = true
				}
			} else {
				glog.Warningf("Failed to validate checksum, err: %s", errstr)
			}
		}
	}
	workFQN := lom.GenFQN(fs.WorkfileType, fs.WorkfileColdget)
	if !coldGet {
		_ = lom.Fill("", cluster.LomCksum)
		glog.Infof("cold-GET race: %s(%s, ver=%s) - nothing to do", lom, cmn.B2S(lom.Size, 1), lom.Version)
		crace = true
		goto ret
	}
	//
	// next tier if
	//
	if lom.Bprops != nil && lom.Bprops.NextTierURL != "" && lom.Bprops.ReadPolicy == cmn.RWPolicyNextTier {
		var inNextTier bool
		if inNextTier, errstr, errcode = t.objectInNextTier(lom.Bprops.NextTierURL, lom.Bucket, lom.Objname); errstr == "" {
			if inNextTier {
				if props, errstr, errcode = t.getObjectNextTier(lom.Bprops.NextTierURL, lom.Bucket, lom.Objname, workFQN); errstr == "" {
					coldGet = false
				}
			}
		}
	}
	//
	// cloud
	//
	if coldGet {
		if props, errstr, errcode = getcloudif().getobj(ct, workFQN, lom.Bucket, lom.Objname); errstr != "" {
			t.rtnamemap.Unlock(lom.Uname, true)
			return
		}
	}
	defer func() {
		if errstr != "" {
			t.rtnamemap.Unlock(lom.Uname, true)
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
	lom.RestoredReceived(props)
	if errstr = lom.Persist(); errstr != "" {
		return
	}
ret:
	//
	// NOTE: GET - downgrade and keep the lock, PREFETCH - unlock
	//
	if prefetch {
		t.rtnamemap.Unlock(lom.Uname, true)
	} else {
		if vchanged {
			t.statsif.AddMany(stats.NamedVal64{stats.GetColdCount, 1},
				stats.NamedVal64{stats.GetColdSize, lom.Size},
				stats.NamedVal64{stats.VerChangeSize, lom.Size},
				stats.NamedVal64{stats.VerChangeCount, 1})
		} else if !crace {
			t.statsif.AddMany(stats.NamedVal64{stats.GetColdCount, 1}, stats.NamedVal64{stats.GetColdSize, lom.Size})
		}
		t.rtnamemap.DowngradeLock(lom.Uname)
	}
	return
}

func (t *targetrunner) lookupRemotely(lom *cluster.LOM) *cluster.Snode {
	res := t.broadcastTo(
		cmn.URLPath(cmn.Version, cmn.Objects, lom.Bucket, lom.Objname),
		nil, // query
		http.MethodHead,
		nil,
		t.smapowner.get(),
		lom.Config.Timeout.MaxKeepalive,
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
func (t *targetrunner) listCachedObjects(bucket string, msg *cmn.GetMsg) (outbytes []byte, errstr string, errcode int) {
	reslist, err := t.prepareLocalObjectList(bucket, msg)
	if err != nil {
		return nil, err.Error(), 0
	}

	outbytes, err = jsoniter.Marshal(reslist)
	if err != nil {
		return nil, err.Error(), 0
	}
	return
}

func (t *targetrunner) prepareLocalObjectList(bucket string, msg *cmn.GetMsg) (*cmn.BucketList, error) {
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
		if err := filepath.Walk(dir, r.infos.listwalkf); err != nil {
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
	bckIsLocal := t.bmdowner.get().IsLocal(bucket)
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

	// sort the result and return only first `pageSize` entries
	marker := ""
	if fileCount > pageSize {
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

	if strings.Contains(msg.GetProps, cmn.GetTargetURL) {
		for _, e := range bucketList.Entries {
			e.TargetURL = t.si.PublicNet.DirectURL
		}
	}

	return bucketList, nil
}

func (t *targetrunner) getbucketnames(w http.ResponseWriter, r *http.Request) {
	bucketmd := t.bmdowner.get()
	bucketProvider := r.URL.Query().Get(cmn.URLParamBucketProvider)

	bucketnames := &cmn.BucketNames{
		Local: make([]string, 0, len(bucketmd.LBmap)),
		Cloud: make([]string, 0, 64),
	}
	if bucketProvider != cmn.CloudBs {
		for bucket := range bucketmd.LBmap {
			bucketnames.Local = append(bucketnames.Local, bucket)
		}
	}

	buckets, errstr, errcode := getcloudif().getbucketnames(t.contextWithAuth(r))
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}
	bucketnames.Cloud = buckets

	jsbytes, err := jsoniter.Marshal(bucketnames)
	cmn.AssertNoErr(err)
	t.writeJSON(w, r, jsbytes, "getbucketnames")
}

func (t *targetrunner) doLocalBucketList(w http.ResponseWriter, r *http.Request, bucket string, msg *cmn.GetMsg) (errstr string, ok bool) {
	reslist, err := t.prepareLocalObjectList(bucket, msg)
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
func (t *targetrunner) listbucket(w http.ResponseWriter, r *http.Request, bucket, bucketProvider string, actionMsg *actionMsgInternal) (tag string, ok bool) {
	var (
		jsbytes []byte
		errstr  string
		errcode int
	)
	query := r.URL.Query()
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, bucket, pid)
	}
	bckIsLocal, errstr := t.validateBucketProvider(bucketProvider, bucket)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
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

	var msg cmn.GetMsg
	err = jsoniter.Unmarshal(getMsgJSON, &msg)
	if err != nil {
		errstr := fmt.Sprintf("Unable to unmarshal 'value' in request to a cmn.GetMsg: %v", actionMsg.Value)
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
	if useCache {
		tag = "cloud cached"
		jsbytes, errstr, errcode = t.listCachedObjects(bucket, &msg)
	} else {
		tag = "cloud"
		jsbytes, errstr, errcode = getcloudif().listbucket(t.contextWithAuth(r), bucket, &msg)
	}
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}
	ok = t.writeJSON(w, r, jsbytes, "listbucket")
	return
}

func (t *targetrunner) newFileWalk(bucket string, msg *cmn.GetMsg) *allfinfos {
	// Marker is always a file name, so we need to strip filename from path
	markerDir := ""
	if msg.GetPageMarker != "" {
		markerDir = filepath.Dir(msg.GetPageMarker)
	}

	// A small optimization: set boolean variables need* to avoid
	// doing string search(strings.Contains) for every entry.
	ci := &allfinfos{
		t:            t, // targetrunner
		files:        make([]*cmn.BucketEntry, 0, cmn.DefaultPageSize),
		prefix:       msg.GetPrefix,
		marker:       msg.GetPageMarker,
		markerDir:    markerDir,
		msg:          msg,
		lastFilePath: "",
		bucket:       bucket,
		fileCount:    0,
		rootLength:   0,
		limit:        cmn.DefaultPageSize, // maximum number files to return
		needAtime:    strings.Contains(msg.GetProps, cmn.GetPropsAtime),
		needCtime:    strings.Contains(msg.GetProps, cmn.GetPropsCtime),
		needChkSum:   strings.Contains(msg.GetProps, cmn.GetPropsChecksum),
		needVersion:  strings.Contains(msg.GetProps, cmn.GetPropsVersion),
		needStatus:   strings.Contains(msg.GetProps, cmn.GetPropsStatus),
		needCopies:   strings.Contains(msg.GetProps, cmn.GetPropsCopies),
		atimeRespCh:  make(chan *atime.Response, 1),
	}

	if msg.GetPageSize != 0 {
		ci.limit = msg.GetPageSize
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
	lomAction := 0
	if ci.needAtime {
		lomAction |= cluster.LomAtime
	}
	if ci.needChkSum {
		lomAction |= cluster.LomCksum
	}
	if ci.needVersion {
		lomAction |= cluster.LomVersion
	}
	if lomAction != 0 {
		lom.Fill("", lomAction)
	}
	if ci.needAtime {
		fileInfo.Atime = lom.Atimestr
	}
	if ci.needCtime {
		t := osfi.ModTime()
		switch ci.msg.GetTimeFormat {
		case "":
			fallthrough
		case cmn.RFC822:
			fileInfo.Ctime = t.Format(time.RFC822)
		default:
			fileInfo.Ctime = t.Format(ci.msg.GetTimeFormat)
		}
	}
	if ci.needChkSum && lom.Cksum != nil {
		_, storedCksum := lom.Cksum.Get()
		fileInfo.Checksum = hex.EncodeToString([]byte(storedCksum))
	}
	if ci.needVersion {
		fileInfo.Version = lom.Version
	}
	if ci.needCopies && lom.HasCopy() {
		fileInfo.Copies = 2 // 2-way, or not replicated
	}
	fileInfo.Size = osfi.Size()
	ci.files = append(ci.files, fileInfo)
	ci.lastFilePath = lom.FQN
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
		lom       = &cluster.LOM{T: ci.t, FQN: fqn}
	)
	errstr := lom.Fill("", cluster.LomFstat|cluster.LomCopy)
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
func (t *targetrunner) doPut(r *http.Request, bucket, objname string) (err error, errcode int) {
	var (
		cksumType  = r.Header.Get(cmn.HeaderObjCksumType)
		cksumValue = r.Header.Get(cmn.HeaderObjCksumVal)
		cksum      = cmn.NewCksum(cksumType, cksumValue)
	)

	bucketProvider := r.URL.Query().Get(cmn.URLParamBucketProvider)
	roi := &recvObjInfo{
		t:              t,
		objname:        objname,
		bucket:         bucket,
		r:              r.Body,
		cksumToCheck:   cksum,
		ctx:            t.contextWithAuth(r),
		bucketProvider: bucketProvider,
	}
	if err := roi.init(); err != nil {
		return err, http.StatusInternalServerError
	}

	return roi.recv()
}

// TODO: this function is for now unused because replication does not work
//lint:ignore U1000 unused
func (t *targetrunner) doReplicationPut(r *http.Request, bucket, objname, replicaSrc string) (errstr string) {
	var (
		started    = time.Now()
		bckIsLocal = t.bmdowner.get().IsLocal(bucket)
	)

	fqn, errstr := cluster.FQN(fs.ObjectType, bucket, objname, bckIsLocal)
	if errstr != "" {
		return errstr
	}
	err := getreplicationrunner().reqReceiveReplica(replicaSrc, fqn, r)

	if err != nil {
		return err.Error()
	}

	delta := time.Since(started)
	t.statsif.AddMany(stats.NamedVal64{stats.ReplPutCount, 1}, stats.NamedVal64{stats.ReplPutLatency, int64(delta)})
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("Replication PUT: %s/%s, %d µs", bucket, objname, int64(delta/time.Microsecond))
	}
	return ""
}

func (roi *recvObjInfo) init() error {
	cmn.Assert(roi.t != nil)
	roi.started = time.Now()
	roi.lom = &cluster.LOM{T: roi.t, Objname: roi.objname, Bucket: roi.bucket}
	if errstr := roi.lom.Fill(roi.bucketProvider, cluster.LomFstat); errstr != "" {
		return errors.New(errstr)
	}
	roi.workFQN = roi.lom.GenFQN(fs.WorkfileType, fs.WorkfilePut)
	// All objects which are migrated should contain checksum.
	if roi.migrated {
		cmn.Assert(roi.cksumToCheck != nil)
	}

	return nil
}

func (roi *recvObjInfo) recv() (err error, errCode int) {
	cmn.Assert(roi.lom != nil)
	// optimize out if the checksums do match
	if roi.lom.Exists() && roi.cksumToCheck != nil {
		if errstr := roi.lom.Fill(roi.bucketProvider, cluster.LomCksum); errstr == "" {
			if cmn.EqCksum(roi.lom.Cksum, roi.cksumToCheck) {
				if glog.FastV(4, glog.SmoduleAIS) {
					glog.Infof("%s is valid %s: PUT is a no-op", roi.lom, roi.cksumToCheck)
				}
				io.Copy(ioutil.Discard, roi.r) // drain the reader
				return nil, 0
			}
		}
	}

	if err = roi.writeToFile(); err != nil {
		return err, http.StatusInternalServerError
	}

	if !dryRun.disk && !dryRun.network {
		// commit
		if errstr, errCode := roi.commit(); errstr != "" {
			return errors.New(errstr), errCode
		}
	}

	delta := time.Since(roi.started)
	roi.t.statsif.AddMany(stats.NamedVal64{stats.PutCount, 1}, stats.NamedVal64{stats.PutLatency, int64(delta)})
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("PUT: %s/%s, %d µs", roi.bucket, roi.objname, int64(delta/time.Microsecond))
	}

	roi.t.localMirror(roi.lom)
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
		roi.lom.SetExists(true)
	}

	if !roi.migrated && errstr == "" {
		if err := roi.t.ecmanager.EncodeObject(roi.lom); err != nil && err != ec.ErrorECDisabled {
			errstr = err.Error()
		}
	}

	return
}

func (roi *recvObjInfo) tryCommit() (errstr string, errCode int) {
	if !roi.lom.BckIsLocal && !roi.migrated {
		file, err := os.Open(roi.workFQN)
		if err != nil {
			errstr = fmt.Sprintf("Failed to open %s err: %v", roi.workFQN, err)
			return
		}

		cmn.Assert(roi.lom.Cksum != nil)
		roi.lom.Version, errstr, errCode = getcloudif().putobj(roi.ctx, file, roi.lom.Bucket, roi.lom.Objname, roi.lom.Cksum)
		file.Close()
		if errstr != "" {
			return
		}
	}

	// Lock the uname for the object and rename it from workFQN to actual FQN.
	roi.t.rtnamemap.Lock(roi.lom.Uname, true)
	if roi.lom.BckIsLocal && versioningConfigured(true) {
		if roi.lom.Version, errstr = roi.lom.IncObjectVersion(); errstr != "" {
			roi.t.rtnamemap.Unlock(roi.lom.Uname, true)
			return
		}
	}
	if roi.lom.HasCopy() {
		if errstr = roi.lom.DelCopy(); errstr != "" {
			roi.t.rtnamemap.Unlock(roi.lom.Uname, true)
			return
		}
	}
	if err := cmn.MvFile(roi.workFQN, roi.lom.FQN); err != nil {
		roi.t.rtnamemap.Unlock(roi.lom.Uname, true)
		errstr = fmt.Sprintf("MvFile failed => %s: %v", roi.lom, err)
		return
	}
	if errstr = roi.lom.Persist(); errstr != "" {
		glog.Errorf("Failed to persist %s: %s", roi.lom, errstr)
	}
	roi.t.rtnamemap.Unlock(roi.lom.Uname, true)
	return
}

func (t *targetrunner) localMirror(lom *cluster.LOM) {
	if !lom.Mirror.MirrorEnabled || dryRun.disk {
		return
	}

	if fs.Mountpaths.NumAvail() <= 1 { // FIXME: cache when filling-in the lom
		return
	}

	if t.xcopy == nil || t.xcopy.Finished() || t.xcopy.Bucket() != lom.Bucket || t.xcopy.BckIsLocal != lom.BckIsLocal {
		t.xcopy = t.xactions.renewPutCopies(lom, t)
	}
	if t.xcopy == nil {
		return
	}
	err := t.xcopy.Copy(lom)
	// retry in the (unlikely) case of simultaneous xcopy-timeout
	if _, ok := err.(*cmn.ErrXpired); ok {
		t.xcopy = t.xactions.renewPutCopies(lom, t)
		if t.xcopy != nil {
			err = t.xcopy.Copy(lom)
		}
	}
	if err != nil {
		glog.Errorf("Unexpected: %v", err)
	}
}

func (t *targetrunner) recvRebalanceObj(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
	if err != nil {
		glog.Error(err)
		return
	}

	roi := &recvObjInfo{
		t:            t,
		objname:      hdr.Objname,
		bucket:       hdr.Bucket,
		migrated:     true,
		r:            ioutil.NopCloser(objReader),
		cksumToCheck: cmn.NewCksum(hdr.ObjAttrs.CksumType, hdr.ObjAttrs.CksumValue),
	}
	if err := roi.init(); err != nil {
		glog.Error(err)
		return
	}
	roi.lom.Atime = time.Unix(0, hdr.ObjAttrs.Atime)
	roi.lom.Version = hdr.ObjAttrs.Version

	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("Rebalance %s from %s", roi.lom, hdr.Opaque)
	}

	if err, _ := roi.recv(); err != nil {
		glog.Error(err)
		return
	}

	t.statsif.AddMany(stats.NamedVal64{stats.RxCount, 1}, stats.NamedVal64{stats.RxSize, hdr.ObjAttrs.Size})
}

func (t *targetrunner) objDelete(ct context.Context, lom *cluster.LOM, evict bool) error {
	var (
		errstr  string
		errcode int
	)
	t.rtnamemap.Lock(lom.Uname, true)
	defer t.rtnamemap.Unlock(lom.Uname, true)

	delFromCloud := !lom.BckIsLocal && !evict
	if errstr := lom.Fill("", cluster.LomFstat); errstr != "" {
		return errors.New(errstr)
	}
	delFromAIS := lom.Exists()

	if delFromCloud {
		if errstr, errcode = getcloudif().deleteobj(ct, lom.Bucket, lom.Objname); errstr != "" {
			if errcode == 0 {
				return fmt.Errorf("%s", errstr)
			}
			return fmt.Errorf("%d: %s", errcode, errstr)
		}
		t.statsif.Add(stats.DeleteCount, 1)
	}
	if delFromAIS {
		if lom.HasCopy() {
			if errstr := lom.DelCopy(); errstr != "" {
				return errors.New(errstr)
			}
		}
		if err := os.Remove(lom.FQN); err != nil {
			return err
		} else if evict {
			cmn.Assert(!lom.BckIsLocal)
			t.statsif.AddMany(
				stats.NamedVal64{stats.LruEvictCount, 1},
				stats.NamedVal64{stats.LruEvictSize, lom.Size})
		}
	}
	return nil
}

func (t *targetrunner) renameObject(w http.ResponseWriter, r *http.Request, msg cmn.ActionMsg) {
	var errstr string

	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objnameFrom := apitems[0], apitems[1]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	objnameTo := msg.Name
	uname := cluster.Uname(bucket, objnameFrom)
	t.rtnamemap.Lock(uname, true)

	if errstr = t.renameBucketObject(fs.ObjectType, bucket, objnameFrom, bucket, objnameTo); errstr != "" {
		t.invalmsghdlr(w, r, errstr)
	}
	t.rtnamemap.Unlock(uname, true)
}

func (t *targetrunner) replicate(w http.ResponseWriter, r *http.Request, msg cmn.ActionMsg) {
	t.invalmsghdlr(w, r, cmn.NotSupported) // NOTE: daemon.go, proxy.go, config.sh, and tests/replication
}

func (t *targetrunner) renameBucketObject(contentType, bucketFrom, objnameFrom, bucketTo, objnameTo string) (errstr string) {
	var (
		fi     os.FileInfo
		file   *cmn.FileHandle
		si     *cluster.Snode
		newFQN string
		err    error
	)
	if si, errstr = hrwTarget(bucketTo, objnameTo, t.smapowner.get()); errstr != "" {
		return
	}
	bucketmd := t.bmdowner.get()
	bckIsLocalFrom := bucketmd.IsLocal(bucketFrom)
	fqn, errstr := cluster.FQN(contentType, bucketFrom, objnameFrom, bckIsLocalFrom)
	if errstr != "" {
		return
	}
	if fi, err = os.Stat(fqn); err != nil {
		errstr = fmt.Sprintf("Rename/move: failed to fstat %s (%s/%s), err: %v", fqn, bucketFrom, objnameFrom, err)
		return
	}

	// local rename
	if si.DaemonID == t.si.DaemonID {
		bckIsLocalTo := bucketmd.IsLocal(bucketTo)
		newFQN, errstr = cluster.FQN(contentType, bucketTo, objnameTo, bckIsLocalTo)
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
			bucketFrom, objnameFrom, tname(t.si), bucketTo, objnameTo, tname(si))
	}
	if file, err = cmn.NewFileHandle(fqn); err != nil {
		return fmt.Sprintf("failed to open %s, err: %v", fqn, err)
	}
	defer file.Close()

	lom := &cluster.LOM{T: t, FQN: fqn}
	if errstr := lom.Fill("", cluster.LomFstat|cluster.LomVersion|cluster.LomAtime|cluster.LomCksum|cluster.LomCksumMissingRecomp); errstr != "" {
		return errstr
	}
	cksumType, cksumValue := lom.Cksum.Get()
	hdr := transport.Header{
		Bucket:  bucketTo,
		Objname: objnameTo,
		IsLocal: lom.BckIsLocal,
		Opaque:  []byte(t.si.DaemonID),
		ObjAttrs: transport.ObjectAttrs{
			Size:       fi.Size(),
			Atime:      lom.Atime.UnixNano(),
			CksumType:  cksumType,
			CksumValue: cksumValue,
			Version:    lom.Version,
		},
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	cb := func(hdr transport.Header, r io.ReadCloser, cbErr error) {
		err = cbErr
		wg.Done()
	}
	if err := t.streams.rebalance.SendV(hdr, file, cb, si); err != nil {
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
	if useCache, err = parsebool(useCacheStr); err != nil {
		errstr = fmt.Sprintf("Invalid URL query parameter: %s=%s (expecting: '' | true | false)",
			cmn.URLParamCached, useCacheStr)
		errcode = http.StatusInternalServerError
	}
	return
}

func (t *targetrunner) fetchPrimaryMD(what string, outStruct interface{}) (errstr string) {
	smap := t.smapowner.get()
	if smap == nil || !smap.isValid() {
		return "smap nil or missing"
	}
	psi := smap.ProxySI
	q := url.Values{}
	q.Add(cmn.URLParamWhat, what)
	args := callArgs{
		si: psi,
		req: reqArgs{
			method: http.MethodGet,
			base:   psi.IntraControlNet.DirectURL,
			path:   cmn.URLPath(cmn.Version, cmn.Daemon),
			query:  q,
		},
		timeout: cmn.GCO.Get().Timeout.CplaneOperation,
	}
	res := t.call(args)
	if res.err != nil {
		return fmt.Sprintf("failed to %s=%s, err: %v", what, cmn.GetWhatBucketMeta, res.err)
	}

	err := jsoniter.Unmarshal(res.outjson, outStruct)
	if err != nil {
		return fmt.Sprintf("unexpected: failed to unmarshal %s=%s response, err: %v [%v]",
			what, psi.IntraControlNet.DirectURL, err, string(res.outjson))
	}

	return ""
}

func (t *targetrunner) smapVersionFixup() {
	newsmap := &smapX{}
	errstr := t.fetchPrimaryMD(cmn.GetWhatSmap, newsmap)
	if errstr != "" {
		glog.Errorf(errstr)
		return
	}
	var msgInt = t.newActionMsgInternalStr("get-what="+cmn.GetWhatSmap, newsmap, nil)
	t.receiveSmap(newsmap, msgInt)
}

func (t *targetrunner) bmdVersionFixup() {
	newbucketmd := &bucketMD{}
	errstr := t.fetchPrimaryMD(cmn.GetWhatBucketMeta, newbucketmd)
	if errstr != "" {
		glog.Errorf(errstr)
		return
	}
	var msgInt = t.newActionMsgInternalStr("get-what="+cmn.GetWhatBucketMeta, nil, newbucketmd)
	t.receiveBucketMD(newbucketmd, msgInt, "fixup")
}

//===========================
//
// control plane
//
//===========================

// [METHOD] /v1/daemon
func (t *targetrunner) daemonHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t.httpdaeget(w, r)
	case http.MethodPut:
		t.httpdaeput(w, r)
	case http.MethodPost:
		t.httpdaepost(w, r)
	case http.MethodDelete:
		t.httpdaedelete(w, r)
	default:
		cmn.InvalidHandlerWithMsg(w, r, "invalid method for /daemon path")
	}
}

func (t *targetrunner) httpdaeput(w http.ResponseWriter, r *http.Request) {
	apitems, err := t.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Daemon)
	if err != nil {
		return
	}
	if len(apitems) > 0 {
		switch apitems[0] {
		// PUT /v1/daemon/proxy/newprimaryproxyid
		case cmn.Proxy:
			t.httpdaesetprimaryproxy(w, r, apitems)
			return
		case cmn.SyncSmap:
			var newsmap = &smapX{}
			if cmn.ReadJSON(w, r, newsmap) != nil {
				return
			}
			if errstr := t.smapowner.synchronize(newsmap, false /*saveSmap*/, true /* lesserIsErr */); errstr != "" {
				t.invalmsghdlr(w, r, fmt.Sprintf("Failed to sync Smap: %s", errstr))
			}
			glog.Infof("%s: %s v%d done", tname(t.si), cmn.SyncSmap, newsmap.version())
			return
		case cmn.Mountpaths:
			t.handleMountpathReq(w, r)
			return
		default:
		}
	}
	//
	// other PUT /daemon actions
	//
	var msg cmn.ActionMsg
	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}
	switch msg.Action {
	case cmn.ActSetConfig:
		if value, ok := msg.Value.(string); !ok {
			t.invalmsghdlr(w, r, fmt.Sprintf("Failed to parse cmn.ActionMsg value: Not a string"))
		} else if errstr := t.setconfig(msg.Name, value); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
		} else {
			glog.Infof("setconfig %s=%s", msg.Name, value)
			if msg.Name == "lru_enabled" && value == "false" {
				lruxact := t.xactions.findU(cmn.ActLRU)
				if lruxact != nil {
					if glog.V(3) {
						glog.Infof("Aborting LRU due to lru_enabled config change")
					}
					lruxact.Abort()
				}
			}
		}
	case cmn.ActShutdown:
		_ = syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	default:
		s := fmt.Sprintf("Unexpected cmn.ActionMsg <- JSON [%v]", msg)
		t.invalmsghdlr(w, r, s)
	}
}

func (t *targetrunner) httpdaesetprimaryproxy(w http.ResponseWriter, r *http.Request, apitems []string) {
	var (
		prepare bool
		err     error
	)
	if len(apitems) != 2 {
		s := fmt.Sprintf("Incorrect number of API items: %d, should be: %d", len(apitems), 2)
		t.invalmsghdlr(w, r, s)
		return
	}

	proxyid := apitems[1]
	query := r.URL.Query()
	preparestr := query.Get(cmn.URLParamPrepare)
	if prepare, err = strconv.ParseBool(preparestr); err != nil {
		s := fmt.Sprintf("Failed to parse %s URL Parameter: %v", cmn.URLParamPrepare, err)
		t.invalmsghdlr(w, r, s)
		return
	}

	smap := t.smapowner.get()
	psi := smap.GetProxy(proxyid)
	if psi == nil {
		s := fmt.Sprintf("New primary proxy %s not present in the local %s", proxyid, smap.pp())
		t.invalmsghdlr(w, r, s)
		return
	}
	if prepare {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Info("Preparation step: do nothing")
		}
		return
	}
	t.smapowner.Lock()
	smap = t.smapowner.get()
	if smap.ProxySI.DaemonID != psi.DaemonID {
		clone := smap.clone()
		clone.ProxySI = psi
		if s := t.smapowner.persist(clone, false /*saveSmap*/); s != "" {
			t.smapowner.Unlock()
			t.invalmsghdlr(w, r, s)
			return
		}
		t.smapowner.put(clone)
	}
	t.smapowner.Unlock()
}

func (t *targetrunner) httpdaeget(w http.ResponseWriter, r *http.Request) {
	getWhat := r.URL.Query().Get(cmn.URLParamWhat)
	switch getWhat {
	case cmn.GetWhatConfig, cmn.GetWhatSmap, cmn.GetWhatBucketMeta, cmn.GetWhatSmapVote, cmn.GetWhatDaemonInfo:
		t.httprunner.httpdaeget(w, r)
	case cmn.GetWhatStats:
		rst := getstorstatsrunner()
		jsbytes, err := rst.GetWhatStats()
		cmn.AssertNoErr(err)
		t.writeJSON(w, r, jsbytes, "httpdaeget-"+getWhat)
	case cmn.GetWhatXaction:
		var (
			jsbytes     []byte
			err         error
			sts         = getstorstatsrunner()
			kind        = r.URL.Query().Get(cmn.URLParamProps)
			kindDetails = t.getXactionsByKind(kind)
		)
		if kind == cmn.ActGlobalReb {
			jsbytes = sts.GetRebalanceStats(kindDetails)
		} else if kind == cmn.ActPrefetch {
			jsbytes = sts.GetPrefetchStats(kindDetails)
		} else {
			jsbytes, err = jsoniter.Marshal(kindDetails)
			cmn.AssertNoErr(err)
		}
		t.writeJSON(w, r, jsbytes, "httpdaeget-"+getWhat)
	case cmn.GetWhatMountpaths:
		mpList := cmn.MountpathList{}
		availablePaths, disabledPaths := fs.Mountpaths.Get()
		mpList.Available = make([]string, len(availablePaths))
		mpList.Disabled = make([]string, len(disabledPaths))

		idx := 0
		for mpath := range availablePaths {
			mpList.Available[idx] = mpath
			idx++
		}
		idx = 0
		for mpath := range disabledPaths {
			mpList.Disabled[idx] = mpath
			idx++
		}
		jsbytes, err := jsoniter.Marshal(&mpList)
		if err != nil {
			s := fmt.Sprintf("Failed to marshal mountpaths: %v", err)
			t.invalmsghdlr(w, r, s)
			return
		}
		t.writeJSON(w, r, jsbytes, "httpdaeget-"+getWhat)
	default:
		t.httprunner.httpdaeget(w, r)
	}
}

func (t *targetrunner) getXactionsByKind(kind string) []stats.XactionDetails {
	kindDetails := []stats.XactionDetails{}
	matching := t.xactions.selectL(kind)
	for _, xaction := range matching {
		status := cmn.XactionStatusCompleted
		if !xaction.Finished() {
			status = cmn.XactionStatusInProgress
		}
		details := stats.XactionDetails{ // FIXME: redundant vs XactBase
			ID:        xaction.ID(),
			Kind:      xaction.Kind(),
			Bucket:    xaction.Bucket(),
			StartTime: xaction.StartTime(),
			EndTime:   xaction.EndTime(),
			Status:    status,
		}
		kindDetails = append(kindDetails, details)
	}
	return kindDetails
}

// register target
// enable/disable mountpath
func (t *targetrunner) httpdaepost(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 0, true, cmn.Version, cmn.Daemon)
	if err != nil {
		return
	}

	if len(apiItems) > 0 {
		switch apiItems[0] {
		case cmn.Register:
			// TODO: this is duplicated in `t.register`. The difference is that
			// here we are reregistered by the user and in `t.register` is
			// registering new target. This needs to be consolidated somehow.
			//
			// There's a window of time between:
			// a) target joining existing cluster and b) cluster starting to rebalance itself
			// The latter is driven by metasync (see metasync.go) distributing the new Smap.
			// To handle incoming GETs within this window (which would typically take a few seconds or less)
			// we need to have the current cluster-wide metadata and the temporary gfn state:
			t.gfn.lookup = true
			t.gfn.stopts = time.Now().Add(getFromNeighAfterJoin)

			if glog.V(3) {
				glog.Infoln("Sending register signal to target keepalive control channel")
			}
			gettargetkeepalive().keepalive.controlCh <- controlSignal{msg: register}
			return
		case cmn.Mountpaths:
			t.handleMountpathReq(w, r)
			return
		default:
			t.invalmsghdlr(w, r, "unrecognized path in /daemon POST")
			return
		}
	}

	if status, err := t.register(false, defaultTimeout); err != nil {
		s := fmt.Sprintf("%s failed to register with proxy, status %d, err: %v", t.si, status, err)
		t.invalmsghdlr(w, r, s)
		return
	}

	if glog.V(3) {
		glog.Infof("Registered %s(self)", tname(t.si))
	}
}

// unregister
// remove mountpath
func (t *targetrunner) httpdaedelete(w http.ResponseWriter, r *http.Request) {
	apiItems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Daemon)
	if err != nil {
		return
	}

	switch apiItems[0] {
	case cmn.Mountpaths:
		t.handleMountpathReq(w, r)
		return
	case cmn.Unregister:
		if glog.V(3) {
			glog.Infoln("Sending unregister signal to target keepalive control channel")
		}
		gettargetkeepalive().keepalive.controlCh <- controlSignal{msg: unregister}
		return
	default:
		t.invalmsghdlr(w, r, fmt.Sprintf("unrecognized path: %q in /daemon DELETE", apiItems[0]))
		return
	}
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
		writer = ioutil.Discard
		reader = roi.r
	)

	if dryRun.disk && dryRun.network {
		return
	}
	if dryRun.network {
		reader = newDryReader(dryRun.size)
	}
	if !dryRun.disk {
		if file, err = cmn.CreateFile(roi.workFQN); err != nil {
			roi.t.fshc(err, roi.workFQN)
			return fmt.Errorf("failed to create %s, err: %s", roi.workFQN, err)
		}
		writer = file
	}

	buf, slab := gmem2.AllocFromSlab2(0)
	defer func() { // free & cleanup on err
		slab.Free(buf)
		reader.Close()

		if err != nil && !dryRun.disk {
			if nestedErr := file.Close(); nestedErr != nil {
				glog.Errorf("Nested (%v): failed to close received object %s, err: %v", err, roi.workFQN, nestedErr)
			}
			if nestedErr := os.Remove(roi.workFQN); nestedErr != nil {
				glog.Errorf("Nested (%v): failed to remove %s, err: %v", err, roi.workFQN, nestedErr)
			}
		}
	}()

	if dryRun.disk || dryRun.network {
		_, err = cmn.ReceiveAndChecksum(writer, reader, buf)
		return err
	}

	// receive and checksum
	var (
		written int64

		checkCksumType      string
		expectedCksum       cmn.CksumProvider
		saveHash, checkHash hash.Hash
		hashes              []hash.Hash
	)

	if !roi.cold && roi.lom.Cksumcfg.Checksum != cmn.ChecksumNone {
		checkCksumType = roi.lom.Cksumcfg.Checksum
		cmn.AssertMsg(checkCksumType == cmn.ChecksumXXHash, checkCksumType)

		if !roi.migrated || roi.lom.Cksumcfg.ValidateClusterMigration {
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
			if roi.lom.Cksum = roi.cksumToCheck; roi.lom.Cksum == nil {
				saveHash = xxhash.New64()
				hashes = []hash.Hash{saveHash}
			}
		}
	} else if roi.cold {
		// by default we should calculate xxhash and save it together with file
		saveHash = xxhash.New64()
		hashes = []hash.Hash{saveHash}

		// if configured and the cksum is provied we should also check md5 hash (aws, gcp)
		if roi.lom.Cksumcfg.ValidateColdGet && roi.cksumToCheck != nil {
			expectedCksum = roi.cksumToCheck
			checkCksumType, _ = expectedCksum.Get()
			cmn.AssertMsg(checkCksumType == cmn.ChecksumMD5, checkCksumType)

			checkHash = md5.New()
			hashes = append(hashes, checkHash)
		}
	}

	if written, err = cmn.ReceiveAndChecksum(writer, reader, buf, hashes...); err != nil {
		roi.t.fshc(err, roi.workFQN)
		return
	}
	roi.lom.Size = written

	if checkHash != nil {
		computedCksum := cmn.NewCksum(checkCksumType, cmn.HashToStr(checkHash))
		if !cmn.EqCksum(expectedCksum, computedCksum) {
			err = fmt.Errorf("bad checksum expected %s, got: %s; workFQN: %q", expectedCksum.String(), computedCksum.String(), roi.workFQN)
			roi.t.statsif.AddMany(stats.NamedVal64{stats.ErrCksumCount, 1}, stats.NamedVal64{stats.ErrCksumSize, written})
			return
		}
	}
	if saveHash != nil {
		roi.lom.Cksum = cmn.NewCksum(cmn.ChecksumXXHash, cmn.HashToStr(saveHash))
	}

	if err = file.Close(); err != nil {
		return fmt.Errorf("failed to close received file %s, err: %v", roi.workFQN, err)
	}
	return nil
}

//==============================================================================
//
// target's misc utilities and helpers
//
//==============================================================================
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

// create local directories to test multiple fspaths
func (t *targetrunner) testCachepathMounts() {
	config := cmn.GCO.Get()
	var instpath string
	if config.TestFSP.Instance > 0 {
		instpath = filepath.Join(config.TestFSP.Root, strconv.Itoa(config.TestFSP.Instance))
	} else {
		// container, VM, etc.
		instpath = config.TestFSP.Root
	}
	for i := 0; i < config.TestFSP.Count; i++ {
		mpath := filepath.Join(instpath, strconv.Itoa(i+1))
		if err := cmn.CreateDir(mpath); err != nil {
			glog.Errorf("FATAL: cannot create test cache dir %q, err: %v", mpath, err)
			os.Exit(1)
		}

		err := fs.Mountpaths.Add(mpath)
		cmn.AssertNoErr(err)
	}
}

func (t *targetrunner) detectMpathChanges() {
	// mpath config dir
	mpathconfigfqn := filepath.Join(cmn.GCO.Get().Confdir, cmn.MountpathBackupFile)

	type mfs struct {
		Available cmn.StringSet `json:"available"`
		Disabled  cmn.StringSet `json:"disabled"`
	}

	// load old/prev and compare
	var (
		oldfs = mfs{}
		newfs = mfs{
			Available: make(cmn.StringSet),
			Disabled:  make(cmn.StringSet),
		}
	)

	availablePaths, disabledPath := fs.Mountpaths.Get()
	for mpath := range availablePaths {
		newfs.Available[mpath] = struct{}{}
	}
	for mpath := range disabledPath {
		newfs.Disabled[mpath] = struct{}{}
	}

	if err := cmn.LocalLoad(mpathconfigfqn, &oldfs); err != nil {
		if !os.IsNotExist(err) && err != io.EOF {
			glog.Errorf("Failed to load old mpath config %q, err: %v", mpathconfigfqn, err)
		}
		return
	}

	if !reflect.DeepEqual(oldfs, newfs) {
		oldfsPprint := fmt.Sprintf(
			"available: %v\ndisabled: %v",
			oldfs.Available.String(), oldfs.Disabled.String(),
		)
		newfsPprint := fmt.Sprintf(
			"available: %v\ndisabled: %v",
			newfs.Available.String(), newfs.Disabled.String(),
		)

		glog.Errorf("%s: detected change in the mountpath configuration at %s", tname(t.si), mpathconfigfqn)
		glog.Errorln("OLD: ====================")
		glog.Errorln(oldfsPprint)
		glog.Errorln("NEW: ====================")
		glog.Errorln(newfsPprint)
	}
	// persist
	if err := cmn.LocalSave(mpathconfigfqn, newfs); err != nil {
		glog.Errorf("Error writing config file: %v", err)
	}
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

// Decrypts token and retreives userID from request header
// Returns empty userID in case of token is invalid
func (t *targetrunner) userFromRequest(r *http.Request) (*authRec, error) {
	if r == nil {
		return nil, nil
	}

	token := ""
	tokenParts := strings.SplitN(r.Header.Get("Authorization"), " ", 2)
	if len(tokenParts) == 2 && tokenParts[0] == tokenStart {
		token = tokenParts[1]
	}

	if token == "" {
		// no token in header = use default credentials
		return nil, nil
	}

	authrec, err := t.authn.validateToken(token)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt token [%s]: %v", token, err)
	}

	return authrec, nil
}

// If Authn server is enabled then the function tries to read a user credentials
// (at this moment userID is enough) from HTTP request header: looks for
// 'Authorization' header and decrypts it.
// Extracted user information is put to context that is passed to all consumers
func (t *targetrunner) contextWithAuth(r *http.Request) context.Context {
	ct := context.Background()
	config := cmn.GCO.Get()

	if config.Auth.CredDir == "" || !config.Auth.Enabled {
		return ct
	}

	user, err := t.userFromRequest(r)
	if err != nil {
		glog.Errorf("Failed to extract token: %v", err)
		return ct
	}

	if user != nil {
		ct = context.WithValue(ct, ctxUserID, user.userID)
		ct = context.WithValue(ct, ctxCredsDir, config.Auth.CredDir)
		ct = context.WithValue(ct, ctxUserCreds, user.creds)
	}

	return ct
}

func (t *targetrunner) handleMountpathReq(w http.ResponseWriter, r *http.Request) {
	msg := cmn.ActionMsg{}
	if cmn.ReadJSON(w, r, &msg) != nil {
		t.invalmsghdlr(w, r, "Invalid mountpath request")
		return
	}

	mountpath, ok := msg.Value.(string)
	if !ok {
		t.invalmsghdlr(w, r, "Invalid value in request")
		return
	}

	if mountpath == "" {
		t.invalmsghdlr(w, r, "Mountpath is not defined")
		return
	}

	switch msg.Action {
	case cmn.ActMountpathEnable:
		t.handleEnableMountpathReq(w, r, mountpath)
	case cmn.ActMountpathDisable:
		t.handleDisableMountpathReq(w, r, mountpath)
	case cmn.ActMountpathAdd:
		t.handleAddMountpathReq(w, r, mountpath)
	case cmn.ActMountpathRemove:
		t.handleRemoveMountpathReq(w, r, mountpath)
	default:
		t.invalmsghdlr(w, r, "Invalid action in request")
	}
}

func (t *targetrunner) stopXactions(xacts []string) {
	for _, name := range xacts {
		xactList := t.xactions.selectL(name)
		for _, xact := range xactList {
			if !xact.Finished() {
				xact.Abort()
			}
		}
	}
}

func (t *targetrunner) handleEnableMountpathReq(w http.ResponseWriter, r *http.Request, mountpath string) {
	enabled, exists := t.fsprg.enableMountpath(mountpath)
	if !enabled && exists {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if !enabled && !exists {
		t.invalmsghdlr(w, r, fmt.Sprintf("Mountpath %s not found", mountpath), http.StatusNotFound)
		return
	}
	t.stopXactions([]string{cmn.ActLRU, cmn.ActPutCopies, cmn.ActEraseCopies})
}

func (t *targetrunner) handleDisableMountpathReq(w http.ResponseWriter, r *http.Request, mountpath string) {
	enabled, exists := t.fsprg.disableMountpath(mountpath)
	if !enabled && exists {
		w.WriteHeader(http.StatusNoContent)
		return
	}

	if !enabled && !exists {
		t.invalmsghdlr(w, r, fmt.Sprintf("Mountpath %s not found", mountpath), http.StatusNotFound)
		return
	}

	t.stopXactions([]string{cmn.ActLRU, cmn.ActPutCopies, cmn.ActEraseCopies})
}

func (t *targetrunner) handleAddMountpathReq(w http.ResponseWriter, r *http.Request, mountpath string) {
	err := t.fsprg.addMountpath(mountpath)
	if err != nil {
		t.invalmsghdlr(w, r, fmt.Sprintf("Could not add mountpath, error: %v", err))
		return
	}
	t.stopXactions([]string{cmn.ActLRU, cmn.ActPutCopies, cmn.ActEraseCopies})
}

func (t *targetrunner) handleRemoveMountpathReq(w http.ResponseWriter, r *http.Request, mountpath string) {
	err := t.fsprg.removeMountpath(mountpath)
	if err != nil {
		t.invalmsghdlr(w, r, fmt.Sprintf("Could not remove mountpath, error: %v", err))
		return
	}

	t.stopXactions([]string{cmn.ActLRU, cmn.ActPutCopies, cmn.ActEraseCopies})
}

// FIXME: use the message
func (t *targetrunner) receiveBucketMD(newbucketmd *bucketMD, msgInt *actionMsgInternal, tag string) (errstr string) {
	if msgInt.Action == "" {
		glog.Infof("%s %s v%d", tag, bmdTermName, newbucketmd.version())
	} else {
		glog.Infof("%s %s v%d, action %s", tag, bmdTermName, newbucketmd.version(), msgInt.Action)
	}
	t.bmdowner.Lock()
	bucketmd := t.bmdowner.get()
	myver := bucketmd.version()
	if newbucketmd.version() <= myver {
		t.bmdowner.Unlock()
		if newbucketmd.version() < myver {
			errstr = fmt.Sprintf("Attempt to downgrade %s v%d to v%d", bmdTermName, myver, newbucketmd.version())
		}
		return
	}
	t.bmdowner.put(newbucketmd)
	t.bmdowner.Unlock()

	// Delete buckets that do not exist in the new bucket metadata
	bucketsToDelete := make([]string, 0, len(bucketmd.LBmap))
	for bucket := range bucketmd.LBmap {
		if nprops, ok := newbucketmd.LBmap[bucket]; !ok {
			bucketsToDelete = append(bucketsToDelete, bucket)
			// TODO: separate API to stop ActPut and ActErase xactions and/or disable mirroring
			// (needed in part for cloud buckets)
			t.xactions.abortBucketSpecific(bucket)
		} else if bprops, ok := bucketmd.LBmap[bucket]; ok && bprops != nil && nprops != nil {
			if bprops.MirrorConf.MirrorEnabled && !nprops.MirrorConf.MirrorEnabled {
				t.xactions.abortPutCopies(bucket)
			}
		}
	}
	fs.Mountpaths.CreateDestroyLocalBuckets("receive-bucketmd", false /*false=destroy*/, bucketsToDelete...)

	// Create buckets that have been added
	bucketsToCreate := make([]string, 0, len(newbucketmd.LBmap))
	for bucket := range newbucketmd.LBmap {
		if _, ok := bucketmd.LBmap[bucket]; !ok {
			bucketsToCreate = append(bucketsToCreate, bucket)
		}
	}
	fs.Mountpaths.CreateDestroyLocalBuckets("receive-bucketmd", true /*create*/, bucketsToCreate...)

	return
}

func (t *targetrunner) receiveSmap(newsmap *smapX, msgInt *actionMsgInternal) (errstr string) {
	var (
		newTargetID, s string
		existentialQ   bool
	)
	// proxy => target control protocol (see p.httpclupost)
	action, id := msgInt.Action, msgInt.NewDaemonID
	if action != "" {
		newTargetID = id
	}
	if msgInt.Action != "" {
		s = ", action " + msgInt.Action
	}
	glog.Infof("%s: receive Smap v%d, ntargets %d, primary %s%s",
		tname(t.si), newsmap.version(), newsmap.CountTargets(), pname(newsmap.ProxySI), s)
	for id, si := range newsmap.Tmap { // log
		if id == t.si.DaemonID {
			existentialQ = true
			glog.Infof("%s <= self", tname(si))
		} else {
			glog.Infof("%s", tname(si))
		}
	}
	if !existentialQ {
		errstr = fmt.Sprintf("Not finding %s(self) in the new %s", tname(t.si), newsmap.pp())
		return
	}
	if errstr = t.smapowner.synchronize(newsmap, false /*saveSmap*/, true /* lesserIsErr */); errstr != "" {
		return
	}
	if msgInt.Action == cmn.ActGlobalReb {
		go t.runRebalance(newsmap, newTargetID)
		return
	}
	if !cmn.GCO.Get().Rebalance.Enabled {
		glog.Infoln("auto-rebalancing disabled")
		return
	}
	if newTargetID == "" {
		return
	}
	glog.Infof("%s receiveSmap: go rebalance(newTargetID=%s)", tname(t.si), newTargetID)
	go t.runRebalance(newsmap, newTargetID)
	return
}

func (t *targetrunner) ensureLatestMD(msgInt actionMsgInternal) {
	smap := t.smapowner.Get()
	smapVersion := msgInt.SmapVersion
	if smap.Version < smapVersion {
		glog.Errorf("own smap version %d < %d - fetching latest for %v", smap.Version, smapVersion, msgInt.Action)
		t.statsif.Add(stats.ErrMetadataCount, 1)
		t.smapVersionFixup()
	} else if smap.Version > smapVersion {
		//if metasync outraces the request, we end up here, just log it and continue
		glog.Errorf("own smap version %d > %d - encountered during %v", smap.Version, smapVersion, msgInt.Action)
		t.statsif.Add(stats.ErrMetadataCount, 1)
	}

	bucketmd := t.bmdowner.Get()
	bmdVersion := msgInt.BMDVersion
	if bucketmd.Version < bmdVersion {
		glog.Errorf("own bucket-metadata version %d < %d - fetching latest for %v", bucketmd.Version, bmdVersion, msgInt.Action)
		t.statsif.Add(stats.ErrMetadataCount, 1)
		t.bmdVersionFixup()
	} else if bucketmd.Version > bmdVersion {
		//if metasync outraces the request, we end up here, just log it and continue
		glog.Errorf("own bucket-metadata version %d > %d - encountered during %v", bucketmd.Version, bmdVersion, msgInt.Action)
		t.statsif.Add(stats.ErrMetadataCount, 1)
	}
}

func (t *targetrunner) pollClusterStarted(timeout time.Duration) {
	for {
		time.Sleep(time.Second)
		smap := t.smapowner.get()
		if smap == nil || !smap.isValid() {
			continue
		}
		psi := smap.ProxySI
		args := callArgs{
			si: psi,
			req: reqArgs{
				method: http.MethodGet,
				base:   psi.IntraControlNet.DirectURL,
				path:   cmn.URLPath(cmn.Version, cmn.Health),
			},
			timeout: timeout,
		}
		res := t.call(args)
		if res.err != nil {
			continue
		}
		proxystats := &stats.ProxyCoreStats{} // FIXME
		err := jsoniter.Unmarshal(res.outjson, proxystats)
		if err != nil {
			glog.Errorf("Unexpected: failed to unmarshal %s response, err: %v [%v]",
				psi.PublicNet.DirectURL, err, string(res.outjson))
			continue
		}
		if proxystats.Tracker[stats.Uptime].Value != 0 {
			glog.Infof("%s startup: primary %s", tname(t.si), pname(psi))
			break
		}
	}
	atomic.StoreInt32(&t.clusterStarted, 1)
}

func (t *targetrunner) httpTokenDelete(w http.ResponseWriter, r *http.Request) {
	tokenList := &TokenList{}
	if _, err := t.checkRESTItems(w, r, 0, false, cmn.Version, cmn.Tokens); err != nil {
		return
	}

	if err := cmn.ReadJSON(w, r, tokenList); err != nil {
		s := fmt.Sprintf("Invalid token list: %v", err)
		t.invalmsghdlr(w, r, s)
		return
	}

	t.authn.updateRevokedList(tokenList)
}

// unregisters the target and marks it as disabled by an internal event
func (t *targetrunner) disable() error {
	var (
		status int
		eunreg error
	)

	t.regstate.Lock()
	defer t.regstate.Unlock()

	if t.regstate.disabled {
		return nil
	}

	glog.Infof("Disabling %s", tname(t.si))
	for i := 0; i < maxRetrySeconds; i++ {
		if status, eunreg = t.unregister(); eunreg != nil {
			if cmn.IsErrConnectionRefused(eunreg) || status == http.StatusRequestTimeout {
				glog.Errorf("%s: retrying unregistration...", tname(t.si))
				time.Sleep(time.Second)
				continue
			}
		}
		break
	}

	if status >= http.StatusBadRequest || eunreg != nil {
		// do not update state on error
		if eunreg == nil {
			eunreg = fmt.Errorf("error unregistering %s, http status %d", tname(t.si), status)
		}
		return eunreg
	}

	t.regstate.disabled = true
	glog.Infof("%s has been disabled (unregistered)", tname(t.si))
	return nil
}

// registers the target again if it was disabled by and internal event
func (t *targetrunner) enable() error {
	var (
		status int
		ereg   error
	)

	t.regstate.Lock()
	defer t.regstate.Unlock()

	if !t.regstate.disabled {
		return nil
	}

	glog.Infof("Enabling %s", tname(t.si))
	for i := 0; i < maxRetrySeconds; i++ {
		if status, ereg = t.register(false, defaultTimeout); ereg != nil {
			if cmn.IsErrConnectionRefused(ereg) || status == http.StatusRequestTimeout {
				glog.Errorf("%s: retrying registration...", tname(t.si))
				time.Sleep(time.Second)
				continue
			}
		}
		break
	}

	if status >= http.StatusBadRequest || ereg != nil {
		// do not update state on error
		if ereg == nil {
			ereg = fmt.Errorf("error registering %s, http status: %d", tname(t.si), status)
		}
		return ereg
	}

	t.regstate.disabled = false
	glog.Infof("%s has been enabled", tname(t.si))
	return nil
}

// Disable implements fspathDispatcher interface
func (t *targetrunner) Disable(mountpath string, why string) (disabled, exists bool) {
	// TODO: notify admin that the mountpath is gone
	glog.Warningf("Disabling mountpath %s: %s", mountpath, why)
	t.stopXactions([]string{cmn.ActLRU, cmn.ActPutCopies, cmn.ActEraseCopies})
	return t.fsprg.disableMountpath(mountpath)
}

func (t *targetrunner) getFromOtherLocalFS(lom *cluster.LOM) (fqn string, size int64) {
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
