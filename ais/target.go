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
	"net"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/ais/util/readers"
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
	internalPageSize = 10000     // number of objects in a page for internal call between target and proxy to get atime/iscached
	MaxPageSize      = 64 * 1024 // max number of objects in a page (warning logged if requested page size exceeds this limit)
	maxBytesInMem    = 256 * cmn.KiB

	// GET: when object is missing and rebalance is running we schedule couple of retries.
	restoreMissingRetries   = 10
	restoreMissingIntervals = 300 * time.Millisecond
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
		cksumToCheck cmn.CksumValue
		// Context used when receiving object which is contained in cloud bucket.
		// It usually contains credentials to access the cloud.
		ctx context.Context

		// INTERNAL

		// FQN which is used only temporarily for receiving file. After
		// successful receive is renamed to actual FQN.
		workFQN string
		started time.Time // started time of receiving - used to calculate the recv duration
		lom     *cluster.LOM
	}

	targetrunner struct {
		httprunner
		cloudif        cloudif // multi-cloud backend
		uxprocess      *uxprocess
		rtnamemap      *rtnamemap
		prefetchQueue  chan filesWithDeadline
		authn          *authManager
		clusterStarted int64
		regstate       regstate // registration state - the state of being registered (with the proxy), or maybe not
		fsprg          fsprungroup
		readahead      readaheader
		xcopy          *mirror.XactCopy
		ecmanager      *ecManager

		streams struct {
			rebalance *transport.StreamBundle
		}
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

	t.rtnamemap = newrtnamemap(128) // lock/unlock name

	bucketmd := newBucketMD()
	t.bmdowner.put(bucketmd)

	smap := newSmap()
	smap.Tmap[t.si.DaemonID] = t.si
	t.smapowner.put(smap)

	for i := 0; i < maxRetrySeconds; i++ {
		var status int
		if status, ereg = t.register(false, defaultTimeout); ereg != nil {
			if cmn.IsErrConnectionRefused(ereg) || status == http.StatusRequestTimeout {
				glog.Errorf("Target %s: retrying registration...", t.si)
				time.Sleep(time.Second)
				continue
			}
		}
		break
	}
	if ereg != nil {
		glog.Errorf("Target %s failed to register, err: %v", t.si, ereg)
		glog.Errorf("Target %s is terminating", t.si)
		return ereg
	}

	go t.pollClusterStarted()

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
		t.cloudif = &awsimpl{t}
	} else {
		t.cloudif = &gcpimpl{t}
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

	// Public network
	t.registerPublicNetHandler(cmn.URLPath(cmn.Version, cmn.Buckets)+"/", t.bucketHandler)
	t.registerPublicNetHandler(cmn.URLPath(cmn.Version, cmn.Objects)+"/", t.objectHandler)
	t.registerPublicNetHandler(cmn.URLPath(cmn.Version, cmn.Daemon), t.daemonHandler)
	t.registerPublicNetHandler(cmn.URLPath(cmn.Version, cmn.Push)+"/", t.pushHandler)
	t.registerPublicNetHandler(cmn.URLPath(cmn.Version, cmn.Tokens), t.tokenHandler)
	transport.SetMux(cmn.NetworkPublic, t.publicServer.mux) // to register transport handlers at runtime
	t.registerPublicNetHandler("/", cmn.InvalidHandler)

	// Intra control network
	t.registerIntraControlNetHandler(cmn.URLPath(cmn.Version, cmn.Metasync), t.metasyncHandler)
	t.registerIntraControlNetHandler(cmn.URLPath(cmn.Version, cmn.Health), t.healthHandler)
	t.registerIntraControlNetHandler(cmn.URLPath(cmn.Version, cmn.Vote), t.voteHandler)
	t.registerIntraControlNetHandler(cmn.URLPath(cmn.Version, cmn.Sort), dsort.SortHandler)
	if config.Net.UseIntraControl {
		transport.SetMux(cmn.NetworkIntraControl, t.intraControlServer.mux) // to register transport handlers at runtime
		t.registerIntraControlNetHandler("/", cmn.InvalidHandler)
	}

	// Intra data network
	if config.Net.UseIntraData {
		transport.SetMux(cmn.NetworkIntraData, t.intraDataServer.mux) // to register transport handlers at runtime
		t.registerIntraDataNetHandler(cmn.URLPath(cmn.Version, cmn.Objects)+"/", t.objectHandler)
		t.registerIntraDataNetHandler("/", cmn.InvalidHandler)
	}

	if err := t.setupStreams(); err != nil {
		glog.Error(err)
		os.Exit(1)
	}

	glog.Infof("target %s is ready", t.si)
	glog.Flush()
	pid := int64(os.Getpid())
	t.uxprocess = &uxprocess{time.Now(), strconv.FormatInt(pid, 16), pid}

	getfshealthchecker().SetDispatcher(t)

	ec.Init()
	t.ecmanager = NewECM(t)

	aborted, _ := t.xactions.isAbortedOrRunningLocalRebalance()
	if aborted {
		// resume local rebalance
		f := func() {
			glog.Infoln("resuming local rebalance...")
			t.runLocalRebalance()
		}
		go runLocalRebalanceOnce.Do(f) // only once at startup
	}

	dsort.RegisterNode(t.smapowner, t.si, t.rtnamemap)
	return t.httprunner.run()
}

func (t *targetrunner) setupStreams() error {
	var (
		network = cmn.NetworkIntraData
		ncpu    = runtime.NumCPU()
	)

	// fallback to network public if intra data is not available
	if !cmn.GCO.Get().Net.UseIntraData {
		network = cmn.NetworkPublic
	}

	if _, err := transport.Register(network, "rebalance", t.recvRebalanceObj); err != nil {
		return err
	}

	defaultTransport := http.DefaultTransport.(*http.Transport)
	client := &http.Client{Transport: &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          1000,
		IdleConnTimeout:       30 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: defaultTransport.ExpectContinueTimeout,
		MaxIdleConnsPerHost:   ncpu,
	}}
	t.streams.rebalance = transport.NewStreamBundle(t.smapowner, t.si, client, network, "rebalance", nil, cluster.Targets, ncpu)
	return nil
}

// target-only stats
func (t *targetrunner) registerStats() {
	t.statsif.Register(stats.PutLatency, stats.KindLatency)
	t.statsif.Register(stats.GetColdCount, stats.KindCounter)
	t.statsif.Register(stats.GetColdSize, stats.KindCounter)
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
	t.statsif.Register(stats.GetRedirLatency, stats.KindLatency)
	t.statsif.Register(stats.PutRedirLatency, stats.KindLatency)
	t.statsif.Register(stats.RebalGlobalCount, stats.KindCounter)
	t.statsif.Register(stats.RebalLocalCount, stats.KindCounter)
	t.statsif.Register(stats.RebalGlobalSize, stats.KindCounter)
	t.statsif.Register(stats.RebalLocalSize, stats.KindCounter)
	t.statsif.Register(stats.ReplPutCount, stats.KindCounter)
	t.statsif.Register(stats.ReplPutLatency, stats.KindLatency)
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
func (t *targetrunner) register(keepalive bool, timeout time.Duration) (int, error) {
	var (
		newbucketmd bucketMD
		res         callResult
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
	// not being sent at cluster startup and keepalive..
	if len(res.outjson) > 0 {
		err := jsoniter.Unmarshal(res.outjson, &newbucketmd)
		cmn.Assert(err == nil, err)
		t.bmdowner.Lock()
		v := t.bmdowner.get().version()
		if v > newbucketmd.version() {
			glog.Errorf("register target - got bucket-metadata: local version %d > %d", v, newbucketmd.version())
		} else {
			glog.Infof("register target - got bucket-metadata: upgrading local version %d to %d", v, newbucketmd.version())
		}
		t.bmdowner.put(&newbucketmd)
		t.bmdowner.Unlock()
	}
	return 0, nil
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
			bucket := fwd.bucket
			bucketmd := t.bmdowner.get()
			if islocal := bucketmd.IsLocal(bucket); islocal {
				glog.Errorf("prefetch: bucket  %s is local, nothing to do", bucket)
			} else {
				for _, objname := range fwd.objnames {
					t.prefetchMissing(fwd.ctx, objname, bucket)
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

// GET /v1/objects/bucket[+"/"+objname]
// Checks if the object exists locally (if not, downloads it) and sends it back
// If the bucket is in the Cloud one and ValidateWarmGet is enabled there is an extra
// check whether the object exists locally. Version is checked as well if configured.
func (t *targetrunner) httpobjget(w http.ResponseWriter, r *http.Request) {
	var (
		bucket, objname    string
		errstr             string
		rangeOff, rangeLen int64
		lom                *cluster.LOM
		started            time.Time
		config             = cmn.GCO.Get()
		versioncfg         = &config.Ver
		ct                 = t.contextWithAuth(r)
		errcode            int
		coldget, vchanged  bool
	)
	//
	// 1. start, init lom, readahead
	//
	started = time.Now()
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname = apitems[0], apitems[1]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	query := r.URL.Query()
	rangeOff, rangeLen, errstr = t.offsetAndLength(query)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr)
		return
	}
	lom = &cluster.LOM{T: t, Bucket: bucket, Objname: objname}
	if errstr = lom.Fill(cluster.LomFstat, config); errstr != "" { // (doesnotexist|misplaced -> ok, other)
		t.invalmsghdlr(w, r, errstr)
		return
	}
	if !dryRun.disk && lom.Exists() {
		if x := query.Get(cmn.URLParamReadahead); x != "" { // FIXME
			t.readahead.ahead(lom.Fqn, rangeOff, rangeLen)
			return
		}
	}
	if glog.V(4) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, lom, pid)
	}
	if redelta := t.redirectLatency(started, query); redelta != 0 {
		t.statsif.Add(stats.GetRedirLatency, redelta)
	}
	if errstr, errcode = t.IsBucketLocal(bucket, lom.Bucketmd, query, lom.Bislocal); errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}
	//
	// 2. under lock: versioning, checksum, restore from cluster
	//
	t.rtnamemap.Lock(lom.Uname, false)

	if lom.Exists() {
		if errstr = lom.Fill(cluster.LomVersion | cluster.LomCksum); errstr != "" {
			t.rtnamemap.Unlock(lom.Uname, false)
			t.invalmsghdlr(w, r, errstr)
			return
		}

		// does not exist in the local bucket: restore from neighbors
		// Need to check if dryrun is enabled to stop from getting LOM
	} else if lom.Bislocal && !dryRun.disk && !dryRun.network {
		if errstr, errcode = t.restoreObjLocalBucket(lom, r); errstr == "" {
			t.objGetComplete(w, r, lom, started, rangeOff, rangeLen, false)
			t.rtnamemap.Unlock(lom.Uname, false)
			return
		}
		t.rtnamemap.Unlock(lom.Uname, false)
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}

	coldget = lom.DoesNotExist
	if !coldget && !lom.Bislocal { // exists && cloud-bucket: check ver if requested
		if versioncfg.ValidateWarmGet && (lom.Version != "" && versioningConfigured(lom.Bislocal)) {
			if vchanged, errstr, errcode = t.checkCloudVersion(ct, bucket, objname, lom.Version); errstr != "" {
				t.rtnamemap.Unlock(lom.Uname, false)
				t.invalmsghdlr(w, r, errstr, errcode)
				return
			}
			coldget = vchanged
		}
	}
	// checksum validation, if requested
	if !coldget && lom.Cksumcfg.ValidateWarmGet {
		errstr = lom.Fill(cluster.LomCksum | cluster.LomCksumPresentRecomp | cluster.LomCksumMissingRecomp)
		if lom.Badchecksum {
			glog.Errorln(errstr)
			if lom.Bislocal {
				if err := os.Remove(lom.Fqn); err != nil {
					glog.Warningf("%s - failed to remove, err: %v", errstr, err)
				}
				t.rtnamemap.Unlock(lom.Uname, false)
				t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
				return
			}
			coldget = true
		} else if errstr != "" {
			t.rtnamemap.Unlock(lom.Uname, false)
			t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
			return
		}
	}
	//
	// 3. coldget
	//
	if coldget && !dryRun.disk && !dryRun.network {
		t.rtnamemap.Unlock(lom.Uname, false)
		if errstr, errcode := t.getCold(ct, lom, false); errstr != "" {
			t.invalmsghdlr(w, r, errstr, errcode)
			return
		}
	}
	// 4. finally
	t.objGetComplete(w, r, lom, started, rangeOff, rangeLen, coldget)
	t.rtnamemap.Unlock(lom.Uname, false)
}

//
// 3a. attempt to restore an object that is missing in the LOCAL BUCKET - from:
//     1) local FS, 2) this cluster, 3) other tiers in the DC 4) from other
//		targets using erasure coding(if it is on)
// FIXME: must be done => (getfqn, and under write lock)
//
func (t *targetrunner) restoreObjLocalBucket(lom *cluster.LOM, r *http.Request) (errstr string, errcode int) {
	// check FS-wide if local rebalance is running
	aborted, running := t.xactions.isAbortedOrRunningLocalRebalance()
	if aborted || running {
		// FIXME: move this part to lom
		oldFQN, oldSize := t.getFromOtherLocalFS(lom)
		if oldFQN != "" {
			if glog.V(4) {
				glog.Infof("Restored (local rebalance in-progress?): %s (%s)", oldFQN, cmn.B2S(oldSize, 1))
			}
			lom.Fqn = oldFQN
			lom.Size = oldSize
			lom.DoesNotExist = false
			return
		}
	}
	// check cluster-wide when global rebalance is running
	aborted, running = t.xactions.isAbortedOrRunningRebalance()
	if aborted || running {
		// Do couple of retries in case the object was moved or is being moved right now.
		for retry := 0; retry < restoreMissingRetries; retry++ {
			if props, errs := t.getFromNeighbor(r, lom); errs == "" {
				lom.RestoredReceived(props)
				if glog.V(4) {
					glog.Infof("Restored (global rebalance in-progress?): %s (%s)", lom, cmn.B2S(lom.Size, 1))
				}
				return
			}

			time.Sleep(restoreMissingIntervals)
		}
	} else {
		if lom.Bprops != nil && lom.Bprops.NextTierURL != "" {
			var (
				props      *cluster.LOM
				inNextTier bool
			)
			if inNextTier, errstr, errcode = t.objectInNextTier(lom.Bprops.NextTierURL, lom.Bucket, lom.Objname); inNextTier {
				if props, errstr, errcode =
					t.getObjectNextTier(lom.Bprops.NextTierURL, lom.Bucket, lom.Objname, lom.Fqn); errstr == "" {
					lom.RestoredReceived(props)
					return
				}
			}
		}
	}

	// restore from existing EC slices if possible
	if ecErr := t.ecmanager.RestoreObject(lom); ecErr == nil {
		if glog.V(4) {
			glog.Infof("%s/%s is restored successfully", lom.Bucket, lom.Objname)
		}
		lom.Fill(cluster.LomFstat | cluster.LomAtime)
		lom.DoesNotExist = false
		return
	} else if ecErr != ec.ErrorECDisabled {
		errstr = fmt.Sprintf("Failed to restore object %s/%s: %v", lom.Bucket, lom.Objname, ecErr)
	}

	s := fmt.Sprintf("GET local: %s(%s) %s", lom, lom.Fqn, cmn.DoesNotExist)
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
// 4. read local, write http (note: coldget() keeps the read lock if successful)
//
func (t *targetrunner) objGetComplete(w http.ResponseWriter, r *http.Request, lom *cluster.LOM, started time.Time,
	rangeOff, rangeLen int64, coldget bool) {
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
		fqn         = lom.Fqn
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

	if lom.Nhobj != nil && !cksumRange {
		htype, hval := lom.Nhobj.Get()
		hdr.Add(cmn.HeaderObjCksumType, htype)
		hdr.Add(cmn.HeaderObjCksumVal, hval)
	}
	if lom.Version != "" {
		hdr.Add(cmn.HeaderObjVersion, lom.Version)
	}
	hdr.Add(cmn.HeaderObjSize, strconv.FormatInt(lom.Size, 10))

	// loopback if disk IO is disabled
	if dryRun.disk {
		if !dryRun.network {
			rd := readers.NewRandReader(dryRun.size)
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
	fqn = lom.ChooseMirror()
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
		buf, slab = gmem2.AllocFromSlab2(lom.Size)
	} else {
		buf, slab = gmem2.AllocFromSlab2(rangeLen)
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

	if !coldget {
		lom.UpdateAtime(started)
	}
	if glog.V(4) {
		s := fmt.Sprintf("GET: %s(%s), %d µs", lom, cmn.B2S(written, 1), int64(time.Since(started)/time.Microsecond))
		if coldget {
			s += " (cold)"
		}
		glog.Infoln(s)
	}

	delta := time.Since(started)
	t.statsif.AddMany(stats.NamedVal64{stats.GetCount, 1}, stats.NamedVal64{stats.GetLatency, int64(delta)})
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
	offsetStr, lengthStr := query.Get(cmn.URLParamOffset), query.Get(cmn.URLParamLength)
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
	pid := query.Get(cmn.URLParamProxyID)
	if glog.V(4) {
		glog.Infof("%s %s/%s <= %s", r.Method, bucket, objname, pid)
	}
	if pid == "" {
		t.invalmsghdlr(w, r, "PUT requests are expected to be redirected")
		return
	}
	if t.smapowner.get().GetProxy(pid) == nil {
		t.invalmsghdlr(w, r, fmt.Sprintf("PUT from an unknown proxy/gateway ID '%s' - Smap out of sync?", pid))
		return
	}

	if err, errCode := t.doPut(r, bucket, objname); err != nil {
		t.invalmsghdlr(w, r, err.Error(), errCode)
	}
}

// DELETE { action} /v1/objects/bucket-name
func (t *targetrunner) httpbckdelete(w http.ResponseWriter, r *http.Request) {
	var (
		bucket  string
		msg     cmn.ActionMsg
		started = time.Now()
		ok      = true
	)
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket = apitems[0]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	b, err := ioutil.ReadAll(r.Body)
	defer func() {
		if ok && err == nil && bool(glog.V(4)) {
			glog.Infof("DELETE list|range: %s, %d µs", bucket, int64(time.Since(started)/time.Microsecond))
		}
	}()
	if err == nil && len(b) > 0 {
		err = jsoniter.Unmarshal(b, &msg)
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
	if len(b) > 0 { // must be a List/Range request
		if err := t.listRangeOperation(r, apitems, msg); err != nil {
			t.invalmsghdlr(w, r, fmt.Sprintf("Failed to delete files: %v", err))
		}
		return
	}
	s := fmt.Sprintf("Invalid API request: no message body")
	t.invalmsghdlr(w, r, s)
	ok = false
}

// DELETE [ { action } ] /v1/objects/bucket-name/object-name
func (t *targetrunner) httpobjdelete(w http.ResponseWriter, r *http.Request) {
	var (
		msg     cmn.ActionMsg
		evict   bool
		started = time.Now()
		ok      = true
	)
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	if !t.validatebckname(w, r, bucket) {
		return
	}

	b, err := ioutil.ReadAll(r.Body)
	defer func() {
		if ok && err == nil && bool(glog.V(4)) {
			glog.Infof("DELETE: %s/%s, %d µs", bucket, objname, int64(time.Since(started)/time.Microsecond))
		}
	}()
	if err == nil && len(b) > 0 {
		err = jsoniter.Unmarshal(b, &msg)
		if err == nil {
			evict = (msg.Action == cmn.ActEvict)
		}
	} else if err != nil {
		s := fmt.Sprintf("objDelete: Failed to read %s request, err: %v", r.Method, err)
		if err == io.EOF {
			trailer := r.Trailer.Get("Error")
			if trailer != "" {
				s = fmt.Sprintf("objDelete: Failed to read %s request, err: %v, trailer: %s", r.Method, err, trailer)
			}
		}
		t.invalmsghdlr(w, r, s)
		return
	}
	if objname != "" {
		lom := &cluster.LOM{T: t, Bucket: bucket, Objname: objname}
		if errstr := lom.Fill(0); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
		}

		err := t.objDelete(t.contextWithAuth(r), bucket, objname, evict)
		if err != nil {
			s := fmt.Sprintf("Error deleting %s/%s: %v", bucket, objname, err)
			t.invalmsghdlr(w, r, s)
			return
		}

		// EC cleanup if EC is enabled
		t.ecmanager.CleanupObject(lom)
		return
	}
	s := fmt.Sprintf("Invalid API request: No object name or message body.")
	t.invalmsghdlr(w, r, s)
	ok = false
}

// POST /v1/buckets/bucket-name
func (t *targetrunner) httpbckpost(w http.ResponseWriter, r *http.Request) {
	started := time.Now()
	var msg cmn.ActionMsg
	if cmn.ReadJSON(w, r, &msg) != nil {
		return
	}
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}

	switch msg.Action {
	case cmn.ActPrefetch:
		if err := t.listRangeOperation(r, apitems, msg); err != nil {
			t.invalmsghdlr(w, r, fmt.Sprintf("Failed to prefetch files: %v", err))
		}
	case cmn.ActRenameLB:
		lbucket := apitems[0]
		if !t.validatebckname(w, r, lbucket) {
			return
		}
		bucketFrom, bucketTo := lbucket, msg.Name

		t.bmdowner.Lock() // lock#1 begin

		bucketmd := t.bmdowner.get()
		props, ok := bucketmd.Get(bucketFrom, true)
		if !ok {
			t.bmdowner.Unlock()
			s := fmt.Sprintf("Local bucket %s %s", bucketFrom, cmn.DoesNotExist)
			t.invalmsghdlr(w, r, s)
			return
		}
		bmd4ren, ok := msg.Value.(map[string]interface{})
		if !ok {
			t.bmdowner.Unlock()
			t.invalmsghdlr(w, r, fmt.Sprintf("Unexpected Value format %+v, %T", msg.Value, msg.Value))
			return
		}
		v1, ok1 := bmd4ren["version"]
		v2, ok2 := v1.(float64)
		if !ok1 || !ok2 {
			t.bmdowner.Unlock()
			t.invalmsghdlr(w, r, fmt.Sprintf("Invalid Value format (%+v, %T), (%+v, %T)", v1, v1, v2, v2))
			return
		}
		version := int64(v2)
		if bucketmd.version() != version {
			glog.Warningf("bucket-metadata version %d != %d - proceeding to rename anyway", bucketmd.version(), version)
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

		glog.Infof("renamed local bucket %s => %s, bucket-metadata version %d", bucketFrom, bucketTo, clone.version())
	case cmn.ActListObjects:
		lbucket := apitems[0]
		if !t.validatebckname(w, r, lbucket) {
			return
		}
		// list the bucket and return
		tag, ok := t.listbucket(w, r, lbucket, &msg)
		if ok {
			delta := time.Since(started)
			t.statsif.AddMany(stats.NamedVal64{stats.ListCount, 1}, stats.NamedVal64{stats.ListLatency, int64(delta)})
			if glog.V(4) {
				glog.Infof("LIST %s: %s, %d µs", tag, lbucket, int64(delta/time.Microsecond))
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
		islocal := bucketmd.IsLocal(bucket)
		t.xactions.renewEraseCopies(bucket, t, islocal)
	default:
		t.invalmsghdlr(w, r, "Unexpected action "+msg.Action)
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
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	query := r.URL.Query()
	if glog.V(4) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, bucket, pid)
	}
	bucketmd := t.bmdowner.get()
	islocal := bucketmd.IsLocal(bucket)
	errstr, errcode := t.IsBucketLocal(bucket, &bucketmd.BMD, query, islocal)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}

	if !islocal {
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
	if !versioningConfigured(islocal) {
		bucketprops[cmn.HeaderVersioning] = cmn.VersionNone
	}

	hdr := w.Header()
	for k, v := range bucketprops {
		hdr.Add(k, v)
	}

	// include bucket's own config override
	props, ok := bucketmd.Get(bucket, islocal)
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
	checkCached, _ = parsebool(r.URL.Query().Get(cmn.URLParamCheckCached))
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname = apitems[0], apitems[1]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	lom := &cluster.LOM{T: t, Bucket: bucket, Objname: objname}
	if errstr = lom.Fill(cluster.LomFstat | cluster.LomVersion); errstr != "" { // (doesnotexist -> ok, other)
		t.invalmsghdlr(w, r, errstr)
		return
	}
	query := r.URL.Query()
	if glog.V(4) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, lom, pid)
	}
	errstr, errcode = t.IsBucketLocal(bucket, lom.Bucketmd, query, lom.Bislocal)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}
	if lom.Bislocal || checkCached {
		if lom.DoesNotExist {
			status := http.StatusNotFound
			http.Error(w, http.StatusText(status), status)
			return
		} else if checkCached {
			return
		}
		objmeta = make(cmn.SimpleKVs)
		objmeta[cmn.HeaderObjSize] = strconv.FormatInt(lom.Size, 10)
		objmeta[cmn.HeaderObjVersion] = lom.Version
		if glog.V(4) {
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
	query := r.URL.Query()
	from := query.Get(cmn.URLParamFromID)

	aborted, running := t.xactions.isAbortedOrRunningRebalance()
	if !aborted && !running {
		aborted, running = t.xactions.isAbortedOrRunningLocalRebalance()
	}
	status := &thealthstatus{IsRebalancing: aborted || running}

	jsbytes, err := jsoniter.Marshal(status)
	cmn.Assert(err == nil, err)
	if ok := t.writeJSON(w, r, jsbytes, "thealthstatus"); !ok {
		return
	}

	t.keepalive.heardFrom(from, false)
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
			return fmt.Errorf("Unexpected: bucket %s != %s bucketFrom", bucket, renctx.bucketFrom)
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

func (t *targetrunner) getFromNeighbor(r *http.Request, lom *cluster.LOM) (rlom *cluster.LOM, errstr string) {
	neighsi := t.lookupRemotely(lom)
	if neighsi == nil {
		errstr = fmt.Sprintf("Failed cluster-wide lookup %s", lom)
		return
	}
	if glog.V(4) {
		glog.Infof("Found %s at %s", lom, neighsi)
	}

	// FIXME: this code below looks like a general code for sending request
	geturl := fmt.Sprintf("%s%s?%s=%t", neighsi.PublicNet.DirectURL, r.URL.Path, cmn.URLParamLocal, lom.Bislocal)
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
		hval    = response.Header.Get(cmn.HeaderObjCksumVal)
		htype   = response.Header.Get(cmn.HeaderObjCksumType)
		hksum   = cmn.NewCksum(htype, hval)
		version = response.Header.Get(cmn.HeaderObjVersion)
		workFQN = fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfileRemote)
	)

	// FIXME: Similarly to the Fill(...) we could have: Clone(...) were we would specify what needs to be cloned.
	rlom = &cluster.LOM{}
	*rlom = *lom
	rlom.Nhobj = hksum
	rlom.Version = version

	roi := &recvObjInfo{
		t:        t,
		workFQN:  workFQN,
		migrated: true,
		r:        response.Body,
		lom:      rlom,
	}

	if err = roi.writeToFile(); err != nil { // FIXME: transfer atime as well
		errstr = err.Error()
		return
	}
	// commit
	if err = cmn.MvFile(workFQN, rlom.Fqn); err != nil {
		errstr = err.Error()
		return
	}
	if errstr = rlom.Persist(); errstr != "" {
		return
	}
	if glog.V(4) {
		glog.Infof("Success: %s (%s, %s) from %s", rlom, cmn.B2S(rlom.Size, 1), rlom.Nhobj, neighsi)
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
	if errstr = lom.Fill(cluster.LomFstat | cluster.LomCksum | cluster.LomVersion); errstr != "" {
		glog.Warning(errstr)
		lom.DoesNotExist = true // NOTE: in an attempt to fix it
	}

	// existence, access & versioning
	coldget := lom.DoesNotExist
	if !coldget {
		if versioncfg.ValidateWarmGet && lom.Version != "" && versioningConfigured(lom.Bislocal) {
			vchanged, errv, _ = t.checkCloudVersion(ct, lom.Bucket, lom.Objname, lom.Version)
			if errv == "" {
				coldget = vchanged
			}
		}
		if !coldget && lom.Cksumcfg.ValidateWarmGet {
			errstr := lom.Fill(cluster.LomCksum | cluster.LomCksumPresentRecomp | cluster.LomCksumMissingRecomp)
			if lom.Badchecksum {
				coldget = true
			} else if errstr == "" {
				if lom.Nhobj == nil {
					coldget = true
				}
			} else {
				glog.Warningf("Failed to validate checksum, err: %s", errstr)
			}
		}
	}
	getfqn := fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfileColdget)
	if !coldget {
		_ = lom.Fill(cluster.LomCksum)
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
				if props, errstr, errcode =
					t.getObjectNextTier(lom.Bprops.NextTierURL, lom.Bucket, lom.Objname, getfqn); errstr == "" {
					coldget = false
				}
			}
		}
	}
	//
	// cloud
	//
	if coldget {
		if props, errstr, errcode = getcloudif().getobj(ct, getfqn, lom.Bucket, lom.Objname); errstr != "" {
			t.rtnamemap.Unlock(lom.Uname, true)
			return
		}
	}
	defer func() {
		if errstr != "" {
			t.rtnamemap.Unlock(lom.Uname, true)
			if errRemove := os.Remove(getfqn); errRemove != nil {
				glog.Errorf("Nested error %s => (remove %s => err: %v)", errstr, getfqn, errRemove)
				t.fshc(errRemove, getfqn)
			}
		}
	}()
	if err = cmn.MvFile(getfqn, lom.Fqn); err != nil {
		errstr = fmt.Sprintf("Unexpected failure to rename %s => %s, err: %v", getfqn, lom.Fqn, err)
		t.fshc(err, lom.Fqn)
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
	islocal := t.bmdowner.get().IsLocal(bucket)
	for contentType, contentResolver := range fs.CSM.RegisteredContentTypes {
		if !contentResolver.PermToProcess() {
			continue
		}
		for _, mpathInfo := range availablePaths {
			wg.Add(1)
			dir := mpathInfo.MakePathBucket(contentType, bucket, islocal)
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
				return nil, fmt.Errorf("Failed to read %s", r.failedPath)
			} else {
				continue
			}
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

	bucketnames := &cmn.BucketNames{
		Local: make([]string, 0, len(bucketmd.LBmap)),
		Cloud: make([]string, 0, 64),
	}
	for bucket := range bucketmd.LBmap {
		bucketnames.Local = append(bucketnames.Local, bucket)
	}

	q := r.URL.Query()
	localonly, _ := parsebool(q.Get(cmn.URLParamLocal))
	if !localonly {
		buckets, errstr, errcode := getcloudif().getbucketnames(t.contextWithAuth(r))
		if errstr != "" {
			t.invalmsghdlr(w, r, errstr, errcode)
			return
		}
		bucketnames.Cloud = buckets
	}

	jsbytes, err := jsoniter.Marshal(bucketnames)
	cmn.Assert(err == nil, err)
	t.writeJSON(w, r, jsbytes, "getbucketnames")
}

func (t *targetrunner) doLocalBucketList(w http.ResponseWriter, r *http.Request, bucket string, msg *cmn.GetMsg) (errstr string, ok bool) {
	reslist, err := t.prepareLocalObjectList(bucket, msg)
	if err != nil {
		errstr = fmt.Sprintf("List local bucket %s failed, err: %v", bucket, err)
		return
	}
	jsbytes, err := jsoniter.Marshal(reslist)
	cmn.Assert(err == nil, err)
	ok = t.writeJSON(w, r, jsbytes, "listbucket")
	return
}

// List bucket returns a list of objects in a bucket (with optional prefix)
// Special case:
// If URL contains cachedonly=true then the function returns the list of
// locally cached objects. Paging is used to return a long list of objects
func (t *targetrunner) listbucket(w http.ResponseWriter, r *http.Request, bucket string, actionMsg *cmn.ActionMsg) (tag string, ok bool) {
	var (
		jsbytes []byte
		errstr  string
		errcode int
	)
	query := r.URL.Query()
	if glog.V(4) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, bucket, pid)
	}
	bucketmd := t.bmdowner.get()
	islocal := bucketmd.IsLocal(bucket)
	errstr, errcode = t.IsBucketLocal(bucket, &bucketmd.BMD, query, islocal)
	if errstr != "" {
		t.invalmsghdlr(w, r, errstr, errcode)
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
	if islocal {
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
//  - Object name is not in early processed directories by the previos call:
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
	relname := lom.Fqn[ci.rootLength:]
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
		lom.Fill(lomAction)
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
	if ci.needChkSum && lom.Nhobj != nil {
		_, storedCksum := lom.Nhobj.Get()
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
	ci.lastFilePath = lom.Fqn
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
		lom       = &cluster.LOM{T: ci.t, Fqn: fqn}
	)
	errstr := lom.Fill(cluster.LomFstat | cluster.LomCopy)
	if lom.DoesNotExist {
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

	roi := &recvObjInfo{
		t:            t,
		objname:      objname,
		bucket:       bucket,
		r:            r.Body,
		cksumToCheck: cksum,
		ctx:          t.contextWithAuth(r),
	}
	if err := roi.init(); err != nil {
		return err, http.StatusInternalServerError
	}

	return roi.recv()
}

// TODO: this function is for now unused because replication does not work
func (t *targetrunner) doReplicationPut(r *http.Request, bucket, objname, replicaSrc string) (errstr string) {

	var (
		started = time.Now()
		islocal = t.bmdowner.get().IsLocal(bucket)
	)

	fqn, errstr := cluster.FQN(fs.ObjectType, bucket, objname, islocal)
	if errstr != "" {
		return errstr
	}
	err := getreplicationrunner().reqReceiveReplica(replicaSrc, fqn, r)

	if err != nil {
		return err.Error()
	}

	delta := time.Since(started)
	t.statsif.AddMany(stats.NamedVal64{stats.ReplPutCount, 1}, stats.NamedVal64{stats.ReplPutLatency, int64(delta)})
	if glog.V(4) {
		glog.Infof("Replication PUT: %s/%s, %d µs", bucket, objname, int64(delta/time.Microsecond))
	}
	return ""
}

func (roi *recvObjInfo) init() error {
	cmn.Assert(roi.t != nil)

	roi.started = time.Now()
	roi.lom = &cluster.LOM{T: roi.t, Objname: roi.objname, Bucket: roi.bucket}
	if errstr := roi.lom.Fill(cluster.LomFstat); errstr != "" {
		return errors.New(errstr)
	}
	roi.workFQN = fs.CSM.GenContentParsedFQN(roi.lom.ParsedFQN, fs.WorkfileType, fs.WorkfilePut)

	// All objects which are migrated should contain checksum.
	if roi.migrated {
		cmn.Assert(roi.cksumToCheck != nil)
	}

	return nil
}

func (roi *recvObjInfo) recv() (err error, errCode int) {
	cmn.Assert(roi.lom != nil)

	// optimize out if the checksums do match
	if roi.lom.Exists() {
		if errstr := roi.lom.Fill(cluster.LomCksum); errstr == "" {
			if cmn.EqCksum(roi.lom.Nhobj, roi.cksumToCheck) {
				glog.Infof("Existing %s is valid: PUT is a no-op", roi.lom)
				io.Copy(ioutil.Discard, roi.r) // drain the reader
				return nil, 0
			}
		}
	}

	if err = roi.writeToFile(); err != nil {
		return err, http.StatusInternalServerError
	}

	// commit
	if !dryRun.disk && !dryRun.network {
		if errstr, errCode := roi.commit(); errstr != "" {
			return errors.New(errstr), errCode
		}
	}

	delta := time.Since(roi.started)
	roi.t.statsif.AddMany(stats.NamedVal64{stats.PutCount, 1}, stats.NamedVal64{stats.PutLatency, int64(delta)})
	if glog.V(4) {
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
		roi.lom.DoesNotExist = false
	}

	if !roi.migrated && errstr == "" {
		if err := roi.t.ecmanager.EncodeObject(roi.lom); err != nil && err != ec.ErrorECDisabled {
			errstr = err.Error()
		}
	}

	return
}

func (roi *recvObjInfo) tryCommit() (errstr string, errCode int) {
	if !roi.lom.Bislocal && !roi.migrated {
		if file, err := os.Open(roi.workFQN); err != nil {
			errstr = fmt.Sprintf("Failed to open %s err: %v", roi.workFQN, err)
			return
		} else {
			cmn.Assert(roi.lom.Nhobj != nil)
			roi.lom.Version, errstr, errCode = getcloudif().putobj(roi.ctx, file, roi.lom.Bucket, roi.lom.Objname, roi.lom.Nhobj)
			file.Close()
			if errstr != "" {
				return
			}
		}
	}

	// Lock the uname for the object and rename it from workFQN to actual FQN.
	roi.t.rtnamemap.Lock(roi.lom.Uname, true)
	if roi.lom.Bislocal && versioningConfigured(true) {
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
	if err := cmn.MvFile(roi.workFQN, roi.lom.Fqn); err != nil {
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
	if dryRun.disk || !lom.Mirror.MirrorEnabled {
		return
	}

	if fs.Mountpaths.NumAvail() <= 1 { // FIXME: cache when filling-in the lom
		return
	}

	if t.xcopy == nil || t.xcopy.Finished() || t.xcopy.Bucket() != lom.Bucket || t.xcopy.Bislocal != lom.Bislocal {
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
		cksumToCheck: cmn.NewCksum(hdr.ObjAttrs.CksumType, hdr.ObjAttrs.Cksum),
	}
	if err := roi.init(); err != nil {
		glog.Error(err)
		return
	}
	roi.lom.Atime = time.Unix(0, hdr.ObjAttrs.Atime)
	roi.lom.Version = hdr.ObjAttrs.Version

	if glog.V(4) {
		glog.Infof("Rebalance %s from %s", roi.lom, hdr.Opaque)
	}

	if err, _ := roi.recv(); err != nil {
		glog.Error(err)
		return
	}

	t.statsif.AddMany(stats.NamedVal64{stats.RxCount, 1}, stats.NamedVal64{stats.RxSize, hdr.ObjAttrs.Size})
}

func (t *targetrunner) objDelete(ct context.Context, bucket, objname string, evict bool) error {
	var (
		errstr  string
		errcode int
		lom     = &cluster.LOM{T: t, Bucket: bucket, Objname: objname}
	)
	if errstr = lom.Fill(0); errstr != "" {
		return errors.New(errstr)
	}
	t.rtnamemap.Lock(lom.Uname, true)
	defer t.rtnamemap.Unlock(lom.Uname, true)

	if !lom.Bislocal && !evict {
		if errstr, errcode = getcloudif().deleteobj(ct, bucket, objname); errstr != "" {
			if errcode == 0 {
				return fmt.Errorf("%s", errstr)
			}
			return fmt.Errorf("%d: %s", errcode, errstr)
		}
		t.statsif.Add(stats.DeleteCount, 1)
	}

	_ = lom.Fill(cluster.LomFstat | cluster.LomCopy) // ignore fstat errors
	if lom.DoesNotExist {
		return nil
	}
	if !(evict && lom.Bislocal) {
		// Don't evict from a local bucket (this would be deletion)
		if lom.HasCopy() {
			if errstr := lom.DelCopy(); errstr != "" {
				return errors.New(errstr)
			}
		}

		if err := os.Remove(lom.Fqn); err != nil {
			return err
		} else if evict {
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
	islocalFrom := bucketmd.IsLocal(bucketFrom)
	fqn, errstr := cluster.FQN(contentType, bucketFrom, objnameFrom, islocalFrom)
	if errstr != "" {
		return
	}
	if fi, err = os.Stat(fqn); err != nil {
		errstr = fmt.Sprintf("Rename/move: failed to fstat %s (%s/%s), err: %v", fqn, bucketFrom, objnameFrom, err)
		return
	}

	// local rename
	if si.DaemonID == t.si.DaemonID {
		islocalTo := bucketmd.IsLocal(bucketTo)
		newFQN, errstr = cluster.FQN(contentType, bucketTo, objnameTo, islocalTo)
		if errstr != "" {
			return
		}
		if err := cmn.MvFile(fqn, newFQN); err != nil {
			errstr = fmt.Sprintf("Rename object %s/%s: %v", bucketFrom, objnameFrom, err)
		} else {
			t.statsif.Add(stats.RenameCount, 1)
			if glog.V(3) {
				glog.Infof("Renamed %s => %s", fqn, newFQN)
			}
		}
		return
	}

	// migrate to another target
	glog.Infof("Migrating %s/%s at %s => %s/%s at %s", bucketFrom, objnameFrom, t.si, bucketTo, objnameTo, si)

	if file, err = cmn.NewFileHandle(fqn); err != nil {
		return fmt.Sprintf("failed to open %s, err: %v", fqn, err)
	}
	defer file.Close()

	lom := &cluster.LOM{T: t, Fqn: fqn}
	if errstr := lom.Fill(cluster.LomFstat | cluster.LomVersion | cluster.LomAtime | cluster.LomCksum | cluster.LomCksumMissingRecomp); errstr != "" {
		return errstr
	}

	cksumType, cksum := lom.Nhobj.Get()
	hdr := transport.Header{
		Bucket:  bucketTo,
		Objname: objnameTo,
		IsLocal: lom.Bislocal,
		Opaque:  []byte(t.si.DaemonID),
		ObjAttrs: transport.ObjectAttrs{
			Size:      fi.Size(),
			Atime:     lom.Atime.UnixNano(),
			CksumType: cksumType,
			Cksum:     cksum,
			Version:   lom.Version,
		},
	}
	wg := &sync.WaitGroup{}
	wg.Add(1)
	cb := func(hdr transport.Header, r io.ReadCloser, cbErr error) {
		err = cbErr
		wg.Done()
	}
	if err := t.streams.rebalance.SendV(hdr, file, cb, si); err != nil {
		glog.Errorf("failed to migrate: %s, err: %v", lom.Fqn, err)
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

func (t *targetrunner) IsBucketLocal(bucket string, bmd *cluster.BMD, q url.Values, islocal bool) (errstr string, errcode int) {
	proxylocalstr := q.Get(cmn.URLParamLocal)
	if proxylocalstr == "" {
		return
	}
	proxylocal, err := parsebool(proxylocalstr)
	if err != nil {
		errstr = fmt.Sprintf("Invalid URL query parameter for bucket %s: %s=%s (expecting bool)", bucket, cmn.URLParamLocal, proxylocalstr)
		errcode = http.StatusInternalServerError
		return
	}
	if islocal == proxylocal {
		return
	}
	errstr = fmt.Sprintf("%s: bucket %s is-local mismatch: (proxy) %t != %t (self)", t.si, bucket, proxylocal, islocal)
	errcode = http.StatusInternalServerError
	if s := q.Get(cmn.URLParamBMDVersion); s != "" {
		if v, err := strconv.ParseInt(s, 0, 64); err != nil {
			glog.Errorf("Unexpected: failed to convert %s to int, err: %v", s, err)
		} else if v < bmd.Version {
			glog.Errorf("bucket-metadata v%d > v%d (primary)", bmd.Version, v)
		} else if v > bmd.Version { // fixup
			glog.Errorf("Warning: bucket-metadata v%d < v%d (primary) - updating...", bmd.Version, v)
			t.bmdVersionFixup()
			islocal := t.bmdowner.get().IsLocal(bucket)
			if islocal == proxylocal {
				glog.Infof("Success: updated bucket-metadata to v%d - resolved 'islocal' mismatch", bmd.Version)
				errstr, errcode = "", 0
			}
		}
	}
	return
}

func (t *targetrunner) bmdVersionFixup() {
	smap := t.smapowner.get()
	if smap == nil || !smap.isValid() {
		return
	}
	psi := smap.ProxySI
	q := url.Values{}
	q.Add(cmn.URLParamWhat, cmn.GetWhatBucketMeta)
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
		glog.Errorf("Failed to get-what=%s, err: %v", cmn.GetWhatBucketMeta, res.err)
		return
	}
	newbucketmd := &bucketMD{}
	err := jsoniter.Unmarshal(res.outjson, newbucketmd)
	if err != nil {
		glog.Errorf("Unexpected: failed to unmarshal get-what=%s response from %s, err: %v [%v]",
			cmn.GetWhatBucketMeta, psi.IntraControlNet.DirectURL, err, string(res.outjson))
		return
	}
	var msg = cmn.ActionMsg{Action: "get-what=" + cmn.GetWhatBucketMeta}
	t.receiveBucketMD(newbucketmd, &msg, "fixup")
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
			glog.Infof("%s: %s v%d done", t.si, cmn.SyncSmap, newsmap.version())
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
		if glog.V(4) {
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
		cmn.Assert(err == nil, err)
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
			cmn.Assert(err == nil, err)
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
			Id:        xaction.ID(),
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
		glog.Infof("Registered self %s", t.si)
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
		reader = readers.NewRandReader(dryRun.size)
	}
	if !dryRun.disk {
		if file, err = cmn.CreateFile(roi.workFQN); err != nil {
			roi.t.fshc(err, roi.workFQN)
			return fmt.Errorf("Failed to create %s, err: %s", roi.workFQN, err)
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
		expectedCksum       cmn.CksumValue
		saveHash, checkHash hash.Hash
		hashes              []hash.Hash
	)

	if !roi.cold && roi.lom.Cksumcfg.Checksum != cmn.ChecksumNone && (!roi.migrated || roi.lom.Cksumcfg.ValidateClusterMigration) {
		checkCksumType = roi.lom.Cksumcfg.Checksum
		cmn.Assert(checkCksumType == cmn.ChecksumXXHash, checkCksumType)

		saveHash = xxhash.New64()
		hashes = []hash.Hash{saveHash}

		// if sender provided checksum we need to ensure that it is correct
		if expectedCksum = roi.cksumToCheck; expectedCksum != nil {
			checkHash = saveHash
		}
	} else if roi.cold {
		// by default we should calculate xxhash and save it together with file
		saveHash = xxhash.New64()
		hashes = []hash.Hash{saveHash}

		// if configured and the cksum is provied we should also check md5 hash (aws, gcp)
		if roi.lom.Cksumcfg.ValidateColdGet && roi.cksumToCheck != nil {
			expectedCksum = roi.cksumToCheck
			checkCksumType, _ = expectedCksum.Get()
			cmn.Assert(checkCksumType == cmn.ChecksumMD5, checkCksumType)

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
			err = fmt.Errorf("Bad checksum expected %s, got: %s; workFQN: %q", expectedCksum.String(), computedCksum.String(), roi.workFQN)
			roi.t.statsif.AddMany(stats.NamedVal64{stats.ErrCksumCount, 1}, stats.NamedVal64{stats.ErrCksumSize, written})
			return
		}
	}
	if saveHash != nil {
		roi.lom.Nhobj = cmn.NewCksum(cmn.ChecksumXXHash, cmn.HashToStr(saveHash))
	}

	if err = file.Close(); err != nil {
		return fmt.Errorf("Failed to close received file %s, err: %v", roi.workFQN, err)
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
		cmn.Assert(err == nil, err)
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

		glog.Errorf("%s: detected change in the mountpath configuration at %s", t.si, mpathconfigfqn)
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
		return nil, fmt.Errorf("Failed to decrypt token [%s]: %v", token, err)
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
func (t *targetrunner) receiveBucketMD(newbucketmd *bucketMD, msg *cmn.ActionMsg, tag string) (errstr string) {
	if msg.Action == "" {
		glog.Infof("%s bucket-metadata: version %d", tag, newbucketmd.version())
	} else {
		glog.Infof("%s bucket-metadata: version %d, action %s", tag, newbucketmd.version(), msg.Action)
	}
	t.bmdowner.Lock()
	bucketmd := t.bmdowner.get()
	myver := bucketmd.version()
	if newbucketmd.version() <= myver {
		t.bmdowner.Unlock()
		if newbucketmd.version() < myver {
			errstr = fmt.Sprintf("Attempt to downgrade bucket-metadata version %d to %d", myver, newbucketmd.version())
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

func (t *targetrunner) receiveSmap(newsmap *smapX, msg *cmn.ActionMsg) (errstr string) {
	var (
		newtargetid, s                 string
		existentialQ, aborted, running bool
		pid                            = newsmap.ProxySI.DaemonID
	)
	action, id := path.Split(msg.Action)
	if action != "" {
		newtargetid = id
	}
	if msg.Action != "" {
		s = ", action " + msg.Action
	}
	glog.Infof("receive Smap: v%d, ntargets %d, primary %s%s", newsmap.version(), newsmap.CountTargets(), pid, s)

	for id, si := range newsmap.Tmap { // log
		if id == t.si.DaemonID {
			existentialQ = true
			glog.Infof("target: %s <= self", si)
		} else {
			glog.Infof("target: %s", si)
		}
	}
	if !existentialQ {
		errstr = fmt.Sprintf("Not finding self %s in the new %s", t.si, newsmap.pp())
		return
	}
	if errstr = t.smapowner.synchronize(newsmap, false /*saveSmap*/, true /* lesserIsErr */); errstr != "" {
		return
	}
	if msg.Action == cmn.ActGlobalReb {
		go t.runRebalance(newsmap, newtargetid)
		return
	}
	if !cmn.GCO.Get().Rebalance.Enabled {
		glog.Infoln("auto-rebalancing disabled")
		return
	}
	if newtargetid == "" {
		return
	}

	glog.Infof("receiveSmap: newtargetid=%s", newtargetid)
	if atomic.LoadInt64(&t.clusterStarted) != 0 {
		go t.runRebalance(newsmap, newtargetid) // auto-rebalancing xaction
		return
	}

	aborted, running = t.xactions.isAbortedOrRunningRebalance()
	if !aborted || running {
		glog.Infof("the cluster is starting up, rebalancing=(aborted=%t, running=%t)", aborted, running)
		return
	}
	// resume global rebalance
	f := func() {
		glog.Infoln("waiting for cluster startup to resume rebalance...")
		for {
			time.Sleep(time.Second)
			if atomic.LoadInt64(&t.clusterStarted) != 0 {
				break
			}
		}
		glog.Infoln("resuming rebalance...")
		t.runRebalance(t.smapowner.get(), "")
	}
	go runRebalanceOnce.Do(f) // only once at startup
	return
}

func (t *targetrunner) pollClusterStarted() {
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
			timeout: defaultTimeout,
		}
		res := t.call(args)
		if res.err != nil {
			continue
		}
		proxystats := &stats.ProxyCoreStats{} // FIXME
		err := jsoniter.Unmarshal(res.outjson, proxystats)
		if err != nil {
			glog.Errorf("Unexpected: failed to unmarshal %s response, err: %v [%v]", psi.PublicNet.DirectURL, err, string(res.outjson))
			continue
		}
		if proxystats.Tracker[stats.Uptime].Value != 0 {
			break
		}
	}
	atomic.StoreInt64(&t.clusterStarted, 1)
	glog.Infoln("cluster started up")
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

	glog.Info("Disabling the target")
	for i := 0; i < maxRetrySeconds; i++ {
		if status, eunreg = t.unregister(); eunreg != nil {
			if cmn.IsErrConnectionRefused(eunreg) || status == http.StatusRequestTimeout {
				glog.Errorf("Target %s: retrying unregistration...", t.si)
				time.Sleep(time.Second)
				continue
			}
		}
		break
	}

	if status >= http.StatusBadRequest || eunreg != nil {
		// do not update state on error
		if eunreg == nil {
			eunreg = fmt.Errorf("Error unregistering target, http error code: %d", status)
		}
		return eunreg
	}

	t.regstate.disabled = true
	glog.Info("Target has been disabled")
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

	glog.Info("Enabling the target")
	for i := 0; i < maxRetrySeconds; i++ {
		if status, ereg = t.register(false, defaultTimeout); ereg != nil {
			if cmn.IsErrConnectionRefused(ereg) || status == http.StatusRequestTimeout {
				glog.Errorf("Target %s: retrying registration...", t.si)
				time.Sleep(time.Second)
				continue
			}
		}
		break
	}

	if status >= http.StatusBadRequest || ereg != nil {
		// do not update state on error
		if ereg == nil {
			ereg = fmt.Errorf("Error registering target, http error code: %d", status)
		}
		return ereg
	}

	t.regstate.disabled = false
	glog.Info("Target has been enabled")
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
		filePath := mpathInfo.MakePathBucketObject(fs.ObjectType, lom.Bucket, lom.Objname, lom.Bislocal)
		stat, err := os.Stat(filePath)
		if err == nil {
			return filePath, stat.Size()
		}
	}
	return
}
