// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dfc

import (
	"context"
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/atime"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/dfc/util/readers"
	"github.com/NVIDIA/dfcpub/dsort"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/lru"
	"github.com/NVIDIA/dfcpub/memsys"
	"github.com/NVIDIA/dfcpub/stats"
	"github.com/NVIDIA/dfcpub/stats/statsd"
	"github.com/NVIDIA/dfcpub/transport"
	"github.com/OneOfOne/xxhash"
	jsoniter "github.com/json-iterator/go"
)

const (
	internalPageSize = 10000     // number of objects in a page for internal call between target and proxy to get atime/iscached
	MaxPageSize      = 64 * 1024 // max number of objects in a page (warning logged if requested page size exceeds this limit)
	maxBytesInMem    = 256 * cmn.KiB
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
	targetrunner struct {
		httprunner
		cloudif        cloudif // multi-cloud backend
		uxprocess      *uxprocess
		rtnamemap      *rtnamemap
		prefetchQueue  chan filesWithDeadline
		authn          *authManager
		clusterStarted int64
		regstate       regstate // registration state - the state of being registered (with the proxy) or maybe not
		fsprg          fsprungroup
		readahead      readaheader
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

	if err := fs.Mountpaths.CreateBucketDir(fs.BucketLocalType); err != nil {
		glog.Error(err)
		os.Exit(1)
	}
	if err := fs.Mountpaths.CreateBucketDir(fs.BucketCloudType); err != nil {
		glog.Error(err)
		os.Exit(1)
	}
	t.detectMpathChanges()

	// cloud provider
	if config.CloudProvider == cmn.ProviderAmazon {
		// TODO: sessions
		t.cloudif = &awsimpl{t}

	} else {
		cmn.Assert(config.CloudProvider == cmn.ProviderGoogle)
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

	glog.Infof("target %s is ready", t.si)
	glog.Flush()
	pid := int64(os.Getpid())
	t.uxprocess = &uxprocess{time.Now(), strconv.FormatInt(pid, 16), pid}

	getfshealthchecker().SetDispatcher(t)

	aborted, _ := t.xactions.isAbortedOrRunningLocalRebalance()
	if aborted {
		// resume local rebalance
		f := func() {
			glog.Infoln("resuming local rebalance...")
			t.runLocalRebalance()
		}
		go runLocalRebalanceOnce.Do(f) // only once at startup
	}

	return t.httprunner.run()
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
	xlru := t.xactions.renewLRU()
	if xlru == nil {
		return
	}
	ini := lru.InitLRU{
		Xlru:        xlru,
		Namelocker:  t.rtnamemap,
		Statsif:     t.statsif,
		T:           t,
		CtxResolver: fs.CSM,
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
	if errstr = lom.Fill(cluster.LomFstat); errstr != "" { // (doesnotexist -> ok, other)
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
	errstr, errcode = t.IsBucketLocal(bucket, lom.Bucketmd, query, lom.Bislocal)
	if errstr != "" {
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
	}
	if lom.Doesnotexist && lom.Bislocal { // does not exist in the local bucket: restore from neighbors
		if errstr, errcode = t.restoreObjLocalBucket(lom, r); errstr == "" {
			t.objGetComplete(w, r, lom, started, rangeOff, rangeLen, false)
			t.rtnamemap.Unlock(lom.Uname, false)
			return
		}
		t.rtnamemap.Unlock(lom.Uname, false)
		t.invalmsghdlr(w, r, errstr, errcode)
		return
	}
	coldget = lom.Doesnotexist
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
	if coldget && !dryRun.disk {
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
//     1) local FS, 2) this cluster, 3) other tiers in the DC
// FIXME: must be done => (getfqn, and under write lock)
//
func (t *targetrunner) restoreObjLocalBucket(lom *cluster.LOM, r *http.Request) (errstr string, errcode int) {
	// check FS-wide if local rebalance is running
	aborted, running := t.xactions.isAbortedOrRunningLocalRebalance()
	if aborted || running {
		oldFQN, oldSize := t.getFromOtherLocalFS(lom.Bucket, lom.Objname, lom.Bislocal)
		if oldFQN != "" {
			if glog.V(4) {
				glog.Infof("Restored (local rebalance in-progress?): %s (%s)", oldFQN, cmn.B2S(oldSize, 1))
			}
			lom.Fqn = oldFQN
			lom.Size = oldSize
			return
		}
	}
	// check cluster-wide when global rebalance is running
	aborted, running = t.xactions.isAbortedOrRunningRebalance()
	if aborted || running {
		if props, errs := t.getFromNeighbor(r, lom); errs == "" {
			lom.RestoredReceived(props)
			if glog.V(4) {
				glog.Infof("Restored (global rebalance in-progress?): %s (%s)", lom, cmn.B2S(lom.Size, 1))
			}
			return
		}
	} else {
		if lom.Bprops != nil && lom.Bprops.NextTierURL != "" {
			var (
				props      *cluster.LOM
				inNextTier bool
			)
			if inNextTier, errstr, errcode = t.objectInNextTier(lom.Bprops.NextTierURL, lom.Bucket, lom.Objname); inNextTier {
				if props, errstr, errcode = t.getObjectNextTier(lom.Bprops.NextTierURL, lom.Bucket, lom.Objname, lom.Fqn); errstr == "" {
					lom.RestoredReceived(props)
					return
				}
			}
		}
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
		file               *os.File
		sgl                *memsys.SGL
		slab               *memsys.Slab2
		buf                []byte
		rangeReader        io.ReadSeeker
		reader             io.Reader
		rahSize, written   int64
		err                error
		errstr             string
		rahfcacher, rahsgl = t.readahead.get(lom.Fqn)
		sendMore           bool
	)
	defer func() {
		rahfcacher.got()
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
	if lom.Nhobj != nil && !cksumRange {
		htype, hval := lom.Nhobj.Get()
		w.Header().Add(cmn.HeaderDFCChecksumType, htype)
		w.Header().Add(cmn.HeaderDFCChecksumVal, hval)
	}
	if lom.Version != "" {
		w.Header().Add(cmn.HeaderDFCObjVersion, lom.Version)
	}

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
	if rahsgl != nil {
		rahSize = rahsgl.Size()
		if rangeLen == 0 {
			sendMore = rahSize < lom.Size
		} else {
			sendMore = rahSize < rangeLen
		}
	}
	if rahSize == 0 || sendMore {
		file, err = os.Open(lom.Fqn)
		if err != nil {
			if os.IsPermission(err) {
				errstr = fmt.Sprintf("Permission to access %s denied, err: %v", lom.Fqn, err)
				t.invalmsghdlr(w, r, errstr, http.StatusForbidden)
			} else {
				errstr = fmt.Sprintf("Failed to open %s, err: %v", lom.Fqn, err)
				t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
			}
			t.fshc(err, lom.Fqn)
			return
		}
	}

send:
	if rahsgl != nil && rahSize > 0 {
		// reader = memsys.NewReader(rahsgl) - NOTE
		reader = rahsgl
		slab = rahsgl.Slab()
		buf = slab.Alloc()
		glog.Infof("%s readahead %d", lom.Fqn, rahSize) // FIXME: DEBUG
	} else if rangeLen == 0 {
		if rahSize > 0 {
			file.Seek(rahSize, io.SeekStart)
		}
		reader = file
		buf, slab = gmem2.AllocFromSlab2(lom.Size)
	} else {
		if rahSize > 0 {
			rangeOff += rahSize
			rangeLen -= rahSize
		}
		buf, slab = gmem2.AllocFromSlab2(rangeLen)
		if cksumRange {
			cmn.Assert(rahSize == 0, "NOT IMPLEMENTED YET") // TODO
			var cksum string
			cksum, sgl, rangeReader, errstr = t.rangeCksum(file, lom.Fqn, rangeOff, rangeLen, buf)
			if errstr != "" {
				t.invalmsghdlr(w, r, errstr, http.StatusInternalServerError)
				return

			}
			reader = rangeReader
			w.Header().Add(cmn.HeaderDFCChecksumType, lom.Cksumcfg.Checksum)
			w.Header().Add(cmn.HeaderDFCChecksumVal, cksum)
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
			errstr = fmt.Sprintf("Failed to GET %s, err: %v", lom.Fqn, err)
		} else {
			errstr = fmt.Sprintf("dry-run: failed to read/discard %s, err: %v", lom.Fqn, err)
		}
		glog.Error(errstr)
		t.fshc(err, lom.Fqn)
		t.statsif.Add(stats.ErrGetCount, 1)
		return
	}
	if sendMore {
		rahsgl = nil
		sendMore = false
		goto send
	}

	if !coldget && lom.LRUenabled() {
		getatimerunner().Touch(lom.Fqn)
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
	cksum string, sgl *memsys.SGL, rangeReader io.ReadSeeker, errstr string) {
	rangeReader = io.NewSectionReader(file, offset, length)
	xx := xxhash.New64()
	if length <= maxBytesInMem {
		sgl = gmem2.NewSGL(length)
		_, err := cmn.ReceiveAndChecksum(sgl, rangeReader, buf, xx)
		if err != nil {
			errstr = fmt.Sprintf("failed to read byte range, offset:%d, length:%d from %s, err: %v", offset, length, fqn, err)
			t.fshc(err, fqn)
			return
		}
		// overriding rangeReader here to read from the sgl
		rangeReader = memsys.NewReader(sgl)
	}

	_, err := rangeReader.Seek(0, io.SeekStart)
	if err != nil {
		errstr = fmt.Sprintf("failed to seek file %s to beginning, err: %v", fqn, err)
		t.fshc(err, fqn)
		return
	}

	hashIn64 := xx.Sum64()
	hashInBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(hashInBytes, hashIn64)
	cksum = hex.EncodeToString(hashInBytes)
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
	from, to := query.Get(cmn.URLParamFromID), query.Get(cmn.URLParamToID)
	if from != "" && to != "" {
		// REBALANCE "?from_id="+from_id+"&to_id="+to_id
		if objname == "" {
			s := "Invalid URL: missing object name"
			t.invalmsghdlr(w, r, s)
			return
		}
		if errstr := t.dorebalance(r, from, to, bucket, objname); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
		}
	} else {
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

		errstr, errcode := t.doPut(r, bucket, objname)
		if errstr != "" {
			t.invalmsghdlr(w, r, errstr, errcode)
		}
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
		s := fmt.Sprintf("fildelete: Failed to read %s request, err: %v", r.Method, err)
		if err == io.EOF {
			trailer := r.Trailer.Get("Error")
			if trailer != "" {
				s = fmt.Sprintf("fildelete: Failed to read %s request, err: %v, trailer: %s", r.Method, err, trailer)
			}
		}
		t.invalmsghdlr(w, r, s)
		return
	}
	if objname != "" {
		err := t.fildelete(t.contextWithAuth(r), bucket, objname, evict)
		if err != nil {
			s := fmt.Sprintf("Error deleting %s/%s: %v", bucket, objname, err)
			t.invalmsghdlr(w, r, s)
		}
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
		t.renamefile(w, r, msg)
	case cmn.ActReplicate:
		t.replicate(w, r, msg)
	default:
		t.invalmsghdlr(w, r, "Unexpected action "+msg.Action)
	}
}

// HEAD /v1/buckets/bucket-name
func (t *targetrunner) httpbckhead(w http.ResponseWriter, r *http.Request) {
	var (
		bucket      string
		islocal     bool
		errstr      string
		errcode     int
		bucketprops cmn.SimpleKVs
		cksumcfg    *cmn.CksumConf
	)
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket = apitems[0]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	query := r.URL.Query()
	if glog.V(4) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, bucket, pid)
	}
	bucketmd := t.bmdowner.get()
	islocal = bucketmd.IsLocal(bucket)
	errstr, errcode = t.IsBucketLocal(bucket, &bucketmd.BMD, query, islocal)
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
		bucketprops[cmn.HeaderCloudProvider] = cmn.ProviderDFC
		bucketprops[cmn.HeaderVersioning] = cmn.VersionLocal
	}
	// double check if we support versioning internally for the bucket
	if !versioningConfigured(islocal) {
		bucketprops[cmn.HeaderVersioning] = cmn.VersionNone
	}

	for k, v := range bucketprops {
		w.Header().Add(k, v)
	}

	props, _, defined := bucketmd.propsAndChecksum(bucket)
	// include checksumming settings in the response
	cksumcfg = &cmn.GCO.Get().Cksum
	if defined {
		cksumcfg = &props.CksumConf
	}

	// include lru settings in the response
	w.Header().Add(cmn.HeaderNextTierURL, props.NextTierURL)
	w.Header().Add(cmn.HeaderReadPolicy, props.ReadPolicy)
	w.Header().Add(cmn.HeaderWritePolicy, props.WritePolicy)
	w.Header().Add(cmn.HeaderBucketChecksumType, cksumcfg.Checksum)
	w.Header().Add(cmn.HeaderBucketValidateColdGet, strconv.FormatBool(cksumcfg.ValidateColdGet))
	w.Header().Add(cmn.HeaderBucketValidateWarmGet, strconv.FormatBool(cksumcfg.ValidateWarmGet))
	w.Header().Add(cmn.HeaderBucketValidateRange, strconv.FormatBool(cksumcfg.EnableReadRangeChecksum))
	w.Header().Add(cmn.HeaderBucketLRULowWM, strconv.FormatUint(uint64(props.LowWM), 10))
	w.Header().Add(cmn.HeaderBucketLRUHighWM, strconv.FormatUint(uint64(props.HighWM), 10))
	w.Header().Add(cmn.HeaderBucketAtimeCacheMax, strconv.FormatInt(props.AtimeCacheMax, 10))
	w.Header().Add(cmn.HeaderBucketDontEvictTime, props.DontEvictTimeStr)
	w.Header().Add(cmn.HeaderBucketCapUpdTime, props.CapacityUpdTimeStr)
	w.Header().Add(cmn.HeaderBucketLRUEnabled, strconv.FormatBool(props.LRUEnabled))
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
		if lom.Doesnotexist {
			status := http.StatusNotFound
			http.Error(w, http.StatusText(status), status)
			return
		} else if checkCached {
			return
		}
		objmeta = make(cmn.SimpleKVs)
		objmeta["size"] = strconv.FormatInt(lom.Size, 10)
		objmeta["version"] = lom.Version
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
	for k, v := range objmeta {
		w.Header().Add(k, v)
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
			toDir := filepath.Join(fs.Mountpaths.MakePathLocal(mpathInfo.Path, contentType), bucketTo)
			if err := cmn.CreateDir(toDir); err != nil {
				ch <- fmt.Sprintf("Failed to create dir %s, error: %v", toDir, err)
				continue
			}

			wg.Add(1)
			fromDir := filepath.Join(fs.Mountpaths.MakePathLocal(mpathInfo.Path, contentType), bucketFrom)
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
	removeBuckets("rename", bucketFrom)
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
		glog.Errorf("walkf invoked with err: %v", err)
		return err
	}
	if osfi.Mode().IsDir() {
		return nil
	}
	// FIXME: workfiles indicate work in progress. Renaming could break ongoing
	// operations and not renaming it probably result in having file in wrong directory.
	if !fs.CSM.PermToProcess(fqn) {
		return nil
	}
	parsedFQN, _, err := cluster.ResolveFQN(fqn, nil, true /* bucket is local */)
	contentType, bucket, objname := parsedFQN.ContentType, parsedFQN.Bucket, parsedFQN.Objname
	if err == nil {
		if bucket != renctx.bucketFrom {
			return fmt.Errorf("Unexpected: bucket %s != %s bucketFrom", bucket, renctx.bucketFrom)
		}
	}
	if errstr := renctx.t.renameObject(contentType, bucket, objname, renctx.bucketTo, objname); errstr != "" {
		return fmt.Errorf(errstr)
	}
	return nil
}

// checkCloudVersion returns if versions of an object differ in Cloud and DFC cache
// and the object should be refreshed from Cloud storage
// It should be called only in case of the object is present in DFC cache
func (t *targetrunner) checkCloudVersion(ct context.Context, bucket, objname, version string) (vchanged bool, errstr string, errcode int) {
	var objmeta cmn.SimpleKVs
	if objmeta, errstr, errcode = t.cloudif.headobject(ct, bucket, objname); errstr != "" {
		return
	}
	if cloudVersion, ok := objmeta["version"]; ok {
		if version != cloudVersion {
			glog.Infof("Object %s/%s version changed, current version %s (old/local %s)",
				bucket, objname, cloudVersion, version)
			vchanged = true
		}
	}
	return
}

func (t *targetrunner) getFromNeighbor(r *http.Request, lom *cluster.LOM) (props *cluster.LOM, errstr string) {
	neighsi := t.lookupRemotely(lom.Bucket, lom.Objname)
	if neighsi == nil {
		errstr = fmt.Sprintf("Failed cluster-wide lookup %s", lom)
		return
	}
	if glog.V(4) {
		glog.Infof("Found %s at %s", lom, neighsi)
	}

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
	contextwith, cancel := context.WithTimeout(context.Background(), cmn.GCO.Get().Timeout.SendFile)
	defer cancel()
	newrequest := newr.WithContext(contextwith)

	response, err := t.httpclientLongTimeout.Do(newrequest)
	if err != nil {
		errstr = fmt.Sprintf("Failed to GET redirect URL %q, err: %v", geturl, err)
		return
	}
	var (
		hval    = response.Header.Get(cmn.HeaderDFCChecksumVal)
		htype   = response.Header.Get(cmn.HeaderDFCChecksumType)
		hksum   = cmn.NewCksum(htype, hval)
		version = response.Header.Get(cmn.HeaderDFCObjVersion)
		getfqn  = fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfileRemote)
	)
	props = &cluster.LOM{}
	*props = *lom
	props.Nhobj = hksum
	props.Version = version
	if _, lom.Nhobj, lom.Size, errstr = t.receive(getfqn, props, "", response.Body); errstr != "" {
		response.Body.Close()
		return
	}
	response.Body.Close()
	// commit
	if err = cmn.MvFile(getfqn, props.Fqn); err != nil {
		glog.Errorln(err)
		return
	}
	if errstr = props.Persist(); errstr != "" {
		return
	}
	if glog.V(4) {
		glog.Infof("Success: %s (%s, %s) from %s", lom, cmn.B2S(lom.Size, 1), lom.Nhobj, neighsi)
	}
	return
}

// FIXME: when called with a bad checksum will recompute yet again (optimize)
func (t *targetrunner) getCold(ct context.Context, lom *cluster.LOM, prefetch bool) (errstr string, errcode int) {
	var (
		config          = cmn.GCO.Get()
		versioncfg      = &config.Ver
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
		lom.Doesnotexist = true // NOTE: in an attempt to fix it
	}

	// existence, access & versioning
	coldget := lom.Doesnotexist
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

func (t *targetrunner) lookupRemotely(bucket, objname string) *cluster.Snode {
	res := t.broadcastTo(
		cmn.URLPath(cmn.Version, cmn.Objects, bucket, objname),
		nil, // query
		http.MethodHead,
		nil,
		t.smapowner.get(),
		cmn.GCO.Get().Timeout.MaxKeepalive,
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
			var localDir string
			if islocal {
				localDir = filepath.Join(fs.Mountpaths.MakePathLocal(mpathInfo.Path, contentType), bucket)
			} else {
				localDir = filepath.Join(fs.Mountpaths.MakePathCloud(mpathInfo.Path, contentType), bucket)
			}

			go walkMpath(localDir)
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
			t.fshc(r.err, r.failedPath)
			return nil, fmt.Errorf("Failed to read %s", r.failedPath)
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

	if ci.markerDir != "" && relname < ci.markerDir {
		return filepath.SkipDir
	}

	return nil
}

// Adds an info about cached object to the list if:
//  - its name starts with prefix (if prefix is set)
//  - it has not been already returned by previous page request
//  - this target responses getobj request for the object
func (ci *allfinfos) processRegularFile(lom *cluster.LOM, osfi os.FileInfo, objStatus string) error {
	relname := lom.Fqn[ci.rootLength:]
	if ci.prefix != "" && !strings.HasPrefix(relname, ci.prefix) {
		return nil
	}

	if ci.marker != "" && relname <= ci.marker {
		return nil
	}

	// the file passed all checks - add it to the batch
	ci.fileCount++
	fileInfo := &cmn.BucketEntry{
		Name:     relname,
		Atime:    "",
		IsCached: true,
		Status:   objStatus,
	}
	if ci.needAtime {
		lom.Fill(cluster.LomAtime)
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
	if ci.needChkSum {
		errstr := lom.Fill(cluster.LomCksum)
		if errstr == "" && lom.Nhobj != nil {
			_, storedCksum := lom.Nhobj.Get()
			fileInfo.Checksum = hex.EncodeToString([]byte(storedCksum))
		}
	}
	if ci.needVersion {
		errstr := lom.Fill(cluster.LomVersion)
		if errstr == "" {
			fileInfo.Version = lom.Version
		}
	}
	fileInfo.Size = osfi.Size()
	ci.files = append(ci.files, fileInfo)
	ci.lastFilePath = lom.Fqn
	return nil
}

func (ci *allfinfos) listwalkf(fqn string, osfi os.FileInfo, err error) error {
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		glog.Errorf("listwalkf callback invoked with err: %v", err)
		return err
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
	errstr := lom.Fill(0)
	if ci.needStatus || ci.needAtime {
		if errstr != "" || lom.Misplaced {
			glog.Warning(errstr)
			objStatus = cmn.ObjStatusMoved
		}
		si, errstr := hrwTarget(lom.Bucket, lom.Objname, ci.t.smapowner.get())
		if errstr != "" || ci.t.si.DaemonID != si.DaemonID {
			glog.Warningf("%s appears to be misplaced: %s", lom, errstr)
			objStatus = cmn.ObjStatusMoved
		}
	}
	return ci.processRegularFile(lom, osfi, objStatus)
}

// After putting a new version it updates xattr attributes for the object
// Local bucket:
//  - if bucket versioning is enable("all" or "local") then the version is autoincremented
// Cloud bucket:
//  - if the Cloud returns a new version id then save it to xattr
// In both case a new checksum is saved to xattrs
func (t *targetrunner) doPut(r *http.Request, bucket, objname string) (errstr string, errcode int) {
	var (
		htype   = r.Header.Get(cmn.HeaderDFCChecksumType)
		hval    = r.Header.Get(cmn.HeaderDFCChecksumVal)
		hksum   = cmn.NewCksum(htype, hval)
		started = time.Now()
	)
	lom := &cluster.LOM{T: t, Bucket: bucket, Objname: objname}
	if errstr = lom.Fill(cluster.LomFstat); errstr != "" {
		return
	}
	putfqn := fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfilePut)

	// optimize out if the checksums do match
	if lom.Exists() {
		if errstr = lom.Fill(cluster.LomCksum); errstr == "" {
			if cmn.EqCksum(lom.Nhobj, hksum) {
				glog.Infof("Existing %s is valid: PUT is a no-op", lom)
				return
			}
		}
	}
	lom.Nhobj = hksum
	if _, lom.Nhobj, lom.Size, errstr = t.receive(putfqn, lom, "", r.Body); errstr != "" {
		return
	}
	// commit
	if !dryRun.disk && !dryRun.network {
		errstr, errcode = t.putCommit(t.contextWithAuth(r), lom, putfqn, false /*rebalance*/)
	}
	if errstr == "" {
		delta := time.Since(started)
		t.statsif.AddMany(stats.NamedVal64{stats.PutCount, 1}, stats.NamedVal64{stats.PutLatency, int64(delta)})
		if glog.V(4) {
			glog.Infof("PUT: %s/%s, %d µs", bucket, objname, int64(delta/time.Microsecond))
		}

		if false { // TODO: bucket n-way mirror props
			xlr := t.xactions.renewPutLR(lom.Bucket, lom.Bislocal) // TODO: optimize find
			xlr.Copy(lom)
		}
	}
	return
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

func (t *targetrunner) putCommit(ct context.Context, lom *cluster.LOM, putfqn string, rebalance bool) (errstr string, errcode int) {
	var renamed bool
	renamed, errstr, errcode = t.doPutCommit(ct, lom, putfqn, rebalance)
	if errstr != "" && !renamed {
		if _, err := os.Stat(putfqn); err == nil || !os.IsNotExist(err) {
			t.fshc(err, putfqn)
			if err = os.Remove(putfqn); err != nil {
				glog.Errorf("Nested error: %s => (remove %s => err: %v)", errstr, putfqn, err)
			}
		}
	}
	return
}

func (t *targetrunner) doPutCommit(ct context.Context, lom *cluster.LOM, putfqn string,
	rebalance bool) (renamed bool, errstr string, errcode int) {
	if !lom.Bislocal && !rebalance {
		if file, err := os.Open(putfqn); err != nil {
			errstr = fmt.Sprintf("Failed to open %s err: %v", putfqn, err)
			return
		} else {
			lom.Version, errstr, errcode = getcloudif().putobj(ct, file, lom.Bucket, lom.Objname, lom.Nhobj)
			file.Close()
			if errstr != "" {
				return
			}
		}
	}
	// when all set and done:
	t.rtnamemap.Lock(lom.Uname, true)

	if lom.Bislocal && versioningConfigured(true) {
		if lom.Version, errstr = lom.IncObjectVersion(); errstr != "" {
			t.rtnamemap.Unlock(lom.Uname, true)
			return
		}
	}
	if err := cmn.MvFile(putfqn, lom.Fqn); err != nil {
		t.rtnamemap.Unlock(lom.Uname, true)
		errstr = fmt.Sprintf("work => %s: %v", lom, err)
		return
	}
	renamed = true
	if errstr = lom.Persist(); errstr != "" {
		glog.Errorf("Failed to persist %s: %s", lom, errstr)
	}
	t.rtnamemap.Unlock(lom.Uname, true)
	return
}

func (t *targetrunner) dorebalance(r *http.Request, from, to, bucket, objname string) (errstr string) {
	if t.si.DaemonID != from && t.si.DaemonID != to {
		errstr = fmt.Sprintf("File copy: %s is not the intended source %s nor the destination %s",
			t.si.DaemonID, from, to)
		return
	}
	var (
		putfqn string
		lom    = &cluster.LOM{T: t, Bucket: bucket, Objname: objname}
	)
	if errstr = lom.Fill(0); errstr != "" {
		return
	}
	if t.si.DaemonID == from {
		//
		// the source
		//
		t.rtnamemap.Lock(lom.Uname, false)
		defer t.rtnamemap.Unlock(lom.Uname, false)

		if errstr = lom.Fill(cluster.LomFstat); errstr != "" {
			return
		}
		if lom.Doesnotexist {
			errstr = fmt.Sprintf("Rebalance: %s at the source %s", lom, t.si)
			return
		}
		if glog.V(3) {
			glog.Infof("Rebalancing %s from %s(self) to %s", lom, from, to)
		}
		si := t.smapowner.get().GetTarget(to)
		if si == nil {
			errstr = fmt.Sprintf("Unknown destination %s (Smap not in-sync?)", to)
			return
		}
		if errstr = t.sendfile(r.Method, bucket, objname, si, lom.Size, "", "", nil); errstr != "" {
			return
		}
		if glog.V(4) {
			glog.Infof("Rebalance %s(%s) done", lom, cmn.B2S(lom.Size, 1))
		}
	} else {
		//
		// the destination
		//
		if glog.V(4) {
			glog.Infof("Rebalance %s from %s to %s(self)", lom, from, to)
		}
		if errstr = lom.Fill(cluster.LomFstat); errstr != "" {
			return
		}
		if lom.Exists() {
			glog.Infof("%s already exists at the destination %s", lom, t.si)
			return // not an error, nothing to do
		}
		putfqn = fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfileRebalance)

		var (
			size  int64
			htype = r.Header.Get(cmn.HeaderDFCChecksumType)
			hval  = r.Header.Get(cmn.HeaderDFCChecksumVal)
			hksum = cmn.NewCksum(htype, hval)
		)
		lom.Version = r.Header.Get(cmn.HeaderDFCObjVersion)
		if timeStr := r.Header.Get(cmn.HeaderDFCObjAtime); timeStr != "" {
			lom.Atimestr = timeStr
			if tm, err := time.Parse(time.RFC822, timeStr); err == nil {
				lom.Atime = tm
			}
		}
		lom.Nhobj = hksum
		if _, lom.Nhobj, lom.Size, errstr = t.receive(putfqn, lom, "", r.Body); errstr != "" {
			return
		}
		errstr, _ = t.putCommit(t.contextWithAuth(r), lom, putfqn, true /*rebalance*/)
		if errstr == "" {
			t.statsif.AddMany(stats.NamedVal64{stats.RxCount, 1}, stats.NamedVal64{stats.RxSize, size})
		}
	}
	return
}

func (t *targetrunner) fildelete(ct context.Context, bucket, objname string, evict bool) error {
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

	_ = lom.Fill(cluster.LomFstat) // ignore fstat errors
	if lom.Doesnotexist {
		return nil
	}
	if !(evict && lom.Bislocal) {
		// Don't evict from a local bucket (this would be deletion)
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

func (t *targetrunner) renamefile(w http.ResponseWriter, r *http.Request, msg cmn.ActionMsg) {
	var errstr string

	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objname := apitems[0], apitems[1]
	if !t.validatebckname(w, r, bucket) {
		return
	}
	newobjname := msg.Name
	uname := cluster.Uname(bucket, objname)
	t.rtnamemap.Lock(uname, true)

	if errstr = t.renameObject(fs.ObjectType, bucket, objname, bucket, newobjname); errstr != "" {
		t.invalmsghdlr(w, r, errstr)
	}
	t.rtnamemap.Unlock(uname, true)
}

func (t *targetrunner) replicate(w http.ResponseWriter, r *http.Request, msg cmn.ActionMsg) {
	t.invalmsghdlr(w, r, "not supported yet") // NOTE: daemon.go, proxy.go, config.sh, and tests/replication
}

func (t *targetrunner) renameObject(contentType, bucketFrom, objnameFrom, bucketTo, objnameTo string) (errstr string) {
	var (
		si     *cluster.Snode
		newfqn string
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
	finfo, err := os.Stat(fqn)
	if err != nil {
		errstr = fmt.Sprintf("Rename/move: failed to fstat %s (%s/%s), err: %v", fqn, bucketFrom, objnameFrom, err)
		return
	}
	// local rename
	if si.DaemonID == t.si.DaemonID {
		islocalTo := bucketmd.IsLocal(bucketTo)
		newfqn, errstr = cluster.FQN(contentType, bucketTo, objnameTo, islocalTo)
		if errstr != "" {
			return
		}
		if err := cmn.MvFile(fqn, newfqn); err != nil {
			errstr = fmt.Sprintf("Rename object %s/%s: %v", bucketFrom, objnameFrom, err)
		} else {
			t.statsif.Add(stats.RenameCount, 1)
			if glog.V(3) {
				glog.Infof("Renamed %s => %s", fqn, newfqn)
			}
		}
		return
	}
	// move/migrate
	glog.Infof("Migrating %s/%s at %s => %s/%s at %s", bucketFrom, objnameFrom, t.si, bucketTo, objnameTo, si)

	errstr = t.sendfile(http.MethodPut, bucketFrom, objnameFrom, si, finfo.Size(), bucketTo, objnameTo, nil)
	return
}

// Rebalancing supports versioning. If an object in DFC cache has version in
// xattrs then the sender adds to HTTP header object version. A receiver side
// reads version from headers and set xattrs if the version is not empty
func (t *targetrunner) sendfile(method, bucket, objname string, destsi *cluster.Snode, size int64,
	newbucket, newobjname string, atimeRespCh chan *atime.Response) string {
	if newobjname == "" {
		newobjname = objname
	}
	if newbucket == "" {
		newbucket = bucket
	}

	lom := &cluster.LOM{T: t, Size: size, Bucket: bucket, Objname: objname, AtimeRespCh: atimeRespCh}
	errstr := lom.Fill(cluster.LomFstat | cluster.LomVersion | cluster.LomAtime | cluster.LomCksum | cluster.LomCksumMissingRecomp)
	if errstr != "" {
		return errstr
	}
	if lom.Doesnotexist {
		return lom.String()
	}
	// TODO make sure this Open will have no atime "side-effect", here and elsewhere
	file, err := os.Open(lom.Fqn) // FIXME: make sure Open has no atime "side-effect", here and elsewhere
	if err != nil {
		return fmt.Sprintf("Failed to open %s(%q), err: %v", lom, lom.Fqn, err)
	}
	defer file.Close()

	//
	// http request
	//
	fromid, toid := t.si.DaemonID, destsi.DaemonID // source=self and destination
	url := destsi.IntraDataNet.DirectURL + cmn.URLPath(cmn.Version, cmn.Objects, newbucket, newobjname)
	url += fmt.Sprintf("?%s=%s&%s=%s", cmn.URLParamFromID, fromid, cmn.URLParamToID, toid)
	request, err := http.NewRequest(method, url, file)
	if err != nil {
		return fmt.Sprintf("Unexpected failure to create %s request %s, err: %v", method, url, err)
	}
	if lom.Nhobj != nil {
		htype, hval := lom.Nhobj.Get()
		request.Header.Set(cmn.HeaderDFCChecksumType, htype)
		request.Header.Set(cmn.HeaderDFCChecksumVal, hval)
	}
	if lom.Version != "" {
		request.Header.Set(cmn.HeaderDFCObjVersion, lom.Version)
	}
	if lom.Atimestr != "" {
		request.Header.Set(cmn.HeaderDFCObjAtime, lom.Atimestr)
	}

	// Do
	contextwith, cancel := context.WithTimeout(context.Background(), cmn.GCO.Get().Timeout.SendFile)
	defer cancel()
	newrequest := request.WithContext(contextwith)

	response, err := t.httpclientLongTimeout.Do(newrequest)

	// err handle
	if err != nil {
		errstr = fmt.Sprintf("Failed to sendfile %s/%s at %s => %s/%s at %s, err: %v",
			bucket, objname, fromid, newbucket, newobjname, toid, err)
		if response != nil && response.StatusCode > 0 {
			errstr += fmt.Sprintf(", status %s", response.Status)
		}
		return errstr
	}
	if _, err = ioutil.ReadAll(response.Body); err != nil {
		errstr = fmt.Sprintf("Failed to read sendfile response: %s/%s at %s => %s/%s at %s, err: %v",
			bucket, objname, fromid, newbucket, newobjname, toid, err)
		if err == io.EOF {
			trailer := response.Trailer.Get("Error")
			if trailer != "" {
				errstr += fmt.Sprintf(", trailer: %s", trailer)
			}
		}
		response.Body.Close()
		return errstr
	}
	response.Body.Close()
	// stats
	t.statsif.AddMany(stats.NamedVal64{stats.TxCount, 1}, stats.NamedVal64{stats.TxSize, size})
	return ""
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
	glog.Flush()
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
		kind := r.URL.Query().Get(cmn.URLParamProps)
		if errstr := validateXactionQueryable(kind); errstr != "" {
			t.invalmsghdlr(w, r, errstr)
			return
		}
		var (
			jsbytes           []byte
			sts               = getstorstatsrunner()
			allXactionDetails = t.getXactionsByKind(kind)
		)
		if kind == cmn.XactionRebalance {
			jsbytes = sts.GetRebalanceStats(allXactionDetails)
		} else {
			cmn.Assert(kind == cmn.XactionPrefetch)
			jsbytes = sts.GetPrefetchStats(allXactionDetails)
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
	allXactionDetails := []stats.XactionDetails{}
	for _, xaction := range t.xactions.v {
		if xaction.Kind() == kind {
			status := cmn.XactionStatusCompleted
			if !xaction.Finished() {
				status = cmn.XactionStatusInProgress
			}

			xactionStats := stats.XactionDetails{
				Id:        xaction.ID(),
				StartTime: xaction.StartTime(),
				EndTime:   xaction.EndTime(),
				Status:    status,
			}

			allXactionDetails = append(allXactionDetails, xactionStats)
		}
	}

	return allXactionDetails
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
func (t *targetrunner) receive(workfqn string, lom *cluster.LOM, omd5 string,
	reader io.Reader) (sgl *memsys.SGL /* NIY */, nhobj cmn.CksumValue, written int64, errstr string) {
	var (
		err        error
		file       *os.File
		filewriter io.Writer
		nhval      string
		cksumcfg   = &cmn.GCO.Get().Cksum
	)

	if dryRun.disk && dryRun.network {
		return
	}
	if lom.Cksumcfg == nil { // FIXME: always initialize in the callers, include aws/gcp
		lom.Fill(0)
	}
	if dryRun.network {
		reader = readers.NewRandReader(dryRun.size)
	}
	if !dryRun.disk {
		if file, err = cmn.CreateFile(workfqn); err != nil {
			t.fshc(err, workfqn)
			errstr = fmt.Sprintf("Failed to create %s, err: %s", workfqn, err)
			return
		}
		filewriter = file
	} else {
		filewriter = ioutil.Discard
	}

	buf, slab := gmem2.AllocFromSlab2(0)
	defer func() { // free & cleanup on err
		slab.Free(buf)
		if errstr == "" {
			return
		}
		if !dryRun.disk {
			if err = file.Close(); err != nil {
				glog.Errorf("Nested: failed to close received file %s, err: %v", workfqn, err)
			}
			if err = os.Remove(workfqn); err != nil {
				glog.Errorf("Nested error %s => (remove %s => err: %v)", errstr, workfqn, err)
			}
		}
	}()

	if dryRun.disk || dryRun.network {
		if written, err = cmn.ReceiveAndChecksum(filewriter, reader, buf); err != nil {
			errstr = err.Error()
		}
		if !dryRun.disk {
			if err = file.Close(); err != nil {
				errstr = fmt.Sprintf("Failed to close received file %s, err: %v", workfqn, err)
			}
		}
		return
	}

	// receive and checksum
	if lom.Cksumcfg.Checksum != cmn.ChecksumNone {
		cmn.Assert(cksumcfg.Checksum == cmn.ChecksumXXHash)
		xx := xxhash.New64()
		if written, err = cmn.ReceiveAndChecksum(filewriter, reader, buf, xx); err != nil {
			errstr = err.Error()
			t.fshc(err, workfqn)
			return
		}
		hashIn64 := xx.Sum64()
		hashInBytes := make([]byte, 8)
		binary.BigEndian.PutUint64(hashInBytes, hashIn64)
		nhval = hex.EncodeToString(hashInBytes)
		nhobj = cmn.NewCksum(cmn.ChecksumXXHash, nhval)
		if lom.Nhobj != nil {
			if !cmn.EqCksum(nhobj, lom.Nhobj) {
				errstr = fmt.Sprintf("%s work %q", lom.BadChecksum(nhobj), workfqn)
				t.statsif.AddMany(stats.NamedVal64{stats.ErrCksumCount, 1}, stats.NamedVal64{stats.ErrCksumSize, written})
				return
			}
		}
	} else if omd5 != "" && cksumcfg.ValidateColdGet {
		md5 := md5.New()
		if written, err = cmn.ReceiveAndChecksum(filewriter, reader, buf, md5); err != nil {
			errstr = err.Error()
			t.fshc(err, workfqn)
			return
		}
		hashInBytes := md5.Sum(nil)[:16]
		md5hash := hex.EncodeToString(hashInBytes)
		if omd5 != md5hash {
			errstr = fmt.Sprintf("Bad md5: %s %s != %s work %q", lom, omd5, md5hash, workfqn)
			t.statsif.AddMany(stats.NamedVal64{stats.ErrCksumCount, 1}, stats.NamedVal64{stats.ErrCksumSize, written})
			return
		}
	} else {
		if written, err = cmn.ReceiveAndChecksum(filewriter, reader, buf); err != nil {
			errstr = err.Error()
			t.fshc(err, workfqn)
			return
		}
	}
	if err = file.Close(); err != nil {
		errstr = fmt.Sprintf("Failed to close received file %s, err: %v", workfqn, err)
	}
	return
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
	glog.Errorf("FSHC: fqn %s, err %v", filepath, err)
	if !cmn.IsIOError(err) {
		return
	}
	mpathInfo, _ := fs.Mountpaths.Path2MpathInfo(filepath)
	if mpathInfo != nil {
		keyName := mpathInfo.Path
		// keyName is the mountpath is the fspath - counting IO errors on a per basis..
		t.statsdC.Send(keyName+".io.errors", metric{statsd.Counter, "count", 1})
	}

	if cmn.GCO.Get().FSHC.Enabled {
		getfshealthchecker().OnErr(filepath)
	}
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
}

func (t *targetrunner) handleAddMountpathReq(w http.ResponseWriter, r *http.Request, mountpath string) {
	err := t.fsprg.addMountpath(mountpath)
	if err != nil {
		t.invalmsghdlr(w, r, fmt.Sprintf("Could not add mountpath, error: %v", err))
		return
	}
}

func (t *targetrunner) handleRemoveMountpathReq(w http.ResponseWriter, r *http.Request, mountpath string) {
	err := t.fsprg.removeMountpath(mountpath)
	if err != nil {
		t.invalmsghdlr(w, r, fmt.Sprintf("Could not remove mountpath, error: %v", err))
		return
	}
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

	// Delete buckets which don't exist in (new)bucketmd
	bucketsToDelete := make([]string, 0, len(bucketmd.LBmap))
	for bucket := range bucketmd.LBmap {
		if _, ok := newbucketmd.LBmap[bucket]; !ok {
			bucketsToDelete = append(bucketsToDelete, bucket)
		}
	}
	removeBuckets("receive-bucketmd", bucketsToDelete...)

	// Create buckets which don't exist in (old)bucketmd
	bucketsToCreate := make([]string, 0, len(newbucketmd.LBmap))
	for bucket := range newbucketmd.LBmap {
		if _, ok := bucketmd.LBmap[bucket]; !ok {
			bucketsToCreate = append(bucketsToCreate, bucket)
		}
	}
	createBuckets("receive-bucketmd", bucketsToCreate...)

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
	return t.fsprg.disableMountpath(mountpath)
}

func (t *targetrunner) getFromOtherLocalFS(bucket, object string, islocal bool) (fqn string, size int64) {
	availablePaths, _ := fs.Mountpaths.Get()
	fn := fs.Mountpaths.MakePathCloud
	if islocal {
		fn = fs.Mountpaths.MakePathLocal
	}
	for _, mpathInfo := range availablePaths {
		dir := fn(mpathInfo.Path, fs.ObjectType)

		filePath := filepath.Join(dir, bucket, object)
		stat, err := os.Stat(filePath)
		if err == nil {
			return filePath, stat.Size()
		}
	}

	return "", 0
}

func removeBuckets(op string, buckets ...string) {
	availablePaths, _ := fs.Mountpaths.Get()
	contentTypes := fs.CSM.RegisteredContentTypes

	wg := &sync.WaitGroup{}
	for _, mpathInfo := range availablePaths {
		wg.Add(1)
		go func(mi *fs.MountpathInfo) {
			for _, bucket := range buckets {
				for contentType := range contentTypes {
					localBucketFQN := filepath.Join(fs.Mountpaths.MakePathLocal(mi.Path, contentType), bucket)
					glog.Warningf("%s: FastRemoveDir %q for content-type %q, bucket %q", op, localBucketFQN, contentType, bucket)
					if err := mi.FastRemoveDir(localBucketFQN); err != nil {
						// TODO: in case of error, we need to abort and rollback whole operation
						glog.Errorf("Failed to destroy local bucket dir %q, err: %v", localBucketFQN, err)
					}
				}
			}
			wg.Done()
		}(mpathInfo)
	}
	wg.Wait()
}

func createBuckets(op string, buckets ...string) {
	availablePaths, _ := fs.Mountpaths.Get()
	contentTypes := fs.CSM.RegisteredContentTypes

	wg := &sync.WaitGroup{}
	for _, mpathInfo := range availablePaths {
		wg.Add(1)
		go func(mi *fs.MountpathInfo) {
			for _, bucket := range buckets {
				for contentType := range contentTypes {
					localBucketFQN := filepath.Join(fs.Mountpaths.MakePathLocal(mi.Path, contentType), bucket)
					glog.Warningf("%s: CreateDir %q for content-type %q, bucket %q", op, localBucketFQN, contentType, bucket)
					if err := cmn.CreateDir(localBucketFQN); err != nil {
						// TODO: in case of error, we need to abort and rollback whole operation
						glog.Errorf("Failed to create local bucket dir %q, err: %v", localBucketFQN, err)
					}
				}
			}
			wg.Done()
		}(mpathInfo)
	}
	wg.Wait()
}
