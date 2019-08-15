// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
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
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/mirror"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/stats/statsd"
	"github.com/NVIDIA/aistore/transport"
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
	regstate struct {
		sync.Mutex
		disabled bool // target was unregistered by internal event (e.g, all mountpaths are down)
	}

	// The state that may influence GET logic when mountpath is added/enabled
	localGFN struct {
		lookup atomic.Bool
	}

	// The state that may influence GET logic when new target joins cluster
	globalGFN struct {
		lookup       atomic.Bool
		stopDeadline atomic.Int64 // (reg-time + const) when we stop trying to GET from neighbors
	}

	capUsed struct {
		sync.RWMutex
		used int32
		oos  bool
	}

	targetrunner struct {
		httprunner
		cloud          cloudProvider // multi-cloud backend
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

func (gfn *globalGFN) active() bool {
	if !gfn.lookup.Load() {
		return false
	}

	// Deadline exceeded - probably primary proxy notified about new smap
	// but did not update it due to some failures.
	if time.Now().UnixNano() > gfn.stopDeadline.Load() {
		gfn.deactivate()
		return false
	}

	return true
}

func (gfn *globalGFN) activate() {
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
	t.httprunner.init(getstorstatsrunner(), config)
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

		{r: "/", h: cmn.InvalidHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl, cmn.NetworkIntraData}},
	}
	t.registerNetworkHandlers(networkHandlers)

	// rebalance Rx endpoints
	t.setupRebalanceRx(config)

	getfshealthchecker().SetDispatcher(t)

	ec.Init()
	t.ecmanager = newECM(t)

	aborted, _ := t.xactions.isRebalancing(cmn.ActLocalReb)
	if aborted {
		go func() {
			glog.Infoln("resuming local rebalance...")
			t.rebManager.runLocalReb(false /*skipGplaced=false*/)
		}()
	}

	dsort.RegisterNode(t.smapowner, t.bmdowner, t.si, t, t.statsif)
	if err := t.httprunner.run(); err != nil {
		return err
	}
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
	// dsort
	t.statsif.Register(stats.DSortCreationReqCount, stats.KindCounter)
	t.statsif.Register(stats.DSortCreationReqLatency, stats.KindLatency)
	t.statsif.Register(stats.DSortCreationRespCount, stats.KindCounter)
	t.statsif.Register(stats.DSortCreationRespLatency, stats.KindLatency)
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
	bucket := apiItems[0]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)

	normalizedBckProvider, err := cmn.ProviderFromStr(bckProvider)
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
		config          = cmn.GCO.Get()
		query           = r.URL.Query()
		isGFNRequest, _ = cmn.ParseBool(query.Get(cmn.URLParamIsGFNRequest))
	)

	apiItems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objName := apiItems[0], apiItems[1]
	bckProvider := query.Get(cmn.URLParamBckProvider)
	started := time.Now()
	if redirDelta := t.redirectLatency(started, query); redirDelta != 0 {
		t.statsif.Add(stats.GetRedirLatency, redirDelta)
	}
	rangeOff, rangeLen, err := t.offsetAndLength(query)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	lom, err := cluster.LOM{T: t, Bucket: bucket, Objname: objName}.Init(bckProvider, config)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
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
		gfn:     isGFNRequest,
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

//
// 3a. attempt to restore an object that is missing in the LOCAL BUCKET - from:
//     1) local FS, 2) this cluster, 3) other tiers in the DC 4) from other
//		targets using erasure coding(if it is on)
// FIXME: must be done => (getfqn, and under write lock)
//
func (t *targetrunner) restoreObjLBNeigh(lom *cluster.LOM) (err error, errCode int) {
	// check FS-wide if local rebalance is running
	aborted, running := t.xactions.isRebalancing(cmn.ActLocalReb)
	gfnActive := t.gfn.local.active()
	if aborted || running || gfnActive {
		oldFQN, oldSize := getFromOtherLocalFS(lom)
		if oldFQN != "" {
			lom.FQN = oldFQN
			lom.SetSize(oldSize)
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("restored from LFS %s (%s)", lom, cmn.B2S(oldSize, 1))
			}
			return
		}
	}

	// HACK: if there's not enough EC targets to restore an sliced object, we might be able to restore it
	// if it was replicated. In this case even just one additional target might be sufficient
	// This won't succeed if an object was sliced, neither will ecmanager.RestoreObject(lom)
	enoughECRestoreTargets := lom.BckProps.EC.RequiredRestoreTargets() <= t.smapowner.Get().CountTargets()

	// check cluster-wide ("ask neighbors")
	aborted, running = t.xactions.isRebalancing(cmn.ActGlobalReb)
	gfnActive = t.gfn.global.active()
	if aborted || running || gfnActive || !enoughECRestoreTargets {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s: GFN (aborted=%t, running=%t, active=%t)", t.si.Name(), aborted, running, gfnActive)
		}
		// retry in case the object is being moved right now
		for retry := 0; retry < getFromNeighRetries; retry++ {
			if err := t.getFromNeighbor(lom); err != nil {
				time.Sleep(getFromNeighSleep)
				continue
			}
			if glog.FastV(4, glog.SmoduleAIS) {
				glog.Infof("%s: GFN restored %s (%s)", t.si.Name(), lom, cmn.B2S(lom.Size(), 1))
			}
			return
		}
	}
	// restore from existing EC slices if possible
	if ecErr := t.ecmanager.RestoreObject(lom); ecErr == nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s: EC-recovered %s", t.si.Name(), lom)
		}
		lom.Load()
		return
	} else if ecErr != ec.ErrorECDisabled {
		err = fmt.Errorf("%s: failed to EC-recover %s: %v", t.si.Name(), lom, ecErr)
	}

	s := fmt.Sprintf("GET local: %s(%s) %s", lom, lom.FQN, cmn.DoesNotExist)
	if err != nil {
		err = fmt.Errorf("%s => [%v]", s, err)
	} else {
		err = errors.New(s)
	}
	if errCode == 0 {
		errCode = http.StatusNotFound
	}
	return
}

func (t *targetrunner) rangeCksum(r io.ReaderAt, fqn string, offset, length int64, buf []byte) (cksumValue string, sgl *memsys.SGL, rangeReader io.ReadSeeker, err error) {
	rangeReader = io.NewSectionReader(r, offset, length)
	if length <= maxBytesInMem {
		sgl = nodeCtx.mm.NewSGL(length)
		if _, cksumValue, err = cmn.WriteWithHash(sgl, rangeReader, buf); err != nil {
			err = fmt.Errorf("failed to read byte range, offset:%d, length:%d from %s, err: %v", offset, length, fqn, err)
			t.fshc(err, fqn)
			return
		}
		// overriding rangeReader here to read from the sgl
		rangeReader = memsys.NewReader(sgl)
	}

	if _, err = rangeReader.Seek(0, io.SeekStart); err != nil {
		err = fmt.Errorf("failed to seek file %s to beginning, err: %v", fqn, err)
		t.fshc(err, fqn)
		return
	}

	return
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
	lom, err := cluster.LOM{T: t, Bucket: bucket, Objname: objname}.Init(bckProvider)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if err = lom.AllowPUT(); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if lom.BckIsLocal && lom.VerConf().Enabled {
		lom.Load() // need to know the current version if versionig enabled
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

	lom, err := cluster.LOM{T: t, Bucket: bucket, Objname: objname}.Init(bckProvider)
	if err != nil {
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
		msgInt  actionMsgInternal
		bmd     *bucketMD
		started = time.Now()
	)
	if cmn.ReadJSON(w, r, &msgInt) != nil {
		return
	}
	apitems, err := t.checkRESTItems(w, r, 1, true, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}

	t.ensureLatestMD(msgInt)

	bucket := apitems[0]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	bmd, bckIsLocal := t.validateBucket(w, r, bucket, bckProvider)
	if bmd == nil {
		return
	}

	switch msgInt.Action {
	case cmn.ActPrefetch: // validation done in proxy.go
		if err := t.listRangeOperation(r, apitems, bckProvider, &msgInt); err != nil {
			t.invalmsghdlr(w, r, fmt.Sprintf("Failed to prefetch files: %v", err))
			return
		}
	case cmn.ActCopyLB, cmn.ActRenameLB:
		var (
			phase                = apitems[1]
			bucketFrom, bucketTo = bucket, msgInt.Name
		)
		switch phase {
		case cmn.ActBegin:
			err = t.beginCopyRenameLB(bucketFrom, bucketTo, msgInt.Action)
		case cmn.ActAbort:
			err = t.abortCopyRenameLB(bucketFrom, bucketTo, msgInt.Action)
		case cmn.ActCommit:
			err = t.commitCopyRenameLB(bucketFrom, bucketTo, msgInt.Action)
		default:
			err = fmt.Errorf("invalid phase %s: %s %s => %s", phase, msgInt.Action, bucketFrom, bucketTo)
		}
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
		bmd := t.bmdowner.get()
		glog.Infof("%s %s bucket %s => %s, %s v%d", phase, msgInt.Action, bucketFrom, bucketTo, bmdTermName, bmd.version())
	case cmn.ActListObjects:
		// list the bucket and return
		if ok := t.listbucket(w, r, bucket, bckIsLocal, &msgInt); !ok {
			return
		}

		delta := time.Since(started)
		t.statsif.AddMany(stats.NamedVal64{stats.ListCount, 1}, stats.NamedVal64{stats.ListLatency, int64(delta)})
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("LIST: %s, %d µs", bmd.Bstring(bucket, bckIsLocal), int64(delta/time.Microsecond))
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
		t.renameObject(w, r, msg)
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
		bucketProps, err, errCode = t.cloud.headBucket(t.contextWithAuth(r.Header), bucket)
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
	hdr.Set(cmn.HeaderBucketVerValidateWarm, strconv.FormatBool(verConf.ValidateWarmGet))
	// for Cloud buckets correct Versioning.Enabled is combination of
	// local and cloud settings and it is true only if versioning
	// is enabled at both sides: Cloud and for local usage
	if bckIsLocal || !verConf.Enabled {
		hdr.Set(cmn.HeaderBucketVerEnabled, strconv.FormatBool(verConf.Enabled))
	} else if enabled, err := cmn.ParseBool(bucketProps[cmn.HeaderBucketVerEnabled]); !enabled && err == nil {
		hdr.Set(cmn.HeaderBucketVerEnabled, strconv.FormatBool(false))
	}

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
		bucket, objName string
		objmeta         cmn.SimpleKVs
		query           = r.URL.Query()

		errCode        int
		exists         bool
		checkCached, _ = cmn.ParseBool(query.Get(cmn.URLParamCheckCached)) // establish local presence, ignore obj attrs
		silent, _      = cmn.ParseBool(query.Get(cmn.URLParamSilent))
	)

	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objName = apitems[0], apitems[1]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	invalidHandler := t.invalmsghdlr
	if silent {
		invalidHandler = t.invalmsghdlrsilent
	}

	lom, err := cluster.LOM{T: t, Bucket: bucket, Objname: objName}.Init(bckProvider)
	if err != nil {
		invalidHandler(w, r, err.Error())
		return
	}
	cluster.ObjectLocker.Lock(lom.Uname(), false)
	defer cluster.ObjectLocker.Unlock(lom.Uname(), false)

	if err = lom.Load(); err != nil { // (doesnotexist -> ok, other)
		invalidHandler(w, r, err.Error())
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, lom.StringEx(), pid)
	}

	exists = lom.Exists()
	if lom.BckIsLocal || checkCached {
		if !exists {
			invalidHandler(w, r, fmt.Sprintf("no such object %s in bucket %s", objName, bucket), http.StatusNotFound)
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
		objmeta, err, errCode = t.cloud.headObj(t.contextWithAuth(r.Header), lom)
		if err != nil {
			errMsg := fmt.Sprintf("%s: failed to head metadata, err: %v", lom, err)
			invalidHandler(w, r, errMsg, errCode)
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

func (t *targetrunner) getFromNeighbor(lom *cluster.LOM) (err error) {
	smap := t.smapowner.get()
	neighsi := t.lookupRemotely(lom, smap)
	if neighsi == nil {
		err = fmt.Errorf("failed cluster-wide lookup %s", lom)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("Found %s at %s", lom, neighsi)
	}

	query := url.Values{}
	query.Add(cmn.URLParamBckProvider, cmn.ProviderFromLoc(lom.BckIsLocal))
	query.Add(cmn.URLParamIsGFNRequest, "true")
	reqArgs := cmn.ReqArgs{
		Method: http.MethodGet,
		Base:   neighsi.URL(cmn.NetworkIntraData),
		Path:   cmn.URLPath(cmn.Version, cmn.Objects, lom.Bucket, lom.Objname),
		Query:  query,
	}

	req, _, cancel, err := reqArgs.ReqWithTimeout(lom.Config().Timeout.SendFile)
	if err != nil {
		return fmt.Errorf("failed to create request, err: %v", err)
	}
	defer cancel()

	resp, err := t.httpclientLongTimeout.Do(req)
	if err != nil {
		return fmt.Errorf("failed to GET redirect URL %q, err: %v", reqArgs.URL(), err)
	}
	var (
		cksumValue = resp.Header.Get(cmn.HeaderObjCksumVal)
		cksumType  = resp.Header.Get(cmn.HeaderObjCksumType)
		cksum      = cmn.NewCksum(cksumType, cksumValue)
		version    = resp.Header.Get(cmn.HeaderObjVersion)
		workFQN    = fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfileRemote)
		atimeStr   = resp.Header.Get(cmn.HeaderObjAtime)
	)

	// The string in the header is an int represented as a string, NOT a formatted date string.
	atime, err := cmn.S2TimeUnix(atimeStr)
	if err != nil {
		return
	}
	lom.SetCksum(cksum)
	lom.SetVersion(version)
	lom.SetAtimeUnix(atime)
	poi := &putObjInfo{
		t:        t,
		lom:      lom,
		workFQN:  workFQN,
		r:        resp.Body,
		migrated: true,
	}
	if err = poi.writeToFile(); err != nil {
		return
	}
	// commit
	if err = cmn.Rename(workFQN, lom.FQN); err != nil {
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

	buckets, err, errcode := t.cloud.getBucketNames(t.contextWithAuth(r.Header))
	if err != nil {
		errMsg := fmt.Sprintf("failed to list all buckets, err: %v", err)
		t.invalmsghdlr(w, r, errMsg, errcode)
		return
	}
	bucketNames.Cloud = buckets

	body := cmn.MustMarshal(bucketNames)
	t.writeJSON(w, r, body, "getbucketnames")
}

// After putting a new version it updates xattr attributes for the object
// Local bucket:
//  - if bucket versioning is enable("all" or "local") then the version is autoincremented
// Cloud bucket:
//  - if the Cloud returns a new version id then save it to xattr
// In both case a new checksum is saved to xattrs
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

func (t *targetrunner) objDelete(ctx context.Context, lom *cluster.LOM, evict bool) error {
	var (
		cloudErr error
		errRet   error
	)

	cluster.ObjectLocker.Lock(lom.Uname(), true)
	defer cluster.ObjectLocker.Unlock(lom.Uname(), true)

	delFromCloud := !lom.BckIsLocal && !evict
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

///////////////////
// RENAME OBJECT //
///////////////////

func (t *targetrunner) renameObject(w http.ResponseWriter, r *http.Request, msg cmn.ActionMsg) {
	apitems, err := t.checkRESTItems(w, r, 2, false, cmn.Version, cmn.Objects)
	if err != nil {
		return
	}
	bucket, objnameFrom := apitems[0], apitems[1]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)

	lom, err := cluster.LOM{T: t, Bucket: bucket, Objname: objnameFrom}.Init(bckProvider)
	if err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if !lom.BckIsLocal {
		t.invalmsghdlr(w, r, fmt.Sprintf("%s: cannot rename object from Cloud bucket", lom))
		return
	}
	// TODO -- FIXME: cannot rename erasure-coded
	if err = lom.AllowRENAME(); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}

	buf, slab := nodeCtx.mm.AllocDefault()
	ri := &replicInfo{smap: t.smapowner.get(), t: t, bucketTo: bucket, buf: buf}
	if copied, err := ri.copyObject(lom, msg.Name /* new objname */); err != nil {
		t.invalmsghdlr(w, r, err.Error())
	} else if copied {
		cluster.ObjectLocker.Lock(lom.Uname(), true)
		// TODO: additional copies and ec cleanups
		if err = lom.Remove(); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
		cluster.ObjectLocker.Unlock(lom.Uname(), true)
	}
	slab.Free(buf)
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
