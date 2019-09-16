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

func (gfn *baseGFN) active() bool { return gfn.lookup.Load() }
func (gfn *baseGFN) activate()    { gfn.lookup.Store(true); glog.Infoln(gfn.tag, "activated") }
func (gfn *baseGFN) deactivate()  { gfn.lookup.Store(false); glog.Infoln(gfn.tag, "deactivated") }

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
	t.gfn.local.tag, t.gfn.global.tag = "local GFN", "global GFN"

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

		{r: "/", h: cmn.InvalidHandler, net: []string{cmn.NetworkPublic, cmn.NetworkIntraControl, cmn.NetworkIntraData}},
	}
	t.registerNetworkHandlers(networkHandlers)

	t.initRebManager(config)

	ec.Init()
	t.ecmanager = newECM(t)

	aborted, _ := t.xactions.isRebalancing(cmn.ActLocalReb)
	if aborted {
		go func() {
			glog.Infoln("resuming local rebalance...")
			t.rebManager.runLocalReb(false /*skipGlobMisplaced=false*/)
		}()
	}

	dsort.RegisterNode(t.smapowner, t.bmdowner, t.si, t, t.statsif)
	if err := t.httprunner.run(); err != nil {
		return err
	}
	return nil
}

func (t *targetrunner) initRebManager(config *cmn.Config) {
	reb := &rebManager{t: t, filterGFN: filter.NewDefaultFilter()}
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
	// serialization: one at a time
	reb.semaCh = make(chan struct{}, 1)
	reb.semaCh <- struct{}{}

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

	switch apiItems[0] {
	case cmn.List:
		bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)

		normalizedBckProvider, err := cmn.ProviderFromStr(bckProvider)
		if err != nil {
			t.invalmsghdlr(w, r, err.Error())
			return
		}

		query := r.URL.Query()
		what := query.Get(cmn.URLParamWhat)
		if what == cmn.GetWhatBucketMetaX {
			t.bucketsFromXattr(w, r)
		} else {
			t.getbucketnames(w, r, normalizedBckProvider)
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
	lom := &cluster.LOM{T: t, Objname: objName}
	if err = lom.Init(bucket, bckProvider, config); err != nil {
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

func (t *targetrunner) rangeCksum(r io.ReaderAt, fqn string, offset, length int64,
	buf []byte) (cksumValue string, sgl *memsys.SGL, rangeReader io.ReadSeeker, err error) {
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
	lom := &cluster.LOM{T: t, Objname: objname}
	if err = lom.Init(bucket, bckProvider); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if err = lom.AllowPUT(); err != nil {
		t.invalmsghdlr(w, r, err.Error())
		return
	}
	if lom.IsAIS() && lom.VerConf().Enabled {
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
		msgInt  = &actionMsgInternal{}
		started = time.Now()
	)
	apitems, err := t.checkRESTItems(w, r, 1, false, cmn.Version, cmn.Buckets)
	if err != nil {
		return
	}
	bucket := apitems[0]
	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	bck := &cluster.Bck{Name: bucket, Provider: bckProvider}
	if err = bck.Init(t.bmdowner); err != nil {
		if _, ok := err.(*cmn.ErrorCloudBucketDoesNotExist); !ok {
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
			err := t.listRangeOperation(r, apitems, bckProvider, msgInt)
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

	lom := &cluster.LOM{T: t, Objname: objname}
	if err = lom.Init(bucket, bckProvider); err != nil {
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

	bckProvider := r.URL.Query().Get(cmn.URLParamBckProvider)
	if len(apiItems) == 0 {
		switch msgInt.Action {
		case cmn.ActSummaryBucket:
			bck := &cluster.Bck{Name: "", Provider: bckProvider}
			if ok := t.bucketSummary(w, r, bck, msgInt); !ok {
				return
			}
		default:
			t.invalmsghdlr(w, r, "path is too short: bucket name expected")
		}
		return
	}

	bucket = apiItems[0]
	bck := &cluster.Bck{Name: bucket, Provider: bckProvider}
	if err = bck.Init(t.bmdowner); err != nil {
		if _, ok := err.(*cmn.ErrorCloudBucketDoesNotExist); !ok {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	}
	switch msgInt.Action {
	case cmn.ActPrefetch:
		if err := t.listRangeOperation(r, apiItems, bckProvider, msgInt); err != nil {
			t.invalmsghdlr(w, r, fmt.Sprintf("Failed to prefetch, err: %v", err))
			return
		}
	case cmn.ActCopyLB, cmn.ActRenameLB:
		var (
			phase                = apiItems[1]
			bucketFrom, bucketTo = bucket, msgInt.Name
		)
		switch phase {
		case cmn.ActBegin:
			err = t.beginCopyRenameLB(bucketFrom, bucketTo, msgInt.Action)
		case cmn.ActAbort:
			err = t.abortCopyRenameLB(bucketFrom, bucketTo, msgInt.Action)
		case cmn.ActCommit:
			err = t.commitCopyRenameLB(bucketFrom, bucketTo, msgInt)
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
		t.statsif.AddMany(stats.NamedVal64{stats.ListCount, 1}, stats.NamedVal64{stats.ListLatency, int64(delta)})
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("LIST: %s, %d µs", bucket, int64(delta/time.Microsecond))
		}
	case cmn.ActSummaryBucket:
		if ok := t.bucketSummary(w, r, bck, msgInt); !ok {
			return
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
		t.xactions.renewBckMakeNCopies(bucket, t, copies, bck.IsAIS())
	default:
		s := fmt.Sprintf(fmtUnknownAct, msgInt)
		t.invalmsghdlr(w, r, s)
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
		s := fmt.Sprintf(fmtUnknownAct, msg)
		t.invalmsghdlr(w, r, s)
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
	bck := &cluster.Bck{Name: bucket, Provider: bckProvider}
	if err = bck.Init(t.bmdowner); err != nil {
		if _, ok := err.(*cmn.ErrorCloudBucketDoesNotExist); !ok {
			t.invalmsghdlr(w, r, err.Error())
			return
		}
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		pid := query.Get(cmn.URLParamProxyID)
		glog.Infof("%s %s <= %s", r.Method, bucket, pid)
	}
	config := cmn.GCO.Get()
	if !bck.IsAIS() {
		bucketProps, err, errCode = t.cloud.headBucket(t.contextWithAuth(r.Header), bucket)
		if err != nil {
			if errCode == http.StatusNotFound {
				err = cmn.NewErrorCloudBucketDoesNotExist(bucket)
			} else {
				err = fmt.Errorf("%s: bucket %s, err: %v", t.si.Name(), bucket, err)
			}
			t.invalmsghdlr(w, r, err.Error(), errCode)
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
		checkCached, _ = cmn.ParseBool(query.Get(cmn.URLParamCheckCached))
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

	lom := &cluster.LOM{T: t, Objname: objName}
	if err = lom.Init(bucket, bckProvider); err != nil {
		invalidHandler(w, r, err.Error())
		return
	}

	lom.Lock(false)
	if err = lom.Load(); err != nil { // (doesnotexist -> ok, other)
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
	// * checkCached establishes local presence by looking up all mountpaths (and not only the current (hrw))
	// * checkCached also copies (i.e., effectively, recovers) misplaced object if it finds one
	// * see also: GFN
	if !exists && checkCached {
		exists = lom.CopyObjectFromAny()
	}

	if lom.IsAIS() || checkCached {
		if !exists {
			invalidHandler(w, r, fmt.Sprintf("%s/%s %s", bucket, objName, cmn.DoesNotExist), http.StatusNotFound)
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
	objmeta[cmn.HeaderObjBckIsAIS] = strconv.FormatBool(lom.IsAIS())
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
			AIS:   make([]string, 0, len(bmd.LBmap)),
			Cloud: make([]string, 0, 64),
		}
	)

	if bckProvider != cmn.Cloud {
		for bucket := range bmd.LBmap {
			bucketNames.AIS = append(bucketNames.AIS, bucket)
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
// ais bucket:
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
	const (
		retries = 2
	)

	mirrConf := lom.MirrorConf()
	if !mirrConf.Enabled {
		return
	}
	if nmp := fs.Mountpaths.NumAvail(); nmp < int(mirrConf.Copies) {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Warningf("insufficient ## mountpaths %d (bucket %s, ## copies %d)",
				nmp, lom.Bucket(), mirrConf.Copies)
		}
		return
	}

	var (
		err error
	)
	for i := 0; i < retries; i++ {
		xputlrep := t.xactions.renewPutLocReplicas(lom)
		if xputlrep == nil {
			return
		}
		err = xputlrep.Repl(lom)
		if _, ok := err.(*cmn.ErrXpired); !ok {
			break
		}

		// retry upon race vs (just finished/timedout)
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

	lom := &cluster.LOM{T: t, Objname: objnameFrom}
	if err = lom.Init(bucket, bckProvider); err != nil {
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
	ri := &replicInfo{smap: t.smapowner.get(), t: t, bucketTo: bucket, buf: buf}
	if copied, err := ri.copyObject(lom, msg.Name /* new objname */); err != nil {
		t.invalmsghdlr(w, r, err.Error())
	} else if copied {
		lom.Lock(true)
		// TODO: additional copies and ec cleanups
		if err = lom.Remove(); err != nil {
			t.invalmsghdlr(w, r, err.Error())
		}
		lom.Unlock(true)
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
