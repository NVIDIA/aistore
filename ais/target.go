// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/ais/backend"
	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/kvdb"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/ext/dload"
	"github.com/NVIDIA/aistore/ext/dsort"
	"github.com/NVIDIA/aistore/ext/etl"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/fs/health"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/mirror"
	"github.com/NVIDIA/aistore/reb"
	"github.com/NVIDIA/aistore/res"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/volume"
	"github.com/NVIDIA/aistore/xact/xreg"
)

const dbName = "ais.db"

const clusterClockDrift = 5 * time.Millisecond // is expected to be bounded by

type (
	regstate struct {
		mu       sync.Mutex  // serialize metasync Rx, stopping, and transitioning to standby
		disabled atomic.Bool // true: standing by
		prevbmd  atomic.Bool // special
	}
	backends map[string]cluster.BackendProvider
	// main
	target struct {
		htrun
		backend      backends
		fshc         *health.FSHC
		fsprg        fsprungroup
		reb          *reb.Reb
		res          *res.Res
		transactions transactions
		regstate     regstate
	}
)

type redial struct {
	t         *target
	dialTout  time.Duration
	totalTout time.Duration
	inUse     string
}

// interface guard
var (
	_ cos.Runner = (*target)(nil)
	_ htext      = (*target)(nil)
)

func (*target) Name() string { return apc.Target } // as cos.Runner

// as htext
func (*target) interruptedRestarted() (interrupted, restarted bool) {
	interrupted = fs.MarkerExists(fname.RebalanceMarker)
	restarted = fs.MarkerExists(fname.NodeRestartedPrev)
	return
}

func sparseVerbStats(tm int64) bool  { return tm&7 == 1 }
func sparseRedirStats(tm int64) bool { return tm&3 == 2 }

//
// target
//

func (t *target) initBackends() {
	config := cmn.GCO.Get()
	backend.Init(config)

	aisBackend := backend.NewAIS(t)
	t.backend[apc.AIS] = aisBackend                  // always present
	t.backend[apc.HTTP] = backend.NewHTTP(t, config) // ditto

	if aisConf := config.Backend.Get(apc.AIS); aisConf != nil {
		if err := aisBackend.Apply(aisConf, "init", &config.ClusterConfig); err != nil {
			nlog.Errorln(t.String()+":", err, "- proceeding to start anyway")
		} else {
			nlog.Infoln(t.String()+": remote-ais", aisConf)
		}
	}

	if err := t._initBuiltin(); err != nil {
		cos.ExitLog(err)
	}
}

// init built-in (via build tags) backends
// - remote (e.g. cloud) backends  w/ empty stubs unless populated via build tags
// - enabled/disabled via config.Backend
func (t *target) _initBuiltin() error {
	var (
		enabled, disabled, notlinked []string
		config                       = cmn.GCO.Get()
	)
	for provider := range apc.Providers {
		var (
			add cluster.BackendProvider
			err error
		)
		switch provider {
		case apc.AWS:
			add, err = backend.NewAWS(t)
		case apc.GCP:
			add, err = backend.NewGCP(t)
		case apc.Azure:
			add, err = backend.NewAzure(t)
		case apc.HDFS:
			add, err = backend.NewHDFS(t)
		case apc.AIS, apc.HTTP:
			continue
		default:
			return fmt.Errorf(cmn.FmtErrUnknown, t, "backend provider", provider)
		}
		t.backend[provider] = add

		configured := config.Backend.Get(provider) != nil
		switch {
		case err == nil && configured:
			enabled = append(enabled, provider)
		case err == nil && !configured:
			disabled = append(disabled, provider)
		case err != nil && configured:
			notlinked = append(notlinked, provider)
		}
	}
	switch {
	case len(notlinked) > 0:
		nlog.Errorf("%s backends: enabled %v, disabled %v, missing in the build %v", t, enabled, disabled, notlinked)
	case len(disabled) > 0:
		nlog.Warningf("%s backends: enabled %v, disabled %v", t, enabled, disabled)
	default:
		nlog.Infoln(t.String(), "backends:", enabled)
	}
	return nil
}

func (t *target) aisBackend() *backend.AISBackendProvider {
	bendp := t.backend[apc.AIS]
	return bendp.(*backend.AISBackendProvider)
}

func (t *target) init(config *cmn.Config) {
	t.initNetworks()

	// (a) get node ID from command-line or env var (see envDaemonID())
	// (b) load existing node ID (replicated xattr at roots of respective mountpaths)
	// (c) generate a new one (genDaemonID())
	// - in that exact sequence
	tid, generated := initTID(config)
	if generated && len(config.FSP.Paths) > 0 {
		// in an unlikely case of losing all mountpath-stored IDs but still having a volume
		tid = volume.RecoverTID(tid, config.FSP.Paths)
	}
	t.si.Init(tid, apc.Target)

	cos.InitShortID(t.si.Digest())

	memsys.Init(t.SID(), t.SID(), config)

	newVol := volume.Init(t, config, daemon.cli.target.allowSharedDisksAndNoDisks,
		daemon.cli.target.useLoopbackDevs, daemon.cli.target.startWithLostMountpath)

	t.initHostIP()
	daemon.rg.add(t)

	ts := stats.NewTrunner(t) // iostat below
	startedUp := ts.Init(t)   // reg common metrics (and target-only - via RegMetrics/regDiskMetrics below)
	daemon.rg.add(ts)
	t.statsT = ts // stats tracker

	k := newTalive(t, ts, startedUp)
	daemon.rg.add(k)
	t.keepalive = k

	t.fsprg.init(t, newVol) // subgroup of the daemon.rg rungroup

	sc := transport.Init(ts, config) // init transport sub-system; new stream collector
	daemon.rg.add(sc)

	fshc := health.NewFSHC(t)
	daemon.rg.add(fshc)
	t.fshc = fshc

	if err := ts.InitCDF(); err != nil { // goes after fs.New
		cos.ExitLog(err)
	}
	fs.Clblk()

	s3.Init() // s3 multipart
}

func (t *target) initHostIP() {
	var hostIP string
	if hostIP = os.Getenv("AIS_HOST_IP"); hostIP == "" {
		return
	}
	var (
		config  = cmn.GCO.Get()
		port    = config.HostNet.Port
		extAddr = net.ParseIP(hostIP)
		extPort = port
	)
	if portStr := os.Getenv("AIS_HOST_PORT"); portStr != "" {
		portNum, err := cmn.ParsePort(portStr)
		cos.AssertNoErr(err)
		extPort = portNum
	}
	t.si.PubNet.Hostname = extAddr.String()
	t.si.PubNet.Port = strconv.Itoa(extPort)
	t.si.PubNet.URL = fmt.Sprintf("%s://%s:%d", config.Net.HTTP.Proto, extAddr.String(), extPort)
	nlog.Infof("AIS_HOST_IP=%s; PubNetwork=%s", hostIP, t.si.URL(cmn.NetPublic))

	// applies to intra-cluster networks unless separately defined
	if !config.HostNet.UseIntraControl {
		t.si.ControlNet = t.si.PubNet
	}
	if !config.HostNet.UseIntraData {
		t.si.DataNet = t.si.PubNet
	}
}

func initTID(config *cmn.Config) (tid string, generated bool) {
	if tid = envDaemonID(apc.Target); tid != "" {
		if err := cos.ValidateDaemonID(tid); err != nil {
			nlog.Errorf("Warning: %v", err)
		}
		return
	}

	var err error
	if tid, err = fs.LoadNodeID(config.FSP.Paths); err != nil {
		cos.ExitLog(err)
	}
	if tid != "" {
		return
	}

	tid = genDaemonID(apc.Target, config)
	err = cos.ValidateDaemonID(tid)
	debug.AssertNoErr(err)
	nlog.Infof("t[%s] ID randomly generated", tid)
	generated = true
	return
}

func regDiskMetrics(node *meta.Snode, tstats *stats.Trunner, mpi fs.MPI) {
	for _, mi := range mpi {
		for _, disk := range mi.Disks {
			tstats.RegDiskMetrics(node, disk)
		}
	}
}

func (t *target) Run() error {
	if err := t.si.Validate(); err != nil {
		cos.ExitLog(err)
	}
	config := cmn.GCO.Get()
	t.htrun.init(config)

	tstats := t.statsT.(*stats.Trunner)

	cluster.Tinit(t, tstats, true /*run hk*/)

	// metrics, disks first
	availablePaths, disabledPaths := fs.Get()
	if len(availablePaths) == 0 {
		cos.ExitLog(cmn.ErrNoMountpaths)
	}
	regDiskMetrics(t.si, tstats, availablePaths)
	regDiskMetrics(t.si, tstats, disabledPaths)
	t.statsT.RegMetrics(t.si) // + Prometheus, if configured

	fatalErr, writeErr := t.checkRestarted(config)
	if fatalErr != nil {
		cos.ExitLog(fatalErr)
	}
	if writeErr != nil {
		nlog.Errorln("")
		nlog.Errorln(writeErr)
		nlog.Errorln("")
	}

	// register object type and workfile type
	fs.CSM.Reg(fs.ObjectType, &fs.ObjectContentResolver{})
	fs.CSM.Reg(fs.WorkfileType, &fs.WorkfileContentResolver{})

	// Init meta-owners and load local instances
	if prev := t.owner.bmd.init(); prev {
		t.regstate.prevbmd.Store(true)
	}
	t.owner.etl.init()

	smap, reliable := t.loadSmap()
	if !reliable {
		smap = newSmap()
		smap.Tmap[t.SID()] = t.si // add self to initial temp smap
	} else {
		nlog.Infoln(t.String()+": loaded", smap.StringEx())
	}
	t.owner.smap.put(smap)

	if daemon.cli.target.standby {
		tstats.Standby(true)
		t.regstate.disabled.Store(true)
		nlog.Warningln(t.String(), "not joining - standing by")

		// see endStartupStandby()
	} else {
		// discover primary and join cluster (compare with manual `apc.AdminJoin`)
		if status, err := t.joinCluster(apc.ActSelfJoinTarget); err != nil {
			nlog.Errorf("%s failed to join cluster: %v(%d)", t, err, status)
			nlog.Errorln(t.String(), "terminating")
			return err
		}
		t.markNodeStarted()
		go t.gojoin(config)
	}

	t.initBackends()

	db, err := kvdb.NewBuntDB(filepath.Join(config.ConfigDir, dbName))
	if err != nil {
		nlog.Errorln(t.String(), "failed to initialize kvdb:", err)
		return err
	}

	dload.SetDB(db)

	archive.Init(config.Features)

	// transactions
	t.transactions.init(t)

	t.reb = reb.New(t, config)
	t.res = res.New(t)

	// register storage target's handler(s) and start listening
	t.initRecvHandlers()

	ec.Init(t)
	mirror.Init()

	xreg.RegWithHK()

	marked := xreg.GetResilverMarked()
	if marked.Interrupted || daemon.resilver.required {
		go t.goreslver(marked.Interrupted)
	}

	dsort.Tinit(t, t.statsT, db)
	dload.Init(t)

	err = t.htrun.run(config)

	etl.StopAll(t)                             // stop all running ETLs if any
	cos.Close(db)                              // close kv db
	fs.RemoveMarker(fname.NodeRestartedMarker) // exit gracefully
	return err
}

func (t *target) gojoin(config *cmn.Config) {
	smap := t.owner.smap.get()
	cii := t.pollClusterStarted(config, smap.Primary)
	if daemon.stopping.Load() {
		return
	}
	if cii != nil {
		if status, err := t.joinCluster(apc.ActSelfJoinTarget,
			cii.Smap.Primary.CtrlURL, cii.Smap.Primary.PubURL); err != nil {
			nlog.Errorf("%s failed to re-join cluster (status: %d, err: %v)", t, status, err)
			return
		}
	}
	t.markClusterStarted()

	if t.fsprg.newVol && !config.TestingEnv() {
		config := cmn.GCO.BeginUpdate()
		fspathsSave(config)
	}
}

func (t *target) goreslver(interrupted bool) {
	if interrupted {
		nlog.Infoln("Resuming resilver...")
	} else if daemon.resilver.required {
		nlog.Infof("Starting resilver, reason: %q", daemon.resilver.reason)
	}
	t.runResilver(res.Args{}, nil /*wg*/)
}

func (t *target) runResilver(args res.Args, wg *sync.WaitGroup) {
	// with no cluster-wide UUID it's a local run
	if args.UUID == "" {
		args.UUID = cos.GenUUID()
		regMsg := xactRegMsg{UUID: args.UUID, Kind: apc.ActResilver, Srcs: []string{t.SID()}}
		msg := t.newAmsgActVal(apc.ActRegGlobalXaction, regMsg)
		t.bcastAsyncIC(msg)
	}
	if wg != nil {
		wg.Done() // compare w/ xact.GoRunW(()
	}
	t.res.RunResilver(args)
}

func (t *target) endStartupStandby() (err error) {
	smap := t.owner.smap.get()
	if err = smap.validate(); err != nil {
		return
	}
	daemon.cli.target.standby = false
	t.markNodeStarted()
	t.markClusterStarted()
	t.regstate.disabled.Store(false)
	tstats := t.statsT.(*stats.Trunner)
	tstats.Standby(false)
	nlog.Infof("%s enabled and joined (%s)", t, smap.StringEx())

	config := cmn.GCO.Get()
	if t.fsprg.newVol && !config.TestingEnv() {
		config = cmn.GCO.BeginUpdate()
		fspathsSave(config)
	}
	return
}

func (t *target) initRecvHandlers() {
	networkHandlers := []networkHandler{
		{r: apc.Buckets, h: t.bucketHandler, net: accessNetAll},
		{r: apc.Objects, h: t.objectHandler, net: accessNetAll},
		{r: apc.Daemon, h: t.daemonHandler, net: accessNetPublicControl},
		{r: apc.Metasync, h: t.metasyncHandler, net: accessNetIntraControl},
		{r: apc.Health, h: t.healthHandler, net: accessNetPublicControl},
		{r: apc.Xactions, h: t.xactHandler, net: accessNetIntraControl},
		{r: apc.EC, h: t.ecHandler, net: accessNetIntraData},
		{r: apc.Vote, h: t.voteHandler, net: accessNetIntraControl},
		{r: apc.Txn, h: t.txnHandler, net: accessNetIntraControl},
		{r: apc.ObjStream, h: transport.RxAnyStream, net: accessControlData},

		{r: apc.Download, h: t.downloadHandler, net: accessNetIntraControl},
		{r: apc.Sort, h: dsort.TargetHandler, net: accessControlData},
		{r: apc.ETL, h: t.etlHandler, net: accessNetAll},

		{r: "/" + apc.S3, h: t.s3Handler, net: accessNetPublicData},
		{r: "/", h: t.errURL, net: accessNetAll},
	}
	t.regNetHandlers(networkHandlers)
}

func (t *target) checkRestarted(config *cmn.Config) (fatalErr, writeErr error) {
	if fs.MarkerExists(fname.NodeRestartedMarker) {
		red := redial{t: t, dialTout: config.Timeout.CplaneOperation.D(), totalTout: config.Timeout.MaxKeepalive.D()}
		if red.acked() {
			fatalErr = fmt.Errorf("%s: %q is in use (duplicate or overlapping run?)", t, red.inUse)
			return
		}
		t.statsT.Inc(stats.RestartCount)
		fs.PersistMarker(fname.NodeRestartedPrev)
	}
	fatalErr, writeErr = fs.PersistMarker(fname.NodeRestartedMarker)
	return
}

// NOTE in re 'node-restarted' scenario: the risk of "overlapping" aisnode run -
// which'll fail shortly with "bind: address already in use" but not before
// triggering (`NodeRestartedPrev` => GFN) sequence and stealing nlog symlinks
// - this risk exists, and that's why we go extra length
func (red *redial) acked() bool {
	var (
		err   error
		tsi   = red.t.si
		sleep = cos.ProbingFrequency(red.totalTout)
		addrs = []string{tsi.PubNet.TCPEndpoint()}
		once  bool
	)
	if ep := red.t.si.DataNet.TCPEndpoint(); ep != addrs[0] {
		addrs = append(addrs, ep)
	} else if ep := red.t.si.ControlNet.TCPEndpoint(); ep != addrs[0] {
		addrs = append(addrs, ep)
	}
	for _, addr := range addrs {
		for elapsed := time.Duration(0); elapsed < red.totalTout; elapsed += sleep {
			_, err = net.DialTimeout("tcp4", addr, max(2*time.Second, red.dialTout))
			if err != nil {
				break
			}
			once = true
			time.Sleep(sleep)
			// could be shutting down
		}
		if !once {
			return false
		}
		if err == nil {
			if red.inUse == "" {
				red.inUse = addr
			}
			return true
		}
		time.Sleep(sleep)
	}
	return false // got tcp synack at least once but not (getting it) any longer
}

//
// http handlers
//

func (t *target) errURL(w http.ResponseWriter, r *http.Request) {
	if r.URL.Scheme != "" {
		t.writeErrURL(w, r)
		return
	}
	path := r.URL.Path
	if len(path) > 0 && path[0] == '/' {
		path = path[1:]
	}
	split := strings.Split(path, "/")
	// "easy URL"
	if len(split) > 0 &&
		(split[0] == apc.GSScheme || split[0] == apc.AZScheme || split[0] == apc.AISScheme) {
		t.writeErrMsg(w, r, "trying to execute \"easy URL\" via AIS target? (hint: use proxy)")
	} else {
		t.writeErrURL(w, r)
	}
}

// verb /v1/buckets
func (t *target) bucketHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t.httpbckget(w, r)
	case http.MethodDelete:
		apireq := apiReqAlloc(1, apc.URLPathBuckets.L, false)
		t.httpbckdelete(w, r, apireq)
		apiReqFree(apireq)
	case http.MethodPost:
		apireq := apiReqAlloc(1, apc.URLPathBuckets.L, false)
		t.httpbckpost(w, r, apireq)
		apiReqFree(apireq)
	case http.MethodHead:
		apireq := apiReqAlloc(1, apc.URLPathBuckets.L, false)
		t.httpbckhead(w, r, apireq)
		apiReqFree(apireq)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodHead, http.MethodPost)
	}
}

// verb /v1/objects
func (t *target) objectHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		apireq := apiReqAlloc(2, apc.URLPathObjects.L, true /*dpq*/)
		t.httpobjget(w, r, apireq)
		apiReqFree(apireq)
	case http.MethodHead:
		apireq := apiReqAlloc(2, apc.URLPathObjects.L, false)
		t.httpobjhead(w, r, apireq)
		apiReqFree(apireq)
	case http.MethodPut:
		apireq := apiReqAlloc(2, apc.URLPathObjects.L, true /*dpq*/)
		if err := t.parseReq(w, r, apireq); err == nil {
			lom := cluster.AllocLOM(apireq.items[1])
			t.httpobjput(w, r, apireq, lom)
			cluster.FreeLOM(lom)
		}
		apiReqFree(apireq)
	case http.MethodDelete:
		apireq := apiReqAlloc(2, apc.URLPathObjects.L, false)
		t.httpobjdelete(w, r, apireq)
		apiReqFree(apireq)
	case http.MethodPost:
		apireq := apiReqAlloc(2, apc.URLPathObjects.L, false /*useDpq*/)
		t.httpobjpost(w, r, apireq)
		apiReqFree(apireq)
	case http.MethodPatch:
		apireq := apiReqAlloc(2, apc.URLPathObjects.L, false)
		t.httpobjpatch(w, r, apireq)
		apiReqFree(apireq)
	default:
		cmn.WriteErr405(w, r, http.MethodDelete, http.MethodGet, http.MethodHead,
			http.MethodPost, http.MethodPut)
	}
}

// verb /v1/slices
// Non-public inerface
func (t *target) ecHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t.httpecget(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodGet)
	}
}

//
// httpobj* handlers
//

// GET /v1/objects/<bucket-name>/<object-name>
//
// Initially validates if the request is internal request (either from proxy
// or target) and calls getObject.
//
// Checks if the object exists locally (if not, downloads it) and sends it back
// If the bucket is in the Cloud one and ValidateWarmGet is enabled there is an extra
// check whether the object exists locally. Version is checked as well if configured.
func (t *target) httpobjget(w http.ResponseWriter, r *http.Request, apireq *apiRequest) {
	if err := t.parseReq(w, r, apireq); err != nil {
		return
	}
	if err := apireq.dpq.fromRawQ(r.URL.RawQuery); err != nil {
		debug.AssertNoErr(err)
		t.writeErr(w, r, err)
		return
	}
	if cmn.Features.IsSet(feat.EnforceIntraClusterAccess) {
		if apireq.dpq.ptime == "" /*isRedirect*/ && t.isIntraCall(r.Header, false /*from primary*/) != nil {
			t.writeErrf(w, r, "%s: %s(obj) is expected to be redirected (remaddr=%s)",
				t.si, r.Method, r.RemoteAddr)
			return
		}
	}
	lom := cluster.AllocLOM(apireq.items[1])
	lom = t.getObject(w, r, apireq.dpq, apireq.bck, lom)
	cluster.FreeLOM(lom)
}

// getObject is main function to get the object. It doesn't check request origin,
// so it must be done by the caller (if necessary).
func (t *target) getObject(w http.ResponseWriter, r *http.Request, dpq *dpq, bck *meta.Bck, lom *cluster.LOM) *cluster.LOM {
	if err := lom.InitBck(bck.Bucket()); err != nil {
		if cmn.IsErrRemoteBckNotFound(err) {
			t.BMDVersionFixup(r)
			err = lom.InitBck(bck.Bucket())
		}
		if err != nil {
			t._erris(w, r, dpq.silent, err, 0)
			return lom
		}
	}

	debug.Assert(dpq.uuid == "", dpq.uuid)
	if dpq.etlName != "" {
		t.doETL(w, r, dpq.etlName, bck, lom.ObjName)
		return lom
	}

	filename := dpq.archpath // apc.QparamArchpath
	if strings.HasPrefix(filename, lom.ObjName) {
		if rel, err := filepath.Rel(lom.ObjName, filename); err == nil {
			filename = rel
		}
	}
	// GET context
	goi := allocGOI()
	{
		goi.atime = time.Now().UnixNano()
		if dpq.ptime != "" && sparseRedirStats(goi.atime) {
			if d := ptLatency(goi.atime, dpq.ptime, r.Header.Get(apc.HdrCallerIsPrimary)); d > 0 {
				t.statsT.Add(stats.GetRedirLatency, d)
			}
		}
		goi.t = t
		goi.lom = lom
		goi.w = w
		goi.ctx = context.Background()
		goi.ranges = byteRanges{Range: r.Header.Get(cos.HdrRange), Size: 0}
		goi.archive = archiveQuery{
			filename: filename,
			mime:     dpq.archmime, // query.Get(apc.QparamArchmime)
		}
		goi.isGFN = cos.IsParseBool(dpq.isGFN) // query.Get(apc.QparamIsGFNRequest)
		// goi.chunked = cmn.GCO.Get().Net.HTTP.Chunked NOTE: disabled - no need
	}
	if bck.IsHTTP() {
		originalURL := dpq.origURL // query.Get(apc.QparamOrigURL)
		goi.ctx = context.WithValue(goi.ctx, cos.CtxOriginalURL, originalURL)
	}
	if errCode, err := goi.getObject(); err != nil {
		t.statsT.IncErr(stats.GetCount)
		if err != errSendingResp {
			t._erris(w, r, dpq.silent, err, errCode)
		}
	}
	lom = goi.lom
	freeGOI(goi)
	return lom
}

// err in silence
func (t *target) _erris(w http.ResponseWriter, r *http.Request, silent string /*apc.QparamSilent*/, err error, code int) {
	if cos.IsParseBool(silent) {
		t.writeErr(w, r, err, code, Silent)
	} else {
		t.writeErr(w, r, err, code)
	}
}

// PUT /v1/objects/bucket-name/object-name; does:
// 1) append object 2) append to archive 3) PUT
func (t *target) httpobjput(w http.ResponseWriter, r *http.Request, apireq *apiRequest, lom *cluster.LOM) {
	var (
		config  = cmn.GCO.Get()
		started = time.Now().UnixNano()
		t2tput  = isT2TPut(r.Header)
	)
	if apireq.dpq.ptime == "" && !t2tput {
		t.writeErrf(w, r, "%s: %s(obj) is expected to be redirected or replicated", t.si, r.Method)
		return
	}
	cs := fs.Cap()
	if errCap := cs.Err(); errCap != nil || cs.PctMax > int32(config.Space.CleanupWM) {
		cs = t.OOS(nil)
		if cs.IsOOS() {
			// fail this write
			t.writeErr(w, r, errCap, http.StatusInsufficientStorage)
			return
		}
	}

	// init
	if err := lom.InitBck(apireq.bck.Bucket()); err != nil {
		if cmn.IsErrRemoteBckNotFound(err) {
			t.BMDVersionFixup(r)
			err = lom.InitBck(apireq.bck.Bucket())
		}
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
	}

	// load (maybe)
	var (
		errdb  error
		skipVC = cmn.Features.IsSet(feat.SkipVC) || cos.IsParseBool(apireq.dpq.skipVC) // apc.QparamSkipVC
	)
	if skipVC {
		errdb = lom.AllowDisconnectedBackend(false)
	} else if lom.Load(true, false) == nil {
		errdb = lom.AllowDisconnectedBackend(true)
	}
	if errdb != nil {
		t.writeErr(w, r, errdb)
		return
	}

	// do
	var (
		handle  string
		err     error
		errCode int
	)
	switch {
	case apireq.dpq.archpath != "": // apc.QparamArchpath
		apireq.dpq.archmime, err = archive.MimeFQN(t.smm, apireq.dpq.archmime, lom.FQN)
		if err != nil {
			break
		}
		// do
		lom.Lock(true)
		errCode, err = t.putApndArch(r, lom, started, apireq.dpq)
		lom.Unlock(true)
	case apireq.dpq.appendTy != "": // apc.QparamAppendType
		handle, errCode, err = t.appendObj(r, lom, started, apireq.dpq)
		if err == nil {
			w.Header().Set(apc.HdrAppendHandle, handle)
			return
		}
		t.statsT.IncErr(stats.AppendCount)
	default:
		poi := allocPOI()
		{
			poi.atime = started
			if apireq.dpq.ptime != "" && sparseRedirStats(poi.atime) {
				if d := ptLatency(poi.atime, apireq.dpq.ptime, r.Header.Get(apc.HdrCallerIsPrimary)); d > 0 {
					t.statsT.Add(stats.PutRedirLatency, d)
				}
			}
			poi.t = t
			poi.lom = lom
			poi.config = config
			poi.skipVC = skipVC
			poi.restful = true
			poi.t2t = t2tput
		}
		errCode, err = poi.do(w.Header(), r, apireq.dpq)
		freePOI(poi)
	}
	if err != nil {
		t.fsErr(err, lom.FQN)
		t.writeErr(w, r, err, errCode)
	}
}

// DELETE [ { action } ] /v1/objects/bucket-name/object-name
func (t *target) httpobjdelete(w http.ResponseWriter, r *http.Request, apireq *apiRequest) {
	var msg aisMsg
	if err := readJSON(w, r, &msg); err != nil {
		return
	}
	if err := t.parseReq(w, r, apireq); err != nil {
		return
	}
	objName := apireq.items[1]
	if !t.isValidObjname(w, r, objName) {
		return
	}
	if isRedirect(apireq.query) == "" {
		t.writeErrf(w, r, "%s: %s(obj) is expected to be redirected", t.si, r.Method)
		return
	}

	evict := msg.Action == apc.ActEvictObjects
	lom := cluster.AllocLOM(objName)
	if err := lom.InitBck(apireq.bck.Bucket()); err != nil {
		t.writeErr(w, r, err)
		cluster.FreeLOM(lom)
		return
	}

	errCode, err := t.DeleteObject(lom, evict)
	if err == nil {
		// EC cleanup if EC is enabled
		ec.ECM.CleanupObject(lom)
	} else {
		if errCode == http.StatusNotFound {
			t.writeErrSilentf(w, r, http.StatusNotFound, "%s doesn't exist", lom.Cname())
		} else {
			t.writeErr(w, r, err, errCode)
		}
	}
	cluster.FreeLOM(lom)
}

// POST /v1/objects/bucket-name/object-name
func (t *target) httpobjpost(w http.ResponseWriter, r *http.Request, apireq *apiRequest) {
	msg, err := t.readActionMsg(w, r)
	if err != nil {
		return
	}
	if msg.Action != apc.ActRenameObject {
		t.writeErrAct(w, r, msg.Action)
		return
	}
	if t.parseReq(w, r, apireq) != nil {
		return
	}
	if isRedirect(apireq.query) == "" {
		t.writeErrf(w, r, "%s: %s-%s(obj) is expected to be redirected", t.si, r.Method, msg.Action)
		return
	}

	lom := cluster.AllocLOM(apireq.items[1])
	err = lom.InitBck(apireq.bck.Bucket())
	if err == nil {
		err = t.objMv(lom, msg)
	}
	if err == nil {
		t.statsT.Inc(stats.RenameCount)
	} else {
		t.statsT.IncErr(stats.RenameCount)
		t.writeErr(w, r, err)
	}
	cluster.FreeLOM(lom)
}

// HEAD /v1/objects/<bucket-name>/<object-name>
func (t *target) httpobjhead(w http.ResponseWriter, r *http.Request, apireq *apiRequest) {
	if err := t.parseReq(w, r, apireq); err != nil {
		return
	}
	query, bck, objName := apireq.query, apireq.bck, apireq.items[1]
	if cmn.Features.IsSet(feat.EnforceIntraClusterAccess) {
		// validates that the request is internal (by a node in the same cluster)
		if isRedirect(query) == "" && t.isIntraCall(r.Header, false) != nil {
			t.writeErrf(w, r, "%s: %s(obj) is expected to be redirected (remaddr=%s)",
				t.si, r.Method, r.RemoteAddr)
			return
		}
	}
	lom := cluster.AllocLOM(objName)
	errCode, err := t.objhead(w.Header(), query, bck, lom)
	cluster.FreeLOM(lom)
	if err != nil {
		t._erris(w, r, query.Get(apc.QparamSilent), err, errCode)
	}
}

func (t *target) objhead(hdr http.Header, query url.Values, bck *meta.Bck, lom *cluster.LOM) (errCode int, err error) {
	var (
		fltPresence int
		exists      = true
		hasEC       bool
	)
	if tmp := query.Get(apc.QparamFltPresence); tmp != "" {
		var erp error
		fltPresence, erp = strconv.Atoi(tmp)
		debug.AssertNoErr(erp)
	}
	if err = lom.InitBck(bck.Bucket()); err != nil {
		if cmn.IsErrBucketNought(err) {
			errCode = http.StatusNotFound
		}
		return
	}
	err = lom.Load(true /*cache it*/, false /*locked*/)
	if err == nil {
		if apc.IsFltNoProps(fltPresence) {
			return
		}
		if fltPresence == apc.FltExistsOutside {
			err = fmt.Errorf(fmtOutside, lom.Cname(), fltPresence)
			return
		}
	} else {
		if !cmn.IsObjNotExist(err) {
			errCode = http.StatusNotFound
			return
		}
		exists = false
		if fltPresence == apc.FltPresentCluster {
			exists = lom.RestoreToLocation()
		}
	}

	if !exists {
		if bck.IsAIS() || apc.IsFltPresent(fltPresence) {
			err = cos.NewErrNotFound("%s: object %s", t, lom.Cname())
			return http.StatusNotFound, err
		}
	}

	// props
	op := cmn.ObjectProps{Name: lom.ObjName, Bck: *lom.Bucket(), Present: exists}
	if exists {
		op.ObjAttrs = *lom.ObjAttrs()
		op.Location = lom.Location()
		op.Mirror.Copies = lom.NumCopies()
		if lom.HasCopies() {
			lom.Lock(false)
			for fs := range lom.GetCopies() {
				if idx := strings.Index(fs, "/@"); idx >= 0 {
					fs = fs[:idx]
				}
				op.Mirror.Paths = append(op.Mirror.Paths, fs)
			}
			lom.Unlock(false)
		} else {
			fs := lom.FQN
			if idx := strings.Index(fs, "/@"); idx >= 0 {
				fs = fs[:idx]
			}
			op.Mirror.Paths = append(op.Mirror.Paths, fs)
		}
		if lom.Bck().Props.EC.Enabled {
			if md, err := ec.ObjectMetadata(lom.Bck(), lom.ObjName); err == nil {
				hasEC = true
				op.EC.DataSlices = md.Data
				op.EC.ParitySlices = md.Parity
				op.EC.IsECCopy = md.IsCopy
				op.EC.Generation = md.Generation
			}
		}
	} else {
		// cold HEAD
		var oa *cmn.ObjAttrs
		oa, errCode, err = t.Backend(lom.Bck()).HeadObj(context.Background(), lom)
		if err != nil {
			if errCode != http.StatusNotFound {
				err = cmn.NewErrFailedTo(t, "HEAD", lom, err)
			}
			return
		}
		if apc.IsFltNoProps(fltPresence) {
			return
		}
		op.ObjAttrs = *oa
		op.ObjAttrs.Atime = 0
	}

	// to header
	cmn.ToHeader(&op.ObjAttrs, hdr)
	if op.ObjAttrs.Cksum == nil {
		// cos.Cksum does not have default nil/zero value (reflection)
		op.ObjAttrs.Cksum = cos.NewCksum("", "")
	}
	errIter := cmn.IterFields(op, func(tag string, field cmn.IterField) (err error, b bool) {
		if !hasEC && strings.HasPrefix(tag, "ec.") {
			return nil, false
		}
		// NOTE: op.ObjAttrs were already added via cmn.ToHeader
		if tag[0] == '.' {
			return nil, false
		}
		v := field.String()
		if v == "" {
			return nil, false
		}
		name := cmn.PropToHeader(tag)
		debug.Func(func() {
			vv := hdr.Get(name)
			debug.Assertf(vv == "", "not expecting duplications: %s=(%q, %q)", name, v, vv)
		})
		hdr.Set(name, v)
		return nil, false
	})
	debug.AssertNoErr(errIter)
	return
}

// PATCH /v1/objects/<bucket-name>/<object-name>
// By default, adds or updates existing custom keys. Will remove all existing keys and
// replace them with the specified ones _iff_ `apc.QparamNewCustom` is set.
func (t *target) httpobjpatch(w http.ResponseWriter, r *http.Request, apireq *apiRequest) {
	if err := t.parseReq(w, r, apireq); err != nil {
		return
	}
	if cmn.Features.IsSet(feat.EnforceIntraClusterAccess) {
		if isRedirect(apireq.query) == "" && t.isIntraCall(r.Header, false) != nil {
			t.writeErrf(w, r, "%s: %s(obj) is expected to be redirected (remaddr=%s)",
				t.si, r.Method, r.RemoteAddr)
			return
		}
	}
	msg, err := t.readActionMsg(w, r)
	if err != nil {
		return
	}
	custom := cos.StrKVs{}
	if err := cos.MorphMarshal(msg.Value, &custom); err != nil {
		t.writeErrf(w, r, cmn.FmtErrMorphUnmarshal, t.si, "set-custom", msg.Value, err)
		return
	}
	lom := cluster.AllocLOM(apireq.items[1] /*objName*/)
	defer cluster.FreeLOM(lom)
	if err := lom.InitBck(apireq.bck.Bucket()); err != nil {
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
	delOldSetNew := cos.IsParseBool(apireq.query.Get(apc.QparamNewCustom))
	if delOldSetNew {
		lom.SetCustomMD(custom)
	} else {
		for key, val := range custom {
			lom.SetCustomKey(key, val)
		}
	}
	lom.Persist()
}

//
// httpec* handlers
//

// Returns a slice. Does not use GFN.
func (t *target) httpecget(w http.ResponseWriter, r *http.Request) {
	apireq := apiReqAlloc(3, apc.URLPathEC.L, false)
	apireq.bckIdx = 1
	if err := t.parseReq(w, r, apireq); err != nil {
		apiReqFree(apireq)
		return
	}
	switch apireq.items[0] {
	case ec.URLMeta:
		t.sendECMetafile(w, r, apireq.bck, apireq.items[2])
	case ec.URLCT:
		t.sendECCT(w, r, apireq.bck, apireq.items[2])
	default:
		t.writeErrURL(w, r)
	}
	apiReqFree(apireq)
}

// Returns a CT's metadata.
func (t *target) sendECMetafile(w http.ResponseWriter, r *http.Request, bck *meta.Bck, objName string) {
	if err := bck.Init(t.owner.bmd); err != nil {
		if !cmn.IsErrRemoteBckNotFound(err) { // is ais
			t.writeErr(w, r, err, Silent)
			return
		}
	}
	md, err := ec.ObjectMetadata(bck, objName)
	if err != nil {
		if os.IsNotExist(err) {
			t.writeErr(w, r, err, http.StatusNotFound, Silent)
		} else {
			t.writeErr(w, r, err, http.StatusInternalServerError, Silent)
		}
		return
	}
	b := md.NewPack()
	w.Header().Set(cos.HdrContentLength, strconv.Itoa(len(b)))
	w.Write(b)
}

func (t *target) sendECCT(w http.ResponseWriter, r *http.Request, bck *meta.Bck, objName string) {
	lom := cluster.AllocLOM(objName)
	defer cluster.FreeLOM(lom)
	if err := lom.InitBck(bck.Bucket()); err != nil {
		if cmn.IsErrRemoteBckNotFound(err) {
			t.BMDVersionFixup(r)
			err = lom.InitBck(bck.Bucket())
		}
		if err != nil {
			t.writeErr(w, r, err)
			return
		}
	}
	sliceFQN := lom.Mountpath().MakePathFQN(bck.Bucket(), fs.ECSliceType, objName)
	finfo, err := os.Stat(sliceFQN)
	if err != nil {
		t.writeErr(w, r, err, http.StatusNotFound, Silent)
		return
	}
	file, err := os.Open(sliceFQN)
	if err != nil {
		t.fsErr(err, sliceFQN)
		t.writeErr(w, r, err, http.StatusInternalServerError)
		return
	}

	w.Header().Set(cos.HdrContentLength, strconv.FormatInt(finfo.Size(), 10))
	_, err = io.Copy(w, file) // No need for `io.CopyBuffer` as `sendfile` syscall will be used.
	cos.Close(file)
	if err != nil {
		nlog.Errorf("Failed to send slice %s: %v", bck.Cname(objName), err)
	}
}

//
// supporting methods
//

func (t *target) CompareObjects(ctx context.Context, lom *cluster.LOM) (equal bool, errCode int, err error) {
	var objAttrs *cmn.ObjAttrs
	objAttrs, errCode, err = t.Backend(lom.Bck()).HeadObj(ctx, lom)
	if err != nil {
		err = cmn.NewErrFailedTo(t, "HEAD(object)", lom, err)
	} else {
		equal = lom.Equal(objAttrs)
	}
	return
}

func (t *target) appendObj(r *http.Request, lom *cluster.LOM, started int64, dpq *dpq) (string, int, error) {
	var (
		cksumValue    = r.Header.Get(apc.HdrObjCksumVal)
		cksumType     = r.Header.Get(apc.HdrObjCksumType)
		contentLength = r.Header.Get(cos.HdrContentLength)
	)
	hdl, err := parseAppendHandle(dpq.appendHdl) // apc.QparamAppendHandle
	if err != nil {
		return "", http.StatusBadRequest, err
	}
	a := &apndOI{
		started: started,
		t:       t,
		lom:     lom,
		r:       r.Body,
		op:      dpq.appendTy, // apc.QparamAppendType
		hdl:     hdl,
	}
	if a.op != apc.AppendOp && a.op != apc.FlushOp {
		err = fmt.Errorf("invalid operation %q (expecting either %q or %q) - check %q query",
			a.op, apc.AppendOp, apc.FlushOp, apc.QparamAppendType)
		return "", http.StatusBadRequest, err
	}
	if contentLength != "" {
		if size, ers := strconv.ParseInt(contentLength, 10, 64); ers == nil {
			a.size = size
		}
	}
	if cksumValue != "" {
		a.cksum = cos.NewCksum(cksumType, cksumValue)
	}
	return a.do()
}

// called under lock
func (t *target) putApndArch(r *http.Request, lom *cluster.LOM, started int64, dpq *dpq) (int, error) {
	var (
		mime     = dpq.archmime // apc.QparamArchmime
		filename = dpq.archpath // apc.QparamArchpath
		flags    int64
	)
	if strings.HasPrefix(filename, lom.ObjName) {
		if rel, err := filepath.Rel(lom.ObjName, filename); err == nil {
			filename = rel
		}
	}
	if s := r.Header.Get(apc.HdrPutApndArchFlags); s != "" {
		var errV error
		if flags, errV = strconv.ParseInt(s, 10, 64); errV != nil {
			return http.StatusBadRequest,
				fmt.Errorf("failed to archive %s: invalid flags %q in the request", lom.Cname(), s)
		}
	}
	a := &putA2I{
		started:  started,
		t:        t,
		lom:      lom,
		r:        r.Body,
		filename: filename,
		mime:     mime,
		put:      false, // below
	}
	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		if !os.IsNotExist(err) {
			return http.StatusInternalServerError, err
		}
		if flags == apc.ArchAppend {
			return http.StatusNotFound, err
		}
		a.put = true
	} else {
		a.put = (flags == 0)
	}
	if s := r.Header.Get(cos.HdrContentLength); s != "" {
		if size, err := strconv.ParseInt(s, 10, 64); err == nil {
			a.size = size
		}
	}
	if a.size == 0 {
		return http.StatusBadRequest, fmt.Errorf("failed to archive %s: missing %q in the request",
			lom.Cname(), cos.HdrContentLength)
	}
	return a.do()
}

func (t *target) DeleteObject(lom *cluster.LOM, evict bool) (code int, err error) {
	var isback bool
	lom.Lock(true)
	code, err, isback = t.delobj(lom, evict)
	lom.Unlock(true)

	// special corner-case retry (quote):
	// - googleapi: "Error 503: We encountered an internal error. Please try again."
	// - aws-error[InternalError: We encountered an internal error. Please try again.]
	if err != nil && isback {
		if code == http.StatusServiceUnavailable || strings.Contains(err.Error(), "try again") {
			nlog.Errorf("failed to delete %s: %v(%d) - retrying...", lom, err, code)
			time.Sleep(time.Second)
			code, err = t.Backend(lom.Bck()).DeleteObj(lom)
		}
	}
	if err == nil {
		t.statsT.Inc(stats.DeleteCount)
	} else {
		t.statsT.IncErr(stats.DeleteCount) // TODO: count GET/PUT/DELETE remote errors separately..
	}
	return
}

func (t *target) delobj(lom *cluster.LOM, evict bool) (int, error, bool) {
	var (
		aisErr, backendErr         error
		aisErrCode, backendErrCode int
		delFromAIS, delFromBackend bool
	)
	delFromBackend = lom.Bck().IsRemote() && !evict
	if err := lom.Load(false /*cache it*/, true /*locked*/); err == nil {
		delFromAIS = true
	} else if !cmn.IsObjNotExist(err) {
		return 0, err, false
	} else {
		aisErrCode = http.StatusNotFound
		if !delFromBackend {
			return http.StatusNotFound, err, false
		}
	}

	if delFromBackend {
		backendErrCode, backendErr = t.Backend(lom.Bck()).DeleteObj(lom)
	}
	if delFromAIS {
		size := lom.SizeBytes()
		aisErr = lom.Remove()
		if aisErr != nil {
			if !os.IsNotExist(aisErr) {
				if backendErr != nil {
					// unlikely
					nlog.Errorf("double-failure to delete %s: ais err %v, backend err %v(%d)",
						lom, aisErr, backendErr, backendErrCode)
				}
				return 0, aisErr, false
			}
		} else if evict {
			debug.Assert(lom.Bck().IsRemote())
			t.statsT.AddMany(
				cos.NamedVal64{Name: stats.LruEvictCount, Value: 1},
				cos.NamedVal64{Name: stats.LruEvictSize, Value: size},
			)
		}
	}
	if backendErr != nil {
		return backendErrCode, backendErr, true
	}
	return aisErrCode, aisErr, false
}

// rename obj
func (t *target) objMv(lom *cluster.LOM, msg *apc.ActMsg) error {
	if lom.Bck().IsRemote() {
		return fmt.Errorf("%s: cannot rename object %s from a remote bucket", t.si, lom)
	}
	if lom.Bck().Props.EC.Enabled {
		return fmt.Errorf("%s: cannot rename erasure-coded object %s", t.si, lom)
	}
	if msg.Name == lom.ObjName {
		return fmt.Errorf("%s: cannot rename/move object %s onto itself", t.si, lom)
	}

	buf, slab := t.gmm.Alloc()
	coi := allocCOI()
	{
		coi.CopyObjectParams = cluster.CopyObjectParams{BckTo: lom.Bck(), Buf: buf}
		coi.t = t
		coi.owt = cmn.OwtMigrate
		coi.finalize = true
	}
	_, err := coi.copyObject(lom, msg.Name /* new object name */)
	slab.Free(buf)
	freeCOI(coi)
	if err != nil {
		return err
	}

	// TODO: combine copy+delete under a single write lock
	lom.Lock(true)
	if err := lom.Remove(); err != nil {
		nlog.Warningf("%s: failed to delete renamed object %s (new name %s): %v", t, lom, msg.Name, err)
	}
	lom.Unlock(true)
	return nil
}

func (t *target) fsErr(err error, filepath string) {
	if !cmn.GCO.Get().FSHC.Enabled || !cos.IsIOError(err) {
		return
	}
	mi, _ := fs.Path2Mpath(filepath)
	if mi == nil {
		return
	}
	if cos.IsErrOOS(err) {
		cs := t.OOS(nil)
		nlog.Errorf("%s: fsErr %s", t, cs.String())
		return
	}
	nlog.Errorf("%s: waking up FSHC to check %q for err %v", t, filepath, err)
	keyName := mi.Path
	// keyName is the mountpath is the fspath - counting IO errors on a per basis..
	t.statsT.AddMany(cos.NamedVal64{Name: stats.ErrIOCount, NameSuffix: keyName, Value: 1})
	t.fshc.OnErr(filepath)
}
