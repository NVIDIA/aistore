// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018-2022, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"context"
	"errors"
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

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/ais/backend"
	"github.com/NVIDIA/aistore/ais/s3"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/feat"
	"github.com/NVIDIA/aistore/cmn/fname"
	"github.com/NVIDIA/aistore/cmn/kvdb"
	"github.com/NVIDIA/aistore/cmn/mono"
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
		sync.Mutex
		disabled atomic.Bool // target was unregistered by internal event (e.g, all mountpaths are down)
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
		regstate     regstate // the state of being registered with the primary, can be (en/dis)abled via API
	}
)

// interface guard
var _ cos.Runner = (*target)(nil)

//
// target
//

func (t *target) initBackends() {
	backend.Init()

	config := cmn.GCO.Get()
	aisBackend := backend.NewAIS(t)
	t.backend[apc.AIS] = aisBackend                  // always present
	t.backend[apc.HTTP] = backend.NewHTTP(t, config) // ditto

	if aisConf := config.Backend.Get(apc.AIS); aisConf != nil {
		if err := aisBackend.Apply(aisConf, "init", &config.ClusterConfig); err != nil {
			glog.Errorf("%s: %v - proceeding to start anyway...", t, err)
		} else {
			glog.Infof("%s: remote-ais %v", t, aisConf)
		}
	}

	if err := t._initBuiltin(); err != nil {
		cos.ExitLogf("%v", err)
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
		glog.Errorf("%s backends: enabled %v, disabled %v, missing in the build %v", t, enabled, disabled, notlinked)
	case len(disabled) > 0:
		glog.Warningf("%s backends: enabled %v, disabled %v", t, enabled, disabled)
	default:
		glog.Infof("%s backends: %v", t, enabled)
	}
	return nil
}

func (t *target) aisBackend() *backend.AISBackendProvider {
	bendp := t.backend[apc.AIS]
	return bendp.(*backend.AISBackendProvider)
}

func (t *target) init(config *cmn.Config) {
	t.initNetworks()
	tid, generated := initTID(config)
	if generated && len(config.FSP.Paths) > 0 {
		// in an unlikely case of losing all mountpath-stored IDs but still having a volume
		tid = volume.RecoverTID(tid, config.FSP.Paths)
	}
	t.si.Init(tid, apc.Target)

	cos.InitShortID(t.si.Digest())

	memsys.Init(t.si.ID(), t.si.ID(), config)

	newVol := volume.Init(t, config, daemon.cli.target.allowSharedDisksAndNoDisks,
		daemon.cli.target.useLoopbackDevs, daemon.cli.target.startWithLostMountpath)

	t.initHostIP()
	daemon.rg.add(t)

	ts := stats.NewTrunner(t) // iostat below
	startedUp := ts.Init(t)
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

	if err := ts.InitCapacity(); err != nil { // goes after fs.New
		cos.ExitLogf("%s", err)
	}

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
	glog.Infof("AIS_HOST_IP=%s; PubNetwork=%s", hostIP, t.si.URL(cmn.NetPublic))

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
			glog.Errorf("Warning: %v", err)
		}
		return
	}

	var err error
	if tid, err = fs.LoadNodeID(config.FSP.Paths); err != nil {
		cos.ExitLogf("%v", err)
	}
	if tid != "" {
		return
	}

	tid = genDaemonID(apc.Target, config)
	err = cos.ValidateDaemonID(tid)
	debug.AssertNoErr(err)
	glog.Infof("t[%s] ID randomly generated", tid)
	generated = true
	return
}

func regDiskMetrics(tstats *stats.Trunner, mpi fs.MPI) {
	for _, mi := range mpi {
		for _, disk := range mi.Disks {
			tstats.RegDiskMetrics(disk)
		}
	}
}

func (t *target) Run() error {
	if err := t.si.Validate(); err != nil {
		cos.ExitLogf("%v", err)
	}
	config := cmn.GCO.Get()
	t.htrun.init(config)

	cluster.Init(t)
	cluster.RegLomCacheWithHK(t)

	// metrics, disks first
	tstats := t.statsT.(*stats.Trunner)
	availablePaths, disabledPaths := fs.Get()
	if len(availablePaths) == 0 {
		cos.ExitLogf("%v", cmn.ErrNoMountpaths)
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
	if err := fs.CSM.Reg(fs.ObjectType, &fs.ObjectContentResolver{}); err != nil {
		cos.ExitLogf("%v", err)
	}
	if err := fs.CSM.Reg(fs.WorkfileType, &fs.WorkfileContentResolver{}); err != nil {
		cos.ExitLogf("%v", err)
	}

	// Init meta-owners and load local instances
	t.owner.bmd.init()
	t.owner.etl.init()

	smap, reliable := t.tryLoadSmap()
	if !reliable {
		smap = newSmap()
	}
	// Add self to the cluster map
	smap.Tmap[t.si.ID()] = t.si
	t.owner.smap.put(smap)

	if daemon.cli.target.standby {
		tstats.Standby(true)
		t.regstate.disabled.Store(true)
		glog.Warningf("%s not joining - standing by...", t.si)
		go func() {
			for !t.ClusterStarted() {
				time.Sleep(2 * config.Periodic.StatsTime.D())
				glog.Flush()
			}
		}()
		// see endStartupStandby()
	} else {
		// discover primary and join cluster (compare with manual `apc.AdminJoin`)
		if status, err := t.joinCluster(apc.ActSelfJoinTarget); err != nil {
			glog.Errorf("%s failed to join cluster (status: %d, err: %v)", t, status, err)
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
				if status, err := t.joinCluster(apc.ActSelfJoinTarget,
					cii.Smap.Primary.CtrlURL, cii.Smap.Primary.PubURL); err != nil {
					glog.Errorf("%s failed to re-join cluster (status: %d, err: %v)", t, status, err)
					return
				}
			}
			t.markClusterStarted()

			if t.fsprg.newVol && !config.TestingEnv() {
				config := cmn.GCO.BeginUpdate()
				fspathsSave(config)
			}
		}()
	}

	t.initBackends()

	db, err := kvdb.NewBuntDB(filepath.Join(config.ConfigDir, dbName))
	if err != nil {
		glog.Errorf("Failed to initialize DB: %v", err)
		return err
	}
	defer cos.Close(db)

	dload.SetDB(db)

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
		go func() {
			if marked.Interrupted {
				glog.Info("Resuming resilver...")
			} else if daemon.resilver.required {
				glog.Infof("Starting resilver, reason: %q", daemon.resilver.reason)
			}
			t.runResilver(res.Args{}, nil /*wg*/)
		}()
	}

	dsort.InitManagers(db)
	dsort.RegisterNode(t.owner.smap, t.owner.bmd, t.si, t, t.statsT)

	defer etl.StopAll(t) // Always try to stop running ETLs.

	err = t.htrun.run()

	// do it after the `run()` to retain `restarted` marker on panic
	fs.RemoveMarker(fname.NodeRestartedMarker)
	return err
}

func (t *target) runResilver(args res.Args, wg *sync.WaitGroup) {
	// with no cluster-wide UUID it's a local run
	if args.UUID == "" {
		args.UUID = cos.GenUUID()
		regMsg := xactRegMsg{UUID: args.UUID, Kind: apc.ActResilver, Srcs: []string{t.si.ID()}}
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
	glog.Infof("%s enabled and joined (%s)", t, smap.StringEx())

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
	t.registerNetworkHandlers(networkHandlers)
}

// stop gracefully
// TODO: write shutdown-marker
func (t *target) Stop(err error) {
	// NOTE: vs metasync
	t.regstate.Lock()
	daemon.stopping.Store(true)
	t.regstate.Unlock()

	f := glog.Infof
	if err != nil {
		f = glog.Warningf
	}
	f("Stopping %s, err: %v", t.si, err)
	xreg.AbortAll(err)
	t.htrun.stop(t.netServ.pub.s != nil && !isErrNoUnregister(err) /*rm from Smap*/)
}

func (t *target) checkRestarted() (fatalErr, writeErr error) {
	if fs.MarkerExists(fname.NodeRestartedMarker) {
		t.statsT.Add(stats.RestartCount, 1)
	} else {
		fatalErr, writeErr = fs.PersistMarker(fname.NodeRestartedMarker)
	}
	return
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
func (t *target) objectHandler(w http.ResponseWriter, r *http.Request) {
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
func (t *target) ecHandler(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		t.httpecget(w, r)
	default:
		cmn.WriteErr405(w, r, http.MethodGet)
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
func (t *target) httpobjget(w http.ResponseWriter, r *http.Request) {
	apireq := apiReqAlloc(2, apc.URLPathObjects.L, true /*dpq*/)
	if err := t.parseReq(w, r, apireq); err != nil {
		apiReqFree(apireq)
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
	apiReqFree(apireq)
}

// getObject is main function to get the object. It doesn't check request origin,
// so it must be done by the caller (if necessary).
func (t *target) getObject(w http.ResponseWriter, r *http.Request, dpq *dpq, bck *cluster.Bck, lom *cluster.LOM) *cluster.LOM {
	if err := lom.InitBck(bck.Bucket()); err != nil {
		if cmn.IsErrRemoteBckNotFound(err) {
			t.BMDVersionFixup(r)
			err = lom.InitBck(bck.Bucket())
		}
		if err != nil {
			t.writeErr(w, r, err)
			return lom
		}
	}
	// isETLRequest (TODO: !4455 comment)
	if dpq.uuid != "" {
		t.doETL(w, r, dpq.uuid, bck, lom.ObjName)
		return lom
	}
	filename := dpq.archpath // apc.QparamArchpath
	if strings.HasPrefix(filename, lom.ObjName) {
		if rel, err := filepath.Rel(lom.ObjName, filename); err == nil {
			filename = rel
		}
	}
	nanotim := mono.NanoTime()
	atime := time.Now().UnixNano()
	if dpq.ptime != "" && nanotim&0x5 == 5 {
		if redelta := ptLatency(atime, dpq.ptime); redelta != 0 {
			t.statsT.Add(stats.GetRedirLatency, redelta)
		}
	}
	goi := allocGetObjInfo()
	{
		goi.atime = atime
		goi.nanotim = nanotim
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
		goi.chunked = cmn.GCO.Get().Net.HTTP.Chunked
	}
	if bck.IsHTTP() {
		originalURL := dpq.origURL // query.Get(apc.QparamOrigURL)
		goi.ctx = context.WithValue(goi.ctx, cos.CtxOriginalURL, originalURL)
	}
	if errCode, err := goi.getObject(); err != nil && err != errSendingResp {
		t.writeErr(w, r, err, errCode)
	}
	lom = goi.lom
	freeGetObjInfo(goi)
	return lom
}

// PUT /v1/objects/bucket-name/object-name
func (t *target) httpobjput(w http.ResponseWriter, r *http.Request) {
	apireq := apiReqAlloc(2, apc.URLPathObjects.L, true /*dpq*/)
	defer apiReqFree(apireq)
	if err := t.parseReq(w, r, apireq); err != nil {
		return
	}

	// prep and check
	var (
		config  = cmn.GCO.Get()
		objName = apireq.items[1]
		started = time.Now()
		t2tput  = isT2TPut(r.Header)
	)
	if apireq.dpq.ptime == "" {
		if !t2tput {
			t.writeErrf(w, r, "%s: %s(obj) is expected to be redirected or replicated", t.si, r.Method)
			return
		}
	} else if redelta := ptLatency(started.UnixNano(), apireq.dpq.ptime); redelta != 0 {
		t.statsT.Add(stats.PutRedirLatency, redelta)
	}
	if cs := fs.GetCapStatus(); cs.Err != nil || cs.PctMax > int32(config.Space.CleanupWM) {
		cs = t.OOS(nil)
		if cs.OOS {
			// fail this write
			t.writeErr(w, r, cs.Err, http.StatusInsufficientStorage)
			return
		}
	}

	// init
	lom := cluster.AllocLOM(objName)
	defer cluster.FreeLOM(lom)
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
		errCode, err = t.doAppendArch(r, lom, started, apireq.dpq)
	case apireq.dpq.appendTy != "": // apc.QparamAppendType
		handle, errCode, err = t.doAppend(r, lom, started, apireq.dpq)
		if err == nil {
			w.Header().Set(apc.HdrAppendHandle, handle)
			return
		}
	default:
		poi := allocPutObjInfo()
		{
			poi.atime = started
			poi.t = t
			poi.lom = lom
			poi.skipVC = skipVC
			poi.restful = true
			poi.t2t = t2tput
		}
		errCode, err = poi.do(r, apireq.dpq)
		freePutObjInfo(poi)
	}
	if err != nil {
		t.fsErr(err, lom.FQN)
		t.writeErr(w, r, err, errCode)
	}
}

// DELETE [ { action } ] /v1/objects/bucket-name/object-name
func (t *target) httpobjdelete(w http.ResponseWriter, r *http.Request) {
	var msg aisMsg
	apireq := apiReqAlloc(2, apc.URLPathObjects.L, false)
	defer apiReqFree(apireq)
	if err := readJSON(w, r, &msg); err != nil {
		return
	}
	if err := t.parseReq(w, r, apireq); err != nil {
		return
	}
	if isRedirect(apireq.query) == "" {
		t.writeErrf(w, r, "%s: %s(obj) is expected to be redirected", t.si, r.Method)
		return
	}

	evict := msg.Action == apc.ActEvictObjects
	lom := cluster.AllocLOM(apireq.items[1])
	defer cluster.FreeLOM(lom)
	if err := lom.InitBck(apireq.bck.Bucket()); err != nil {
		t.writeErr(w, r, err)
		return
	}

	errCode, err := t.DeleteObject(lom, evict)
	if errCode == http.StatusServiceUnavailable {
		// (googleapi: "Error 503: We encountered an internal error. Please try again.")
		time.Sleep(time.Second)
		errCode, err = t.DeleteObject(lom, evict)
	}
	if err != nil {
		if errCode == http.StatusNotFound {
			t.writeErrSilentf(w, r, http.StatusNotFound, "object %s/%s doesn't exist", lom.Bucket(), lom.ObjName)
		} else {
			t.writeErr(w, r, err, errCode)
		}
		return
	}
	// EC cleanup if EC is enabled
	ec.ECM.CleanupObject(lom)
}

// POST /v1/objects/bucket-name/object-name
func (t *target) httpobjpost(w http.ResponseWriter, r *http.Request) {
	msg, err := t.readActionMsg(w, r)
	if err != nil {
		return
	}
	switch msg.Action {
	case apc.ActRenameObject:
		query := r.URL.Query()
		if isRedirect(query) == "" {
			t.writeErrf(w, r, "%s: %s-%s(obj) is expected to be redirected", t.si, r.Method, msg.Action)
			return
		}
		t.objMv(w, r, msg)
	default:
		t.writeErrAct(w, r, msg.Action)
	}
}

// HEAD /v1/objects/<bucket-name>/<object-name>
func (t *target) httpobjhead(w http.ResponseWriter, r *http.Request) {
	apireq := apiReqAlloc(2, apc.URLPathObjects.L, false)
	err := t.parseReq(w, r, apireq)
	query, bck, objName := apireq.query, apireq.bck, apireq.items[1]
	apiReqFree(apireq)
	if err != nil {
		return
	}
	if cmn.Features.IsSet(feat.EnforceIntraClusterAccess) {
		// validates that the request is internal (by a node in the same cluster)
		if isRedirect(query) == "" && t.isIntraCall(r.Header, false) != nil {
			t.writeErrf(w, r, "%s: %s(obj) is expected to be redirected (remaddr=%s)",
				t.si, r.Method, r.RemoteAddr)
			return
		}
	}
	lom := cluster.AllocLOM(objName)
	t.headObject(w, r, query, bck, lom)
	cluster.FreeLOM(lom)
}

// headObject is main function to head the object. It doesn't check request origin,
// so it must be done by the caller (if necessary).
func (t *target) headObject(w http.ResponseWriter, r *http.Request, query url.Values, bck *cluster.Bck, lom *cluster.LOM) {
	var (
		invalidHandler = t.writeErr
		hdr            = w.Header()
		fltPresence    int
		silent         = cos.IsParseBool(query.Get(apc.QparamSilent))
		exists         = true
		hasEC          bool
	)
	if silent {
		invalidHandler = t.writeErrSilent
	}
	if tmp := query.Get(apc.QparamFltPresence); tmp != "" {
		var erp error
		fltPresence, erp = strconv.Atoi(tmp)
		debug.AssertNoErr(erp)
	}
	if err := lom.InitBck(bck.Bucket()); err != nil {
		invalidHandler(w, r, err)
		return
	}
	err := lom.Load(true /*cache it*/, false /*locked*/)
	if err == nil {
		if apc.IsFltNoProps(fltPresence) {
			return
		}
		if fltPresence == apc.FltExistsOutside {
			err := fmt.Errorf("%s is present (flt %d=\"outside\")", lom.FullName(), fltPresence)
			invalidHandler(w, r, err)
			return
		}
	} else {
		if !cmn.IsObjNotExist(err) {
			invalidHandler(w, r, err)
			return
		}
		exists = false
		if fltPresence == apc.FltPresentAnywhere {
			exists = lom.RestoreToLocation()
		}
	}

	if !exists {
		if bck.IsAIS() || apc.IsFltPresent(fltPresence) {
			err := cmn.NewErrNotFound("%s: object %s", t, lom.FullName())
			invalidHandler(w, r, err, http.StatusNotFound)
			return
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
		objAttrs, errCode, err := t.Backend(lom.Bck()).HeadObj(context.Background(), lom)
		if err != nil {
			invalidHandler(w, r, cmn.NewErrFailedTo(t, "HEAD", lom, err), errCode)
			return
		}
		if apc.IsFltNoProps(fltPresence) {
			return
		}
		op.ObjAttrs = *objAttrs
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
}

// PATCH /v1/objects/<bucket-name>/<object-name>
// By default, adds or updates existing custom keys. Will remove all existing keys and
// replace them with the specified ones _iff_ `apc.QparamNewCustom` is set.
func (t *target) httpobjpatch(w http.ResponseWriter, r *http.Request) {
	apireq := apiReqAlloc(2, apc.URLPathObjects.L, false)
	defer apiReqFree(apireq)
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

//////////////////////
// httpec* handlers //
//////////////////////

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
func (t *target) sendECMetafile(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) {
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

func (t *target) sendECCT(w http.ResponseWriter, r *http.Request, bck *cluster.Bck, objName string) {
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
	sliceFQN := lom.MpathInfo().MakePathFQN(bck.Bucket(), fs.ECSliceType, objName)
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

	w.Header().Set(cos.HdrContentLength, strconv.FormatInt(finfo.Size(), 10))
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
func (t *target) CompareObjects(ctx context.Context, lom *cluster.LOM) (equal bool, errCode int, err error) {
	var objAttrs *cmn.ObjAttrs
	objAttrs, errCode, err = t.Backend(lom.Bck()).HeadObj(ctx, lom)
	if err != nil {
		err = cmn.NewErrFailedTo(t, "head metadata of", lom, err)
		return
	}
	if lom.Bck().IsHDFS() {
		equal = true // no versioning in HDFS
		return
	}
	equal = lom.Equal(objAttrs)
	return
}

func (t *target) doAppend(r *http.Request, lom *cluster.LOM, started time.Time, dpq *dpq) (newHandle string,
	errCode int, err error) {
	var (
		cksumValue    = r.Header.Get(apc.HdrObjCksumVal)
		cksumType     = r.Header.Get(apc.HdrObjCksumType)
		contentLength = r.Header.Get(cos.HdrContentLength)
		handle        = dpq.appendHdl // apc.QparamAppendHandle
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
		op:      dpq.appendTy, // apc.QparamAppendType
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

func (t *target) doAppendArch(r *http.Request, lom *cluster.LOM, started time.Time, dpq *dpq) (errCode int, err error) {
	var (
		sizeStr  = r.Header.Get(cos.HdrContentLength)
		mime     = dpq.archmime // apc.QparamArchmime
		filename = dpq.archpath // apc.QparamArchpath
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

func (t *target) putMirror(lom *cluster.LOM) {
	mconfig := lom.MirrorConf()
	if !mconfig.Enabled {
		return
	}
	if mpathCnt := fs.NumAvail(); mpathCnt < int(mconfig.Copies) {
		t.statsT.Add(stats.ErrPutCount, 1) // TODO: differentiate put err metrics
		nanotim := mono.NanoTime()
		if nanotim&0x7 == 7 {
			if mpathCnt == 0 {
				glog.Errorf("%s: %v", t, cmn.ErrNoMountpaths)
			} else {
				glog.Errorf(fmtErrInsuffMpaths2, t, mpathCnt, lom, mconfig.Copies)
			}
		}
		return
	}
	rns := xreg.RenewPutMirror(t, lom)
	if rns.Err != nil {
		glog.Errorf("%s: %s %v", t, lom, rns.Err)
		debug.AssertNoErr(rns.Err)
		return
	}
	xctn := rns.Entry.Get()
	xputlrep := xctn.(*mirror.XactPut)
	xputlrep.Repl(lom)
}

func (t *target) DeleteObject(lom *cluster.LOM, evict bool) (int, error) {
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
			debug.Assert(lom.Bck().IsRemote())
			t.statsT.AddMany(
				cos.NamedVal64{Name: stats.LruEvictCount, Value: 1},
				cos.NamedVal64{Name: stats.LruEvictSize, Value: size},
			)
		}
	}
	if backendErr != nil {
		return backendErrCode, backendErr
	}
	return aisErrCode, aisErr
}

// rename obj (TODO: consider unifying with Promote)
func (t *target) objMv(w http.ResponseWriter, r *http.Request, msg *apc.ActionMsg) {
	apireq := apiReqAlloc(2, apc.URLPathObjects.L, false)
	defer apiReqFree(apireq)
	if err := t.parseReq(w, r, apireq); err != nil {
		return
	}
	lom := cluster.AllocLOM(apireq.items[1])
	defer cluster.FreeLOM(lom)
	if err := lom.InitBck(apireq.bck.Bucket()); err != nil {
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
		coi.owt = cmn.OwtMigrate
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
		glog.Warningf("%s: failed to delete renamed object %s (new name %s): %v", t, lom, msg.Name, err)
	}
	lom.Unlock(true)
}

func (t *target) fsErr(err error, filepath string) {
	if !cmn.GCO.Get().FSHC.Enabled || !cos.IsIOError(err) {
		return
	}
	mpathInfo, _ := fs.Path2Mpath(filepath)
	if mpathInfo == nil {
		return
	}
	if cos.IsErrOOS(err) {
		cs := t.OOS(nil)
		glog.Errorf("%s: %s", t, cs.String())
		return
	}
	glog.Errorf("%s: waking up FSHC to check %q for err %v", t, filepath, err)
	keyName := mpathInfo.Path
	// keyName is the mountpath is the fspath - counting IO errors on a per basis..
	t.statsT.AddMany(cos.NamedVal64{Name: stats.ErrIOCount, NameSuffix: keyName, Value: 1})
	t.fshc.OnErr(filepath)
}
