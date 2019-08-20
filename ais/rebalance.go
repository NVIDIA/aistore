// Package ais provides core functionality for the AIStore object storage.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/filter"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	jsoniter "github.com/json-iterator/go"
)

const (
	rebalanceStreamName = "rebalance"
	rebalanceAcksName   = "remwack" // NOTE: can become generic remote-write-acknowledgment
)

// rebalance stage enum
const (
	rebStageInactive = iota
	rebStageInit
	rebStageTraverse
	rebStageWaitAck
	rebStageFin
	rebStageFinStreams
	rebStageDone
)

type (
	rebSyncCallback func(tsi *cluster.Snode, config *cmn.Config, ver int64, xreb *xactGlobalReb) (ok bool)

	rebStatus struct {
		Tmap        cluster.NodeMap         `json:"tmap"`         // targets I'm waiting for ACKs from
		SmapVersion int64                   `json:"smap_version"` // current Smap version (via smapowner)
		RebVersion  int64                   `json:"reb_version"`  // Smap version of *this* rebalancing operation (m.b. invariant)
		StatsDelta  stats.ExtRebalanceStats `json:"stats_delta"`  // objects and sizes transmitted/received by this reb oper
		Stage       uint32                  `json:"stage"`        // the current stage - see enum above
		Aborted     bool                    `json:"aborted"`      // aborted?
		Running     bool                    `json:"running"`      // running?
	}
	rebManager struct {
		t          *targetrunner
		filterGFN  *filter.Filter
		netd, netc string
		smap       atomic.Pointer // new smap which will be soon live
		streams    *transport.StreamBundle
		acks       *transport.StreamBundle
		lomacks    [fs.LomCacheMask + 1]*LomAcks
		tcache     struct { // not to recompute very often
			tmap cluster.NodeMap
			ts   time.Time
			mu   *sync.Mutex
		}
		beginStats atomic.Pointer // *stats.ExtRebalanceStats
		laterx     atomic.Bool
		stage      atomic.Uint32 // rebStage* enum
	}
	rebJoggerBase struct {
		m     *rebManager
		xreb  *xactRebBase
		mpath string
		wg    *sync.WaitGroup
	}
	globalRebJogger struct {
		rebJoggerBase
		smap  *smapX
		sema  chan struct{}
		errCh chan error
		ver   int64
	}
	localRebJogger struct {
		rebJoggerBase
		slab              *memsys.Slab2
		buf               []byte
		skipGlobMisplaced bool
	}
	LomAcks struct {
		mu *sync.Mutex
		q  map[string]*cluster.LOM // on the wire, waiting for ACK
	}
)

var rebStage = map[uint32]string{
	rebStageInactive:   "<inactive>",
	rebStageInit:       "<init>",
	rebStageTraverse:   "<traverse>",
	rebStageWaitAck:    "<wack>",
	rebStageFin:        "<fin>",
	rebStageFinStreams: "<fin-streams>",
	rebStageDone:       "<done>",
}

//
// rebManager
//
func (reb *rebManager) lomAcks() *[fs.LomCacheMask + 1]*LomAcks { return &reb.lomacks }

// via GET /v1/health (cmn.Health)
func (reb *rebManager) getGlobStatus(status *rebStatus) {
	var (
		now        time.Time
		tmap       cluster.NodeMap
		config     = cmn.GCO.Get()
		sleepRetry = keepaliveRetryDuration(config)
		rsmap      = (*smapX)(reb.smap.Load())
		tsmap      = reb.t.smapowner.get()
	)
	status.Aborted, status.Running = reb.t.xactions.isRebalancing(cmn.ActGlobalReb)
	status.Stage = reb.stage.Load()
	status.SmapVersion = tsmap.version()
	if rsmap != nil {
		status.RebVersion = rsmap.version()
	}
	// stats
	beginStats := (*stats.ExtRebalanceStats)(reb.beginStats.Load())
	if beginStats == nil {
		return
	}
	curntStats := reb.getStats()
	status.StatsDelta.TxRebCount = curntStats.TxRebCount - beginStats.TxRebCount
	status.StatsDelta.RxRebCount = curntStats.RxRebCount - beginStats.RxRebCount
	status.StatsDelta.TxRebSize = curntStats.TxRebSize - beginStats.TxRebSize
	status.StatsDelta.RxRebSize = curntStats.RxRebSize - beginStats.RxRebSize

	// wack info
	if status.Stage != rebStageWaitAck {
		return
	}
	if status.SmapVersion != status.RebVersion {
		glog.Warningf("%s: Smap version %d != %d", reb.t.si.Name(), status.SmapVersion, status.RebVersion)
		return
	}

	reb.tcache.mu.Lock()
	status.Tmap, tmap = reb.tcache.tmap, reb.tcache.tmap
	now = time.Now()
	if now.Sub(reb.tcache.ts) < sleepRetry {
		reb.tcache.mu.Unlock()
		return
	}
	reb.tcache.ts = now
	for tid := range reb.tcache.tmap {
		delete(reb.tcache.tmap, tid)
	}
	max := rsmap.CountTargets() - 1
	for _, lomack := range reb.lomAcks() {
		lomack.mu.Lock()
		for _, lom := range lomack.q {
			tsi, err := cluster.HrwTarget(lom.Bucket, lom.Objname, &rsmap.Smap)
			if err != nil {
				continue
			}
			tmap[tsi.DaemonID] = tsi
			if len(tmap) >= max {
				lomack.mu.Unlock()
				goto ret
			}
		}
		lomack.mu.Unlock()
	}
ret:
	reb.tcache.mu.Unlock()
	status.Stage = reb.stage.Load()
}

// main method: 10 stages
func (reb *rebManager) runGlobalReb(smap *smapX) {
	var (
		tname  = reb.t.si.Name()
		ver    = smap.version()
		config = cmn.GCO.Get()
		sleep  = config.Timeout.CplaneOperation // NOTE: TODO: used throughout; must be separately assigned and calibrated
		maxwt  = config.Rebalance.DestRetryTime
		wg     = &sync.WaitGroup{}
		curwt  time.Duration
		aPaths fs.MPI
		xreb   *xactGlobalReb
		cnt    int
	)
	// 1. check whether other targets are up and running
	if errCnt := reb.bcast(smap, config, reb.pingTarget, nil /*xreb*/); errCnt > 0 {
		return
	}
	if smap.version() == 0 {
		smap = reb.t.smapowner.get()
	}
	// 2. serialize (rebalancing operations - one at a time post this point)
	//    start new xaction unless the one for the current version is already in progress
	if newerSmap, alreadyRunning := reb.serialize(smap, config); newerSmap || alreadyRunning {
		return
	}
	if smap.version() == 0 {
		smap = reb.t.smapowner.get()
	}

	/* ================== rebStageInit ================== */
	availablePaths, _ := fs.Mountpaths.Get()
	xreb = reb.t.xactions.renewGlobalReb(ver, len(availablePaths)*2)
	cmn.Assert(xreb != nil) // must renew given the CAS and checks above

	// 3. init streams and data structures
	reb.beginStats.Store(unsafe.Pointer(reb.getStats()))
	reb.beginStreams(config)
	reb.filterGFN.Reset() // start with empty filters
	reb.tcache.tmap = make(cluster.NodeMap, smap.CountTargets()-1)
	reb.tcache.mu = &sync.Mutex{}
	acks := reb.lomAcks()
	for i := 0; i < len(acks); i++ { // init lom acks
		acks[i] = &LomAcks{mu: &sync.Mutex{}, q: make(map[string]*cluster.LOM, 64)}
	}

	// 4. create persistent mark
	pmarker := persistentMarker(cmn.ActGlobalReb)
	file, err := cmn.CreateFile(pmarker)
	if err != nil {
		glog.Errorln("Failed to create", pmarker, err)
		pmarker = ""
	} else {
		_ = file.Close()
	}

	// 5. ready - can receive objects
	reb.smap.Store(unsafe.Pointer(smap))
	glog.Infoln(xreb.String())

	// 6. capture stats, start mpath joggers TODO: currently supporting only fs.ObjectType (content-type)
	reb.stage.Store(rebStageTraverse)

	// 6.5. synchronize
	glog.Infof("%s: poll targets for: stage=%s", tname, rebStage[rebStageTraverse])
	_ = reb.bcast(smap, config, reb.rxReady, xreb) // NOTE: ignore timeout
	if xreb.Aborted() {
		glog.Infoln("abrt")
		goto term
	}
	for _, mpathInfo := range availablePaths {
		var (
			sema   chan struct{}
			mpathL string
		)
		mpathL = mpathInfo.MakePath(fs.ObjectType, true /*local*/)
		if config.Rebalance.Multiplier > 1 {
			sema = make(chan struct{}, config.Rebalance.Multiplier)
		}
		rl := &globalRebJogger{rebJoggerBase: rebJoggerBase{m: reb, mpath: mpathL, xreb: &xreb.xactRebBase, wg: wg},
			smap: smap, sema: sema, ver: ver}
		wg.Add(1)
		go rl.jog()
	}
	for _, mpathInfo := range availablePaths {
		var sema chan struct{}
		mpathC := mpathInfo.MakePath(fs.ObjectType, false /*cloud*/)
		if config.Rebalance.Multiplier > 1 {
			sema = make(chan struct{}, config.Rebalance.Multiplier)
		}
		rc := &globalRebJogger{rebJoggerBase: rebJoggerBase{m: reb, mpath: mpathC, xreb: &xreb.xactRebBase, wg: wg},
			smap: smap, sema: sema, ver: ver}
		wg.Add(1)
		go rc.jog()
	}
	wg.Wait()
	if xreb.Aborted() {
		glog.Infoln("abrt")
		goto term
	}

	// 7. wait for ACKs
wack:
	reb.stage.Store(rebStageWaitAck)
	curwt = 0
	maxwt += time.Duration(int64(time.Minute) * int64(smap.CountTargets()/10))
	maxwt = cmn.MinDur(maxwt, config.Rebalance.DestRetryTime*2)
	// poll for no more than maxwt while keeping track of the cumulative polling time via curwt
	// (here and elsewhere)
	for curwt < maxwt {
		cnt = 0
		var logged bool
		for _, lomack := range reb.lomAcks() {
			lomack.mu.Lock()
			if l := len(lomack.q); l > 0 {
				cnt += l
				if !logged {
					for _, lom := range lomack.q {
						tsi, err := cluster.HrwTarget(lom.Bucket, lom.Objname, &smap.Smap)
						if err == nil {
							glog.Infof("waiting for %s ACK from %s", lom, tsi)
							logged = true
							break
						}
					}
				}
			}
			lomack.mu.Unlock()
			if xreb.Aborted() {
				glog.Infoln("abrt")
				goto term
			}
		}
		if cnt == 0 {
			glog.Infof("%s: received all ACKs", tname)
			break
		}
		glog.Warningf("%s: waiting for %d ACKs", tname, cnt)
		time.Sleep(sleep)
		if xreb.Aborted() {
			glog.Infoln("abrt")
			goto term
		}
		curwt += sleep
	}
	if cnt > 0 {
		glog.Warningf("%s: timed-out waiting for %d ACK(s)", tname, cnt)
	}

	// NOTE: requires locally migrated objects *not* to be removed at the src
	aPaths, _ = fs.Mountpaths.Get()
	if len(aPaths) > len(availablePaths) {
		glog.Warningf("%s: mountpath changes detected (%d, %d)", tname, len(aPaths), len(availablePaths))
	}

	// 8. synchronize
	glog.Infof("%s: poll targets for: stage=(%s or %s***)", tname, rebStage[rebStageFin], rebStage[rebStageWaitAck])
	_ = reb.bcast(smap, config, reb.waitFinExtended, xreb)

	// 9. retransmit if needed
	cnt = reb.retransmit(xreb)
	if cnt > 0 {
		goto wack
	}

term:
	// 10. close streams, end xaction, deactivate GFN (FIXME: hardcoded sleep)
	reb.stage.Store(rebStageFin)
	quiescent, maxquiet := 0, 10 // e.g., 10 * 2s (Cplane) = 20 seconds of /quiet/ time - see laterx
	aborted := xreb.Aborted()
	for quiescent < maxquiet && !aborted {
		if !reb.laterx.CAS(true, false) {
			quiescent++
		} else {
			quiescent = 0
		}
		time.Sleep(sleep)
		aborted = xreb.Aborted()
	}
	if !aborted {
		if err := os.Remove(pmarker); err != nil && !os.IsNotExist(err) {
			glog.Errorf("%s: failed to remove in-progress mark %s, err: %v", tname, pmarker, err)
		}
	}
	reb.endStreams()
	reb.t.gfn.global.deactivate()
	if !xreb.Finished() {
		xreb.EndTime(time.Now())
	} else {
		glog.Infoln(xreb.String())
	}
	{
		status := &rebStatus{}
		reb.getGlobStatus(status)
		delta, err := jsoniter.MarshalIndent(&status.StatsDelta, "", " ")
		if err == nil {
			glog.Infoln(string(delta))
		}
	}
	reb.stage.Store(rebStageDone)
}

func (reb *rebManager) serialize(smap *smapX, config *cmn.Config) (newerSmap, alreadyRunning bool) {
	var (
		tname = reb.t.si.Name()
		ver   = smap.version()
		sleep = config.Timeout.CplaneOperation
	)
	for {
		if reb.stage.CAS(rebStageInactive, rebStageInit) {
			break
		}
		if reb.stage.CAS(rebStageDone, rebStageInit) {
			break
		}
		//
		// vs newer Smap
		//
		nver := reb.t.smapowner.get().version()
		if nver > ver {
			glog.Warningf("%s %s: Smap v(%d, %d) - see newer Smap, not running",
				tname, rebStage[reb.stage.Load()], ver, nver)
			newerSmap = true
			return
		}
		//
		// vs current xaction
		//
		entry := reb.t.xactions.GetL(cmn.ActGlobalReb)
		if entry != nil {
			xact := entry.Get()
			if !xact.Finished() {
				runningXreb := xact.(*xactGlobalReb)
				if runningXreb.smapVersion == ver {
					glog.Warningf("%s %s: Smap v%d - is already running", tname, rebStage[reb.stage.Load()], ver)
					alreadyRunning = true
					return
				}
				if runningXreb.smapVersion < ver {
					runningXreb.Abort()
					glog.Warningf("%s %s: Smap v(%d > %d) - aborting prev and waiting for it to cleanup/exit",
						tname, rebStage[reb.stage.Load()], ver, runningXreb.smapVersion)
				}
			} else {
				glog.Warningf("%s %s: Smap v%d - waiting for %s", tname, rebStage[reb.stage.Load()], ver,
					rebStage[rebStageDone])
			}
		} else {
			glog.Warningf("%s %s: Smap v%d - waiting...", tname, rebStage[reb.stage.Load()], ver)
		}
		time.Sleep(sleep)
	}
	return
}

func (reb *rebManager) getStats() (s *stats.ExtRebalanceStats) {
	s = &stats.ExtRebalanceStats{}
	statsRunner := getstorstatsrunner()
	s.TxRebCount = statsRunner.Get(stats.TxRebCount)
	s.RxRebCount = statsRunner.Get(stats.RxRebCount)
	s.TxRebSize = statsRunner.Get(stats.TxRebSize)
	s.RxRebSize = statsRunner.Get(stats.RxRebSize)
	return
}

func (reb *rebManager) beginStreams(config *cmn.Config) {
	cmn.Assert(reb.stage.Load() == rebStageInit)
	if config.Rebalance.Multiplier == 0 {
		config.Rebalance.Multiplier = 1
	} else if config.Rebalance.Multiplier > 8 {
		glog.Errorf("%s: stream-and-mp-jogger multiplier=%d - misconfigured?",
			reb.t.si.Name(), config.Rebalance.Multiplier)
	}
	//
	// objects
	//
	client := transport.NewDefaultClient()
	sbArgs := transport.SBArgs{
		Network: reb.netd,
		Trname:  rebalanceStreamName,
		Extra: &transport.Extra{
			Compression: config.Rebalance.Compression,
			Config:      config,
			Mem2:        nodeCtx.mm},
		Multiplier:   int(config.Rebalance.Multiplier),
		ManualResync: true,
	}
	reb.streams = transport.NewStreamBundle(reb.t.smapowner, reb.t.si, client, sbArgs)

	//
	// ACKs (notice client with default transport args)
	//
	clientAcks := cmn.NewClient(cmn.TransportArgs{})
	sbArgs = transport.SBArgs{
		ManualResync: true,
		Network:      reb.netc,
		Trname:       rebalanceAcksName,
	}
	reb.acks = transport.NewStreamBundle(reb.t.smapowner, reb.t.si, clientAcks, sbArgs)
	reb.laterx.Store(false)
}

func (reb *rebManager) endStreams() {
	if reb.stage.CAS(rebStageFin, rebStageFinStreams) { // TODO: must always succeed?
		reb.streams.Close(true /* graceful */)
		reb.streams = nil
		reb.acks.Close(true)
	}
}

func (reb *rebManager) recvObj(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
	if err != nil {
		glog.Error(err)
		return
	}
	smap := (*smapX)(reb.smap.Load())
	if smap == nil {
		var (
			config = cmn.GCO.Get()
			sleep  = config.Timeout.CplaneOperation
			maxwt  = config.Rebalance.DestRetryTime
			curwt  time.Duration
		)
		maxwt = cmn.MinDur(maxwt, config.Timeout.SendFile/3)
		glog.Warningf("%s: waiting to start...", reb.t.si.Name())
		time.Sleep(sleep)
		for curwt < maxwt {
			smap = (*smapX)(reb.smap.Load())
			if smap != nil {
				break
			}
			time.Sleep(sleep)
			curwt += sleep
		}
		if curwt >= maxwt {
			glog.Errorf("%s: timed-out waiting to start, dropping %s/%s", reb.t.si.Name(), hdr.Bucket, hdr.Objname)
			return
		}
	}
	var (
		tsid = string(hdr.Opaque) // the sender
		tsi  = smap.GetTarget(tsid)
	)
	// Rx
	lom, err := cluster.LOM{T: reb.t, Bucket: hdr.Bucket, Objname: hdr.Objname}.Init(cmn.ProviderFromLoc(hdr.IsLocal))
	if err != nil {
		glog.Error(err)
		return
	}
	if stage := reb.stage.Load(); stage >= rebStageFin {
		reb.laterx.Store(true)
		f := glog.Warningf
		if stage > rebStageFin {
			f = glog.Errorf
		}
		f("%s: late receive from %s %s (stage %s)", reb.t.si.Name(), tsid, lom, rebStage[stage])
	} else if stage < rebStageTraverse {
		glog.Errorf("%s: early receive from %s %s (stage %s)", reb.t.si.Name(), tsid, lom, rebStage[stage])
	}
	lom.SetAtimeUnix(hdr.ObjAttrs.Atime)
	lom.SetVersion(hdr.ObjAttrs.Version)
	poi := &putObjInfo{
		started:      time.Now(),
		t:            reb.t,
		lom:          lom,
		workFQN:      fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfilePut),
		r:            ioutil.NopCloser(objReader),
		cksumToCheck: cmn.NewCksum(hdr.ObjAttrs.CksumType, hdr.ObjAttrs.CksumValue),
		migrated:     true,
	}
	if err, _ := poi.putObject(); err != nil {
		glog.Error(err)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s: from %s %s", reb.t.si.Name(), tsid, lom)
	}
	reb.t.statsif.AddMany(
		stats.NamedVal64{stats.RxRebCount, 1},
		stats.NamedVal64{stats.RxRebSize, hdr.ObjAttrs.Size})
	// ACK
	if tsi == nil {
		return
	}
	if stage := reb.stage.Load(); stage < rebStageFinStreams && stage != rebStageInactive {
		hdr.Opaque = []byte(reb.t.si.DaemonID) // self == src
		hdr.ObjAttrs.Size = 0
		if err := reb.acks.SendV(hdr, nil /*reader*/, nil /*callback*/, nil /*ptr*/, tsi); err != nil {
			glog.Error(err) // TODO: collapse same-type errors e.g. "src-id=>network: destination mismatch"
		}
	}
}

func (reb *rebManager) recvAck(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
	if err != nil {
		glog.Error(err)
		return
	}
	lom, err := cluster.LOM{T: reb.t, Bucket: hdr.Bucket, Objname: hdr.Objname}.Init(cmn.ProviderFromLoc(hdr.IsLocal))
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s: ack from %s on %s", reb.t.si.Name(), string(hdr.Opaque), lom)
	}
	if err != nil {
		glog.Error(err)
		return
	}
	var (
		_, idx = lom.Hkey()
		uname  = lom.Uname()
		lomack = reb.lomAcks()[idx]
	)
	lomack.mu.Lock()
	delete(lomack.q, uname)
	lomack.mu.Unlock()

	// TODO: configurable delay - postponed or manual object deletion
	cluster.ObjectLocker.Lock(uname, true)
	if err = lom.Remove(); err != nil {
		glog.Errorf("%s: error removing %s, err: %v", reb.t.si.Name(), lom, err)
	}
	cluster.ObjectLocker.Unlock(uname, true)
}

func (reb *rebManager) retransmit(xreb *xactGlobalReb) (cnt int) {
	smap := (*smapX)(reb.smap.Load())
	aborted := func() (yes bool) {
		yes = xreb.Aborted()
		yes = yes || (smap.version() != reb.t.smapowner.get().version())
		return
	}
	if aborted() {
		return
	}
	var (
		rj    = &globalRebJogger{rebJoggerBase: rebJoggerBase{m: reb, xreb: &xreb.xactRebBase, wg: &sync.WaitGroup{}}, smap: smap}
		tname = reb.t.si.Name()
		query = url.Values{}
	)
	query.Add(cmn.URLParamSilent, "true")
	for _, lomack := range reb.lomAcks() {
		lomack.mu.Lock()
		for uname, lom := range lomack.q {
			if err := lom.Load(false); err != nil {
				glog.Errorf("%s: failed loading %s, err: %s", tname, lom, err)
				delete(lomack.q, uname)
				continue
			}
			if !lom.Exists() {
				glog.Warningf("%s: %s %s", tname, lom, cmn.DoesNotExist)
				delete(lomack.q, uname)
				continue
			}
			tsi, _ := cluster.HrwTarget(lom.Bucket, lom.Objname, &smap.Smap)
			if reb.t.lookupRemote(lom, tsi) {
				if glog.FastV(4, glog.SmoduleAIS) {
					glog.Infof("%s: HEAD ok %s at %s", tname, lom, tsi.Name())
				}
				delete(lomack.q, uname)
				continue
			}
			// send obj
			if err := rj.send(lom, tsi, lom.Size()); err == nil {
				glog.Warningf("%s: resending %s => %s", tname, lom, tsi.Name())
				cnt++
			} else {
				glog.Errorf("%s: failed resending %s => %s, err: %v", tname, lom, tsi.Name(), err)
			}
		}
		lomack.mu.Unlock()
		if aborted() {
			return 0
		}
	}
	return
}

//
// globalRebJogger
//

func (rj *globalRebJogger) jog() {
	if rj.sema != nil {
		rj.errCh = make(chan error, cap(rj.sema)+1)
	}
	if err := filepath.Walk(rj.mpath, rj.walk); err != nil {
		if rj.xreb.Aborted() {
			glog.Infof("Aborting %s traversal", rj.mpath)
		} else {
			glog.Errorf("%s: failed to traverse %s, err: %v", rj.m.t.si.Name(), rj.mpath, err)
		}
	}
	rj.xreb.confirmCh <- struct{}{}
	rj.wg.Done()
}

func (rj *globalRebJogger) objSentCallback(hdr transport.Header, r io.ReadCloser, lomptr unsafe.Pointer, err error) {
	var (
		lom   = (*cluster.LOM)(lomptr)
		uname = lom.Uname()
		t     = rj.m.t
		tname = t.si.Name()
	)
	cluster.ObjectLocker.Unlock(uname, false)

	if err != nil {
		glog.Errorf("%s: failed to send o[%s/%s], err: %v", tname, hdr.Bucket, hdr.Objname, err)
		return
	}
	cmn.AssertMsg(hdr.ObjAttrs.Size == lom.Size(), lom.String()) // TODO: remove
	t.statsif.AddMany(
		stats.NamedVal64{stats.TxRebCount, 1},
		stats.NamedVal64{stats.TxRebSize, hdr.ObjAttrs.Size})
}

// the walking callback is executed by the LRU xaction
func (rj *globalRebJogger) walk(fqn string, fi os.FileInfo, inerr error) (err error) {
	var (
		lom *cluster.LOM
		tsi *cluster.Snode
		t   = rj.m.t
	)
	if rj.xreb.Aborted() {
		return fmt.Errorf("%s: aborted, path %s", rj.xreb, rj.mpath)
	}
	if inerr == nil && len(rj.errCh) > 0 {
		inerr = <-rj.errCh
	}
	if inerr != nil {
		if err := cmn.PathWalkErr(inerr); err != nil {
			glog.Error(err)
			return inerr
		}
		return nil
	}
	if fi.Mode().IsDir() {
		return nil
	}
	lom, err = cluster.LOM{T: t, FQN: fqn}.Init("")
	if err != nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s, err %s - skipping...", lom, err)
		}
		return nil
	}
	// rebalance, maybe
	tsi, err = cluster.HrwTarget(lom.Bucket, lom.Objname, &rj.smap.Smap)
	if err != nil {
		return err
	}
	if tsi.DaemonID == t.si.DaemonID {
		return nil
	}
	nver := t.smapowner.get().version()
	if nver > rj.ver {
		rj.xreb.Abort()
		return fmt.Errorf("%s: Smap v%d < v%d, path %s", rj.xreb, rj.ver, nver, rj.mpath)
	}

	// skip objects that were already sent via GFN (due to probabilistic filtering
	// false-positives, albeit rare, are still possible)
	uname := []byte(lom.Uname())
	if rj.m.filterGFN.Lookup(uname) {
		rj.m.filterGFN.Delete(uname) // it will not be used anymore
		return nil
	}

	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s => %s", lom, t.si.Name(), tsi.Name())
	}
	if rj.sema == nil { // rebalance.multiplier == 1
		err = rj.send(lom, tsi, fi.Size())
	} else { // // rebalance.multiplier > 1
		rj.sema <- struct{}{}
		go func() {
			ers := rj.send(lom, tsi, fi.Size())
			<-rj.sema
			if ers != nil {
				rj.errCh <- ers
			}
		}()
	}
	return
}

func (rj *globalRebJogger) send(lom *cluster.LOM, tsi *cluster.Snode, size int64) (err error) {
	var (
		file                  *cmn.FileHandle
		hdr                   transport.Header
		cksum                 cmn.Cksummer
		cksumType, cksumValue string
		lomack                *LomAcks
		idx                   int
	)
	uname := lom.Uname()
	cluster.ObjectLocker.Lock(uname, false) // NOTE: unlock in objSentCallback()

	err = lom.Load(false)
	if err != nil || !lom.Exists() || lom.IsCopy() {
		goto rerr
	}
	if cksum, err = lom.CksumComputeIfMissing(); err != nil {
		goto rerr
	}
	cksumType, cksumValue = cksum.Get()
	if file, err = cmn.NewFileHandle(lom.FQN); err != nil {
		goto rerr
	}
	if lom.Size() != size {
		glog.Errorf("%s: %s %d != %d", rj.m.t.si.Name(), lom, lom.Size(), size) // TODO: remove
	}
	hdr = transport.Header{
		Bucket:  lom.Bucket,
		Objname: lom.Objname,
		IsLocal: lom.BckIsLocal,
		Opaque:  []byte(rj.m.t.si.DaemonID), // self == src
		ObjAttrs: transport.ObjectAttrs{
			Size:       lom.Size(),
			Atime:      lom.Atime().UnixNano(),
			CksumType:  cksumType,
			CksumValue: cksumValue,
			Version:    lom.Version(),
		},
	}
	// cache it as pending-acknowledgement (optimistically - see objSentCallback)
	_, idx = lom.Hkey()
	lomack = rj.m.lomAcks()[idx]
	lomack.mu.Lock()
	lomack.q[uname] = lom
	lomack.mu.Unlock()
	// transmit
	if err := rj.m.t.rebManager.streams.SendV(hdr, file, rj.objSentCallback, unsafe.Pointer(lom) /* cmpl ptr */, tsi); err != nil {
		lomack.mu.Lock()
		delete(lomack.q, uname)
		lomack.mu.Unlock()
		goto rerr
	}
	return nil
rerr:
	cluster.ObjectLocker.Unlock(uname, false)
	if err != nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Errorf("%s, err: %v", lom, err)
		}
	}
	return
}

//======================================================================================
//
// Resilver
//
//======================================================================================

// TODO: support non-object content types
func (reb *rebManager) runLocalReb(skipGlobMisplaced bool, buckets ...string) {
	var (
		xreb              *xactLocalReb
		availablePaths, _ = fs.Mountpaths.Get()
		pmarker           = persistentMarker(cmn.ActLocalReb)
		file, err         = cmn.CreateFile(pmarker)
		bucket            string
		wg                = &sync.WaitGroup{}
	)
	if err != nil {
		glog.Errorln("Failed to create", pmarker, err)
		pmarker = ""
	} else {
		_ = file.Close()
	}
	if len(buckets) > 0 {
		bucket = buckets[0] // special case: local bucket
		xreb = reb.t.xactions.renewLocalReb(len(availablePaths))
	} else {
		xreb = reb.t.xactions.renewLocalReb(len(availablePaths) * 2)
	}
	slab, err := nodeCtx.mm.GetSlab2(memsys.MaxSlabSize) // TODO: estimate
	cmn.AssertNoErr(err)

	for _, mpathInfo := range availablePaths {
		var mpathL string
		if bucket == "" {
			mpathL = mpathInfo.MakePath(fs.ObjectType, true /*local*/)
		} else {
			mpathL = mpathInfo.MakePathBucket(fs.ObjectType, bucket, true)
		}
		jogger := &localRebJogger{rebJoggerBase: rebJoggerBase{m: reb, mpath: mpathL, xreb: &xreb.xactRebBase, wg: wg},
			slab:              slab,
			skipGlobMisplaced: skipGlobMisplaced,
		}
		wg.Add(1)
		go jogger.jog()
	}
	if bucket != "" {
		goto wait
	}
	for _, mpathInfo := range availablePaths {
		mpathC := mpathInfo.MakePath(fs.ObjectType, false /*cloud*/)
		jogger := &localRebJogger{rebJoggerBase: rebJoggerBase{m: reb, mpath: mpathC, xreb: &xreb.xactRebBase, wg: wg},
			slab:              slab,
			skipGlobMisplaced: skipGlobMisplaced,
		}
		wg.Add(1)
		go jogger.jog()
	}
wait:
	glog.Infoln(xreb.String())
	wg.Wait()

	if pmarker != "" {
		if !xreb.Aborted() {
			if err := os.Remove(pmarker); err != nil && !os.IsNotExist(err) {
				glog.Errorf("%s: failed to remove in-progress mark %s, err: %v", reb.t.si.Name(), pmarker, err)
			}
		}
	}
	reb.t.gfn.local.deactivate()
	xreb.EndTime(time.Now())
}

//
// localRebJogger
//

func (rj *localRebJogger) jog() {
	rj.buf = rj.slab.Alloc()
	if err := filepath.Walk(rj.mpath, rj.walk); err != nil {
		if rj.xreb.Aborted() {
			glog.Infof("Aborting %s traversal", rj.mpath)
		} else {
			glog.Errorf("%s: failed to traverse %s, err: %v", rj.m.t.si.Name(), rj.mpath, err)
		}
	}
	rj.xreb.confirmCh <- struct{}{}
	rj.slab.Free(rj.buf)
	rj.wg.Done()
}

func (rj *localRebJogger) walk(fqn string, fileInfo os.FileInfo, err error) error {
	var t = rj.m.t
	if rj.xreb.Aborted() {
		return fmt.Errorf("%s aborted, path %s", rj.xreb, rj.mpath)
	}
	if err != nil {
		if err := cmn.PathWalkErr(err); err != nil {
			glog.Error(err)
			return err
		}
		return nil
	}
	if fileInfo.IsDir() {
		return nil
	}
	lom, err := cluster.LOM{T: t, FQN: fqn}.Init("")
	if err != nil {
		return nil
	}
	// optionally, skip those that must be globally rebalanced
	if rj.skipGlobMisplaced {
		smap := t.smapowner.get()
		if tsi, err := cluster.HrwTarget(lom.Bucket, lom.Objname, &smap.Smap); err == nil {
			if tsi.DaemonID != t.si.DaemonID {
				return nil
			}
		}
	}
	// skip those that are _not_ locally misplaced
	if !lom.Misplaced() {
		return nil
	}

	ri := &replicInfo{t: t, bucketTo: lom.Bucket, buf: rj.buf, localCopy: true}
	copied, err := ri.copyObject(lom, lom.Objname)
	if err != nil {
		glog.Warningf("%s: %v", lom, err)
		return nil
	}
	if !copied {
		return nil
	}
	if lom.HasCopies() { // TODO: punt replicated and erasure copied to LRU
		return nil
	}
	// misplaced with no copies? remove right away
	cluster.ObjectLocker.Lock(lom.Uname(), true)
	if err = os.Remove(lom.FQN); err != nil {
		glog.Warningf("%s: %v", lom, err)
	}
	cluster.ObjectLocker.Unlock(lom.Uname(), true)
	return nil
}

//
// helpers
//

// persistent mark indicating rebalancing in progress
func persistentMarker(kind string) (pm string) {
	switch kind {
	case cmn.ActLocalReb:
		pm = filepath.Join(cmn.GCO.Get().Confdir, cmn.LocalRebMarker)
	case cmn.ActGlobalReb:
		pm = filepath.Join(cmn.GCO.Get().Confdir, cmn.GlobalRebMarker)
	default:
		cmn.Assert(false)
	}
	return
}
