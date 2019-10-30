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
	"path/filepath"
	"strings"
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
	globRebMarker  = ".global_rebalancing"
	localRebMarker = ".resilvering"
)

const (
	rebalanceStreamName = "rebalance"
	rebalanceAcksName   = "remwack" // NOTE: can become generic remote-write-acknowledgment
	rebalancePushName   = "rebpush" // push notifications stream
)

// rebalance stage enum
const (
	rebStageInactive = iota
	rebStageInit
	rebStageTraverse
	rebStageECNameSpace  // local slice list built
	rebStageECDetect     // all lists are received, start detecting which objects to fix
	rebStageECGlobRepair // all local slices are fine, targets start using remote data
	rebStageECBatch      // target sends message that the current batch is processed
	rebStageECCleanup    // all is done, time to cleanup memory etc
	rebStageWaitAck
	rebStageFin
	rebStageFinStreams
	rebStageDone
)

type (
	rebSyncCallback func(tsi *cluster.Snode, md *globalRebArgs) (ok bool)

	rebManager struct {
		t          *targetrunner
		filterGFN  *filter.Filter
		netd, netc string
		smap       atomic.Pointer // new smap which will be soon live
		streams    *transport.StreamBundle
		acks       *transport.StreamBundle
		pushes     *transport.StreamBundle
		lomacks    [fs.LomCacheMask + 1]*LomAcks
		tcache     struct { // not to recompute very often
			tmap cluster.NodeMap
			ts   time.Time
			mu   *sync.Mutex
		}
		semaCh     chan struct{}
		beginStats atomic.Pointer // *stats.ExtRebalanceStats
		ecReb      *ecRebalancer
		stageMtx   sync.Mutex
		nodeStages map[string]uint32
		globRebID  atomic.Int64
		stage      atomic.Uint32 // rebStage* enum
		laterx     atomic.Bool
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
	globalRebArgs struct {
		smap    *smapX
		config  *cmn.Config
		xreb    *xactGlobalReb
		paths   fs.MPI
		pmarker string
		ecUsed  bool
		dryRun  bool
	}
)

var rebStage = map[uint32]string{
	rebStageInactive:     "<inactive>",
	rebStageInit:         "<init>",
	rebStageTraverse:     "<traverse>",
	rebStageWaitAck:      "<wack>",
	rebStageFin:          "<fin>",
	rebStageFinStreams:   "<fin-streams>",
	rebStageDone:         "<done>",
	rebStageECNameSpace:  "<namespace>",
	rebStageECDetect:     "<build-fix-list>",
	rebStageECGlobRepair: "<ec-transfer>",
	rebStageECCleanup:    "<ec-fin>",
}

//
// rebManager
//
func (reb *rebManager) lomAcks() *[fs.LomCacheMask + 1]*LomAcks { return &reb.lomacks }

func (reb *rebManager) loghdr(globRebID int64, smap *smapX) string {
	var (
		tname = reb.t.si.Name()
		stage = rebStage[reb.stage.Load()]
	)
	return fmt.Sprintf("%s[g%d,v%d,%s]", tname, globRebID, smap.version(), stage)
}

func (reb *rebManager) globalRebPrecheck(md *globalRebArgs, globRebID int64) bool {
	// get EC rebalancer ready
	if md.ecUsed {
		reb.ecReb.cleanup()
		reb.ecReb.waiter.waitFor.Store(0)
	}
	// 1. check whether other targets are up and running
	if errCnt := reb.bcast(md, reb.pingTarget); errCnt > 0 {
		return false
	}
	if md.smap.version() == 0 {
		md.smap = reb.t.smapowner.get()
	}

	// 2. serialize (rebalancing operations - one at a time post this point)
	//    start new xaction unless the one for the current version is already in progress
	if newerSmap, alreadyRunning := reb.serialize(md.smap, md.config, globRebID); newerSmap || alreadyRunning {
		return false
	}
	if md.smap.version() == 0 {
		md.smap = reb.t.smapowner.get()
	}

	md.paths, _ = fs.Mountpaths.Get()
	return true
}

func (reb *rebManager) globalRebInit(md *globalRebArgs, globRebID int64, buckets ...string) bool {
	/* ================== rebStageInit ================== */
	reb.stage.Store(rebStageInit)
	md.xreb = reb.t.xactions.renewGlobalReb(md.smap.version(), globRebID, len(md.paths)*2)
	cmn.Assert(md.xreb != nil) // must renew given the CAS and checks above

	if len(buckets) > 0 {
		md.xreb.bucket = buckets[0] // for better identity (limited usage)
	}

	// 3. init streams and data structures
	reb.beginStats.Store(unsafe.Pointer(reb.getStats()))
	reb.beginStreams(md)
	reb.filterGFN.Reset() // start with empty filters
	reb.tcache.tmap = make(cluster.NodeMap, md.smap.CountTargets()-1)
	reb.tcache.mu = &sync.Mutex{}
	acks := reb.lomAcks()
	for i := 0; i < len(acks); i++ { // init lom acks
		acks[i] = &LomAcks{mu: &sync.Mutex{}, q: make(map[string]*cluster.LOM, 64)}
	}

	// 4. create persistent mark
	md.pmarker = persistentMarker(cmn.ActGlobalReb)
	file, err := cmn.CreateFile(md.pmarker)
	if err != nil {
		glog.Errorln("Failed to create", md.pmarker, err)
		md.pmarker = ""
	} else {
		_ = file.Close()
	}

	// 5. ready - can receive objects
	reb.smap.Store(unsafe.Pointer(md.smap))
	reb.globRebID.Store(globRebID)
	glog.Infof("%s: %s", reb.loghdr(globRebID, md.smap), md.xreb.String())

	return true
}

// look for local slices/replicas
func (reb *rebManager) buildECNamespace(md *globalRebArgs) int {
	reb.ecReb.run()
	reb.stage.Store(rebStageECNameSpace)
	if reb.waitForPushReqs(md, rebStageECNameSpace) {
		return 0
	}
	return reb.bcast(md, reb.waitNamespace)
}

// send all collected slices to a correct target(that must have "main" object).
// It is a two-step process:
//   1. The target sends all colected data to correct targets
//   2. If the target is too fast, it may send too early(or in case of network
//      troubles) that results in data loss. But the target does not know if
//		the destination received the data. So, the targets enters
//		`rebStageECDetect` state that means "I'm ready to receive data
//		exchange requests"
//   3. In a perfect case, all push requests are successful and
//		`rebStageECDetect` stage will be finished in no time without any
//		data transfer
func (reb *rebManager) distributeECNamespace(md *globalRebArgs) error {
	const distributeTimeout = 5 * time.Minute
	if err := reb.ecReb.exchange(); err != nil {
		return err
	}
	reb.stage.Store(rebStageECDetect)
	if reb.waitForPushReqs(md, rebStageECDetect, distributeTimeout) {
		return nil
	}
	cnt := reb.bcast(md, reb.waitECData)
	if cnt != 0 {
		return fmt.Errorf("%d node failed to send their data", cnt)
	}
	return nil
}

// find out which objects are broken and how to fix them
func (reb *rebManager) generateECFixList(md *globalRebArgs) []*ecRebObject {
	broken := reb.ecReb.checkSlices()
	if bool(glog.FastV(4, glog.SmoduleAIS)) || md.dryRun {
		glog.Infof("Number of objects misplaced locally: %d", len(reb.ecReb.localActions))
		glog.Infof("Number of objects needs to be reconstructed/resent: %d", len(broken))
	}
	return broken
}

func (reb *rebManager) ecFixLocal(md *globalRebArgs) error {
	if err := reb.ecReb.repairLocal(); err != nil {
		return fmt.Errorf("Failed to rebalance local slices/objects: %v", err)
	}

	reb.stage.Store(rebStageECGlobRepair)
	reb.setStage(reb.t.si.DaemonID, rebStageECGlobRepair)
	if cnt := reb.bcast(md, reb.waitECLocalReb); cnt != 0 {
		return fmt.Errorf("%d targets failed to complete local rebalance", cnt)
	}
	return nil
}

func (reb *rebManager) ecFixGlobal(md *globalRebArgs, broken []*ecRebObject) error {
	if err := reb.ecReb.repairGlobal(broken); err != nil {
		if !md.xreb.Aborted() {
			glog.Errorf("EC rebalance failed: %v", err)
			md.xreb.Abort()
		}
		return err
	}

	reb.stage.Store(rebStageECCleanup)
	reb.setStage(reb.t.si.DaemonID, rebStageECCleanup)
	if cnt := reb.bcast(md, reb.waitECCleanup); cnt != 0 {
		return fmt.Errorf("%d targets failed to complete local rebalance", cnt)
	}
	return nil
}

// when at least one bucket has EC enabled
func (reb *rebManager) globalRebRunEC(md *globalRebArgs) error {
	var (
		cnt    int            // the number of targets failed to reach some stage
		broken []*ecRebObject // objects with missing or misplaced parts
	)
	// collect all local slices
	if cnt = reb.buildECNamespace(md); cnt != 0 {
		return fmt.Errorf("%d targets failed to build namespace", cnt)
	}
	// wait for all targets send their lists of slices to other nodes
	if err := reb.distributeECNamespace(md); err != nil {
		return err
	}
	// detect objects with misplaced or missing parts
	broken = reb.generateECFixList(md)

	// fix objects that are on local target but they are misplaced
	if err := reb.ecFixLocal(md); err != nil {
		return err
	}

	// fix objects that needs network transfers and/or object rebuild
	if err := reb.ecFixGlobal(md, broken); err != nil {
		return err
	}

	glog.Infof("[%s] RebalanceEC done", reb.t.si.DaemonID)
	return nil
}

// when no bucket has EC enabled
func (reb *rebManager) globalRebRun(md *globalRebArgs) error {
	wg := &sync.WaitGroup{}
	ver := md.smap.version()
	globRebID := reb.globRebID.Load()
	multiplier := md.config.Rebalance.Multiplier
	_ = reb.bcast(md, reb.rxReady) // NOTE: ignore timeout
	if md.xreb.Aborted() {
		err := fmt.Errorf("%s: aborted", reb.loghdr(globRebID, md.smap))
		return err
	}
	for _, mpathInfo := range md.paths {
		var (
			sema   chan struct{}
			mpathL string
		)
		mpathL = mpathInfo.MakePath(fs.ObjectType, cmn.AIS)
		if multiplier > 1 {
			sema = make(chan struct{}, multiplier)
		}
		rl := &globalRebJogger{rebJoggerBase: rebJoggerBase{m: reb, mpath: mpathL, xreb: &md.xreb.xactRebBase, wg: wg},
			smap: md.smap, sema: sema, ver: ver}
		wg.Add(1)
		go rl.jog()
	}
	for _, mpathInfo := range md.paths {
		var sema chan struct{}
		mpathC := mpathInfo.MakePath(fs.ObjectType, cmn.Cloud)
		if multiplier > 1 {
			sema = make(chan struct{}, multiplier)
		}
		rc := &globalRebJogger{rebJoggerBase: rebJoggerBase{m: reb, mpath: mpathC, xreb: &md.xreb.xactRebBase, wg: wg},
			smap: md.smap, sema: sema, ver: ver}
		wg.Add(1)
		go rc.jog()
	}
	wg.Wait()
	if md.xreb.Aborted() {
		err := fmt.Errorf("%s: aborted", reb.loghdr(globRebID, md.smap))
		return err
	}
	return nil
}

func (reb *rebManager) globalRebSyncAndRun(md *globalRebArgs) error {
	// 6. capture stats, start mpath joggers TODO: currently supporting only fs.ObjectType (content-type)
	reb.stage.Store(rebStageTraverse)
	if md.ecUsed {
		glog.Infof("EC detected - starting EC-friendly rebalance")
		return reb.globalRebRunEC(md)
	}

	return reb.globalRebRun(md)
}

func (reb *rebManager) globalRebWaitAck(md *globalRebArgs) (errCnt int) {
	reb.stage.Store(rebStageWaitAck)
	globRebID := reb.globRebID.Load()
	loghdr := reb.loghdr(globRebID, md.smap)
	sleep := md.config.Timeout.CplaneOperation // NOTE: TODO: used throughout; must be separately assigned and calibrated
	maxwt := md.config.Rebalance.DestRetryTime
	cnt := 0
	maxwt += time.Duration(int64(time.Minute) * int64(md.smap.CountTargets()/10))
	maxwt = cmn.MinDur(maxwt, md.config.Rebalance.DestRetryTime*2)

	for {
		curwt := time.Duration(0)
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
							tsi, err := cluster.HrwTarget(lom.Bck(), lom.Objname, &md.smap.Smap)
							if err == nil {
								glog.Infof("waiting for %s ACK from %s", lom, tsi)
								logged = true
								break
							}
						}
					}
				}
				lomack.mu.Unlock()
				if md.xreb.Aborted() {
					glog.Infof("%s: abrt", loghdr)
					return
				}
			}
			if cnt == 0 {
				glog.Infof("%s: received all ACKs", loghdr)
				break
			}
			glog.Warningf("%s: waiting for %d ACKs", loghdr, cnt)
			if md.xreb.abortedAfter(sleep) {
				glog.Infof("%s: abrt", loghdr)
				return
			}
			curwt += sleep
		}
		if cnt > 0 {
			glog.Warningf("%s: timed-out waiting for %d ACK(s)", loghdr, cnt)
		}
		if md.xreb.Aborted() {
			return
		}

		// NOTE: requires locally migrated objects *not* to be removed at the src
		aPaths, _ := fs.Mountpaths.Get()
		if len(aPaths) > len(md.paths) {
			glog.Warningf("%s: mountpath changes detected (%d, %d)", loghdr, len(aPaths), len(md.paths))
		}

		// 8. synchronize
		glog.Infof("%s: poll targets for: stage=(%s or %s***)", loghdr, rebStage[rebStageFin], rebStage[rebStageWaitAck])
		if !reb.waitForPushReqs(md, rebStageFin) {
			// no need to broadcast if all targets have sent notifications
			errCnt = reb.bcast(md, reb.waitFinExtended)
		}
		if md.xreb.Aborted() {
			return
		}

		// 9. retransmit if needed
		cnt = reb.retransmit(md.xreb, globRebID)
		if cnt == 0 || md.xreb.Aborted() {
			break
		}
		glog.Warningf("%s: retransmitted %d, more wack...", loghdr, cnt)
	}

	return
}

func (reb *rebManager) globalRebFini(md *globalRebArgs) {
	sleep := md.config.Timeout.CplaneOperation // NOTE: TODO: used throughout; must be separately assigned and calibrated
	// 10.5. keep at it... (can't close the streams as long as)
	quiescent, maxquiet := 0, 10 // e.g., 10 * 2s (Cplane) = 20 seconds of /quiet/ time - see laterx
	aborted := md.xreb.Aborted()
	for quiescent < maxquiet && !aborted {
		if !reb.laterx.CAS(true, false) {
			quiescent++
		} else {
			quiescent = 0
		}
		aborted = md.xreb.abortedAfter(sleep)
	}
	if !aborted {
		if err := cmn.RemoveFile(md.pmarker); err != nil {
			glog.Errorf("%s: failed to remove in-progress mark %s, err: %v", reb.loghdr(reb.globRebID.Load(), md.smap), md.pmarker, err)
		}
	}
	reb.endStreams()
	if !md.xreb.Finished() {
		md.xreb.EndTime(time.Now())
	} else {
		glog.Infoln(md.xreb.String())
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
	reb.semaCh <- struct{}{}
}

// main method: 10 stages
func (reb *rebManager) runGlobalReb(smap *smapX, globRebID int64, buckets ...string) {
	md := &globalRebArgs{
		smap:   smap,
		config: cmn.GCO.Get(),
	}
	if len(buckets) == 0 || buckets[0] == "" {
		md.ecUsed = reb.t.bmdowner.get().ecUsed()
	} else {
		// single bucket rebalance is AIS case only
		bck := cluster.Bck{Name: buckets[0], Provider: cmn.AIS}
		props, ok := reb.t.bmdowner.Get().Get(&bck)
		if !ok {
			glog.Errorf("Bucket %q not found", bck.Name)
			return
		}
		md.ecUsed = props.EC.Enabled
	}

	// TODO: remove after everything is done (it is for debugging EC rebalance)
	if md.ecUsed {
		md.dryRun = true
	}
	if !reb.globalRebPrecheck(md, globRebID) {
		return
	}
	if !reb.globalRebInit(md, globRebID, buckets...) {
		return
	}

	// At this point only one rebalance is running so we can safely enable regular GFN.
	reb.t.gfn.global.activate()
	defer reb.t.gfn.global.deactivate()

	errCnt := 0
	if err := reb.globalRebSyncAndRun(md); err == nil {
		errCnt = reb.globalRebWaitAck(md)
	} else {
		glog.Warning(err)
	}
	reb.stage.Store(rebStageFin)
	for errCnt != 0 && !md.xreb.Aborted() {
		errCnt = reb.bcast(md, reb.waitFinExtended)
	}
	reb.globalRebFini(md)
	// clean up all collected data
	if reb.t.bmdowner.get().ecUsed() {
		reb.ecReb.cleanup()
	}

	reb.nodeStages = make(map[string]uint32) // cleanup after run
}

func (reb *rebManager) serialize(smap *smapX, config *cmn.Config, globRebID int64) (newerSmap, alreadyRunning bool) {
	var (
		ver    = smap.version()
		sleep  = config.Timeout.CplaneOperation
		canRun bool
	)
	for {
		select {
		case <-reb.semaCh:
			canRun = true
		default:
		}
		//
		// vs newer Smap
		//
		nver := reb.t.smapowner.get().version()
		loghdr := reb.loghdr(globRebID, smap)
		if nver > ver {
			glog.Warningf("%s: seeing newer Smap v%d, not running", loghdr, nver)
			newerSmap = true
			if canRun {
				reb.semaCh <- struct{}{}
			}
			return
		}
		if reb.globRebID.Load() == globRebID {
			if canRun {
				reb.semaCh <- struct{}{}
			}
			glog.Warningf("%s: g%d is already running", loghdr, globRebID)
			alreadyRunning = true
			return
		}
		//
		// vs current xaction
		//
		entry := reb.t.xactions.GetL(cmn.ActGlobalReb)
		if entry == nil {
			if canRun {
				return
			}
			glog.Warningf("%s: waiting for ???...", loghdr)
		} else {
			xact := entry.Get()
			otherXreb := xact.(*xactGlobalReb) // running or previous
			if canRun {
				cmn.Assert(otherXreb.Finished())
				return
			}
			if otherXreb.smapVersion < ver && !otherXreb.Finished() {
				otherXreb.Abort()
				glog.Warningf("%s: aborting older Smap [%s]", loghdr, otherXreb)
			}
		}
		cmn.Assert(!canRun)
		time.Sleep(sleep)
	}
}

func (reb *rebManager) getStats() (s *stats.ExtRebalanceStats) {
	s = &stats.ExtRebalanceStats{}
	statsRunner := getstorstatsrunner()
	s.TxRebCount = statsRunner.Get(stats.TxRebCount)
	s.RxRebCount = statsRunner.Get(stats.RxRebCount)
	s.TxRebSize = statsRunner.Get(stats.TxRebSize)
	s.RxRebSize = statsRunner.Get(stats.RxRebSize)
	s.GlobalRebID = reb.globRebID.Load()
	return
}

func (reb *rebManager) beginStreams(md *globalRebArgs) {
	cmn.Assert(reb.stage.Load() == rebStageInit)
	if md.config.Rebalance.Multiplier == 0 {
		md.config.Rebalance.Multiplier = 1
	} else if md.config.Rebalance.Multiplier > 8 {
		glog.Errorf("%s: stream-and-mp-jogger multiplier=%d - misconfigured?",
			reb.t.si.Name(), md.config.Rebalance.Multiplier)
	}
	//
	// objects
	//
	client := transport.NewIntraDataClient()
	sbArgs := transport.SBArgs{
		Network: reb.netd,
		Trname:  rebalanceStreamName,
		Extra: &transport.Extra{
			Compression: md.config.Rebalance.Compression,
			Config:      md.config,
			Mem2:        nodeCtx.mm},
		Multiplier:   int(md.config.Rebalance.Multiplier),
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

	pushArgs := transport.SBArgs{
		Network: reb.netc,
		Trname:  rebalancePushName,
	}
	reb.pushes = transport.NewStreamBundle(reb.t.smapowner, reb.t.Snode(), client, pushArgs)

	// Init ecRebalancer streams
	if md.ecUsed {
		reb.ecReb.init(md, reb.netd)
	}
	reb.laterx.Store(false)
}

func (reb *rebManager) endStreams() {
	if reb.stage.CAS(rebStageFin, rebStageFinStreams) { // TODO: must always succeed?
		reb.streams.Close(true /* graceful */)
		reb.streams = nil
		reb.acks.Close(true)
		reb.pushes.Close(true)
		reb.ecReb.endStreams()
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
	lom := &cluster.LOM{T: reb.t, Objname: hdr.Objname}
	if err = lom.Init(hdr.Bucket, cmn.ProviderFromBool(hdr.BckIsAIS)); err != nil {
		glog.Error(err)
		return
	}
	aborted, running := reb.t.xactions.isRebalancing(cmn.ActGlobalReb)
	if aborted || !running {
		io.Copy(ioutil.Discard, objReader) // drain the reader
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

// Mark a 'node' that it has passed the 'stage'
func (reb *rebManager) setStage(node string, stage uint32) {
	reb.stageMtx.Lock()
	reb.nodeStages[node] = stage
	reb.stageMtx.Unlock()
}

// Rebalance moves to the next stage:
// - update internal stage
// - send notification to all other targets that this one is in a new stage
func (reb *rebManager) changeStage(newStage uint32) {
	// first, set its own stage
	reb.setStage(reb.t.si.DaemonID, newStage)

	smap := reb.t.smapowner.Get()
	nodes := make([]*cluster.Snode, 0, len(smap.Tmap))
	for _, node := range smap.Tmap {
		if node.DaemonID == reb.t.si.DaemonID {
			continue
		}
		nodes = append(nodes, node)
	}

	req := pushReq{DaemonID: reb.t.si.DaemonID, Stage: newStage, RebID: reb.globRebID.Load()}
	hdr := transport.Header{
		ObjAttrs: transport.ObjectAttrs{Size: 0},
		Opaque:   cmn.MustMarshal(req),
	}
	// second, notify all other targets
	if err := reb.pushes.SendV(hdr, nil, nil, nil, nodes...); err != nil {
		var sb strings.Builder
		for idx, n := range nodes {
			sb.WriteString(n.DaemonID)
			if idx < len(nodes)-1 {
				sb.WriteString(",")
			}
		}
		// in case of error only log a warning - other targets should be able
		// to pull the new stage and continue later
		glog.Warningf("Failed to send ack %s to %s nodes: %v", rebStage[newStage], sb.String(), err)
		return
	}
}

// returns true if the node is in or passed `stage`
func (reb *rebManager) isNodeInStage(si *cluster.Snode, stage uint32) bool {
	reb.stageMtx.Lock()
	nodeStage, ok := reb.nodeStages[si.DaemonID]
	reb.stageMtx.Unlock()
	return ok && nodeStage >= stage
}

func (reb *rebManager) recvPush(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
	if err != nil {
		glog.Errorf("Failed to get notification %s from %s: %v", hdr.Objname, hdr.Bucket, err)
		return
	}

	var req pushReq
	if err := jsoniter.Unmarshal(hdr.Opaque, &req); err != nil {
		glog.Errorf("Invalid push notification: %v", err)
		return
	}
	// a target was too late in sending(rebID is obsolete) its data or too early (md == nil)
	if req.RebID < reb.globRebID.Load() {
		// TODO: warning
		glog.Errorf("Rebalance IDs mismatch: %d vs %d. %s is late(%s)?", reb.globRebID.Load(), req.RebID, req.DaemonID, rebStage[req.Stage])
		return
	}

	if req.Stage == rebStageECBatch {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s Target %s finished batch %d", reb.t.si.Name(), req.DaemonID, req.Batch)
		}
		reb.ecReb.batchDone(req.DaemonID, req.Batch)
		return
	}

	reb.setStage(req.DaemonID, req.Stage)
}

func (reb *rebManager) recvAck(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
	if err != nil {
		glog.Error(err)
		return
	}
	lom := &cluster.LOM{T: reb.t, Objname: hdr.Objname}
	if err = lom.Init(hdr.Bucket, cmn.ProviderFromBool(hdr.BckIsAIS)); err != nil {
		glog.Error(err)
		return
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s: ack from %s on %s", reb.t.si.Name(), string(hdr.Opaque), lom)
	}
	var (
		_, idx = lom.Hkey()
		lomack = reb.lomAcks()[idx]
	)
	lomack.mu.Lock()
	delete(lomack.q, lom.Uname())
	lomack.mu.Unlock()

	// TODO: configurable delay - postponed or manual object deletion
	lom.Lock(true)
	if err = lom.Remove(); err != nil {
		glog.Errorf("%s: error removing %s, err: %v", reb.t.si.Name(), lom, err)
	}
	lom.Unlock(true)
}

func (reb *rebManager) retransmit(xreb *xactGlobalReb, globRebID int64) (cnt int) {
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
		query = url.Values{}
	)
	query.Add(cmn.URLParamSilent, "true")
	for _, lomack := range reb.lomAcks() {
		lomack.mu.Lock()
		for uname, lom := range lomack.q {
			if err := lom.Load(false); err != nil {
				glog.Errorf("%s: failed loading %s, err: %s", reb.loghdr(globRebID, smap), lom, err)
				delete(lomack.q, uname)
				continue
			}
			if !lom.Exists() {
				glog.Warningf("%s: %s %s", reb.loghdr(globRebID, smap), lom, cmn.DoesNotExist)
				delete(lomack.q, uname)
				continue
			}
			tsi, _ := cluster.HrwTarget(lom.Bck(), lom.Objname, &smap.Smap)
			if reb.t.lookupRemoteSingle(lom, tsi) {
				if glog.FastV(4, glog.SmoduleAIS) {
					glog.Infof("%s: HEAD ok %s at %s", reb.loghdr(globRebID, smap), lom, tsi.Name())
				}
				delete(lomack.q, uname)
				continue
			}
			// send obj
			if err := rj.send(lom, tsi); err == nil {
				glog.Warningf("%s: resending %s => %s", reb.loghdr(globRebID, smap), lom, tsi.Name())
				cnt++
			} else {
				glog.Errorf("%s: failed resending %s => %s, err: %v", reb.loghdr(globRebID, smap), lom, tsi.Name(), err)
			}
			if aborted() {
				lomack.mu.Unlock()
				return 0
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
	opts := &fs.Options{
		Callback: rj.walk,
		Sorted:   false,
	}
	if err := fs.Walk(rj.mpath, opts); err != nil {
		if rj.xreb.Aborted() || rj.xreb.Finished() {
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
		lom = (*cluster.LOM)(lomptr)
		t   = rj.m.t
	)
	lom.Unlock(false)

	if err != nil {
		glog.Errorf("%s: failed to send o[%s/%s], err: %v", t.si.Name(), hdr.Bucket, hdr.Objname, err)
		return
	}
	cmn.AssertMsg(hdr.ObjAttrs.Size == lom.Size(), lom.String()) // TODO: remove
	t.statsif.AddMany(
		stats.NamedVal64{stats.TxRebCount, 1},
		stats.NamedVal64{stats.TxRebSize, hdr.ObjAttrs.Size})
}

// the walking callback is executed by the LRU xaction
func (rj *globalRebJogger) walk(fqn string, de fs.DirEntry) (err error) {
	var (
		lom *cluster.LOM
		tsi *cluster.Snode
		t   = rj.m.t
	)
	if rj.xreb.Aborted() || rj.xreb.Finished() {
		return fmt.Errorf("%s: aborted, path %s", rj.xreb, rj.mpath)
	}
	if de.IsDir() {
		return nil
	}
	lom = &cluster.LOM{T: t, FQN: fqn}
	err = lom.Init("", "")
	if err != nil {
		if glog.FastV(4, glog.SmoduleAIS) {
			glog.Infof("%s, err %s - skipping...", lom, err)
		}
		return nil
	}
	// rebalance, maybe
	tsi, err = cluster.HrwTarget(lom.Bck(), lom.Objname, &rj.smap.Smap)
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

	if err := lom.Load(); err != nil {
		return err
	}
	if glog.FastV(4, glog.SmoduleAIS) {
		glog.Infof("%s %s => %s", lom, t.si.Name(), tsi.Name())
	}
	if rj.sema == nil { // rebalance.multiplier == 1
		err = rj.send(lom, tsi)
	} else { // // rebalance.multiplier > 1
		rj.sema <- struct{}{}
		go func() {
			ers := rj.send(lom, tsi)
			<-rj.sema
			if ers != nil {
				rj.errCh <- ers
			}
		}()
	}
	return
}

func (rj *globalRebJogger) send(lom *cluster.LOM, tsi *cluster.Snode) (err error) {
	var (
		file                  *cmn.FileHandle
		hdr                   transport.Header
		cksum                 *cmn.Cksum
		cksumType, cksumValue string
		lomack                *LomAcks
		idx                   int
	)
	lom.Lock(false) // NOTE: unlock in objSentCallback()

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
	hdr = transport.Header{
		Bucket:   lom.Bucket(),
		Objname:  lom.Objname,
		BckIsAIS: lom.IsAIS(),
		Opaque:   []byte(rj.m.t.si.DaemonID), // self == src
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
	lomack.q[lom.Uname()] = lom
	lomack.mu.Unlock()
	// transmit
	if err := rj.m.t.rebManager.streams.SendV(hdr, file, rj.objSentCallback, unsafe.Pointer(lom) /* cmpl ptr */, tsi); err != nil {
		lomack.mu.Lock()
		delete(lomack.q, lom.Uname())
		lomack.mu.Unlock()
		goto rerr
	}
	return nil
rerr:
	lom.Unlock(false)
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
		bucket = buckets[0] // special case: ais bucket
	}
	if bucket != "" {
		xreb = reb.t.xactions.renewLocalReb(len(availablePaths))
		xreb.bucket = bucket
	} else {
		xreb = reb.t.xactions.renewLocalReb(len(availablePaths) * 2)
	}
	slab, err := nodeCtx.mm.GetSlab2(memsys.MaxSlabSize) // TODO: estimate
	cmn.AssertNoErr(err)

	for _, mpathInfo := range availablePaths {
		var mpathL string
		if bucket == "" {
			mpathL = mpathInfo.MakePath(fs.ObjectType, cmn.AIS)
		} else {
			mpathL = mpathInfo.MakePathBucket(fs.ObjectType, bucket, cmn.AIS)
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
		mpathC := mpathInfo.MakePath(fs.ObjectType, cmn.Cloud)
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
			if err := cmn.RemoveFile(pmarker); err != nil {
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
	opts := &fs.Options{
		Callback: rj.walk,
		Sorted:   false,
	}
	if err := fs.Walk(rj.mpath, opts); err != nil {
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

func (rj *localRebJogger) walk(fqn string, de fs.DirEntry) (err error) {
	var t = rj.m.t
	if rj.xreb.Aborted() {
		return fmt.Errorf("%s aborted, path %s", rj.xreb, rj.mpath)
	}
	if de.IsDir() {
		return nil
	}
	lom := &cluster.LOM{T: t, FQN: fqn}
	if err = lom.Init("", ""); err != nil {
		return nil
	}
	// optionally, skip those that must be globally rebalanced
	if rj.skipGlobMisplaced {
		smap := t.smapowner.get()
		if tsi, err := cluster.HrwTarget(lom.Bck(), lom.Objname, &smap.Smap); err == nil {
			if tsi.DaemonID != t.si.DaemonID {
				return nil
			}
		}
	}
	// skip those that are _not_ locally misplaced
	if lom.IsHRW() {
		return nil
	}

	ri := &replicInfo{t: t, bckTo: lom.Bck(), buf: rj.buf, localOnly: true, finalize: false}
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
	lom.Lock(true)
	if err = cmn.RemoveFile(lom.FQN); err != nil {
		glog.Warningf("%s: %v", lom, err)
	}
	lom.Unlock(true)
	return nil
}

//
// helpers
//

// persistent mark indicating rebalancing in progress
func persistentMarker(kind string) (pm string) {
	switch kind {
	case cmn.ActLocalReb:
		pm = filepath.Join(cmn.GCO.Get().Confdir, localRebMarker)
	case cmn.ActGlobalReb:
		pm = filepath.Join(cmn.GCO.Get().Confdir, globRebMarker)
	default:
		cmn.Assert(false)
	}
	return
}
