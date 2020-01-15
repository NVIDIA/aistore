// Package reb provides resilvering and rebalancing functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/filter"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/xaction"
	jsoniter "github.com/json-iterator/go"
)

const pkgName = "rebalance"

const (
	RebalanceStreamName = "rebalance"
	RebalanceAcksName   = "remwack" // NOTE: can become generic remote-write-acknowledgment
	RebalancePushName   = "rebpush" // push notifications stream
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
	rebStageAbort // one of targets aborts the rebalancing (never set, only sent)
)

type (
	syncCallback func(tsi *cluster.Snode, md *globArgs) (ok bool)
	joggerBase   struct {
		m     *Manager
		xreb  *xaction.RebBase
		mpath string
		wg    *sync.WaitGroup
	}

	Manager struct {
		t          cluster.Target
		statRunner *stats.Trunner
		filterGFN  *filter.Filter
		netd, netc string
		smap       atomic.Pointer // new smap which will be soon live
		streams    *transport.StreamBundle
		acks       *transport.StreamBundle
		pushes     *transport.StreamBundle
		lomacks    [cmn.MultiSyncMapCount]*LomAcks
		tcache     struct { // not to recompute very often
			tmap cluster.NodeMap
			ts   time.Time
			mu   *sync.Mutex
		}
		semaCh     chan struct{}
		beginStats atomic.Pointer // *stats.ExtRebalanceStats
		xreb       *xaction.GlobalReb
		ecReb      *ecRebalancer
		stageMtx   sync.Mutex
		nodeStages map[string]uint32
		globRebID  atomic.Int64
		stage      atomic.Uint32 // rebStage* enum
		laterx     atomic.Bool
	}
	LomAcks struct {
		mu *sync.Mutex
		q  map[string]*cluster.LOM // on the wire, waiting for ACK
	}
)

var stages = map[uint32]string{
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

func init() {
	if logLvl, ok := cmn.CheckDebug(pkgName); ok {
		glog.SetV(glog.SmoduleReb, logLvl)
	}
}

//
// Manager
//
func NewManager(t cluster.Target, config *cmn.Config, strunner *stats.Trunner) *Manager {
	netd, netc := cmn.NetworkPublic, cmn.NetworkPublic
	if config.Net.UseIntraData {
		netd = cmn.NetworkIntraData
	}
	if config.Net.UseIntraControl {
		netc = cmn.NetworkIntraControl
	}
	reb := &Manager{
		t:          t,
		filterGFN:  filter.NewDefaultFilter(),
		netd:       netd,
		netc:       netc,
		nodeStages: make(map[string]uint32),
		statRunner: strunner,
	}
	reb.ecReb = newECRebalancer(t, reb, strunner)
	reb.initStreams()
	return reb
}

func (reb *Manager) initStreams() {
	if _, err := transport.Register(reb.netd, RebalanceStreamName, reb.recvObj); err != nil {
		cmn.ExitLogf("%v", err)
	}
	if _, err := transport.Register(reb.netc, RebalanceAcksName, reb.recvAck); err != nil {
		cmn.ExitLogf("%v", err)
	}
	if _, err := transport.Register(reb.netc, RebalancePushName, reb.recvPush); err != nil {
		cmn.ExitLogf("%v", err)
	}
	if _, err := transport.Register(reb.netc, DataECRebStreamName, reb.onECData); err != nil {
		cmn.ExitLogf("%v", err)
	}
	// serialization: one at a time
	reb.semaCh = make(chan struct{}, 1)
	reb.semaCh <- struct{}{}
}

func (reb *Manager) GlobRebID() int64       { return reb.globRebID.Load() }
func (reb *Manager) FilterAdd(uname []byte) { reb.filterGFN.Insert(uname) }

func (reb *Manager) lomAcks() *[cmn.MultiSyncMapCount]*LomAcks { return &reb.lomacks }

func (reb *Manager) loghdr(globRebID int64, smap *cluster.Smap) string {
	var (
		tname = reb.t.Snode().Name()
		stage = stages[reb.stage.Load()]
	)
	return fmt.Sprintf("%s[g%d,v%d,%s]", tname, globRebID, smap.Version, stage)
}

func (reb *Manager) serialize(smap *cluster.Smap, config *cmn.Config, globRebID int64) (newerSmap, alreadyRunning bool) {
	var (
		ver    = smap.Version
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
		nver := reb.t.GetSowner().Get().Version
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
		entry := xaction.Registry.GetL(cmn.ActGlobalReb)
		if entry == nil {
			if canRun {
				return
			}
			glog.Warningf("%s: waiting for ???...", loghdr)
		} else {
			xact := entry.Get()
			otherXreb := xact.(*xaction.GlobalReb) // running or previous
			if canRun {
				cmn.Assert(otherXreb.Finished())
				return
			}
			if otherXreb.SmapVersion < ver && !otherXreb.Finished() {
				otherXreb.Abort()
				glog.Warningf("%s: aborting older Smap [%s]", loghdr, otherXreb)
			}
		}
		cmn.Assert(!canRun)
		time.Sleep(sleep)
	}
}

func (reb *Manager) getStats() (s *stats.ExtRebalanceStats) {
	s = &stats.ExtRebalanceStats{}
	statsRunner := reb.statRunner
	s.TxRebCount = statsRunner.Get(stats.TxRebCount)
	s.RxRebCount = statsRunner.Get(stats.RxRebCount)
	s.TxRebSize = statsRunner.Get(stats.TxRebSize)
	s.RxRebSize = statsRunner.Get(stats.RxRebSize)
	s.GlobalRebID = reb.globRebID.Load()
	return
}

func (reb *Manager) beginStreams(md *globArgs) {
	cmn.Assert(reb.stage.Load() == rebStageInit)
	if md.config.Rebalance.Multiplier == 0 {
		md.config.Rebalance.Multiplier = 1
	} else if md.config.Rebalance.Multiplier > 8 {
		glog.Errorf("%s: stream-and-mp-jogger multiplier=%d - misconfigured?",
			reb.t.Snode().Name(), md.config.Rebalance.Multiplier)
	}
	//
	// objects
	//
	client := transport.NewIntraDataClient()
	sbArgs := transport.SBArgs{
		Network: reb.netd,
		Trname:  RebalanceStreamName,
		Extra: &transport.Extra{
			Compression: md.config.Rebalance.Compression,
			Config:      md.config,
			Mem2:        reb.t.GetMem2()},
		Multiplier:   int(md.config.Rebalance.Multiplier),
		ManualResync: true,
	}
	reb.streams = transport.NewStreamBundle(reb.t.GetSowner(), reb.t.Snode(), client, sbArgs)

	//
	// ACKs
	//
	clientAcks := transport.NewIntraDataClient()
	sbArgs = transport.SBArgs{
		ManualResync: true,
		Network:      reb.netc,
		Trname:       RebalanceAcksName,
	}
	reb.acks = transport.NewStreamBundle(reb.t.GetSowner(), reb.t.Snode(), clientAcks, sbArgs)

	pushArgs := transport.SBArgs{
		Network: reb.netc,
		Trname:  RebalancePushName,
	}
	reb.pushes = transport.NewStreamBundle(reb.t.GetSowner(), reb.t.Snode(), client, pushArgs)

	// Init ecRebalancer streams
	if md.ecUsed {
		reb.ecReb.init(md, reb.netd)
	}
	reb.laterx.Store(false)
}

func (reb *Manager) endStreams() {
	if reb.stage.CAS(rebStageFin, rebStageFinStreams) { // TODO: must always succeed?
		reb.streams.Close(true /* graceful */)
		reb.streams = nil
		reb.acks.Close(true)
		reb.pushes.Close(true)
		reb.ecReb.endStreams()
	}
}

func (reb *Manager) recvObj(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
	if err != nil {
		glog.Error(err)
		return
	}
	smap := (*cluster.Smap)(reb.smap.Load())
	if smap == nil {
		var (
			config = cmn.GCO.Get()
			sleep  = config.Timeout.CplaneOperation
			maxwt  = config.Rebalance.DestRetryTime
			curwt  time.Duration
		)
		maxwt = cmn.MinDur(maxwt, config.Timeout.SendFile/3)
		glog.Warningf("%s: waiting to start...", reb.t.Snode().Name())
		time.Sleep(sleep)
		for curwt < maxwt {
			smap = (*cluster.Smap)(reb.smap.Load())
			if smap != nil {
				break
			}
			time.Sleep(sleep)
			curwt += sleep
		}
		if curwt >= maxwt {
			glog.Errorf("%s: timed-out waiting to start, dropping %s/%s/%s", reb.t.Snode().Name(), hdr.Provider, hdr.Bucket, hdr.ObjName)
			return
		}
	}
	var (
		tsid = string(hdr.Opaque) // the sender
		tsi  = smap.GetTarget(tsid)
	)
	// Rx
	lom := &cluster.LOM{T: reb.t, Objname: hdr.ObjName}
	if err = lom.Init(hdr.Bucket, hdr.Provider); err != nil {
		glog.Error(err)
		return
	}
	aborted, running := xaction.Registry.IsRebalancing(cmn.ActGlobalReb)
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
		f("%s: late receive from %s %s (stage %s)", reb.t.Snode().Name(), tsid, lom, stages[stage])
	} else if stage < rebStageTraverse {
		glog.Errorf("%s: early receive from %s %s (stage %s)", reb.t.Snode().Name(), tsid, lom, stages[stage])
	}
	lom.SetAtimeUnix(hdr.ObjAttrs.Atime)
	lom.SetVersion(hdr.ObjAttrs.Version)

	if err := reb.t.PutObject(
		fs.CSM.GenContentParsedFQN(lom.ParsedFQN, fs.WorkfileType, fs.WorkfilePut),
		ioutil.NopCloser(objReader),
		lom,
		cluster.Migrated,
		cmn.NewCksum(hdr.ObjAttrs.CksumType, hdr.ObjAttrs.CksumValue),
		time.Now(),
	); err != nil {
		glog.Error(err)
		return
	}

	if glog.FastV(5, glog.SmoduleReb) {
		glog.Infof("%s: from %s %s", reb.t.Snode().Name(), tsid, lom)
	}
	reb.statRunner.AddMany(
		stats.NamedVal64{Name: stats.RxRebCount, Value: 1},
		stats.NamedVal64{Name: stats.RxRebSize, Value: hdr.ObjAttrs.Size},
	)
	// ACK
	if tsi == nil {
		return
	}
	if stage := reb.stage.Load(); stage < rebStageFinStreams && stage != rebStageInactive {
		hdr.Opaque = []byte(reb.t.Snode().DaemonID) // self == src
		hdr.ObjAttrs.Size = 0
		if err := reb.acks.Send(transport.Obj{Hdr: hdr}, nil, tsi); err != nil {
			glog.Error(err) // TODO: collapse same-type errors e.g. "src-id=>network: destination mismatch"
		}
	}
}

// Mark a 'node' that it has passed the 'stage'
func (reb *Manager) setStage(node string, stage uint32) {
	reb.stageMtx.Lock()
	reb.nodeStages[node] = stage
	reb.stageMtx.Unlock()
}

// Rebalance moves to the next stage:
// - update internal stage
// - send notification to all other targets that this one is in a new stage
func (reb *Manager) changeStage(newStage uint32) {
	// first, set stage
	reb.setStage(reb.t.Snode().DaemonID, newStage)

	req := pushReq{DaemonID: reb.t.Snode().DaemonID, Stage: newStage, RebID: reb.globRebID.Load()}
	hdr := transport.Header{
		ObjAttrs: transport.ObjectAttrs{Size: 0},
		Opaque:   cmn.MustMarshal(req),
	}
	// second, notify all
	if err := reb.pushes.Send(transport.Obj{Hdr: hdr}, nil); err != nil {
		glog.Warningf("Failed to broadcast ack %s: %v", stages[newStage], err)
	}
}

// returns true if the node is in or passed `stage`
func (reb *Manager) isNodeInStage(si *cluster.Snode, stage uint32) bool {
	reb.stageMtx.Lock()
	nodeStage, ok := reb.nodeStages[si.DaemonID]
	reb.stageMtx.Unlock()
	return ok && nodeStage >= stage
}

func (reb *Manager) recvPush(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
	if err != nil {
		glog.Errorf("Failed to get notification %s from %s[%s]: %v", hdr.ObjName, hdr.Bucket, hdr.Provider, err)
		return
	}

	var req pushReq
	if err := jsoniter.Unmarshal(hdr.Opaque, &req); err != nil {
		glog.Errorf("Invalid push notification: %v", err)
		return
	}

	if req.Stage == rebStageAbort {
		// a target aborted its xaction and sent the signal to others
		glog.Warningf("Got rebalance %d abort notification from %s (local RebID: %d)",
			req.RebID, req.DaemonID, reb.GlobRebID())
		if reb.xreb != nil && reb.GlobRebID() == req.RebID {
			reb.xreb.Abort()
		}
		return
	}

	// a target was too late in sending(rebID is obsolete) its data or too early (md == nil)
	if req.RebID < reb.globRebID.Load() {
		// TODO: warning
		glog.Errorf("Rebalance IDs mismatch: %d vs %d. %s is late(%s)?", reb.globRebID.Load(), req.RebID, req.DaemonID, stages[req.Stage])
		return
	}

	if req.Stage == rebStageECBatch {
		if glog.FastV(4, glog.SmoduleReb) {
			glog.Infof("%s Target %s finished batch %d", reb.t.Snode().Name(), req.DaemonID, req.Batch)
		}
		reb.ecReb.batchDone(req.DaemonID, req.Batch)
		return
	}

	reb.setStage(req.DaemonID, req.Stage)
}

func (reb *Manager) recvECAck(hdr transport.Header) {
	opaque := string(hdr.Opaque)
	uid := ackID(hdr.Bucket, hdr.Provider, hdr.ObjName, string(hdr.Opaque))
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("%s: EC ack from %s on %s/%s/%s", reb.t.Snode().Name(), opaque, hdr.Provider, hdr.Bucket, hdr.ObjName)
	}
	reb.ecReb.ackCTs.mtx.Lock()
	delete(reb.ecReb.ackCTs.ct, uid)
	reb.ecReb.ackCTs.mtx.Unlock()
}

func (reb *Manager) recvAck(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
	if err != nil {
		glog.Error(err)
		return
	}
	// regular rebalance sends "daemonID" as opaque
	// EC rebalance sends "@sliceID/daemonID" as opaque
	opaque := string(hdr.Opaque)
	if strings.HasPrefix(opaque, "@") {
		reb.recvECAck(hdr)
		return
	}

	lom := &cluster.LOM{T: reb.t, Objname: hdr.ObjName}
	if err = lom.Init(hdr.Bucket, hdr.Provider); err != nil {
		glog.Error(err)
		return
	}
	if glog.FastV(5, glog.SmoduleReb) {
		glog.Infof("%s: ack from %s on %s", reb.t.Snode().Name(), opaque, lom)
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
		glog.Errorf("%s: error removing %s, err: %v", reb.t.Snode().Name(), lom, err)
	}
	lom.Unlock(true)
}

func (reb *Manager) retransmit(xreb *xaction.GlobalReb, globRebID int64) (cnt int) {
	smap := (*cluster.Smap)(reb.smap.Load())
	aborted := func() (yes bool) {
		yes = xreb.Aborted()
		yes = yes || (smap.Version != reb.t.GetSowner().Get().Version)
		return
	}
	if aborted() {
		return
	}
	var (
		rj    = &globalJogger{joggerBase: joggerBase{m: reb, xreb: &xreb.RebBase, wg: &sync.WaitGroup{}}, smap: smap}
		query = url.Values{}
	)
	query.Add(cmn.URLParamSilent, "true")
	for _, lomack := range reb.lomAcks() {
		lomack.mu.Lock()
		for uname, lom := range lomack.q {
			if err := lom.Load(false); err != nil {
				if cmn.IsNotObjExist(err) {
					glog.Warningf("%s: %s %s", reb.loghdr(globRebID, smap), lom, cmn.DoesNotExist)
				} else {
					glog.Errorf("%s: failed loading %s, err: %s", reb.loghdr(globRebID, smap), lom, err)
				}
				delete(lomack.q, uname)
				continue
			}
			tsi, _ := cluster.HrwTarget(lom.Uname(), smap)
			if reb.t.LookupRemoteSingle(lom, tsi) {
				if glog.FastV(4, glog.SmoduleReb) {
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

// Aborts local global rebalance xaction and notifies all other targets
// that they has to abort rebalance as well.
// Useful for EC rebalance: after each batch EC rebalance waits in a loop
// for all targets to finish their batches. No stream interactions in this loop,
// except listening to push notifications. So, if any target stops its xaction
// and closes all its streams, others wouldn't notice that. That is why the
// target should send notification.
func (reb *Manager) abortGlobal() {
	if reb.xreb == nil || reb.xreb.Aborted() || reb.xreb.Finished() {
		return
	}
	glog.Info("Aborting global rebalance...")
	reb.xreb.Abort()
	req := pushReq{
		DaemonID: reb.t.Snode().DaemonID,
		RebID:    reb.GlobRebID(),
		Stage:    rebStageAbort,
	}
	hdr := transport.Header{
		ObjAttrs: transport.ObjectAttrs{Size: 0},
		Opaque:   cmn.MustMarshal(req),
	}
	if err := reb.pushes.Send(transport.Obj{Hdr: hdr}, nil); err != nil {
		glog.Errorf("Failed to broadcast abort notification: %v", err)
	}
}
