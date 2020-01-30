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
	"runtime"
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
	rebStageECNamespace  // local CT list built
	rebStageECDetect     // all lists are received, start detecting which objects to fix
	rebStageECGlobRepair // all local CTs are fine, targets start inter-cluster rebalance
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
		stages     *nodeStages
		ec         *ecData
		globRebID  atomic.Int64
		laterx     atomic.Bool
		inQueue    atomic.Int64
	}
	// Stage status of a single target
	stageStatus struct {
		batchID int64  // current batch ID (0 for non-EC stages)
		stage   uint32 // current stage
	}
	nodeStages struct {
		// Info about remote targets. It needs mutex for it can be
		// updated from different goroutines
		mtx     sync.Mutex
		targets map[string]*stageStatus // daemonID <-> stageStatus
		// Info about this target rebalance status. This info is used oftener
		// than remote target ones, and updated more frequently locally.
		// That is why it uses atomics instead of global mutex
		currBatch atomic.Int64  // EC rebalance: current batch ID
		lastBatch atomic.Int64  // EC rebalance: ID of the last batch
		stage     atomic.Uint32 // rebStage* enum: this target current stage
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
	rebStageECNamespace:  "<namespace>",
	rebStageECDetect:     "<build-fix-list>",
	rebStageECGlobRepair: "<ec-transfer>",
	rebStageECCleanup:    "<ec-fin>",
	rebStageECBatch:      "<ec-batch>",
	rebStageAbort:        "<abort>",
}

func init() {
	if logLvl, ok := cmn.CheckDebug(pkgName); ok {
		glog.SetV(glog.SmoduleReb, logLvl)
	}
}

//
// nodeStages
//

func newNodeStages() *nodeStages {
	return &nodeStages{targets: make(map[string]*stageStatus)}
}

// Returns true if the target is in `newStage` or in any next stage
func (ns *nodeStages) stageReached(status *stageStatus, newStage uint32, newBatchID int64) bool {
	// for simple stages: just check the stage
	if newBatchID == 0 {
		return status.stage >= newStage
	}
	// for cyclic stage (used in EC): check both batch ID and stage
	return status.batchID > newBatchID ||
		(status.batchID == newBatchID && status.stage >= newStage) ||
		(status.stage >= rebStageECCleanup && status.stage > newStage)
}

// Mark a 'node' that it has reached the 'stage'. Do nothing if the target
// is already in this stage or has finished it already
func (ns *nodeStages) setStage(daemonID string, stage uint32, batchID int64) {
	ns.mtx.Lock()
	status, ok := ns.targets[daemonID]
	if !ok {
		status = &stageStatus{}
		ns.targets[daemonID] = status
	}

	if !ns.stageReached(status, stage, batchID) {
		status.stage = stage
		status.batchID = batchID
	}
	ns.mtx.Unlock()
}

// Returns true if the target is in `newStage` or in any next stage.
func (ns *nodeStages) isInStage(si *cluster.Snode, stage uint32) bool {
	ns.mtx.Lock()
	inStage := ns.isInStageBatchUnlocked(si, stage, 0)
	ns.mtx.Unlock()
	return inStage
}

// Returns true if the target is in `newStage` and has reached the given
// batch ID or it is in any next stage
func (ns *nodeStages) isInStageBatchUnlocked(si *cluster.Snode, stage uint32, batchID int64) bool {
	status, ok := ns.targets[si.ID()]
	if !ok {
		return false
	}
	return ns.stageReached(status, stage, batchID)
}

func (ns *nodeStages) cleanup() {
	ns.mtx.Lock()
	for k := range ns.targets {
		delete(ns.targets, k)
	}
	ns.mtx.Unlock()
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
		statRunner: strunner,
		stages:     newNodeStages(),
	}
	reb.ec = newECData(t)
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
		stage = stages[reb.stages.stage.Load()]
	)
	return fmt.Sprintf("%s[g%d,v%d,%s]", tname, globRebID, smap.Version, stage)
}
func (reb *Manager) rebIDMismatchMsg(remoteID int64) string {
	return fmt.Sprintf("rebalance IDs mismatch: local %d, remote %d", reb.GlobRebID(), remoteID)
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
			runtime.Gosched()
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
	cmn.Assert(reb.stages.stage.Load() == rebStageInit)
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
		reb.initEC(md, reb.netd)
	}
	reb.laterx.Store(false)
	reb.inQueue.Store(0)
}

func (reb *Manager) endStreams() {
	if reb.stages.stage.CAS(rebStageFin, rebStageFinStreams) { // TODO: must always succeed?
		reb.streams.Close(true /* graceful */)
		reb.streams = nil
		reb.acks.Close(true)
		reb.pushes.Close(true)
		reb.endECStreams()
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
			glog.Errorf("%s: timed-out waiting to start, dropping %s/%s", reb.t.Snode().Name(), hdr.Bck, hdr.ObjName)
			return
		}
	}

	ack := &regularAck{}
	unpacker := cmn.NewUnpacker(hdr.Opaque)
	act, err := unpacker.ReadByte()
	if err != nil {
		glog.Errorf("Failed to read message type: %v", err)
		return
	}
	if act != rebMsgAckRegular {
		glog.Errorf("Invalid ACK type %d, expected %d", act, rebMsgAckRegular)
	}

	if err = unpacker.ReadAny(ack); err != nil {
		glog.Errorf("Failed to parse acknowledge: %v", err)
		return
	}

	if ack.globRebID != reb.GlobRebID() {
		glog.Warningf("Received object %s/%s: %s", hdr.Bck, hdr.ObjName, reb.rebIDMismatchMsg(ack.globRebID))
		io.Copy(ioutil.Discard, objReader) // drain the reader
		return
	}
	tsid := ack.daemonID // the sender
	// Rx
	lom := &cluster.LOM{T: reb.t, Objname: hdr.ObjName}
	if err = lom.Init(hdr.Bck); err != nil {
		glog.Error(err)
		return
	}
	aborted, running := xaction.Registry.IsRebalancing(cmn.ActGlobalReb)
	if aborted || !running {
		io.Copy(ioutil.Discard, objReader) // drain the reader
		return
	}

	if stage := reb.stages.stage.Load(); stage >= rebStageFin {
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
	tsi := smap.GetTarget(tsid)
	if tsi == nil {
		glog.Errorf("%s target is not found in smap", tsid)
		return
	}
	if stage := reb.stages.stage.Load(); stage < rebStageFinStreams && stage != rebStageInactive {
		ack := &regularAck{globRebID: reb.GlobRebID(), daemonID: reb.t.Snode().ID()}
		packer := cmn.NewPacker(rebMsgKindSize + ack.PackedSize())
		packer.WriteByte(rebMsgAckRegular)
		packer.WriteAny(ack)
		hdr.Opaque = packer.Bytes()
		hdr.ObjAttrs.Size = 0
		if err := reb.acks.Send(transport.Obj{Hdr: hdr}, nil, tsi); err != nil {
			glog.Error(err) // TODO: collapse same-type errors e.g. "src-id=>network: destination mismatch"
		}
	}
}

// Rebalance moves to the next stage:
// - update internal stage
// - send notification to all other targets that this one is in a new stage
func (reb *Manager) changeStage(newStage uint32, batchID int64) {
	// first, set own stage
	reb.stages.stage.Store(newStage)

	req := pushReq{
		daemonID: reb.t.Snode().DaemonID, stage: newStage,
		rebID: reb.globRebID.Load(), batch: int(batchID),
	}
	hdr := transport.Header{
		ObjAttrs: transport.ObjectAttrs{Size: 0},
		Opaque:   reb.encodePushReq(&req),
	}
	// second, notify all
	if err := reb.pushes.Send(transport.Obj{Hdr: hdr}, nil); err != nil {
		glog.Warningf("Failed to broadcast ack %s: %v", stages[newStage], err)
	}
}

func (reb *Manager) recvPush(w http.ResponseWriter, hdr transport.Header, objReader io.Reader, err error) {
	if err != nil {
		glog.Errorf("Failed to get notification %s from %s: %v", hdr.ObjName, hdr.Bck, err)
		return
	}

	req, err := reb.decodePushReq(hdr.Opaque)
	if err != nil {
		glog.Error(err)
		return
	}

	if reb.GlobRebID() != req.rebID {
		glog.Warningf("Stage %v push notification: %s", stages[req.stage], reb.rebIDMismatchMsg(req.rebID))
		return
	}

	if req.stage == rebStageAbort {
		// a target aborted its xaction and sent the signal to others
		glog.Warningf("Rebalance abort notification from %s", req.daemonID)
		if reb.xreb != nil {
			reb.xreb.Abort()
		}
		return
	}

	if req.stage == rebStageECBatch {
		if glog.FastV(4, glog.SmoduleReb) {
			glog.Infof("%s Target %s finished batch %d", reb.t.Snode().Name(), req.daemonID, req.batch)
		}
	}

	reb.stages.setStage(req.daemonID, req.stage, int64(req.batch))
}

func (reb *Manager) recvECAck(hdr transport.Header, unpacker *cmn.ByteUnpack) {
	ack := &ecAck{}
	if err := unpacker.ReadAny(ack); err != nil {
		glog.Errorf("Failed to unmarshal EC ACK for %s/%s: %v", hdr.Bck, hdr.ObjName, err)
		return
	}

	cid := ctUID(int(ack.sliceID), ack.daemonID)
	uid := ackID(hdr.Bck, hdr.ObjName, cid)
	if glog.FastV(5, glog.SmoduleReb) {
		glog.Infof(
			"%s: EC ack from %s on %s/%s",
			reb.t.Snode().Name(), cid, hdr.Bck, hdr.ObjName,
		)
	}
	reb.ec.ackCTs.mtx.Lock()
	delete(reb.ec.ackCTs.ct, uid)
	reb.ec.ackCTs.mtx.Unlock()
}

func (reb *Manager) recvAck(w http.ResponseWriter, hdr transport.Header, _ io.Reader, err error) {
	if err != nil {
		glog.Error(err)
		return
	}

	unpacker := cmn.NewUnpacker(hdr.Opaque)
	act, err := unpacker.ReadByte()
	if err != nil {
		glog.Errorf("Failed to read message type: %v", err)
		return
	}

	if act == rebMsgAckEC {
		reb.recvECAck(hdr, unpacker)
		return
	}
	if act != rebMsgAckRegular {
		glog.Errorf("Invalid ACK type %d, expected %d", act, rebMsgAckRegular)
	}

	ack := &regularAck{}
	if err = unpacker.ReadAny(ack); err != nil {
		glog.Errorf("Failed to parse acknowledge: %v", err)
		return
	}
	if ack.globRebID != reb.globRebID.Load() {
		glog.Warningf("ACK from %s: %s", ack.daemonID, reb.rebIDMismatchMsg(ack.globRebID))
		return
	}

	lom := &cluster.LOM{T: reb.t, Objname: hdr.ObjName}
	if err = lom.Init(hdr.Bck); err != nil {
		glog.Error(err)
		return
	}
	if glog.FastV(5, glog.SmoduleReb) {
		glog.Infof("%s: ack from %s on %s", reb.t.Snode().Name(), string(hdr.Opaque), lom)
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
		daemonID: reb.t.Snode().DaemonID,
		rebID:    reb.GlobRebID(),
		stage:    rebStageAbort,
	}
	hdr := transport.Header{
		ObjAttrs: transport.ObjectAttrs{Size: 0},
		Opaque:   reb.encodePushReq(&req),
	}
	if err := reb.pushes.Send(transport.Obj{Hdr: hdr}, nil); err != nil {
		glog.Errorf("Failed to broadcast abort notification: %v", err)
	}
}

// Returns if the target is quiescent: transport queue is empty, or xaction
// has already aborted or finished
func (reb *Manager) isQuiescent() bool {
	// Finished or aborted xaction = no traffic
	if reb.xreb == nil || reb.xreb.Aborted() || reb.xreb.Finished() {
		return true
	}
	// Has not finished the stage that generates network traffic yet
	if reb.stages.stage.Load() < rebStageECBatch {
		return false
	}
	// Check for both regular and EC transport queues are empty
	return reb.inQueue.Load() == 0 && reb.ec.onAir.Load() == 0
}
