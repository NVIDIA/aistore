// Package reb provides local resilver and global rebalance for AIStore.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/filter"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xreg"
	"github.com/NVIDIA/aistore/xs"
)

const (
	rebTrname     = "rebalance"
	rebPushTrname = "rebpush" // broadcast push notifications
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
	rebStageAbort // one of targets aborts the rebalancing (never set, only sent)
)

const maxWackTargets = 4

type (
	syncCallback func(tsi *cluster.Snode, md *rebArgs) (ok bool)
	joggerBase   struct {
		m    *Manager
		xreb *xs.RebBase
		wg   *sync.WaitGroup
	}

	Manager struct {
		sync.RWMutex
		t           cluster.Target
		dm          *bundle.DataMover
		pushes      *bundle.Streams // broadcast notifications
		statTracker stats.Tracker
		filterGFN   *filter.Filter
		smap        atomic.Pointer // new smap which will be soon live
		lomacks     [cos.MultiSyncMapCount]*lomAcks
		awaiting    struct {
			mu      sync.Mutex
			targets cluster.Nodes // targets for which we are waiting for
			ts      int64         // last time we have recomputed
		}
		semaCh     *cos.Semaphore
		beginStats atomic.Pointer // *stats.ExtRebalanceStats
		xreb       atomic.Pointer // *xaction.Rebalance
		stages     *nodeStages
		ecClient   *http.Client
		rebID      atomic.Int64
		inQueue    atomic.Int64
		onAir      atomic.Int64
		laterx     atomic.Bool
	}
	lomAcks struct {
		mu *sync.Mutex
		q  map[string]*cluster.LOM // on the wire, waiting for ACK
	}
)

var stages = map[uint32]string{
	rebStageInactive:   "<inactive>",
	rebStageInit:       "<init>",
	rebStageTraverse:   "<traverse>",
	rebStageWaitAck:    "<wack>",
	rebStageFin:        "<fin>",
	rebStageFinStreams: "<fin-streams>",
	rebStageDone:       "<done>",
	rebStageAbort:      "<abort>",
}

//////////////////////////////////////////////
// rebalance manager: init, receive, common //
//////////////////////////////////////////////

func NewManager(t cluster.Target, config *cmn.Config, st stats.Tracker) *Manager {
	ecClient := cmn.NewClient(cmn.TransportArgs{
		Timeout:    config.Client.Timeout.D(),
		UseHTTPS:   config.Net.HTTP.UseHTTPS,
		SkipVerify: config.Net.HTTP.SkipVerify,
	})
	reb := &Manager{
		t:           t,
		filterGFN:   filter.NewDefaultFilter(),
		statTracker: st,
		stages:      newNodeStages(),
		ecClient:    ecClient,
	}
	rebcfg := &config.Rebalance
	dmExtra := bundle.Extra{
		RecvAck:     reb.recvAck,
		Compression: rebcfg.Compression,
		Multiplier:  int(rebcfg.Multiplier),
	}
	dm, err := bundle.NewDataMover(t, rebTrname, reb.recvObj, cluster.Migrated, dmExtra)
	if err != nil {
		cos.ExitLogf("%v", err)
	}
	reb.dm = dm
	reb.registerRecv()
	return reb
}

// NOTE: these receive handlers are statically present throughout: unreg never done
func (reb *Manager) registerRecv() {
	if err := reb.dm.RegRecv(); err != nil {
		cos.ExitLogf("%v", err)
	}
	if err := transport.HandleObjStream(rebPushTrname, reb.recvPush); err != nil {
		cos.ExitLogf("%v", err)
	}
	// serialization: one at a time
	reb.semaCh = cos.NewSemaphore(1)
}

func (reb *Manager) RebID() int64                              { return reb.rebID.Load() }
func (reb *Manager) FilterAdd(uname []byte)                    { reb.filterGFN.Insert(uname) }
func (reb *Manager) xact() *xs.Rebalance                       { return (*xs.Rebalance)(reb.xreb.Load()) }
func (reb *Manager) setXact(xact *xs.Rebalance)                { reb.xreb.Store(unsafe.Pointer(xact)) }
func (reb *Manager) lomAcks() *[cos.MultiSyncMapCount]*lomAcks { return &reb.lomacks }

// TODO: lomAck.q[lom.Uname()] = lom.Bprops().BID and, subsequently, LIF => LOM reinit
func (reb *Manager) addLomAck(lom *cluster.LOM) {
	_, idx := lom.Hkey()
	lomAck := reb.lomAcks()[idx]
	lomAck.mu.Lock()
	lomAck.q[lom.Uname()] = lom
	lomAck.mu.Unlock()
}

func (reb *Manager) delLomAck(lom *cluster.LOM) {
	_, idx := lom.Hkey()
	lomAck := reb.lomAcks()[idx]
	lomAck.mu.Lock()
	if lomOrig, ok := lomAck.q[lom.Uname()]; ok {
		delete(lomAck.q, lom.Uname())
		cluster.FreeLOM(lomOrig) // NOTE: free the original (pending) LOM
	}
	lomAck.mu.Unlock()
}

func (reb *Manager) logHdr(md *rebArgs) string {
	stage := stages[reb.stages.stage.Load()]
	return fmt.Sprintf("%s[g%d,v%d,%s]", reb.t.Snode(), md.id, md.smap.Version, stage)
}

func (reb *Manager) rebIDMismatchMsg(remoteID int64) string {
	return fmt.Sprintf("rebalance IDs mismatch: local %d, remote %d", reb.RebID(), remoteID)
}

func (reb *Manager) getStats() (s *stats.ExtRebalanceStats) {
	s = &stats.ExtRebalanceStats{}
	statsRunner := reb.statTracker
	s.RebTxCount = statsRunner.Get(stats.RebTxCount)
	s.RebTxSize = statsRunner.Get(stats.RebTxSize)
	s.RebRxCount = statsRunner.Get(stats.RebRxCount)
	s.RebRxSize = statsRunner.Get(stats.RebRxSize)
	s.RebID = reb.rebID.Load()
	return
}

func (reb *Manager) beginStreams() {
	debug.Assert(reb.stages.stage.Load() == rebStageInit)

	reb.dm.Open()
	pushArgs := bundle.Args{Net: reb.dm.NetC(), Trname: rebPushTrname}
	reb.pushes = bundle.NewStreams(reb.t.Sowner(), reb.t.Snode(), transport.NewIntraDataClient(), pushArgs)

	reb.laterx.Store(false)
	reb.inQueue.Store(0)
}

func (reb *Manager) endStreams(err error) {
	if reb.stages.stage.CAS(rebStageFin, rebStageFinStreams) {
		reb.dm.Close(err)
		reb.pushes.Close(true)
	}
}

func (reb *Manager) recvObjRegular(hdr transport.ObjHdr, smap *cluster.Smap, unpacker *cos.ByteUnpack, objReader io.Reader) {
	defer cos.DrainReader(objReader)

	ack := &regularAck{}
	if err := unpacker.ReadAny(ack); err != nil {
		glog.Errorf("Failed to parse acknowledgement: %v", err)
		return
	}

	if ack.rebID != reb.RebID() {
		glog.Warningf("received %s: %s", hdr.FullName(), reb.rebIDMismatchMsg(ack.rebID))
		return
	}
	tsid := ack.daemonID // the sender
	// Rx
	lom := cluster.AllocLOM(hdr.ObjName)
	defer cluster.FreeLOM(lom)
	if err := lom.Init(hdr.Bck); err != nil {
		glog.Error(err)
		return
	}
	marked := xreg.GetRebMarked()
	if marked.Interrupted || marked.Xact == nil {
		return
	}

	if stage := reb.stages.stage.Load(); stage >= rebStageFin {
		reb.laterx.Store(true)
		f := glog.Warningf
		if stage > rebStageFin {
			f = glog.Errorf
		}
		f("%s: late receive from %s %s (stage %s)", reb.t.Snode(), tsid, lom, stages[stage])
	} else if stage < rebStageTraverse {
		glog.Errorf("%s: early receive from %s %s (stage %s)", reb.t.Snode(), tsid, lom, stages[stage])
	}

	lom.CopyAttrs(&hdr.ObjAttrs, true /*skip-checksum*/) // see "PUT is a no-op"
	params := cluster.PutObjectParams{
		Tag:      fs.WorkfilePut,
		Reader:   io.NopCloser(objReader),
		RecvType: cluster.Migrated,
		Cksum:    hdr.ObjAttrs.Cksum,
		Started:  time.Now(),
	}
	if err := reb.t.PutObject(lom, params); err != nil {
		glog.Error(err)
		return
	}

	if glog.FastV(5, glog.SmoduleReb) {
		glog.Infof("%s: from %s %s", reb.t.Snode(), tsid, lom)
	}
	reb.statTracker.AddMany(
		stats.NamedVal64{Name: stats.RebRxCount, Value: 1},
		stats.NamedVal64{Name: stats.RebRxSize, Value: hdr.ObjAttrs.Size},
	)
	// ACK
	tsi := smap.GetTarget(tsid)
	if tsi == nil {
		glog.Errorf("%s target is not found in smap", tsid)
		return
	}
	if stage := reb.stages.stage.Load(); stage < rebStageFinStreams && stage != rebStageInactive {
		ack := &regularAck{rebID: reb.RebID(), daemonID: reb.t.SID()}
		hdr.Opaque = ack.NewPack()
		hdr.ObjAttrs.Size = 0
		if err := reb.dm.ACK(hdr, nil, tsi); err != nil {
			glog.Error(err) // TODO: collapse same-type errors e.g. "src-id=>network: destination mismatch"
		}
	}
}

func (reb *Manager) waitForSmap() (*cluster.Smap, error) {
	smap := (*cluster.Smap)(reb.smap.Load())
	if smap == nil {
		var (
			config = cmn.GCO.Get()
			sleep  = config.Timeout.CplaneOperation.D()
			maxwt  = config.Rebalance.DestRetryTime.D()
			curwt  time.Duration
		)
		maxwt = cos.MinDuration(maxwt, config.Timeout.SendFile.D()/3)
		glog.Warningf("%s: waiting to start...", reb.t.Snode())
		time.Sleep(sleep)
		for curwt < maxwt {
			smap = (*cluster.Smap)(reb.smap.Load())
			if smap != nil {
				return smap, nil
			}
			time.Sleep(sleep)
			curwt += sleep
		}
		if curwt >= maxwt {
			err := fmt.Errorf("%s: timed-out waiting to start", reb.t.Snode())
			return nil, err
		}
	}
	return smap, nil
}

func (reb *Manager) recvObj(hdr transport.ObjHdr, objReader io.Reader, err error) {
	defer transport.FreeRecv(objReader)
	if err != nil {
		glog.Error(err)
		return
	}
	smap, err := reb.waitForSmap()
	if err != nil {
		glog.Errorf("%v: dropping %s", err, hdr.FullName())
		return
	}

	unpacker := cos.NewUnpacker(hdr.Opaque)
	act, err := unpacker.ReadByte()
	if err != nil {
		glog.Errorf("Failed to read message type: %v", err)
		return
	}

	if act == rebMsgRegular {
		reb.recvObjRegular(hdr, smap, unpacker, objReader)
		return
	}

	if act != rebMsgEC {
		glog.Errorf("Invalid ACK type %d, expected %d", act, rebMsgEC)
	}

	reb.recvECData(hdr, unpacker, objReader)
}

// Rebalance moves to the next stage:
// - update internal stage
// - send notification to all other targets that this one is in a new stage
func (reb *Manager) changeStage(newStage uint32) {
	// first, set own stage
	reb.stages.stage.Store(newStage)
	var (
		req = pushReq{
			daemonID: reb.t.SID(), stage: newStage, rebID: reb.rebID.Load(),
		}
		hdr = transport.ObjHdr{}
	)
	hdr.Opaque = reb.encodePushReq(&req)
	// second, notify all
	if err := reb.pushes.Send(&transport.Obj{Hdr: hdr}, nil); err != nil {
		glog.Warningf("Failed to broadcast ack %s: %v", stages[newStage], err)
	}
}

func (reb *Manager) recvPush(hdr transport.ObjHdr, _ io.Reader, err error) {
	if err != nil {
		glog.Errorf("Failed to get notification %s from %s: %v", hdr.ObjName, hdr.Bck, err)
		return
	}

	req, err := reb.decodePushReq(hdr.Opaque)
	if err != nil {
		glog.Error(err)
		return
	}

	if req.stage == rebStageAbort && reb.RebID() <= req.rebID {
		// a target aborted its xaction and sent the signal to others
		glog.Warningf("Rebalance abort notification from %s", req.daemonID)
		if reb.xact() != nil {
			reb.xact().Abort()
		}
		return
	}

	if reb.RebID() != req.rebID {
		glog.Warningf("Stage %v push notification: %s", stages[req.stage], reb.rebIDMismatchMsg(req.rebID))
		return
	}

	reb.stages.setStage(req.daemonID, req.stage)
}

func (*Manager) recvECAck(hdr transport.ObjHdr, unpacker *cos.ByteUnpack) {
	ack := &ecAck{}
	if err := unpacker.ReadAny(ack); err != nil {
		glog.Errorf("Failed to unmarshal EC ACK for %s: %v", hdr.FullName(), err)
		return
	}
}

func (reb *Manager) recvRegularAck(hdr transport.ObjHdr, unpacker *cos.ByteUnpack) {
	ack := &regularAck{}
	if err := unpacker.ReadAny(ack); err != nil {
		glog.Errorf("Failed to parse acknowledge: %v", err)
		return
	}
	if ack.rebID != reb.rebID.Load() {
		glog.Warningf("ACK from %s: %s", ack.daemonID, reb.rebIDMismatchMsg(ack.rebID))
		return
	}

	lom := cluster.AllocLOM(hdr.ObjName)
	defer cluster.FreeLOM(lom)
	if err := lom.Init(hdr.Bck); err != nil {
		glog.Error(err)
		return
	}
	if glog.FastV(5, glog.SmoduleReb) {
		glog.Infof("%s: ack from %s on %s", reb.t.Snode(), string(hdr.Opaque), lom)
	}
	// No immediate file deletion: let LRU cleanup the "misplaced" object
	// TODO: mark the object "Deleted"
	reb.delLomAck(lom)
}

func (reb *Manager) recvAck(hdr transport.ObjHdr, _ io.Reader, err error) {
	if err != nil {
		glog.Error(err)
		return
	}

	unpacker := cos.NewUnpacker(hdr.Opaque)
	act, err := unpacker.ReadByte()
	if err != nil {
		glog.Errorf("failed to read message type: %v", err)
		return
	}

	if act == rebMsgEC {
		reb.recvECAck(hdr, unpacker)
		return
	}
	if act != rebMsgRegular {
		glog.Errorf("Invalid ACK type %d, expected %d", act, rebMsgRegular)
	}

	reb.recvRegularAck(hdr, unpacker)
}

func (reb *Manager) retransmit(md *rebArgs) (cnt int) {
	aborted := func() (yes bool) {
		yes = reb.xact().Aborted()
		yes = yes || (md.smap.Version != reb.t.Sowner().Get().Version)
		return
	}
	if aborted() {
		return
	}
	var (
		rj = &rebJogger{joggerBase: joggerBase{
			m: reb, xreb: &reb.xact().RebBase,
			wg: &sync.WaitGroup{},
		}, smap: md.smap}
		query  = url.Values{}
		loghdr = reb.logHdr(md)
	)
	query.Add(cmn.URLParamSilent, "true")
	for _, lomAck := range reb.lomAcks() {
		lomAck.mu.Lock()
		for uname, lom := range lomAck.q {
			if err := lom.Load(false /*cache it*/, false /*locked*/); err != nil {
				if cmn.IsObjNotExist(err) {
					glog.Warningf("%s: object not found (lom: %s, err: %v)", loghdr, lom, err)
				} else {
					glog.Errorf("%s: failed loading %s, err: %s", loghdr, lom, err)
				}
				delete(lomAck.q, uname)
				continue
			}
			tsi, _ := cluster.HrwTarget(lom.Uname(), md.smap)
			if reb.t.LookupRemoteSingle(lom, tsi) {
				if glog.FastV(4, glog.SmoduleReb) {
					glog.Infof("%s: HEAD ok %s at %s", loghdr, lom, tsi)
				}
				delete(lomAck.q, uname)
				continue
			}
			// retransmit
			if roc, err := _prepSend(lom); err != nil {
				glog.Errorf("%s: failed to retransmit %s => %s: %v", loghdr, lom, tsi, err)
			} else {
				rj.doSend(lom, tsi, roc)
				glog.Warningf("%s: retransmitting %s => %s", loghdr, lom, tsi)
				cnt++
			}
			if aborted() {
				lomAck.mu.Unlock()
				return 0
			}
		}
		lomAck.mu.Unlock()
		if aborted() {
			return 0
		}
	}
	return
}

// Aborts rebalance xaction and notifies all other targets.
func (reb *Manager) abortRebalance() {
	xreb := reb.xact()
	if xreb == nil || xreb.Aborted() || xreb.Finished() {
		return
	}
	glog.Info("aborting rebalance...")
	xreb.Abort()
	var (
		req = pushReq{
			daemonID: reb.t.SID(),
			rebID:    reb.RebID(),
			stage:    rebStageAbort,
		}
		hdr = transport.ObjHdr{}
	)
	hdr.Opaque = reb.encodePushReq(&req)
	if err := reb.pushes.Send(&transport.Obj{Hdr: hdr}, nil); err != nil {
		glog.Errorf("Failed to broadcast abort notification: %v", err)
	}
}

// Returns if the target is quiescent: transport queue is empty, or xaction
// has already aborted or finished.
func (reb *Manager) isQuiescent() bool {
	// Finished or aborted xaction = no traffic
	xact := reb.xact()
	if xact == nil || xact.Aborted() || xact.Finished() {
		return true
	}

	// Check for both regular and EC transport queues are empty
	return reb.inQueue.Load() == 0 && reb.onAir.Load() == 0
}
