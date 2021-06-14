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
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	jsoniter "github.com/json-iterator/go"
	"github.com/klauspost/reedsolomon"
)

// TODO: At this moment the module contains duplicated code borrowed from EC
// package. Extracting common stuff and moving them to EC package is
// a goal for the next MRs.

// High level overview of how EC rebalance works.
// NOTE: really it is not only rebalance, it also repairs damaged objects
// 01. When rebalance starts, it checks if EC is enabled. If not, it starts
//     regular rebalance.
// 02. First stage is to build global namespace of existing data (rebStageTraverse)
// 03. Each target traverses local directories that contain EC metadata and
//     collects those that have corresponding slice/object
// 04. When the list is complete, a target sends the list to other targets
// 05. Each target waits for all other targets to receive the data and then
//     rebalance moves to next stage (rebStageECDetect)
// 06. Each target processes the list of CTs and groups by Bck/ObjName/ObjHash
// 07. All other steps use the newest CT list of an object. Though it has
//     and issue. TODO: rare corner case: current object is EC'ed, after putting a
//     new version, the object gets replicated. Replicated object requires less
//     CTs, so the algorithm chooses the list that belongs to older hash version
// 08. If a local CT is on incorrect mpath, it is added to local repair list.
// 09. If one or few object parts are missing, and it is possible to restore
//     them from existing ones, the object is added to 'broken' object list
// 10. If 'broken' and 'local repair' lists are empty, the rebalance finishes.
// 11. 'broken' list is sorted by Bck/ObjName to have on all targets
//     determined order.
// 12. First, 'local repair' list is processed and all CTs are moved to correct mpath.
// 13. Next the rebalance proceeds with the next stage (rebStageECRepair)
//     and wait for all other nodes.
// 14. To minimize memory/GC load, the 'broken' list is processed in batches.
//     At this moment a batch size is 8 objects.
// 15. If object was replicated:
//     - this node does not have a replica. Calculate HrwTargetList, and if
//       the node is in it, start waiting for replica from another node
//     - this node has a replica. Calculate HrwTarget and if the node is the
//       first one in the list that has replica, start sending replica to other nodes
// 16. If object was EC'ed:
//   Step #1 - transferring existing CTs:
//     - if full object does not exist, and this targets has a slice, the node
//       sends slice to the "default" node to rebuild
//     - if full object does not exist, and this node should have a slice by HRW,
//       it starts waiting for a slice from "default" target
//     - if this node is "default" and object does not exist, the node starts
//       waiting all existing slices and add the object to 'rebuild' list
//     - if full object exists but misplaced, the "default" node starts waiting for it
//     - if full object exists, and this nodes should have a slices, wait for it
//   Step #2 - now "default" node has ether full object or all existing slices
//     - if 'rebuild' list is not empty, rebuild missing slices of all objects
//       from the list and send to other nodes
// 17. Notify other nodes that batch is done and wait for other nodes
// 18. After the batch is processed, cleanup all allocated memory and open the next batch
// 19. If anything goes wrong, rebalance may abort and in deferred procedure
//     it cleans up all allocated resources
// 20. All batches processed, rebalance moves to the next stage (rebStageECCleanup).
//     Targets finalize rebalance.

// Status of an object which CT the target awaits
const (
	// not enough local slices to rebuild the object
	objWaiting = iota
	// all slices have been received, can start rebuilding
	objReceived
	// object has been rebuilt and all new slices were sent to other targets
	objDone
)

type (
	// a full info about a CT found on a target.
	// It is information sent to other targets that must be sufficient
	// to check and restore everything
	rebCT struct {
		realFQN string       // mpath CT real
		hrwFQN  string       // mpath CT by HRW
		meta    *ec.Metadata // metadata loaded from a local file
		sgl     *memsys.SGL  // for received data

		cmn.Bck
		ObjName      string `json:"obj"`
		DaemonID     string `json:"sid"` // a target that has the CT
		ObjHash      string `json:"cksum"`
		ObjSize      int64  `json:"size"`
		SliceID      int16  `json:"sliceid,omitempty"`
		DataSlices   int16  `json:"data"`
		ParitySlices int16  `json:"parity"`
	}

	ctList = map[string][]*rebCT // EC CTs grouped by a rule
	// a full info about an object that resides on the local target.
	// Contains both global and calculated local info
	rebObject struct {
		mtx          sync.Mutex
		cts          ctList            // obj hash <-> list of CTs with the same hash
		hrwTargets   []*cluster.Snode  // the list of targets that should have CT
		rebuildSGLs  []*memsys.SGL     // temporary slices for [re]building EC
		fh           *cos.FileHandle   // main fh when building slices from existing main
		sender       *cluster.Snode    // target responsible to send replicas over the cluster (first by HRW)
		locCT        map[string]*rebCT // CT locations: maps daemonID to CT for faster check what nodes have the CT
		ctExist      []bool            // marks existing CT: SliceID <=> Exists
		mainDaemon   string            // hrw target for an object
		uid          string            // unique identifier for the object (Bucket#Object#IsAIS)
		bck          cmn.Bck
		objName      string       // object name for faster access
		objSize      int64        // object size
		sliceSize    int64        // a size of an object slice
		ready        atomic.Int32 // object state: objWaiting, objReceived, objDone
		dataSlices   int16        // the number of data slices
		paritySlices int16        // the number of parity slices
		mainSliceID  int16        // sliceID on the main target
		isECCopy     bool         // replicated or erasure coded
		hasCT        bool         // local target has any obj's CT
		mainHasAny   bool         // is default target has any part of the object
		isMain       bool         // is local target a default one
		inHrwList    bool         // is local target should have any CT according to HRW
		fullObjFound bool         // some target has the full object, no need to rebuild, just copy
		hasAllSlices bool         // true: all slices existed before rebalance
	}
	rebBck struct {
		cmn.Bck
		objs map[string]*rebObject // maps ObjectName <-> object info
	}
	// final result of scanning the existing objects
	globalCTList struct {
		bcks map[string]*rebBck // maps BucketFQN <-> objects
	}
	// CT destination (to use later for retransmitting lost CTs)
	retransmitCT struct {
		daemonID string           // destination
		header   transport.ObjHdr // original transport header copy
		sliceID  int16
	}
	// Struct to receive data from GET slice API request
	sliceGetResp struct {
		err     error
		sgl     *memsys.SGL
		meta    *ec.Metadata
		sliceID int16
	}

	// a list of CTs waiting for receive acknowledge from remote targets
	ackCT struct {
		mtx sync.Mutex
		ct  []*retransmitCT
	}

	// Callback is called if a target did not report that it is in `stage` or
	// its notification was lost. Callback either request the current state
	// directly or makes the target to resend.
	// The callback returns `true` only if the target is in "stage" stage or
	// reached any next stage already
	StageCallback = func(si *cluster.Snode) bool

	ecData struct {
		cts    ctList // maps daemonID <-> CT List
		mtx    sync.Mutex
		broken []*rebObject
		ackCTs ackCT
		onAir  atomic.Int64 // the number of CTs passed to transport but not yet sent to a remote target

		// list of CTs that should be moved between local mpaths
		localActions []*rebCT
	}
)

// look for local slices/replicas
func (reb *Manager) buildECNamespace(md *rebArgs) int {
	reb.runECjoggers()
	if reb.waitForPushReqs(md, rebStageECNamespace) {
		return 0
	}
	return reb.bcast(md, reb.waitNamespace)
}

// main method - starts all mountpaths walkers, waits for them to finish, and
// changes internal stage after that to 'traverse done', so the caller may continue
// rebalancing: send collected data to other targets, rebuild slices etc
func (reb *Manager) runECjoggers() {
	var (
		wg                = &sync.WaitGroup{}
		availablePaths, _ = fs.Get()
		cfg               = cmn.GCO.Get()
		bck               = reb.xact().Bck()
	)

	for _, mpathInfo := range availablePaths {
		bck := cmn.Bck{Name: bck.Name, Provider: cmn.ProviderAIS, Ns: bck.Ns}
		wg.Add(1)
		go reb.jogEC(mpathInfo, bck, wg)
	}

	for _, provider := range cfg.Backend.Providers {
		for _, mpathInfo := range availablePaths {
			bck := cmn.Bck{Name: bck.Name, Provider: provider.Name, Ns: bck.Ns}
			wg.Add(1)
			go reb.jogEC(mpathInfo, bck, wg)
		}
	}
	wg.Wait()
	reb.changeStage(rebStageECNamespace, 0)
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
func (reb *Manager) distributeECNamespace(md *rebArgs) error {
	const distributeTimeout = 5 * time.Minute
	if err := reb.exchange(md); err != nil {
		return err
	}
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
func (reb *Manager) generateECFixList(md *rebArgs) {
	reb.checkCTs(md)
	glog.Infof("number of objects misplaced locally: %d", len(reb.ec.localActions))
	glog.Infof("number of objects to be reconstructed/resent: %d", len(reb.ec.broken))
}

func (reb *Manager) ecFixLocal(md *rebArgs) error {
	if err := reb.resilverCT(); err != nil {
		return fmt.Errorf("failed to resilver slices/objects: %v", err)
	}

	if cnt := reb.bcast(md, reb.waitECResilver); cnt != 0 {
		return fmt.Errorf("%d targets failed to complete resilver", cnt)
	}
	return nil
}

func (reb *Manager) ecFixGlobal(md *rebArgs) error {
	if err := reb.ecGlobal(md); err != nil {
		if !reb.xact().Aborted() {
			glog.Errorf("EC rebalance failed: %v", err)
			reb.abortRebalance()
		}
		return err
	}

	if cnt := reb.bcast(md, reb.waitECCleanup); cnt != 0 {
		return fmt.Errorf("%d targets failed to complete resilver", cnt)
	}
	return nil
}

// Sends local CT along with EC metadata to default target.
// The CT is on a local drive and not loaded into SGL. Just read and send.
func (reb *Manager) sendFromDisk(ct *rebCT, target *cluster.Snode) (err error) {
	var (
		lom       *cluster.LOM
		parsedFQN fs.ParsedFQN
		roc       cos.ReadOpenCloser
		fqn       = ct.realFQN
	)
	debug.Assert(ct.meta != nil)
	if ct.hrwFQN != "" {
		fqn = ct.hrwFQN
	}
	if parsedFQN, _, err = cluster.ResolveFQN(fqn); err != nil {
		return
	}
	// FIXME: We should unify acquiring a reader for LOM and CT. Both should be
	//  locked and handled similarly.
	if parsedFQN.ContentType == fs.ObjectType {
		lom = cluster.AllocLOM(parsedFQN.ObjName)
		if err = lom.Init(ct.Bck); err != nil {
			cluster.FreeLOM(lom)
			return
		}
		lom.Lock(false)
		if err = lom.Load(false /*cache it*/, true /*locked*/); err != nil {
			lom.Unlock(false)
			cluster.FreeLOM(lom)
			return
		}
	} else {
		lom = nil // sending slice; TODO: rlock
	}
	// open
	if roc, err = cos.NewFileHandle(fqn); err != nil {
		if lom != nil {
			lom.Unlock(false)
			cluster.FreeLOM(lom)
		}
		return
	} else if lom != nil {
		roc = cos.NewDeferROC(roc, func() {
			lom.Unlock(false)
			cluster.FreeLOM(lom)
		})
	}
	// transmit
	req := pushReq{daemonID: reb.t.SID(), stage: rebStageECRepair, rebID: reb.rebID.Load(), md: ct.meta}
	o := transport.AllocSend()
	o.Hdr = transport.ObjHdr{
		Bck:      ct.Bck,
		ObjName:  ct.ObjName,
		ObjAttrs: transport.ObjectAttrs{Size: ct.ObjSize},
	}
	if lom != nil {
		o.Hdr.ObjAttrs.Atime = lom.AtimeUnix()
		o.Hdr.ObjAttrs.Version = lom.Version()
		if cksum := lom.Cksum(); cksum != nil {
			o.Hdr.ObjAttrs.CksumType, o.Hdr.ObjAttrs.CksumValue = cksum.Get()
		}
	}
	if ct.SliceID != 0 {
		o.Hdr.ObjAttrs.Size = ec.SliceSize(ct.ObjSize, int(ct.DataSlices))
	}
	reb.ec.onAir.Inc()
	o.Hdr.Opaque = req.NewPack(rebMsgEC)
	o.Callback = reb.transportECCB
	if err = reb.dm.Send(o, roc, target); err != nil {
		err = fmt.Errorf("failed to send slices to nodes [%s..]: %v", target.ID(), err)
		return
	}
	reb.statTracker.AddMany(
		stats.NamedVal64{Name: stats.RebTxCount, Value: 1},
		stats.NamedVal64{Name: stats.RebTxSize, Value: o.Hdr.ObjAttrs.Size},
	)
	return
}

// send completion
func (reb *Manager) transportECCB(_ transport.ObjHdr, _ io.ReadCloser, _ interface{}, _ error) {
	reb.ec.onAir.Dec()
}

// Sends reconstructed slice along with EC metadata to remote target.
// EC metadata is of main object, so its internal field SliceID must be
// fixed prior to sending.
// Use the function to send only slices (not for replicas/full object)
func (reb *Manager) sendFromReader(reader cos.ReadOpenCloser,
	ct *rebCT, sliceID int, cksum *cos.CksumHash, target *cluster.Snode, rt *retransmitCT) error {
	cos.AssertMsg(ct.meta != nil, ct.ObjName)
	cos.AssertMsg(ct.ObjSize != 0, ct.ObjName)
	var (
		newMeta = *ct.meta // copy of meta (flat struct of primitive types)
		req     = pushReq{
			daemonID: reb.t.SID(),
			stage:    rebStageECRepair,
			rebID:    reb.rebID.Load(),
			md:       &newMeta,
		}
		size = ec.SliceSize(ct.ObjSize, int(ct.DataSlices))
		o    = transport.AllocSend()
	)
	o.Hdr = transport.ObjHdr{
		Bck:      ct.Bck,
		ObjName:  ct.ObjName,
		ObjAttrs: transport.ObjectAttrs{Size: size},
	}
	newMeta.SliceID = sliceID
	o.Hdr.Opaque = req.NewPack(rebMsgEC)
	if cksum != nil {
		o.Hdr.ObjAttrs.CksumValue = cksum.Value()
		o.Hdr.ObjAttrs.CksumType = cksum.Type()
	}

	rt.daemonID = target.ID()
	rt.header = o.Hdr
	reb.ec.ackCTs.add(rt)

	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("sending slice %d(%d)%s of %s/%s to %s", sliceID, size, cksum, ct.Bck.Name, ct.ObjName, target)
	}
	reb.ec.onAir.Inc()
	o.Callback = reb.transportECCB
	if err := reb.dm.Send(o, reader, target); err != nil {
		reb.ec.ackCTs.remove(rt)
		return fmt.Errorf("failed to send slices to node %s: %v", target, err)
	}
	reb.statTracker.AddMany(
		stats.NamedVal64{Name: stats.RebTxCount, Value: 1},
		stats.NamedVal64{Name: stats.RebTxSize, Value: size},
	)
	return nil
}

// Saves received CT to a local drive if needed:
//   1. Full object/replica is received
//   2. A CT is received and this target is not the default target (it
//      means that the CTs came from default target after EC had been rebuilt)
func (reb *Manager) saveCTToDisk(data io.Reader, req *pushReq, hdr transport.ObjHdr) error {
	cos.Assert(req.md != nil)
	var (
		err      error
		bck      = cluster.NewBckEmbed(hdr.Bck)
		needSave = req.md.SliceID == 0 // full object always saved
	)
	if err := bck.Init(reb.t.Bowner()); err != nil {
		return err
	}
	uname := bck.MakeUname(hdr.ObjName)
	if !needSave {
		// slice is saved only if this target is not "main" one.
		// Main one receives slices as well but it uses them only to rebuild
		tgt, err := cluster.HrwTarget(uname, reb.t.Sowner().Get())
		if err != nil {
			return err
		}
		needSave = tgt.ID() != reb.t.SID()
	}
	if !needSave {
		return nil
	}

	md := req.md.NewPack()
	if req.md.SliceID != 0 {
		args := &ec.WriteArgs{Reader: data, MD: md}
		err = ec.WriteSliceAndMeta(reb.t, hdr, args)
	} else {
		var lom *cluster.LOM
		lom, err = ec.LomFromHeader(hdr)
		if err == nil {
			args := &ec.WriteArgs{
				Reader:     data,
				MD:         md,
				CksumType:  hdr.ObjAttrs.CksumType,
				CksumValue: hdr.ObjAttrs.CksumValue,
			}
			err = ec.WriteReplicaAndMeta(reb.t, lom, args)
		}
		cluster.FreeLOM(lom)
	}
	return err
}

// Receives a CT from another target, saves to local drive because it is a missing one
func (reb *Manager) receiveCT(req *pushReq, hdr transport.ObjHdr, reader io.Reader) error {
	const fmtErrCleanedUp = "%s/%s slice %d has been already cleaned up"
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("%s GOT CT for %s/%s from %s", reb.t.Snode(), hdr.Bck, hdr.ObjName, req.daemonID)
	}
	cos.Assert(req.md != nil)

	reb.laterx.Store(true)
	sliceID := req.md.SliceID
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof(">>> %s got CT %d for %s/%s %d", reb.t.Snode(), sliceID, hdr.Bck, hdr.ObjName, hdr.ObjAttrs.Size)
	}
	uid := uniqueWaitID(hdr.Bck, hdr.ObjName)

	// TODO: batch size can be changed on the fly
	batchSize := cmn.GCO.Get().EC.BatchSize
	obj := reb.objByUID(batchSize, uid)
	if obj == nil {
		glog.Warningf("DaemonID %s sent slice %d of %s to late?", req.daemonID, req.md.SliceID, uid)
		return nil
	}
	obj.mtx.Lock()
	defer obj.mtx.Unlock()
	ct, ok := obj.locCT[req.daemonID]
	cos.Assertf(ok || sliceID != 0, "%s : %s", req.daemonID, uid)
	if !ok {
		// Main target sends a rebuilt slice, need to receive it and save.
		if glog.FastV(4, glog.SmoduleReb) {
			glog.Infof("Saving to local drives %s[%d]", uid, sliceID)
		}
		if err := reb.saveCTToDisk(reader, req, hdr); err != nil {
			glog.Errorf("failed to save CT %d of %s: %v", sliceID, hdr.ObjName, err)
			if obj.isMain {
				reb.abortRebalance()
				return err
			}
			// If the target is not a default one, return `nil immediately`.
			// In this case, the default target does receive ACK and marks
			// the slice to resend by a scrubber
		}
		return nil
	}
	if ct.sgl != nil || obj.ready.Load() != objWaiting {
		cos.DrainReader(reader)
		glog.Infof(fmtErrCleanedUp, hdr.Bck, hdr.ObjName, sliceID)
		return nil
	}
	size := cos.MinI64(obj.objSize, cos.MiB)
	ct.sgl = reb.t.MMSA().NewSGL(size)
	ct.meta = req.md
	n, err := io.Copy(ct.sgl, reader) // No need for `io.CopyBuffer` since SGL implements `io.ReaderFrom`.
	if err != nil {
		return fmt.Errorf("failed to read slice %d for %s/%s: %v", sliceID, hdr.Bck, hdr.ObjName, err)
	}

	reb.statTracker.AddMany(
		stats.NamedVal64{Name: stats.RebRxCount, Value: 1},
		stats.NamedVal64{Name: stats.RebRxSize, Value: n},
	)
	if hdr.ObjAttrs.CksumValue != "" {
		cksum, err := checksumSlice(memsys.NewReader(ct.sgl), ct.sgl.Size(), hdr.ObjAttrs.CksumType, reb.t.MMSA())
		if err != nil {
			return fmt.Errorf("failed to checksum slice %d for %s/%s: %v", sliceID, hdr.Bck, hdr.ObjName, err)
		}
		if hdr.ObjAttrs.CksumValue != cksum.Value() {
			reb.statTracker.AddMany(stats.NamedVal64{Name: stats.ErrCksumCount, Value: 1})
			return cos.NewBadDataCksumError(
				cos.NewCksum(hdr.ObjAttrs.CksumType, hdr.ObjAttrs.CksumValue),
				&cksum.Cksum,
				"rebalance")
		}
	}
	saveToDisk := ct.SliceID == 0 || !obj.isMain
	if saveToDisk {
		obj.ready.Store(objDone)
		if err := reb.saveCTToDisk(memsys.NewReader(ct.sgl), req, hdr); err != nil {
			glog.Errorf("failed to save CT %d of %s: %v", sliceID, hdr.ObjName, err)
			if obj.isMain {
				reb.abortRebalance()
				return err
			}
			// If the target is not a default one, return `nil immediately`.
			// In this case, the default target does receive ACK and marks
			// the slice to resend by a scrubber
			return nil
		}
	} else {
		_updRebuildInfo(obj)
	}
	if glog.FastV(4, glog.SmoduleReb) {
		waitSlice, waitRebuild := reb.toWait(batchSize)
		glog.Infof("CTs to get remains: %d, waiting for rebuild: %d", waitSlice, waitRebuild)
	}

	// ACK
	var (
		smap = (*cluster.Smap)(reb.smap.Load())
		tsi  = smap.GetTarget(req.daemonID)
		ack  = &ecAck{sliceID: uint16(sliceID), daemonID: reb.t.SID()}
	)
	hdr.Opaque = ack.NewPack()
	hdr.ObjAttrs.Size = 0
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("Sending ACK for %s/%s to %s", hdr.Bck, hdr.ObjName, tsi.ID())
	}
	if err := reb.dm.ACK(hdr, nil, tsi); err != nil {
		glog.Error(err)
	}

	return nil
}

// receiving EC CT or EC namespace
func (reb *Manager) recvECData(hdr transport.ObjHdr, unpacker *cos.ByteUnpack, reader io.Reader) {
	defer cos.DrainReader(reader)

	req := &pushReq{}
	err := unpacker.ReadAny(req)
	if err != nil {
		glog.Errorf("invalid push notification %s: %v", hdr.ObjName, err)
		return
	}

	if req.rebID != reb.rebID.Load() {
		glog.Warningf("local node has not started or already has finished rebalancing")
		return
	}

	// a remote target sent CT
	if req.stage == rebStageECRepair {
		if err := reb.receiveCT(req, hdr, reader); err != nil {
			glog.Errorf("failed to receive CT for %s/%s: %v", hdr.Bck, hdr.ObjName, err)
			return
		}
		return
	}

	// otherwise a remote target sent collected namespace:
	// - receive the CT list
	// - update the remote target stage
	if req.stage != rebStageECNamespace {
		glog.Errorf("invalid stage %s : %s (must be %s)", hdr.ObjName,
			stages[req.stage], stages[rebStageECNamespace])
		return
	}

	b, err := io.ReadAll(reader)
	if err != nil {
		glog.Errorf("failed to read data from %s: %v", req.daemonID, err)
		return
	}

	cts := make([]*rebCT, 0)
	if err = jsoniter.Unmarshal(b, &cts); err != nil {
		glog.Errorf("failed to unmarshal data from %s: %v", req.daemonID, err)
		return
	}

	reb.ec.setNodeData(req.daemonID, cts)
	reb.stages.setStage(req.daemonID, req.stage, 0)
}

// Build a list buckets with their objects from a flat list of all CTs
func (reb *Manager) mergeCTs(md *rebArgs) *globalCTList {
	res := &globalCTList{
		bcks: make(map[string]*rebBck),
	}

	// process all received CTs
	localDaemon := reb.t.SID()
	for sid := range md.smap.Tmap {
		local := sid == localDaemon
		ctList, ok := reb.ec.nodeData(sid)
		if !ok {
			continue
		}
		for _, ct := range ctList {
			// For erasure encoded buckets do the extra check:
			// skip all slices on the "default" target because the
			// default must contain only "full" object
			if ct.SliceID != 0 {
				b := cluster.NewBckEmbed(ct.Bck)
				if err := b.Init(reb.t.Bowner()); err != nil {
					reb.abortRebalance()
					return nil
				}
				t, err := cluster.HrwTarget(b.MakeUname(ct.ObjName), md.smap)
				cos.Assert(err == nil)
				if local && t.ID() == localDaemon {
					glog.Infof("skipping CT %d of %s (it must have main object)", ct.SliceID, ct.ObjName)
					continue
				}
				if !local && sid == t.ID() {
					glog.Infof("skipping CT %d of %s on default target", ct.SliceID, ct.ObjName)
					continue
				}
			}

			if err := res.addCT(md, ct, reb.t); err != nil {
				if cmn.IsErrBucketNought(err) {
					reb.abortRebalance()
					return nil
				}
				glog.Warningf("%s: %v", sid, err)
				continue
			}
			if local && ct.hrwFQN != ct.realFQN {
				reb.ec.localActions = append(reb.ec.localActions, ct)
				if glog.FastV(4, glog.SmoduleReb) {
					glog.Infof("%s %s -> %s", reb.t.Snode(), ct.hrwFQN, ct.realFQN)
				}
			}
		}
	}
	return res
}

// Find objects that have either missing or misplaced parts. If a part is a
// slice or replica (not the "default" object) and mpath is correct the object
// is not considered as broken one even if its target is not in HRW list
func (reb *Manager) detectBroken(md *rebArgs, res *globalCTList) {
	reb.ec.broken = make([]*rebObject, 0)
	bowner := reb.t.Bowner()
	bmd := bowner.Get()

	for _, rebBck := range res.bcks {
		bck := cluster.NewBck(rebBck.Name, rebBck.Provider, rebBck.Ns)
		if err := bck.Init(bowner); err != nil {
			// bucket might be deleted while rebalancing - skip it
			glog.Errorf("invalid bucket %s: %v", rebBck.Name, err)
			delete(res.bcks, rebBck.Bck.String())
			continue
		}
		bprops, ok := bmd.Get(bck)
		if !ok {
			// bucket might be deleted while rebalancing - skip it
			glog.Errorf("bucket %s does not exist", rebBck.Name)
			delete(res.bcks, rebBck.Bck.String())
			continue
		}
		// Select only objects that have "main" part missing or misplaced.
		// Other missing slices/replicas must be added to ZIL
		for objName, obj := range rebBck.objs {
			if err := reb.calcLocalProps(bck, obj, md.smap, &bprops.EC); err != nil {
				glog.Warningf("detect %s failed, skipping: %v", obj.objName, err)
				continue
			}

			mainHasObject := (obj.mainSliceID == 0 || obj.isECCopy) && obj.mainHasAny
			if mainHasObject {
				// TODO: put to ZIL slices to rebuild locally and resend
				// or slices to move locally if needed
				glog.Infof("#0. Main has full object: %s", objName)
				continue
			}

			if glog.FastV(4, glog.SmoduleReb) {
				glog.Infof("[%s] BROKEN: %s [Main %d on %s], CTs %d of %d",
					reb.t.Snode(), objName, obj.mainSliceID, obj.mainDaemon, obj.foundCT(), obj.requiredCT())
			}
			reb.ec.broken = append(reb.ec.broken, obj)
		}
	}

	// sort the list of broken object to have deterministic order on all targets
	// sort order: IsAIS/Bucket name/Object name
	ctLess := func(i, j int) bool {
		if reb.ec.broken[i].bck.Provider != reb.ec.broken[j].bck.Provider {
			return reb.ec.broken[j].bck.IsAIS()
		}
		bi := reb.ec.broken[i].bck.Name
		bj := reb.ec.broken[j].bck.Name
		if bi != bj {
			return bi < bj
		}
		return reb.ec.broken[i].objName < reb.ec.broken[j].objName
	}
	sort.Slice(reb.ec.broken, ctLess)
}

// merge, sort, and detect what to fix and how
func (reb *Manager) checkCTs(md *rebArgs) {
	cts := reb.mergeCTs(md)
	if cts == nil {
		return
	}
	reb.detectBroken(md, cts)
}

// mountpath walker - walks through files in /meta/ directory
func (reb *Manager) jogEC(mpathInfo *fs.MountpathInfo, bck cmn.Bck, wg *sync.WaitGroup) {
	defer wg.Done()
	opts := &fs.Options{
		Mpath: mpathInfo,
		Bck:   bck,
		CTs:   []string{ec.MetaType},

		Callback: reb.walkEC,
		Sorted:   false,
	}
	if err := fs.Walk(opts); err != nil {
		if reb.xact().Aborted() || reb.xact().Finished() {
			glog.Infof("aborting traversal")
		} else {
			glog.Warningf("failed to traverse, err: %v", err)
		}
	}
}

// a file walker:
// - loads EC metadata from file
// - checks if the corresponding CT exists
// - calculates where "main" object for the CT is
// - store all the info above to memory
func (reb *Manager) walkEC(fqn string, de fs.DirEntry) (err error) {
	if reb.xact().Aborted() {
		// notify `dir.Walk` to stop iterations
		return cmn.NewAbortedError("interrupt walk - xaction aborted")
	}

	if de.IsDir() {
		return nil
	}

	ct, err := cluster.NewCTFromFQN(fqn, reb.t.Bowner())
	if err != nil {
		return nil
	}
	// do not touch directories for buckets with EC disabled (for now)
	if !ct.Bck().Props.EC.Enabled {
		return filepath.SkipDir
	}

	md, err := ec.LoadMetadata(fqn)
	if err != nil {
		glog.Warningf("damaged file? - failed to load metadata from %q: %v", fqn, err)
		return nil
	}

	// check if both slice/replica and metafile exist
	isReplica := md.SliceID == 0
	var fileFQN, hrwFQN string
	if isReplica {
		fileFQN = ct.Make(fs.ObjectType)
	} else {
		fileFQN = ct.Make(ec.SliceType)
	}
	if err := fs.Access(fileFQN); err != nil {
		glog.Warningf("%s no CT for metadata[%d]: %s", reb.t.Snode(), md.SliceID, fileFQN)
		return nil
	}

	// calculate correct FQN
	if isReplica {
		hrwFQN, _, err = cluster.HrwFQN(ct.Bck(), fs.ObjectType, ct.ObjectName())
	} else {
		hrwFQN, _, err = cluster.HrwFQN(ct.Bck(), ec.SliceType, ct.ObjectName())
	}
	if err != nil {
		return err
	}

	ct, err = cluster.NewCTFromFQN(fileFQN, reb.t.Bowner())
	if err != nil {
		return nil
	}

	id := reb.t.SID()
	rec := &rebCT{
		Bck:          ct.Bucket(),
		ObjName:      ct.ObjectName(),
		DaemonID:     id,
		ObjHash:      md.ObjCksum,
		ObjSize:      md.Size,
		SliceID:      int16(md.SliceID),
		DataSlices:   int16(md.Data),
		ParitySlices: int16(md.Parity),
		realFQN:      fileFQN,
		hrwFQN:       hrwFQN,
		meta:         md,
	}
	reb.ec.appendNodeData(id, rec)

	return nil
}

// Empties internal temporary data to be ready for the next rebalance.
func (reb *Manager) cleanupEC() {
	reb.ec.mtx.Lock()
	reb.ec.cts = make(ctList)
	reb.ec.localActions = make([]*rebCT, 0)
	reb.ec.broken = nil
	reb.ec.mtx.Unlock()
}

// send collected CTs to all targets with retry (to assemble the entire namespace)
func (reb *Manager) exchange(md *rebArgs) (err error) {
	const retries = 3 // number of retries to send collected CT info
	var (
		rebID   = reb.rebID.Load()
		sendTo  = cluster.AllocNodes(len(md.smap.Tmap))
		failed  = cluster.AllocNodes(len(md.smap.Tmap))
		emptyCT = make([]*rebCT, 0)
		config  = cmn.GCO.Get()
		sleep   = config.Timeout.MaxKeepalive.D() // delay between retries
	)
	for _, node := range md.smap.Tmap {
		if node.ID() == reb.t.SID() {
			continue
		}
		sendTo = append(sendTo, node)
	}
	cts, ok := reb.ec.nodeData(reb.t.SID())
	if !ok {
		cts = emptyCT // no data collected for the target, send empty notification
	}
	var (
		req    = pushReq{daemonID: reb.t.SID(), stage: rebStageECNamespace, rebID: rebID}
		body   = cos.MustMarshal(cts)
		opaque = req.NewPack(rebMsgEC)
		o      = transport.AllocSend()
	)
	o.Hdr.ObjAttrs = transport.ObjectAttrs{Size: int64(len(body))}
	o.Hdr.Opaque = opaque
	for i := 0; i < retries; i++ {
		failed = failed[:0]
		for _, node := range sendTo {
			if reb.xact().Aborted() {
				err = cmn.NewAbortedError("exchange")
				goto ret
			}
			rd := cos.NewByteHandle(body)
			if er1 := reb.dm.Send(o, rd, node); er1 != nil {
				glog.Errorf("Failed to send CTs to node %s: %v", node.ID(), er1)
				failed = append(failed, node)
			}
			reb.statTracker.AddMany(
				stats.NamedVal64{Name: stats.RebTxCount, Value: 1},
				stats.NamedVal64{Name: stats.RebTxSize, Value: int64(len(body))},
			)
		}
		if len(failed) == 0 {
			reb.changeStage(rebStageECDetect, 0)
			goto ret
		}
		time.Sleep(sleep)
		copy(sendTo, failed)
	}
	err = fmt.Errorf("could not send data to %d nodes", len(failed))
ret:
	cluster.FreeNodes(sendTo)
	cluster.FreeNodes(failed)
	return
}

func (reb *Manager) resilverSlice(fromFQN, toFQN string, buf []byte) error {
	if _, _, err := cos.CopyFile(fromFQN, toFQN, buf, cos.ChecksumNone); err != nil {
		reb.t.FSHC(err, fromFQN)
		reb.t.FSHC(err, toFQN)
		return err
	}
	if rmErr := os.Remove(fromFQN); rmErr != nil { // not severe error, can continue
		glog.Errorf("Error cleaning up %q: %v", fromFQN, rmErr)
	}
	return nil
}

func (reb *Manager) resilverObject(fromMpath fs.ParsedFQN, fromFQN, toFQN string, buf []byte) error {
	lom := cluster.AllocLOMbyFQN(fromFQN)
	defer cluster.FreeLOM(lom)

	if err := lom.Init(fromMpath.Bck); err != nil {
		return err
	}

	lom.Lock(true)
	defer lom.Unlock(true)
	if err := lom.Load(false /*cache it*/, true /*locked*/); err != nil {
		return err
	}

	lom.Uncache(true /*delDirty*/)
	clone, err := lom.CopyObject(toFQN, buf)
	cluster.FreeLOM(clone)
	if err != nil {
		reb.t.FSHC(err, fromFQN)
		reb.t.FSHC(err, toFQN)
		return err
	}

	if err := os.Remove(fromFQN); err != nil {
		glog.Errorf("Failed to cleanup %q: %v", fromFQN, err)
	}
	return nil
}

// Moves local misplaced CT to correct mpath
func (reb *Manager) resilverCT() error {
	buf, slab := reb.t.MMSA().Alloc()
	defer slab.Free(buf)
	for _, act := range reb.ec.localActions {
		if glog.FastV(4, glog.SmoduleReb) {
			glog.Infof("%s repair local %s -> %s", reb.t.Snode(), act.realFQN, act.hrwFQN)
		}
		mpathSrc, _, err := cluster.ResolveFQN(act.realFQN)
		if err != nil {
			return err
		}
		mpathDst, _, err := cluster.ResolveFQN(act.hrwFQN)
		if err != nil {
			return err
		}

		metaSrcFQN := mpathSrc.MpathInfo.MakePathFQN(mpathSrc.Bck, ec.MetaType, mpathSrc.ObjName)
		metaDstFQN := mpathDst.MpathInfo.MakePathFQN(mpathDst.Bck, ec.MetaType, mpathDst.ObjName)
		_, _, err = cos.CopyFile(metaSrcFQN, metaDstFQN, buf, cos.ChecksumNone)
		if err != nil {
			return err
		}

		// slice case
		if act.SliceID != 0 {
			if err := reb.resilverSlice(act.realFQN, act.hrwFQN, buf); err != nil {
				if rmErr := os.Remove(metaDstFQN); rmErr != nil {
					glog.Errorf("error cleaning up %q: %v", metaDstFQN, rmErr)
				}
				return err
			}
			continue
		}

		// object/replica case
		if err := reb.resilverObject(mpathSrc, act.realFQN, act.hrwFQN, buf); err != nil {
			if rmErr := os.Remove(metaDstFQN); rmErr != nil {
				glog.Errorf("error cleaning up %q: %v", metaDstFQN, rmErr)
			}
			return err
		}

		if err := os.Remove(metaSrcFQN); err != nil {
			glog.Errorf("failed to cleanup %q: %v", metaSrcFQN, err)
		}
	}

	reb.changeStage(rebStageECRepair, 0)
	return nil
}

// Fills object properties with props that must be calculated locally
func (reb *Manager) calcLocalProps(bck *cluster.Bck, obj *rebObject, smap *cluster.Smap, ecConfig *cmn.ECConf) (err error) {
	localDaemon := reb.t.SID()
	cts := obj.newest()
	cos.Assert(len(cts) != 0) // cannot happen
	mainSlice := cts[0]

	obj.bck = mainSlice.Bck
	obj.objName = mainSlice.ObjName
	obj.objSize = mainSlice.ObjSize
	obj.isECCopy = ec.IsECCopy(obj.objSize, ecConfig)
	obj.dataSlices = mainSlice.DataSlices
	obj.paritySlices = mainSlice.ParitySlices
	obj.sliceSize = ec.SliceSize(obj.objSize, int(obj.dataSlices))

	ctReq := obj.requiredCT()
	obj.ctExist = make([]bool, ctReq)
	obj.locCT = make(map[string]*rebCT)

	obj.uid = uniqueWaitID(obj.bck, mainSlice.ObjName)
	obj.isMain = obj.mainDaemon == localDaemon

	// TODO: must check only slices of the newest object version
	// FIXME: after EC versioning is implemented
	ctCnt := int16(0)
	for _, ct := range cts {
		if int(ct.SliceID) >= ctReq {
			// Data/parity got lower, so it is an old slice. Skip it.
			glog.Infof("Skip old slice %d of %s", ct.SliceID, obj.objName)
			continue
		}
		obj.locCT[ct.DaemonID] = ct
		if ct.DaemonID == localDaemon {
			obj.hasCT = true
		}
		if ct.DaemonID == obj.mainDaemon {
			obj.mainHasAny = true
			obj.mainSliceID = ct.SliceID
		}
		if ct.SliceID == 0 {
			obj.fullObjFound = true
		}
		obj.ctExist[ct.SliceID] = true
		if ct.SliceID != 0 {
			ctCnt++
		}
	}
	ctFound := obj.foundCT()
	obj.hasAllSlices = ctCnt >= obj.dataSlices+obj.paritySlices

	genCount := cos.Max(ctReq, len(smap.Tmap))
	obj.hrwTargets, err = cluster.HrwTargetList(bck.MakeUname(obj.objName), smap, genCount)
	if err != nil {
		return err
	}
	// check if HRW thinks this target must have any CT
	toCheck := ctReq - ctFound
	for _, tgt := range obj.hrwTargets[:ctReq] {
		if toCheck == 0 {
			break
		}
		if tgt.ID() == localDaemon {
			obj.inHrwList = true
			break
		}
		if _, ok := obj.locCT[tgt.ID()]; !ok {
			toCheck--
		}
	}
	// detect which target is responsible to send missing replicas to all
	// other target that miss their replicas
	for _, si := range obj.hrwTargets {
		if _, ok := obj.locCT[si.ID()]; ok {
			obj.sender = si
			break
		}
	}
	// If a cluster has the only slice left on the node that is under
	// maintenance, HrwList does not contain any target, it results in
	// obj.sender is nil here. Take the target from main slice as a sender.
	if obj.sender == nil {
		obj.sender = smap.GetTarget(mainSlice.DaemonID)
	}

	if obj.sender == nil {
		// slow path - must not happen
		daemonList, hrwList := make([]string, 0, len(obj.locCT)), make([]string, 0, len(obj.hrwTargets))
		for k := range obj.locCT {
			daemonList = append(daemonList, k)
		}
		for _, si := range obj.hrwTargets {
			hrwList = append(hrwList, si.ID())
		}
		return fmt.Errorf("failed to select a sender from %v for %v", hrwList, daemonList)
	}
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("%s %s: hasSlice %v, fullObjExist: %v, isMain %v [mainHas: %v - %d], obj size: %d[%s]",
			reb.t.Snode(), obj.uid, obj.hasCT, obj.fullObjFound, obj.isMain, obj.mainHasAny,
			obj.mainSliceID, obj.objSize, mainSlice.ObjHash)
		glog.Infof("%s: slice found %d vs required %d[all slices: %v], is in HRW %v [sender %s]",
			obj.uid, ctFound, ctReq, obj.hasAllSlices, obj.inHrwList, obj.sender)
	}
	reb.stages.currBatch.Store(0)
	return nil
}

// Generate unique ID for an object (all its slices gets the same uid)
func uniqueWaitID(bck cmn.Bck, objName string) string {
	return fmt.Sprintf("%s/%s", bck, objName)
}

// true if this target is not "default" one, and does not have any CT,
// and does not want any CT, the target can skip the object
func (reb *Manager) shouldSkipObj(obj *rebObject) bool {
	anyOnMain := obj.mainHasAny && obj.mainSliceID == 0
	noOnSecondary := !obj.hasCT && !obj.isMain
	notSender := obj.sender != nil && obj.isECCopy && obj.sender.ID() != reb.t.SID() && !obj.isMain
	skip := anyOnMain || noOnSecondary || notSender
	if skip && bool(glog.FastV(4, glog.SmoduleReb)) {
		glog.Infof("#0 SKIP %s - main has it: %v - %v -%v", obj.uid, anyOnMain, noOnSecondary, notSender)
		// TODO: else add to ZIL
	}
	return skip
}

// Get the ordinal number of a target in HRW list of targets that have a slice.
// Returns -1 if target is not found in the list.
func _targetIndex(daemonID string, obj *rebObject) int {
	cnt := 0
	// always skip the "default" target
	for _, tgt := range obj.hrwTargets[1:] {
		if _, ok := obj.locCT[tgt.ID()]; !ok {
			continue
		}
		if tgt.ID() == daemonID {
			return cnt
		}
		cnt++
	}
	return -1
}

// True if local target has a slice and it should send it to "default" target
// to rebuild the full object as it is missing. Even if the target has a slice
// it may skip sending it to the main target: the case is when there are
// already 'dataSliceCount' targets are going to send their slices(by HRW).
// Trading network traffic for main target's CPU.
func (reb *Manager) shouldSendSlice(obj *rebObject) (hasSlice, shouldSend bool) {
	if obj.isMain {
		return false, false
	}
	// First check if this target in the first 'dataSliceCount' slices.
	// Skip the first target in list for it is the main one.
	tgtIndex := _targetIndex(reb.t.SID(), obj)
	shouldSend = tgtIndex >= 0 && tgtIndex < int(obj.dataSlices)
	hasSlice = obj.hasCT && !obj.isMain && !obj.isECCopy && !obj.fullObjFound
	if hasSlice && (bool(glog.FastV(4, glog.SmoduleReb))) {
		locSlice := obj.locCT[reb.t.SID()]
		glog.Infof("should send: %s[%d - %d] - %d : %v / %v", obj.uid, locSlice.SliceID, obj.sliceSize, tgtIndex,
			hasSlice, shouldSend)
	}
	return hasSlice, shouldSend
}

// true if the object is not replicated, and this target has full object, and the
// target is not the default target, and default target does not have full object
func (reb *Manager) hasFullObjMisplaced(obj *rebObject) bool {
	locCT, ok := obj.locCT[reb.t.SID()]
	return ok && !obj.isECCopy && !obj.isMain && locCT.SliceID == 0 &&
		(!obj.mainHasAny || obj.mainSliceID != 0)
}

// Read CT from local drive and send to another target.
// If destination is not defined the target sends its data to "default by HRW" target
func (reb *Manager) sendLocalData(md *rebArgs, obj *rebObject, si ...*cluster.Snode) error {
	var target *cluster.Snode
	ct := obj.locCT[reb.t.SID()]
	if len(si) != 0 {
		target = si[0]
	} else {
		target = md.smap.Tmap[obj.mainDaemon]
	}
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("%s sending a slice/replica #%d of %s to main %s", reb.t.Snode(), ct.SliceID, ct.ObjName, target)
	}
	return reb.sendFromDisk(ct, target)
}

// Sends replica to the default target.
func (reb *Manager) sendLocalReplica(md *rebArgs, obj *rebObject) error {
	cos.Assert(obj.sender != nil) // mustn't happen
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("%s object %s sender %s", reb.t.Snode(), obj.uid, obj.sender)
	}
	// Another node should send replicas, do noting
	if obj.sender.DaemonID != reb.t.SID() {
		return nil
	}

	sendTo := md.smap.Tmap[obj.mainDaemon]
	ct, ok := obj.locCT[reb.t.SID()]
	cos.Assert(ok)
	if err := reb.sendFromDisk(ct, sendTo); err != nil {
		return fmt.Errorf("failed to send %s: %v", ct.ObjName, err)
	}
	return nil
}

func (reb *Manager) rebuildReceived(md *rebArgs) {
	for obj := reb.firstReadyObj(md); obj != nil; obj = reb.firstReadyObj(md) {
		if reb.xact().Aborted() {
			return
		}
		obj.mtx.Lock()
		err := reb.rebuildAndSend(obj)
		obj.ready.Store(objDone)
		obj.mtx.Unlock()
		if err != nil {
			glog.Errorf("Failed to rebuild %s: %v", obj.uid, err)
			reb.abortRebalance()
			break
		}
	}
}

// Internal function used in goroutines, so it does not return anything.
// Requests a CT and its metadata from a target:
//   1. Gets metadata from the remote target
//   2. If it exists, downloads the CT into `slice` writer
//   3. When successful, fills `slice` fields with received information
//   4. On error, copies the error to `slice` argument
func (reb *Manager) getCT(si *cluster.Snode, obj *rebObject, slice *sliceGetResp) {
	// First, read metadata
	qMeta := url.Values{}
	qMeta = cmn.AddBckToQuery(qMeta, obj.bck)
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("Getting slice %d for %s via API", slice.sliceID, obj.objName)
	}
	path := cmn.URLPathEC.Join(ec.URLMeta, obj.bck.Name, obj.objName)
	urlPath := si.URL(cmn.NetworkIntraData) + path

	var (
		rq   *http.Request
		resp *http.Response
		md   *ec.Metadata
	)
	if rq, slice.err = http.NewRequest(http.MethodGet, urlPath, nil); slice.err != nil {
		return
	}
	rq.Header.Add(cmn.HdrCallerID, reb.t.SID())
	rq.Header.Add(cmn.HdrCallerName, reb.t.Sname())
	rq.URL.RawQuery = qMeta.Encode()
	if resp, slice.err = reb.ecClient.Do(rq); slice.err != nil { // nolint:bodyclose // closed inside cos.Close
		return
	}
	if resp.StatusCode != http.StatusOK {
		cos.Close(resp.Body)
		slice.err = fmt.Errorf("failed to read metadata, HTTP status: %d", resp.StatusCode)
		return
	}
	md, slice.err = ec.MetaFromReader(resp.Body)
	cos.Close(resp.Body)
	if slice.err != nil {
		return
	}

	// Second, get the slice
	if slice.sliceID != 0 {
		path = cmn.URLPathEC.Join(ec.URLCT, obj.bck.Name, obj.objName)
	} else {
		path = cmn.URLPathObjects.Join(obj.bck.Name, obj.objName)
	}
	urlPath = si.URL(cmn.NetworkIntraData) + path
	if rq, slice.err = http.NewRequest(http.MethodGet, urlPath, nil); slice.err != nil {
		return
	}
	rq.Header.Add(cmn.HdrCallerID, reb.t.SID())
	rq.Header.Add(cmn.HdrCallerName, reb.t.Sname())
	rq.URL.RawQuery = qMeta.Encode()
	if resp, slice.err = reb.ecClient.Do(rq); slice.err != nil { // nolint:bodyclose // closed inside cos.Close
		return
	}
	defer cos.Close(resp.Body)
	if resp.StatusCode != http.StatusOK {
		cos.DrainReader(resp.Body)
		slice.err = fmt.Errorf("failed to read slice, HTTP status: %d", resp.StatusCode)
		return
	}
	if _, slice.err = io.Copy(slice.sgl, resp.Body); slice.err != nil {
		return
	}
	slice.meta = md
}

// Fetches missing slices from targets that so far failed to send them.
// Returns an error if unable to get sufficient number of slices.
func (reb *Manager) reRequestObj(md *rebArgs, obj *rebObject) error {
	if obj.ready.Load() == objDone {
		return nil
	}

	// detect received
	toRequest := make(map[string]*sliceGetResp, len(obj.locCT))
	for daemonID, rec := range obj.locCT {
		if rec.sgl != nil {
			continue
		}
		rec.sgl = reb.t.MMSA().NewSGL(cos.MinI64(obj.objSize, cos.MiB))
		toRequest[daemonID] = &sliceGetResp{sliceID: rec.SliceID, sgl: rec.sgl}
	}
	if len(toRequest) == 0 {
		return nil
	}

	// get missing slices
	wg := cos.NewLimitedWaitGroup(cluster.MaxBcastParallel(), len(toRequest))
	for sid, slice := range toRequest {
		debug.Assert(reb.t.SID() != sid)
		si := md.smap.Tmap[sid]
		wg.Add(1)
		go func(si *cluster.Snode, slice *sliceGetResp) {
			defer wg.Done()
			reb.getCT(si, obj, slice)
			if slice.err != nil {
				// It is OK to have one or few reads failed. Later it
				// checks if there are enough slices to rebuild.
				glog.Errorf("Failed to read slice %d from %s: %v", slice.sliceID, si, slice.err)
			}
		}(si, slice)
	}
	wg.Wait()
	for sid, resp := range toRequest {
		if resp.err != nil {
			continue
		}
		obj.locCT[sid].meta = resp.meta
	}
	// update object status: e.g, mark an object ready to rebuild if it
	// received enough slice to do it
	_updRebuildInfo(obj)
	state := obj.ready.Load()
	if state == objWaiting {
		return fmt.Errorf("failed to restore %s/%s - insufficient number of slices", obj.bck, obj.objName)
	}
	return nil
}

// When the target finishes the current batch, it checks if it has to
// rebuild any objects. When such object is found and the target still has
// not received enough slices, it rerequest them from all targets that have
// slices and have not sent anything to this one. After receiving all the
// data, the function updates the object status (changes objWaiting to
// objReceived or objDone depending on whether the object needs rebuilding)
func (reb *Manager) reRequest(md *rebArgs) {
	var (
		batchSize = md.config.EC.BatchSize
		start     = int(reb.stages.currBatch.Load())
	)
	for objIdx := start; objIdx < start+batchSize; objIdx++ {
		if reb.xact().Aborted() {
			return
		}
		if objIdx >= len(reb.ec.broken) {
			break
		}
		obj := reb.ec.broken[objIdx]
		obj.mtx.Lock()
		err := reb.reRequestObj(md, obj)
		obj.mtx.Unlock()
		if err != nil {
			glog.Error(err)
			reb.abortRebalance()
			break
		}
	}
}

// Returns true if all target have finished the current batch.
func (reb *Manager) allNodesCompletedBatch(md *rebArgs) bool {
	cnt := 0
	batchID := reb.stages.currBatch.Load()
	reb.stages.mtx.Lock()
	smap := md.smap.Tmap
	for _, si := range smap {
		if si.ID() == reb.t.SID() {
			// local target is always in the stage
			cnt++
			continue
		}
		if reb.stages.isInStageBatchUnlocked(si, rebStageECBatch, batchID) {
			cnt++
		}
	}
	reb.stages.mtx.Unlock()
	return cnt == len(smap)
}

// Sends full replica to the default target if the default does not have it
func (reb *Manager) restoreReplica(md *rebArgs, obj *rebObject) (err error) {
	// add to ZIL if any replica is missing and it is main target
	if obj.isMain && obj.mainHasAny {
		if glog.FastV(4, glog.SmoduleReb) {
			glog.Infof("#4 Skip replicas sending %s - main has it", obj.uid)
		}
		obj.ready.Store(objDone)
		return nil
	}
	if obj.isMain {
		if glog.FastV(4, glog.SmoduleReb) {
			glog.Infof("#4 Waiting for replica %s", obj.uid)
		}
		return
	}
	obj.ready.Store(objDone)
	return reb.sendLocalReplica(md, obj)
}

func (reb *Manager) recoverObj(md *rebArgs, obj *rebObject) (err error) {
	// Case #1: this target does not have to do anything
	if reb.shouldSkipObj(obj) {
		if glog.FastV(4, glog.SmoduleReb) {
			glog.Infof("SKIPPING %s", obj.uid)
		}
		obj.ready.Store(objDone)
		return nil
	}

	// Case #2: this target has someone's main object
	if reb.hasFullObjMisplaced(obj) {
		obj.ready.Store(objDone)
		return reb.sendLocalData(md, obj)
	}

	// Case #3: this target has a slice while the main must be restored.
	// Send local slice only if this target is in `dataSliceCount` first
	// targets which have any slice.
	hasSlice, shouldSend := reb.shouldSendSlice(obj)
	if !obj.fullObjFound && hasSlice {
		obj.ready.Store(objDone)
		if shouldSend {
			return reb.sendLocalData(md, obj)
		}
		return nil
	}

	// Case #4: object was replicated
	if obj.isECCopy {
		return reb.restoreReplica(md, obj)
	}

	// Case #5: the object is erasure coded

	// Case #5.1: it is not main target and has slice or does not need any
	if !obj.isMain && (obj.hasCT || !obj.inHrwList) {
		if glog.FastV(4, glog.SmoduleReb) {
			glog.Infof("#5.1 Object %s skipped", obj.uid)
		}
		obj.ready.Store(objDone)
		return nil
	}

	// Case #5.3: main has nothing, but full object and all slices exists
	noReplicaOnMain := !obj.mainHasAny || (obj.mainHasAny && obj.mainSliceID != 0)
	if obj.isMain && noReplicaOnMain {
		if glog.FastV(4, glog.SmoduleReb) {
			glog.Infof("#5.3 Waiting for an object %s or its slices", obj.uid)
		}
		return nil
	}

	cos.Assertf(obj.isMain && obj.mainHasAny && obj.mainSliceID == 0,
		"%s%s/%s: isMain %t - mainHasSome %t - mainID %d",
		reb.t.Snode(), obj.bck, obj.objName, obj.isMain, obj.mainHasAny, obj.mainSliceID)
	return nil
}

// Frees all SGLs and file handles opened during rebalancing the current batch
func (reb *Manager) cleanupBatch(md *rebArgs) {
	reb.releaseSGLs(md, reb.ec.broken)
	reb.ec.ackCTs.clear()
}

// Wait for all targets to finish the current batch and then free allocated resources
func (reb *Manager) finalizeBatch(md *rebArgs) error {
	// First, wait for all slices the local target wants to receive
	if q := reb.quiesce(md, md.config.Rebalance.Quiesce.D(), reb.allCTReceived); q == cluster.QuiAborted {
		return cmn.NewAbortedError("finalize batch - all ct received")
	}
	// wait until all rebuilt slices are sent
	reb.waitECAck(md)

	// mark batch done and notify other targets
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("%s batch %d done. Wait cluster-wide quiescence", reb.t.Snode(), reb.stages.currBatch.Load())
	}

	reb.changeStage(rebStageECBatch, reb.stages.currBatch.Load())
	// wait for all targets to finish sending/receiving
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("%s waiting for ALL batch %d DONE", reb.t.SID(), reb.stages.currBatch.Load())
	}
	if aborted := reb.waitEvent(md, reb.allNodesCompletedBatch); aborted {
		return cmn.NewAbortedError("finalize batch - wait nodes to complete")
	}

	return nil
}

// Add to log all slices that were sent to targets but this target
// has not received ACKs
func (reb *Manager) logNoECAck() {
	reb.ec.ackCTs.mtx.Lock()
	for _, dest := range reb.ec.ackCTs.ct {
		// TODO: add to ZIL
		reb.ec.ackCTs.removeUnlocked(dest)
	}
	reb.ec.ackCTs.mtx.Unlock()
}

func (reb *Manager) waitECAck(md *rebArgs) {
	logHdr := reb.logHdr(md)
	sleep := md.config.Timeout.CplaneOperation.D()
	// loop without timeout - wait until all CTs put into transport
	// queue are processed (either sent or failed)
	for reb.ec.onAir.Load() > 0 {
		if reb.xact().AbortedAfter(sleep) {
			reb.ec.onAir.Store(0)
			glog.Infof("%s: abort", logHdr)
			return
		}
	}
	// short wait for cluster-wide quiescence
	if q := reb.quiesce(md, sleep, reb.nodesQuiescent); q == cluster.QuiAborted {
		return
	}
	reb.logNoECAck()
}

// Rebalances the current batch of broken objects
func (reb *Manager) doBatch(md *rebArgs, batchCurr int64) error {
	batchEnd := cos.Min(int(batchCurr)+md.config.EC.BatchSize, len(reb.ec.broken))
	for objIdx := int(batchCurr); objIdx < batchEnd; objIdx++ {
		if reb.xact().Aborted() {
			return cmn.NewAbortedError("rebalance batch")
		}

		obj := reb.ec.broken[objIdx]
		if glog.FastV(4, glog.SmoduleReb) {
			glog.Infof("--- Starting object [%d] %s ---", objIdx, obj.uid)
		}
		cos.Assert(len(obj.locCT) != 0) // cannot happen

		if err := reb.recoverObj(md, obj); err != nil {
			return err
		}
	}
	return nil
}

// cluster-wide EC rebalance
func (reb *Manager) ecGlobal(md *rebArgs) (err error) {
	batchCurr := int64(0)
	batchLast := int64(len(reb.ec.broken) - 1)
	reb.stages.currBatch.Store(batchCurr)
	reb.stages.lastBatch.Store(batchLast)
	for batchCurr <= batchLast {
		if glog.FastV(4, glog.SmoduleReb) {
			glog.Infof("Starting batch of %d from %d", md.config.EC.BatchSize, batchCurr)
		}

		if err = reb.doBatch(md, batchCurr); err != nil {
			reb.cleanupBatch(md)
			return err
		}

		err = reb.finalizeBatch(md)
		reb.cleanupBatch(md)
		if err != nil {
			return err
		}
		batchCurr = reb.stages.currBatch.Add(int64(md.config.EC.BatchSize))
	}
	reb.changeStage(rebStageECCleanup, 0)
	return nil
}

// Free allocated memory for EC reconstruction, close opened file handles of replicas.
// Used to clean up memory after finishing a batch
func (reb *Manager) releaseSGLs(md *rebArgs, objList []*rebObject) {
	batchCurr := int(reb.stages.currBatch.Load())
	batchSize := md.config.EC.BatchSize
	go func(objList []*rebObject) {
		// Allow all HTTPs to finish sending all SGLs. Should fix errors like:
		//   panic: runtime error: invalid memory address or nil pointer dereference
		//       goroutine 820839 [running]:
		//       github.com/NVIDIA/aistore/memsys.(*Slab).Size(...)
		// See discussions at:
		//   https://gitlab-master.nvidia.com/aistorage/aistore/issues/472#note_4212419
		//   https://github.com/golang/go/issues/30597
		time.Sleep(5 * time.Second)
		for i := batchCurr; i < batchCurr+batchSize && i < len(objList); i++ {
			obj := objList[i]
			for _, sg := range obj.rebuildSGLs {
				if sg != nil {
					sg.Free()
				}
			}
			obj.rebuildSGLs = nil
			if obj.fh != nil {
				cos.Close(obj.fh)
				obj.fh = nil
			}
			for _, ct := range obj.locCT {
				if ct.sgl != nil {
					ct.sgl.Free()
				}
			}
		}
	}(objList)
}

// Object is missing (and maybe a few slices as well). Default target receives all
// existing slices into SGLs, restores the object, rebuilds slices, and finally
// sends missing slices to other targets
func (reb *Manager) rebuildFromSlices(obj *rebObject, conf *cmn.CksumConf) error {
	var (
		meta     *ec.Metadata
		sliceCnt = obj.dataSlices + obj.paritySlices
		readers  = make([]io.Reader, sliceCnt)

		// since io.Reader cannot be reopened, we need to have a copy for saving object
		rereaders   = make([]io.Reader, sliceCnt)
		writers     = make([]io.Writer, sliceCnt)
		slicesFound = int16(0)
	)
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("%s rebuilding slices (mem) of %s and send them", reb.t.Snode(), obj.objName)
	}
	obj.rebuildSGLs = make([]*memsys.SGL, sliceCnt)

	// put existing slices to readers list, and create SGL as writers for missing ones
	for _, sl := range obj.locCT {
		if sl.sgl == nil || sl.sgl.Size() == 0 {
			continue
		}
		id := sl.SliceID - 1
		cos.AssertMsg(sl.SliceID != 0 && readers[id] == nil, obj.uid)
		readers[id] = memsys.NewReader(sl.sgl)
		rereaders[id] = memsys.NewReader(sl.sgl)
		slicesFound++
		if meta == nil {
			meta = sl.meta
		}
	}
	// keep it for a while to be sure it does not happen
	cos.AssertMsg(meta.Size == obj.objSize, obj.uid)

	ecMD := meta.Clone()
	for i, rd := range readers {
		if rd != nil {
			continue
		}
		obj.rebuildSGLs[i] = reb.t.MMSA().NewSGL(cos.MinI64(obj.sliceSize, cos.MiB))
		writers[i] = obj.rebuildSGLs[i]
	}

	stream, err := reedsolomon.NewStreamC(int(obj.dataSlices), int(obj.paritySlices), true, true)
	if err != nil {
		return fmt.Errorf("failed to create initialize EC for %q: %v", obj.objName, err)
	}
	if err := stream.Reconstruct(readers, writers); err != nil {
		return fmt.Errorf("failed to build EC for %q: %v", obj.objName, err)
	}

	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("saving restored full object %s[%d]", obj.objName, obj.objSize)
	}
	// Save the object and its metadata first
	srcCnt := int(obj.dataSlices)
	srcReaders := make([]io.Reader, srcCnt)
	for i := 0; i < srcCnt; i++ {
		if readers[i] != nil {
			srcReaders[i] = rereaders[i]
			continue
		}
		cos.Assert(obj.rebuildSGLs[i] != nil)
		srcReaders[i] = memsys.NewReader(obj.rebuildSGLs[i])
	}
	src := io.MultiReader(srcReaders...)
	objMD := ecMD.Clone()
	objMD.SliceID = 0
	if err := reb.restoreObject(obj, objMD, src); err != nil {
		return err
	}

	var (
		sendErr     error
		freeTargets = cluster.AllocNodes(8)
	)
	freeTargets = obj.emptyTargets(reb.t.Snode(), freeTargets)
	for i, wr := range writers {
		if wr == nil {
			continue
		}
		sliceID := i + 1
		if exists := obj.ctExist[sliceID]; exists {
			if glog.FastV(4, glog.SmoduleReb) {
				glog.Infof("object %s slice %d: already exists", obj.uid, sliceID)
			}
			continue
		}
		if len(freeTargets) == 0 {
			sendErr = fmt.Errorf("failed to send slice %d of %s - no free target", sliceID, obj.uid)
			break
		}
		cksum, err := checksumSlice(memsys.NewReader(obj.rebuildSGLs[i]), obj.sliceSize, conf.Type, reb.t.MMSA())
		if err != nil {
			sendErr = fmt.Errorf("failed to checksum %s: %v", obj.objName, err)
			break
		}
		reader := memsys.NewReader(obj.rebuildSGLs[i])
		si := freeTargets[0]
		freeTargets = freeTargets[1:]

		sliceMD := ecMD.Clone()
		sliceMD.SliceID = sliceID
		sliceMD.CksumType = cksum.Type()
		sliceMD.CksumValue = cksum.Value()
		sl := &rebCT{
			Bck:          obj.bck,
			ObjName:      obj.objName,
			ObjSize:      sliceMD.Size,
			DaemonID:     reb.t.SID(),
			SliceID:      int16(sliceID),
			DataSlices:   int16(ecMD.Data),
			ParitySlices: int16(ecMD.Parity),
			meta:         sliceMD,
		}

		// TODO: mark this retransmition as non-vital and just add it to LOG
		// if we do not receive ACK back
		dest := &retransmitCT{sliceID: int16(sliceID)}
		if err := reb.sendFromReader(reader, sl, sliceID, cksum, si, dest); err != nil {
			sendErr = fmt.Errorf("failed to send slice %d of %s to %s: %v", i, obj.uid, si, err)
			break
		}
	}
	cluster.FreeNodes(freeTargets)
	return sendErr
}

func (reb *Manager) restoreObject(obj *rebObject, objMD *ec.Metadata, src io.Reader) (err error) {
	lom := cluster.AllocLOM(obj.objName)
	defer cluster.FreeLOM(lom)
	if err := lom.Init(obj.bck); err != nil {
		return err
	}
	lom.SetSize(obj.objSize)
	args := &ec.WriteArgs{
		Reader:    src,
		MD:        objMD.NewPack(),
		CksumType: lom.Bprops().Cksum.Type,
	}
	return ec.WriteReplicaAndMeta(reb.t, lom, args)
}

// Default target does not have object (but it can be on another target) and
// few slices may be missing. The function detects whether it needs to reconstruct
// the object and then rebuild and send missing slices
func (reb *Manager) rebuildAndSend(obj *rebObject) error {
	// Look through received slices if one of them is the object's replica.
	// In this case there is nothing to rebuild, just save it to disk
	for _, s := range obj.locCT {
		if s.SliceID != 0 || s.sgl == nil || s.sgl.Size() == 0 {
			continue
		}
		cos.AssertMsg(s.meta != nil, obj.uid)
		req := &pushReq{md: s.meta}
		cksumType, cksumValue := s.meta.CksumType, s.meta.ObjCksum
		if cksumType == "" && cksumValue != "" {
			glog.Errorf("%s/%s[%d] missing checksum type", obj.bck.Name, obj.objName, s.SliceID)
		}
		hdr := transport.ObjHdr{
			Bck:     obj.bck,
			ObjName: obj.objName,
			ObjAttrs: transport.ObjectAttrs{
				Size:       obj.objSize,
				CksumType:  cksumType,
				CksumValue: cksumValue,
				Version:    s.meta.ObjVersion,
			},
		}
		reb.saveCTToDisk(memsys.NewReader(s.sgl), req, hdr)
		obj.ready.Store(objDone)
		return nil
	}

	bck := cluster.NewBckEmbed(obj.bck)
	if err := bck.Init(reb.t.Bowner()); err != nil {
		return err
	}
	conf := bck.CksumConf()
	return reb.rebuildFromSlices(obj, conf)
}

func checksumSlice(reader io.Reader, sliceSize int64, cksumType string, mem *memsys.MMSA) (cksum *cos.CksumHash, err error) {
	buf, slab := mem.Alloc(sliceSize)
	_, cksum, err = cos.CopyAndChecksum(io.Discard, reader, buf, cksumType)
	slab.Free(buf)
	return
}

// Returns an object within the current batch by its UID.
// It is called from function that does not have rebArgs, so batchSize is explicit
func (reb *Manager) objByUID(batchSize int, uid string) *rebObject {
	var (
		obj   *rebObject
		start = int(reb.stages.currBatch.Load())
	)
	// a target can receive a CT from the next batch if another target
	// starts the next batch a bit faster, so "batchSize*2"
	for objIdx := start; objIdx < start+batchSize*2; objIdx++ {
		if objIdx >= len(reb.ec.broken) {
			break
		}
		if reb.ec.broken[objIdx].uid == uid {
			obj = reb.ec.broken[objIdx]
			break
		}
	}
	return obj
}

// Returns the first object that has received enough slices to rebuild object
func (reb *Manager) firstReadyObj(md *rebArgs) *rebObject {
	start := int(reb.stages.currBatch.Load())
	for objIdx := start; objIdx < start+md.config.EC.BatchSize; objIdx++ {
		if objIdx >= len(reb.ec.broken) {
			break
		}
		if reb.ec.broken[objIdx].ready.Load() == objReceived {
			return reb.ec.broken[objIdx]
		}
	}
	return nil
}

// Returns the number of objects waiting for a slice/replica, and the
// number of objects waiting for full object reconstuction.
func (reb *Manager) toWait(batchSize int) (wait, rebuild int) {
	start := int(reb.stages.currBatch.Load())
	for objIdx := start; objIdx < start+batchSize; objIdx++ {
		if objIdx >= len(reb.ec.broken) {
			break
		}
		switch reb.ec.broken[objIdx].ready.Load() {
		case objWaiting:
			wait++
		case objReceived:
			rebuild++
		}
	}
	return
}

// Check if the target received enough slices to start rebuilding
func _updRebuildInfo(obj *rebObject) {
	if obj.ready.Load() != objWaiting {
		return
	}
	cnt := 0
	for _, ct := range obj.locCT {
		if ct.sgl != nil && ct.sgl.Size() != 0 {
			cnt++
		}
	}
	if cnt != 0 && !obj.isMain {
		// if it is not main target, it needs only one slice
		obj.ready.Store(objDone)
	} else if obj.isMain && obj.isECCopy && cnt != 0 {
		obj.ready.Store(objReceived)
		// TODO: add to ZIL  missing replicas
	} else if cnt >= int(obj.dataSlices) {
		// if it is main target, it needs dataSlices slices to rebuild
		obj.ready.CAS(objWaiting, objReceived)
	}
}

///////////////
// rebObject //
///////////////

// All methods should be called only after it is clear that the object exists
// _or_ when we have one or more slices.
// Since a new slice is added to the list only when it matches a previously
// added one, it is OK to always read all info from the very first slice

// Returns the list of slices with the same object hash that is the newest one.
// In majority of cases the object will contain only one list.
//
// TODO: implement better detection of the newest object version. Now the newest
//       is determined only by the number of slices: the newest has the biggest number
func (so *rebObject) newest() []*rebCT {
	var (
		l       []*rebCT
		maxHash string
		max     int
	)
	for hash, ctList := range so.cts {
		if max > len(ctList) || len(ctList) == 0 {
			continue
		}
		// Choose the longest list of CTs.
		// If length is the same, take the list with greater hash value.
		if max < len(ctList) || hash > maxHash {
			max = len(ctList)
			l = ctList
			maxHash = hash
		} else if max == len(ctList) {
			glog.Warningf("The same size but hash is less. Skipping %s", hash)
		}
	}
	return l
}

// Returns the number of CTs that must exist (full replica including)
func (so *rebObject) requiredCT() int {
	if so.isECCopy {
		return int(so.paritySlices + 1)
	}
	return int(so.dataSlices + so.paritySlices + 1)
}

// Returns num CTs found across all targets
func (so *rebObject) foundCT() int {
	return len(so.locCT)
}

// Returns the list of targets that does not have any CT but
// they should have according to HRW.
func (so *rebObject) emptyTargets(skip *cluster.Snode, freeTargets cluster.Nodes) cluster.Nodes {
	for _, tgt := range so.hrwTargets {
		if skip != nil && skip.ID() == tgt.ID() {
			continue
		}
		if _, ok := so.locCT[tgt.ID()]; ok {
			continue
		}
		freeTargets = append(freeTargets, tgt)
	}
	return freeTargets
}

///////////
// ackCT //
///////////

func (ack *ackCT) add(rt *retransmitCT) {
	cos.Assert(rt.header.ObjName != "" && rt.header.Bck.Name != "")
	ack.mtx.Lock()
	ack.ct = append(ack.ct, rt)
	ack.mtx.Unlock()
}

// Linear search is sufficient at this moment: ackCT holds only ACKs for the
// current batch that is 8. Even if everything is broken an object is
// repairable only if there are at least `Data` slices exist.
// So, at most ackCT holds <number of objects> * <number of parity slices>.
// Even for 10 parity slices it makes only 80 items in array at max.
func (ack *ackCT) remove(rt *retransmitCT) {
	ack.mtx.Lock()
	ack.removeUnlocked(rt)
	ack.mtx.Unlock()
}

func (ack *ackCT) removeUnlocked(rt *retransmitCT) {
	for idx, c := range ack.ct {
		if !rt.equal(c) {
			continue
		}
		l := len(ack.ct) - 1
		if idx != l {
			ack.ct[idx] = ack.ct[l]
		}
		ack.ct = ack.ct[:l]
		break
	}
}

// Keep allocated capacity for the next batch
func (ack *ackCT) clear() {
	ack.mtx.Lock()
	ack.ct = ack.ct[:0]
	ack.mtx.Unlock()
}

//////////////////
// globalCTList //
//////////////////

// Merge given CT with already existing CTs of an object.
// It checks if the CT is unique(in case of the object is erasure coded),
// and the CT's info about object matches previously found CTs.
func (rr *globalCTList) addCT(md *rebArgs, ct *rebCT, tgt cluster.Target) error {
	bckList := rr.bcks
	bck, ok := bckList[ct.Bck.String()]
	if !ok {
		bck = &rebBck{Bck: ct.Bck, objs: make(map[string]*rebObject)}
		bckList[ct.Bck.String()] = bck
	}

	b := cluster.NewBckEmbed(ct.Bck)
	if err := b.Init(tgt.Bowner()); err != nil {
		return err
	}
	obj, ok := bck.objs[ct.ObjName]
	if !ok {
		// first CT of the object
		si, err := cluster.HrwTarget(b.MakeUname(ct.ObjName), md.smap)
		if err != nil {
			return err
		}
		obj = &rebObject{
			cts:        make(ctList),
			mainDaemon: si.ID(),
			bck:        ct.Bck,
		}
		obj.cts[ct.ObjHash] = []*rebCT{ct}
		bck.objs[ct.ObjName] = obj
		return nil
	}

	// When the duplicated slice is detected, the algorithm must choose
	// only one to proceed. To make all targets to choose the same one,
	// targets select slice by HRW
	if ct.SliceID != 0 {
		list := obj.cts[ct.ObjHash]
		for _, found := range list {
			if found.SliceID != ct.SliceID {
				continue
			}
			tgtList, errHrw := cluster.HrwTargetList(b.MakeUname(obj.objName), md.smap, len(md.smap.Tmap))
			if errHrw != nil {
				return errHrw
			}
			err := fmt.Errorf("duplicated %s/%s SliceID %d from %s (discarded)",
				ct.Name, ct.ObjName, ct.SliceID, ct.DaemonID)
			for _, tgt := range tgtList {
				if tgt.ID() == found.DaemonID {
					if found.DaemonID == ct.DaemonID && found.meta == nil {
						found.meta = ct.meta
						found.hrwFQN = ct.hrwFQN
						found.realFQN = ct.realFQN
						err = fmt.Errorf("duplicated slice from the same daemon: %s", ct.ObjName)
					}
					return err
				}
				if tgt.ID() == ct.DaemonID {
					err = fmt.Errorf("duplicated %s/%s SliceID %d replaced daemonID %s with %s",
						ct.Name, ct.ObjName, ct.SliceID, found.DaemonID, ct.DaemonID)
					found.DaemonID = ct.DaemonID
					if found.meta == nil {
						found.meta = ct.meta
					}
					// Copy the following fields as they are vital for local slices
					found.hrwFQN = ct.hrwFQN
					found.realFQN = ct.realFQN
					return err
				}
			}
			return err
		}
	}
	obj.cts[ct.ObjHash] = append(obj.cts[ct.ObjHash], ct)
	return nil
}

////////////
// ecData //
////////////

func newECData() *ecData {
	return &ecData{
		cts:          make(ctList),
		localActions: make([]*rebCT, 0),
		ackCTs:       ackCT{ct: make([]*retransmitCT, 0)},
	}
}

// Returns a CT list collected by `si` target
func (s *ecData) nodeData(daemonID string) ([]*rebCT, bool) {
	s.mtx.Lock()
	cts, ok := s.cts[daemonID]
	s.mtx.Unlock()
	return cts, ok
}

// Store a CT list received from `daemonID` target
func (s *ecData) setNodeData(daemonID string, cts []*rebCT) {
	s.mtx.Lock()
	s.cts[daemonID] = cts
	s.mtx.Unlock()
}

// Add a CT to list of CTs of a given target.
func (s *ecData) appendNodeData(daemonID string, ct *rebCT) {
	s.mtx.Lock()
	s.cts[daemonID] = append(s.cts[daemonID], ct)
	s.mtx.Unlock()
}

//////////////////
// retransmitCT //
//////////////////

func (rt *retransmitCT) equal(rhs *retransmitCT) bool {
	return rt.sliceID == rhs.sliceID &&
		rt.header.ObjName == rhs.header.ObjName &&
		rt.daemonID == rhs.daemonID &&
		rt.header.Bck.Equal(rhs.header.Bck)
}
