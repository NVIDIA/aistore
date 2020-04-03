// Package reb provides resilvering and rebalancing functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
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
// Note: really it is not only rebalance, it also repairs damaged objects
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
//     - if full object does not exists, and this node should have a slice by HRW,
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

const (
	// a target wait for the first slice(not a full replica) to come
	anySliceID = math.MaxInt16
)

// A "default" target wait state when it does not have full object
const (
	// target wait for a single slice that would be just saved to local drives
	waitForSingleSlice = iota
	// full object exists but on another target, wait until that target sends it
	waitForReplica
	// full object does exist, wait other targets send their slices and then rebuild
	waitForAllSlices
)

// status of an object which CT the target awaits
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

		cmn.Bck
		ObjName      string `json:"obj"`
		DaemonID     string `json:"sid"` // a target that has the CT
		ObjHash      string `json:"cksum"`
		ObjSize      int64  `json:"size"`
		SliceID      int16  `json:"sliceid,omitempty"`
		DataSlices   int16  `json:"data"`
		ParitySlices int16  `json:"parity"`
	}

	// A single object description which CT the targets waits for
	waitObject struct {
		// list of CTs to wait
		cts []*waitCT
		// wait type: see waitFor* enum
		wt int
		// object wait status: objWaiting, objReceived, objDone
		status int
	}

	ctList     = map[string][]*rebCT    // EC CTs grouped by a rule
	ctWaitList = map[string]*waitObject // object uid <-> list of CTs
	// a full info about an object that resides on the local target.
	// Contains both global and calculated local info
	rebObject struct {
		cts          ctList            // obj hash <-> list of CTs with the same hash
		hrwTargets   []*cluster.Snode  // the list of targets that should have CT
		rebuildSGLs  []*memsys.SGL     // temporary slices for [re]building EC
		fh           *cmn.FileHandle   // main fh when building slices from existing main
		sender       *cluster.Snode    // target responsible to send replicas over the cluster (first by HRW)
		locCT        map[string]*rebCT // CT locations: maps daemonID to CT for faster check what nodes have the CT
		ctExist      []bool            // marks existing CT: SliceID <=> Exists
		mainDaemon   string            // hrw target for an object
		uid          string            // unique identifier for the object (Bucket#Object#IsAIS)
		bck          cmn.Bck
		objName      string // object name for faster access
		objSize      int64  // object size
		sliceSize    int64  // a size of an object slice
		dataSlices   int16  // the number of data slices
		paritySlices int16  // the number of parity slices
		mainSliceID  int16  // sliceID on the main target
		isECCopy     bool   // replicated or erasure coded
		hasCT        bool   // local target has any obj's CT
		mainHasAny   bool   // is default target has any part of the object
		isMain       bool   // is local target a default one
		inHrwList    bool   // is local target should have any CT according to HRW
		fullObjFound bool   // some target has the full object, no need to rebuild, just copy
		hasAllSlices bool   // true: all slices existed before rebalance
	}
	rebBck struct {
		objs map[string]*rebObject // maps ObjectName <-> object info
	}
	// final result of scanning the existing objects
	globalCTList struct {
		ais   map[string]*rebBck // maps BucketName <-> map of objects
		cloud map[string]*rebBck // maps BucketName <-> map of objects
	}

	// CT destination (to use later for retransmitting lost CTs)
	retransmitCT struct {
		daemonID string           // destination
		header   transport.Header // original transport header copy
		sliceID  int16
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
		waiter *ctWaiter // helper to manage a list of CT for current batch to wait by local target
		broken []*rebObject
		ackCTs ackCT
		onAir  atomic.Int64 // the number of CTs passed to transport but not yet sent to a remote target

		// list of CTs that should be moved between local mpaths
		localActions []*rebCT
	}
	// a description of a CT that local target awaits from another target
	waitCT struct {
		mtx     sync.Mutex
		sgl     *memsys.SGL  // SGL to save the received CT
		meta    *ec.Metadata // CT's EC metadata
		sliceID int16        // slice ID to wait (special value `anySliceID` - wait for the first slice for the object)
		recv    atomic.Bool  // if this CT has been received already (for proper clean, rebuild, and check)
	}
	// helper object that manages slices the local target waits for from remote targets
	ctWaiter struct {
		mx        sync.Mutex
		waitFor   atomic.Int32 // the current number of slices local target awaits
		toRebuild atomic.Int32 // the current number of objects the target has to rebuild
		objs      ctWaitList   // the CT for the current batch
		mem       *memsys.MMSA
	}
)

// Generate unique ID for an object (all its slices gets the same uid)
func uniqueWaitID(bck cmn.Bck, objName string) string {
	return fmt.Sprintf("%s/%s", bck, objName)
}

//
// Rebalance object methods
// All methods should be called only after it is clear that the object exists
// or we have one or few slices. That is why `Assert` is used.
// And since a new slice is added to the list only if it matches previously
// added one, it is OK to read all info from the very first slice always
//

// Returns the list of slices with the same object hash that is the newest one.
// In majority of cases the object will contain only one list.
// TODO: implement better detection of the newest object version. Now the newest
//  is determined only by the number of slices: the newest has the biggest number
func (so *rebObject) newest() []*rebCT {
	var l []*rebCT
	max := 0
	for _, ctList := range so.cts {
		if max < len(ctList) {
			max = len(ctList)
			l = ctList
		}
	}
	return l
}

// Returns how many CTs (including the original object) must exists
func (so *rebObject) requiredCT() int {
	if so.isECCopy {
		return int(so.paritySlices + 1)
	}
	return int(so.dataSlices + so.paritySlices + 1)
}

// Returns how many CTs found across all targets
func (so *rebObject) foundCT() int {
	return len(so.locCT)
}

// Returns the list of targets that does not have any CT but
// they should have according to HRW.
func (so *rebObject) emptyTargets(skip *cluster.Snode) cluster.Nodes {
	freeTargets := make(cluster.Nodes, 0)
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

func (rt *retransmitCT) equal(rhs *retransmitCT) bool {
	return rt.sliceID == rhs.sliceID &&
		rt.header.ObjName == rhs.header.ObjName &&
		rt.daemonID == rhs.daemonID &&
		rt.header.Bck.Equal(rhs.header.Bck)
}

func (ack *ackCT) add(rt *retransmitCT) {
	cmn.Assert(rt.header.ObjName != "" && rt.header.Bck.Name != "")
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
	ack.mtx.Unlock()
}

// Returns the number of ACKs that the node have not received yet
func (ack *ackCT) waiting() int {
	ack.mtx.Lock()
	cnt := len(ack.ct)
	ack.mtx.Unlock()
	return cnt
}

// Keep allocated capacity for the next batch
func (ack *ackCT) clear(mm *memsys.MMSA) {
	ack.mtx.Lock()
	for _, rt := range ack.ct {
		mm.Free(rt.header.Opaque)
	}
	ack.ct = ack.ct[:0]
	ack.mtx.Unlock()
}

//
//  Rebalance result methods
//

// Merge given CT with already existing CTs of an object.
// It checks if the CT is unique(in case of the object is erasure coded),
// and the CT's info about object matches previously found CTs.
func (rr *globalCTList) addCT(md *rebArgs, ct *rebCT, tgt cluster.Target) error {
	bckList := rr.ais
	if cmn.IsProviderCloud(ct.Bck, false /*acceptAnon*/) {
		bckList = rr.cloud
	}
	bck, ok := bckList[ct.Name]
	if !ok {
		bck = &rebBck{objs: make(map[string]*rebObject)}
		bckList[ct.Name] = bck
	}

	b := cluster.NewBckEmbed(ct.Bck)
	if err := b.Init(tgt.GetBowner(), tgt.Snode()); err != nil {
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
					return err
				}
				if tgt.ID() == ct.DaemonID {
					found.DaemonID = ct.DaemonID
					if found.meta == nil {
						found.meta = ct.meta
					}
					// Copy the following fields as they are vital for local slices
					found.hrwFQN = ct.hrwFQN
					found.realFQN = ct.realFQN
					err = fmt.Errorf("duplicated %s/%s SliceID %d replaced daemonID %s with %s",
						ct.Name, ct.ObjName, ct.SliceID, found.DaemonID, ct.DaemonID)
					return err
				}
			}
			return err
		}
	}
	obj.cts[ct.ObjHash] = append(obj.cts[ct.ObjHash], ct)
	return nil
}

//
// EC Rebalancer methods and utilities
//

func newECData(t cluster.Target) *ecData {
	return &ecData{
		waiter:       newWaiter(t.GetMMSA()),
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

// Sends local CT along with EC metadata to default target.
// The CT is on a local drive and not loaded into SGL. Just read and send.
func (reb *Manager) sendFromDisk(ct *rebCT, target *cluster.Snode) error {
	var (
		lom *cluster.LOM
		fqn = ct.realFQN
	)
	cmn.Assert(ct.meta != nil)
	if ct.hrwFQN != "" {
		fqn = ct.hrwFQN
	}
	lom = &cluster.LOM{T: reb.t, FQN: fqn}
	if err := lom.Init(ct.Bck); err != nil {
		return err
	}
	if lom.ParsedFQN.ContentType == fs.ObjectType {
		if err := lom.Load(false); err != nil {
			return err
		}
	} else {
		lom = nil // sending slice
	}
	// open
	fh, err := cmn.NewFileHandle(fqn)
	if err != nil {
		return err
	}

	// transmit
	var (
		req = pushReq{
			daemonID: reb.t.Snode().ID(),
			stage:    rebStageECRepair,
			rebID:    reb.rebID.Load(),
			md:       ct.meta,
		}
		mm  = reb.t.GetSmallMMSA()
		hdr = transport.Header{
			Bck:      ct.Bck,
			ObjName:  ct.ObjName,
			ObjAttrs: transport.ObjectAttrs{Size: ct.ObjSize},
		}
	)
	if lom != nil {
		hdr.ObjAttrs.Atime = lom.AtimeUnix()
		hdr.ObjAttrs.Version = lom.Version()
		if cksum := lom.Cksum(); cksum != nil {
			hdr.ObjAttrs.CksumType, hdr.ObjAttrs.CksumValue = cksum.Get()
		}
	}
	if ct.SliceID != 0 {
		hdr.ObjAttrs.Size = ec.SliceSize(ct.ObjSize, int(ct.DataSlices))
	}
	reb.ec.onAir.Inc()
	hdr.Opaque = req.NewPack(mm)

	if err := reb.streams.Send(transport.Obj{Hdr: hdr, Callback: reb.transportECCB}, fh, target); err != nil {
		reb.ec.onAir.Dec()
		mm.Free(hdr.Opaque)
		fh.Close()
		return fmt.Errorf("failed to send slices to nodes [%s..]: %v", target.ID(), err)
	}
	reb.statRunner.AddMany(
		stats.NamedVal64{Name: stats.TxRebCount, Value: 1},
		stats.NamedVal64{Name: stats.TxRebSize, Value: hdr.ObjAttrs.Size},
	)
	return nil
}

// Track the number of sent CTs
func (reb *Manager) transportECCB(hdr transport.Header, reader io.ReadCloser, _ unsafe.Pointer, _ error) {
	reb.ec.onAir.Dec()
	reb.t.GetSmallMMSA().Free(hdr.Opaque)
}

// Sends reconstructed slice along with EC metadata to remote target.
// EC metadata is of main object, so its internal field SliceID must be
// fixed prior to sending.
// Use the function to send only slices (not for replicas/full object)
func (reb *Manager) sendFromReader(reader cmn.ReadOpenCloser,
	ct *rebCT, sliceID int, xxhash string, target *cluster.Snode, rt *retransmitCT) error {
	cmn.AssertMsg(ct.meta != nil, ct.ObjName)
	cmn.AssertMsg(ct.ObjSize != 0, ct.ObjName)
	cmn.Assert(rt != nil)
	var (
		newMeta = *ct.meta // copy of meta (flat struct of primitive types)
		req     = pushReq{
			daemonID: reb.t.Snode().ID(),
			stage:    rebStageECRepair,
			rebID:    reb.rebID.Load(),
			md:       &newMeta,
		}
		size = ec.SliceSize(ct.ObjSize, int(ct.DataSlices))
		hdr  = transport.Header{
			Bck:      ct.Bck,
			ObjName:  ct.ObjName,
			ObjAttrs: transport.ObjectAttrs{Size: size},
		}
		mm = reb.t.GetSmallMMSA()
	)
	newMeta.SliceID = sliceID
	hdr.Opaque = req.NewPack(mm)
	if xxhash != "" {
		hdr.ObjAttrs.CksumValue = xxhash
		hdr.ObjAttrs.CksumType = cmn.ChecksumXXHash
	}

	rt.daemonID = target.ID()
	rt.header = hdr
	reb.ec.ackCTs.add(rt)

	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("sending slice %d(%d)[%s] of %s/%s to %s", sliceID, size, xxhash, ct.Bck.Name, ct.ObjName, target)
	}
	reb.ec.onAir.Inc()
	if err := reb.streams.Send(transport.Obj{Hdr: hdr, Callback: reb.transportECCB}, reader, target); err != nil {
		reb.ec.onAir.Dec()
		mm.Free(hdr.Opaque)
		reb.ec.ackCTs.remove(rt)
		return fmt.Errorf("failed to send slices to node %s: %v", target, err)
	}
	reb.statRunner.AddMany(
		stats.NamedVal64{Name: stats.TxRebCount, Value: 1},
		stats.NamedVal64{Name: stats.TxRebSize, Value: size},
	)
	return nil
}

// Saves received CT to a local drive if needed:
//   1. Full object/replica is received
//   2. A CT is received and this target is not the default target (it
//      means that the CTs came from default target after EC had been rebuilt)
func (reb *Manager) saveCTToDisk(data *memsys.SGL, req *pushReq, hdr transport.Header) error {
	cmn.Assert(req.md != nil)
	var (
		ctFQN    string
		lom      *cluster.LOM
		bck      = cluster.NewBckEmbed(hdr.Bck)
		needSave = req.md.SliceID == 0 // full object always saved
	)
	if err := bck.Init(reb.t.GetBowner(), reb.t.Snode()); err != nil {
		return err
	}
	uname := bck.MakeUname(hdr.ObjName)
	if !needSave {
		// slice is saved only if this target is not "main" one.
		// Main one receives slices as well but it uses them only to rebuild "full"
		tgt, err := cluster.HrwTarget(uname, reb.t.GetSowner().Get())
		if err != nil {
			return err
		}
		needSave = tgt.ID() != reb.t.Snode().ID()
	}
	if !needSave {
		return nil
	}
	mpath, _, err := cluster.HrwMpath(uname)
	if err != nil {
		return err
	}
	if req.md.SliceID != 0 {
		ctFQN = mpath.MakePathFQN(hdr.Bck, ec.SliceType, hdr.ObjName)
	} else {
		lom = &cluster.LOM{T: reb.t, ObjName: hdr.ObjName}
		if err := lom.Init(hdr.Bck); err != nil {
			return err
		}
		ctFQN = lom.FQN
		lom.SetSize(hdr.ObjAttrs.Size)
		if hdr.ObjAttrs.Version != "" {
			lom.SetVersion(hdr.ObjAttrs.Version)
		}
		if hdr.ObjAttrs.CksumType != "" {
			lom.SetCksum(cmn.NewCksum(hdr.ObjAttrs.CksumType, hdr.ObjAttrs.CksumValue))
		}
		if hdr.ObjAttrs.Atime != 0 {
			lom.SetAtimeUnix(hdr.ObjAttrs.Atime)
		}
		lom.Lock(true)
		defer lom.Unlock(true)
		lom.Uncache()
	}

	buffer, slab := reb.t.GetMMSA().Alloc()
	metaFQN := mpath.MakePathFQN(hdr.Bck, ec.MetaType, hdr.ObjName)
	_, err = cmn.SaveReader(metaFQN, bytes.NewReader(req.md.Marshal()), buffer, false)
	if err != nil {
		slab.Free(buffer)
		return err
	}
	tmpFQN := mpath.MakePathFQN(hdr.Bck, fs.WorkfileType, hdr.ObjName)
	cksum, err := cmn.SaveReaderSafe(tmpFQN, ctFQN, memsys.NewReader(data), buffer, true)
	if err == nil {
		if req.md.SliceID == 0 && hdr.ObjAttrs.CksumType == cmn.ChecksumXXHash && hdr.ObjAttrs.CksumValue != cksum.Value() {
			err = fmt.Errorf("mismatched hash for %s/%s, version %s, hash calculated %s/header %s/md %s",
				hdr.Bck, hdr.ObjName, hdr.ObjAttrs.Version, cksum.Value(), hdr.ObjAttrs.CksumValue, req.md.ObjCksum)
		}
	}
	slab.Free(buffer)
	if err != nil {
		// Persist may call FSHC, too. To avoid double FSHC call, do extra check now.
		reb.t.FSHC(err, ctFQN)
	} else if req.md.SliceID == 0 {
		err = lom.Persist()
	}

	if err != nil {
		os.Remove(tmpFQN)
		if rmErr := os.Remove(metaFQN); rmErr != nil && !os.IsNotExist(rmErr) {
			glog.Errorf("nested error: save replica -> remove metadata file: %v", rmErr)
		}
		if rmErr := os.Remove(ctFQN); rmErr != nil && !os.IsNotExist(rmErr) {
			glog.Errorf("nested error: save replica -> remove replica: %v", rmErr)
		}
	}

	return err
}

// Receives a CT from another target, saves to local drive because it is a missing one
func (reb *Manager) receiveCT(req *pushReq, hdr transport.Header, reader io.Reader) error {
	const fmtErrCleanedUp = "%s/%s slice %d has been already cleaned up"
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("%s GOT CT for %s/%s from %s", reb.t.Snode(), hdr.Bck, hdr.ObjName, req.daemonID)
	}
	cmn.Assert(req.md != nil)

	reb.laterx.Store(true)
	sliceID := req.md.SliceID
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof(">>> %s got CT %d for %s/%s %d", reb.t.Snode(), sliceID, hdr.Bck, hdr.ObjName, hdr.ObjAttrs.Size)
	}
	uid := uniqueWaitID(hdr.Bck, hdr.ObjName)
	waitFor := reb.ec.waiter.lookupCreate(uid, int16(sliceID), waitForSingleSlice)
	if waitFor == nil {
		return fmt.Errorf(fmtErrCleanedUp, hdr.Bck, hdr.ObjName, sliceID)
	}
	waitFor.mtx.Lock()
	defer waitFor.mtx.Unlock()
	// Double check - sgl could be freed while awaiting Lock
	if waitFor.sgl == nil {
		return fmt.Errorf(fmtErrCleanedUp, hdr.Bck, hdr.ObjName, sliceID)
	}
	if waitFor.recv.Load() {
		return nil
	}

	waitFor.meta = req.md
	n, err := io.Copy(waitFor.sgl, reader)
	if err != nil {
		return fmt.Errorf("failed to read slice %d for %s/%s: %v", sliceID, hdr.Bck, hdr.ObjName, err)
	}
	reb.statRunner.AddMany(
		stats.NamedVal64{Name: stats.RxRebCount, Value: 1},
		stats.NamedVal64{Name: stats.RxRebSize, Value: n},
	)
	if hdr.ObjAttrs.CksumValue != "" {
		ckval, _ := cksumForSlice(memsys.NewReader(waitFor.sgl), waitFor.sgl.Size(), reb.t.GetMMSA())
		if hdr.ObjAttrs.CksumValue != ckval {
			reb.statRunner.AddMany(stats.NamedVal64{Name: stats.ErrCksumCount, Value: 1})
			return cmn.NewBadDataCksumError(
				cmn.NewCksum(cmn.ChecksumXXHash, ckval),
				cmn.NewCksum(hdr.ObjAttrs.CksumType, hdr.ObjAttrs.CksumValue),
				"rebalance")
		}
	}
	waitFor.recv.Store(true)
	waitRebuild := reb.ec.waiter.updateRebuildInfo(uid)

	if !waitRebuild || sliceID == 0 {
		if err := reb.saveCTToDisk(waitFor.sgl, req, hdr); err != nil {
			glog.Errorf("failed to save CT %d of %s: %v", sliceID, hdr.ObjName, err)
			reb.abortRebalance()
		}
	}

	// notify that another slice is received successfully
	remains := reb.ec.waiter.waitFor.Dec()
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("CTs to get remains: %d", remains)
	}

	// ACK
	var (
		smap = (*cluster.Smap)(reb.smap.Load())
		tsi  = smap.GetTarget(req.daemonID)
		mm   = reb.t.GetSmallMMSA()
		ack  = &ecAck{sliceID: uint16(sliceID), daemonID: reb.t.Snode().ID()}
	)
	hdr.Opaque = ack.NewPack(mm)
	hdr.ObjAttrs.Size = 0
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("Sending ACK for %s/%s to %s", hdr.Bck, hdr.ObjName, tsi.ID())
	}
	if err := reb.acks.Send(transport.Obj{Hdr: hdr, Callback: reb.eackSentCallback}, nil, tsi); err != nil {
		mm.Free(hdr.Opaque)
		glog.Error(err)
	}

	return nil
}

func (reb *Manager) eackSentCallback(hdr transport.Header, _ io.ReadCloser, _ unsafe.Pointer, _ error) {
	reb.t.GetSmallMMSA().Free(hdr.Opaque)
}

// On receiving an EC CT or EC namespace
func (reb *Manager) recvECData(hdr transport.Header, unpacker *cmn.ByteUnpack, reader io.Reader) {
	defer cmn.DrainReader(reader)

	req := &pushReq{}
	err := unpacker.ReadAny(req)
	if err != nil {
		glog.Errorf("invalid push notification: %v", err)
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

	b, err := ioutil.ReadAll(reader)
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
		ais:   make(map[string]*rebBck),
		cloud: make(map[string]*rebBck),
	}

	// process all received CTs
	localDaemon := reb.t.Snode().ID()
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
				if err := b.Init(reb.t.GetBowner(), reb.t.Snode()); err != nil {
					reb.abortRebalance()
					return nil
				}
				t, err := cluster.HrwTarget(b.MakeUname(ct.ObjName), md.smap)
				cmn.Assert(err == nil)
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
				glog.Warning(err)
				continue
			}
			if local && ct.hrwFQN != ct.realFQN {
				reb.ec.localActions = append(reb.ec.localActions, ct)
				if bool(glog.FastV(4, glog.SmoduleReb)) {
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
	bowner := reb.t.GetBowner()
	bmd := bowner.Get()

	providers := map[string]map[string]*rebBck{
		cmn.ProviderAIS:              res.ais,
		cmn.GCO.Get().Cloud.Provider: res.cloud,
	}
	for provider, tp := range providers {
		for bckName, objs := range tp {
			if provider == "" {
				// Cloud provider can be empty so we do not need to do anything.
				continue
			}

			bck := cluster.NewBck(bckName, provider, cmn.NsGlobal)
			if err := bck.Init(bowner, reb.t.Snode()); err != nil {
				// bucket might be deleted while rebalancing - skip it
				glog.Errorf("invalid bucket %s: %v", bckName, err)
				delete(tp, bckName)
				continue
			}
			bprops, ok := bmd.Get(bck)
			if !ok {
				// bucket might be deleted while rebalancing - skip it
				glog.Errorf("bucket %s does not exist", bckName)
				delete(tp, bckName)
				continue
			}
			// Select only objects that have "main" part missing or misplaced.
			// Other missing slices/replicas must be added to ZIL
			for objName, obj := range objs.objs {
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

				if bool(glog.FastV(4, glog.SmoduleReb)) {
					glog.Infof("[%s] BROKEN: %s [Main %d on %s], CTs %d of %d",
						reb.t.Snode(), objName, obj.mainSliceID, obj.mainDaemon, obj.foundCT(), obj.requiredCT())
				}
				reb.ec.broken = append(reb.ec.broken, obj)
			}
		}
	}

	// sort the list of broken object to have deterministic order on all targets
	// sort order: IsAIS/Bucket name/Object name
	ctLess := func(i, j int) bool {
		if reb.ec.broken[i].bck.Provider != reb.ec.broken[j].bck.Provider {
			return cmn.IsProviderAIS(reb.ec.broken[j].bck)
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

	ct, err := cluster.NewCTFromFQN(fqn, reb.t.GetBowner())
	if err != nil {
		return nil
	}
	// do not touch directories for buckets with EC disabled (for now)
	// TODO: what to do if we found metafile on a bucket with EC disabled?
	if !ct.Bprops().EC.Enabled {
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
		hrwFQN, _, err = cluster.HrwFQN(ct.Bck(), fs.ObjectType, ct.ObjName())
	} else {
		hrwFQN, _, err = cluster.HrwFQN(ct.Bck(), ec.SliceType, ct.ObjName())
	}
	if err != nil {
		return err
	}

	ct, err = cluster.NewCTFromFQN(fileFQN, reb.t.GetBowner())
	if err != nil {
		return nil
	}

	id := reb.t.Snode().ID()
	rec := &rebCT{
		Bck:          ct.Bck().Bck,
		ObjName:      ct.ObjName(),
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
	reb.ec.waiter.cleanup()
}

// Main method - starts all mountpaths walkers, waits for them to finish, and
// changes internal stage after that to 'traverse done', so the caller may continue
// rebalancing: send collected data to other targets, rebuild slices etc
func (reb *Manager) runEC() {
	var (
		wg                = &sync.WaitGroup{}
		availablePaths, _ = fs.Mountpaths.Get()
		cfg               = cmn.GCO.Get()
		bck               = reb.xact().Bck()
	)

	for _, mpathInfo := range availablePaths {
		bck := cmn.Bck{Name: bck.Name, Provider: cmn.ProviderAIS, Ns: bck.Ns}
		wg.Add(1)
		go reb.jogEC(mpathInfo, bck, wg)
	}

	if cfg.Cloud.Supported {
		for _, mpathInfo := range availablePaths {
			bck := cmn.Bck{Name: bck.Name, Provider: cfg.Cloud.Provider, Ns: bck.Ns}
			wg.Add(1)
			go reb.jogEC(mpathInfo, bck, wg)
		}
	}
	wg.Wait()
	reb.changeStage(rebStageECNamespace, 0)
}

// send collected CTs to all targets with retry (to assemble the entire namespace)
func (reb *Manager) exchange(md *rebArgs) error {
	const (
		retries = 3               // number of retries to send collected CT info
		sleep   = 5 * time.Second // delay between retries
	)
	var (
		rebID   = reb.rebID.Load()
		sendTo  = make(cluster.Nodes, 0, len(md.smap.Tmap))
		failed  = make(cluster.Nodes, 0, len(md.smap.Tmap))
		emptyCT = make([]*rebCT, 0)
	)
	for _, node := range md.smap.Tmap {
		if node.ID() == reb.t.Snode().ID() {
			continue
		}
		sendTo = append(sendTo, node)
	}
	cts, ok := reb.ec.nodeData(reb.t.Snode().ID())
	if !ok {
		cts = emptyCT // no data collected for the target, send empty notification
	}
	var (
		req = pushReq{
			daemonID: reb.t.Snode().ID(),
			stage:    rebStageECNamespace,
			rebID:    rebID,
		}
		body   = cmn.MustMarshal(cts)
		opaque = req.NewPack(nil)
		hdr    = transport.Header{
			ObjAttrs: transport.ObjectAttrs{Size: int64(len(body))},
			Opaque:   opaque,
		}
	)
	for i := 0; i < retries; i++ {
		failed = failed[:0]
		for _, node := range sendTo {
			if reb.xact().Aborted() {
				return cmn.NewAbortedError("exchange")
			}
			rd := cmn.NewByteHandle(body)
			if err := reb.streams.Send(transport.Obj{Hdr: hdr}, rd, node); err != nil {
				glog.Errorf("Failed to send CTs to node %s: %v", node.ID(), err)
				failed = append(failed, node)
			}
			reb.statRunner.AddMany(
				stats.NamedVal64{Name: stats.TxRebCount, Value: 1},
				stats.NamedVal64{Name: stats.TxRebSize, Value: int64(len(body))},
			)
		}
		if len(failed) == 0 {
			reb.changeStage(rebStageECDetect, 0)
			return nil
		}
		time.Sleep(sleep)
		copy(sendTo, failed)
	}
	return fmt.Errorf("could not send data to %d nodes", len(failed))
}

func (reb *Manager) resilverSlice(fromFQN, toFQN string, buf []byte) error {
	if _, _, err := cmn.CopyFile(fromFQN, toFQN, buf, false); err != nil {
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
	lom := &cluster.LOM{T: reb.t, FQN: fromFQN}
	err := lom.Init(fromMpath.Bck)
	if err == nil {
		err = lom.Load()
	}
	if err != nil {
		return err
	}

	lom.Lock(true)
	lom.Uncache()
	_, err = lom.CopyObject(toFQN, buf)
	lom.Unlock(true)
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
	buf, slab := reb.t.GetMMSA().Alloc()
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
		_, _, err = cmn.CopyFile(metaSrcFQN, metaDstFQN, buf, false)
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
	localDaemon := reb.t.Snode().ID()
	cts := obj.newest()
	cmn.Assert(len(cts) != 0) // cannot happen
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

	genCount := cmn.Max(ctReq, len(smap.Tmap))
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

	cmn.Assert(obj.sender != nil) // must not happen
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("%s %s: hasSlice %v, fullObjExist: %v, isMain %v [mainHas: %v - %d]",
			reb.t.Snode(), obj.uid, obj.hasCT, obj.fullObjFound, obj.isMain, obj.mainHasAny, obj.mainSliceID)
		glog.Infof("%s: slice found %d vs required %d[all slices: %v], is in HRW %v [sender %s]",
			obj.uid, ctFound, ctReq, obj.hasAllSlices, obj.inHrwList, obj.sender)
	}
	return nil
}

// true if this target is not "default" one, and does not have any CT,
// and does not want any CT, the target can skip the object
func (reb *Manager) shouldSkipObj(obj *rebObject) bool {
	anyOnMain := obj.mainHasAny && obj.mainSliceID == 0
	noOnSecondary := !obj.hasCT && !obj.isMain
	notSender := obj.sender != nil && obj.isECCopy && obj.sender.ID() != reb.t.Snode().ID()
	skip := anyOnMain || noOnSecondary || notSender
	if skip && bool(glog.FastV(4, glog.SmoduleReb)) {
		glog.Infof("#0 SKIP %s - main has it", obj.uid)
		// TODO: else add to ZIL
	}
	return skip
}

// Get the ordinal number of a target in HRW list of targets that have a slice.
// Returns -1 if target is not found in the list.
func (reb *Manager) targetIndex(daemonID string, obj *rebObject) int {
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
	tgtIndex := reb.targetIndex(reb.t.Snode().ID(), obj)
	shouldSend = tgtIndex >= 0 && tgtIndex < int(obj.dataSlices)
	hasSlice = obj.hasCT && !obj.isMain && !obj.isECCopy && !obj.fullObjFound
	if hasSlice && (bool(glog.FastV(4, glog.SmoduleReb))) {
		locSlice := obj.locCT[reb.t.Snode().ID()]
		glog.Infof("should send: %s[%d] - %d : %v / %v", obj.uid, locSlice.SliceID, tgtIndex,
			hasSlice, shouldSend)
	}
	return hasSlice, shouldSend
}

// true if the object is not replicated, and this target has full object, and the
// target is not the default target, and default target does not have full object
func (reb *Manager) hasFullObjMisplaced(obj *rebObject) bool {
	locCT, ok := obj.locCT[reb.t.Snode().ID()]
	return ok && !obj.isECCopy && !obj.isMain && locCT.SliceID == 0 &&
		(!obj.mainHasAny || obj.mainSliceID != 0)
}

// Read CT from local drive and send to another target.
// If destination is not defined the target sends its data to "default by HRW" target
func (reb *Manager) sendLocalData(md *rebArgs, obj *rebObject, si ...*cluster.Snode) error {
	reb.laterx.Store(true)
	ct, ok := obj.locCT[reb.t.Snode().ID()]
	cmn.Assert(ok)
	var target *cluster.Snode
	if len(si) != 0 {
		target = si[0]
	} else {
		mainSI, ok := md.smap.Tmap[obj.mainDaemon]
		cmn.Assert(ok)
		target = mainSI
	}
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("%s sending a slice/replica #%d of %s to main %s", reb.t.Snode(), ct.SliceID, ct.ObjName, target)
	}
	return reb.sendFromDisk(ct, target)
}

// Sends replica to the default target.
func (reb *Manager) sendLocalReplica(md *rebArgs, obj *rebObject) error {
	cmn.Assert(obj.sender != nil) // mustn't happen
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("%s object %s sender %s", reb.t.Snode(), obj.uid, obj.sender)
	}
	// Another node should send replicas, do noting
	if obj.sender.DaemonID != reb.t.Snode().ID() {
		return nil
	}

	sendTo := md.smap.Tmap[obj.mainDaemon]
	ct, ok := obj.locCT[reb.t.Snode().ID()]
	cmn.Assert(ok)
	if err := reb.sendFromDisk(ct, sendTo); err != nil {
		return fmt.Errorf("failed to send %s: %v", ct.ObjName, err)
	}
	return nil
}

func (reb *Manager) allCTReceived(md *rebArgs) bool {
	for {
		if reb.xact().Aborted() {
			return false
		}
		uid, wObj := reb.ec.waiter.nextReadyObj()
		if wObj == nil {
			break
		}
		var obj *rebObject
		batchCurr := int(reb.stages.currBatch.Load())
		for j := 0; j+batchCurr < len(reb.ec.broken) && j < md.config.EC.BatchSize; j++ {
			o := reb.ec.broken[j+batchCurr]
			if uid == o.uid {
				obj = o
				break
			}
		}
		cmn.Assert(obj != nil)
		// Rebuild only if there were missing slices or main object.
		// Otherwise, just mark it done and continue.
		rebuildSlices := obj.isECCopy && !obj.hasAllSlices
		rebuildObject := !obj.mainHasAny && !obj.fullObjFound
		if rebuildSlices || rebuildObject {
			if err := reb.rebuildAndSend(obj, wObj.cts); err != nil {
				glog.Errorf("Failed to rebuild %s: %v", uid, err)
			}
		}
		wObj.status = objDone
		reb.ec.waiter.toRebuild.Dec()
	}
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("CT waitFor: %d, toRebuild: %d", reb.ec.waiter.waitFor.Load(), reb.ec.waiter.toRebuild.Load())
	}

	// must be the last check, because even if a target has all slices
	// it may need to rebuild and send repaired slices
	return reb.ec.waiter.waitFor.Load() == 0 && reb.ec.waiter.toRebuild.Load() == 0
}

func (reb *Manager) allNodesCompletedBatch(md *rebArgs) bool {
	cnt := 0
	batchID := reb.stages.currBatch.Load()
	reb.stages.mtx.Lock()
	smap := md.smap.Tmap
	for _, si := range smap {
		if si.ID() == reb.t.Snode().ID() {
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

func (reb *Manager) waitForFullObject(obj *rebObject) error {
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("#5.3/4 Waiting for an object %s", obj.uid)
	}
	reb.ec.waiter.lookupCreate(obj.uid, 0, waitForReplica)
	reb.ec.waiter.updateRebuildInfo(obj.uid)
	reb.ec.waiter.toRebuild.Inc()
	return nil
}

func (reb *Manager) waitForExistingSlices(obj *rebObject) (err error) {
	for _, sl := range obj.locCT {
		// case with sliceID == 0 must be processed in the beginning
		cmn.Assert(sl.SliceID != 0)

		// wait slices only from `dataSliceCount` first HRW targets
		tgtIndex := reb.targetIndex(sl.DaemonID, obj)
		if tgtIndex < 0 || tgtIndex >= int(obj.dataSlices) {
			if glog.FastV(4, glog.SmoduleReb) {
				glog.Infof("#5.5 Waiting for slice %d %s - [SKIPPED %d]", sl.SliceID, obj.uid, tgtIndex)
			}
			continue
		}

		if glog.FastV(4, glog.SmoduleReb) {
			glog.Infof("#5.5 Waiting for slice %d %s", sl.SliceID, obj.uid)
		}
		reb.ec.waiter.lookupCreate(obj.uid, sl.SliceID, waitForAllSlices)
	}
	reb.ec.waiter.updateRebuildInfo(obj.uid)
	reb.ec.waiter.toRebuild.Inc()
	return nil
}

func (reb *Manager) restoreReplicas(md *rebArgs, obj *rebObject) (err error) {
	// add to ZIL if any replica is missing and it is main target
	if obj.isMain && obj.mainHasAny {
		if glog.FastV(4, glog.SmoduleReb) {
			glog.Infof("#4 Skip replicas sending %s - main has it", obj.uid)
		}
		return nil
	}
	if obj.isMain {
		if glog.FastV(4, glog.SmoduleReb) {
			glog.Infof("#4 Waiting for replica %s", obj.uid)
		}
		reb.ec.waiter.lookupCreate(obj.uid, 0, waitForSingleSlice)
	}
	return reb.sendLocalReplica(md, obj)
}

func (reb *Manager) rebalanceObject(md *rebArgs, obj *rebObject) (err error) {
	// Case #1: this target does not have to do anything
	if reb.shouldSkipObj(obj) {
		if glog.FastV(4, glog.SmoduleReb) {
			glog.Infof("SKIPPING %s", obj.uid)
		}
		return nil
	}

	// Case #2: this target has someone's main object
	if reb.hasFullObjMisplaced(obj) {
		return reb.sendLocalData(md, obj)
	}

	// Case #3: this target has a slice while the main must be restored.
	// Send local slice only if this target is in `dataSliceCount` first
	// targets which have any slice.
	hasSlice, shouldSend := reb.shouldSendSlice(obj)
	if !obj.fullObjFound && hasSlice {
		if shouldSend {
			return reb.sendLocalData(md, obj)
		}
		return nil
	}

	// Case #4: object was replicated
	if obj.isECCopy {
		return reb.restoreReplicas(md, obj)
	}

	// Case #5: the object is erasure coded

	// Case #5.1: it is not main target and has slice or does not need any
	if !obj.isMain && (obj.hasCT || !obj.inHrwList) {
		if glog.FastV(4, glog.SmoduleReb) {
			glog.Infof("#5.1 Object %s skipped", obj.uid)
		}
		return nil
	}

	// Case #5.3: main has nothing, but full object and all slices exists
	if obj.isMain && !obj.mainHasAny && obj.fullObjFound {
		return reb.waitForFullObject(obj)
	}

	// Case #5.4: main has a slice instead of a full object, send local
	// slice to a free target and wait for another target sends the full obj
	if obj.isMain && obj.mainHasAny && obj.mainSliceID != 0 && obj.fullObjFound {
		// TODO: add to ZIL
		return nil
	}

	// Case #5.5: it is main target and full object is missing
	if obj.isMain && !obj.fullObjFound {
		return reb.waitForExistingSlices(obj)
	}

	cmn.AssertMsg(obj.isMain && obj.mainHasAny && obj.mainSliceID == 0,
		fmt.Sprintf("%s%s/%s: isMain %t - mainHasSome %t - mainID %d",
			reb.t.Snode(), obj.bck, obj.objName, obj.isMain, obj.mainHasAny, obj.mainSliceID))
	return nil
}

func (reb *Manager) cleanupBatch(md *rebArgs) {
	reb.ec.waiter.cleanupBatch(md, reb.ec.broken, int(reb.stages.currBatch.Load()))
	reb.releaseSGLs(md, reb.ec.broken)
	reb.ec.ackCTs.clear(reb.t.GetSmallMMSA())
}

// Wait for all targets to finish the current batch and then free allocated resources
func (reb *Manager) finalizeBatch(md *rebArgs) error {
	// First, wait for all slices the local target wants to receive
	maxWait := md.config.Rebalance.Quiesce
	if aborted := reb.waitQuiesce(md, maxWait, reb.allCTReceived); aborted {
		reb.ec.waiter.waitFor.Store(0)
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
		glog.Infof("%s waiting for ALL batch %d DONE", reb.t.Snode().ID(), reb.stages.currBatch.Load())
	}
	if aborted := reb.waitEvent(md, reb.allNodesCompletedBatch); aborted {
		reb.ec.waiter.waitFor.Store(0)
		return cmn.NewAbortedError("finalize batch - wait nodes to complete")
	}

	return nil
}

// Add to log all slices that were sent to targets but this target
// has not received ACKs
func (reb *Manager) logNoAck() {
	reb.ec.ackCTs.mtx.Lock()
	for _, dest := range reb.ec.ackCTs.ct {
		// TODO: add to ZIL
		reb.ec.ackCTs.remove(dest)
	}
	reb.ec.ackCTs.mtx.Unlock()
}

func (reb *Manager) allAckReceived(_ *rebArgs) bool {
	if reb.xact().Aborted() {
		return false
	}
	return reb.ec.ackCTs.waiting() == 0
}

func (reb *Manager) waitECAck(md *rebArgs) {
	logHdr := reb.logHdr(md)
	sleep := md.config.Timeout.CplaneOperation // NOTE: TODO: used throughout; must be separately assigned and calibrated

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
	if aborted := reb.waitQuiesce(md, sleep, reb.nodesQuiescent); aborted {
		return
	}

	// now check that all ACKs received after quiescent state
	maxWait := md.config.Rebalance.Quiesce
	if aborted := reb.waitEvent(md, reb.allAckReceived, maxWait); aborted {
		return
	}
	reb.logNoAck()
}

// Rebalances the current batch of broken objects
func (reb *Manager) rebalanceBatch(md *rebArgs, batchCurr int64) error {
	batchEnd := cmn.Min(int(batchCurr)+md.config.EC.BatchSize, len(reb.ec.broken))
	for objIdx := int(batchCurr); objIdx < batchEnd; objIdx++ {
		if reb.xact().Aborted() {
			return cmn.NewAbortedError("rebalance batch")
		}

		obj := reb.ec.broken[objIdx]
		if glog.FastV(4, glog.SmoduleReb) {
			glog.Infof("--- Starting object [%d] %s ---", objIdx, obj.uid)
		}
		cmn.Assert(len(obj.locCT) != 0) // cannot happen

		if err := reb.rebalanceObject(md, obj); err != nil {
			return err
		}
	}
	return nil
}

// Does cluster-wide rebalance
func (reb *Manager) rebalance(md *rebArgs) (err error) {
	batchCurr := int64(0)
	batchLast := int64(len(reb.ec.broken) - 1)
	reb.stages.currBatch.Store(batchCurr)
	reb.stages.lastBatch.Store(batchLast)
	for batchCurr <= batchLast {
		if glog.FastV(4, glog.SmoduleReb) {
			glog.Infof("Starting batch of %d from %d", md.config.EC.BatchSize, batchCurr)
		}

		if err = reb.rebalanceBatch(md, batchCurr); err != nil {
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
	for i := batchCurr; i < batchCurr+md.config.EC.BatchSize && i < len(objList); i++ {
		obj := objList[i]
		for _, sg := range obj.rebuildSGLs {
			if sg != nil {
				sg.Free()
			}
		}
		obj.rebuildSGLs = nil
		if obj.fh != nil {
			obj.fh.Close()
			obj.fh = nil
		}
	}
}

// when local target is a default one, and the full object is missing, the target
// receives existing slices with metadata, then it rebuild the object and missing
// slices. Finally it sends rebuilt slices to other targets, for this it needs
// correct metadata. The function generates metadata for a new slice for first
// received slice
func (reb *Manager) metadataForSlice(slices []*waitCT, sliceID int) *ec.Metadata {
	for _, sl := range slices {
		if !sl.recv.Load() {
			continue
		}
		md := *sl.meta
		md.SliceID = sliceID
		return &md
	}
	return nil
}

// Object is missing (and maybe a few slices as well). Default target receives all
// existing slices into SGLs, restores the object, rebuilds slices, and finally
// send missing slices to other targets
func (reb *Manager) rebuildFromSlices(obj *rebObject, slices []*waitCT) (err error) {
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("%s rebuilding slices (mem) of %s and send them", reb.t.Snode(), obj.objName)
	}

	var (
		meta *ec.Metadata

		slicesFound = int16(0)
		sliceCnt    = obj.dataSlices + obj.paritySlices
		readers     = make([]io.Reader, sliceCnt)
		// since io.Reader cannot be reopened, we need to have a copy for saving object
		rereaders = make([]io.Reader, sliceCnt)
		writers   = make([]io.Writer, sliceCnt)
	)
	obj.rebuildSGLs = make([]*memsys.SGL, sliceCnt)

	// put existing slices to readers list, and create SGL as writers for missing ones
	for _, sl := range slices {
		if !sl.recv.Load() {
			continue
		}
		id := sl.sliceID - 1
		cmn.Assert(readers[id] == nil)
		readers[id] = memsys.NewReader(sl.sgl)
		rereaders[id] = memsys.NewReader(sl.sgl)
		slicesFound++
		if meta == nil {
			meta = sl.meta
		}
	}

	cmn.Assert(meta != nil)
	ecMD := *meta // clone
	for i, rd := range readers {
		if rd != nil {
			continue
		}
		obj.rebuildSGLs[i] = reb.t.GetMMSA().NewSGL(cmn.MinI64(obj.sliceSize, cmn.MiB))
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
		cmn.Assert(obj.rebuildSGLs[i] != nil)
		srcReaders[i] = memsys.NewReader(obj.rebuildSGLs[i])
	}
	src := io.MultiReader(srcReaders...)
	objMD := ecMD // copy
	objMD.SliceID = 0

	if err := reb.restoreObject(obj, objMD, src); err != nil {
		return err
	}

	freeTargets := obj.emptyTargets(reb.t.Snode())
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
			return fmt.Errorf("failed to send slice %d of %s - no free target", sliceID, obj.uid)
		}
		cksumVal, err := cksumForSlice(memsys.NewReader(obj.rebuildSGLs[i]), obj.sliceSize, reb.t.GetMMSA())
		if err != nil {
			return fmt.Errorf("failed to calculate checksum of %s: %v", obj.objName, err)
		}
		reader := memsys.NewReader(obj.rebuildSGLs[i])
		si := freeTargets[0]
		freeTargets = freeTargets[1:]
		md := reb.metadataForSlice(slices, sliceID)
		cmn.Assert(md != nil)

		sliceMD := ecMD // copy
		sliceMD.SliceID = sliceID
		sl := &rebCT{
			Bck:          obj.bck,
			ObjName:      obj.objName,
			ObjSize:      sliceMD.Size,
			DaemonID:     reb.t.Snode().ID(),
			SliceID:      int16(sliceID),
			DataSlices:   int16(ecMD.Data),
			ParitySlices: int16(ecMD.Parity),
			meta:         &sliceMD,
		}

		// TODO: mark this retransmition as non-vital and just add it to LOG
		// if we do not receive ACK back
		dest := &retransmitCT{sliceID: int16(i + 1)}
		if err := reb.sendFromReader(reader, sl, i+1, cksumVal, si, dest); err != nil {
			return fmt.Errorf("failed to send slice %d of %s to %s: %v", i, obj.uid, si, err)
		}
	}

	return nil
}

func (reb *Manager) restoreObject(obj *rebObject, objMD ec.Metadata, src io.Reader) (err error) {
	var (
		cksum *cmn.Cksum
		lom   = &cluster.LOM{T: reb.t, ObjName: obj.objName}
	)

	if err := lom.Init(obj.bck); err != nil {
		return err
	}
	lom.Lock(true)
	defer lom.Unlock(true)
	if glog.FastV(4, glog.SmoduleReb) {
		glog.Infof("saving restored full object %s to %q", obj.objName, lom.FQN)
	}
	tmpFQN := fs.CSM.GenContentFQN(lom.FQN, fs.WorkfileType, "ec")
	buffer, slab := reb.t.GetMMSA().Alloc()
	if cksum, err = cmn.SaveReaderSafe(tmpFQN, lom.FQN, src, buffer, true, obj.objSize); err != nil {
		glog.Error(err)
		slab.Free(buffer)
		reb.t.FSHC(err, lom.FQN)
		return err
	}

	lom.SetSize(obj.objSize)
	lom.SetCksum(cksum)
	metaFQN := lom.ParsedFQN.MpathInfo.MakePathFQN(obj.bck, ec.MetaType, obj.objName)
	metaBuf := cmn.MustMarshal(&objMD)
	if _, err := cmn.SaveReader(metaFQN, bytes.NewReader(metaBuf), buffer, false); err != nil {
		glog.Error(err)
		slab.Free(buffer)
		if rmErr := os.Remove(lom.FQN); rmErr != nil {
			glog.Errorf("Nested error while cleaning up: %v", rmErr)
		}
		reb.t.FSHC(err, metaFQN)
		return err
	}
	slab.Free(buffer)
	if err := lom.Persist(); err != nil {
		if rmErr := os.Remove(metaFQN); rmErr != nil && !os.IsNotExist(rmErr) {
			glog.Errorf("nested error: save LOM -> remove metadata file: %v", rmErr)
		}
		if rmErr := os.Remove(lom.FQN); rmErr != nil && !os.IsNotExist(rmErr) {
			glog.Errorf("nested error: save LOM -> remove replica: %v", rmErr)
		}
		return err
	}
	lom.Uncache()
	return nil
}

// Default target does not have object (but it can be on another target) and
// few slices may be missing. The function detects whether it needs to reconstruct
// the object and then rebuild and send missing slices
func (reb *Manager) rebuildAndSend(obj *rebObject, slices []*waitCT) error {
	// look through received slices if one of them is the object's replica
	var replica *waitCT
	recv := 0
	for _, s := range slices {
		if s.sliceID == 0 {
			replica = s
		}
		if s.recv.Load() {
			recv++
		}
	}
	cmn.Assert(recv != 0) // sanity check

	if replica != nil {
		// Main target has replica, add slices to ZIL
		return nil
	}

	return reb.rebuildFromSlices(obj, slices)
}

// Returns XXHash calculated for the reader
func cksumForSlice(reader io.Reader, sliceSize int64, mem *memsys.MMSA) (string, error) {
	buf, slab := mem.Alloc(sliceSize)
	defer slab.Free(buf)
	return cmn.ComputeXXHash(reader, buf)
}

//
// ctWaiter
//

func newWaiter(mem *memsys.MMSA) *ctWaiter {
	return &ctWaiter{
		objs: make(ctWaitList),
		mem:  mem,
	}
}

// object is processed, cleanup allocated memory
func (wt *ctWaiter) removeObj(uid string) {
	wt.mx.Lock()
	wt.removeObjUnlocked(uid)
	wt.mx.Unlock()
}

func (wt *ctWaiter) removeObjUnlocked(uid string) {
	wo, ok := wt.objs[uid]
	if ok {
		for _, slice := range wo.cts {
			slice.mtx.Lock()
			if slice.sgl != nil {
				slice.sgl.Free()
			}
			slice.mtx.Unlock()
		}
		delete(wt.objs, uid)
	}
}

// final cleanup after rebalance is done
func (wt *ctWaiter) cleanup() {
	for uid := range wt.objs {
		wt.removeObj(uid)
	}
	wt.waitFor.Store(0)
	wt.toRebuild.Store(0)
}

// Range freeing: if idx is not defined, cleanup all waiting objects,
// otherwise cleanup only objects which names matches objects in range idx0..idx1
func (wt *ctWaiter) cleanupBatch(md *rebArgs, broken []*rebObject, idx ...int) {
	wt.mx.Lock()
	if len(idx) == 0 {
		for uid := range wt.objs {
			wt.removeObjUnlocked(uid)
		}
	} else {
		start := idx[0]
		for objIdx := start; objIdx < start+md.config.EC.BatchSize; objIdx++ {
			if objIdx >= len(broken) {
				break
			}
			wt.removeObjUnlocked(broken[objIdx].uid)
		}
	}
	wt.mx.Unlock()
}

// Looks through the list of slices to wait and returns the one
// with given uid. If nothing found, it creates a new wait object and
// returns it. This case is possible when another target is faster than this
// one and starts sending slices before this target builds its list
func (wt *ctWaiter) lookupCreate(uid string, sliceID int16, waitType int) *waitCT {
	wt.mx.Lock()
	defer wt.mx.Unlock()

	wObj, ok := wt.objs[uid]
	if !ok {
		// first slice of the object, initialize everything
		slice := &waitCT{
			sliceID: sliceID,
			sgl:     wt.mem.NewSGL(memsys.DefaultBufSize),
		}
		wt.objs[uid] = &waitObject{wt: waitType, cts: []*waitCT{slice}}
		wt.waitFor.Inc()
		return slice
	}

	// in case of other target sent a slice before this one had initialized
	// wait structure, replace current waitType if it is not a generic one
	if waitType != waitForSingleSlice {
		wObj.wt = waitType
	}

	// check if the slice is already initialized and return it
	for _, ct := range wObj.cts {
		if ct.sliceID == anySliceID || ct.sliceID == sliceID || sliceID == anySliceID {
			return ct
		}
	}

	// slice is not in wait list yet, add it
	ct := &waitCT{
		sliceID: sliceID,
		sgl:     wt.mem.NewSGL(memsys.DefaultBufSize),
	}
	wt.objs[uid].cts = append(wt.objs[uid].cts, ct)
	wt.waitFor.Inc()
	return ct
}

// Updates object readiness to be rebuild (i.e., the target has received all
// required slices/replicas).
// Returns `true` if a target waits for slices only to rebuild the object,
// so the received slices should not be saved to local drives.
func (wt *ctWaiter) updateRebuildInfo(uid string) bool {
	wt.mx.Lock()
	defer wt.mx.Unlock()

	wObj, ok := wt.objs[uid]
	cmn.Assert(ok)
	if wObj.wt == waitForSingleSlice || wObj.status != objWaiting {
		// object should not be rebuilt, or it is already done: nothing to do
		return wObj.wt != waitForSingleSlice
	}

	if wObj.wt == waitForReplica {
		// For replica case, a target needs only 1 replica to start rebuilding
		for _, ct := range wObj.cts {
			if ct.sliceID == 0 && ct.recv.Load() {
				wObj.status = objReceived
				break
			}
		}
	} else {
		// For EC case, a target needs all slices to start rebuilding
		done := true
		for _, ct := range wObj.cts {
			if !ct.recv.Load() {
				done = false
				break
			}
		}
		if done {
			wObj.status = objReceived
		}
	}
	return wObj.wt != waitForSingleSlice
}

// Returns UID and data for the next object that has all slices/replicas
// received and can be rebuild.
// The number of object in `wt.objs` map is less than the number of object
// in a batch (md.config.EC.BatchSize). So, linear algorithm is fast enough.
func (wt *ctWaiter) nextReadyObj() (uid string, wObj *waitObject) {
	wt.mx.Lock()
	defer wt.mx.Unlock()

	for uid, obj := range wt.objs {
		if obj.status == objReceived && obj.wt != waitForSingleSlice {
			return uid, obj
		}
	}

	return "", nil
}
