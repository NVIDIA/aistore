// Package reb provides resilvering and rebalancing functionality for the AIStore object storage.
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package reb

import (
	"bytes"
	"errors"
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
// 06. Each target processes the list of CTs and groups by isAIS/Bucket/Objname/ObjHash
// 07. All other steps use the newest CT list of an object. Though it has
//     and issue. TODO: rare corner case: current object is EC'ed, after putting a
//     new version, the object gets replicated. Replicated object requires less
//     CTs, so the algorithm chooses the list that belongs to older hash version
// 08. If a local CT is on incorrect mpath, it is added to local repair list.
// 09. If one or few object parts are missing, and it is possible to restore
//     them from existing ones, the object is added to 'broken' object list
// 10. If 'broken' and 'local repair' lists are empty, the rebalance finishes.
// 11. 'broken' list is sorted by isAIS/Bucket/Objname to have on all targets
//     determined order
// 12. First, 'local repair' list is processed and all CTs are moved to correct mpath
// 13. Next the rebalance proceeds with the next stage (rebStageECGlobRepair)
//     and wait for all other nodes
// 14. To minimize memory/GC load, the 'broken' list is processed in batches.
//     At this moment a batch size is 8 objects
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
	// transport name for CT list exchange
	DataECRebStreamName = "reb-ec-data"
	// the number of objects processed a time
	ecRebBatchSize = 8
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
		Objname      string `json:"obj"`
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
		bck         cmn.Bck
		objName     string
		sliceID     int16
		daemonID    string           // destination
		header      transport.Header // original transport header copy
		fqn         string           // for sending CT from local drives
		sgl         *memsys.SGL      // CT was rebuilt and it is in-memory one
		sliceOffset int64            // slice offset for data slice
		sliceSize   int64            // slice data size for data slice
		slicePad    int64            // slice padding size for data slice
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

var (
	ecPadding    = make([]byte, 256) // more than enough for max number of slices
	ErrorAborted = errors.New("Aborted")
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
// is determined only by the number of slices: the newest has the biggest number
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

// Returns how many CTs(including the original object) must exists
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

// Returns a random CT which has metadata.
func (so *rebObject) ctWithMD() *rebCT {
	for _, ct := range so.locCT {
		if ct.meta != nil {
			return ct
		}
	}
	return nil
}

// Returns the list of targets that does not have any CT but
// they should have according to HRW
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
		rt.objName == rhs.objName &&
		rt.daemonID == rhs.daemonID &&
		rt.bck.Equal(rhs.bck)
}

func (ack *ackCT) add(rt *retransmitCT) {
	cmn.Assert(rt.objName != "" && rt.bck.Name != "")
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

// Removes all waiting ACKs for the object. Used after sending slices
// failed, so no ACK would be sent by a destination
func (ack *ackCT) removeObject(bck cmn.Bck, objName string) {
	ack.mtx.Lock()
	l := len(ack.ct)
	for idx := 0; idx < l; {
		if ack.ct[idx].objName != objName || !ack.ct[idx].bck.Equal(bck) {
			idx++
			continue
		}
		l--
		ack.ct[idx] = ack.ct[l]
		ack.ct = ack.ct[:l]
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
func (ack *ackCT) clear() {
	ack.mtx.Lock()
	ack.ct = ack.ct[:0]
	ack.mtx.Unlock()
}

//
//  Rebalance result methods
//

// Merge given CT with already existing CTs of an object.
// It checks if the CT is unique(in case of the object is erasure coded),
// and the CT's info about object matches previously found CTs.
func (rr *globalCTList) addCT(ct *rebCT, tgt cluster.Target) error {
	bckList := rr.ais
	if cmn.IsProviderCloud(ct.Bck, false /*acceptAnon*/) {
		bckList = rr.cloud
	}
	bck, ok := bckList[ct.Name]
	if !ok {
		bck = &rebBck{objs: make(map[string]*rebObject)}
		bckList[ct.Name] = bck
	}

	obj, ok := bck.objs[ct.Objname]
	if !ok {
		// first CT of the object
		b := cluster.NewBckEmbed(ct.Bck)
		if err := b.Init(tgt.GetBowner()); err != nil {
			return err
		}
		si, err := cluster.HrwTarget(b.MakeUname(ct.Objname), tgt.GetSowner().Get())
		if err != nil {
			return err
		}
		obj = &rebObject{
			cts:        make(ctList),
			mainDaemon: si.ID(),
			bck:        ct.Bck,
		}
		obj.cts[ct.ObjHash] = []*rebCT{ct}
		bck.objs[ct.Objname] = obj
		return nil
	}

	// sanity check: sliceID must be unique (unless it is 0)
	if ct.SliceID != 0 {
		list := obj.cts[ct.ObjHash]
		for _, found := range list {
			if found.SliceID == ct.SliceID {
				err := fmt.Errorf("Duplicated %s/%s SliceID %d from %s (discarded)",
					ct.Name, ct.Objname, ct.SliceID, ct.DaemonID)
				return err
			}
		}
	}
	obj.cts[ct.ObjHash] = append(obj.cts[ct.ObjHash], ct)
	return nil
}

//
//  EC Rebalancer methods and utilities
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

// Add a CT to list of CTs of a given target
func (s *ecData) appendNodeData(daemonID string, ct *rebCT) {
	s.mtx.Lock()
	s.cts[daemonID] = append(s.cts[daemonID], ct)
	s.mtx.Unlock()
}

// Sends local CT along with EC metadata to remote targets.
// The CT is on a local drive and not loaded into SGL. Just read and send.
func (reb *Manager) sendFromDisk(ct *rebCT, targets ...*cluster.Snode) error {
	cmn.Assert(ct.meta != nil)
	req := pushReq{
		daemonID: reb.t.Snode().ID(),
		stage:    rebStageECGlobRepair,
		rebID:    reb.globRebID.Load(),
		md:       ct.meta,
	}

	fqn := ct.realFQN
	if ct.hrwFQN != "" {
		fqn = ct.hrwFQN
	}
	resolved, _, err := cluster.ResolveFQN(fqn)
	if err != nil {
		return err
	}
	opaque := req.NewPack()
	hdr := transport.Header{
		Bck:      ct.Bck,
		ObjName:  ct.Objname,
		ObjAttrs: transport.ObjectAttrs{Size: ct.ObjSize},
		Opaque:   opaque,
	}

	if resolved.ContentType == fs.ObjectType {
		lom := cluster.LOM{T: reb.t, FQN: fqn}
		if err := lom.Init(ct.Bck); err != nil {
			return err
		}
		if err := lom.Load(false); err != nil {
			return err
		}

		hdr.ObjAttrs.Atime = lom.AtimeUnix()
		hdr.ObjAttrs.Version = lom.Version()
		if cksum := lom.Cksum(); cksum != nil {
			hdr.ObjAttrs.CksumType, hdr.ObjAttrs.CksumValue = cksum.Get()
		}
	}
	if ct.SliceID != 0 {
		hdr.ObjAttrs.Size = ec.SliceSize(ct.ObjSize, int(ct.DataSlices))
	}

	fh, err := cmn.NewFileHandle(ct.hrwFQN)
	if err != nil {
		return err
	}

	for _, tgt := range targets {
		dest := &retransmitCT{
			bck: ct.Bck, objName: ct.Objname, sliceID: ct.SliceID,
			daemonID: tgt.ID(), header: hdr, fqn: ct.hrwFQN}
		reb.ec.ackCTs.add(dest)
	}
	reb.ec.onAir.Inc()
	if err := reb.streams.Send(transport.Obj{Hdr: hdr, Callback: reb.transportECCB}, fh, targets...); err != nil {
		reb.ec.onAir.Dec()
		fh.Close()
		reb.ec.ackCTs.removeObject(ct.Bck, ct.Objname)
		return fmt.Errorf("Failed to send slices to nodes [%s..]: %v", targets[0].ID(), err)
	}
	reb.statRunner.AddMany(
		stats.NamedVal64{Name: stats.TxRebCount, Value: 1},
		stats.NamedVal64{Name: stats.TxRebSize, Value: hdr.ObjAttrs.Size},
	)
	return nil
}

// Track the number of sent CTs
func (reb *Manager) transportECCB(_ transport.Header, reader io.ReadCloser, _ unsafe.Pointer, _ error) {
	reb.ec.onAir.Dec()
}

// Sends reconstructed slice along with EC metadata to remote target.
// EC metadata is of main object, so its internal field SliceID must be
// fixed prior to sending.
// Use the function to send only slices (not for replicas/full object)
func (reb *Manager) sendFromReader(reader cmn.ReadOpenCloser,
	ct *rebCT, sliceID int, xxhash string, target *cluster.Snode, rt *retransmitCT) error {
	cmn.AssertMsg(ct.meta != nil, ct.Objname)
	newMeta := *ct.meta // copy meta (it does not contain pointers)
	if bool(glog.FastV(4, glog.SmoduleReb)) {
		glog.Infof("Sending slice %d[%s] of %s to %s", sliceID, xxhash, ct.Objname, target.Name())
	}
	newMeta.SliceID = sliceID
	req := pushReq{
		daemonID: reb.t.Snode().ID(),
		stage:    rebStageECGlobRepair,
		rebID:    reb.globRebID.Load(),
		md:       &newMeta,
	}
	cmn.AssertMsg(ct.ObjSize != 0, ct.Objname)
	size := ec.SliceSize(ct.ObjSize, int(ct.DataSlices))
	opaque := req.NewPack()
	hdr := transport.Header{
		Bck:      ct.Bck,
		ObjName:  ct.Objname,
		ObjAttrs: transport.ObjectAttrs{Size: size},
		Opaque:   opaque,
	}
	if xxhash != "" {
		hdr.ObjAttrs.CksumValue = xxhash
		hdr.ObjAttrs.CksumType = cmn.ChecksumXXHash
	}

	cmn.Assert(rt != nil)
	rt.daemonID = target.ID()
	rt.header = hdr
	reb.ec.ackCTs.add(rt)

	reb.ec.onAir.Inc()
	if err := reb.streams.Send(transport.Obj{Hdr: hdr, Callback: reb.transportECCB}, reader, target); err != nil {
		reb.ec.onAir.Dec()
		reb.ec.ackCTs.remove(rt)
		return fmt.Errorf("Failed to send slices to node %s: %v", target.Name(), err)
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
	if err := bck.Init(reb.t.GetBowner()); err != nil {
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
		lom = &cluster.LOM{T: reb.t, Objname: hdr.ObjName}
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
	if req.md.SliceID == 0 && hdr.ObjAttrs.CksumType == cmn.ChecksumXXHash && hdr.ObjAttrs.CksumValue != cksum.Value() {
		err = fmt.Errorf("Mismatched hash for %s/%s, version %s, hash calculated %s/header %s/md %s",
			hdr.Bck, hdr.ObjName, hdr.ObjAttrs.Version, cksum.Value(), hdr.ObjAttrs.CksumValue, req.md.ObjCksum)
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
			glog.Errorf("Nested error: save replica -> remove metadata file: %v", rmErr)
		}
		if rmErr := os.Remove(ctFQN); rmErr != nil && !os.IsNotExist(rmErr) {
			glog.Errorf("Nested error: save replica -> remove replica: %v", rmErr)
		}
	}

	return err
}

// Receives a CT from another target, saves to local drive because it is a missing one
func (reb *Manager) receiveCT(req *pushReq, hdr transport.Header, reader io.Reader) error {
	if bool(glog.FastV(4, glog.SmoduleReb)) {
		glog.Infof("%s GOT CT for %s/%s from %s", reb.t.Snode().Name(), hdr.Bck, hdr.ObjName, req.daemonID)
	}
	cmn.Assert(req.md != nil)

	reb.laterx.Store(true)
	sliceID := req.md.SliceID
	if bool(glog.FastV(4, glog.SmoduleReb)) {
		glog.Infof(">>> %s got CT %d for %s/%s", reb.t.Snode().Name(), sliceID, hdr.Bck, hdr.ObjName)
	}
	uid := uniqueWaitID(hdr.Bck, hdr.ObjName)
	waitFor := reb.ec.waiter.lookupCreate(uid, int16(sliceID), waitForSingleSlice)
	cmn.Assert(waitFor != nil)
	cmn.Assert(waitFor.sgl != nil)
	cmn.Assert(!waitFor.recv.Load())

	waitFor.meta = req.md
	n, err := io.Copy(waitFor.sgl, reader)
	if err != nil {
		return fmt.Errorf("failed to read slice %d for %s/%s: %v", sliceID, hdr.Bck, hdr.ObjName, err)
	}
	reb.statRunner.AddMany(
		stats.NamedVal64{Name: stats.RxRebCount, Value: 1},
		stats.NamedVal64{Name: stats.RxRebSize, Value: n},
	)
	ckval, _ := cksumForSlice(memsys.NewReader(waitFor.sgl), waitFor.sgl.Size(), reb.t.GetMMSA())
	if hdr.ObjAttrs.CksumValue != "" && hdr.ObjAttrs.CksumValue != ckval {
		return fmt.Errorf("received checksum mismatches checksum in header %s vs %s",
			hdr.ObjAttrs.CksumValue, ckval)
	}
	waitFor.recv.Store(true)
	waitRebuild := reb.ec.waiter.updateRebuildInfo(uid)

	if !waitRebuild || sliceID == 0 {
		if err := reb.saveCTToDisk(waitFor.sgl, req, hdr); err != nil {
			glog.Errorf("Failed to save CT %d of %s: %v", sliceID, hdr.ObjName, err)
			reb.abortGlobal()
		}
	}

	// notify that another slice is received successfully
	remains := reb.ec.waiter.waitFor.Dec()
	if bool(glog.FastV(4, glog.SmoduleReb)) {
		glog.Infof("CTs to get remains: %d", remains)
	}

	// send acknowledge back to the caller
	smap := (*cluster.Smap)(reb.smap.Load())
	tsi := smap.GetTarget(req.daemonID)
	ack := &ecAck{sliceID: uint16(sliceID), daemonID: reb.t.Snode().ID()}
	hdr.Opaque = ack.NewPack()
	hdr.ObjAttrs.Size = 0
	if bool(glog.FastV(4, glog.SmoduleReb)) {
		glog.Infof("Sending ACK for %s/%s to %s", hdr.Bck, hdr.ObjName, tsi.ID())
	}
	if err := reb.acks.Send(transport.Obj{Hdr: hdr}, nil, tsi); err != nil {
		glog.Error(err)
	}

	return nil
}

// On receiving an EC CT or EC namespace
func (reb *Manager) recvECData(hdr transport.Header, unpacker *cmn.ByteUnpack, reader io.Reader) {
	req := &pushReq{}
	err := unpacker.ReadAny(req)
	if err != nil {
		glog.Errorf("Invalid push notification: %v", err)
		return
	}

	if req.rebID != reb.globRebID.Load() {
		glog.Warningf("Local node has not started or already has finished rebalancing")
		cmn.DrainReader(reader)
		return
	}

	// a remote target sent CT
	if req.stage == rebStageECGlobRepair {
		if err := reb.receiveCT(req, hdr, reader); err != nil {
			glog.Errorf("Failed to receive CT for %s/%s: %v", hdr.Bck, hdr.ObjName, err)
			return
		}
		return
	}

	// otherwise a remote target sent collected namespace:
	// - receive the CT list
	// - update the remote target stage
	if req.stage != rebStageECNamespace {
		glog.Errorf("Invalid stage %s : %s (must be %s)", hdr.ObjName,
			stages[req.stage], stages[rebStageECNamespace])
		cmn.DrainReader(reader)
		return
	}

	b, err := ioutil.ReadAll(reader)
	if err != nil {
		glog.Errorf("Failed to read data from %s: %v", req.daemonID, err)
		return
	}

	cts := make([]*rebCT, 0)
	if err = jsoniter.Unmarshal(b, &cts); err != nil {
		glog.Errorf("Failed to unmarshal data from %s: %v", req.daemonID, err)
		return
	}

	reb.ec.setNodeData(req.daemonID, cts)
	reb.stages.setStage(req.daemonID, req.stage, 0)
}

// Build a list buckets with their objects from a flat list of all CTs
func (reb *Manager) mergeCTs() *globalCTList {
	res := &globalCTList{
		ais:   make(map[string]*rebBck),
		cloud: make(map[string]*rebBck),
	}

	// process all received CTs
	localDaemon := reb.t.Snode().ID()
	smap := reb.t.GetSowner().Get()
	for sid := range smap.Tmap {
		local := sid == localDaemon
		ctList, ok := reb.ec.nodeData(sid)
		if !ok {
			continue
		}
		for _, ct := range ctList {
			if ct.SliceID != 0 && local {
				b := cluster.NewBckEmbed(ct.Bck)
				if err := b.Init(reb.t.GetBowner()); err != nil {
					reb.abortGlobal()
					return nil
				}
				t, err := cluster.HrwTarget(b.MakeUname(ct.Objname), smap)
				cmn.Assert(err == nil)
				if t.ID() == localDaemon {
					glog.Infof("Skipping CT %d of %s (it must have main object)", ct.SliceID, ct.Objname)
					continue
				}
			}
			if err := res.addCT(ct, reb.t); err != nil {
				glog.Warning(err)
				continue
			}
			if local && ct.hrwFQN != ct.realFQN {
				reb.ec.localActions = append(reb.ec.localActions, ct)
				if bool(glog.FastV(4, glog.SmoduleReb)) {
					glog.Infof("%s %s -> %s", reb.t.Snode().Name(), ct.hrwFQN, ct.realFQN)
				}
			}
		}
	}
	return res
}

// Find objects that have either missing or misplaced parts. If a part is a
// slice or replica(not the "default" object) and mpath is correct the object
// is not considered as broken one even if its target is not in HRW list
func (reb *Manager) detectBroken(res *globalCTList) {
	reb.ec.broken = make([]*rebObject, 0)
	bowner := reb.t.GetBowner()
	bmd := bowner.Get()
	smap := reb.t.GetSowner().Get()

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
			if err := bck.Init(bowner); err != nil {
				// bucket might be deleted while rebalancing - skip it
				glog.Errorf("Invalid bucket %s: %v", bckName, err)
				delete(tp, bckName)
				continue
			}
			bprops, ok := bmd.Get(bck)
			if !ok {
				// bucket might be deleted while rebalancing - skip it
				glog.Errorf("Bucket %s does not exist", bckName)
				delete(tp, bckName)
				continue
			}
			for objName, obj := range objs.objs {
				if err := reb.calcLocalProps(bck, obj, smap, &bprops.EC); err != nil {
					glog.Warningf("Detect %s failed, skipping: %v", obj.objName, err)
					continue
				}

				mainHasObject := (obj.mainSliceID == 0 || obj.isECCopy) && obj.mainHasAny
				allCTFound := obj.foundCT() >= obj.requiredCT()
				// the object is good, nothing to restore:
				// 1. Either objects is replicated, default target has replica
				//    and total number of replicas is sufficient for EC
				// 2. Or object is EC'ed, default target has the object and
				//    the number of slices equals Data+Parity number
				if allCTFound && mainHasObject {
					continue
				}

				if bool(glog.FastV(4, glog.SmoduleReb)) {
					glog.Infof("[%s] BROKEN: %s [Main %d on %s], CTs %d of %d",
						reb.t.Snode().Name(), objName, obj.mainSliceID, obj.mainDaemon, obj.foundCT(), obj.requiredCT())
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
func (reb *Manager) checkCTs() {
	cts := reb.mergeCTs()
	if cts == nil {
		return
	}
	reb.detectBroken(cts)
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
		if reb.xreb.Aborted() || reb.xreb.Finished() {
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
	if reb.xreb.Aborted() {
		// notify `dir.Walk` to stop iterations
		return errors.New("Interrupt walk")
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
		glog.Warningf("Damaged file? Failed to load metadata from %q: %v", fqn, err)
		return nil
	}

	// generate CT path in the same mpath that metadata is, and detect CT
	isReplica := true
	fileFQN := ct.Make(fs.ObjectType)
	if _, err := os.Stat(fileFQN); err != nil {
		isReplica = false
		fileFQN = ct.Make(ec.SliceType)
		_, err = os.Stat(fileFQN)
		// found metadata without a corresponding CT
		if err != nil {
			glog.Warningf("%s no CT for metadata: %s", reb.t.Snode().Name(), fileFQN)
			return nil
		}
	}

	// calculate correct FQN
	var hrwFQN string
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
		Objname:      ct.ObjName(),
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
	)

	for _, mpathInfo := range availablePaths {
		bck := cmn.Bck{Name: reb.xreb.Bck().Name, Provider: cmn.ProviderAIS, Ns: reb.xreb.Bck().Ns}
		wg.Add(1)
		go reb.jogEC(mpathInfo, bck, wg)
	}

	if cfg.Cloud.Supported {
		for _, mpathInfo := range availablePaths {
			bck := cmn.Bck{Name: reb.xreb.Bck().Name, Provider: cfg.Cloud.Provider, Ns: reb.xreb.Bck().Ns}
			wg.Add(1)
			go reb.jogEC(mpathInfo, bck, wg)
		}
	}
	wg.Wait()
	reb.changeStage(rebStageECNamespace, 0)
}

// send collected CTs to all targets with retry
func (reb *Manager) exchange() error {
	const (
		retries = 3               // number of retries to send collected CT info
		sleep   = 5 * time.Second // delay between retries
	)

	globRebID := reb.globRebID.Load()
	smap := reb.t.GetSowner().Get()

	// TODO -- FIXME: add a helper in the cluster pkg: NodeMap => Nodes skipping self

	sendTo := make(cluster.Nodes, 0, len(smap.Tmap))
	failed := make(cluster.Nodes, 0, len(smap.Tmap))
	for _, node := range smap.Tmap {
		if node.ID() == reb.t.Snode().ID() {
			continue
		}
		sendTo = append(sendTo, node)
	}

	emptyCT := make([]*rebCT, 0)
	for i := 0; i < retries; i++ {
		failed = failed[:0]
		for _, node := range sendTo {
			if reb.xreb.Aborted() {
				return fmt.Errorf("%d: aborted", globRebID)
			}

			cts, ok := reb.ec.nodeData(reb.t.Snode().ID())
			if !ok {
				// no data collected for the target, send empty notification
				cts = emptyCT
			}

			req := pushReq{
				daemonID: reb.t.Snode().ID(),
				stage:    rebStageECNamespace,
				rebID:    globRebID,
			}
			body := cmn.MustMarshal(cts)
			opaque := req.NewPack()
			hdr := transport.Header{
				ObjAttrs: transport.ObjectAttrs{Size: int64(len(body))},
				Opaque:   opaque,
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

	return fmt.Errorf("Could not sent data to %d nodes", len(failed))
}

func (reb *Manager) rebalanceLocalSlice(fromFQN, toFQN string, buf []byte) error {
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

func (reb *Manager) rebalanceLocalObject(fromMpath fs.ParsedFQN, fromFQN, toFQN string, buf []byte) error {
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
func (reb *Manager) rebalanceLocal() error {
	buf, slab := reb.t.GetMMSA().Alloc()
	defer slab.Free(buf)
	for _, act := range reb.ec.localActions {
		if bool(glog.FastV(4, glog.SmoduleReb)) {
			glog.Infof("%s Repair local %s -> %s", reb.t.Snode().Name(), act.realFQN, act.hrwFQN)
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
			if err := reb.rebalanceLocalSlice(act.realFQN, act.hrwFQN, buf); err != nil {
				os.Remove(metaDstFQN)
				return err
			}
			continue
		}

		// object/replica case
		if err := reb.rebalanceLocalObject(mpathSrc, act.realFQN, act.hrwFQN, buf); err != nil {
			if rmErr := os.Remove(metaDstFQN); rmErr != nil {
				glog.Errorf("Error cleaning up %q: %v", metaDstFQN, rmErr)
			}
			return err
		}

		if err := os.Remove(metaSrcFQN); err != nil {
			glog.Errorf("Failed to cleanup %q: %v", metaSrcFQN, err)
		}
	}

	reb.changeStage(rebStageECGlobRepair, 0)
	return nil
}

// Fills object properties with props that must be calculated locally
func (reb *Manager) calcLocalProps(bck *cluster.Bck, obj *rebObject, smap *cluster.Smap, ecConfig *cmn.ECConf) (err error) {
	localDaemon := reb.t.Snode().ID()
	cts := obj.newest()
	cmn.Assert(len(cts) != 0) // cannot happen
	mainSlice := cts[0]

	obj.bck = mainSlice.Bck
	obj.objName = mainSlice.Objname
	obj.objSize = mainSlice.ObjSize
	obj.isECCopy = ec.IsECCopy(obj.objSize, ecConfig)
	obj.dataSlices = mainSlice.DataSlices
	obj.paritySlices = mainSlice.ParitySlices
	obj.sliceSize = ec.SliceSize(obj.objSize, int(obj.dataSlices))

	ctFound := obj.foundCT()
	ctReq := obj.requiredCT()
	obj.ctExist = make([]bool, ctReq)
	obj.locCT = make(map[string]*rebCT, ctFound)

	obj.uid = uniqueWaitID(obj.bck, mainSlice.Objname)
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
	if bool(glog.FastV(4, glog.SmoduleReb)) {
		if obj.sender == nil {
			glog.Infof("%s %s: hasSlice %v, fullObjExist: %v, isMain %v [mainHas: %v - %d], slice found %d vs required %d[all slices: %v], is in HRW %v",
				reb.t.Snode().Name(), obj.uid, obj.hasCT, obj.fullObjFound, obj.isMain, obj.mainHasAny, obj.mainSliceID, ctFound, ctReq, obj.hasAllSlices, obj.inHrwList)
		} else {
			glog.Infof("%s %s: hasSlice %v, fullObjExist: %v, isMain %v [mainHas: %v - %d], slice found %d vs required %d[all slices: %v], is in HRW %v [sender %s]",
				reb.t.Snode().Name(), obj.uid, obj.hasCT, obj.fullObjFound, obj.isMain, obj.mainHasAny, obj.mainSliceID, ctFound, ctReq, obj.hasAllSlices, obj.inHrwList, obj.sender.Name())
		}
	}
	return nil
}

// true if this target is not "default" one, and does not have any CT,
// and does not want any CT, the target can skip the object
func (reb *Manager) shouldSkipObj(obj *rebObject) bool {
	return (!obj.inHrwList && !obj.hasCT) ||
		(!obj.isMain && obj.mainHasAny && obj.mainSliceID == 0 &&
			(!obj.inHrwList || obj.hasCT))
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
func (reb *Manager) shouldSendSlice(obj *rebObject) (hasSlice bool, shouldSend bool) {
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
		glog.Infof("Should send: %s[%d] - %d : %v / %v", obj.uid, locSlice.SliceID, tgtIndex,
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

// true if the target needs replica: if it is default one and replica is missing,
// or the total number of replicas is less than required and this target must
// have a replica according to HRW
func (reb *Manager) needsReplica(obj *rebObject) bool {
	return (obj.isMain && !obj.mainHasAny) ||
		(!obj.hasCT && obj.inHrwList)
}

// Read CT from local drive and send to another target.
// If destination is not defined the target sends its data to "default by HRW" target
func (reb *Manager) sendLocalData(md *globArgs, obj *rebObject, si ...*cluster.Snode) error {
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
	if bool(glog.FastV(4, glog.SmoduleReb)) {
		glog.Infof("%s sending a slice/replica #%d of %s to main %s", reb.t.Snode().Name(), ct.SliceID, ct.Objname, target.Name())
	}
	return reb.sendFromDisk(ct, target)
}

// Sends one or more replicas of the object to fulfill EC parity requirement.
// First, check that local target is responsible for it: it must be the first
// target by HRW that has one of replicas
func (reb *Manager) bcastLocalReplica(obj *rebObject) error {
	cmn.Assert(obj.sender != nil) // mustn't happen
	if bool(glog.FastV(4, glog.SmoduleReb)) {
		glog.Infof("%s Object %s sender %s", reb.t.Snode().Name(), obj.uid, obj.sender.Name())
	}
	// Another node should send replicas, do noting
	if obj.sender.DaemonID != reb.t.Snode().ID() {
		return nil
	}

	// calculate how many replicas the target should send
	ctDiff := obj.requiredCT() - obj.foundCT()
	if ctDiff == 0 && !obj.mainHasAny {
		// when 'main' target does not have replica but the number
		// of replicas is OK, we have to copy replica to main anyway
		ctDiff = 1
	}
	sendTo := make([]*cluster.Snode, 0, ctDiff+1)
	ct, ok := obj.locCT[reb.t.Snode().ID()]
	cmn.Assert(ok)
	for _, si := range obj.hrwTargets {
		if _, ok := obj.locCT[si.ID()]; ok {
			continue
		}
		sendTo = append(sendTo, si)
		if bool(glog.FastV(4, glog.SmoduleReb)) {
			glog.Infof("%s #4.4 - sending %s a replica of %s to %s", reb.t.Snode().Name(), ct.hrwFQN, ct.Objname, si.Name())
		}
		ctDiff--
		if ctDiff == 0 {
			break
		}
	}
	cmn.Assert(len(sendTo) != 0)
	if err := reb.sendFromDisk(ct, sendTo...); err != nil {
		return fmt.Errorf("Failed to send %s: %v", ct.Objname, err)
	}
	return nil
}

// Return the first target in HRW list that does not have any CT
func (reb *Manager) firstEmptyTgt(obj *rebObject) *cluster.Snode {
	localDaemon := reb.t.Snode().ID()
	for i, tgt := range obj.hrwTargets {
		if _, ok := obj.locCT[tgt.ID()]; ok {
			continue
		}
		// must not happen
		cmn.Assert(tgt.ID() != localDaemon)
		// first is main, we must reach this line only if main has something
		cmn.Assert(i != 0)
		ct, ok := obj.locCT[reb.t.Snode().ID()]
		cmn.Assert(ok && ct.SliceID != 0)
		return tgt
	}
	return nil
}

func (reb *Manager) allCTReceived(_ *globArgs) bool {
	for {
		if reb.xreb.Aborted() {
			return false
		}
		uid, wObj := reb.ec.waiter.nextReadyObj()
		if wObj == nil {
			break
		}
		var obj *rebObject
		batchCurr := int(reb.stages.currBatch.Load())
		for j := 0; j+batchCurr < len(reb.ec.broken) && j < ecRebBatchSize; j++ {
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

	// must be the last check, because even if a target has all slices
	// it may need to rebuild and send repaired slices
	return reb.ec.waiter.waitFor.Load() == 0 && reb.ec.waiter.toRebuild.Load() == 0
}

func (reb *Manager) allNodesCompletedBatch(md *globArgs) bool {
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

func (reb *Manager) waitForFullObject(md *globArgs, obj *rebObject, moveLocalSlice bool) error {
	if moveLocalSlice {
		// Default target has a slice, so it must be sent to another target
		// before receiving a full object from some other target
		if bool(glog.FastV(4, glog.SmoduleReb)) {
			glog.Infof("#5.4 Sending local slice before getting full object %s", obj.uid)
		}
		tgt := reb.firstEmptyTgt(obj)
		cmn.Assert(tgt != nil) // must not happen
		if err := reb.sendLocalData(md, obj, tgt); err != nil {
			return err
		}
	}
	if bool(glog.FastV(4, glog.SmoduleReb)) {
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
			if bool(glog.FastV(4, glog.SmoduleReb)) {
				glog.Infof("#5.5 Waiting for slice %d %s - [SKIPPED %d]", sl.SliceID, obj.uid, tgtIndex)
			}
			continue
		}

		if bool(glog.FastV(4, glog.SmoduleReb)) {
			glog.Infof("#5.5 Waiting for slice %d %s", sl.SliceID, obj.uid)
		}
		reb.ec.waiter.lookupCreate(obj.uid, sl.SliceID, waitForAllSlices)
	}
	reb.ec.waiter.updateRebuildInfo(obj.uid)
	reb.ec.waiter.toRebuild.Inc()
	return nil
}

func (reb *Manager) restoreReplicas(obj *rebObject) (err error) {
	if !reb.needsReplica(obj) {
		return reb.bcastLocalReplica(obj)
	}
	if bool(glog.FastV(4, glog.SmoduleReb)) {
		glog.Infof("#4 Waiting for replica %s", obj.uid)
	}
	reb.ec.waiter.lookupCreate(obj.uid, 0, waitForSingleSlice)
	return nil
}

func (reb *Manager) rebalanceObject(md *globArgs, obj *rebObject) (err error) {
	// Case #1: this target does not have to do anything
	if reb.shouldSkipObj(obj) {
		if bool(glog.FastV(4, glog.SmoduleReb)) {
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
		return reb.restoreReplicas(obj)
	}

	// Case #5: the object is erasure coded

	// Case #5.1: it is not main target and has slice or does not need any
	if !obj.isMain && (obj.hasCT || !obj.inHrwList) {
		if bool(glog.FastV(4, glog.SmoduleReb)) {
			glog.Infof("#5.1 Object %s skipped", obj.uid)
		}
		return nil
	}

	// Case #5.2: it is not main target, has no slice, needs according to HRW
	// but won't receive since there are few slices outside HRW
	if !obj.isMain && !obj.hasCT && obj.inHrwList {
		if bool(glog.FastV(4, glog.SmoduleReb)) {
			glog.Infof("#5.2 Waiting for object %s", obj.uid)
		}
		reb.ec.waiter.lookupCreate(obj.uid, anySliceID, waitForSingleSlice)
		return nil
	}

	// Case #5.3: main has nothing, but full object and all slices exists
	if obj.isMain && !obj.mainHasAny && obj.fullObjFound {
		return reb.waitForFullObject(md, obj, false)
	}

	// Case #5.4: main has a slice instead of a full object, send local
	// slice to a free target and wait for another target sends the full obj
	if obj.isMain && obj.mainHasAny && obj.mainSliceID != 0 && obj.fullObjFound {
		return reb.waitForFullObject(md, obj, true)
	}

	// Case #5.5: it is main target and full object is missing
	if obj.isMain && !obj.fullObjFound {
		return reb.waitForExistingSlices(obj)
	}

	// The last case: must be main with object. Rebuild and send missing slices
	cmn.AssertMsg(obj.isMain && obj.mainHasAny && obj.mainSliceID == 0,
		fmt.Sprintf("%s%s/%s: isMain %t - mainHasSome %t - mainID %d",
			reb.t.Snode().Name(), obj.bck, obj.objName, obj.isMain, obj.mainHasAny, obj.mainSliceID))
	if bool(glog.FastV(4, glog.SmoduleReb)) {
		glog.Infof("rebuilding slices of %s and send them", obj.objName)
	}
	return reb.rebuildFromDisk(obj)
}

func (reb *Manager) cleanupBatch() {
	reb.ec.waiter.cleanupBatch(reb.ec.broken, int(reb.stages.currBatch.Load()))
	reb.releaseSGLs(reb.ec.broken)
	reb.ec.ackCTs.clear()
}

// Wait for all targets to finish the current batch and then free allocated resources
func (reb *Manager) finalizeBatch(md *globArgs) error {
	// First, wait for all slices the local target wants to receive
	maxWait := md.config.Rebalance.Quiesce
	if aborted := reb.waitQuiesce(md, maxWait, reb.allCTReceived); aborted {
		reb.ec.waiter.waitFor.Store(0)
		return ErrorAborted
	}
	// wait until all rebiult slices are sent
	reb.waitECAck(md)

	// mark batch done and notify other targets
	if bool(glog.FastV(4, glog.SmoduleReb)) {
		glog.Infof("%s batch %d done. Wait cluster-wide quiescence",
			reb.t.Snode().Name(), reb.stages.currBatch.Load())
	}

	reb.changeStage(rebStageECBatch, reb.stages.currBatch.Load())
	// wait for all targets to finish sending/receiving
	if bool(glog.FastV(4, glog.SmoduleReb)) {
		glog.Infof("%s waiting for ALL batch %d DONE", reb.t.Snode().ID(), reb.stages.currBatch.Load())
	}
	if aborted := reb.waitEvent(md, reb.allNodesCompletedBatch); aborted {
		reb.ec.waiter.waitFor.Store(0)
		return ErrorAborted
	}

	return nil
}

func (reb *Manager) tryRetransmit(md *globArgs, ct *retransmitCT,
	wg *sync.WaitGroup, cnt *atomic.Int32) {
	defer wg.Done()
	si := md.smap.Tmap[ct.daemonID]
	_, err := ec.RequestECMeta(ct.header.Bck, ct.header.ObjName, si)
	if err == nil {
		// Destination got new CT, but failed to send - cleanup ACK
		if bool(glog.FastV(4, glog.SmoduleReb)) {
			glog.Infof("%s Did not receive ACK %s/%s but the data transferred. Cleanup ACK",
				reb.t.Snode().ID(), ct.header.Bck, ct.header.ObjName)
		}
		reb.ec.ackCTs.remove(ct)
		return
	}

	// Destination still waits for the data, resend
	glog.Warningf("Did not receive ACK from %s for %s/%s. Retransmit",
		ct.daemonID, ct.header.Bck, ct.header.ObjName)
	reb.resendCT(md, ct)

	cnt.Inc()
	reb.statRunner.AddMany(
		stats.NamedVal64{Name: stats.TxRebCount, Value: 1},
		stats.NamedVal64{Name: stats.TxRebSize, Value: ct.header.ObjAttrs.Size},
	)
}

// Check the list of sent CTs and resent ones that have not been responded.
// All targets are checked in parallel for better performance
// Returns the number of resent CTs
func (reb *Manager) retransmitEC(md *globArgs) int {
	var (
		counter = atomic.NewInt32(0)
		wg      = &sync.WaitGroup{}
	)
	// Note on intended "double" `ackCTs` lock: it is locked for a short
	// time while the loop is creating goroutines. And if a goroutine manages
	// to send request and receive respond or resend the whole object before
	// this loop finishes, that goroutine will wait on its internal lock
	// until this loop completes before removing ACK from the list.
	reb.ec.ackCTs.mtx.Lock()
	for _, dest := range reb.ec.ackCTs.ct {
		if reb.xreb.Aborted() {
			break
		}
		wg.Add(1)
		go reb.tryRetransmit(md, dest, wg, counter)
	}
	reb.ec.ackCTs.mtx.Unlock()
	wg.Wait()

	if reb.xreb.Aborted() {
		return 0
	}
	return int(counter.Load())
}

func (reb *Manager) allAckReceived(_ *globArgs) bool {
	if reb.xreb.Aborted() {
		return false
	}
	return reb.ec.ackCTs.waiting() == 0
}

func (reb *Manager) waitECAck(md *globArgs) {
	globRebID := reb.globRebID.Load()
	loghdr := reb.loghdr(globRebID, md.smap)
	sleep := md.config.Timeout.CplaneOperation // NOTE: TODO: used throughout; must be separately assigned and calibrated

	// loop without timeout - wait until all CTs put into transport
	// queue are processed (either sent or failed)
	for reb.ec.onAir.Load() > 0 {
		if reb.xreb.AbortedAfter(sleep) {
			reb.ec.onAir.Store(0)
			glog.Infof("%s: abrt", loghdr)
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
	maxwt := md.config.Rebalance.DestRetryTime
	maxwt += time.Duration(int64(time.Minute) * int64(md.smap.CountTargets()/10))
	maxwt = cmn.MinDur(maxwt, md.config.Rebalance.DestRetryTime*2)
	curwt := time.Duration(0)
	for curwt < maxwt {
		cnt := reb.retransmitEC(md)
		if cnt == 0 || reb.xreb.Aborted() {
			return
		}
		if reb.xreb.AbortedAfter(sleep) {
			glog.Infof("%s: abrt", loghdr)
			return
		}
		curwt += sleep
		for reb.ec.onAir.Load() > 0 {
			// `retransmit` has resent some CTs, so we have to wait for
			// transfer to finish, and then reset the time counter.
			// Time counter does not increase while waiting.
			if reb.xreb.AbortedAfter(sleep) {
				reb.ec.onAir.Store(0)
				glog.Infof("%s: abrt", loghdr)
				return
			}
			curwt = 0
		}
		glog.Warningf("%s: retransmitted %d, more wack...", loghdr, cnt)
	}
	// short wait for cluster-wide quiescence after anything retransmitted
	if aborted := reb.waitQuiesce(md, sleep, reb.nodesQuiescent); aborted {
		return
	}
}

// Rebalances the current batch of broken objects
func (reb *Manager) rebalanceBatch(md *globArgs, batchCurr int64) error {
	batchEnd := cmn.Min(int(batchCurr)+ecRebBatchSize, len(reb.ec.broken))
	for objIdx := int(batchCurr); objIdx < batchEnd; objIdx++ {
		if reb.xreb.Aborted() {
			return ErrorAborted
		}

		obj := reb.ec.broken[objIdx]
		if bool(glog.FastV(4, glog.SmoduleReb)) {
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
func (reb *Manager) rebalanceGlobal(md *globArgs) (err error) {
	batchCurr := int64(0)
	batchLast := int64(len(reb.ec.broken) - 1)
	reb.stages.currBatch.Store(batchCurr)
	reb.stages.lastBatch.Store(batchLast)
	for batchCurr <= batchLast {
		if bool(glog.FastV(4, glog.SmoduleReb)) {
			glog.Infof("Starting batch of %d from %d", ecRebBatchSize, batchCurr)
		}

		if err = reb.rebalanceBatch(md, batchCurr); err != nil {
			reb.cleanupBatch()
			return err
		}

		err = reb.finalizeBatch(md)
		reb.cleanupBatch()
		if err != nil {
			return err
		}
		batchCurr = reb.stages.currBatch.Add(int64(ecRebBatchSize))
	}
	reb.changeStage(rebStageECCleanup, 0)
	return nil
}

// Free allocated memory for EC reconstruction, close opened file handles of replicas.
// Used to clean up memory after finishing a batch
func (reb *Manager) releaseSGLs(objList []*rebObject) {
	batchCurr := int(reb.stages.currBatch.Load())
	for i := batchCurr; i < batchCurr+ecRebBatchSize && i < len(objList); i++ {
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

// Local target has the full object on local drive and it is "default" target.
// Just rebuild slices and send missing ones to other targets
// TODO: rebuildFromDisk, rebuildFromMem, and rebuildFromSlices shares some
// code. See what can be done to deduplicate it. Some code may go to EC package
func (reb *Manager) rebuildFromDisk(obj *rebObject) (err error) {
	if bool(glog.FastV(4, glog.SmoduleReb)) {
		glog.Infof("%s rebuilding slices of %s and send them", reb.t.Snode().Name(), obj.objName)
	}
	slice, ok := obj.locCT[reb.t.Snode().ID()]
	cmn.Assert(ok && slice.SliceID == 0)
	padSize := obj.sliceSize*int64(obj.dataSlices) - obj.objSize
	obj.fh, err = cmn.NewFileHandle(slice.hrwFQN)
	if err != nil {
		return fmt.Errorf("Failed to open local object from %q: %v", slice.hrwFQN, err)
	}
	readers := make([]io.Reader, obj.dataSlices)
	readerSend := make([]cmn.ReadOpenCloser, obj.dataSlices)
	obj.rebuildSGLs = make([]*memsys.SGL, obj.paritySlices)
	writers := make([]io.Writer, obj.paritySlices)
	sizeLeft := obj.objSize
	for i := 0; int16(i) < obj.dataSlices; i++ {
		var reader cmn.ReadOpenCloser
		if sizeLeft < obj.sliceSize {
			reader, err = cmn.NewFileSectionHandle(obj.fh, int64(i)*obj.sliceSize, sizeLeft, padSize)
		} else {
			reader, err = cmn.NewFileSectionHandle(obj.fh, int64(i)*obj.sliceSize, obj.sliceSize, 0)
		}
		if err != nil {
			return fmt.Errorf("Failed to create file section reader for %q: %v", obj.objName, err)
		}
		readers[i] = reader
		readerSend[i] = reader
		sizeLeft -= obj.sliceSize
	}
	for i := 0; int16(i) < obj.paritySlices; i++ {
		obj.rebuildSGLs[i] = reb.t.GetMMSA().NewSGL(cmn.MinI64(obj.sliceSize, cmn.MiB))
		writers[i] = obj.rebuildSGLs[i]
	}

	stream, err := reedsolomon.NewStreamC(int(obj.dataSlices), int(obj.paritySlices), true, true)
	if err != nil {
		return fmt.Errorf("Failed to create initialize EC for %q: %v", obj.objName, err)
	}
	if err := stream.Encode(readers, writers); err != nil {
		return fmt.Errorf("Failed to build EC for %q: %v", obj.objName, err)
	}

	// Detect missing slices.
	// The main object that has metadata.
	ecSliceMD := obj.ctWithMD()
	cmn.Assert(ecSliceMD != nil)
	freeTargets := obj.emptyTargets(reb.t.Snode())
	for idx, exists := range obj.ctExist {
		if exists {
			continue
		}
		cmn.Assert(idx != 0)              // because the full object must exist on local drive
		cmn.Assert(len(freeTargets) != 0) // 0 - means we have issue in broken objects detection
		si := freeTargets[0]
		freeTargets = freeTargets[1:]
		if idx <= int(obj.dataSlices) {
			if bool(glog.FastV(4, glog.SmoduleReb)) {
				glog.Infof("Sending %s data slice %d to %s", obj.objName, idx, si.Name())
			}
			reader := readerSend[idx-1]
			reader.Open()
			ckval, err := cksumForSlice(reader, obj.sliceSize, reb.t.GetMMSA())
			if err != nil {
				return fmt.Errorf("Failed to calculate checksum of %s: %v", obj.objName, err)
			}
			reader.Open()
			dataSize := obj.sliceSize
			sliceIdx := idx - 1
			offset := int64(sliceIdx) * obj.sliceSize
			if obj.sliceSize*int64(sliceIdx+1) > obj.objSize {
				dataSize = obj.objSize - obj.sliceSize*(int64(sliceIdx))
			}
			padSize := obj.sliceSize - dataSize
			dest := &retransmitCT{
				objName: obj.objName, bck: obj.bck,
				fqn: slice.hrwFQN, sliceOffset: offset, sliceSize: dataSize,
				slicePad: padSize, sliceID: int16(idx),
			}
			if err := reb.sendFromReader(reader, ecSliceMD, idx, ckval, si, dest); err != nil {
				glog.Errorf("Failed to send data slice %d[%s] to %s", idx, obj.objName, si.Name())
				// continue to fix as many as possible
				continue
			}
		} else {
			sglIdx := idx - int(obj.dataSlices) - 1
			if bool(glog.FastV(4, glog.SmoduleReb)) {
				glog.Infof("Sending %s parity slice %d[%d] to %s", obj.objName, idx, sglIdx, si.Name())
			}
			ckval, err := cksumForSlice(memsys.NewReader(obj.rebuildSGLs[sglIdx]), obj.sliceSize, reb.t.GetMMSA())
			if err != nil {
				return fmt.Errorf("Failed to calculate checksum of %s: %v", obj.objName, err)
			}
			reader := memsys.NewReader(obj.rebuildSGLs[sglIdx])
			dest := &retransmitCT{
				objName: obj.objName, bck: obj.bck,
				sliceID: int16(idx), sgl: obj.rebuildSGLs[sglIdx],
			}
			if err := reb.sendFromReader(reader, ecSliceMD, idx, ckval, si, dest); err != nil {
				glog.Errorf("Failed to send parity slice %d[%s] to %s", idx, obj.objName, si.Name())
				// continue to fix as many as possible
				continue
			}
		}
	}
	return nil
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

// The object is misplaced and few slices are missing. The default target
// receives the object into SGL, rebuilds missing slices, and sends them
func (reb *Manager) rebuildFromMem(obj *rebObject, slices []*waitCT) (err error) {
	if bool(glog.FastV(4, glog.SmoduleReb)) {
		glog.Infof("%s rebuilding slices of %s and send them", reb.t.Snode().Name(), obj.objName)
	}
	cmn.Assert(len(slices) != 0)
	slice := slices[0]
	cmn.Assert(slice != nil && slice.sliceID == 0) // received slice must be object

	padSize := obj.sliceSize*int64(obj.dataSlices) - obj.objSize
	if padSize > 0 {
		cmn.Assert(padSize < 256)
		padding := ecPadding[:padSize]
		if _, err = slice.sgl.Write(padding); err != nil {
			return err
		}
	}

	readers := make([]io.Reader, obj.dataSlices)
	readerSGLs := make([]*memsys.SliceReader, obj.dataSlices)
	obj.rebuildSGLs = make([]*memsys.SGL, obj.paritySlices)
	writers := make([]io.Writer, obj.paritySlices)
	for i := 0; int16(i) < obj.dataSlices; i++ {
		readerSGLs[i] = memsys.NewSliceReader(slice.sgl, int64(i)*obj.sliceSize, obj.sliceSize)
		readers[i] = readerSGLs[i]
	}
	for i := 0; int16(i) < obj.paritySlices; i++ {
		obj.rebuildSGLs[i] = reb.t.GetMMSA().NewSGL(cmn.MinI64(obj.sliceSize, cmn.MiB))
		writers[i] = obj.rebuildSGLs[i]
	}

	stream, err := reedsolomon.NewStreamC(int(obj.dataSlices), int(obj.paritySlices), true, true)
	if err != nil {
		return fmt.Errorf("Failed to create initialize EC for %q: %v", obj.objName, err)
	}
	if err := stream.Encode(readers, writers); err != nil {
		return fmt.Errorf("Failed to build EC for %q: %v", obj.objName, err)
	}

	// detect missing slices
	// The main object that has metadata.
	ecSliceMD := obj.ctWithMD()
	cmn.Assert(ecSliceMD != nil)
	freeTargets := obj.emptyTargets(reb.t.Snode())
	for idx, exists := range obj.ctExist {
		if exists {
			continue
		}
		cmn.Assert(idx != 0) // full object must exists
		cmn.Assert(len(freeTargets) != 0)
		si := freeTargets[0]
		freeTargets = freeTargets[1:]
		if idx <= int(obj.dataSlices) {
			if bool(glog.FastV(4, glog.SmoduleReb)) {
				glog.Infof("Sending %s data slice %d to %s", obj.objName, idx, si.Name())
			}
			reader := readerSGLs[idx-1]
			ckval, err := cksumForSlice(readerSGLs[idx-1], obj.sliceSize, reb.t.GetMMSA())
			if err != nil {
				return fmt.Errorf("Failed to calculate checksum of %s: %v", obj.objName, err)
			}
			reader.Open()
			dest := &retransmitCT{
				bck: obj.bck, objName: obj.objName,
				sgl: slice.sgl, sliceOffset: int64(idx-1) * obj.sliceSize,
				sliceSize: obj.sliceSize, sliceID: int16(idx),
			}
			if err := reb.sendFromReader(reader, ecSliceMD, idx, ckval, si, dest); err != nil {
				glog.Errorf("Failed to send data slice %d[%s] to %s", idx, obj.objName, si.Name())
				// keep on working to restore as many as possible
				continue
			}
		} else {
			sglIdx := idx - int(obj.dataSlices) - 1
			if bool(glog.FastV(4, glog.SmoduleReb)) {
				glog.Infof("Sending %s parity slice %d[%d] to %s", obj.objName, idx, sglIdx, si.Name())
			}
			ckval, err := cksumForSlice(memsys.NewReader(obj.rebuildSGLs[sglIdx]), obj.sliceSize, reb.t.GetMMSA())
			if err != nil {
				return fmt.Errorf("Failed to calculate checksum of %s: %v", obj.objName, err)
			}
			reader := memsys.NewReader(obj.rebuildSGLs[sglIdx])
			dest := &retransmitCT{
				bck: obj.bck, objName: obj.objName,
				sgl: obj.rebuildSGLs[sglIdx], sliceID: int16(idx),
			}
			if err := reb.sendFromReader(reader, ecSliceMD, idx, ckval, si, dest); err != nil {
				glog.Errorf("Failed to send parity slice %d[%s] to %s", idx, obj.objName, si.Name())
				// keep on working to restore as many as possible
				continue
			}
		}
	}
	return nil
}

// Object is missing(and maybe a few slices as well). Default target receives all
// existing slices into SGLs, restores the object, rebuilds slices, and finally
// send missing slices to other targets
func (reb *Manager) rebuildFromSlices(obj *rebObject, slices []*waitCT) (err error) {
	if bool(glog.FastV(4, glog.SmoduleReb)) {
		glog.Infof("%s rebuilding slices of %s and send them(mem)", reb.t.Snode().Name(), obj.objName)
	}

	sliceCnt := obj.dataSlices + obj.paritySlices
	obj.rebuildSGLs = make([]*memsys.SGL, sliceCnt)
	readers := make([]io.Reader, sliceCnt)
	// since io.Reader cannot be reopened, we need to have a copy for saving object
	rereaders := make([]io.Reader, sliceCnt)
	writers := make([]io.Writer, sliceCnt)

	// put existing slices to readers list, and create SGL as writers for missing ones
	slicesFound := int16(0)
	var (
		meta  *ec.Metadata
		cksum *cmn.Cksum
	)
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
		return fmt.Errorf("Failed to create initialize EC for %q: %v", obj.objName, err)
	}
	if err := stream.Reconstruct(readers, writers); err != nil {
		return fmt.Errorf("Failed to build EC for %q: %v", obj.objName, err)
	}

	if bool(glog.FastV(4, glog.SmoduleReb)) {
		glog.Infof("Saving restored full object %s[%d]", obj.objName, obj.objSize)
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

	lom := &cluster.LOM{T: reb.t, Objname: obj.objName}
	err = lom.Init(obj.bck)
	if err != nil {
		return err
	}
	lom.Uncache()
	if bool(glog.FastV(4, glog.SmoduleReb)) {
		glog.Infof("Saving restored full object %s to %q", obj.objName, lom.FQN)
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
			glog.Errorf("Nested error: save LOM -> remove metadata file: %v", rmErr)
		}
		if rmErr := os.Remove(lom.FQN); rmErr != nil && !os.IsNotExist(rmErr) {
			glog.Errorf("Nested error: save LOM -> remove replica: %v", rmErr)
		}
		return err
	}

	freeTargets := obj.emptyTargets(reb.t.Snode())
	for i, wr := range writers {
		if wr == nil {
			continue
		}
		sliceID := i + 1
		if exists := obj.ctExist[sliceID]; exists {
			if bool(glog.FastV(4, glog.SmoduleReb)) {
				glog.Infof("Object %s slice %d: already exists", obj.uid, sliceID)
			}
			continue
		}
		if len(freeTargets) == 0 {
			return fmt.Errorf("Failed to send slice %d of %s - no free target", sliceID, obj.uid)
		}
		ckval, err := cksumForSlice(memsys.NewReader(obj.rebuildSGLs[i]), obj.sliceSize, reb.t.GetMMSA())
		if err != nil {
			return fmt.Errorf("Failed to calculate checksum of %s: %v", obj.objName, err)
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
			Objname:      obj.objName,
			ObjSize:      sliceMD.Size,
			DaemonID:     reb.t.Snode().ID(),
			SliceID:      int16(sliceID),
			DataSlices:   int16(ecMD.Data),
			ParitySlices: int16(ecMD.Parity),
			meta:         &sliceMD,
		}

		dest := &retransmitCT{
			bck: obj.bck, objName: obj.objName,
			sgl: obj.rebuildSGLs[i], sliceID: int16(i + 1),
		}
		if err := reb.sendFromReader(reader, sl, i+1, ckval, si, dest); err != nil {
			return fmt.Errorf("Failed to send slice %d of %s to %s: %v", i, obj.uid, si.Name(), err)
		}
	}

	return nil
}

// Default target does not have object(but it can be on another target) and
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
		return reb.rebuildFromMem(obj, slices)
	}

	return reb.rebuildFromSlices(obj, slices)
}

// Resends any CTs that was sent to a target but the destination has not
// received it (no ACK and the destination does not have the CT yet)
func (reb *Manager) resendCT(md *globArgs, ct *retransmitCT) {
	var err error
	si := md.smap.Tmap[ct.daemonID]
	reb.ec.onAir.Inc()
	if ct.sgl != nil {
		trObj := transport.Obj{Hdr: ct.header, Callback: reb.transportECCB}
		// Resend from memory
		if ct.sliceSize == 0 {
			err = reb.streams.Send(trObj, memsys.NewReader(ct.sgl), si)
		} else {
			reader := memsys.NewSliceReader(ct.sgl, ct.sliceOffset, ct.sliceSize)
			err = reb.streams.Send(trObj, reader, si)
		}
		if err != nil {
			glog.Errorf("[sgl]Failed to retransmit %s/%s to %s: %v",
				ct.header.Bck.Name, ct.header.ObjName, ct.daemonID, err)
			reb.ec.onAir.Dec()
		}
		return
	}

	// Resend a file from local drive
	cmn.Assert(ct.fqn != "")
	fh, err := cmn.NewFileHandle(ct.fqn)
	if err != nil {
		// It was available, where it has gone? Maybe someone deleted the
		// object in-between. Force clean the ACK and forget
		reb.ec.onAir.Dec()
		reb.ec.ackCTs.remove(ct)
		return
	}
	cb := func(_ transport.Header, _ io.ReadCloser, _ unsafe.Pointer, _ error) {
		// Reader is a file slice, so transport does not know about
		// a file opened for it and cannot close it
		fh.Close()
		reb.ec.onAir.Dec()
	}
	trObj := transport.Obj{Hdr: ct.header, Callback: cb}
	if ct.sliceSize == 0 {
		// Entire file
		err = reb.streams.Send(trObj, fh, si)
	} else {
		// A slice of a file. Constructor never fails
		reader, _ := cmn.NewFileSectionHandle(fh, ct.sliceOffset, ct.sliceSize, ct.slicePad)
		err = reb.streams.Send(trObj, reader, si)
	}
	if err != nil {
		glog.Errorf("[disk]Failed to retransmit %s/%s to %s: %v",
			ct.header.Bck.Name, ct.header.ObjName, ct.daemonID, err)
		fh.Close()
		reb.ec.onAir.Dec()
	}
}

// Returns XXHash calculated for the reader
func cksumForSlice(reader cmn.ReadOpenCloser, sliceSize int64, mem *memsys.MMSA) (string, error) {
	reader.Open()
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
			if slice.sgl != nil {
				slice.sgl.Free()
			}
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
func (wt *ctWaiter) cleanupBatch(broken []*rebObject, idx ...int) {
	wt.mx.Lock()
	if len(idx) == 0 {
		for uid := range wt.objs {
			wt.removeObjUnlocked(uid)
		}
	} else {
		start := idx[0]
		for objIdx := start; objIdx < start+ecRebBatchSize; objIdx++ {
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
// in a batch (ecRebBatchSize). So, linear algorihtm is fast enough.
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
