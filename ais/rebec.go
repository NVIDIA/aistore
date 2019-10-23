package ais

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/transport"
	jsoniter "github.com/json-iterator/go"
)

const (
	// transport name for push notifications
	ackECRebStreamName  = "reb-ec-ack"
	dataECRebStreamName = "reb-ec-data"
)

const (
	ecRebActMainRestore  = iota // full object not found, GET it
	ecRebActFetchMain           // full object found on the incorrect target, copy it
	ecRebActRestoreSlice        // need to rebuild EC and copy a rebuilt slice(s)
	ecRebActCopyReplica         // copy replica(s) to have enough redundancy
	ecRebActMoveSlice           // move a slice between nodes (e.g, when "main" should get rid of its slice before restoring "main" object)
)

type (
	// a full info about a slice found on a target.
	// It is information sent to other targets that must be sufficient
	// to check and restore all main objects/replicas/slices
	ecRebSlice struct {
		realFQN string // what mpath slice/replica is
		hrwFQN  string // what mpath slice/replica should be

		Bucket  string `json:"bck"`
		Objname string `json:"obj"`
		// a target that has the slice
		DaemonID string `json:"sid"`
		// info from EC metadata
		ObjHash      string `json:"cksum"`
		ObjSize      int64  `json:"size"`
		SliceID      int16  `json:"sliceid,omitempty"`
		DataSlices   int16  `json:"data"`
		ParitySlices int16  `json:"parity"`
		IsAIS        bool   `json:"ais,omitempty"`
	}

	// what to do to make the object complete and healthy
	ecRebAction struct {
		action  int    // see ecRebAct* enum
		dest    string // to send new slice, or from which fetch main object
		sliceID int16
	}

	// a full info about an object that resides on the local target
	ecRebObject struct {
		slices     []*ecRebSlice  // object slices and replicas
		actions    []*ecRebAction // what to do to make object healthy
		replicated bool           // replicated or erasure coded
	}
	ecRebBck struct {
		objs map[string]*ecRebObject // maps ObjectName <-> object info
	}
	// final result of scanning the existing objects
	ecRebResult struct {
		ais   map[string]*ecRebBck // maps BucketName <-> map of objects
		cloud map[string]*ecRebBck // maps BucketName <-> map of objects
	}

	// common arguments for all rebalance functions (object-dependent ones)
	ecRebArgs struct {
		mainDaemon  string           // target currently contains the full object
		usedTargets cmn.StringSet    // targets that get new slices after rebalance
		hrwTargets  []*cluster.Snode // preferred list of target for slices/replicas
		sliceSize   int64            // a size of an object slice
		sliceFound  int              // how many slices were found
		sliceReq    int              // how many slices must exist
		locID       int16            // ID of local slice if any exists (-1 otherwise)
	}

	// push notification struct - a target sends it when it enters `stage`
	pushNotify struct {
		DaemonID string `json:"sid"`
		RebID    int64  `json:"rebid"`
		Stage    uint32 `json:"stage"`
	}

	// Callback is called if a target did not report that it is in `stage` or
	// its notification was lost. Callback either request the current state
	// directly or makes the target to resend.
	// The callback returns `true` only if it is safe to treat the target as
	// being in "stage" stage.
	StageCallback = func(si *cluster.Snode) bool
	ecSliceList   = map[string][]*ecRebSlice

	ecRebalancer struct {
		t        *targetrunner
		slices   ecSliceList // maps daemonID by HRW <-> Slice
		received ecSliceList // maps daemonID-sender <-> Slice
		md       *globalRebArgs
		ack      *transport.StreamBundle
		data     *transport.StreamBundle
		netc     string
		netd     string
		mtx      sync.Mutex

		// list of slices/replicas that should be moved between local mpaths
		localActions []*ecRebSlice
		// a map of nodes and stages they have reached by the current moment
		nodes map[string]uint32
	}
)

//
// Rebalance object methods
// All methods should be called only after it is clear that the object exists
// or we have one or few slices. That is why `Assert` is used
//

func (so *ecRebObject) bucket() string {
	cmn.Assert(len(so.slices) > 0)
	return so.slices[0].Bucket
}

func (so *ecRebObject) objName() string {
	cmn.Assert(len(so.slices) > 0)
	return so.slices[0].Objname
}

func (so *ecRebObject) objSize() int64 {
	cmn.Assert(len(so.slices) > 0)
	return so.slices[0].ObjSize
}

func (so *ecRebObject) dataSlices() int16 {
	cmn.Assert(len(so.slices) > 0)
	return so.slices[0].DataSlices
}

func (so *ecRebObject) paritySlices() int16 {
	cmn.Assert(len(so.slices) > 0)
	return so.slices[0].ParitySlices
}

// returns how many slices/replicas(including the original object) must exists
func (so *ecRebObject) sliceRequired() int {
	cmn.Assert(len(so.slices) > 0)
	s := so.slices[0]
	if so.replicated {
		return int(s.ParitySlices + 1)
	}
	return int(s.DataSlices + s.ParitySlices + 1)
}

// returns how many slices/replicas found across all targets
func (so *ecRebObject) sliceFound() int {
	return len(so.slices)
}

// returns if the given target has any slice/replica
func (so *ecRebObject) sliceExistAt(daemonID string) bool {
	for _, sl := range so.slices {
		if sl.DaemonID == daemonID {
			return true
		}
	}
	return false
}

// returns if the given slice exists on any target
func (so *ecRebObject) sliceIDExists(id int16) bool {
	for _, sl := range so.slices {
		if sl.SliceID == id {
			return true
		}
	}
	return false
}

// add a new action required to restore object/slice/replica
func (so *ecRebObject) addAction(action int, dest string, sliceID int16) {
	so.actions = append(so.actions,
		&ecRebAction{action: action, dest: dest, sliceID: sliceID},
	)
}

// Finds out on which target the full object is (objDaemon) and the sliceID
// of the local replica/slice (locID) that it is -1 if the local target does not
// have any replica/slice
func (so *ecRebObject) objLocation(daemonID string) (locID int16, objDaemon string) {
	locID = int16(-1)
	for _, sl := range so.slices {
		if sl.DaemonID == daemonID {
			locID = sl.SliceID
		}
		if sl.SliceID == 0 && objDaemon != daemonID {
			objDaemon = sl.DaemonID
		}
	}
	return locID, objDaemon
}

func (so *ecRebObject) sliceMatches(slice *ecRebSlice) bool {
	// the first slice - it always matches
	if len(so.slices) == 0 {
		return true
	}
	// check if the slices matches the first one
	first := so.slices[0]
	return first.ObjSize == slice.ObjSize &&
		first.ObjHash == slice.ObjHash &&
		first.DataSlices == slice.DataSlices &&
		first.ParitySlices == slice.ParitySlices
}

//
//  Rebalance result methods
//

// merge given slice with already existing slices of an object.
// It checks if the slice is unique(in case of the object is erasure coded),
// and the slice's info about object matches previously found slices.
// TODO: how to detect invalid slices better? At this moment the first slice
// is considered an example and all other must match it. Though, it is possible
// that the first slice is the only "bad" one
func (rr *ecRebResult) addSlice(slice *ecRebSlice) error {
	bckList := rr.ais
	if !slice.IsAIS {
		bckList = rr.cloud
	}
	bck, ok := bckList[slice.Bucket]
	if !ok {
		bck = &ecRebBck{objs: make(map[string]*ecRebObject)}
		bckList[slice.Bucket] = bck
	}

	obj, ok := bck.objs[slice.Objname]
	if !ok {
		bck.objs[slice.Objname] = &ecRebObject{slices: []*ecRebSlice{slice}}
		return nil
	}

	if !obj.sliceMatches(slice) {
		err := fmt.Errorf("Mismatched slices for %s/%s. Discard slice from %s (ID: %d)",
			slice.Bucket, slice.Objname, slice.DaemonID, slice.SliceID)
		return err
	}

	// sanity check: sliceID must be unique (unless it is 0)
	if slice.SliceID != 0 {
		for _, found := range obj.slices {
			if found.SliceID == slice.SliceID {
				err := fmt.Errorf("Duplicated %s/%s SliceID %d from %s (discarded)",
					slice.Bucket, slice.Objname, slice.SliceID, slice.DaemonID)
				return err
			}
		}
	}
	obj.slices = append(obj.slices, slice)
	return nil
}

//
//  EC Rebalancer methods and utilities
//

func newECRebList() ecSliceList {
	return make(map[string][]*ecRebSlice)
}

func newECRebalancer(t *targetrunner) *ecRebalancer {
	return &ecRebalancer{
		t:            t,
		slices:       newECRebList(),
		received:     newECRebList(),
		nodes:        make(map[string]uint32),
		localActions: make([]*ecRebSlice, 0),
	}
}

// Mark a 'node' that it has passed the 'stage'. If the node has
// already higher stage, the function does nothing
func (s *ecRebalancer) setStage(node string, stage uint32) {
	s.mtx.Lock()
	if currStage, ok := s.nodes[node]; !ok || currStage < stage {
		s.nodes[node] = stage
	}
	s.mtx.Unlock()
}

// Return the list of targets that has not sent push notifications.
// For ECExchange stage there is extra condition: the local target
// must have received the data. It ensures that the data was not lost.
// Used by pull notification to minimize the number of network request.
func (s *ecRebalancer) nodesNotInStage(stage uint32) []*cluster.Snode {
	smap := s.t.smapowner.get()
	nodes := make([]*cluster.Snode, 0, len(smap.Tmap))
	s.mtx.Lock()
	for _, si := range smap.Tmap {
		// first check that this target has received the remote node slices
		if si.DaemonID != s.t.si.DaemonID && stage == rebStageECExchange {
			if _, ok := s.received[si.DaemonID]; !ok {
				nodes = append(nodes, si)
				continue
			}
		}
		if currStage, ok := s.nodes[si.DaemonID]; !ok || currStage < stage {
			nodes = append(nodes, si)
			continue
		}
	}
	s.mtx.Unlock()
	return nodes
}

// Waits until all nodes are in 'stage'.
// Checks push notifications, and request stage from all nodes that have not
// sent any notification yet
func (s *ecRebalancer) waitAllStage(stage uint32, cb StageCallback) error {
	const (
		// time between pull requests
		loopSleep = 5 * time.Second
	)

	smap := s.t.smapowner.get()
	nodeCount := len(smap.Tmap)
	// by default, start pulling only after at least half of targets has sent push
	// notification. It makes possible to avoid too many useless pull requests
	// if filepath.Walk takes much time
	minDiff := nodeCount / 2
	wg := &sync.WaitGroup{}
	// TODO: timed wait loop
	for {
		// First, check push notifications: get list of targets that did not notify
		nodes := s.nodesNotInStage(stage)
		if len(nodes) == 0 {
			return nil
		}
		if s.md.xreb.Aborted() {
			return errors.New("Aborted")
		}

		// Second, give a little time for targets to finish their work if the
		// number of target reached the stage is low
		if cb == nil || nodeCount-len(nodes) < minDiff {
			glog.Infof("Stage %d: %d targets of %d are not in %d stage",
				stage, nodeCount-len(nodes), nodeCount, stage)
			time.Sleep(loopSleep)
			continue
		}

		// Third, pull target states
		// NOTE: callback must pull all the required info. It is possible to
		// implement a wait loop without pull requests: the callback should
		// just return false always and, maybe, it should send to a remote
		// target a request to repeat its push notification
		anyFailed := atomic.NewBool(false)
		for _, si := range nodes {
			wg.Add(1)
			go func(si *cluster.Snode) {
				defer wg.Done()
				if ok := cb(si); !ok {
					anyFailed.Store(true)
				} else {
					s.setStage(si.DaemonID, stage)
				}
			}(si)
		}
		wg.Wait()

		// All targets responded and all are in the desired stage: break the loop
		if !anyFailed.Load() {
			return nil
		}
		time.Sleep(loopSleep)
	}
}

// Rebalancer moves to the next stage:
// - update internal stage
// - send notification to all other targets that this one is in a new stage
func (s *ecRebalancer) changeStage(newStage uint32) {
	// first, set its own stage
	s.setStage(s.t.si.DaemonID, newStage)

	smap := s.t.smapowner.Get()
	nodes := make([]*cluster.Snode, 0, len(smap.Tmap))
	for _, node := range smap.Tmap {
		if node.DaemonID == s.t.si.DaemonID {
			continue
		}
		nodes = append(nodes, node)
	}

	req := pushNotify{DaemonID: s.t.si.DaemonID, Stage: newStage, RebID: s.md.globRebID}
	hdr := transport.Header{
		ObjAttrs: transport.ObjectAttrs{Size: 0},
		Opaque:   cmn.MustMarshal(req),
	}
	// second, notify all other targets
	if err := s.ack.SendV(hdr, nil, nil, nil, nodes...); err != nil {
		var sb strings.Builder
		for idx, n := range nodes {
			sb.WriteString(n.DaemonID)
			if idx < len(nodes)-1 {
				sb.WriteString(",")
			}
		}
		// in case of error only log a warning - other targets should be able
		// to pull the new stage and continue later
		glog.Warningf("Failed to send ack %d to %s nodes: %v", newStage, sb.String(), err)
		return
	}
}

// On receiving a stage-change notification from another target
func (s *ecRebalancer) OnAck(w http.ResponseWriter, hdr transport.Header, reader io.Reader, err error) {
	if err != nil {
		glog.Errorf("Failed to get ack %s from %s: %v", hdr.Objname, hdr.Bucket, err)
		return
	}

	var req pushNotify
	if err := jsoniter.Unmarshal(hdr.Opaque, &req); err != nil {
		glog.Errorf("Invalid push notification: %v", err)
		return
	}
	// a target was too late in sending(rebID is obsolete) its data or too early (md == nil)
	if s.md == nil || req.RebID != s.md.globRebID {
		glog.Warningf("Local node has not started or already has finished rebalancing")
		return
	}

	s.setStage(req.DaemonID, req.Stage)
}

// On receiving a list of collected slices from another target
func (s *ecRebalancer) OnData(w http.ResponseWriter, hdr transport.Header, reader io.Reader, err error) {
	if err != nil {
		glog.Errorf("Failed to get ack %s from %s: %v", hdr.Objname, hdr.Bucket, err)
		return
	}

	var req pushNotify
	if err := jsoniter.Unmarshal(hdr.Opaque, &req); err != nil {
		glog.Errorf("Invalid push notification: %v", err)
		return
	}

	// only rebStageECExchange is supported for now
	if req.Stage != rebStageECExchange {
		glog.Errorf("Invalid stage : %d (must be %d)", req.Stage, rebStageECExchange)
		cmn.DrainReader(reader)
		return
	}
	// a target was too late in sending(rebID is obsolete) its data or too early (md == nil)
	if s.md == nil || req.RebID != s.md.globRebID {
		glog.Warningf("Local node has not started or already has finished rebalancing")
		cmn.DrainReader(reader)
		return
	}

	b, err := ioutil.ReadAll(reader)
	if err != nil {
		glog.Errorf("Failed to read data from %s: %v", req.DaemonID, err)
		return
	}

	slices := make([]*ecRebSlice, 0)
	if err = jsoniter.Unmarshal(b, &slices); err != nil {
		glog.Errorf("Failed to unmarshal data from %s: %v", req.DaemonID, err)
		return
	}

	s.mtx.Lock()
	s.received[req.DaemonID] = slices
	s.mtx.Unlock()
	s.setStage(req.DaemonID, req.Stage)
}

// build a list buckets with their objects from a flat list of all slices
func (s *ecRebalancer) mergeSlices() *ecRebResult {
	res := &ecRebResult{
		ais:   make(map[string]*ecRebBck),
		cloud: make(map[string]*ecRebBck),
	}

	// first go through local "slices"
	slices := s.slices[s.t.si.DaemonID]
	for _, slice := range slices {
		if err := res.addSlice(slice); err != nil {
			// cannot happen: it is the first slice adding to the list
			glog.Error(err)
			continue
		}
		// check if it is on HRW mpath
		// TODO: localAction is unused in the current MR
		if slice.hrwFQN != slice.realFQN {
			s.localActions = append(s.localActions, slice)
			s.updateTxStats(slice.ObjSize, true)
		}
	}

	// process all received slices
	for _, sliceList := range s.received {
		for _, slice := range sliceList {
			if err := res.addSlice(slice); err != nil {
				// TODO: add to a list of slices to delete?
				glog.Error(err)
			}
		}
	}
	return res
}

// find objects that have either missing parts or misplaced full object
func (s *ecRebalancer) detectBroken(res *ecRebResult) {
	// remove good objects that have all its slices and the "main" object
	// is on the correct target
	bowner := s.t.GetBowner()
	bmd := bowner.Get()
	for idx, tp := range []map[string]*ecRebBck{res.ais, res.cloud} {
		for bckName, objs := range tp {
			bck := &cluster.Bck{Name: bckName, Provider: cmn.ProviderFromBool(idx == 0)}
			if err := bck.Init(bowner); err != nil {
				glog.Errorf("Invalid bucket %s: %v", bckName, err)
				delete(tp, bckName)
				continue
			}
			bprops, ok := bmd.Get(bck)
			if !ok {
				// TODO: bucket was deleted while rebalance was collecting info?
				glog.Errorf("Bucket %s does not exist", bckName)
				delete(tp, bckName)
				continue
			}
			for objName, obj := range objs.objs {
				obj.replicated = ec.IsECCopy(obj.objSize(), &bprops.EC)
				sliceCnt := obj.sliceRequired()

				mainLocal := false
				// check if the "main" object is on this target
				for _, slice := range obj.slices {
					if slice.DaemonID == s.t.si.DaemonID && slice.SliceID == 0 {
						mainLocal = true
						break
					}
				}
				allSlicesFound := sliceCnt == obj.sliceFound()
				// the object is good, nothing to restore
				if allSlicesFound && mainLocal {
					delete(objs.objs, objName)
					continue
				}
			}
			// all objects are fine - skip the bucket
			if len(objs.objs) == 0 {
				delete(tp, bckName)
			}
		}
	}
}

// select a target from HRW list that did not have a slice before rebalance
// (targets) and is not going to receive a slice while rebalancing (usedTargets)
func (s *ecRebalancer) nextFreeTarget(targets []*cluster.Snode,
	obj *ecRebObject, usedTargets cmn.StringSet) *cluster.Snode {
	for _, tgt := range targets {
		used := usedTargets != nil && usedTargets.Contains(tgt.DaemonID)
		if !used && !obj.sliceExistAt(tgt.DaemonID) {
			if usedTargets != nil {
				usedTargets.Add(tgt.DaemonID)
			}
			return tgt
		}
	}
	// for existing workflow it is impossible: restoring slices kicks in if the
	// number of slices less than it should be and the list of HRW targets always
	// contains the number of targets that equals the required number of slices
	cmn.AssertMsg(false, "No free targets")
	return nil
}

// updates stats(for dry-run mode), to report how many slices/replicas should
// be transferred via network. But this number may be incorrect: e.g, in case
// of restoring an object from existing slices, it is possible that a slice is
// missing, but rebalancer counts only the main object
func (s *ecRebalancer) updateTxStats(size int64, isTx bool) {
	if !s.md.dryRun {
		return
	}
	if isTx {
		s.t.statsif.AddMany(
			stats.NamedVal64{stats.TxRebCount, 1},
			stats.NamedVal64{stats.TxRebSize, size})
	} else {
		s.t.statsif.AddMany(
			stats.NamedVal64{stats.RxRebCount, 1},
			stats.NamedVal64{stats.RxRebSize, size})
	}
}

// Full object is not found on any target. Restore the main object using
// ecManager. Corner case: this target may contain a slice of an object,
// so it has to move existing slice to a free target beforehand
func (s *ecRebalancer) restoreFullObject(obj *ecRebObject, md *ecRebArgs) {
	// First, move local slice to another target if the local target has any
	if md.locID > 0 {
		tgt := s.nextFreeTarget(md.hrwTargets, obj, nil)
		obj.addAction(ecRebActMoveSlice, tgt.DaemonID, md.locID)
		s.updateTxStats(md.sliceSize, true)
	}

	// Second, restore the main object - it is missing
	obj.addAction(ecRebActMainRestore, "", 0)
	s.updateTxStats(obj.objSize(), true)
}

// An object is OK but one or few replicas are missing - just make more copies
// Corner case: this target does not have the object as well - restore with ecManager
func (s *ecRebalancer) restoreReplicas(obj *ecRebObject, md *ecRebArgs) {
	if md.locID != 0 {
		// "main" object is missing, just restore it
		// and resend its slices (ecManager does it on GET)
		obj.addAction(ecRebActMainRestore, "", 0)
		s.updateTxStats(obj.objSize(), true)
	} else {
		// replicas are missing
		sliceDiff := md.sliceReq - md.sliceFound
		for sliceDiff > 0 {
			tgt := s.nextFreeTarget(md.hrwTargets, obj, md.usedTargets)
			obj.addAction(ecRebActCopyReplica, tgt.DaemonID, 0)
			s.updateTxStats(obj.objSize(), true)
			sliceDiff--
		}
	}
}

// This target does not have full object and one or few slices can be missing.
// Fetch a full object from another target if any has the object, then
// rebuild EC slices and send missing ones
func (s *ecRebalancer) fetchMainAndRebuild(obj *ecRebObject, md *ecRebArgs) {
	// First, if the "main" object is on another target we should copy it
	localDaemon := s.t.si.DaemonID
	if localDaemon != md.mainDaemon {
		// copy local slice to a free target if the slice exists
		if md.locID > 0 {
			tgt := s.nextFreeTarget(md.hrwTargets, obj, md.usedTargets)
			obj.addAction(ecRebActMoveSlice, tgt.DaemonID, md.locID)
			s.updateTxStats(md.sliceSize, true)
		}
		// fetch main object
		obj.addAction(ecRebActFetchMain, md.mainDaemon, 0)
		s.updateTxStats(obj.objSize(), true)
	}

	// Second, rebuild missing slices
	s.findMissingSlices(obj, md.hrwTargets, md.usedTargets)
}

// determine which slices must be moved to other targets. Local moves are already
// detected in mergeSlices
func (s *ecRebalancer) findSolutions(slices *ecRebResult) {
	if len(slices.ais) == 0 && len(slices.cloud) == 0 && len(s.localActions) == 0 {
		// Everything is fine. Nothing to do
		return
	}

	var err error
	localDaemon := s.t.si.DaemonID
	for _, tp := range []map[string]*ecRebBck{slices.cloud, slices.ais} {
		for _, objs := range tp {
			for objName, obj := range objs.objs {
				rmd := &ecRebArgs{
					sliceReq:   obj.sliceRequired(),
					sliceFound: obj.sliceFound(),
					sliceSize:  ec.SliceSize(obj.objSize(), int(obj.dataSlices())),
				}
				// calculate preferred targets
				rmd.hrwTargets, err = cluster.HrwTargetList(obj.bucket(), obj.objName(), s.t.smapowner.Get(), rmd.sliceReq)
				if err != nil {
					s.updateTxStats(obj.objSize(), false)
					// TODO: abort xaction?
					glog.Error(err)
					delete(objs.objs, objName)
					continue
				}
				rmd.locID, rmd.mainDaemon = obj.objLocation(localDaemon)

				// 1. check if we can restore missing object
				//    subtract '1' as 'sliceReq' includes the full object
				sliceDiff := rmd.sliceReq - rmd.sliceFound - 1
				if rmd.mainDaemon == "" && !obj.replicated && sliceDiff > int(obj.paritySlices()) {
					glog.Errorf("[%s] %s/%s unrepairable - too few slices(missing %d of max %d)",
						localDaemon, obj.bucket(), obj.objName(), sliceDiff, obj.paritySlices())
					s.updateTxStats(obj.objSize(), false)
					delete(objs.objs, objName)
					// TODO: for cloud we can reget the object. For ais - delete?
					continue
				}

				// 2. the simplest case: "main" object is missing. Just restore
				// it and resend the slices (ecManager does it on GET)
				if rmd.mainDaemon == "" {
					s.restoreFullObject(obj, rmd)
					continue
				}

				rmd.usedTargets = make(cmn.StringSet)
				// 3. another simple case: one or few replicas are missing
				if obj.replicated {
					s.restoreReplicas(obj, rmd)
					continue
				}

				// erasure coding cases
				// after this line case mainDaemon == "" && locID == 0 is impossible
				cmn.AssertMsg(rmd.mainDaemon != "" || rmd.locID != 0, "mainDaemon or locID must be defined")

				// 4. "main" exists but some remote slices missing
				s.fetchMainAndRebuild(obj, rmd)
			}
		}
	}
}

// Checks if any slice is missing
func (s *ecRebalancer) findMissingSlices(obj *ecRebObject, targets []*cluster.Snode, usedTargets cmn.StringSet) {
	sliceSize := ec.SliceSize(obj.objSize(), int(obj.dataSlices()))
	for i := int16(1); i <= int16(obj.sliceRequired()); i++ {
		if !obj.sliceIDExists(i) {
			tgt := s.nextFreeTarget(targets, obj, usedTargets)
			obj.addAction(ecRebActRestoreSlice, tgt.DaemonID, i)
			s.updateTxStats(sliceSize, true)
			break
		}
	}
}

// merge, sort, and detect what to fix and how
func (s *ecRebalancer) checkSlices() *ecRebResult {
	slices := s.mergeSlices()
	s.detectBroken(slices)
	s.findSolutions(slices)
	s.changeStage(rebStageECDetect)
	return slices
}

// mountpath walker - walks through files in /meta/ directory
func (s *ecRebalancer) jog(path string, wg *sync.WaitGroup) {
	defer wg.Done()
	opts := &fs.Options{
		Callback: s.walk,
		Sorted:   false,
	}
	if err := fs.Walk(path, opts); err != nil {
		if s.md.xreb.Aborted() || s.md.xreb.Finished() {
			glog.Infof("Aborting %s traversal", path)
		} else {
			glog.Warningf("failed to traverse %q, err: %v", path, err)
		}
	}
}

// a file walker:
// - loads EC metadata from file
// - checks if the corresponding slice/replica exists
// - calculates where "main" object for the slice/replica is
// - store all the info above to memory
func (s *ecRebalancer) walk(fqn string, de fs.DirEntry) (err error) {
	if s.md.xreb.Aborted() {
		// notify `dir.Walk` to stop iterations
		return errors.New("Interrupt walk")
	}

	if de.IsDir() {
		return nil
	}

	md, err := ec.LoadMetadata(fqn)
	if err != nil {
		// TODO: abort?
		glog.Error(err)
		return nil
	}

	lom := &cluster.LOM{T: s.t, FQN: fqn}
	err = lom.Init("", "")
	if err != nil {
		return nil
	}
	// do not touch directories for buckets with EC disabled (for now)
	// TODO: what to do if we found metafile on a bucket with EC disabled?
	if !lom.ECEnabled() {
		return filepath.SkipDir
	}

	// generate slice path in the same mpath that metadata is
	var fileFQN string
	if ec.IsECCopy(md.Size, &lom.Bck().Props.EC) {
		fileFQN = lom.ParsedFQN.MpathInfo.MakePathBucketObject(fs.ObjectType, lom.Bucket(), lom.Bck().Provider, lom.Objname)
	} else {
		fileFQN = lom.ParsedFQN.MpathInfo.MakePathBucketObject(ec.SliceType, lom.Bucket(), lom.Bck().Provider, lom.Objname)
	}
	// found metadata without a corresponding slice - delete stray metadata
	if _, err := os.Stat(fileFQN); err != nil {
		os.Remove(fqn)
		return nil
	}

	// calculate correct FQN
	hrwFQN, _, err := cluster.HrwFQN(fs.ObjectType, lom.Bck(), lom.Objname)
	if err != nil {
		// TODO: abort?
		return err
	}
	lom = &cluster.LOM{T: s.t, FQN: fileFQN}
	err = lom.Init("", "")
	if err != nil {
		return nil
	}

	si, err := cluster.HrwTarget(lom.Bck(), lom.Objname, s.t.smapowner.Get())
	if err != nil {
		// TODO: abort? something bad happens, e.g not enough targets
		return err
	}
	rec := &ecRebSlice{
		Bucket:       lom.Bucket(),
		Objname:      lom.Objname,
		DaemonID:     s.t.si.DaemonID,
		ObjHash:      md.ObjChecksum,
		ObjSize:      md.Size,
		SliceID:      int16(md.SliceID),
		DataSlices:   int16(md.Data),
		ParitySlices: int16(md.Parity),
		IsAIS:        lom.IsAIS(),
		realFQN:      fileFQN,
		hrwFQN:       hrwFQN,
	}
	s.mtx.Lock()
	s.slices[si.DaemonID] = append(s.slices[si.DaemonID], rec)
	s.mtx.Unlock()

	return nil
}

// Empties internal temporary data to be ready for the next rebalance.
// It does not nullify slices/maps because ecRebalancer is global thing and
// is not recreated every rebalance run
func (s *ecRebalancer) cleanup() {
	s.mtx.Lock()
	s.nodes = make(map[string]uint32)
	s.received = make(ecSliceList)
	s.slices = make(ecSliceList)
	s.localActions = make([]*ecRebSlice, 0)
	s.mtx.Unlock()
}

func (s *ecRebalancer) stop() {
	s.ack.Close(true)
	s.data.Close(true)
}

// establishes transport stream bundles
func (s *ecRebalancer) init(md *globalRebArgs) error {
	s.md = md
	cbReq := func(hdr transport.Header, reader io.ReadCloser, _ unsafe.Pointer, err error) {
		if err != nil {
			glog.Errorf("Failed to request %s/%s: %v", hdr.Bucket, hdr.Objname, err)
		}
	}
	extraReq := transport.Extra{
		Callback: cbReq,
	}
	ackArgs := transport.SBArgs{
		Extra:      &extraReq,
		Network:    s.netc,
		Trname:     ackECRebStreamName,
		Multiplier: 1,
	}
	client := transport.NewIntraDataClient()
	s.ack = transport.NewStreamBundle(s.t.smapowner, s.t.Snode(), client, ackArgs)
	dataArgs := transport.SBArgs{
		Extra:      &extraReq,
		Network:    s.netd,
		Trname:     dataECRebStreamName,
		Multiplier: 1,
	}
	s.data = transport.NewStreamBundle(s.t.smapowner, s.t.Snode(), client, dataArgs)
	return nil
}

// Main method - starts all mountpaths walkers, waits for them to finish, and
// changes internal stage after that to 'traverse done', so the caller may continue
// rebalancing: send collected data to other targets, rebuild slices etc
func (s *ecRebalancer) run() error {
	var mpath string
	wg := sync.WaitGroup{}
	availablePaths, _ := fs.Mountpaths.Get()
	for _, mpathInfo := range availablePaths {
		if s.md.xreb.bucket == "" {
			mpath = mpathInfo.MakePath(ec.MetaType, cmn.AIS)
		} else {
			mpath = mpathInfo.MakePathBucket(ec.MetaType, s.md.xreb.bucket, cmn.AIS)
		}
		wg.Add(1)
		go s.jog(mpath, &wg)
	}
	for _, mpathInfo := range availablePaths {
		if s.md.xreb.bucket == "" {
			mpath = mpathInfo.MakePath(ec.MetaType, cmn.Cloud)
		} else {
			mpath = mpathInfo.MakePathBucket(ec.MetaType, s.md.xreb.bucket, cmn.Cloud)
		}
		wg.Add(1)
		go s.jog(mpath, &wg)
	}
	wg.Wait()
	s.changeStage(rebStageECNameSpace)
	return nil
}

// send collected slices to correct targets with retry
func (s *ecRebalancer) exchange() error {
	const (
		// number of retries to send collected slice info
		retries = 3
		// delay between retries
		sleep = 5 * time.Second
	)

	smap := s.t.smapowner.Get()
	sendTo := make([]*cluster.Snode, 0, len(smap.Tmap))
	failed := make([]*cluster.Snode, 0, len(smap.Tmap))
	for _, node := range smap.Tmap {
		if node.DaemonID == s.t.si.DaemonID {
			continue
		}
		sendTo = append(sendTo, node)
	}

	emptySlice := make([]*ecRebSlice, 0)
	for i := 0; i < retries; i++ {
		failed = failed[:0]
		for _, node := range sendTo {
			if s.md.xreb.Aborted() {
				return fmt.Errorf("%d: aborted", s.md.globRebID)
			}

			slices, ok := s.slices[node.DaemonID]
			if !ok {
				// no data collected for the target, send empty list anyway
				slices = emptySlice
			}

			req := pushNotify{
				DaemonID: s.t.si.DaemonID,
				Stage:    rebStageECExchange,
				RebID:    s.md.globRebID,
			}
			body := cmn.MustMarshal(slices)
			hdr := transport.Header{
				ObjAttrs: transport.ObjectAttrs{Size: int64(len(body))},
				Opaque:   cmn.MustMarshal(req),
			}

			rd := cmn.NewByteHandle(body)
			if err := s.data.SendV(hdr, rd, nil, nil, node); err != nil {
				glog.Errorf("Failed to send slices to node %s: %v", node.DaemonID, err)
				failed = append(failed, node)
			}
		}

		if len(failed) == 0 {
			s.setStage(s.t.si.DaemonID, rebStageECExchange)
			return nil
		}

		time.Sleep(sleep)
		copy(sendTo, failed)
	}

	return fmt.Errorf("Could not sent data to %d nodes", len(failed))
}

// re-request EC slice/objects data prepared by the remote node for this node
// If the remote has not prepared the data(e.g, it is still sorting and
// calculating), it responds with StatusNoContent
func (s *ecRebalancer) requestRebData(tsi *cluster.Snode) bool {
	var (
		query  = url.Values{}
		header = make(http.Header)
		loghdr = fmt.Sprintf("[%d]", s.md.globRebID)
	)

	header.Set(cmn.HeaderCallerID, s.t.si.DaemonID)
	query.Add(cmn.URLParamRebData, "true")
	args := callArgs{
		si: tsi,
		req: cmn.ReqArgs{
			Method: http.MethodGet,
			Header: header,
			Base:   tsi.URL(cmn.NetworkIntraData),
			Path:   cmn.URLPath(cmn.Version, cmn.Rebalance),
			Query:  query,
		},
		timeout: defaultTimeout,
	}
	res := s.t.call(args)
	// the callee is still processing the data, wait for more
	if res.status == http.StatusAccepted {
		return false
	}
	if res.err != nil {
		// something bad happened, aborting
		glog.Errorf("%s: failed to call %s, err: %v", loghdr, tsi.Name(), res.err)
		s.md.xreb.Abort()
		return false
	}
	slices := make([]*ecRebSlice, 0)
	// the callee has processed the data but has no information for this target
	if res.status != http.StatusNoContent {
		// TODO: send the number of items in push request and preallocate `slices`
		if err := jsoniter.Unmarshal(res.outjson, &slices); err != nil {
			// not a severe error: return false so next wait loop re-requests the data
			glog.Errorf("Invalid JSON received from %s: %v", tsi.Name(), err)
			return false
		}
	}
	s.mtx.Lock()
	s.received[tsi.DaemonID] = slices
	s.mtx.Unlock()
	return true
}

// return list of slices collected by local target for a given one
func (s *ecRebalancer) targetSlices(daemonID string) []*ecRebSlice {
	s.mtx.Lock()
	defer s.mtx.Unlock()
	if slices, ok := s.slices[daemonID]; ok {
		return slices
	}
	return nil
}
