package ais

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path/filepath"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/ec"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/transport"
	jsoniter "github.com/json-iterator/go"
)

const (
	// transport name for push notifications
	ackECScrubStreamName = "scrub-ec-ack"
)

type (
	// a full info about a slice found on a target.
	// It is information sent to other targets that must be sufficient
	// to check and restore all main objects/replicas/slices
	scrubECSlice struct {
		Bucket  string `json:"bck"`
		Objname string `json:"obj"`
		// a target that has the found slice
		DaemonID     string `json:"sid"`
		ObjHash      string `json:"cksum"`
		ObjSize      int64  `json:"size"`
		SliceID      int16  `json:"sliceid,omitempty"`
		DataSlices   int16  `json:"data"`
		ParitySlices int16  `json:"parity"`
		IsAIS        bool   `json:"ais,omitempty"`
		// a target has only metadata file but the corresponding
		// slice/replica/main object does not exist
		FileExists bool `json:"exists,omitempty"`
	}

	pushReq struct {
		DaemonID string `json:"sid"`
		Stage    uint32 `json:"stage"`
	}

	// a listener for push notifications from other targets.
	// A target, after moving to the next stage, sends the new stage to
	// all targets. This object receives all notifications and keeps a list
	// of targets with their states.
	// Later it minimize the amount of network requests when waiting for all
	// targets to finish doing something: only targets which did not send
	// notification would receive requests
	pushListener struct {
		// a map of nodes and stages they have reached by the current moment
		nodes     map[string]uint32
		smap      *smapX
		mtx       sync.Mutex
		globRebID int64
	}

	ecSliceList = map[string][]*scrubECSlice

	ecScrubber struct {
		t *targetrunner
		// maps daemonID by HRW <-> Slice
		// TODO: it is not used in this MR - the next MRs will
		// sort it, distribute and then do real rebalance
		slices ecSliceList //
		xreb   *xactGlobalReb
		waiter *pushListener
		ack    *transport.StreamBundle
		netc   string
		mtx    sync.Mutex
	}
)

func newPushListener(smap *smapX, globRebID int64) *pushListener {
	w := &pushListener{
		nodes:     make(map[string]uint32),
		smap:      smap,
		globRebID: globRebID,
	}
	return w
}

// Mark a 'node' that it has passed the 'stage'. If the node has already
// marked or its stage is higher, the function does nothing
func (w *pushListener) setStage(node string, stage uint32) {
	w.mtx.Lock()
	if currStage, ok := w.nodes[node]; !ok || currStage < stage {
		w.nodes[node] = stage
	}
	w.mtx.Unlock()
}

// Return the list of nodes that do not reach the stage yet.
// Used by pull notification to minimize the number of network request
func (w *pushListener) nodesNotInStage(stage uint32) []*cluster.Snode {
	nodes := make([]*cluster.Snode, 0, len(w.smap.Tmap))
	w.mtx.Lock()
	for _, si := range w.smap.Tmap {
		if currStage, ok := w.nodes[si.DaemonID]; !ok || currStage < stage {
			nodes = append(nodes, si)
			continue
		}
	}
	w.mtx.Unlock()
	return nodes
}

// Waits until all nodes are in 'stage'.
// Checks push notifications, and request stage from all nodes that have not
// sent any notification yet
func (w *pushListener) waitAllStage(reb *rebManager, xreb *xactGlobalReb, stage uint32) error {
	const sleep = 5 * time.Second
	config := cmn.GCO.Get()
	ver := w.smap.Version
	for {
		nodes := w.nodesNotInStage(stage)
		if len(nodes) == 0 {
			return nil
		}
		if xreb.Aborted() {
			return errors.New("Aborted")
		}
		anyFailed := false
		for _, si := range nodes {
			status, ok := reb.checkGlobStatus(si, w.smap, w.globRebID, config, ver, xreb, stage)
			if !ok || status.Stage < stage {
				anyFailed = true
				break
			}
		}
		if !anyFailed {
			return nil
		}
		time.Sleep(sleep)
	}
}

func newECScrubList() ecSliceList {
	return make(map[string][]*scrubECSlice)
}

func newECScrubber(t *targetrunner, xreb *xactGlobalReb, globRebID int64) *ecScrubber {
	return &ecScrubber{
		t:      t,
		slices: newECScrubList(),
		waiter: newPushListener(t.smapowner.get(), globRebID),
		xreb:   xreb,
	}
}

// Scrubber moves to the next stage:
// - update internal stage
// - send notification to all other targets that this one is in a new stage
func (s *ecScrubber) changeStage(newStage uint32) error {
	// first, set self stage
	s.waiter.setStage(s.t.Snode().DaemonID, newStage)

	smap := s.t.smapowner.Get()
	nodes := make([]*cluster.Snode, 0, len(smap.Tmap))
	for _, node := range smap.Tmap {
		if node.DaemonID == s.t.Snode().DaemonID {
			continue
		}
		nodes = append(nodes, node)
	}

	req := pushReq{
		DaemonID: s.t.Snode().DaemonID,
		Stage:    newStage,
	}
	hdr := transport.Header{
		ObjAttrs: transport.ObjectAttrs{
			Size: 0,
		},
		Opaque: cmn.MustMarshal(req),
	}
	if err := s.ack.SendV(hdr, nil, nil, nil, nodes...); err != nil {
		glog.Errorf("Failed to send ack %d to %d nodes: %v", newStage, len(nodes), err)
		return err
	}
	return nil
}

// On receiving a notification from another target
func (s *ecScrubber) OnAck(w http.ResponseWriter, hdr transport.Header, reader io.Reader, err error) {
	if err != nil {
		glog.Errorf("Failed to get ack %s from %s: %v", hdr.Objname, hdr.Bucket, err)
		return
	}

	var req pushReq
	if err := jsoniter.Unmarshal(hdr.Opaque, &req); err != nil {
		glog.Errorf("Invalid push notification: %v", err)
		return
	}

	s.waiter.setStage(req.DaemonID, req.Stage)
}

// New slice/replica found - put it into correct list depending on slice HRW
func (s *ecScrubber) add(daemonID string, obj *scrubECSlice) {
	s.mtx.Lock()
	s.slices[daemonID] = append(s.slices[daemonID], obj)
	s.mtx.Unlock()
}

// mountpath walker - walks through files in /meta/ directory
func (s *ecScrubber) jog(path string, wg *sync.WaitGroup) {
	defer wg.Done()
	opts := &fs.Options{
		Callback: s.walk,
		Sorted:   false,
	}
	if err := fs.Walk(path, opts); err != nil {
		if s.xreb.Aborted() || s.xreb.Finished() {
			glog.Infof("Aborting %s traversal", path)
		} else {
			glog.Errorf("failed to traverse %q, err: %v", path, err)
		}
	}
}

// a file walker:
// - loads EC metadata from file
// - checks if the corresponding slice/replica exists
// - calculates where "main" object for the slice/replica is
// - store all the info above to memory
func (s *ecScrubber) walk(fqn string, de fs.DirEntry) (err error) {
	if s.xreb.Aborted() {
		// notify `dir.Walk` to stop iterations
		return errors.New("Interrupt walk")
	}

	if de.IsDir() {
		return nil
	}
	lom := &cluster.LOM{T: s.t, FQN: fqn}
	err = lom.Init("", "")
	if err != nil {
		glog.Infof("Skipping %q: %v", fqn, err)
		return nil
	}

	// do not touch directories for buckets with EC disabled (for now)
	// TODO: what to do if we found metafile on a bucket with EC disabled?
	if !lom.ECEnabled() {
		return filepath.SkipDir
	}

	b, err := ioutil.ReadFile(fqn)
	if err != nil {
		glog.Errorf("Failed to read metafile %q: %v", fqn, err)
		return nil
	}
	var md ec.Metadata
	if err := jsoniter.Unmarshal(b, &md); err != nil {
		glog.Infof("Damaged metafile %q: %v", fqn, err)
		return nil
	}

	fileFQN, _, err := cluster.HrwFQN(fs.ObjectType, lom.Bck(), lom.Objname)
	if err != nil {
		glog.Infof("Object fail %q: %v", fqn, err)
		return nil
	}
	lom = &cluster.LOM{T: s.t, FQN: fileFQN}
	err = lom.Init("", "")
	if err != nil {
		glog.Infof("Skipping %q: %v", fileFQN, err)
		return nil
	}

	// TODO: correct mpath? what if disk was added?
	si, err := cluster.HrwTarget(lom.Bck(), lom.Objname, s.t.smapowner.Get())
	if err != nil {
		return err
	}
	rec := &scrubECSlice{
		Bucket:       lom.Bucket(),
		Objname:      lom.Objname,
		DaemonID:     s.t.Snode().DaemonID,
		ObjHash:      md.ObjChecksum,
		ObjSize:      md.Size,
		SliceID:      int16(md.SliceID),
		DataSlices:   int16(md.Data),
		ParitySlices: int16(md.Parity),
		IsAIS:        lom.IsAIS(),
		FileExists:   lom.Exists(),
	}
	s.add(si.DaemonID, rec)

	return nil
}

// closes and unregisters all transport streams, all channels etc
func (s *ecScrubber) stop() {
	s.ack.Close(true)
	transport.Unregister(s.netc, ackECScrubStreamName)
}

// establishes transport stream bundles
func (s *ecScrubber) init() error {
	s.netc = cmn.NetworkPublic
	config := cmn.GCO.Get()
	if config.Net.UseIntraControl {
		s.netc = cmn.NetworkIntraControl
	}
	cbReq := func(hdr transport.Header, reader io.ReadCloser, _ unsafe.Pointer, err error) {
		if err != nil {
			glog.Errorf("Failed to request %s/%s: %v", hdr.Bucket, hdr.Objname, err)
		}
	}
	extraReq := transport.Extra{
		Callback: cbReq,
	}
	ackArgs := transport.SBArgs{
		Extra:   &extraReq,
		Network: s.netc,
		Trname:  ackECScrubStreamName,
	}
	client := transport.NewDefaultClient()
	s.ack = transport.NewStreamBundle(s.t.smapowner, s.t.Snode(), client, ackArgs)
	if _, err := transport.Register(s.netc, ackECScrubStreamName, s.OnAck); err != nil {
		err := fmt.Errorf("Failed to register ack stream for %s: %v", ackECScrubStreamName, err)
		return err
	}
	return nil
}

// Main method - starts all mountpaths walkers, waits for them to finish, and
// changes internal stage after that to 'traverse done', so the caller may continue
// rebalancing: send collected data to other targets, rebuild slices etc
// TODO: support selected bucket only
func (s *ecScrubber) run() error {
	if err := s.init(); err != nil {
		glog.Errorf("Failed to start EC rebalance: %v", err)
		return err
	}

	wg := sync.WaitGroup{}
	availablePaths, _ := fs.Mountpaths.Get()
	for _, mpathInfo := range availablePaths {
		mpath := mpathInfo.MakePath(ec.MetaType, cmn.AIS)
		wg.Add(1)
		go s.jog(mpath, &wg)
	}
	for _, mpathInfo := range availablePaths {
		mpath := mpathInfo.MakePath(ec.MetaType, cmn.Cloud)
		wg.Add(1)
		go s.jog(mpath, &wg)
	}
	wg.Wait()
	s.changeStage(rebStageECNameSpace)
	return nil
}
