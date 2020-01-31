// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dsort

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort/extract"
	"github.com/NVIDIA/aistore/dsort/filetype"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/pkg/errors"
)

const (
	// Stream names
	recvReqStreamNameFmt  = cmn.DSortNameLowercase + "-%s-recv_req"
	recvRespStreamNameFmt = cmn.DSortNameLowercase + "-%s-recv_resp"
	shardStreamNameFmt    = cmn.DSortNameLowercase + "-%s-shard"
)

// State of the cleans - see `cleanup` and `finalCleanup`
const (
	noCleanedState = iota
	initiallyCleanedState
	finallyCleanedState
)

var (
	ctx      dsortContext
	mm       *memsys.MMSA
	once     sync.Once
	initOnce = func() {
		fs.CSM.RegisterContentType(filetype.DSortFileType, &filetype.DSortFile{})
		fs.CSM.RegisterContentType(filetype.DSortWorkfileType, &filetype.DSortFile{})
		if mm == nil {
			mm = memsys.DefaultPageMM() // NOTE: only for unit tests
		}

	}
	_ cluster.Slistener = &Manager{}
)

type (
	dsortContext struct {
		smap     cluster.Sowner
		bmdowner cluster.Bowner
		node     *cluster.Snode
		t        cluster.Target
		stats    stats.Tracker
	}

	creationPhaseMetadata struct {
		Shards    []*extract.Shard          `json:"shards"`
		SendOrder map[string]*extract.Shard `json:"send_order"`
	}

	buildingShardInfo struct {
		ShardName string `json:"shard_name"`
	}

	// progressState abstracts all information meta information about progress of
	// the job.
	progressState struct {
		inProgress bool
		aborted    bool
		cleaned    uint8      // current state of the cleanliness - no cleanup, initial cleanup, final cleanup
		cleanWait  *sync.Cond // waiting room for `cleanup` and `finalCleanup` method so then can run in correct order
		wg         *sync.WaitGroup
		// doneCh is closed when the job is aborted so that goroutines know when
		// they need to stop.
		doneCh chan struct{}
	}

	remoteResponse struct {
		Record    *extract.Record    `json:"r"`
		RecordObj *extract.RecordObj `json:"o"`
	}

	// Manager maintains all the state required for a single run of a distributed archive file shuffle.
	Manager struct {
		// Fields with json tags are the only fields which are persisted
		// into the disk once the dSort is finished.
		ManagerUUID string   `json:"manager_uuid"`
		Metrics     *Metrics `json:"metrics"`

		mu   sync.Mutex
		ctx  dsortContext
		smap *cluster.Smap

		recManager     *extract.RecordManager
		extractCreator extract.ExtractCreator

		startShardCreation chan struct{}
		rs                 *ParsedRequestSpec

		client        *http.Client // Client for sending records metadata
		fileExtension string
		compression   struct {
			compressed   atomic.Int64 // Total compressed size
			uncompressed atomic.Int64 // Total uncompressed size
		}
		received struct {
			count atomic.Int32 // Number of FileMeta slices received, defining what step in the sort a target is in.
			ch    chan int32
		}
		refCount        atomic.Int64 // Reference counter used to determine if we can do cleanup
		state           progressState
		extractionPhase struct {
			adjuster *concAdjuster
		}
		streams struct {
			shards *transport.StreamBundle // streams for pushing streams to other targets if the fqn is non-local
		}
		creationPhase struct {
			metadata creationPhaseMetadata
		}
		finishedAck struct {
			mu sync.Mutex
			m  map[string]struct{} // finished acks: daemonID -> ack
		}

		dsorter dsorter

		callTimeout time.Duration // Maximal time we will wait for other node to respond
	}
)

func RegisterNode(smap cluster.Sowner, bmdowner cluster.Bowner, snode *cluster.Snode, mmsa *memsys.MMSA,
	t cluster.Target, stats stats.Tracker) {
	ctx.smap = smap
	ctx.bmdowner = bmdowner
	ctx.node = snode
	ctx.t = t
	ctx.stats = stats
	// TODO: try to introduce and benchmark a separate MMSA instance, e.g.:
	//       mm = &memsys.MMSA{Name: cmn.DSortName + ".MMSA", TimeIval: time.Minute * 10, ...}
	cmn.Assert(mm == nil)
	mm = mmsa
}

// init initializes all necessary fields.
//
// NOTE: should be done under lock.
func (m *Manager) init(rs *ParsedRequestSpec) error {
	// Initialize memsys and register dsort file type but only at the first
	// time some manager will be initialized.
	once.Do(initOnce)

	// smap, nameLocker setup
	m.ctx = ctx
	m.smap = m.ctx.smap.Get()

	m.ctx.smap.Listeners().Reg(m)

	targetCount := m.smap.CountTargets()

	m.rs = rs
	m.Metrics = newMetrics(rs.Description, rs.ExtendedMetrics)
	m.startShardCreation = make(chan struct{}, 1)

	if err := m.setDSorter(); err != nil {
		return err
	}

	if err := m.dsorter.init(); err != nil {
		return err
	}

	// Set extract creator depending on extension provided by the user
	if err := m.setExtractCreator(); err != nil {
		return err
	}

	// NOTE: Total size of the records metadata can sometimes be large
	// and so this is why we need such a long timeout.
	m.client = cmn.NewClient(cmn.TransportArgs{
		DialTimeout: 5 * time.Minute,
		Timeout:     30 * time.Minute,
	})

	m.fileExtension = rs.Extension
	m.received.ch = make(chan int32, 10)

	// By default we want avg compression ratio to be equal to 1
	m.compression.compressed = *atomic.NewInt64(1)
	m.compression.uncompressed = *atomic.NewInt64(1)

	// Concurrency

	// Number of goroutines should be larger than number of concurrency limit
	// but it should not be:
	// * too small - we don't want to artificially bottleneck the phases.
	// * too large - we don't want too much goroutines in the system, it can cause
	//               too much overhead on context switching and managing the goroutines.
	//               Also for large workloads goroutines can take a lot of memory.
	//
	// Coefficient for extraction should be larger and depends on target count
	// because we will skip a lot shards (which do not belong to us).
	m.extractionPhase.adjuster = newConcAdjuster(rs.ExtractConcLimit, 3*targetCount /*goroutineLimitCoef*/)

	// Fill ack map with current daemons. Once the finished ack is received from
	// another daemon we will remove it from the map until len(ack) == 0 (then
	// we will know that all daemons have finished operation).
	m.finishedAck.m = make(map[string]struct{}, targetCount)
	for sid := range m.smap.Tmap {
		m.finishedAck.m[sid] = struct{}{}
	}

	m.setInProgressTo(true)
	m.setAbortedTo(false)
	m.state.cleanWait = sync.NewCond(&m.mu)

	m.callTimeout = cmn.GCO.Get().DSort.CallTimeout

	return nil
}

// TODO: Currently we create streams for each dSort job but maybe we should
// create streams once and have them available for all the dSort jobs so they
// would share the resource rather than competing for it.
func (m *Manager) initStreams() error {
	cfg := cmn.GCO.Get()

	// Responses to the other targets are objects that is why we want to use
	// intraData network.
	respNetwork := cmn.NetworkIntraData
	if !cfg.Net.UseIntraData {
		respNetwork = cmn.NetworkPublic
	}

	trname := fmt.Sprintf(shardStreamNameFmt, m.ManagerUUID)
	shardsSbArgs := transport.SBArgs{
		Multiplier: transport.IntraBundleMultiplier,
		Network:    respNetwork,
		Trname:     trname,
		Ntype:      cluster.Targets,
		Extra: &transport.Extra{
			Compression: cfg.DSort.Compression,
			Config:      cfg,
			MMSA:        mm,
		},
	}
	if _, err := transport.Register(respNetwork, trname, m.makeRecvShardFunc()); err != nil {
		return errors.WithStack(err)
	}

	client := transport.NewIntraDataClient()
	m.streams.shards = transport.NewStreamBundle(m.ctx.smap, m.ctx.node, client, shardsSbArgs)
	return nil
}

func (m *Manager) cleanupStreams() error {
	cfg := cmn.GCO.Get()
	// Responses to the other targets are objects that is why we want to use
	// intraData network.
	respNetwork := cmn.NetworkIntraData
	if !cfg.Net.UseIntraData {
		respNetwork = cmn.NetworkPublic
	}

	if m.streams.shards != nil {
		trname := fmt.Sprintf(shardStreamNameFmt, m.ManagerUUID)
		if err := transport.Unregister(respNetwork, trname); err != nil {
			return errors.WithStack(err)
		}
	}

	for _, streamBundle := range []*transport.StreamBundle{m.streams.shards} {
		if streamBundle != nil {
			streamBundle.Close(false)
		}
	}

	return nil
}

// cleanup removes all memory allocated and removes all files created during sort run.
//
// PRECONDITION: manager must be not in progress state (either actual finish or abort).
//
// NOTE: If cleanup is invoked during the run it is treated as abort.
func (m *Manager) cleanup() {
	m.lock()
	if m.state.cleaned != noCleanedState {
		m.unlock()
		return // do not clean if already scheduled
	}

	m.dsorter.cleanup()
	glog.Infof("%s %s has started a cleanup", cmn.DSortName, m.ManagerUUID)
	now := time.Now()

	defer func() {
		m.state.cleaned = initiallyCleanedState
		m.state.cleanWait.Signal()
		m.unlock()
		glog.Infof("%s %s cleanup has been finished in %v", cmn.DSortName, m.ManagerUUID, time.Since(now))
	}()

	cmn.AssertMsg(!m.inProgress(), fmt.Sprintf("%s: was still in progress", m.ManagerUUID))

	m.extractCreator = nil
	m.client = nil

	m.ctx.smap.Listeners().Unreg(m)

	if !m.aborted() {
		m.updateFinishedAck(m.ctx.node.DaemonID)
	}
}

// finalCleanup is invoked only when all the target confirmed finishing the
// dSort operations. To ensure that finalCleanup is not invoked before regular
// cleanup is finished, we also ack ourselves.
//
// finalCleanup can be invoked only after cleanup and this is ensured by
// maintaining current state of the cleanliness and having conditional variable
// on which finalCleanup will sleep if needed. Note that it is hard (or even
// impossible) to ensure that cleanup and finalCleanup will be invoked in order
// without having ordering mechanism since cleanup and finalCleanup are invoked
// in goroutines (there is possibility that finalCleanup would start before
// cleanup) - this cannot happen with current ordering mechanism.
func (m *Manager) finalCleanup() {
	m.lock()
	for m.state.cleaned != initiallyCleanedState {
		if m.state.cleaned == finallyCleanedState {
			m.unlock()
			return // do not clean if already cleaned
		} else if m.state.cleaned == noCleanedState {
			m.state.cleanWait.Wait() // wait for wake up from `cleanup` or other `finalCleanup` method
		}
	}

	glog.Infof("%s %s has started a final cleanup", cmn.DSortName, m.ManagerUUID)
	now := time.Now()

	if err := m.cleanupStreams(); err != nil {
		glog.Error(err)
	}

	if err := m.dsorter.finalCleanup(); err != nil {
		glog.Error(err)
	}

	// The reason why this is not in regular cleanup is because we are only sure
	// that this can be freed once we cleanup streams - streams are asynchronous
	// and we may have race between in-flight request and cleanup.
	m.recManager.Cleanup()
	extract.FreeMemory()

	m.creationPhase.metadata.SendOrder = nil
	m.creationPhase.metadata.Shards = nil

	m.finishedAck.m = nil

	// Update clean state
	m.state.cleaned = finallyCleanedState
	m.state.cleanWait.Signal() // if there is another `finalCleanup` waiting it should be woken up to check the state and exit
	m.unlock()

	Managers.persist(m.ManagerUUID)
	glog.Infof("%s %s final cleanup has been finished in %v", cmn.DSortName, m.ManagerUUID, time.Since(now))
}

// abort stops currently running sort job and frees associated resources.
func (m *Manager) abort(errs ...error) {
	m.lock()
	if m.aborted() { // do not abort if already aborted
		m.unlock()
		return
	}

	if len(errs) > 0 {
		m.Metrics.lock()
		for _, err := range errs {
			m.Metrics.Errors = append(m.Metrics.Errors, err.Error())
		}
		m.Metrics.unlock()
	}

	glog.Infof("manager %s has been aborted", m.ManagerUUID)
	m.setAbortedTo(true)
	inProgress := m.inProgress()
	m.unlock()

	// If job has already finished we just free resources.
	if inProgress {
		m.dsorter.onAbort()
		m.waitForFinish()
	}

	go func() {
		m.cleanup()
		m.finalCleanup() // on abort always perform final cleanup
	}()
}

// setDSorter sets what type of dsorter implementation should be used
func (m *Manager) setDSorter() (err error) {
	switch m.rs.DSorterType {
	case DSorterGeneralType:
		m.dsorter, err = newDSorterGeneral(m)
	case DSorterMemType:
		m.dsorter = newDSorterMem(m)
	default:
		cmn.AssertMsg(false, fmt.Sprintf("dsorter type is invalid: %q", m.rs.DSorterType))
	}
	return
}

// setExtractCreator sets what type of file extraction and creation is used based on the RequestSpec.
func (m *Manager) setExtractCreator() (err error) {
	var (
		cfg          = cmn.GCO.Get().DSort
		keyExtractor extract.KeyExtractor
	)

	switch m.rs.Algorithm.Kind {
	case SortKindContent:
		keyExtractor, err = extract.NewContentKeyExtractor(m.rs.Algorithm.FormatType, m.rs.Algorithm.Extension)
	case SortKindMD5:
		keyExtractor, err = extract.NewMD5KeyExtractor()
	default:
		keyExtractor, err = extract.NewNameKeyExtractor()
	}

	if err != nil {
		return errors.WithStack(err)
	}

	onDuplicatedRecords := func(msg string) error {
		return m.react(cfg.DuplicatedRecords, msg)
	}

	var extractCreator extract.ExtractCreator
	switch m.rs.Extension {
	case ExtTar:
		extractCreator = extract.NewTarExtractCreator()
	case ExtTarTgz, ExtTgz:
		extractCreator = extract.NewTargzExtractCreator()
	case ExtZip:
		extractCreator = extract.NewZipExtractCreator()
	default:
		cmn.AssertMsg(false, fmt.Sprintf("unknown extension %s", m.rs.Extension))
	}

	if !m.rs.DryRun {
		m.extractCreator = extractCreator
	} else {
		m.extractCreator = extract.NopExtractCreator(extractCreator)
	}

	m.recManager = extract.NewRecordManager(m.ctx.t, m.ctx.node.DaemonID, m.rs.Bucket, m.rs.Provider,
		m.rs.Extension, m.extractCreator, keyExtractor, onDuplicatedRecords)

	return nil
}

// updateFinishedAck marks daemonID as finished. If all daemons ack then the
// finalCleanup is dispatched in separate goroutine.
func (m *Manager) updateFinishedAck(daemonID string) {
	m.finishedAck.mu.Lock()
	delete(m.finishedAck.m, daemonID)
	if len(m.finishedAck.m) == 0 {
		go m.finalCleanup()
	}
	m.finishedAck.mu.Unlock()
}

// incrementReceived increments number of received records batches. Also puts
// the information in the channel so other waiting goroutine can be informed
// that the information has been updated.
func (m *Manager) incrementReceived() {
	m.received.ch <- m.received.count.Inc()
}

// listenReceived returns channel on which waiting goroutine can hang and wait
// until received count value has been updated (see: incrementReceived).
func (m *Manager) listenReceived() chan int32 {
	return m.received.ch
}

func (m *Manager) addCompressionSizes(compressed, uncompressed int64) {
	m.compression.compressed.Add(compressed)
	m.compression.uncompressed.Add(uncompressed)
}

func (m *Manager) totalCompressedSize() int64 {
	return m.compression.compressed.Load()
}

func (m *Manager) totalUncompressedSize() int64 {
	return m.compression.uncompressed.Load()
}

func (m *Manager) avgCompressionRatio() float64 {
	return float64(m.totalCompressedSize()) / float64(m.totalUncompressedSize())
}

// incrementRef increments reference counter. This prevents from premature cleanup.
// Each increment should have coresponding decrement to prevent memory leaks.
//
// NOTE: Manager should increment ref everytime some data of it is used, otherwise
// unexpected things can happen.
func (m *Manager) incrementRef(by int64) {
	m.refCount.Add(by)
}

// decrementRef decrements reference counter. If it is 0 or below and dsort has
// already finished returns true. Otherwise, false is returned.
func (m *Manager) decrementRef(by int64) {
	newRefCount := m.refCount.Sub(by)
	if newRefCount <= 0 {
		// When ref count is below zero or zero we should schedule cleanup
		m.lock()
		if !m.inProgress() {
			m.unlock()
			go m.cleanup()
			return
		}
		m.unlock()
	}
}

func (m *Manager) inProgress() bool {
	return m.state.inProgress
}

func (m *Manager) aborted() bool {
	return m.state.aborted
}

// listenAborted returns channel which is closed when DSort job was aborted.
// This allows for the listen to be notified when job is aborted.
func (m *Manager) listenAborted() chan struct{} {
	return m.state.doneCh
}

// waitForFinish waits for DSort job to be finished. Note that aborted is also
// considered finished.
func (m *Manager) waitForFinish() {
	m.state.wg.Wait()
}

// setInProgressTo updates in progress state. If inProgress is set to false and
// sort was aborted this means someone is waiting. Therefore the function is
// waking up everyone who is waiting.
//
// NOTE: Should be used under lock.
func (m *Manager) setInProgressTo(inProgress bool) {
	// If marking as finished and job was aborted to need to free everyone
	// who is waiting.
	m.state.inProgress = inProgress
	if !inProgress && m.aborted() {
		m.state.wg.Done()
	}
}

// setAbortedTo updates aborted state. If aborted is set to true and sort is not
// yet finished. We need to inform current phase about abort (closing channel)
// and mark that we will wait until it is finished.
//
// NOTE: Should be used under lock.
func (m *Manager) setAbortedTo(aborted bool) {
	if aborted {
		// If not finished and not yet aborted we should mark that we will wait.
		if m.inProgress() && !m.aborted() {
			close(m.state.doneCh)
			m.state.wg.Add(1)
		}
	} else {
		// This is invoked when starting - on start doneCh should be open and
		// closed when aborted. wg is used to keep all waiting process on finish.
		m.state.doneCh = make(chan struct{})
		m.state.wg = &sync.WaitGroup{}
	}
	m.state.aborted = aborted
	m.Metrics.setAbortedTo(aborted)
}

func (m *Manager) lock() {
	m.mu.Lock()
}

func (m *Manager) unlock() {
	m.mu.Unlock()
}

func (m *Manager) sentCallback(hdr transport.Header, rc io.ReadCloser, x unsafe.Pointer, err error) {
	if m.Metrics.extended {
		dur := time.Since(*(*time.Time)(x))
		m.Metrics.Creation.Lock()
		m.Metrics.Creation.LocalSendStats.updateTime(dur)
		m.Metrics.Creation.LocalSendStats.updateThroughput(hdr.ObjAttrs.Size, dur)
		m.Metrics.Creation.Unlock()
	}

	if sgl, ok := rc.(*memsys.SGL); ok {
		sgl.Free()
	}
	m.decrementRef(1)
	if err != nil {
		m.abort(err)
	}
}

func (m *Manager) makeRecvShardFunc() transport.Receive {
	return func(w http.ResponseWriter, hdr transport.Header, object io.Reader, err error) {
		if err != nil {
			m.abort(err)
			return
		}
		if m.aborted() {
			return
		}
		cksum := cmn.NewCksum(hdr.ObjAttrs.CksumType, hdr.ObjAttrs.CksumValue)
		lom := &cluster.LOM{T: m.ctx.t, Objname: hdr.ObjName}
		if err = lom.Init(hdr.Bck); err == nil {
			err = lom.Load()
		}
		if err != nil && !os.IsNotExist(err) {
			m.abort(err)
			return
		}
		if err == nil {
			if lom.Cksum() != nil && cmn.EqCksum(lom.Cksum(), cksum) {
				glog.Infof("shard (%s) already exists and checksums are equal, skipping", lom)
				io.Copy(ioutil.Discard, object) // drain the reader
				return
			}
			glog.Warningf("shard (%s) already exists, overriding", lom)
		}
		workFQN := fs.CSM.GenContentParsedFQN(lom.ParsedFQN, filetype.DSortWorkfileType, filetype.WorkfileRecvShard)
		started := time.Now()
		lom.SetAtimeUnix(started.UnixNano())
		rc := ioutil.NopCloser(object)
		if err := m.ctx.t.PutObject(workFQN, rc, lom, cluster.WarmGet, nil, started); err != nil {
			m.abort(err)
			return
		}
	}
}

// doWithAbort sends requests through client. If manager aborts during the call
// request is canceled.
func (m *Manager) doWithAbort(reqArgs *cmn.ReqArgs) error {
	req, _, cancel, err := reqArgs.ReqWithCancel()
	if err != nil {
		return err
	}

	// Start request
	doneCh := make(chan struct{}, 1)
	errCh := make(chan error, 1)
	go func() {
		defer func() {
			doneCh <- struct{}{}
		}()
		resp, err := m.client.Do(req)
		if err != nil {
			errCh <- err
			return
		}

		if resp.StatusCode >= http.StatusBadRequest {
			b, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				errCh <- err
			} else {
				errCh <- errors.New(string(b))
			}
			return
		}
	}()

	// Wait for abort or request to finish
	select {
	case <-m.listenAborted():
		cancel()
		<-doneCh
		return newAbortError(m.ManagerUUID)
	case <-doneCh:
		break
	}

	close(errCh)
	return errors.WithStack(<-errCh)
}

func (m *Manager) ListenSmapChanged(ch chan int64) {
	for {
		newSmapVersion, ok := <-ch

		if !ok {
			// channel was closed by unregister
			return
		}

		if newSmapVersion <= m.smap.Version {
			// We initialized with the same/older smap, safe to skip
			continue
		}

		newSmap := m.ctx.smap.Get()
		if newSmap.CountTargets() != m.smap.CountTargets() {
			// Currently adding new target as well as removing one is not
			// supported during the run.
			//
			// TODO: dSort should survive adding new target. For now it is
			// not possible as rebalance deletes moved object - dSort needs
			// to use `GetObject` method instead of relaying on simple `os.Open`
			err := errors.Errorf("number of target has changed during dSort run, aborting due to possible errors")
			m.abort(err)
		}
	}
}

func (m *Manager) String() string {
	return m.ManagerUUID
}

func (m *Manager) freeMemory() uint64 {
	curMem, err := sys.Mem()
	if err != nil {
		return 0
	}

	maxMemoryToUse := calcMaxMemoryUsage(m.rs.MaxMemUsage, curMem)
	return maxMemoryToUse - curMem.ActualUsed
}

func (m *Manager) react(reaction, msg string) error {
	switch reaction {
	case cmn.IgnoreReaction:
		return nil
	case cmn.WarnReaction:
		m.Metrics.lock()
		m.Metrics.Warnings = append(m.Metrics.Warnings, msg)
		m.Metrics.unlock()
		return nil
	case cmn.AbortReaction:
		return fmt.Errorf("%s", msg) // error will be reported on abort
	default:
		cmn.AssertMsg(false, reaction)
		return nil
	}
}

func calcMaxMemoryUsage(maxUsage cmn.ParsedQuantity, mem sys.MemStat) uint64 {
	switch maxUsage.Type {
	case cmn.QuantityPercent:
		return maxUsage.Value * (mem.Total / 100)
	case cmn.QuantityBytes:
		return cmn.MinU64(maxUsage.Value, mem.Total)
	default:
		cmn.AssertMsg(false, fmt.Sprintf("mem usage type (%s) is not recognized.. something went wrong", maxUsage.Type))
		return 0
	}
}
