// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cluster/meta"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/archive"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/ext/dsort/extract"
	"github.com/NVIDIA/aistore/ext/dsort/filetype"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/pkg/errors"
)

const (
	// Stream names
	recvReqStreamNameFmt  = DSortName + "-%s-recv_req"
	recvRespStreamNameFmt = DSortName + "-%s-recv_resp"
	shardStreamNameFmt    = DSortName + "-%s-shard"
)

// State of the cleans - see `cleanup` and `finalCleanup`
const (
	noCleanedState = iota
	initiallyCleanedState
	finallyCleanedState
)

const (
	// Size of the buffer used for serialization of the shards/records.
	serializationBufSize = 10 * cos.MiB
)

var (
	ctx dsortContext
	mm  *memsys.MMSA
)

// interface guard
var (
	_ meta.Slistener = (*Manager)(nil)
	_ cos.Packer     = (*buildingShardInfo)(nil)
	_ cos.Unpacker   = (*buildingShardInfo)(nil)
)

type (
	dsortContext struct {
		smapOwner meta.Sowner
		bmdOwner  meta.Bowner
		node      *meta.Snode
		t         cluster.Target // Set only on target.
		stats     stats.Tracker
		client    *http.Client // Client for broadcast.
	}

	buildingShardInfo struct {
		shardName string
	}

	// progressState abstracts all information meta information about progress of
	// the job.
	progressState struct {
		cleanWait *sync.Cond // waiting room for `cleanup` and `finalCleanup` method so then can run in correct order
		wg        *sync.WaitGroup
		// doneCh is closed when the job is aborted so that goroutines know when
		// they need to stop.
		doneCh     chan struct{}
		inProgress atomic.Bool
		aborted    atomic.Bool
		cleaned    uint8 // current state of the cleanliness - no cleanup, initial cleanup, final cleanup
	}

	// Manager maintains all the state required for a single run of a distributed archive file shuffle.
	Manager struct {
		// Fields with json tags are the only fields which are persisted
		// into the disk once the dSort is finished.
		ManagerUUID string   `json:"manager_uuid"`
		Metrics     *Metrics `json:"metrics"`

		mg *ManagerGroup // parent

		mu   sync.Mutex
		ctx  dsortContext
		smap *meta.Smap

		recManager     *extract.RecordManager
		extractCreator extract.Creator

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
		refCount        atomic.Int64 // Reference counter used to determine if we can do cleanup.
		inFlight        atomic.Int64 // Reference counter that counts number of in-flight stream requests.
		state           progressState
		extractionPhase struct {
			adjuster *concAdjuster
		}
		streams struct {
			shards *bundle.Streams // streams for pushing streams to other targets if the fqn is non-local
		}
		creationPhase struct {
			metadata CreationPhaseMetadata
		}
		finishedAck struct {
			mu sync.Mutex
			m  map[string]struct{} // finished acks: daemonID -> ack
		}

		dsorter        dsorter
		dsorterStarted sync.WaitGroup

		callTimeout time.Duration // Maximal time we will wait for other node to respond

		config *cmn.Config
	}
)

func RegisterNode(smapOwner meta.Sowner, bmdOwner meta.Bowner, snode *meta.Snode, t cluster.Target, stats stats.Tracker) {
	ctx.smapOwner = smapOwner
	ctx.bmdOwner = bmdOwner
	ctx.node = snode
	ctx.t = t
	ctx.stats = stats
	debug.Assert(mm == nil)

	config := cmn.GCO.Get()
	ctx.client = cmn.NewClient(cmn.TransportArgs{
		Timeout:    config.Timeout.MaxHostBusy.D(),
		UseHTTPS:   config.Net.HTTP.UseHTTPS,
		SkipVerify: config.Net.HTTP.SkipVerify,
	})

	if ctx.node.IsTarget() {
		mm = t.PageMM()
		err := fs.CSM.Reg(filetype.DSortFileType, &filetype.DSortFile{})
		debug.AssertNoErr(err)
		err = fs.CSM.Reg(filetype.DSortWorkfileType, &filetype.DSortFile{})
		debug.AssertNoErr(err)
	}
}

// init initializes all necessary fields.
//
// PRECONDITION: `m.mu` must be locked.
func (m *Manager) init(rs *ParsedRequestSpec) error {
	debug.AssertMutexLocked(&m.mu)

	m.ctx = ctx
	m.smap = m.ctx.smapOwner.Get()

	targetCount := m.smap.CountActiveTs()

	m.rs = rs
	m.Metrics = newMetrics(rs.Description, rs.ExtendedMetrics)
	m.startShardCreation = make(chan struct{}, 1)

	m.ctx.smapOwner.Listeners().Reg(m)

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
	m.config = cmn.GCO.Get()
	m.client = cmn.NewClient(cmn.TransportArgs{
		DialTimeout: 5 * time.Minute,
		Timeout:     30 * time.Minute,
		UseHTTPS:    m.config.Net.HTTP.UseHTTPS,
		SkipVerify:  m.config.Net.HTTP.SkipVerify,
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
	m.extractionPhase.adjuster = newConcAdjuster(
		rs.ExtractConcMaxLimit,
		2*targetCount, /*goroutineLimitCoef*/
	)

	// Fill ack map with current daemons. Once the finished ack is received from
	// another daemon we will remove it from the map until len(ack) == 0 (then
	// we will know that all daemons have finished operation).
	m.finishedAck.m = make(map[string]struct{}, targetCount)
	for sid, si := range m.smap.Tmap {
		if m.smap.InMaintOrDecomm(si) {
			continue
		}
		m.finishedAck.m[sid] = struct{}{}
	}

	m.setInProgressTo(true)
	m.setAbortedTo(false)
	m.state.cleanWait = sync.NewCond(&m.mu)

	m.callTimeout = m.config.DSort.CallTimeout.D()
	return nil
}

// TODO: Currently we create streams for each dSort job but maybe we should
//
//	create streams once and have them available for all the dSort jobs so they
//	would share the resource rather than competing for it.
func (m *Manager) initStreams() error {
	config := cmn.GCO.Get()

	// Responses to the other targets are objects that is why we want to use
	// intraData network.
	respNetwork := cmn.NetIntraData
	trname := fmt.Sprintf(shardStreamNameFmt, m.ManagerUUID)
	shardsSbArgs := bundle.Args{
		Multiplier: config.DSort.SbundleMult,
		Net:        respNetwork,
		Trname:     trname,
		Ntype:      cluster.Targets,
		Extra: &transport.Extra{
			Compression: config.DSort.Compression,
			Config:      config,
			MMSA:        mm,
		},
	}
	if err := transport.HandleObjStream(trname, m.makeRecvShardFunc()); err != nil {
		return errors.WithStack(err)
	}
	client := transport.NewIntraDataClient()
	m.streams.shards = bundle.New(m.ctx.smapOwner, m.ctx.node, client, shardsSbArgs)
	return nil
}

func (m *Manager) cleanupStreams() (err error) {
	if m.streams.shards != nil {
		trname := fmt.Sprintf(shardStreamNameFmt, m.ManagerUUID)
		if unhandleErr := transport.Unhandle(trname); unhandleErr != nil {
			err = errors.WithStack(unhandleErr)
		}
	}

	for _, streamBundle := range []*bundle.Streams{m.streams.shards} {
		if streamBundle != nil {
			// NOTE: We don't want stream to send a message at this point as the
			//  receiver might have closed its corresponding stream.
			streamBundle.Close(false /*gracefully*/)
		}
	}

	return err
}

// cleanup removes all memory allocated and removes all files created during sort run.
//
// PRECONDITION: manager must be not in progress state (either actual finish or abort).
//
// NOTE: If cleanup is invoked during the run it is treated as abort.
func (m *Manager) cleanup() {
	glog.Infof("[dsort] %s started cleanup", m.ManagerUUID)
	m.lock()
	if m.state.cleaned != noCleanedState {
		m.unlock()
		return // Do not clean if already scheduled.
	}

	m.dsorter.cleanup()
	now := time.Now()

	defer func() {
		m.state.cleaned = initiallyCleanedState
		m.state.cleanWait.Signal()
		m.unlock()
		glog.Infof("[dsort] %s finished cleanup in %v", m.ManagerUUID, time.Since(now))
	}()

	debug.Assertf(!m.inProgress(), "%s: was still in progress", m.ManagerUUID)

	m.extractCreator = nil
	m.client = nil

	m.ctx.smapOwner.Listeners().Unreg(m)

	if !m.aborted() {
		m.updateFinishedAck(m.ctx.node.ID())
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
			return // Do not clean if already cleaned.
		}
		if m.state.cleaned == noCleanedState {
			// Wait for wake up from `cleanup` or other `finalCleanup` method.
			m.state.cleanWait.Wait()
		}
	}

	glog.Infof("[dsort] %s started final cleanup", m.ManagerUUID)
	now := time.Now()

	if err := m.cleanupStreams(); err != nil {
		glog.Error(err)
	}

	// Wait for all in-flight stream requests after cleaning up streams.
	m.waitForInFlight()

	if err := m.dsorter.finalCleanup(); err != nil {
		glog.Error(err)
	}

	// The reason why this is not in regular cleanup is because we are only sure
	// that this can be freed once we cleanup streams - streams are asynchronous
	// and we may have race between in-flight request and cleanup.
	m.recManager.Cleanup()

	m.creationPhase.metadata.SendOrder = nil
	m.creationPhase.metadata.Shards = nil

	m.finishedAck.m = nil

	// Update clean state.
	m.state.cleaned = finallyCleanedState
	// If there is another `finalCleanup` waiting it should be woken up to check the state and exit.
	m.state.cleanWait.Signal()
	m.unlock()

	m.mg.persist(m.ManagerUUID)
	glog.Infof("[dsort] %s finished final cleanup in %v", m.ManagerUUID, time.Since(now))
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

	glog.Infof("[dsort] %s has been aborted", m.ManagerUUID)
	m.setAbortedTo(true)
	inProgress := m.inProgress()
	m.unlock()

	// If job has already finished we just free resources, otherwise we must wait
	// for it to finish.
	if inProgress {
		if m.config.FastV(4, cos.SmoduleDsort) {
			glog.Infof("[dsort] %s is in progress, waiting for finish", m.ManagerUUID)
		}
		// Wait for dsorter to initialize all the resources.
		m.waitDSorterToStart()

		m.dsorter.onAbort()
		m.waitForFinish()
		if m.config.FastV(4, cos.SmoduleDsort) {
			glog.Infof("[dsort] %s was in progress and finished", m.ManagerUUID)
		}
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
		debug.Assertf(false, "dsorter type is invalid: %q", m.rs.DSorterType)
	}
	m.dsorterStarted.Add(1)
	return
}

func (m *Manager) markDSorterStarted() { m.dsorterStarted.Done() }
func (m *Manager) waitDSorterToStart() { m.dsorterStarted.Wait() }

// setExtractCreator sets what type of file extraction and creation is used based on the RequestSpec.
func (m *Manager) setExtractCreator() (err error) {
	var keyExtractor extract.KeyExtractor

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
		return m.react(m.rs.DuplicatedRecords, msg)
	}

	var extractCreator extract.Creator
	switch m.rs.Extension {
	case archive.ExtTar:
		extractCreator = extract.NewTarExtractCreator(m.ctx.t)
	case archive.ExtTarTgz, archive.ExtTgz:
		extractCreator = extract.NewTargzExtractCreator(m.ctx.t)
	case archive.ExtZip:
		extractCreator = extract.NewZipExtractCreator(m.ctx.t)
	default:
		debug.Assertf(false, "unknown extension %s", m.rs.Extension)
	}

	if !m.rs.DryRun {
		m.extractCreator = extractCreator
	} else {
		m.extractCreator = extract.NopExtractCreator(extractCreator)
	}

	m.recManager = extract.NewRecordManager(
		m.ctx.t, m.rs.Bck,
		m.rs.Extension, m.extractCreator,
		keyExtractor, onDuplicatedRecords,
	)

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

// listenReceived returns channel on which goroutine can wait
// until received count value is updated (see: incrementReceived).
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
// Each increment should have corresponding decrement to prevent memory leaks.
//
// NOTE: Manager should increment ref every time some data of it is used, otherwise
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

func (m *Manager) inFlightInc() { m.inFlight.Inc() }
func (m *Manager) inFlightDec() { m.inFlight.Dec() }

// waitForInFlight waits for all in-flight stream requests to finish.
func (m *Manager) waitForInFlight() {
	for m.inFlight.Load() > 0 {
		time.Sleep(200 * time.Millisecond)
	}
}

func (m *Manager) inProgress() bool {
	return m.state.inProgress.Load()
}

func (m *Manager) aborted() bool {
	return m.state.aborted.Load()
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
// PRECONDITION: `m.mu` must be locked.
func (m *Manager) setInProgressTo(inProgress bool) {
	// If marking as finished and job was aborted to need to free everyone
	// who is waiting.
	m.state.inProgress.Store(inProgress)
	if !inProgress && m.aborted() {
		m.state.wg.Done()
	}
}

// setAbortedTo updates aborted state. If aborted is set to true and sort is not
// yet finished. We need to inform current phase about abort (closing channel)
// and mark that we will wait until it is finished.
//
// PRECONDITION: `m.mu` must be locked.
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
	m.state.aborted.Store(aborted)
	m.Metrics.setAbortedTo(aborted)
}

func (m *Manager) lock() {
	m.mu.Lock()
}

func (m *Manager) unlock() {
	m.mu.Unlock()
}

func (m *Manager) sentCallback(hdr transport.ObjHdr, rc io.ReadCloser, x any, err error) {
	if m.Metrics.extended {
		dur := mono.Since(x.(int64))
		m.Metrics.Creation.mu.Lock()
		m.Metrics.Creation.LocalSendStats.updateTime(dur)
		m.Metrics.Creation.LocalSendStats.updateThroughput(hdr.ObjAttrs.Size, dur)
		m.Metrics.Creation.mu.Unlock()
	}

	if sgl, ok := rc.(*memsys.SGL); ok {
		sgl.Free()
	}
	m.decrementRef(1)
	if err != nil {
		m.abort(err)
	}
}

func (m *Manager) makeRecvShardFunc() transport.RecvObj {
	return func(hdr transport.ObjHdr, object io.Reader, err error) error {
		defer transport.DrainAndFreeReader(object)
		if err != nil {
			m.abort(err)
			return err
		}
		if m.aborted() {
			return newDSortAbortedError(m.ManagerUUID)
		}
		lom := cluster.AllocLOM(hdr.ObjName)
		defer cluster.FreeLOM(lom)
		if err = lom.InitBck(&hdr.Bck); err == nil {
			err = lom.Load(false /*cache it*/, false /*locked*/)
		}
		if err != nil && !os.IsNotExist(err) {
			m.abort(err)
			return err
		}
		if err == nil {
			if lom.EqCksum(hdr.ObjAttrs.Cksum) {
				if m.config.FastV(4, cos.SmoduleDsort) {
					glog.Infof("[dsort] %s shard (%s) already exists and checksums are equal, skipping",
						m.ManagerUUID, lom)
				}
				return nil
			}
			glog.Warningf("[dsort] %s shard (%s) already exists, overriding", m.ManagerUUID, lom)
		}
		started := time.Now()
		lom.SetAtimeUnix(started.UnixNano())
		rc := io.NopCloser(object)

		params := cluster.AllocPutObjParams()
		{
			params.WorkTag = filetype.WorkfileRecvShard
			params.Reader = rc
			params.Cksum = nil
			params.Atime = started
		}
		erp := m.ctx.t.PutObject(lom, params)
		cluster.FreePutObjParams(params)
		if erp != nil {
			m.abort(err)
			return erp
		}
		return nil
	}
}

// doWithAbort sends requests through client. If manager aborts during the call
// request is canceled.
func (m *Manager) doWithAbort(reqArgs *cmn.HreqArgs) error {
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
		resp, err := m.client.Do(req) //nolint:bodyclose // closed inside cos.Close
		if err != nil {
			errCh <- err
			return
		}
		defer cos.Close(resp.Body)

		if resp.StatusCode >= http.StatusBadRequest {
			b, err := io.ReadAll(resp.Body)
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
		return newDSortAbortedError(m.ManagerUUID)
	case <-doneCh:
		break
	}

	close(errCh)
	return errors.WithStack(<-errCh)
}

func (m *Manager) ListenSmapChanged() {
	newSmap := m.ctx.smapOwner.Get()
	if newSmap.Version <= m.smap.Version {
		return
	}

	if newSmap.CountActiveTs() != m.smap.CountActiveTs() {
		// Currently adding new target as well as removing one is not
		// supported during the run.
		//
		// TODO: dSort should survive adding new target. For now it is
		//  not possible as rebalance deletes moved object - dSort needs
		//  to use `GetObject` method instead of relaying on simple `os.Open`.
		err := errors.Errorf("number of target has changed during dSort run, aborting due to possible errors")
		go m.abort(err)
	}
}

func (m *Manager) String() string {
	return m.ManagerUUID
}

func (m *Manager) freeMemory() uint64 {
	var mem sys.MemStat
	if err := mem.Get(); err != nil {
		return 0
	}
	maxMemoryToUse := calcMaxMemoryUsage(m.rs.MaxMemUsage, &mem)
	return maxMemoryToUse - mem.ActualUsed
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
		debug.Assert(false, reaction)
		return nil
	}
}

func (bsi *buildingShardInfo) Unpack(unpacker *cos.ByteUnpack) error {
	var err error
	bsi.shardName, err = unpacker.ReadString()
	return err
}
func (bsi *buildingShardInfo) Pack(packer *cos.BytePack) { packer.WriteString(bsi.shardName) }
func (bsi *buildingShardInfo) PackedSize() int           { return cos.SizeofLen + len(bsi.shardName) }
func (bsi *buildingShardInfo) NewPack(mm *memsys.MMSA) []byte {
	var (
		size   = bsi.PackedSize()
		buf, _ = mm.AllocSize(int64(size))
		packer = cos.NewPacker(buf, size)
	)
	packer.WriteAny(bsi)
	return packer.Bytes()
}

func calcMaxMemoryUsage(maxUsage cos.ParsedQuantity, mem *sys.MemStat) uint64 {
	switch maxUsage.Type {
	case cos.QuantityPercent:
		return maxUsage.Value * (mem.Total / 100)
	case cos.QuantityBytes:
		return cos.MinU64(maxUsage.Value, mem.Total)
	default:
		debug.Assertf(false, "mem usage type (%s) is not recognized.. something went wrong", maxUsage.Type)
		return 0
	}
}
