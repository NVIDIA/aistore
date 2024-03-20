// Package dsort provides distributed massively parallel resharding for very large datasets.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/kvdb"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/ext/dsort/ct"
	"github.com/NVIDIA/aistore/ext/dsort/shard"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/transport"
	"github.com/NVIDIA/aistore/transport/bundle"
	"github.com/NVIDIA/aistore/xact/xreg"
	"github.com/pkg/errors"
)

const (
	// Stream names
	recvReqStreamNameFmt  = "recv-%sq"
	recvRespStreamNameFmt = "recv-%sp"
	shardStreamNameFmt    = "shrd-%ss"
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

type (
	global struct {
		tstats stats.Tracker
		mm     *memsys.MMSA
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
		// tagged fields are the only fields persisted once dsort finishes
		ManagerUUID string         `json:"manager_uuid"`
		Metrics     *Metrics       `json:"metrics"`
		Pars        *parsedReqSpec `json:"pars"`

		mg                 *ManagerGroup // parent
		mu                 sync.Mutex
		smap               *meta.Smap
		recm               *shard.RecordManager
		shardRW            shard.RW
		startShardCreation chan struct{}
		client             *http.Client // Client for sending records metadata
		compression        struct {
			totalShardSize     atomic.Int64
			totalExtractedSize atomic.Int64
		}
		received struct {
			count atomic.Int32 // Number of FileMeta slices received, defining what step in the sort target is in.
			ch    chan int32
		}
		refCount        atomic.Int64 // Refcount to cleanup.
		inFlight        atomic.Int64 // Refcount in-flight stream requests
		state           progressState
		extractionPhase struct {
			adjuster *concAdjuster
		}
		streams struct {
			shards *bundle.Streams
		}
		creationPhase struct {
			metadata CreationPhaseMetadata
		}
		finishedAck struct {
			mu sync.Mutex
			m  map[string]struct{} // finished acks: tid -> ack
		}
		dsorter        dsorter
		dsorterStarted sync.WaitGroup
		callTimeout    time.Duration // max time to wait for another node to respond
		config         *cmn.Config
		xctn           *xaction
	}
)

var g global

// interface guard
var (
	_ cos.Packer   = (*buildingShardInfo)(nil)
	_ cos.Unpacker = (*buildingShardInfo)(nil)
	_ core.Xact    = (*xaction)(nil)
)

func Pinit(si core.Node, config *cmn.Config) {
	psi = si
	newBcastClient(config)
}

func Tinit(tstats stats.Tracker, db kvdb.Driver, config *cmn.Config) {
	Managers = NewManagerGroup(db, false)

	xreg.RegBckXact(&factory{})

	debug.Assert(g.mm == nil) // only once
	{
		g.tstats = tstats
		g.mm = core.T.PageMM()
	}
	fs.CSM.Reg(ct.DsortFileType, &ct.DsortFile{})
	fs.CSM.Reg(ct.DsortWorkfileType, &ct.DsortFile{})

	newBcastClient(config)
}

func newBcastClient(config *cmn.Config) {
	cargs := cmn.TransportArgs{Timeout: config.Timeout.MaxHostBusy.D()}
	if config.Net.HTTP.UseHTTPS {
		bcastClient = cmn.NewIntraClientTLS(cargs, config)
	} else {
		bcastClient = cmn.NewClient(cargs)
	}
}

/////////////
// Manager //
/////////////

func (m *Manager) String() string { return m.ManagerUUID }
func (m *Manager) lock()          { m.mu.Lock() }
func (m *Manager) unlock()        { m.mu.Unlock() }

// init initializes all necessary fields.
// PRECONDITION: `m.mu` must be locked.
func (m *Manager) init(pars *parsedReqSpec) error {
	m.smap = core.T.Sowner().Get()

	targetCount := m.smap.CountActiveTs()

	m.Pars = pars
	m.Metrics = newMetrics(pars.Description)
	m.startShardCreation = make(chan struct{}, 1)

	if err := m.setDsorter(); err != nil {
		return err
	}

	if err := m.dsorter.init(); err != nil {
		return err
	}

	if err := m.setRW(); err != nil {
		return err
	}

	// NOTE: Total size of the records metadata can sometimes be large
	// and so this is why we need such a long timeout.
	m.config = cmn.GCO.Get()

	cargs := cmn.TransportArgs{
		DialTimeout: m.config.Client.Timeout.D(),
		Timeout:     m.config.Client.TimeoutLong.D(),
	}
	if m.config.Net.HTTP.UseHTTPS {
		m.client = cmn.NewIntraClientTLS(cargs, m.config)
	} else {
		m.client = cmn.NewClient(cargs)
	}

	m.received.ch = make(chan int32, 10)

	m.compression.totalShardSize.Store(1)
	m.compression.totalExtractedSize.Store(1)

	// Concurrency:
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
		pars.ExtractConcMaxLimit,
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

	m.callTimeout = m.config.Dsort.CallTimeout.D()
	return nil
}

// TODO -- FIXME: create on demand and reuse streams across jobs
// (in re: global rebalance and EC)
func (m *Manager) initStreams() error {
	config := cmn.GCO.Get()

	// Responses to the other targets are objects that is why we want to use
	// intraData network.
	respNetwork := cmn.NetIntraData
	trname := fmt.Sprintf(shardStreamNameFmt, m.ManagerUUID)
	shardsSbArgs := bundle.Args{
		Multiplier: config.Dsort.SbundleMult,
		Net:        respNetwork,
		Trname:     trname,
		Ntype:      core.Targets,
		Extra: &transport.Extra{
			Compression: config.Dsort.Compression,
			Config:      config,
			WorkChBurst: 1024,
		},
	}
	if err := transport.Handle(trname, m.recvShard); err != nil {
		return errors.WithStack(err)
	}
	client := transport.NewIntraDataClient()
	m.streams.shards = bundle.New(client, shardsSbArgs)
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
			// receiver might have closed its corresponding stream.
			streamBundle.Close(false /*gracefully*/)
		}
	}

	return err
}

// cleanup removes all memory allocated and removes all files created during sort run.
// PRECONDITION: manager must be not in progress state (either actual finish or abort).
// NOTE: If cleanup is invoked during the run it is treated as abort.
func (m *Manager) cleanup() {
	nlog.Infof("[dsort] %s started cleanup", m.ManagerUUID)
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
		nlog.Infof("[dsort] %s finished cleanup in %v", m.ManagerUUID, time.Since(now))
	}()

	debug.Assertf(!m.inProgress(), "%s: was still in progress", m.ManagerUUID)

	m.shardRW = nil
	m.client = nil

	if !m.aborted() {
		m.updateFinishedAck(core.T.SID())
		m.xctn.Finish()
	}
}

// finalCleanup is invoked only when all targets confirm finishing.
// To ensure that finalCleanup is not invoked before regular
// cleanup is finished, we also ack ourselves.
//
// finalCleanup can be invoked only after cleanup and this is ensured by
// maintaining current state of the cleanliness and having conditional variable
// on which finalCleanup will sleep if needed. Note that it is hard (or even
// impossible) to ensure that cleanup and finalCleanup will be invoked in order
// without having ordering mechanism since cleanup and finalCleanup are invoked
// in goroutines (there is a possibility that finalCleanup would start before
// cleanup) - this cannot happen with current ordering mechanism.
func (m *Manager) finalCleanup() {
	nlog.Infof("%s: [dsort] %s started final cleanup", core.T, m.ManagerUUID)

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

	now := time.Now()

	if err := m.cleanupStreams(); err != nil {
		nlog.Errorln(err)
	}

	// Wait for all in-flight stream requests after cleaning up streams.
	m.waitForInFlight()

	if err := m.dsorter.finalCleanup(); err != nil {
		nlog.Errorln(err)
	}

	// The reason why this is not in regular cleanup is because we are only sure
	// that this can be freed once we cleanup streams - streams are asynchronous
	// and we may have race between in-flight request and cleanup.
	// Also, NOTE:
	// recm.Cleanup => gmm.freeMemToOS => cos.FreeMemToOS to forcefully free memory to the OS
	m.recm.Cleanup()

	m.creationPhase.metadata.SendOrder = nil
	m.creationPhase.metadata.Shards = nil

	m.finishedAck.m = nil

	// Update clean state.
	m.state.cleaned = finallyCleanedState
	// If there is another `finalCleanup` waiting it should be woken up to check the state and exit.
	m.state.cleanWait.Signal()
	m.unlock()

	m.mg.persist(m.ManagerUUID)
	nlog.Infof("%s: [dsort] %s finished final cleanup in %v", core.T, m.ManagerUUID, time.Since(now))
}

// stop this job and free associated resources
func (m *Manager) abort(err error) {
	if m.aborted() { // do not abort if already aborted
		return
	}
	// serialize
	m.lock()
	if m.aborted() { // do not abort if already aborted
		m.unlock()
		return
	}
	if err != nil {
		m.Metrics.lock()
		m.Metrics.Errors = append(m.Metrics.Errors, err.Error())
		m.Metrics.unlock()
	}
	m.setAbortedTo(true)
	m.xctn.Base.Abort(err) // notice Base, compare w/ xaction.Abort (xact.go)
	inProgress := m.inProgress()
	m.unlock()

	nlog.Infof("%s: [dsort] %s aborted", core.T, m.ManagerUUID)

	// If job has already finished we just free resources, otherwise we must wait
	// for it to finish.
	if inProgress {
		if cmn.Rom.FastV(4, cos.SmoduleDsort) {
			nlog.Infof("[dsort] %s is in progress, waiting for finish", m.ManagerUUID)
		}
		// Wait for dsorter to initialize all the resources.
		m.waitToStart()

		m.dsorter.onAbort()
		m.waitForFinish()
		if cmn.Rom.FastV(4, cos.SmoduleDsort) {
			nlog.Infof("[dsort] %s was in progress and finished", m.ManagerUUID)
		}
	}

	go func() {
		m.cleanup()
		m.finalCleanup() // on abort always perform final cleanup
	}()
}

// setDsorter sets what type of dsorter implementation should be used
func (m *Manager) setDsorter() (err error) {
	switch m.Pars.DsorterType {
	case GeneralType:
		m.dsorter, err = newDsorterGeneral(m)
	case MemType:
		m.dsorter = newDsorterMem(m)
	default:
		debug.Assertf(false, "dsorter type is invalid: %q", m.Pars.DsorterType)
	}
	m.dsorterStarted.Add(1)
	return
}

func (m *Manager) markStarted()               { m.dsorterStarted.Done() }
func (m *Manager) waitToStart()               { m.dsorterStarted.Wait() }
func (m *Manager) onDupRecs(msg string) error { return m.react(m.Pars.DuplicatedRecords, msg) }

// setRW sets what type of file extraction and creation is used based on the RequestSpec.
func (m *Manager) setRW() (err error) {
	var ke shard.KeyExtractor
	switch m.Pars.Algorithm.Kind {
	case Content:
		ke, err = shard.NewContentKeyExtractor(m.Pars.Algorithm.ContentKeyType, m.Pars.Algorithm.Ext)
	case MD5:
		ke, err = shard.NewMD5KeyExtractor()
	default:
		ke, err = shard.NewNameKeyExtractor()
	}
	if err != nil {
		return errors.WithStack(err)
	}

	m.shardRW = shard.RWs[m.Pars.InputExtension]
	if m.shardRW == nil {
		debug.Assert(!m.Pars.DryRun, "dry-run in combination with _any_ shard extension is not supported")
		debug.Assert(m.Pars.InputExtension == "", m.Pars.InputExtension)
		// TODO -- FIXME: niy
	}
	if m.Pars.DryRun {
		m.shardRW = shard.NopRW(m.shardRW)
	}

	m.recm = shard.NewRecordManager(m.Pars.InputBck, m.shardRW, ke, m.onDupRecs)
	return nil
}

// updateFinishedAck marks tid as finished. If all daemons ack then the
// finalCleanup is dispatched in separate goroutine.
func (m *Manager) updateFinishedAck(tid string) {
	m.finishedAck.mu.Lock()
	delete(m.finishedAck.m, tid)
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

func (m *Manager) addSizes(shardSize, extractedSize int64) {
	if shardSize > extractedSize {
		// .tar with padding or poor compression
		shardSize = extractedSize
	}
	m.compression.totalShardSize.Add(shardSize)
	m.compression.totalExtractedSize.Add(extractedSize)
}

func (m *Manager) totalShardSize() int64     { return m.compression.totalShardSize.Load() }
func (m *Manager) totalExtractedSize() int64 { return m.compression.totalExtractedSize.Load() }

func (m *Manager) compressionRatio() float64 {
	return float64(m.totalShardSize()) / float64(m.totalExtractedSize())
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

func (m *Manager) inFlightInc()     { m.inFlight.Inc() }
func (m *Manager) inFlightDec()     { m.inFlight.Dec() }
func (m *Manager) inProgress() bool { return m.state.inProgress.Load() }
func (m *Manager) aborted() bool    { return m.state.aborted.Load() }

// listenAborted returns channel which is closed when Dsort job was aborted.
// This allows for the listen to be notified when job is aborted.
func (m *Manager) listenAborted() chan struct{} {
	return m.state.doneCh
}

// waitForFinish waits for Dsort job to be finished. Note that aborted is also
// 'finished'.
func (m *Manager) waitForFinish() {
	m.state.wg.Wait()
}

// waitForInFlight waits for all in-flight stream requests to finish.
func (m *Manager) waitForInFlight() {
	for m.inFlight.Load() > 0 {
		time.Sleep(200 * time.Millisecond)
	}
}

// setInProgressTo updates in progress state. If inProgress is set to false and
// sort was aborted this means someone is waiting. Therefore the function is
// waking up everyone who is waiting.
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

func (m *Manager) recvShard(hdr *transport.ObjHdr, objReader io.Reader, err error) error {
	defer transport.DrainAndFreeReader(objReader)
	if err != nil {
		m.abort(err)
		return err
	}
	if m.aborted() {
		return m.newErrAborted()
	}
	lom := core.AllocLOM(hdr.ObjName)
	defer core.FreeLOM(lom)
	if err = lom.InitBck(&hdr.Bck); err == nil {
		err = lom.Load(false /*cache it*/, false /*locked*/)
	}
	if err != nil && !os.IsNotExist(err) {
		m.abort(err)
		return err
	}
	if err == nil {
		if lom.EqCksum(hdr.ObjAttrs.Cksum) {
			if cmn.Rom.FastV(4, cos.SmoduleDsort) {
				nlog.Infof("[dsort] %s shard (%s) already exists and checksums are equal, skipping",
					m.ManagerUUID, lom)
			}
			return nil
		}
		nlog.Warningf("[dsort] %s shard (%s) already exists, overriding", m.ManagerUUID, lom)
	}
	started := time.Now()
	lom.SetAtimeUnix(started.UnixNano())
	rc := io.NopCloser(objReader)

	params := core.AllocPutParams()
	{
		params.WorkTag = ct.WorkfileRecvShard
		params.Reader = rc
		params.Cksum = nil
		params.Atime = started
		params.Size = hdr.ObjAttrs.Size
	}
	erp := core.T.PutObject(lom, params)
	core.FreePutParams(params)
	if erp != nil {
		m.abort(err)
		return erp
	}
	return nil
}

func (m *Manager) freeMemory() uint64 {
	var mem sys.MemStat
	if err := mem.Get(); err != nil {
		return 0
	}
	maxMemoryToUse := calcMaxMemoryUsage(m.Pars.MaxMemUsage, &mem)
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
		return fmt.Errorf("%s", msg) // (dsort job aborts and returns this error)
	default:
		debug.Assert(false, reaction)
		return nil
	}
}

func calcMaxMemoryUsage(maxUsage cos.ParsedQuantity, mem *sys.MemStat) uint64 {
	switch maxUsage.Type {
	case cos.QuantityPercent:
		return maxUsage.Value * (mem.Total / 100)
	case cos.QuantityBytes:
		return min(maxUsage.Value, mem.Total)
	default:
		debug.Assertf(false, "mem usage type (%s) is not recognized.. something went wrong", maxUsage.Type)
		return 0
	}
}

///////////////////////
// buildingShardInfo //
///////////////////////

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
