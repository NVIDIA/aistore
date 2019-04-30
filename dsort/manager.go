// Package dsort provides APIs for distributed archive file shuffling.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dsort

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/dsort/extract"
	"github.com/NVIDIA/aistore/dsort/filetype"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/transport"
	sigar "github.com/cloudfoundry/gosigar"
)

const (
	// defaultCallTimeout determines how long we should wait for the other target to respond.
	//
	// FIXME(januszm): maybe it should be something that is configurable by the user
	// and this would be default value.
	defaultCallTimeout = time.Minute * 10
)

var (
	ctx dsortContext

	mem      *memsys.Mem2
	once     sync.Once
	initOnce = func() {
		fs.CSM.RegisterFileType(filetype.DSortFileType, &filetype.DSortFile{})
		fs.CSM.RegisterFileType(filetype.DSortWorkfileType, &filetype.DSortFile{})

		mem = &memsys.Mem2{
			Name:     "DSort.Mem2",
			TimeIval: time.Minute * 10,
		}
		if err := mem.Init(false); err != nil {
			glog.Error(err)
			return
		}
		go mem.Run()
	}

	_ cluster.Slistener = &Manager{}
)

type dsortContext struct {
	smap       cluster.Sowner
	bmdowner   cluster.Bowner
	node       *cluster.Snode
	t          cluster.Target
	nameLocker cluster.NameLocker
}

// progressState abstracts all information meta information about progress of
// the job.
type progressState struct {
	inProgress bool
	aborted    bool
	cleaned    atomic.Int32
	wg         *sync.WaitGroup
	// doneCh is closed when the job is aborted so that goroutines know when
	// they need to stop.
	doneCh chan struct{}
}

type streamWriter struct {
	w   io.Writer
	n   int64
	err error
	wg  *cmn.TimeoutGroup
}

// Manager maintains all the state required for a single run of a distributed archive file shuffle.
type Manager struct {
	// Fields with json tags are the only fields which are persisted
	// into the disk once the dSort is finished.
	ManagerUUID string   `json:"manager_uuid"`
	Description string   `json:"description"`
	Metrics     *Metrics `json:"metrics"`

	mu   sync.Mutex
	ctx  dsortContext
	smap *cluster.Smap

	recManager         *extract.RecordManager
	shardManager       *extract.ShardManager
	extractCreator     extract.ExtractCreator
	startShardCreation chan struct{}
	rs                 *ParsedRequestSpec

	client        *http.Client
	fileExtension string
	compression   struct {
		compressed   atomic.Int64 // Total compressed size
		uncompressed atomic.Int64 // Total uncompressed size
	}
	totalInputShardsSeen atomic.Uint64 // Number of shards processed during extraction phase
	received             struct {
		count atomic.Int32 // Number of FileMeta slices received, defining what step in the sort a target is in.
		ch    chan int32
	}
	refCount    atomic.Int64 // Reference counter used to determine if we can do cleanup
	state       progressState
	extractSema struct {
		funcCalls   chan struct{} // Counting semaphore to limit concurrent calls to ExtractShard
		gorountines chan struct{} // Counting semaphore to limit number of goroutines created in extract shard phase
	}
	createSema struct {
		funcCalls   chan struct{} // Counting semaphore to limit concurrent calls to CreateShard
		gorountines chan struct{} // Counting semaphore to limit number of goroutines created in create shard phase
	}
	streams struct {
		request  map[string]*StreamPool
		response map[string]*StreamPool
		shards   map[string]*StreamPool // streams for pushing streams to other targets if the fqn is non-local
	}
	streamWriters struct {
		mu      sync.Mutex
		writers map[string]*streamWriter
	}
	finishedAck struct {
		mu sync.Mutex
		m  map[string]struct{} // finished acks: daemonID -> ack
	}
	mw *memoryWatcher

	callTimeout time.Duration // Maximal time we will wait for other node to respond
}

func RegisterNode(smap cluster.Sowner, bmdowner cluster.Bowner, snode *cluster.Snode, t cluster.Target, nameLocker cluster.NameLocker) {
	ctx.smap = smap
	ctx.bmdowner = bmdowner
	ctx.node = snode
	ctx.t = t
	ctx.nameLocker = nameLocker
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
	m.Description = rs.ProcDescription
	m.Metrics = newMetrics(rs.ExtendedMetrics)
	m.startShardCreation = make(chan struct{}, 1)

	// Set extract creator depending on extension provided by the user
	m.setExtractCreator()

	m.client = cmn.NewClient(cmn.ClientArgs{
		DialTimeout: 5 * time.Minute,
		Timeout:     30 * time.Minute,
	})

	m.fileExtension = rs.Extension
	m.received.ch = make(chan int32, 10)

	// By default we want avg compression ratio to be equal to 1
	m.compression.compressed = *atomic.NewInt64(1)
	m.compression.uncompressed = *atomic.NewInt64(1)

	// Concurrency
	m.extractSema.funcCalls = make(chan struct{}, rs.ExtractConcLimit)
	m.createSema.funcCalls = make(chan struct{}, rs.CreateConcLimit)
	// Number of goroutines should be larger than number of concurrency limit
	// but it should not be:
	// * too small - we don't want to artificially bottleneck the phases.
	// * too large - we don't want too much goroutines in the system, it can cause
	//               too much overhead on context switching and managing the goroutines.
	//               Also for large workloads goroutines can take a lot of memory.
	m.extractSema.gorountines = make(chan struct{}, 3*targetCount*rs.ExtractConcLimit)
	m.createSema.gorountines = make(chan struct{}, 3*rs.CreateConcLimit)

	m.streams.request = make(map[string]*StreamPool, 100)
	m.streams.response = make(map[string]*StreamPool, 100)
	m.streams.shards = make(map[string]*StreamPool, 100)

	// Fill ack map with current daemons. Once the finished ack is received from
	// another daemon we will remove it from the map until len(ack) == 0 (then
	// we will know that all daemons have finished operation).
	m.finishedAck.m = make(map[string]struct{}, targetCount)
	for sid := range m.smap.Tmap {
		m.finishedAck.m[sid] = struct{}{}
	}

	m.setInProgressTo(true)
	m.setAbortedTo(false)

	m.streamWriters.writers = make(map[string]*streamWriter, 10000)
	m.callTimeout = defaultCallTimeout

	// Memory watcher
	mem := sigar.Mem{}
	if err := mem.Get(); err != nil {
		return err
	}
	maxMemoryToUse := calcMaxMemoryUsage(rs.MaxMemUsage, mem)
	m.mw = newMemoryWatcher(m, maxMemoryToUse)

	return nil
}

func (m *Manager) initStreams() error {
	// Requests are usually small packets, no more 1KB that is why we want to
	// utilize intraControl network
	config := cmn.GCO.Get()
	reqNetwork := cmn.NetworkIntraControl
	if !config.Net.UseIntraControl {
		reqNetwork = cmn.NetworkPublic
	}
	// Responses to the other targets are objects that is why we want to use
	// intraData network.
	respNetwork := cmn.NetworkIntraData
	if !config.Net.UseIntraData {
		respNetwork = cmn.NetworkPublic
	}

	trname := fmt.Sprintf("dsort-%s-recv_req", m.ManagerUUID)
	reqPath, err := transport.Register(reqNetwork, trname, m.makeRecvRequestFunc())
	if err != nil {
		return err
	}

	trname = fmt.Sprintf("dsort-%s-recv_resp", m.ManagerUUID)
	respPath, err := transport.Register(respNetwork, trname, m.makeRecvResponseFunc())
	if err != nil {
		return err
	}

	trname = fmt.Sprintf("dsort-%s-shard", m.ManagerUUID)
	shardPath, err := transport.Register(respNetwork, trname, m.makeRecvShardFunc())
	if err != nil {
		return err
	}

	for _, si := range ctx.smap.Get().Tmap {
		m.streams.request[si.DaemonID] = NewStreamPool(2)
		m.streams.response[si.DaemonID] = NewStreamPool(transport.IntraBundleMultiplier)
		m.streams.shards[si.DaemonID] = NewStreamPool(transport.IntraBundleMultiplier)
		for i := 0; i < transport.IntraBundleMultiplier; i++ {
			url := si.IntraControlNet.DirectURL + reqPath
			m.streams.request[si.DaemonID].Add(NewStream(url))

			url = si.IntraDataNet.DirectURL + respPath
			m.streams.response[si.DaemonID].Add(NewStream(url))

			url = si.IntraDataNet.DirectURL + shardPath
			m.streams.shards[si.DaemonID].Add(NewStream(url))
		}
	}

	return nil
}

func (m *Manager) cleanupStreams() error {
	config := cmn.GCO.Get()
	reqNetwork := cmn.NetworkIntraControl
	if !config.Net.UseIntraControl {
		reqNetwork = cmn.NetworkPublic
	}
	// Responses to the other targets are objects that is why we want to use
	// intraData network.
	respNetwork := cmn.NetworkIntraData
	if !config.Net.UseIntraData {
		respNetwork = cmn.NetworkPublic
	}

	if len(m.streams.request) > 0 {
		trname := fmt.Sprintf("dsort-%s-recv_req", m.ManagerUUID)
		if err := transport.Unregister(reqNetwork, trname); err != nil {
			return err
		}
	}

	if len(m.streams.response) > 0 {
		trname := fmt.Sprintf("dsort-%s-recv_resp", m.ManagerUUID)
		if err := transport.Unregister(respNetwork, trname); err != nil {
			return err
		}
	}

	if len(m.streams.shards) > 0 {
		trname := fmt.Sprintf("dsort-%s-shard", m.ManagerUUID)
		if err := transport.Unregister(respNetwork, trname); err != nil {
			return err
		}
	}

	for _, streamPoolArr := range []map[string]*StreamPool{m.streams.request, m.streams.response, m.streams.shards} {
		for _, streamPool := range streamPoolArr {
			streamPool.Stop()
		}
	}

	m.streams.request = nil
	m.streams.response = nil
	m.streams.shards = nil
	return nil
}

// cleanup removes all memory allocated and removes all files created during sort run.
//
// PRECONDITION: manager must be not in progress state (either actual finish or abort).
//
// NOTE: If cleanup is invoked during the run it is treated as abort.
func (m *Manager) cleanup() {
	if !m.state.cleaned.CAS(0, 1) {
		return // do not clean if already scheduled
	}

	m.lock()
	m.mw.stop()
	glog.Infof("dsort %s has started a cleanup", m.ManagerUUID)
	now := time.Now()

	defer func() {
		m.unlock()
		glog.Infof("dsort %s cleanup has been finished in %v", m.ManagerUUID, time.Since(now))
	}()

	if m.inProgress() {
		cmn.AssertMsg(false, fmt.Sprintf("%s: was still in progress", m.ManagerUUID))
	}

	m.streamWriters.writers = nil

	m.shardManager.Cleanup()
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
func (m *Manager) finalCleanup() {
	if !m.state.cleaned.CAS(1, 2) {
		return // do not clean if already scheduled
	}

	glog.Infof("dsort %s has started a final cleanup", m.ManagerUUID)
	now := time.Now()
	defer func() {
		glog.Infof("dsort %s final cleanup has been finished in %v", m.ManagerUUID, time.Since(now))
	}()

	if err := m.cleanupStreams(); err != nil {
		glog.Error(err)
	}

	// The reason why this is not in regular cleanup is because we are only sure
	// that this can be freed once we cleanup streams - streams are asynchronous
	// and we may have race between in-flight request and cleanup.
	m.recManager.Cleanup()
	extract.FreeMemory()

	m.finishedAck.m = nil
	Managers.persist(m.ManagerUUID)
}

// abort stops currently running sort job and frees associated resources.
func (m *Manager) abort() {
	m.lock()
	if m.aborted() { // do not abort if already aborted
		m.unlock()
		return
	}

	glog.Infof("manager %s has been aborted", m.ManagerUUID)
	m.setAbortedTo(true)
	inProgress := m.inProgress()
	m.unlock()

	// If job has already finished we just free resources.
	if inProgress {
		m.waitForFinish()
	}

	go func() {
		m.cleanup()
		m.finalCleanup() // on abort always perform final cleanup
	}()
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
		return err
	}

	onDuplicatedRecords := func(msg string) error {
		return m.react(cfg.DuplicatedRecords, msg)
	}

	m.recManager = extract.NewRecordManager(m.ctx.node.DaemonID, m.rs.Extension, keyExtractor, onDuplicatedRecords)
	m.shardManager = extract.NewShardManager()

	switch m.rs.Extension {
	case extTar, extTarTgz, extTgz:
		m.extractCreator = extract.NewTarExtractCreator(m.rs.Extension != extTar)
	case extZip:
		m.extractCreator = extract.NewZipExtractCreator()
	default:
		cmn.AssertMsg(false, fmt.Sprintf("unknown extension %s", m.rs.Extension))
	}

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

func (m *Manager) addToTotalInputShardsSeen(seen uint64) {
	m.totalInputShardsSeen.Add(seen)
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

func (m *Manager) acquireExtractSema() {
	m.extractSema.funcCalls <- struct{}{}
}

func (m *Manager) releaseExtractSema() {
	<-m.extractSema.funcCalls
}

func (m *Manager) acquireExtractGoroutineSema() {
	m.extractSema.gorountines <- struct{}{}
}

func (m *Manager) releaseExtractGoroutineSema() {
	<-m.extractSema.gorountines
}

func (m *Manager) acquireCreateSema() {
	m.createSema.funcCalls <- struct{}{}
}

func (m *Manager) releaseCreateSema() {
	<-m.createSema.funcCalls
}

func (m *Manager) acquireCreateGoroutineSema() {
	m.createSema.gorountines <- struct{}{}
}

func (m *Manager) releaseCreateGoroutineSema() {
	<-m.createSema.gorountines
}

func (m *Manager) newStreamWriter(pathToContents string, w io.Writer) *streamWriter {
	writer := &streamWriter{
		w:  w,
		wg: cmn.NewTimeoutGroup(),
	}
	writer.wg.Add(1)
	m.streamWriters.mu.Lock()
	m.streamWriters.writers[pathToContents] = writer
	m.streamWriters.mu.Unlock()
	return writer
}

func (m *Manager) pullStreamWriter(objName string) *streamWriter {
	m.streamWriters.mu.Lock()
	writer := m.streamWriters.writers[objName]
	delete(m.streamWriters.writers, objName)
	m.streamWriters.mu.Unlock()
	return writer
}

func (m *Manager) responseCallback(hdr transport.Header, rc io.ReadCloser, err error) {
	if sgl, ok := rc.(*memsys.SGL); ok {
		sgl.Free()
	}
	m.decrementRef(1)
	if err != nil {
		glog.Error(err)
	}
}

func (m *Manager) makeRecvRequestFunc() transport.Receive {
	errHandler := func(err error, hdr transport.Header, node *cluster.Snode) {
		hdr.Opaque = []byte(err.Error())
		hdr.ObjAttrs.Size = 0
		if err = m.streams.response[node.DaemonID].Get().Send(hdr, nil, nil); err != nil {
			glog.Error(err)
		}
	}

	return func(w http.ResponseWriter, hdr transport.Header, object io.Reader, err error) {
		fromNode := m.smap.GetTarget(string(hdr.Opaque))
		if err != nil {
			errHandler(err, hdr, fromNode)
			return
		}

		respHdr := transport.Header{
			Objname: hdr.Objname,
		}

		if m.aborted() {
			glog.V(4).Info("dsort process was aborted")
			return
		}

		v, ok := m.recManager.RecordContents().Load(hdr.Objname)
		if !ok {
			f, err := cmn.NewFileHandle(hdr.Objname)
			if err != nil {
				errHandler(err, respHdr, fromNode)
				return
			}
			fi, err := f.Stat()
			if err != nil {
				f.Close()
				errHandler(err, respHdr, fromNode)
				return
			}
			respHdr.ObjAttrs.Size = fi.Size()
			if err := m.streams.response[fromNode.DaemonID].Get().Send(respHdr, f, m.responseCallback); err != nil {
				f.Close()
				glog.Error(err)
			}
		} else {
			m.recManager.RecordContents().Delete(hdr.Objname)
			sgl := v.(*memsys.SGL)
			respHdr.ObjAttrs.Size = sgl.Size()
			if err := m.streams.response[fromNode.DaemonID].Get().Send(respHdr, sgl, m.responseCallback); err != nil {
				sgl.Free()
				glog.Error(err)
			}
		}
	}
}

func (m *Manager) makeRecvResponseFunc() transport.Receive {
	return func(w http.ResponseWriter, hdr transport.Header, object io.Reader, err error) {
		if err != nil {
			glog.Error(err)
			return
		}

		defer io.Copy(ioutil.Discard, object) // drain to prevent unnecessary stream errors

		writer := m.pullStreamWriter(hdr.Objname)
		if writer == nil { // was removed after timing out
			return
		}

		if len(hdr.Opaque) > 0 {
			writer.n, writer.err = 0, errors.New(string(hdr.Opaque))
			writer.wg.Done()
			return
		}

		buf, slab := mem.AllocFromSlab2(cmn.MiB)
		writer.n, writer.err = io.CopyBuffer(writer.w, object, buf)
		writer.wg.Done()
		slab.Free(buf)
	}
}

func (m *Manager) makeRecvShardFunc() transport.Receive {
	return func(w http.ResponseWriter, hdr transport.Header, object io.Reader, err error) {
		if err != nil {
			glog.Error(err)
			return
		}

		if m.aborted() {
			glog.V(4).Info("dsort process was aborted")
			return
		}

		cksum := cmn.NewCksum(hdr.ObjAttrs.CksumType, hdr.ObjAttrs.CksumValue)
		buf, slab := mem.AllocFromSlab2(cmn.MiB)
		defer slab.Free(buf)

		bckProvider := cmn.BckProviderFromLocal(hdr.IsLocal)
		lom, errStr := cluster.LOM{T: m.ctx.t, Bucket: hdr.Bucket, Objname: hdr.Objname, BucketProvider: bckProvider}.Init()
		if errStr == "" {
			_, errStr = lom.Load(true)
		}
		if errStr != "" {
			glog.Error(errStr)
			return
		}
		if lom.Exists() {
			if lom.Cksum() != nil && cmn.EqCksum(lom.Cksum(), cksum) {
				glog.Infof("shard (%s) already exists and checksums are equal, skipping", lom)
				io.Copy(ioutil.Discard, object) // drain the reader
				return
			}

			glog.Warningf("shard (%s) already exists, overriding", lom)
		}
		workFQN := fs.CSM.GenContentParsedFQN(lom.ParsedFQN, filetype.DSortWorkfileType, filetype.WorkfileRecvShard)
		rc := ioutil.NopCloser(object)
		if err := m.ctx.t.Receive(workFQN, rc, lom, cluster.WarmGet, nil); err != nil {
			glog.Error(err)
			return
		}
	}
}

// loadLocalContent returns function to load content from local storage (either disk or memory).
func (m *Manager) loadContent() extract.LoadContentFunc {
	return func(w io.Writer, rec *extract.Record, obj *extract.RecordObj) (int64, error) {
		loadLocal := func(w io.Writer, pathToContent string) (written int64, err error) {
			buf, slab := mem.AllocFromSlab2(cmn.MiB)
			defer func() {
				slab.Free(buf)
				m.decrementRef(1)
			}()

			var n int64
			v, ok := m.recManager.RecordContents().Load(pathToContent)
			if !ok {
				f, err := os.Open(pathToContent)
				if err != nil {
					return written, err
				}
				if n, err = io.CopyBuffer(w, f, buf); err != nil {
					f.Close()
					return written, err
				}
				f.Close()
			} else {
				m.recManager.RecordContents().Delete(pathToContent)
				sgl := v.(*memsys.SGL)
				if n, err = io.CopyBuffer(w, sgl, buf); err != nil {
					sgl.Free()
					return written, err
				}
				sgl.Free()
			}

			written += n
			return
		}

		loadRemote := func(w io.Writer, daemonID, pathToContents string) (int64, error) {
			var (
				cbErr      error
				beforeRecv time.Time
				beforeSend time.Time

				wg      = &sync.WaitGroup{}
				writer  = m.newStreamWriter(pathToContents, w)
				metrics = m.Metrics.Creation

				hdr = transport.Header{
					Objname: pathToContents,
					Opaque:  []byte(m.ctx.node.DaemonID),
				}
				toNode = m.smap.GetTarget(daemonID)
			)

			if m.Metrics.extended {
				beforeSend = time.Now()
			}

			cb := func(hdr transport.Header, r io.ReadCloser, err error) {
				if err != nil {
					cbErr = err
				}
				if m.Metrics.extended {
					metrics.Lock()
					metrics.RequestStats.update(time.Since(beforeSend))
					metrics.Unlock()
				}
				wg.Done()
			}

			wg.Add(1)
			if err := m.streams.request[toNode.DaemonID].Get().Send(hdr, nil, cb); err != nil {
				return 0, err
			}

			// Send should be synchronous to make sure that 'wait timeout' is calculated
			// only for receive side.
			wg.Wait()

			if cbErr != nil {
				return 0, cbErr
			}

			if m.Metrics.extended {
				beforeRecv = time.Now()
			}

			// It may happen that the target we are trying to contact was
			// aborted or for some reason is not responding. Thus we need to do
			// some precaution and wait for the content only for limited time or
			// until we receive abort signal.
			var pulled bool
			timed, stopped := writer.wg.WaitTimeoutWithStop(m.callTimeout, m.listenAborted())
			if timed || stopped {
				// In case of time out or abort we need to pull the writer to
				// avoid concurrent Close and Write on `writer.w`.
				pulled = m.pullStreamWriter(pathToContents) != nil
			}

			if m.Metrics.extended {
				metrics.Lock()
				metrics.ResponseStats.update(time.Since(beforeRecv))
				metrics.Unlock()
			}

			// If we timed out or were stopped but we didn't manage to pull the
			// writer then this means that someone else did it and we barely
			// missed. In this case we should wait for the job to be finished -
			// in case of being stopped we should receive error anyway.
			if !pulled {
				if timed || stopped {
					writer.wg.Wait()
				}
			} else {
				// We managed to pull the writer, we can safely return error.
				var err error
				if stopped {
					err = fmt.Errorf("wait for remote content was aborted")
				} else if timed {
					err = fmt.Errorf("wait for remote content has timed out (%q was waiting for %q)", m.ctx.node.DaemonID, daemonID)
				} else {
					cmn.AssertMsg(false, "pulled but not stopped or timed?!")
				}
				return 0, err
			}

			return writer.n, writer.err
		}

		if rec.DaemonID != m.ctx.node.DaemonID { // File source contents are located on a different target.
			return loadRemote(w, rec.DaemonID, rec.FullContentPath(obj))
		}

		// Load from local source: file or sgl
		return loadLocal(w, rec.FullContentPath(obj))
	}
}

// doWithAbort sends requests through client. If manager aborts during the call
// request is cancelled.
func (m *Manager) doWithAbort(method, u string, body []byte, w io.Writer) (int64, error) {
	req, _, cancel, err := cmn.ReqWithContext(method, u, body)
	if err != nil {
		return 0, err
	}

	// Start request
	doneCh := make(chan struct{}, 1)
	errCh := make(chan error, 1)
	n := int64(0)
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

		if w != nil {
			buf, slab := mem.AllocFromSlab2(cmn.MiB)
			n, err = io.CopyBuffer(w, resp.Body, buf)
			slab.Free(buf)
			if err != nil {
				errCh <- err
				return
			}
		}
	}()

	// Wait for abort or request to finish
	select {
	case <-m.listenAborted():
		cancel()
		<-doneCh
		return n, newAbortError(m.ManagerUUID)
	case <-doneCh:
		break
	}

	close(errCh)
	return n, <-errCh
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
		// check if some target has been removed - abort in case it does
		for sid := range m.smap.Tmap {
			if newSmap.GetTarget(sid) == nil {
				m.abort()
				// return from the listener as the whole manager is aborted
				return
			}
		}
	}
}

func (m *Manager) String() string {
	return m.ManagerUUID
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
		m.Metrics.lock()
		m.Metrics.Errors = append(m.Metrics.Errors, msg)
		m.Metrics.unlock()
		return errors.New(msg)
	default:
		cmn.AssertMsg(false, reaction)
		return nil
	}
}

func calcMaxMemoryUsage(maxUsage *parsedMemUsage, mem sigar.Mem) uint64 {
	switch maxUsage.Type {
	case memPercent:
		return maxUsage.Value * (mem.Total / 100)
	case memNumber:
		return cmn.MinU64(maxUsage.Value, mem.Total)
	default:
		cmn.AssertMsg(false, fmt.Sprintf("mem usage type (%s) is not recognized.. something went wrong", maxUsage.Type))
		return 0
	}
}
