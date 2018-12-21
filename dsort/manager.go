/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */

// Package dsort provides APIs for distributed archive file shuffling.
package dsort

import (
	"crypto/md5"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/memsys"
	"github.com/NVIDIA/dfcpub/transport"
	sigar "github.com/cloudfoundry/gosigar"
	jsoniter "github.com/json-iterator/go"
)

const (
	// defaultCallTimeout determines how long we should wait for the other target to respond.
	//
	// FIXME(januszm): maybe it should be something that is configurable by the user
	// and this would be default value.
	defaultCallTimeout = time.Second * 30
)

var (
	_ json.Marshaler   = &records{}
	_ json.Unmarshaler = &records{}
)

var (
	ctx dsortContext

	mem      *memsys.Mem2
	once     sync.Once
	initOnce = func() {
		fs.CSM.RegisterFileType(dSortFileType, &dsortFile{})
		fs.CSM.RegisterFileType(dSortWorkfileType, &dsortFile{})

		mem = &memsys.Mem2{
			Name: "DSortMem2",
		}
		if err := mem.Init(false); err != nil {
			glog.Error(err)
			return
		}
	}
)

type dsortContext struct {
	smap cluster.Sowner
	node *cluster.Snode
}

// progressState abstracts all information meta information about progress of
// the job.
type progressState struct {
	inProgress bool
	aborted    bool
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
	ManagerUUID string   `json:"manager_uuid"`
	Metrics     *Metrics `json:"metrics"`

	ctx dsortContext

	records  *records
	enqueued struct {
		mu      sync.Mutex
		records []*records // records received from other targets which are waiting to be merged
	}
	extractCreator     extractCreator
	shards             []*shard
	startShardCreation chan struct{}
	rs                 *ParsedRequestSpec

	client        *http.Client
	fileExtension string
	compression   struct {
		compressed   int64 // Total compressed size
		uncompressed int64 // Total uncompressed size
	}
	totalInputShardsSeen uint64 // Number of shards processed during extraction phase
	received             struct {
		count int32 // Number of FileMeta slices received, defining what step in the sort a target is in.
		ch    chan int32
	}
	refCount        int64 // Reference counter used to determine if we can do cleanup
	maxMemUsage     *parsedMemUsage
	state           progressState
	extractionPaths sync.Map // Keys correspond to all paths to record contents on disk.
	extractSema     struct {
		funcCalls   chan struct{} // Counting semaphore to limit concurrent calls to ExtractShard
		gorountines chan struct{} // Counting semaphore to limit number of goroutines created in extract shard phase
	}
	createSema struct {
		funcCalls   chan struct{} // Counting semaphore to limit concurrent calls to CreateShard
		gorountines chan struct{} // Counting semaphore to limit number of goroutines created in create shard phase
	}
	mu sync.Mutex

	streams struct {
		request  map[string]*StreamPool
		response map[string]*StreamPool
		shards   map[string]*StreamPool // streams for pushing streams to other targets if the fqn is non-local
	}
	streamWriters struct {
		writers map[string]*streamWriter
		mu      sync.Mutex
	}

	callTimeout time.Duration // Maximal time we will wait for other node to respond
}

// loadRemoteContentFunc is type for the function which loads content from the
// remote target.
type loadContentFunc func(w io.Writer, rec *record, obj *recordObj) (int64, error)

// contentPathFunc is type for the function which for given key and ext generates
// a contentPath aka recordPath and fullPath.
type contentPathFunc func(string, string) (string, string)

// extractCreator is interface which describes set of functions which each
// shard creator should implement.
type extractCreator interface {
	ExtractShard(r *io.SectionReader, records *records, toDisk bool, path contentPathFunc) (int64, int, error)
	CreateShard(s *shard, w io.Writer, loadContent loadContentFunc) (int64, error)
	UsingCompression() bool
	RecordContents() *sync.Map
	MetadataSize() int64
}

// shard represents the metadata required to construct a single shard (aka an archive file).
type shard struct {
	Name   string `json:"n"`
	Bucket string `json:"b"`
	// isLocal describes if given bucket is local or not.
	IsLocal bool `json:"l"`
	// size is total size of shard to be created.
	Size int64 `json:"s"`
	// records contains all metadata to construct the shard.
	Records *records `json:"r"`
}

// recordObj describes single object of record. Objects inside single record
// differs by extension.
type recordObj struct {
	MetadataSize int64  `json:"ms"`
	Size         int64  `json:"s"`
	Extension    string `json:"e"`
}

// record represents the metadata corresponding to a single file from an archive file.
type record struct {
	Key      string `json:"k"` // Used to determine the sorting order.
	DaemonID string `json:"n"` // ID of the target which maintains the contents for this record.
	// Location on disk where the contents are stored. Doubles as the key for extractCreator's RecordContents.
	// To get full path for given object you need to use `FullContentPath` method.
	ContentPath string `json:"p"`
	// All objects associated with given record. Record can be composed of
	// multiple objects which have the same name but different extension.
	Objects []*recordObj `json:"o"`
}

// records abstract array of records. It safe to be used concurrently.
type records struct {
	mu               sync.Mutex
	arr              []*record
	m                map[string]*record
	totalObjectCount int // total number of objects in all recrods
}

func RegisterNode(smap cluster.Sowner, snode *cluster.Snode) {
	ctx.smap = smap
	ctx.node = snode
}

// init initializes all necessary fields.
//
// NOTE: should be done under lock.
func (m *Manager) init(rs *ParsedRequestSpec) error {
	// Initialize memsys and register dsort file type but only at the first
	// time some manager will be initialized.
	once.Do(initOnce)

	// Set extract creator depending on extension provided by the user
	m.setExtractCreator(rs, ctx.node.DaemonID)

	m.ctx = ctx
	targetCount := m.ctx.smap.Get().CountTargets()

	m.Metrics = newMetrics()
	m.records = newRecords(1024)
	m.enqueued.records = make([]*records, 0, targetCount)
	m.shards = make([]*shard, 1000)
	m.startShardCreation = make(chan struct{}, 1)
	m.rs = rs

	defaultTransport := http.DefaultTransport.(*http.Transport)
	m.client = &http.Client{
		Transport: &http.Transport{
			// defaults
			Proxy: defaultTransport.Proxy,
			// Such long timeouts are dictated by the fact that unmarshaling
			// records can tak a long time, even couple of minutes.
			DialContext: (&net.Dialer{
				Timeout:   300 * time.Second,
				KeepAlive: 300 * time.Second,
				DualStack: true,
			}).DialContext,
			IdleConnTimeout:       defaultTransport.IdleConnTimeout,
			ExpectContinueTimeout: defaultTransport.ExpectContinueTimeout,
			TLSHandshakeTimeout:   defaultTransport.TLSHandshakeTimeout,
			// custom
			MaxIdleConnsPerHost: 1000,
			MaxIdleConns:        0, // Zero means no limit
		},
		Timeout: 30 * time.Minute,
	}

	m.fileExtension = rs.Extension
	m.received.count = int32(0)
	m.received.ch = make(chan int32, 10)
	m.totalInputShardsSeen = 0
	m.refCount = 0
	m.maxMemUsage = rs.MaxMemUsage

	// By default we want avg compression ratio to be equal to 1
	m.compression.compressed = 1
	m.compression.uncompressed = 1

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

	m.setInProgressTo(true)
	m.setAbortedTo(false)

	m.streamWriters.writers = make(map[string]*streamWriter, 10000)
	m.callTimeout = defaultCallTimeout
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

	streamCount := runtime.NumCPU() + 1 // stream count per target
	for _, si := range ctx.smap.Get().Tmap {
		m.streams.request[si.DaemonID] = NewStreamPool(2)
		m.streams.response[si.DaemonID] = NewStreamPool(streamCount)
		m.streams.shards[si.DaemonID] = NewStreamPool(streamCount)
		for i := 0; i < streamCount; i++ {
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

// cleanup removes all memory allocated and removes all files created during sort run.
//
// NOTE: If cleanup is invoked during the run it is treated as abort.
func (m *Manager) cleanup() {
	glog.Infof("dsort %s has started a cleanup", m.ManagerUUID)
	m.lock()
	defer m.unlock()
	if m.inProgress() {
		m.setAbortedTo(true)
		// Don't wait locked on finishing
		m.unlock()
		m.waitForFinish()
		m.lock()
	}

	now := time.Now()
	for _, streamPoolArr := range []map[string]*StreamPool{m.streams.request, m.streams.response, m.streams.shards} {
		for _, streamPool := range streamPoolArr {
			streamPool.Stop()
		}
	}
	m.streams.request = nil
	m.streams.response = nil
	m.streams.shards = nil
	m.streamWriters.writers = nil

	m.records.drain()
	m.shards = nil
	m.extractCreator = nil
	m.client = nil
	m.extractionPaths.Range(func(k, v interface{}) bool {
		if err := os.RemoveAll(k.(string)); err != nil {
			glog.Errorf("%s: could not remove extraction path (%v) from previous run, err: %v", m.ManagerUUID, k, err)
		}
		m.extractionPaths.Delete(k)
		return true
	})
	// Free memsys leftovers
	mem.Free(memsys.FreeSpec{
		Totally: true,
		ToOS:    true,
	})
	glog.Infof("dsort %s cleanup has been finished in %v", m.ManagerUUID, time.Since(now))
}

// abort stops currently running sort job and frees associated resources.
func (m *Manager) abort() {
	glog.Infof("manager %s has been aborted", m.ManagerUUID)
	m.lock()
	m.setAbortedTo(true)
	m.unlock()
	// If job has already finished we just free resources.
	if m.inProgress() {
		m.waitForFinish()
	}

	go m.cleanup()
}

// setExtractCreator sets what type of file extraction and creation is used based on the RequestSpec.
func (m *Manager) setExtractCreator(rs *ParsedRequestSpec, daemonID string) {
	var key keyFunc
	if rs.Algorithm.Kind == SortKindMD5 {
		key = keyMD5
	} else {
		key = keyIdentity
	}

	switch rs.Extension {
	case extTar, extTarTgz, extTgz:
		m.extractCreator = &tarExtractCreator{
			recordContents: &sync.Map{},
			daemonID:       daemonID,
			gzipped:        rs.Extension != extTar,
			h:              md5.New(),
			key:            key,
		}
	case extZip:
		m.extractCreator = &zipExtractCreator{
			recordContents: &sync.Map{},
			daemonID:       daemonID,
			h:              md5.New(),
			key:            key,
		}
	default:
		cmn.Assert(false, fmt.Sprintf("unknown extension %s", rs.Extension))
	}
}

// incrementReceived increments number of received records batches. Also puts
// the information in the channel so other waiting goroutine can be informed
// that the information has been updated.
func (m *Manager) incrementReceived() {
	atomic.AddInt32(&m.received.count, 1)
	m.received.ch <- m.received.count
}

// listenReceived returns channel on which waiting goroutine can hang and wait
// until received count value has been updated (see: incrementReceived).
func (m *Manager) listenReceived() chan int32 {
	return m.received.ch
}

func (m *Manager) addCompressionSizes(compressed, uncompressed int64) {
	atomic.AddInt64(&m.compression.compressed, compressed)
	atomic.AddInt64(&m.compression.uncompressed, uncompressed)
}

func (m *Manager) totalCompressedSize() int64 {
	return atomic.LoadInt64(&m.compression.compressed)
}

func (m *Manager) totalUncompressedSize() int64 {
	return atomic.LoadInt64(&m.compression.uncompressed)
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
	atomic.AddInt64(&m.refCount, by)
}

// decrementRef decrements reference counter. If it is 0 or below and dsort has
// already finished returns true. Otherwise, false is returned.
func (m *Manager) decrementRef(by int64) {
	newRefCount := atomic.AddInt64(&m.refCount, -by)
	if newRefCount <= 0 {
		// When ref count is below zero or zero we should schedule cleanup
		m.lock()
		if !m.inProgress() {
			m.unlock()
			go Managers.persist(m.ManagerUUID)
			return
		}
		m.unlock()
	}
}

func (m *Manager) addToTotalInputShardsSeen(seen uint64) {
	atomic.AddUint64(&m.totalInputShardsSeen, seen)
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
	if !inProgress && m.aborted() {
		m.state.wg.Done()
	}

	m.state.inProgress = inProgress
}

// setAbortedTo updates aborted state. If aborted is set to true and sort is not
// yet finished. We need to inform current phase about abort (closing channel)
// and mark that we will wait until it is finished.
//
// NOTE: Should be used under lock.
func (m *Manager) setAbortedTo(aborted bool) {
	if aborted {
		// If not finished and not yet aborted we should mark that we will wait.
		if m.inProgress() && !m.state.aborted {
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

func (m *Manager) maxMemoryUsage() (uint64, error) {
	mem := sigar.Mem{}
	if err := mem.Get(); err != nil {
		return 0, err
	}
	switch m.maxMemUsage.Type {
	case memPercent:
		return m.maxMemUsage.Value * (mem.Total / 100), nil
	case memNumber:
		return cmn.MinU64(m.maxMemUsage.Value, mem.Total), nil
	default:
		cmn.Assert(false, fmt.Sprintf("mem usage type (%s) is not recognized.. something went wrong", m.maxMemUsage.Type))
		return 0, nil
	}
}

func (m *Manager) addExtractionPath(path string) {
	m.extractionPaths.Store(path, struct{}{})
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

func (m *Manager) enqueueRecords(records *records) {
	m.enqueued.mu.Lock()
	m.enqueued.records = append(m.enqueued.records, records)
	m.enqueued.mu.Unlock()
}

func (m *Manager) mergeEnqueuedRecords() {
	for {
		m.enqueued.mu.Lock()
		lastIdx := len(m.enqueued.records) - 1
		if lastIdx < 0 {
			m.enqueued.mu.Unlock()
			break
		}
		records := m.enqueued.records[lastIdx]
		m.enqueued.records = m.enqueued.records[:lastIdx]
		m.enqueued.mu.Unlock()

		m.records.merge(records)
	}
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
		hdr.Dsize = 0
		if err = m.streams.response[node.DaemonID].Get().Send(hdr, nil, nil); err != nil {
			glog.Error(err)
		}
	}

	return func(w http.ResponseWriter, hdr transport.Header, object io.Reader, err error) {
		fromNode := m.ctx.smap.Get().Tmap[string(hdr.Opaque)]
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

		v, ok := m.extractCreator.RecordContents().Load(hdr.Objname)
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
			respHdr.Dsize = fi.Size()
			if err := m.streams.response[fromNode.DaemonID].Get().Send(respHdr, f, m.responseCallback); err != nil {
				glog.Error(err)
			}
		} else {
			sgl := v.(*memsys.SGL)
			respHdr.Dsize = sgl.Size()
			if err := m.streams.response[fromNode.DaemonID].Get().Send(respHdr, sgl, m.responseCallback); err != nil {
				glog.Error(err)
			}
			m.extractCreator.RecordContents().Delete(hdr.Objname)
		}
	}
}

func (m *Manager) makeRecvResponseFunc() transport.Receive {
	return func(w http.ResponseWriter, hdr transport.Header, object io.Reader, err error) {
		if err != nil {
			glog.Error(err)
			return
		}

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

		buf, slab := mem.AllocFromSlab2(cmn.MiB)
		defer slab.Free(buf)

		isLocal, err := strconv.ParseBool(string(hdr.Opaque))
		if err != nil {
			glog.Error(err)
			return
		}

		shardFQN, errStr := cluster.FQN(fs.ObjectType, hdr.Bucket, hdr.Objname, isLocal)
		if errStr != "" {
			glog.Error(errStr)
			return
		}
		file, err := cmn.CreateFile(shardFQN)
		if err != nil {
			glog.Error(err)
			return
		}
		defer file.Close()
		if _, err := io.CopyBuffer(file, object, buf); err != nil {
			glog.Error(err)
			return
		}
	}
}

// loadLocalContent returns function to load content from local storage (either disk or memory).
func (m *Manager) loadContent() loadContentFunc {
	return func(w io.Writer, rec *record, obj *recordObj) (int64, error) {
		loadLocal := func(w io.Writer, pathToContent string) (written int64, err error) {
			buf, slab := mem.AllocFromSlab2(cmn.MiB)
			defer func() {
				slab.Free(buf)
				m.decrementRef(1)
			}()

			var n int64
			v, ok := m.extractCreator.RecordContents().Load(pathToContent)
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
				sgl := v.(*memsys.SGL)
				if n, err = io.CopyBuffer(w, sgl, buf); err != nil {
					sgl.Free()
					return written, err
				}
				sgl.Free()
				m.extractCreator.RecordContents().Delete(pathToContent)
			}

			written += n
			return
		}

		loadRemote := func(w io.Writer, daemonID, pathToContents string) (int64, error) {
			writer := m.newStreamWriter(pathToContents, w)
			hdr := transport.Header{
				Objname: pathToContents,
				Opaque:  []byte(m.ctx.node.DaemonID),
			}
			toNode := m.ctx.smap.Get().Tmap[daemonID]
			if err := m.streams.request[toNode.DaemonID].Get().Send(hdr, nil, nil); err != nil {
				return 0, err
			}

			// It may happen that the target we are trying to contact was aborted or
			// for some reason is not responding. Thus we need to do some precaution
			// and wait for the content only for limited time.
			if writer.wg.WaitTimeout(m.callTimeout) {
				m.pullStreamWriter(pathToContents)
				return 0, fmt.Errorf("wait for remote content has timed out (%q was waiting for %q)", m.ctx.node.DaemonID, daemonID)
			}
			return writer.n, writer.err
		}

		if rec.DaemonID != m.ctx.node.DaemonID { // File source contents are located on a different target.
			return loadRemote(w, rec.DaemonID, rec.fullContentPath(obj))
		}

		// Load from local source: file or sgl
		return loadLocal(w, rec.fullContentPath(obj))
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

// Merges two records into single one. It is required for records to have the
// same ContentPath. Since records should only differ on objects this is the
// thing that is actually merged.
func (r *record) mergeObjects(other *record) {
	cmn.Assert(r.ContentPath == other.ContentPath)
	r.Objects = append(r.Objects, other.Objects...)
}

// fullContentPath makes path to particular object.
func (r *record) fullContentPath(obj *recordObj) string {
	return makeFullContentPath(r.ContentPath, obj.Extension)
}

func (r *record) totalSize() int64 {
	size := int64(0)
	for _, obj := range r.Objects {
		size += obj.Size
	}
	return size
}

func makeFullContentPath(contentPath, extension string) string {
	return contentPath + extension
}

// getContentPathsFunc for given fqn generate contentPathFunc which then return
// recordPath and fullPath for given key.
func (m *Manager) getContentPathsFunc(fqn string) contentPathFunc {
	return func(key string, ext string) (string, string) {
		fqnWithoutExt := strings.TrimSuffix(fqn, m.rs.Extension)
		keyWithoutExt := strings.TrimSuffix(key, ext)
		recordPath := fs.CSM.GenContentFQN(fqnWithoutExt+"-"+keyWithoutExt, dSortFileType, "")
		fullPath := recordPath + ext
		m.addExtractionPath(fullPath)
		return recordPath, fullPath
	}
}

// newRecords creates new instance of Records struct and allocates n places for
// the actual Record's
func newRecords(n int) *records {
	return &records{
		arr: make([]*record, 0, n),
		m:   make(map[string]*record, n),
	}
}

func (r *records) drain() {
	r.mu.Lock()
	r.arr = nil
	r.m = nil
	r.mu.Unlock()
}

func (r *records) insert(records ...*record) {
	r.mu.Lock()
	for _, record := range records {
		// Checking if record is already registered. If that is the case we need
		// to merge extensions (files with same names but different extensions
		// should be in single record). Otherwise just add new record.
		if existingRecord, ok := r.m[record.ContentPath]; ok {
			existingRecord.mergeObjects(record)
		} else {
			r.arr = append(r.arr, record)
			r.m[record.ContentPath] = record
		}

		r.totalObjectCount += len(record.Objects)
	}
	r.mu.Unlock()
}

func (r *records) merge(records *records) {
	r.insert(records.arr...)
}

func (r *records) all() []*record {
	return r.arr
}

func (r *records) slice(start, end int) *records {
	return &records{
		arr: r.arr[start:end],
	}
}

func (r *records) len() int {
	return len(r.arr)
}

func (r *records) objectCount() int {
	return r.totalObjectCount
}

func (r *records) MarshalJSON() ([]byte, error) {
	return jsoniter.Marshal(r.arr)
}

func (r *records) UnmarshalJSON(b []byte) error {
	return jsoniter.Unmarshal(b, &r.arr)
}
