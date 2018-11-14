/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/dfc/statsd"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/json-iterator/go"
)

//==============================
//
// constants
//
//==============================

const logsTotalSizeCheckTime = time.Hour * 3

const (
	statsKindCounter = "counter"
	statsKindLatency = "latency"
)

// Stats common to proxyCoreStats and targetCoreStats
const (
	statGetCount            = "get.n"
	statPutCount            = "put.n"
	statPostCount           = "pst.n"
	statDeleteCount         = "del.n"
	statRenameCount         = "ren.n"
	statListCount           = "lst.n"
	statGetLatency          = "get.μs"
	statListLatency         = "lst.μs"
	statKeepAliveMinLatency = "kalive.μs.min"
	statKeepAliveMaxLatency = "kalive.μs.max"
	statKeepAliveLatency    = "kalive.μs"
	statUptimeLatency       = "uptime.μs"
	statErrCount            = "err.n"
	statErrGetCount         = "err.get.n"
	statErrDeleteCount      = "err.delete.n"
	statErrPostCount        = "err.post.n"
	statErrPutCount         = "err.put.n"
	statErrHeadCount        = "err.head.n"
	statErrListCount        = "err.list.n"
	statErrRangeCount       = "err.range.n"
)

// Stats only found in targetCoreStats
const (
	statPutLatency       = "put.μs"
	statGetColdCount     = "get.cold.n"
	statGetColdSize      = "get.cold.size"
	statLruEvictSize     = "lru.evict.size"
	statLruEvictCount    = "lru.evict.n"
	statTxCount          = "tx.n"
	statTxSize           = "tx.size"
	statRxCount          = "rx.n"
	statRxSize           = "rx.size"
	statPrefetchCount    = "pre.n"
	statPrefetchSize     = "pre.size"
	statVerChangeCount   = "vchange.n"
	statVerChangeSize    = "vchange.size"
	statErrCksumCount    = "err.cksum.n"
	statErrCksumSize     = "err.cksum.size"
	statGetRedirLatency  = "get.redir.μs"
	statPutRedirLatency  = "put.redir.μs"
	statRebalGlobalCount = "reb.global.n"
	statRebalLocalCount  = "reb.local.n"
	statRebalGlobalSize  = "reb.global.size"
	statRebalLocalSize   = "reb.local.size"
	statReplPutCount     = "replication.put.n"
	statReplPutLatency   = "replication.put.µs"
)

//==============================
//
// types
//
//==============================
type metric = statsd.Metric // type alias

type fscapacity struct {
	Used    uint64 `json:"used"`    // bytes
	Avail   uint64 `json:"avail"`   // ditto
	Usedpct uint32 `json:"usedpct"` // reduntant ok
}

// implemented by the stats runners
type statslogger interface {
	log() (runlru bool)
	housekeep(bool)
	doAdd(nv namedVal64)
}

// implemented by the ***CoreStats types
type statsif interface {
	add(name string, val int64)
	addErrorHTTP(method string, val int64)
	addMany(namedVal64 ...namedVal64)
}

type namedVal64 struct {
	name string
	val  int64
}

// Stats are monitored using a map of stat names (key)
// to statInstances (values). There are two main types
// of stats: counter and latency, which are described using the
// kind field. Only latency stats have associatedVals to them
// that are used in calculating latency measurements.
type statsInstance struct {
	Value         int64 `json:"value"`
	kind          string
	associatedVal int64
}

type statsTracker map[string]*statsInstance

type proxyCoreStats struct {
	Tracker statsTracker
	// omitempty
	statsdC *statsd.Client
	logged  bool
}

type targetCoreStats struct {
	proxyCoreStats
}

type statsrunner struct {
	sync.RWMutex
	cmn.Named
	stopCh    chan struct{}
	workCh    chan namedVal64
	starttime time.Time
}

type proxystatsrunner struct {
	statsrunner
	Core *proxyCoreStats `json:"core"`
}

type storstatsrunner struct {
	statsrunner
	Core     *targetCoreStats       `json:"core"`
	Capacity map[string]*fscapacity `json:"capacity"`
	// iostat
	CPUidle string                   `json:"cpuidle"`
	Disk    map[string]cmn.SimpleKVs `json:"disk"`
	// omitempty
	timeUpdatedCapacity time.Time
	timeCheckedLogSizes time.Time
	fsmap               map[syscall.Fsid]string
}

type ClusterStats struct {
	Proxy  *proxyCoreStats             `json:"proxy"`
	Target map[string]*storstatsrunner `json:"target"`
}

type ClusterStatsRaw struct {
	Proxy  *proxyCoreStats                `json:"proxy"`
	Target map[string]jsoniter.RawMessage `json:"target"`
}

type (
	XactionStatsRetriever interface {
		getStats([]XactionDetails) []byte
	}

	XactionStats struct {
		Kind        string                         `json:"kind"`
		TargetStats map[string]jsoniter.RawMessage `json:"target"`
	}

	XactionDetails struct {
		Id        int64     `json:"id"`
		StartTime time.Time `json:"startTime"`
		EndTime   time.Time `json:"endTime"`
		Status    string    `json:"status"`
	}

	RebalanceTargetStats struct {
		Xactions     []XactionDetails `json:"xactionDetails"`
		NumSentFiles int64            `json:"numSentFiles"`
		NumSentBytes int64            `json:"numSentBytes"`
		NumRecvFiles int64            `json:"numRecvFiles"`
		NumRecvBytes int64            `json:"numRecvBytes"`
	}

	RebalanceStats struct {
		Kind        string                          `json:"kind"`
		TargetStats map[string]RebalanceTargetStats `json:"target"`
	}

	PrefetchTargetStats struct {
		Xactions           []XactionDetails `json:"xactionDetails"`
		NumFilesPrefetched int64            `json:"numFilesPrefetched"`
		NumBytesPrefetched int64            `json:"numBytesPrefetched"`
	}

	PrefetchStats struct {
		Kind        string                   `json:"kind"`
		TargetStats map[string]PrefetchStats `json:"target"`
	}
)

//==================
//
// common statsTracker
//
//==================

func (stats statsTracker) register(key string, kind string) {
	cmn.Assert(kind == statsKindCounter || kind == statsKindLatency, "Invalid stats kind "+kind)
	stats[key] = &statsInstance{0, kind, 0}
}

// These stats are common to proxyCoreStats and targetCoreStats
func (stats statsTracker) registerCommonStats() {
	cmn.Assert(stats != nil, "Error attempting to register stats into nil map")

	stats.register(statGetCount, statsKindCounter)
	stats.register(statPutCount, statsKindCounter)
	stats.register(statPostCount, statsKindCounter)
	stats.register(statDeleteCount, statsKindCounter)
	stats.register(statRenameCount, statsKindCounter)
	stats.register(statListCount, statsKindCounter)
	stats.register(statGetLatency, statsKindCounter)
	stats.register(statListLatency, statsKindLatency)
	stats.register(statKeepAliveMinLatency, statsKindLatency)
	stats.register(statKeepAliveMaxLatency, statsKindLatency)
	stats.register(statKeepAliveLatency, statsKindLatency)
	stats.register(statUptimeLatency, statsKindLatency)
	stats.register(statErrCount, statsKindCounter)
	stats.register(statErrGetCount, statsKindCounter)
	stats.register(statErrDeleteCount, statsKindCounter)
	stats.register(statErrPostCount, statsKindCounter)
	stats.register(statErrPutCount, statsKindCounter)
	stats.register(statErrHeadCount, statsKindCounter)
	stats.register(statErrListCount, statsKindCounter)
	stats.register(statErrRangeCount, statsKindCounter)
}

func (p *proxyCoreStats) initStatsTracker() {
	p.Tracker = statsTracker(map[string]*statsInstance{})
	p.Tracker.registerCommonStats()
}

func (t *targetCoreStats) initStatsTracker() {
	// Call the embedded procxyCoreStats init method then register our own stats
	t.proxyCoreStats.initStatsTracker()

	t.Tracker.register(statPutLatency, statsKindLatency)
	t.Tracker.register(statGetColdCount, statsKindCounter)
	t.Tracker.register(statGetColdSize, statsKindCounter)
	t.Tracker.register(statLruEvictSize, statsKindCounter)
	t.Tracker.register(statLruEvictCount, statsKindCounter)
	t.Tracker.register(statTxCount, statsKindCounter)
	t.Tracker.register(statTxSize, statsKindCounter)
	t.Tracker.register(statRxCount, statsKindCounter)
	t.Tracker.register(statRxSize, statsKindCounter)
	t.Tracker.register(statPrefetchCount, statsKindCounter)
	t.Tracker.register(statPrefetchSize, statsKindCounter)
	t.Tracker.register(statVerChangeCount, statsKindCounter)
	t.Tracker.register(statVerChangeSize, statsKindCounter)
	t.Tracker.register(statErrCksumCount, statsKindCounter)
	t.Tracker.register(statErrCksumSize, statsKindCounter)
	t.Tracker.register(statGetRedirLatency, statsKindLatency)
	t.Tracker.register(statPutRedirLatency, statsKindLatency)
	t.Tracker.register(statRebalGlobalCount, statsKindCounter)
	t.Tracker.register(statRebalLocalCount, statsKindCounter)
	t.Tracker.register(statRebalGlobalSize, statsKindCounter)
	t.Tracker.register(statRebalLocalSize, statsKindCounter)
	t.Tracker.register(statReplPutCount, statsKindCounter)
	t.Tracker.register(statReplPutLatency, statsKindLatency)
}

func (p *proxyCoreStats) MarshalJSON() ([]byte, error) {
	return jsoniter.Marshal(p.Tracker)
}

func (p *proxyCoreStats) UnmarshalJSON(b []byte) error {
	return jsoniter.Unmarshal(b, &p.Tracker)
}

func (t *targetCoreStats) MarshalJSON() ([]byte, error) {
	return jsoniter.Marshal(t.Tracker)
}

func (t *targetCoreStats) UnmarshalJSON(b []byte) error {
	return jsoniter.Unmarshal(b, &t.Tracker)
}

func (stat *statsInstance) MarshalJSON() ([]byte, error) {
	return jsoniter.Marshal(stat.Value)
}

func (stat *statsInstance) UnmarshalJSON(b []byte) error {
	return jsoniter.Unmarshal(b, &stat.Value)
}

//==================
//
// common statsunner
//
//==================
func (r *statsrunner) runcommon(logger statslogger) error {
	r.stopCh = make(chan struct{}, 4)
	r.workCh = make(chan namedVal64, 256)
	r.starttime = time.Now()

	glog.Infof("Starting %s", r.Getname())
	ticker := time.NewTicker(ctx.config.Periodic.StatsTime)
	for {
		select {
		case nv, ok := <-r.workCh:
			if ok {
				logger.doAdd(nv)
			}
		case <-ticker.C:
			runlru := logger.log()
			logger.housekeep(runlru)
		case <-r.stopCh:
			ticker.Stop()
			return nil
		}
	}
}

func (r *statsrunner) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.Getname(), err)
	r.stopCh <- struct{}{}
	close(r.stopCh)
}

// statslogger interface impl
func (r *statsrunner) log() (runlru bool)  { return false }
func (r *statsrunner) housekeep(bool)      {}
func (r *statsrunner) doAdd(nv namedVal64) {}

func (r *statsrunner) addMany(nvs ...namedVal64) {
	for _, nv := range nvs {
		r.workCh <- nv
	}
}

func (r *statsrunner) add(name string, val int64) {
	r.workCh <- namedVal64{name, val}
}

func (r *statsrunner) addErrorHTTP(method string, val int64) {
	switch method {
	case http.MethodGet:
		r.workCh <- namedVal64{statErrGetCount, val}
	case http.MethodDelete:
		r.workCh <- namedVal64{statErrDeleteCount, val}
	case http.MethodPost:
		r.workCh <- namedVal64{statErrPostCount, val}
	case http.MethodPut:
		r.workCh <- namedVal64{statErrPutCount, val}
	case http.MethodHead:
		r.workCh <- namedVal64{statErrHeadCount, val}
	default:
		r.workCh <- namedVal64{statErrCount, val}
	}
}

//=================
//
// proxystatsrunner
//
//=================
func (r *proxystatsrunner) Run() error {
	return r.runcommon(r)
}
func (r *proxystatsrunner) init() {
	r.Core = &proxyCoreStats{}
	r.Core.initStatsTracker()
}

// statslogger interface impl
func (r *proxystatsrunner) log() (runlru bool) {
	r.Lock()
	if r.Core.logged {
		r.Unlock()
		return
	}
	for _, v := range r.Core.Tracker {
		if v.kind == statsKindLatency && v.associatedVal > 0 {
			v.Value /= v.associatedVal
		}
	}
	b, err := jsoniter.Marshal(r.Core)

	// reset all the latency stats only
	for _, v := range r.Core.Tracker {
		if v.kind == statsKindLatency {
			v.Value = 0
			v.associatedVal = 0
		}
	}
	r.Unlock()

	if err == nil {
		glog.Infoln(string(b))
		r.Core.logged = true
	}
	return
}

func (r *proxystatsrunner) doAdd(nv namedVal64) {
	r.Lock()
	s := r.Core
	s.doAdd(nv.name, nv.val)
	r.Unlock()
}

func (s *proxyCoreStats) doAdd(name string, val int64) {
	if v, ok := s.Tracker[name]; !ok {
		cmn.Assert(false, "Invalid stats name "+name)
	} else if v.kind == statsKindLatency {
		s.Tracker[name].associatedVal++
		s.statsdC.Send(name,
			metric{statsd.Counter, "count", 1},
			metric{statsd.Timer, "latency", float64(time.Duration(val) / time.Millisecond)})
		val = int64(time.Duration(val) / time.Microsecond)
	} else {
		switch name {
		case statPostCount, statDeleteCount, statRenameCount:
			s.statsdC.Send(name, metric{statsd.Counter, "count", val})
		}
	}
	s.Tracker[name].Value += val
	s.logged = false
}

//================
//
// storstatsrunner
//
//================
func newFSCapacity(statfs *syscall.Statfs_t) *fscapacity {
	return &fscapacity{
		Used:    (statfs.Blocks - statfs.Bavail) * uint64(statfs.Bsize),
		Avail:   statfs.Bavail * uint64(statfs.Bsize),
		Usedpct: uint32((statfs.Blocks - statfs.Bavail) * 100 / statfs.Blocks),
	}
}

func (r *storstatsrunner) Run() error {
	return r.runcommon(r)
}

func (r *storstatsrunner) init() {
	r.Disk = make(map[string]cmn.SimpleKVs, 8)
	r.updateCapacity()
	r.Core = &targetCoreStats{}
	r.Core.initStatsTracker()
}

func (r *storstatsrunner) log() (runlru bool) {
	r.Lock()
	if r.Core.logged {
		r.Unlock()
		return
	}
	lines := make([]string, 0, 16)
	// core stats
	for _, v := range r.Core.Tracker {
		if v.kind == statsKindLatency && v.associatedVal > 0 {
			v.Value /= v.associatedVal
		}
	}
	r.Core.Tracker[statUptimeLatency].Value = int64(time.Since(r.starttime) / time.Microsecond)

	b, err := jsoniter.Marshal(r.Core)

	// reset all the latency stats only
	for _, v := range r.Core.Tracker {
		if v.kind == statsKindLatency {
			v.Value = 0
			v.associatedVal = 0
		}
	}
	if err == nil {
		lines = append(lines, string(b))
	}
	// capacity
	if time.Since(r.timeUpdatedCapacity) >= ctx.config.LRU.CapacityUpdTime {
		runlru = r.updateCapacity()
		r.timeUpdatedCapacity = time.Now()
		for mpath, fsCapacity := range r.Capacity {
			b, err := jsoniter.Marshal(fsCapacity)
			if err == nil {
				lines = append(lines, mpath+": "+string(b))
			}
		}
	}

	// disk
	riostat := getiostatrunner()
	riostat.RLock()
	r.CPUidle = riostat.CPUidle
	for dev, iometrics := range riostat.Disk {
		r.Disk[dev] = iometrics
		if riostat.IsZeroUtil(dev) {
			continue // skip zeros
		}
		b, err := jsoniter.Marshal(r.Disk[dev])
		if err == nil {
			lines = append(lines, dev+": "+string(b))
		}

		stats := make([]metric, len(iometrics))
		idx := 0
		for k, v := range iometrics {
			stats[idx] = metric{statsd.Gauge, k, v}
			idx++
		}
		gettarget().statsdC.Send("iostat_"+dev, stats...)
	}
	riostat.RUnlock()

	lines = append(lines, fmt.Sprintf("CPU idle: %s%%", r.CPUidle))

	r.Core.logged = true
	r.Unlock()

	// log
	for _, ln := range lines {
		glog.Infoln(ln)
	}
	return
}

func (r *storstatsrunner) housekeep(runlru bool) {
	t := gettarget()

	if runlru && ctx.config.LRU.LRUEnabled {
		go t.runLRU()
	}

	// Run prefetch operation if there are items to be prefetched
	if len(t.prefetchQueue) > 0 {
		go t.doPrefetch()
	}

	// keep total log size below the configured max
	if time.Since(r.timeCheckedLogSizes) >= logsTotalSizeCheckTime {
		go r.removeLogs(ctx.config.Log.MaxTotal)
		r.timeCheckedLogSizes = time.Now()
	}
}

func (r *storstatsrunner) removeLogs(maxtotal uint64) {
	logfinfos, err := ioutil.ReadDir(ctx.config.Log.Dir)
	if err != nil {
		glog.Errorf("GC logs: cannot read log dir %s, err: %v", ctx.config.Log.Dir, err)
		return // ignore error
	}
	// sample name dfc.ip-10-0-2-19.root.log.INFO.20180404-031540.2249
	var logtypes = []string{".INFO.", ".WARNING.", ".ERROR."}
	for _, logtype := range logtypes {
		var (
			tot   = int64(0)
			infos = make([]os.FileInfo, 0, len(logfinfos))
		)
		for _, logfi := range logfinfos {
			if logfi.IsDir() {
				continue
			}
			if !strings.Contains(logfi.Name(), ".log.") {
				continue
			}
			if strings.Contains(logfi.Name(), logtype) {
				tot += logfi.Size()
				infos = append(infos, logfi)
			}
		}
		if tot > int64(maxtotal) {
			if len(infos) <= 1 {
				glog.Errorf("GC logs: %s, total %d for type %s, max %d", ctx.config.Log.Dir, tot, logtype, maxtotal)
				continue
			}
			r.removeOlderLogs(tot, int64(maxtotal), infos)
		}
	}
}

func (r *storstatsrunner) removeOlderLogs(tot, maxtotal int64, filteredInfos []os.FileInfo) {
	fiLess := func(i, j int) bool {
		return filteredInfos[i].ModTime().Before(filteredInfos[j].ModTime())
	}
	if glog.V(3) {
		glog.Infof("GC logs: started")
	}
	sort.Slice(filteredInfos, fiLess)
	for _, logfi := range filteredInfos[:len(filteredInfos)-1] { // except last = current
		logfqn := ctx.config.Log.Dir + "/" + logfi.Name()
		if err := os.Remove(logfqn); err == nil {
			tot -= logfi.Size()
			glog.Infof("GC logs: removed %s", logfqn)
			if tot < maxtotal {
				break
			}
		} else {
			glog.Errorf("GC logs: failed to remove %s", logfqn)
		}
	}
	if glog.V(3) {
		glog.Infof("GC logs: done")
	}
}

func (r *storstatsrunner) updateCapacity() (runlru bool) {
	availableMountpaths, _ := fs.Mountpaths.Get()
	capacities := make(map[string]*fscapacity, len(availableMountpaths))

	for mpath := range availableMountpaths {
		statfs := &syscall.Statfs_t{}
		if err := syscall.Statfs(mpath, statfs); err != nil {
			glog.Errorf("Failed to statfs mp %q, err: %v", mpath, err)
			continue
		}
		fsCap := newFSCapacity(statfs)
		capacities[mpath] = fsCap
		if fsCap.Usedpct >= ctx.config.LRU.HighWM {
			runlru = true
		}
	}

	r.Capacity = capacities
	return
}

func (r *storstatsrunner) doAdd(nv namedVal64) {
	r.Lock()
	s := r.Core
	s.doAdd(nv.name, nv.val)
	r.Unlock()
}

func (s *targetCoreStats) doAdd(name string, val int64) {
	if _, ok := s.Tracker[name]; !ok {
		cmn.Assert(false, "Invalid stats name "+name)
	}

	switch name {
	// common
	case statGetCount, statPutCount, statPostCount, statDeleteCount, statRenameCount, statListCount,
		statGetLatency, statPutLatency, statListLatency,
		statKeepAliveLatency, statKeepAliveMinLatency, statKeepAliveMaxLatency,
		statErrCount, statErrGetCount, statErrDeleteCount, statErrPostCount,
		statErrPutCount, statErrHeadCount, statErrListCount, statErrRangeCount:
		s.proxyCoreStats.doAdd(name, val)
		return
	// target only
	case statGetColdSize:
		s.statsdC.Send("get.cold",
			metric{statsd.Counter, "count", 1},
			metric{statsd.Counter, "get.cold.size", val})
	case statVerChangeSize:
		s.statsdC.Send("get.cold",
			metric{statsd.Counter, "vchanged", 1},
			metric{statsd.Counter, "vchange.size", val})
	case statLruEvictSize, statTxSize, statRxSize, statErrCksumSize: // byte stats
		s.statsdC.Send(name, metric{statsd.Counter, "bytes", val})
	case statLruEvictCount, statTxCount, statRxCount: // files stats
		s.statsdC.Send(name, metric{statsd.Counter, "files", val})
	case statErrCksumCount: // counter stats
		s.statsdC.Send(name, metric{statsd.Counter, "count", val})
	case statGetRedirLatency, statPutRedirLatency: // latency stats
		s.Tracker[name].associatedVal++
		s.statsdC.Send(name,
			metric{statsd.Counter, "count", 1},
			metric{statsd.Timer, "latency", float64(time.Duration(val) / time.Millisecond)})
		val = int64(time.Duration(val) / time.Microsecond)
	}
	s.Tracker[name].Value += val
	s.logged = false
}

func (p PrefetchTargetStats) getStats(allXactionDetails []XactionDetails) []byte {
	rstor := getstorstatsrunner()
	rstor.RLock()
	prefetchXactionStats := PrefetchTargetStats{
		Xactions:           allXactionDetails,
		NumBytesPrefetched: rstor.Core.Tracker[statPrefetchCount].Value,
		NumFilesPrefetched: rstor.Core.Tracker[statPrefetchSize].Value,
	}
	rstor.RUnlock()
	jsonBytes, err := jsoniter.Marshal(prefetchXactionStats)
	cmn.Assert(err == nil, err)
	return jsonBytes
}

func (r RebalanceTargetStats) getStats(allXactionDetails []XactionDetails) []byte {
	rstor := getstorstatsrunner()
	rstor.RLock()
	rebalanceXactionStats := RebalanceTargetStats{
		Xactions:     allXactionDetails,
		NumRecvBytes: rstor.Core.Tracker[statRxSize].Value,
		NumRecvFiles: rstor.Core.Tracker[statRxCount].Value,
		NumSentBytes: rstor.Core.Tracker[statTxSize].Value,
		NumSentFiles: rstor.Core.Tracker[statTxCount].Value,
	}
	rstor.RUnlock()
	jsonBytes, err := jsoniter.Marshal(rebalanceXactionStats)
	cmn.Assert(err == nil, err)
	return jsonBytes
}
