// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
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
	"github.com/NVIDIA/dfcpub/common"
	"github.com/NVIDIA/dfcpub/dfc/statsd"
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
	namedrunner
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
	CPUidle string                      `json:"cpuidle"`
	Disk    map[string]common.SimpleKVs `json:"disk"`
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

type iostatrunner struct {
	sync.RWMutex
	namedrunner
	stopCh      chan struct{}
	CPUidle     string
	metricnames []string
	Disk        map[string]common.SimpleKVs
	process     *os.Process // running iostat process. Required so it can be killed later
	fsdisks     map[string]common.StringSet
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
	common.Assert(kind == statsKindCounter || kind == statsKindLatency, "Invalid stats kind "+kind)
	stats[key] = &statsInstance{0, kind, 0}
}

// These stats are common to proxyCoreStats and targetCoreStats
func (stats statsTracker) registerCommonStats() {
	common.Assert(stats != nil, "Error attempting to register stats into nil map")

	stats.register("get.n", statsKindCounter)
	stats.register("put.n", statsKindCounter)
	stats.register("pst.n", statsKindCounter)
	stats.register("del.n", statsKindCounter)
	stats.register("ren.n", statsKindCounter)
	stats.register("lst.n", statsKindCounter)
	stats.register("put.μs", statsKindLatency)
	stats.register("lst.μs", statsKindLatency)
	stats.register("kalive.μs.min", statsKindLatency)
	stats.register("kalive.μs.max", statsKindLatency)
	stats.register("kalive.μs", statsKindLatency)
	stats.register("uptime.μs", statsKindLatency)
	stats.register("err.n", statsKindCounter)
	stats.register("err.get.n", statsKindCounter)
	stats.register("err.delete.n", statsKindCounter)
	stats.register("err.post.n", statsKindCounter)
	stats.register("err.put.n", statsKindCounter)
	stats.register("err.head.n", statsKindCounter)
	stats.register("err.list.n", statsKindCounter)
	stats.register("err.range.n", statsKindCounter)
}

func (p *proxyCoreStats) initStatsTracker() {
	p.Tracker = statsTracker(map[string]*statsInstance{})
	p.Tracker.registerCommonStats()
}

func (t *targetCoreStats) initStatsTracker() {
	// Call the embedded procxyCoreStats init method then register our own stats
	t.proxyCoreStats.initStatsTracker()

	t.Tracker.register("put.μs", statsKindLatency)
	t.Tracker.register("get.cold.n", statsKindCounter)
	t.Tracker.register("get.cold.size", statsKindCounter)
	t.Tracker.register("lru.evict.size", statsKindCounter)
	t.Tracker.register("lru.evict.n", statsKindCounter)
	t.Tracker.register("tx.n", statsKindCounter)
	t.Tracker.register("tx.size", statsKindCounter)
	t.Tracker.register("rx.n", statsKindCounter)
	t.Tracker.register("rx.size", statsKindCounter)
	t.Tracker.register("pre.n", statsKindCounter)
	t.Tracker.register("pre.size", statsKindCounter)
	t.Tracker.register("vchange.n", statsKindCounter)
	t.Tracker.register("vchange.size", statsKindCounter)
	t.Tracker.register("err.cksum.n", statsKindCounter)
	t.Tracker.register("err.cksum.size", statsKindCounter)
	t.Tracker.register("get.redir.μs", statsKindLatency)
	t.Tracker.register("put.redir.μs", statsKindLatency)
	t.Tracker.register("reb.global.n", statsKindCounter)
	t.Tracker.register("reb.local.n", statsKindCounter)
	t.Tracker.register("reb.global.size", statsKindCounter)
	t.Tracker.register("reb.local.size", statsKindCounter)
	t.Tracker.register("replication.put.n", statsKindCounter)
	t.Tracker.register("replication.put.µs", statsKindLatency)
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

	glog.Infof("Starting %s", r.name)
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

func (r *statsrunner) stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.name, err)
	var v struct{}
	r.stopCh <- v
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
		r.workCh <- namedVal64{"err.get.n", val}
	case http.MethodDelete:
		r.workCh <- namedVal64{"err.del.n", val}
	case http.MethodPost:
		r.workCh <- namedVal64{"err.post.n", val}
	case http.MethodPut:
		r.workCh <- namedVal64{"err.put.n", val}
	case http.MethodHead:
		r.workCh <- namedVal64{"err.head.n", val}
	default:
		r.workCh <- namedVal64{"err.n", val}
	}
}

//=================
//
// proxystatsrunner
//
//=================
func (r *proxystatsrunner) run() error {
	r.Core = &proxyCoreStats{}
	r.Core.initStatsTracker()
	return r.runcommon(r)
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
		common.Assert(false, "Invalid stats name "+name)
	} else if v.kind == statsKindLatency {
		s.Tracker[name].associatedVal++
		s.statsdC.Send(name,
			metric{statsd.Counter, "count", 1},
			metric{statsd.Timer, "latency", float64(time.Duration(val) / time.Millisecond)})
		val = int64(time.Duration(val) / time.Microsecond)
	} else {
		switch name {
		case "pst.n", "del.n", "ren.n":
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

func (r *storstatsrunner) run() error {
	r.init()
	return r.runcommon(r)
}

func (r *storstatsrunner) init() {
	r.Disk = make(map[string]common.SimpleKVs, 8)
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
	r.Core.Tracker["uptime.μs"].Value = int64(time.Since(r.starttime) / time.Microsecond)

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
		if riostat.isZeroUtil(dev) {
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
	availableMountpaths, _ := ctx.mountpaths.Mountpaths()
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
		common.Assert(false, "Invalid stats name "+name)
	}

	switch name {
	// common
	case "get.n", "put.n", "pst.n", "del.n", "ren.n", "lst.n",
		"get.μs", "put.μs", "lst.μs",
		"kalive.μs", "kalive.μs.min", "kalive.μs.max",
		"err.n", "err.get.n", "err.delete.n", "err.post.n",
		"err.put.n", "err.head.n", "err.list.n", "err.range.n":
		s.proxyCoreStats.doAdd(name, val)
		return
	// target only
	case "get.cold.size":
		s.statsdC.Send("get.cold",
			metric{statsd.Counter, "count", 1},
			metric{statsd.Counter, "get.cold.size", val})
	case "vchange.size":
		s.statsdC.Send("get.cold",
			metric{statsd.Counter, "vchanged", 1},
			metric{statsd.Counter, "vchange.size", val})
	case "lru.evict.size", "tx.size", "rx.size", "err.cksum.size": // byte stats
		s.statsdC.Send(name, metric{statsd.Counter, "bytes", val})
	case "lru.evict.n", "tx.n", "rx.n": // files stats
		s.statsdC.Send(name, metric{statsd.Counter, "files", val})
	case "err.cksum.n": // counter stats
		s.statsdC.Send(name, metric{statsd.Counter, "count", val})
	case "get.redir.μs", "put.redir.μs": // latency stats
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
		NumBytesPrefetched: rstor.Core.Tracker["pre.n"].Value,
		NumFilesPrefetched: rstor.Core.Tracker["pre.size"].Value,
	}
	rstor.RUnlock()
	jsonBytes, err := jsoniter.Marshal(prefetchXactionStats)
	common.Assert(err == nil, err)
	return jsonBytes
}

func (r RebalanceTargetStats) getStats(allXactionDetails []XactionDetails) []byte {
	rstor := getstorstatsrunner()
	rstor.RLock()
	rebalanceXactionStats := RebalanceTargetStats{
		Xactions:     allXactionDetails,
		NumRecvBytes: rstor.Core.Tracker["rx.size"].Value,
		NumRecvFiles: rstor.Core.Tracker["rx.n"].Value,
		NumSentBytes: rstor.Core.Tracker["tx.size"].Value,
		NumSentFiles: rstor.Core.Tracker["tx.n"].Value,
	}
	rstor.RUnlock()
	jsonBytes, err := jsoniter.Marshal(rebalanceXactionStats)
	common.Assert(err == nil, err)
	return jsonBytes
}

func getToEvict(mpath string, hwm uint32, lwm uint32) (int64, error) {
	blocks, bavail, bsize, err := getFSStats(mpath)
	if err != nil {
		return -1, err
	}
	used := blocks - bavail
	usedpct := used * 100 / blocks
	glog.Infof("Blocks %d Bavail %d used %d%% hwm %d%% lwm %d%%", blocks, bavail, usedpct, hwm, lwm)
	if usedpct < uint64(hwm) {
		return 0, nil // 0 to evict
	}
	lwmblocks := blocks * uint64(lwm) / 100
	return int64(used-lwmblocks) * bsize, nil
}

func getFSUsedPercentage(path string) (usedPercentage uint64, ok bool) {
	totalBlocks, blocksAvailable, _, err := getFSStats(path)
	if err != nil {
		return
	}
	usedBlocks := totalBlocks - blocksAvailable
	return usedBlocks * 100 / totalBlocks, true
}
