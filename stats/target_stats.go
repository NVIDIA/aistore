/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cluster"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/ios"
	"github.com/NVIDIA/dfcpub/stats/statsd"
	jsoniter "github.com/json-iterator/go"
)

const (
	PutLatency       = "put.μs"
	GetColdCount     = "get.cold.n"
	GetColdSize      = "get.cold.size"
	LruEvictSize     = "lru.evict.size"
	LruEvictCount    = "lru.evict.n"
	TxCount          = "tx.n"
	TxSize           = "tx.size"
	RxCount          = "rx.n"
	RxSize           = "rx.size"
	PrefetchCount    = "pre.n"
	PrefetchSize     = "pre.size"
	VerChangeCount   = "vchange.n"
	VerChangeSize    = "vchange.size"
	ErrCksumCount    = "err.cksum.n"
	ErrCksumSize     = "err.cksum.size"
	GetRedirLatency  = "get.redir.μs"
	PutRedirLatency  = "put.redir.μs"
	RebalGlobalCount = "reb.global.n"
	RebalLocalCount  = "reb.local.n"
	RebalGlobalSize  = "reb.global.size"
	RebalLocalSize   = "reb.local.size"
	ReplPutCount     = "replication.put.n"
	ReplPutLatency   = "replication.put.µs"
)

type (
	fscapacity struct {
		Used    uint64 `json:"used"`    // bytes
		Avail   uint64 `json:"avail"`   // ditto
		Usedpct int64  `json:"usedpct"` // reduntant ok
	}
	targetCoreStats struct {
		ProxyCoreStats
	}
	Trunner struct {
		statsrunner
		TargetRunner cluster.Target
		Riostat      *ios.IostatRunner
		Core         *targetCoreStats       `json:"core"`
		Capacity     map[string]*fscapacity `json:"capacity"`
		// iostat
		CPUidle string                   `json:"cpuidle"`
		Disk    map[string]cmn.SimpleKVs `json:"disk"`
		// omitempty
		timeUpdatedCapacity time.Time
		timeCheckedLogSizes time.Time
		fsmap               map[syscall.Fsid]string
	}
)

//
// targetCoreStats
//

func (t *targetCoreStats) initStatsTracker() {
	t.ProxyCoreStats.initStatsTracker()

	t.Tracker.register(PutLatency, statsKindLatency)
	t.Tracker.register(GetColdCount, statsKindCounter)
	t.Tracker.register(GetColdSize, statsKindCounter)
	t.Tracker.register(LruEvictSize, statsKindCounter)
	t.Tracker.register(LruEvictCount, statsKindCounter)
	t.Tracker.register(TxCount, statsKindCounter)
	t.Tracker.register(TxSize, statsKindCounter)
	t.Tracker.register(RxCount, statsKindCounter)
	t.Tracker.register(RxSize, statsKindCounter)
	t.Tracker.register(PrefetchCount, statsKindCounter)
	t.Tracker.register(PrefetchSize, statsKindCounter)
	t.Tracker.register(VerChangeCount, statsKindCounter)
	t.Tracker.register(VerChangeSize, statsKindCounter)
	t.Tracker.register(ErrCksumCount, statsKindCounter)
	t.Tracker.register(ErrCksumSize, statsKindCounter)
	t.Tracker.register(GetRedirLatency, statsKindLatency)
	t.Tracker.register(PutRedirLatency, statsKindLatency)
	t.Tracker.register(RebalGlobalCount, statsKindCounter)
	t.Tracker.register(RebalLocalCount, statsKindCounter)
	t.Tracker.register(RebalGlobalSize, statsKindCounter)
	t.Tracker.register(RebalLocalSize, statsKindCounter)
	t.Tracker.register(ReplPutCount, statsKindCounter)
	t.Tracker.register(ReplPutLatency, statsKindLatency)
}

func (t *targetCoreStats) doAdd(name string, val int64) {
	if _, ok := t.Tracker[name]; !ok {
		cmn.Assert(false, "Invalid stats name "+name)
	}

	switch name {
	// common
	case GetCount, PutCount, PostCount, DeleteCount, RenameCount, ListCount,
		GetLatency, PutLatency, ListLatency,
		KeepAliveLatency, KeepAliveMinLatency, KeepAliveMaxLatency,
		ErrCount, ErrGetCount, ErrDeleteCount, ErrPostCount,
		ErrPutCount, ErrHeadCount, ErrListCount, ErrRangeCount:
		t.ProxyCoreStats.doAdd(name, val)
		return
	// target only
	case GetColdSize:
		t.StatsdC.Send("get.cold",
			metric{statsd.Counter, "count", 1},
			metric{statsd.Counter, "get.cold.size", val})
	case VerChangeSize:
		t.StatsdC.Send("get.cold",
			metric{statsd.Counter, "vchanged", 1},
			metric{statsd.Counter, "vchange.size", val})
	case LruEvictSize, TxSize, RxSize, ErrCksumSize: // byte stats
		t.StatsdC.Send(name, metric{statsd.Counter, "bytes", val})
	case LruEvictCount, TxCount, RxCount: // files stats
		t.StatsdC.Send(name, metric{statsd.Counter, "files", val})
	case ErrCksumCount: // counter stats
		t.StatsdC.Send(name, metric{statsd.Counter, "count", val})
	case GetRedirLatency, PutRedirLatency: // latency stats
		t.Tracker[name].associatedVal++
		t.StatsdC.Send(name,
			metric{statsd.Counter, "count", 1},
			metric{statsd.Timer, "latency", float64(time.Duration(val) / time.Millisecond)})
		val = int64(time.Duration(val) / time.Microsecond)
	}
	t.Tracker[name].Value += val
	t.logged = false
}

func (t *targetCoreStats) MarshalJSON() ([]byte, error) {
	return jsoniter.Marshal(t.Tracker)
}

func (t *targetCoreStats) UnmarshalJSON(b []byte) error {
	return jsoniter.Unmarshal(b, &t.Tracker)
}

//
// Trunner
//

func newFSCapacity(statfs *syscall.Statfs_t) *fscapacity {
	pct := (statfs.Blocks - statfs.Bavail) * 100 / statfs.Blocks
	return &fscapacity{
		Used:    (statfs.Blocks - statfs.Bavail) * uint64(statfs.Bsize),
		Avail:   statfs.Bavail * uint64(statfs.Bsize),
		Usedpct: int64(pct),
	}
}

func (r *Trunner) Run() error {
	return r.runcommon(r)
}

func (r *Trunner) Init() {
	r.Disk = make(map[string]cmn.SimpleKVs, 8)
	r.UpdateCapacity()
	r.Core = &targetCoreStats{}
	r.Core.initStatsTracker()
}

func (r *Trunner) log() (runlru bool) {
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
	r.Core.Tracker[Uptime].Value = int64(time.Since(r.starttime) / time.Microsecond)

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
	config := r.Getconf()
	if time.Since(r.timeUpdatedCapacity) >= config.LRU.CapacityUpdTime {
		runlru = r.UpdateCapacity()
		r.timeUpdatedCapacity = time.Now()
		for mpath, fsCapacity := range r.Capacity {
			b, err := jsoniter.Marshal(fsCapacity)
			if err == nil {
				lines = append(lines, mpath+": "+string(b))
			}
		}
	}

	// disk
	r.Riostat.RLock()
	r.CPUidle = r.Riostat.CPUidle
	for dev, iometrics := range r.Riostat.Disk {
		r.Disk[dev] = iometrics
		if r.Riostat.IsZeroUtil(dev) {
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
		r.Core.StatsdC.Send("iostat_"+dev, stats...)
	}
	r.Riostat.RUnlock()

	lines = append(lines, fmt.Sprintf("CPU idle: %s%%", r.CPUidle))

	r.Core.logged = true
	r.Unlock()

	// log
	for _, ln := range lines {
		glog.Infoln(ln)
	}
	return
}

func (r *Trunner) housekeep(runlru bool) {
	var (
		t      = r.TargetRunner
		config = r.Getconf()
	)
	if runlru && config.LRU.LRUEnabled {
		go t.RunLRU()
	}

	// Run prefetch operation if there are items to be prefetched
	if t.PrefetchQueueLen() > 0 {
		go t.Prefetch()
	}

	// keep total log size below the configured max
	if time.Since(r.timeCheckedLogSizes) >= logsTotalSizeCheckTime {
		go r.removeLogs(config.Log.MaxTotal)
		r.timeCheckedLogSizes = time.Now()
	}
}

func (r *Trunner) removeLogs(maxtotal uint64) {
	config := r.Getconf()
	logfinfos, err := ioutil.ReadDir(config.Log.Dir)
	if err != nil {
		glog.Errorf("GC logs: cannot read log dir %s, err: %v", config.Log.Dir, err)
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
				glog.Errorf("GC logs: %s, total %d for type %s, max %d", config.Log.Dir, tot, logtype, maxtotal)
				continue
			}
			r.removeOlderLogs(tot, int64(maxtotal), infos)
		}
	}
}

func (r *Trunner) removeOlderLogs(tot, maxtotal int64, filteredInfos []os.FileInfo) {
	fiLess := func(i, j int) bool {
		return filteredInfos[i].ModTime().Before(filteredInfos[j].ModTime())
	}
	if glog.V(3) {
		glog.Infof("GC logs: started")
	}
	sort.Slice(filteredInfos, fiLess)
	config := r.Getconf()
	for _, logfi := range filteredInfos[:len(filteredInfos)-1] { // except last = current
		logfqn := config.Log.Dir + "/" + logfi.Name()
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

func (r *Trunner) UpdateCapacity() (runlru bool) {
	availableMountpaths, _ := fs.Mountpaths.Get()
	capacities := make(map[string]*fscapacity, len(availableMountpaths))
	config := r.Getconf()
	for mpath := range availableMountpaths {
		statfs := &syscall.Statfs_t{}
		if err := syscall.Statfs(mpath, statfs); err != nil {
			glog.Errorf("Failed to statfs mp %q, err: %v", mpath, err)
			continue
		}
		fsCap := newFSCapacity(statfs)
		capacities[mpath] = fsCap
		if fsCap.Usedpct >= config.LRU.HighWM {
			runlru = true
		}
	}

	r.Capacity = capacities
	return
}

func (r *Trunner) doAdd(nv NamedVal64) {
	r.Lock()
	s := r.Core
	s.doAdd(nv.Name, nv.Val)
	r.Unlock()
}

//
// xaction
//

func (r *Trunner) GetPrefetchStats(allXactionDetails []XactionDetails) []byte {
	r.RLock()
	prefetchXactionStats := PrefetchTargetStats{
		Xactions:           allXactionDetails,
		NumBytesPrefetched: r.Core.Tracker[PrefetchCount].Value,
		NumFilesPrefetched: r.Core.Tracker[PrefetchSize].Value,
	}
	r.RUnlock()
	jsonBytes, err := jsoniter.Marshal(prefetchXactionStats)
	cmn.Assert(err == nil, err)
	return jsonBytes
}

func (r *Trunner) GetRebalanceStats(allXactionDetails []XactionDetails) []byte {
	r.RLock()
	rebalanceXactionStats := RebalanceTargetStats{
		Xactions:     allXactionDetails,
		NumRecvBytes: r.Core.Tracker[RxSize].Value,
		NumRecvFiles: r.Core.Tracker[RxCount].Value,
		NumSentBytes: r.Core.Tracker[TxSize].Value,
		NumSentFiles: r.Core.Tracker[TxCount].Value,
	}
	r.RUnlock()
	jsonBytes, err := jsoniter.Marshal(rebalanceXactionStats)
	cmn.Assert(err == nil, err)
	return jsonBytes
}
