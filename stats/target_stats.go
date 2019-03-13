// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"strings"
	"syscall"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/stats/statsd"
	jsoniter "github.com/json-iterator/go"
)

//
// NOTE Naming Convention: "*.n" - counter, "*.µs" - latency, "*.size" - size (in bytes), "*.bps" - throughput (in byte/s)
//
const (
	// KindCounter - QPS and byte counts (always incremented, never reset)
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
	ErrMetadataCount = "err.md.n"
	RebGlobalCount   = "reb.global.n"
	RebLocalCount    = "reb.local.n"
	RebGlobalSize    = "reb.global.size"
	RebLocalSize     = "reb.local.size"
	ReplPutCount     = "repl.n"
	DownloadSize     = "dl.size"

	// KindLatency
	PutLatency      = "put.µs"
	GetRedirLatency = "get.redir.µs"
	PutRedirLatency = "put.redir.µs"
	ReplPutLatency  = "repl.µs"
	DownloadLatency = "dl.µs"

	// KindThroughput
	GetThroughput = "get.bps" // bytes per second
)

//
// public type
//
type (
	Trunner struct {
		statsRunner
		T        cluster.Target         `json:"-"`
		Riostat  *ios.IostatRunner      `json:"-"`
		Core     *CoreStats             `json:"core"`
		Capacity map[string]*fscapacity `json:"capacity"`
		// inner state
		timecounts struct {
			capLimit, capIdx int64 // update capacity: time interval counting
		}
		lines []string
	}
	copyRunner struct {
		Tracker  copyTracker            `json:"core"`
		Capacity map[string]*fscapacity `json:"capacity"`
	}
)

//
// private types
//
type (
	fscapacity struct {
		Used    uint64 `json:"used"`    // bytes
		Avail   uint64 `json:"avail"`   // ditto
		Usedpct int64  `json:"usedpct"` // redundant ok
	}
)

//
// Trunner
//

func (r *Trunner) Register(name string, kind string) { r.Core.Tracker.register(name, kind) }
func (r *Trunner) Run() error                        { return r.runcommon(r) }

func (r *Trunner) Init(daemonStr, daemonID string) {
	r.Core = &CoreStats{}
	r.Core.init(48) // and register common stats (target's own stats are registered elsewhere via the Register() above)
	r.Core.initStatsD(daemonStr, daemonID)

	r.ctracker = make(copyTracker, 48) // these two are allocated once and only used in serial context
	r.lines = make([]string, 0, 16)

	config := cmn.GCO.Get()
	r.Core.statsTime = config.Periodic.StatsTime
	r.timecounts.capLimit = cmn.DivCeil(int64(config.LRU.CapacityUpdTime), int64(config.Periodic.StatsTime))
	r.statsRunner.logLimit = cmn.DivCeil(int64(logsMaxSizeCheckTime), int64(config.Periodic.StatsTime))
	// subscribe to config changes
	cmn.GCO.Subscribe(r)
}

func (r *Trunner) ConfigUpdate(oldConf, newConf *cmn.Config) {
	r.statsRunner.ConfigUpdate(oldConf, newConf)
	r.Core.statsTime = newConf.Periodic.StatsTime
	r.timecounts.capLimit = cmn.DivCeil(int64(newConf.LRU.CapacityUpdTime), int64(newConf.Periodic.StatsTime))
}

func (r *Trunner) GetWhatStats() ([]byte, error) {
	ctracker := make(copyTracker, 48)
	r.Core.copyCumulative(ctracker)

	crunner := &copyRunner{Tracker: ctracker, Capacity: r.Capacity}
	return jsonCompat.Marshal(crunner)
}

func (r *Trunner) log() (runlru bool) {
	// copy stats values while skipping zeros; reset latency stats
	r.Core.Tracker[Uptime].Value = int64(time.Since(r.starttime) / time.Microsecond)
	r.Core.copyZeroReset(r.ctracker)

	r.lines = r.lines[:0]
	b, err := jsonCompat.Marshal(r.ctracker)
	if err == nil {
		r.lines = append(r.lines, string(b))
	}

	// 2. capacity
	r.timecounts.capIdx++
	if r.timecounts.capIdx >= r.timecounts.capLimit {
		runlru = r.UpdateCapacityOOS()
		r.timecounts.capIdx = 0
		for mpath, fsCapacity := range r.Capacity {
			b, err := jsoniter.Marshal(fsCapacity)
			if err == nil {
				r.lines = append(r.lines, mpath+": "+string(b))
			}
		}
	}

	// 3. log
	for _, ln := range r.lines {
		glog.Infoln(ln)
	}
	return
}

func (r *Trunner) housekeep(runlru bool) {
	var config = cmn.GCO.Get()
	if runlru && config.LRU.Enabled {
		go r.T.RunLRU()
	}

	// Run prefetch operation if there are items to be prefetched
	if r.T.PrefetchQueueLen() > 0 {
		go r.T.Prefetch()
	}

	r.statsRunner.housekeep(runlru)
}

func (r *Trunner) UpdateCapacityOOS() (runlru bool) {
	var (
		avgUsed                int64
		config                 = cmn.GCO.Get()
		availableMountpaths, _ = fs.Mountpaths.Get()
		l                      = len(availableMountpaths)
		oos                    = r.T.OOS()
	)
	if l == 0 {
		glog.Errorln("UpdateCapacity: " + cmn.NoMountpaths)
		return
	}
	capacities := make(map[string]*fscapacity, len(availableMountpaths))
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
		avgUsed += fsCap.Usedpct
	}
	r.Capacity = capacities
	// handle out-of-space
	avgUsed /= int64(l)
	if oos && avgUsed < config.LRU.HighWM {
		r.T.OOS(false)
		r.timecounts.capLimit = cmn.DivCeil(int64(config.LRU.CapacityUpdTime), int64(config.Periodic.StatsTime))
		glog.Infof("OOS resolved: avg used = %d%% < (hwm %d%%) across %d mountpath(s)",
			avgUsed, config.LRU.HighWM, l)
		t := time.Duration(r.timecounts.capLimit) * config.Periodic.StatsTime
		glog.Infof("PUTs are allowed to proceed, next capacity check in %v", t)
	} else if !oos && avgUsed > config.LRU.OOS {
		r.T.OOS(true)
		r.timecounts.capLimit = cmn.MinI64(r.timecounts.capLimit, 2)
		glog.Warningf("OOS: avg used = %d%% > (oos %d%%) across %d mountpath(s)", avgUsed, config.LRU.OOS, l)
		t := time.Duration(r.timecounts.capLimit) * config.Periodic.StatsTime
		glog.Warningf("OOS: disallowing new PUTs and checking capacity every %v", t)
	}
	return
}

// NOTE the naming conventions (above)
func (r *Trunner) doAdd(nv NamedVal64) {
	var (
		s    = r.Core
		name = nv.Name
		val  = nv.Val
	)

	v, ok := s.Tracker[name]
	cmn.AssertMsg(ok, "Invalid stats name '"+name+"'")

	// most target stats can be handled by CoreStats.doAdd
	// stats that track data IO are unique to target and are handled here
	// .size stats, as of 2.x and beyond, is one of them
	if !strings.HasSuffix(name, ".size") {
		s.doAdd(name, val)
		return
	}

	// target only suffix
	nroot := strings.TrimSuffix(name, ".size")
	metricType := statsd.Counter

	if nroot == "dl" {
		metricType = statsd.PersistentCounter
	}

	s.statsdC.Send(nroot,
		metric{Type: metricType, Name: "bytes", Value: val},
		metric{Type: metricType, Name: "count", Value: 1},
	)

	v.Lock()
	v.Value += val
	v.Unlock()
}

//
// xaction
//

func (r *Trunner) GetPrefetchStats(allXactionDetails []XactionDetails) []byte {
	v := r.Core.Tracker[PrefetchCount]
	v.RLock()
	prefetchXactionStats := PrefetchTargetStats{
		Xactions:           allXactionDetails,
		NumBytesPrefetched: r.Core.Tracker[PrefetchCount].Value,
		NumFilesPrefetched: r.Core.Tracker[PrefetchSize].Value,
	}
	v.RUnlock()
	jsonBytes, err := jsoniter.Marshal(prefetchXactionStats)
	cmn.AssertNoErr(err)
	return jsonBytes
}

func (r *Trunner) GetRebalanceStats(allXactionDetails []XactionDetails) []byte {
	vr := r.Core.Tracker[RxCount]
	vt := r.Core.Tracker[TxCount]
	vr.RLock()
	vt.RLock()
	rebalanceXactionStats := RebalanceTargetStats{
		Xactions:     allXactionDetails,
		NumRecvBytes: r.Core.Tracker[RxSize].Value,
		NumRecvFiles: r.Core.Tracker[RxCount].Value,
		NumSentBytes: r.Core.Tracker[TxSize].Value,
		NumSentFiles: r.Core.Tracker[TxCount].Value,
	}
	vt.RUnlock()
	vr.RUnlock()
	jsonBytes, err := jsoniter.Marshal(rebalanceXactionStats)
	cmn.AssertNoErr(err)
	return jsonBytes
}

//
// misc
//

func newFSCapacity(statfs *syscall.Statfs_t) *fscapacity {
	pct := (statfs.Blocks - statfs.Bavail) * 100 / statfs.Blocks
	return &fscapacity{
		Used:    (statfs.Blocks - statfs.Bavail) * uint64(statfs.Bsize),
		Avail:   statfs.Bavail * uint64(statfs.Bsize),
		Usedpct: int64(pct),
	}
}
