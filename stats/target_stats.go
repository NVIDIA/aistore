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

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats/statsd"
)

//
// NOTE Naming Convention: "*.n" - counter, "*.µs" - latency, "*.size" - size (in bytes),
// "*.bps" - throughput (in byte/s), "*.id" - ID
//
const (
	// KindCounter - QPS and byte counts (always incremented, never reset)
	GetColdCount     = "get.cold.n"
	GetColdSize      = "get.cold.size"
	LruEvictSize     = "lru.evict.size"
	LruEvictCount    = "lru.evict.n"
	TxRebCount       = "tx.reb.n"
	TxRebSize        = "tx.reb.size"
	RxRebCount       = "rx.reb.n"
	RxRebSize        = "rx.reb.size"
	VerChangeCount   = "vchange.n"
	VerChangeSize    = "vchange.size"
	ErrCksumCount    = "err.cksum.n"
	ErrCksumSize     = "err.cksum.size"
	ErrMetadataCount = "err.md.n"
	ErrIOCount       = "err.io.n"
	DownloadSize     = "dl.size"

	// KindLatency
	PutLatency      = "put.µs"
	AppendLatency   = "append.µs"
	GetRedirLatency = "get.redir.µs"
	PutRedirLatency = "put.redir.µs"
	DownloadLatency = "dl.µs"

	DSortCreationReqCount    = "dsort.creation.req.n"
	DSortCreationReqLatency  = "dsort.creation.req.µs"
	DSortCreationRespCount   = "dsort.creation.resp.n"
	DSortCreationRespLatency = "dsort.creation.resp.µs"

	// KindThroughput
	GetThroughput = "get.bps" // bytes per second

	// KindID
	RebGlobID = "reb.glob.id"
)

//
// public type
//
type (
	Trunner struct {
		statsRunner
		T        cluster.Target         `json:"-"`
		Core     *CoreStats             `json:"core"`
		Capacity map[string]*fscapacity `json:"capacity"`
		// inner state
		timecounts struct {
			capLimit atomic.Int64
			capIdx   int64 // update capacity: time interval counting
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
		Used    uint64 `json:"used,string"`  // bytes
		Avail   uint64 `json:"avail,string"` // ditto
		Usedpct int32  `json:"usedpct"`      // redundant ok
	}
)

//
// Trunner
//

func (r *Trunner) Register(name, kind string)  { r.Core.Tracker.register(name, kind) }
func (r *Trunner) Run() error                  { return r.runcommon(r) }
func (r *Trunner) Get(name string) (val int64) { return r.Core.get(name) }

func (r *Trunner) Init(t cluster.Target) *atomic.Bool { // nolint:interfacer // doesn't make sense to use cluster.Proxy
	r.Core = &CoreStats{}
	r.Core.init(48) // and register common stats (target's own stats are registered elsewhere via the Register() above)
	r.Core.initStatsD(t.Snode())

	r.ctracker = make(copyTracker, 48) // these two are allocated once and only used in serial context
	r.lines = make([]string, 0, 16)

	config := cmn.GCO.Get()
	r.Core.statsTime = config.Periodic.StatsTime
	lim := cmn.DivCeil(int64(config.LRU.CapacityUpdTime), int64(config.Periodic.StatsTime))
	r.timecounts.capLimit.Store(lim)

	r.statsRunner.daemon = t

	r.statsRunner.stopCh = make(chan struct{}, 4)
	r.statsRunner.workCh = make(chan NamedVal64, 256)

	// subscribe to config changes
	cmn.GCO.Subscribe(r)
	return &r.statsRunner.startedUp
}

func (r *Trunner) ConfigUpdate(oldConf, newConf *cmn.Config) {
	r.statsRunner.ConfigUpdate(oldConf, newConf)
	if oldConf.Periodic.StatsTime != newConf.Periodic.StatsTime {
		r.Core.statsTime = newConf.Periodic.StatsTime
	}
	if oldConf.LRU.CapacityUpdTime != newConf.LRU.CapacityUpdTime {
		lim := cmn.DivCeil(int64(newConf.LRU.CapacityUpdTime), int64(newConf.Periodic.StatsTime))
		r.timecounts.capLimit.Store(lim)
	}
}

func (r *Trunner) GetWhatStats() []byte {
	ctracker := make(copyTracker, 48)
	r.Core.copyCumulative(ctracker)

	crunner := &copyRunner{Tracker: ctracker, Capacity: r.Capacity}
	return cmn.MustMarshal(crunner)
}

func (r *Trunner) log(uptime time.Duration) {
	r.lines = r.lines[:0]

	// copy stats, reset latencies
	r.Core.UpdateUptime(uptime)
	if idle := r.Core.copyT(r.ctracker, []string{"kalive", Uptime}); !idle {
		b, _ := jsonCompat.Marshal(r.ctracker)
		r.lines = append(r.lines, string(b))
	}

	// 2. capacity
	availableMountpaths, _ := fs.Mountpaths.Get()
	r.timecounts.capIdx++
	if r.timecounts.capIdx >= r.timecounts.capLimit.Load() {
		if runlru := r.UpdateCapacityOOS(availableMountpaths); runlru {
			if cmn.GCO.Get().LRU.Enabled {
				go r.T.RunLRU("")
			}
		}
		r.timecounts.capIdx = 0
		for mpath, fsCapacity := range r.Capacity {
			b := cmn.MustMarshal(fsCapacity)
			r.lines = append(r.lines, mpath+": "+string(b))
		}
	}

	// 3. io stats
	r.lines = fs.Mountpaths.LogAppend(r.lines)

	// 4. log
	for _, ln := range r.lines {
		glog.Infoln(ln)
	}
}

func (r *Trunner) UpdateCapacityOOS(availableMountpaths fs.MPI) (runlru bool) {
	if availableMountpaths == nil {
		availableMountpaths, _ = fs.Mountpaths.Get()
	}
	var (
		config     = cmn.GCO.Get()
		usedNow    int32
		l          = len(availableMountpaths)
		capInfoPrv = r.T.AvgCapUsed(config)
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
		if int64(fsCap.Usedpct) >= config.LRU.HighWM {
			runlru = true
		}
		usedNow += fsCap.Usedpct
	}
	r.Capacity = capacities

	// handle out-of-space
	usedNow /= int32(l)
	capInfoNow := r.T.AvgCapUsed(config, usedNow)

	if capInfoPrv.OOS && !capInfoNow.OOS {
		lim := cmn.DivCeil(int64(config.LRU.CapacityUpdTime), int64(config.Periodic.StatsTime))
		r.timecounts.capLimit.Store(lim)
		glog.Infof("OOS resolved: avg used = %d%% < (hwm %d%%) across %d mountpath(s)",
			usedNow, config.LRU.HighWM, l)
		t := time.Duration(lim) * config.Periodic.StatsTime
		glog.Infof("PUTs are allowed to proceed, next capacity check in %v", t)
	} else if !capInfoPrv.OOS && capInfoNow.OOS {
		lim := cmn.MinI64(r.timecounts.capLimit.Load(), 2)
		r.timecounts.capLimit.Store(lim)
		glog.Warningf("OOS: avg used = %d%% > (oos %d%%) across %d mountpath(s)", usedNow, config.LRU.OOS, l)
		t := time.Duration(lim) * config.Periodic.StatsTime
		glog.Warningf("OOS: disallowing new PUTs and checking capacity every %v", t)
	}
	return
}

// NOTE the naming conventions (above)
func (r *Trunner) doAdd(nv NamedVal64) {
	var (
		s     = r.Core
		name  = nv.Name
		value = nv.Value
	)

	v, ok := s.Tracker[name]
	cmn.AssertMsg(ok, "Invalid stats name '"+name+"'")

	// most target stats can be handled by CoreStats.doAdd
	// stats that track data IO are unique to target and are handled here
	// .size stats, as of 2.x and beyond, is one of them
	if !strings.HasSuffix(name, ".size") {
		s.doAdd(name, nv.NameSuffix, value)
		return
	}

	// target only suffix
	nroot := strings.TrimSuffix(name, ".size")
	metricType := statsd.Counter

	if nroot == "dl" {
		metricType = statsd.PersistentCounter
	}

	s.statsdC.Send(nroot, 1,
		metric{Type: metricType, Name: "bytes", Value: value},
		metric{Type: metricType, Name: "count", Value: 1},
	)

	v.Lock()
	v.Value += value
	v.Unlock()
}

//
// misc
//

func newFSCapacity(statfs *syscall.Statfs_t) *fscapacity {
	pct := (statfs.Blocks - statfs.Bavail) * 100 / statfs.Blocks
	return &fscapacity{
		Used:    (statfs.Blocks - statfs.Bavail) * uint64(statfs.Bsize),
		Avail:   statfs.Bavail * uint64(statfs.Bsize),
		Usedpct: int32(pct),
	}
}
