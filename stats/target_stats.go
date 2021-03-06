// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"strings"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats/statsd"
)

// Naming Convention:
//  -> "*.n" - counter
//  -> "*.ns" - latency (nanoseconds)
//  -> "*.size" - size (bytes)
//  -> "*.bps" - throughput (in byte/s)
//  -> "*.id" - ID
const (
	// KindCounter - QPS and byte counts (always incremented, never reset)
	GetColdCount   = "get.cold.n"
	GetColdSize    = "get.cold.size"
	LruEvictSize   = "lru.evict.size"
	LruEvictCount  = "lru.evict.n"
	VerChangeCount = "vchange.n"
	VerChangeSize  = "vchange.size"
	// rebalance
	RebTxCount = "reb.tx.n"
	RebTxSize  = "reb.tx.size"
	RebRxCount = "reb.rx.n"
	RebRxSize  = "reb.rx.size"
	// errors
	ErrCksumCount    = "err.cksum.n"
	ErrCksumSize     = "err.cksum.size"
	ErrMetadataCount = "err.md.n"
	ErrIOCount       = "err.io.n"
	// special
	RestartCount = "restart.n"

	// KindLatency
	PutLatency      = "put.ns"
	AppendLatency   = "append.ns"
	GetRedirLatency = "get.redir.ns"
	PutRedirLatency = "put.redir.ns"
	DownloadLatency = "dl.ns"

	// DSort
	DSortCreationReqCount    = "dsort.creation.req.n"
	DSortCreationReqLatency  = "dsort.creation.req.ns"
	DSortCreationRespCount   = "dsort.creation.resp.n"
	DSortCreationRespLatency = "dsort.creation.resp.ns"

	// Downloader
	DownloadSize = "dl.size"

	// KindThroughput
	GetThroughput = "get.bps" // bytes per second
)

//
// public type
//
type (
	Trunner struct {
		statsRunner
		T     cluster.Target `json:"-"`
		Core  *CoreStats     `json:"core"`
		MPCap fs.MPCap       `json:"capacity"`
		lines []string
	}
	copyRunner struct {
		Tracker copyTracker `json:"core"`
		MPCap   fs.MPCap    `json:"capacity"`
	}
)

/////////////
// Trunner //
/////////////

// interface guard
var _ cos.Runner = (*Trunner)(nil)

func (r *Trunner) Register(name, kind string)  { r.Core.Tracker.register(name, kind) }
func (r *Trunner) Run() error                  { return r.runcommon(r) }
func (r *Trunner) CoreStats() *CoreStats       { return r.Core }
func (r *Trunner) Get(name string) (val int64) { return r.Core.get(name) }

func (r *Trunner) Init(t cluster.Target) *atomic.Bool {
	r.Core = &CoreStats{}
	r.Core.init(48) // and register common stats (target's own stats are registered elsewhere via the Register() above)
	r.Core.initStatsD(t.Snode())

	r.ctracker = make(copyTracker, 48) // these two are allocated once and only used in serial context
	r.lines = make([]string, 0, 16)

	config := cmn.GCO.Get()
	r.Core.statsTime = config.Periodic.StatsTime

	r.statsRunner.name = "targetstats"
	r.statsRunner.daemon = t

	r.statsRunner.stopCh = make(chan struct{}, 4)
	r.statsRunner.workCh = make(chan NamedVal64, 256)

	// subscribe to config changes
	cmn.GCO.Reg(r.Name(), r)
	return &r.statsRunner.startedUp
}

func (r *Trunner) InitCapacity() error {
	availableMountpaths, _ := fs.Get()
	r.MPCap = make(fs.MPCap, len(availableMountpaths))
	cs, err := fs.RefreshCapStatus(nil, r.MPCap)
	if err != nil {
		return err
	}
	if cs.Err != nil {
		glog.Errorf("%s: %v", r.T.Snode(), cs.Err)
	}
	return nil
}

func (r *Trunner) RegisterAll() {
	r.Register(PutLatency, KindLatency)
	r.Register(AppendLatency, KindLatency)
	r.Register(GetColdCount, KindCounter)
	r.Register(GetColdSize, KindCounter)
	r.Register(GetThroughput, KindThroughput)
	r.Register(LruEvictSize, KindCounter)
	r.Register(LruEvictCount, KindCounter)
	r.Register(VerChangeCount, KindCounter)
	r.Register(VerChangeSize, KindCounter)
	r.Register(GetRedirLatency, KindLatency)
	r.Register(PutRedirLatency, KindLatency)

	// errors
	r.Register(ErrCksumCount, KindCounter)
	r.Register(ErrCksumSize, KindCounter)
	r.Register(ErrMetadataCount, KindCounter)
	r.Register(ErrIOCount, KindCounter)

	// rebalance
	r.Register(RebTxCount, KindCounter)
	r.Register(RebTxSize, KindCounter)
	r.Register(RebRxCount, KindCounter)
	r.Register(RebRxSize, KindCounter)

	// special
	r.Register(RestartCount, KindCounter)

	// download
	r.Register(DownloadSize, KindCounter)
	r.Register(DownloadLatency, KindLatency)

	// dsort
	r.Register(DSortCreationReqCount, KindCounter)
	r.Register(DSortCreationReqLatency, KindLatency)
	r.Register(DSortCreationRespCount, KindCounter)
	r.Register(DSortCreationRespLatency, KindLatency)
}

func (r *Trunner) ConfigUpdate(oldConf, newConf *cmn.Config) {
	r.statsRunner.ConfigUpdate(oldConf, newConf)
	if oldConf.Periodic.StatsTime != newConf.Periodic.StatsTime {
		r.Core.statsTime = newConf.Periodic.StatsTime
	}
	// TODO: fs.CapPeriodic()
}

func (r *Trunner) GetWhatStats() interface{} {
	ctracker := make(copyTracker, 48)
	r.Core.copyCumulative(ctracker)
	return &copyRunner{Tracker: ctracker, MPCap: r.MPCap}
}

func (r *Trunner) log(uptime time.Duration) {
	r.lines = r.lines[:0]

	// copy stats, reset latencies
	r.Core.UpdateUptime(uptime)
	if idle := r.Core.copyT(r.ctracker, []string{"kalive", Uptime}); !idle {
		b := cos.MustMarshal(r.ctracker)
		r.lines = append(r.lines, string(b))
	}

	// 2. capacity
	cs, updated, _ := fs.CapPeriodic(r.MPCap)
	if updated {
		if cs.Err != nil {
			go r.T.RunLRU("" /*uuid*/, false)
		}
		for mpath, fsCapacity := range r.MPCap {
			b := cos.MustMarshal(fsCapacity)
			r.lines = append(r.lines, mpath+": "+string(b))
		}
	}

	// 3. io stats
	r.lines = fs.LogAppend(r.lines)

	// 4. log
	for _, ln := range r.lines {
		glog.Infoln(ln)
	}
}

// NOTE the naming conventions (above)
func (r *Trunner) doAdd(nv NamedVal64) {
	var (
		s     = r.Core
		name  = nv.Name
		value = nv.Value
	)

	v, ok := s.Tracker[name]
	cos.Assertf(ok, "invalid stats name: %q", name)

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
