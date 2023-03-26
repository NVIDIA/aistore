// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/transport"
)

// Naming Convention:
//
//	-> "*.n" - counter
//	-> "*.ns" - latency (nanoseconds)
//	-> "*.size" - size (bytes)
//	-> "*.bps" - throughput (in byte/s)
//	-> "*.id" - ID
const (
	// KindCounter & KindSize - always incremented
	GetColdCount = "get.cold.n"
	GetColdSize  = "get.cold.size"

	LruEvictCount = "lru.evict.n"
	LruEvictSize  = "lru.evict.size"

	CleanupStoreCount = "cleanup.store.n"
	CleanupStoreSize  = "cleanup.store.size"

	VerChangeCount = "ver.change.n"
	VerChangeSize  = "ver.change.size"

	// intra-cluster transmit & receive
	StreamsOutObjCount = transport.OutObjCount
	StreamsOutObjSize  = transport.OutObjSize
	StreamsInObjCount  = transport.InObjCount
	StreamsInObjSize   = transport.InObjSize

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
	PutThroughput = "put.bps" // ditto

	// same as above via `.cumulative`
	GetSize = "get.size"
	PutSize = "put.size"
)

type (
	Trunner struct {
		t           cluster.NodeMemCap
		TargetCDF   fs.TargetCDF `json:"cdf"`
		disk        ios.AllDiskStats
		xln         string
		statsRunner // the base (compare w/ Prunner)
		lines       []string
		mem         sys.MemStat
		standby     bool
	}
)

const (
	minLogDiskUtil = 10 // skip logDiskStats if below
)

/////////////
// Trunner //
/////////////

// interface guard
var _ cos.Runner = (*Trunner)(nil)

func NewTrunner(t cluster.NodeMemCap) *Trunner { return &Trunner{t: t} }

func (r *Trunner) Run() error     { return r.runcommon(r) }
func (r *Trunner) Standby(v bool) { r.standby = v }

func (r *Trunner) Init(t cluster.Target) *atomic.Bool {
	r.core = &coreStats{}
	r.core.init(t.Snode(), 48) // register common (target's own stats are reg()-ed elsewhere)

	r.ctracker = make(copyTracker, 48) // these two are allocated once and only used in serial context
	r.lines = make([]string, 0, 16)
	r.disk = make(ios.AllDiskStats, 16)

	config := cmn.GCO.Get()
	r.core.statsTime = config.Periodic.StatsTime.D()

	r.statsRunner.name = "targetstats"
	r.statsRunner.daemon = t

	r.statsRunner.stopCh = make(chan struct{}, 4)
	r.statsRunner.workCh = make(chan cos.NamedVal64, 256)

	r.core.initMetricClient(t.Snode(), &r.statsRunner)
	return &r.statsRunner.startedUp
}

func (r *Trunner) InitCDF() error {
	availableMountpaths := fs.GetAvail()
	r.TargetCDF.Mountpaths = make(map[string]*fs.CDF, len(availableMountpaths))
	for mpath := range availableMountpaths {
		r.TargetCDF.Mountpaths[mpath] = &fs.CDF{}
	}

	cs, err := fs.CapRefresh(nil, &r.TargetCDF)
	if err != nil {
		return err
	}
	if cs.Err != nil {
		glog.Errorf("%s: %s", r.t, cs.String())
	}
	return nil
}

// register target-specific metrics in addition to those that must be
// already added via regCommonMetrics()
func (r *Trunner) reg(name, kind string) { r.core.Tracker.reg(r.t.Snode(), name, kind) }

func nameRbps(disk string) string { return "disk." + disk + ".read.bps" }
func nameRavg(disk string) string { return "disk." + disk + ".avg.rsize" }
func nameWbps(disk string) string { return "disk." + disk + ".write.bps" }
func nameWavg(disk string) string { return "disk." + disk + ".avg.wsize" }
func nameUtil(disk string) string { return "disk." + disk + ".util" }

// log vs idle logic
func isDiskUtilMetric(name string) bool {
	return strings.HasPrefix(name, "disk.") && strings.HasSuffix(name, ".util")
}

func (r *Trunner) RegDiskMetrics(disk string) {
	s, n := r.core.Tracker, nameRbps(disk)
	if _, ok := s[n]; ok { // must be config.TestingEnv()
		return
	}
	r.reg(n, KindComputedThroughput)
	r.reg(nameWbps(disk), KindComputedThroughput)

	r.reg(nameRavg(disk), KindGauge)
	r.reg(nameWavg(disk), KindGauge)
	r.reg(nameUtil(disk), KindGauge)
}

func (r *Trunner) RegMetrics(node *cluster.Snode) {
	r.reg(GetColdCount, KindCounter)
	r.reg(GetColdSize, KindSize)

	r.reg(LruEvictCount, KindCounter)
	r.reg(LruEvictSize, KindSize)

	r.reg(CleanupStoreCount, KindCounter)
	r.reg(CleanupStoreSize, KindSize)

	r.reg(VerChangeCount, KindCounter)
	r.reg(VerChangeSize, KindSize)

	r.reg(PutLatency, KindLatency)
	r.reg(AppendLatency, KindLatency)
	r.reg(GetRedirLatency, KindLatency)
	r.reg(PutRedirLatency, KindLatency)

	// bps
	r.reg(GetThroughput, KindThroughput)
	r.reg(PutThroughput, KindThroughput)

	r.reg(GetSize, KindSize)
	r.reg(PutSize, KindSize)

	// errors
	r.reg(ErrCksumCount, KindCounter)
	r.reg(ErrCksumSize, KindSize)

	r.reg(ErrMetadataCount, KindCounter)
	r.reg(ErrIOCount, KindCounter)

	// streams
	r.reg(StreamsOutObjCount, KindCounter)
	r.reg(StreamsOutObjSize, KindSize)
	r.reg(StreamsInObjCount, KindCounter)
	r.reg(StreamsInObjSize, KindSize)

	// special
	r.reg(RestartCount, KindCounter)

	// download
	r.reg(DownloadSize, KindSize)
	r.reg(DownloadLatency, KindLatency)

	// dsort
	r.reg(DSortCreationReqCount, KindCounter)
	r.reg(DSortCreationReqLatency, KindLatency)
	r.reg(DSortCreationRespCount, KindCounter)
	r.reg(DSortCreationRespLatency, KindLatency)

	// Prometheus
	r.core.initProm(node)
}

func (r *Trunner) GetStats() (ds *Node) {
	ds = r.statsRunner.GetStats()
	ds.TargetCDF = r.TargetCDF
	return
}

func (r *Trunner) log(now int64, uptime time.Duration, config *cmn.Config) {
	r.lines = r.lines[:0]

	// 1. collect disk stats and populate the tracker
	s := r.core
	fs.FillDiskStats(r.disk)
	for disk, stats := range r.disk {
		v := s.Tracker[nameRbps(disk)]
		v.Value = stats.RBps
		v = s.Tracker[nameRavg(disk)]
		v.Value = stats.Ravg
		v = s.Tracker[nameWbps(disk)]
		v.Value = stats.WBps
		v = s.Tracker[nameWavg(disk)]
		v.Value = stats.Wavg
		v = s.Tracker[nameUtil(disk)]
		v.Value = stats.Util
	}

	// 2 copy stats, reset latencies, send via StatsD if configured
	s.updateUptime(uptime)
	s.promLock()
	idle := s.copyT(r.ctracker, config.Disk.DiskUtilLowWM)
	s.promUnlock()

	if now >= r.nextLogTime && !idle {
		s.sgl.Reset() // NOTE: sharing the same sgl w/ CoreStats.copyT
		r.ctracker.write(s.sgl)
		bytes := s.sgl.Bytes()
		r.lines = append(r.lines, string(bytes))

		i := int64(config.Log.StatsTime)
		if i == 0 {
			i = dfltStatsLogInterval
		}
		r.nextLogTime = now + cos.MinI64(i, maxStatsLogInterval)
	}

	// 3. capacity
	cs, updated, errfs := fs.CapPeriodic(now, config, &r.TargetCDF)
	if updated {
		if cs.Err != nil || cs.PctMax > int32(config.Space.CleanupWM) {
			r.t.OOS(&cs)
		}
		for mpath, fsCapacity := range r.TargetCDF.Mountpaths {
			ln := cos.MustMarshalToString(fsCapacity)
			r.lines = append(r.lines, mpath+": "+ln)
		}
	} else if errfs != nil {
		glog.Error(errfs)
	}

	// 4. append disk stats to log
	r.logDiskStats()

	// 5. memory pressure
	_ = r.mem.Get()
	mm := r.t.PageMM()
	if p := mm.Pressure(&r.mem); p >= memsys.PressureHigh {
		r.lines = append(r.lines, mm.Str(&r.mem))
	}

	// 6. running xactions
	if !idle {
		var (
			ln     string
			rs, is = r.t.GetAllRunning("", true /*separate idle*/)
		)
		if len(rs) > 0 {
			ln = "running: " + strings.Join(rs, " ")
			if len(is) > 0 {
				ln += "; "
			}
		}
		if len(is) > 0 {
			ln += "idle: " + strings.Join(is, " ")
		}
		if ln != "" && ln != r.xln {
			r.lines = append(r.lines, ln)
		}
		r.xln = ln
	}

	// 7. and, finally
	for _, ln := range r.lines {
		glog.Infoln(ln)
	}
}

// log formatted disk stats:
// [ disk: read throughput, average read size, write throughput, average write size, disk utilization ]
// e.g.: [ sda: 94MiB/s, 68KiB, 25MiB/s, 21KiB, 82% ]
func (r *Trunner) logDiskStats() {
	for disk, stats := range r.disk {
		if stats.Util < minLogDiskUtil {
			continue
		}
		rbps := cos.ToSizeIEC(stats.RBps, 0)
		wbps := cos.ToSizeIEC(stats.WBps, 0)
		ravg := cos.ToSizeIEC(stats.Ravg, 0)
		wavg := cos.ToSizeIEC(stats.Wavg, 0)
		l := len(disk) + len(rbps) + len(wbps) + len(ravg) + len(wavg) + 64
		buf := make([]byte, 0, l)
		buf = append(buf, disk...)
		buf = append(buf, ": "...)
		buf = append(buf, rbps...)
		buf = append(buf, "/s, "...)
		buf = append(buf, ravg...)
		buf = append(buf, ", "...)
		buf = append(buf, wbps...)
		buf = append(buf, "/s, "...)
		buf = append(buf, wavg...)
		buf = append(buf, ", "...)
		buf = append(buf, strconv.FormatInt(stats.Util, 10)...)
		buf = append(buf, "%"...)
		r.lines = append(r.lines, *(*string)(unsafe.Pointer(&buf)))
	}
}

func (r *Trunner) doAdd(nv cos.NamedVal64) {
	var (
		s     = r.core
		name  = nv.Name
		value = nv.Value
	)
	_, ok := s.Tracker[name]
	debug.Assertf(ok, "invalid stats name: %q", name)
	s.doAdd(name, nv.NameSuffix, value)
}

func (r *Trunner) statsTime(newval time.Duration) {
	r.core.statsTime = newval
}

func (r *Trunner) standingBy() bool { return r.standby }
