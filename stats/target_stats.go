// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/ios"
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

	// NOTE semantics:
	// - counts all instances when remote GET is followed by storing of the new object (version) locally
	// - does _not_ count assorted calls to `GetObjReader` (e.g., via tcb/tco -> LDP.Reader)

	LruEvictCount = "lru.evict.n"
	LruEvictSize  = "lru.evict.size"

	CleanupStoreCount = "cleanup.store.n"
	CleanupStoreSize  = "cleanup.store.size"

	VerChangeCount = "ver.change.n"
	VerChangeSize  = "ver.change.size"

	// errors
	ErrCksumCount = errPrefix + "cksum.n"
	ErrCksumSize  = errPrefix + "cksum.size"

	ErrFSHCCount = errPrefix + "fshc.n"

	// IO errors (must have ioErrPrefix)
	IOErrGetCount    = ioErrPrefix + "get.n"
	IOErrPutCount    = ioErrPrefix + "put.n"
	IOErrDeleteCount = ioErrPrefix + "del.n"

	// KindLatency
	PutLatency         = "put.ns"
	PutLatencyTotal    = "put.ns.total"
	PutE2ELatencyTotal = "e2e.put.ns.total" // e2e write-through PUT latency
	AppendLatency      = "append.ns"
	GetRedirLatency    = "get.redir.ns"
	PutRedirLatency    = "put.redir.ns"
	DownloadLatency    = "dl.ns"
	HeadLatency        = "head.ns"
	HeadLatencyTotal   = "head.ns.total"

	// Dsort
	DsortCreationReqCount    = "dsort.creation.req.n"
	DsortCreationRespCount   = "dsort.creation.resp.n"
	DsortCreationRespLatency = "dsort.creation.resp.ns"
	DsortExtractShardDskCnt  = "dsort.extract.shard.dsk.n"
	DsortExtractShardMemCnt  = "dsort.extract.shard.mem.n"
	DsortExtractShardSize    = "dsort.extract.shard.size" // uncompressed

	// Downloader
	DownloadSize = "dl.size"

	// KindThroughput
	GetThroughput = "get.bps" // bytes per second
	PutThroughput = "put.bps" // ditto

	// same as above via `.cumulative`
	GetSize = "get.size"
	PutSize = "put.size"

	// core
	RemoteDeletedDelCount = core.RemoteDeletedDelCount // compare w/ common `DeleteCount`

	LcacheCollisionCount = core.LcacheCollisionCount
	LcacheEvictedCount   = core.LcacheEvictedCount
	LcacheErrCount       = core.LcacheErrCount
	LcacheFlushColdCount = core.LcacheFlushColdCount

	// variable label used for prometheus disk metrics
	diskMetricLabel = "disk"
)

type (
	dmetric map[string]string // "read.bps" => full metric name, etc.

	Trunner struct {
		t    core.Target
		disk struct {
			stats   ios.AllDiskStats   // numbers
			metrics map[string]dmetric // respective names
		}
		xln     string
		xallRun core.AllRunningInOut
		lines   []string // respective names

		fsIDs  []cos.FsID
		Tcdf   fs.Tcdf `json:"cdf"`
		runner         // the base (compare w/ Prunner)
		cs     struct {
			last int64 // mono.Nano
		}
		ioErrs  int64 // sum values of (ioErrNames) counters
		standby bool
	}
)

const (
	minLogDiskUtil = 15 // skip logging idle disks

	numTargetStats = 48 // approx. initial
)

/////////////
// Trunner //
/////////////

// interface guard
var (
	_ cos.Runner = (*Trunner)(nil)
	_ Tracker    = (*Trunner)(nil)
)

// assorted IO errors (discounting connection-reset et al.)
// NOTE: must have ioErrPrefix
var ioErrNames = [...]string{IOErrGetCount, IOErrPutCount, IOErrDeleteCount}

func NewTrunner(t core.Target) *Trunner { return &Trunner{t: t} }

func (r *Trunner) Run() error     { return r._run(r /*as statsLogger*/) }
func (r *Trunner) Standby(v bool) { r.standby = v }

func (r *Trunner) Init() *atomic.Bool {
	r.core = &coreStats{}

	r.core.init(numTargetStats)

	r.regCommon(r.t.Snode())

	r.ctracker = make(copyTracker, numTargetStats) // these two are allocated once and only used in serial context
	r.lines = make([]string, 0, 16)

	r.disk.stats = make(ios.AllDiskStats, 16)
	r.disk.metrics = make(map[string]dmetric, 16)

	config := cmn.GCO.Get()
	r.core.statsTime = config.Periodic.StatsTime.D()

	r.runner.name = "targetstats"
	r.runner.node = r.t

	r.runner.stopCh = make(chan struct{}, 4)

	r.sorted = make([]string, 0, numTargetStats)

	r.xallRun.Running = make([]string, 16)
	r.xallRun.Idle = make([]string, 16)

	return &r.runner.startedUp
}

func (r *Trunner) InitCDF(config *cmn.Config) error {
	fs.InitCDF(&r.Tcdf)
	_, err, errCap := fs.CapRefresh(config, &r.Tcdf)
	if err != nil {
		return err
	}
	if errCap != nil {
		nlog.Errorln(r.t.String()+":", errCap)
	}
	return nil
}

func (r *Trunner) _dmetric(disk, metric string) string {
	var (
		sb strings.Builder
		l  = len(diskMetricLabel) + 1 + len(disk) + 1 + len(metric)
	)
	sb.Grow(l)

	sb.WriteString(diskMetricLabel)
	sb.WriteByte('.')
	sb.WriteString(disk)
	sb.WriteByte('.')
	sb.WriteString(metric)
	fullname := sb.String()

	m, ok := r.disk.metrics[disk]
	if !ok {
		debug.Assert(metric == "read.bps", metric)
		m = make(map[string]string, 5)
		r.disk.metrics[disk] = m

		// init all the rest, as per ios.DiskStats
		r._dmetric(disk, "avg.rsize")
		r._dmetric(disk, "write.bps")
		r._dmetric(disk, "avg.wsize")
		r._dmetric(disk, "util")
	}
	m[metric] = fullname
	return fullname
}

// NOTE: must always be called first and prior to all the other disk-naming metrics (below)
func (r *Trunner) nameRbps(disk string) string {
	if dmetric, ok := r.disk.metrics[disk]; ok {
		return dmetric["read.bps"]
	}
	// init & slow path
	return r._dmetric(disk, "read.bps")
}

func (r *Trunner) nameRavg(disk string) string { return r.disk.metrics[disk]["avg.rsize"] }
func (r *Trunner) nameWbps(disk string) string { return r.disk.metrics[disk]["write.bps"] }
func (r *Trunner) nameWavg(disk string) string { return r.disk.metrics[disk]["avg.wsize"] }
func (r *Trunner) nameUtil(disk string) string { return r.disk.metrics[disk]["util"] }

// log vs idle logic
func isDiskMetric(name string) bool {
	return strings.HasPrefix(name, "disk.")
}

func isDiskUtilMetric(name string) bool {
	return isDiskMetric(name) && strings.HasSuffix(name, ".util")
}

// target-specific metrics, in addition to common and already added via regCommon()
func (r *Trunner) RegMetrics(snode *meta.Snode) {
	r.reg(snode, LruEvictCount, KindCounter,
		&Extra{
			Help: "number of LRU evictions",
		},
	)
	r.reg(snode, LruEvictSize, KindSize,
		&Extra{
			Help: "total cumulative size (bytes) of LRU evictions",
		},
	)

	// removing $deleted objects is currently not counted
	r.reg(snode, CleanupStoreCount, KindCounter,
		&Extra{
			Help: "space cleanup: number of removed misplaced objects and old work files",
		},
	)
	r.reg(snode, CleanupStoreSize, KindSize,
		&Extra{
			Help: "space cleanup: total size (bytes) of all removed misplaced objects and old work files (not including removed deleted objects)",
		},
	)

	// out-of-band (x 3)
	r.reg(snode, VerChangeCount, KindCounter,
		&Extra{
			Help: "number of out-of-band updates (by a 3rd party performing remote PUTs from outside this cluster)",
		},
	)
	r.reg(snode, VerChangeSize, KindSize,
		&Extra{
			Help: "total cumulative size (bytes) of objects that were updated out-of-band across all backends combined",
		},
	)
	r.reg(snode, RemoteDeletedDelCount, KindCounter,
		&Extra{
			Help: "number of out-of-band deletes (by a 3rd party remote DELETE(object) from outside this cluster)",
		},
	)

	r.reg(snode, PutLatency, KindLatency,
		&Extra{
			Help: "PUT: average time (milliseconds) over the last periodic.stats_time interval",
		},
	)
	r.reg(snode, PutLatencyTotal, KindTotal,
		&Extra{
			Help: "PUT: total cumulative time (nanoseconds)",
		},
	)
	r.reg(snode, HeadLatency, KindLatency,
		&Extra{
			Help: "HEAD: average time (milliseconds) over the last periodic.stats_time interval",
		},
	)
	r.reg(snode, HeadLatencyTotal, KindTotal,
		&Extra{
			Help: "HEAD: total cumulative time (nanoseconds)",
		},
	)
	r.reg(snode, AppendLatency, KindLatency,
		&Extra{
			Help: "APPEND(object): average time (milliseconds) over the last periodic.stats_time interval",
		},
	)
	r.reg(snode, GetRedirLatency, KindLatency,
		&Extra{
			Help: "GET: average gateway-to-target HTTP redirect latency (milliseconds) over the last periodic.stats_time interval",
		},
	)
	r.reg(snode, PutRedirLatency, KindLatency,
		&Extra{
			Help: "PUT: average gateway-to-target HTTP redirect latency (milliseconds) over the last periodic.stats_time interval",
		},
	)

	// bps
	r.reg(snode, GetThroughput, KindThroughput,
		&Extra{
			Help: "GET: average throughput (MB/s) over the last periodic.stats_time interval",
		},
	)
	r.reg(snode, PutThroughput, KindThroughput,
		&Extra{
			Help: "PUT: average throughput (MB/s) over the last periodic.stats_time interval",
		},
	)

	r.reg(snode, GetSize, KindSize,
		&Extra{
			Help: "GET: total cumulative size (bytes)",
		},
	)
	r.reg(snode, PutSize, KindSize,
		&Extra{
			Help: "PUT: total cumulative size (bytes)",
		},
	)

	// errors
	r.reg(snode, ErrCksumCount, KindCounter,
		&Extra{
			Help: "number of executed GET(object) requests",
		},
	)
	r.reg(snode, ErrCksumSize, KindSize,
		&Extra{
			Help: "number of executed GET(object) requests",
		},
	)
	r.reg(snode, ErrFSHCCount, KindCounter,
		&Extra{
			Help: "number of times filesystem health checker (FSHC) was triggered by an I/O error or errors",
		},
	)

	r.reg(snode, IOErrGetCount, KindCounter,
		&Extra{
			Help: "GET: number of I/O errors _not_ including remote backend and network errors",
		},
	)
	r.reg(snode, IOErrPutCount, KindCounter,
		&Extra{
			Help: "PUT: number of I/O errors _not_ including remote backend and network errors",
		},
	)
	r.reg(snode, IOErrDeleteCount, KindCounter,
		&Extra{
			Help: "DELETE(object): number of I/O errors _not_ including remote backend and network errors",
		},
	)

	r.reg(snode, LcacheErrCount, KindCounter,
		&Extra{
			Help: "number of LOM flush errors (core, internal)",
		},
	)

	// streams
	r.reg(snode, cos.StreamsOutObjCount, KindCounter,
		&Extra{
			Help: "intra-cluster streaming communications: number of sent objects",
		},
	)
	r.reg(snode, cos.StreamsOutObjSize, KindSize,
		&Extra{
			Help: "intra-cluster streaming communications: total cumulative size (bytes) of all transmitted objects",
		},
	)
	r.reg(snode, cos.StreamsInObjCount, KindCounter,
		&Extra{
			Help: "intra-cluster streaming communications: number of received objects",
		},
	)
	r.reg(snode, cos.StreamsInObjSize, KindSize,
		&Extra{
			Help: "intra-cluster streaming communications: total cumulative size (bytes) of all received objects",
		},
	)

	// download
	r.reg(snode, DownloadSize, KindSize,
		&Extra{
			Help: "total downloaded size (bytes)",
		},
	)
	r.reg(snode, DownloadLatency, KindLatency,
		&Extra{
			Help: "total time it took to execute dowload requests (milliseconds)",
		},
	)

	// dsort
	r.reg(snode, DsortCreationReqCount, KindCounter,
		&Extra{
			Help: "dsort: see https://github.com/NVIDIA/aistore/blob/main/docs/dsort.md#metrics",
		},
	)
	r.reg(snode, DsortCreationRespCount, KindCounter,
		&Extra{
			Help: "dsort: see https://github.com/NVIDIA/aistore/blob/main/docs/dsort.md#metrics",
		},
	)
	r.reg(snode, DsortCreationRespLatency, KindLatency,
		&Extra{
			Help: "dsort: see https://github.com/NVIDIA/aistore/blob/main/docs/dsort.md#metrics",
		},
	)
	r.reg(snode, DsortExtractShardDskCnt, KindCounter,
		&Extra{
			Help: "dsort: see https://github.com/NVIDIA/aistore/blob/main/docs/dsort.md#metrics",
		},
	)
	r.reg(snode, DsortExtractShardMemCnt, KindCounter,
		&Extra{
			Help: "dsort: see https://github.com/NVIDIA/aistore/blob/main/docs/dsort.md#metrics",
		},
	)
	r.reg(snode, DsortExtractShardSize, KindSize,
		&Extra{
			Help: "dsort: see https://github.com/NVIDIA/aistore/blob/main/docs/dsort.md#metrics",
		},
	)

	// core
	r.reg(snode, LcacheCollisionCount, KindCounter,
		&Extra{
			Help: "number of LOM cache collisions (core, internal)",
		},
	)
	r.reg(snode, LcacheEvictedCount, KindCounter,
		&Extra{
			Help: "number of LOM cache evictions (core, internal)",
		},
	)
	r.reg(snode, LcacheFlushColdCount, KindCounter,
		&Extra{
			Help: "number of times a LOM from cache was written to stable storage (core, internal)",
		},
	)
}

func (r *Trunner) RegDiskMetrics(snode *meta.Snode, disk string) {
	s := r.core.Tracker
	name := r.nameRbps(disk)
	if _, ok := s[name]; ok {
		return // all metrics - only once, at once
	}

	// "disk.<DISK>.<METRIC> (e.g.: "disk.nvme0n1.read.bps")
	r.reg(snode, name, KindComputedThroughput,
		&Extra{Help: "read bandwidth (MB/s)", StrName: "disk_read_mbps", Labels: cos.StrKVs{"disk": disk}},
	)
	r.reg(snode, r.nameRavg(disk), KindGauge,
		&Extra{Help: "average read size (bytes)", StrName: "disk_avg_rsize", Labels: cos.StrKVs{"disk": disk}},
	)
	r.reg(snode, r.nameWbps(disk), KindComputedThroughput,
		&Extra{Help: "write bandwidth (MB/s)", StrName: "disk_write_mbps", Labels: cos.StrKVs{"disk": disk}},
	)
	r.reg(snode, r.nameWavg(disk), KindGauge,
		&Extra{Help: "average write size (bytes)", StrName: "disk_avg_wsize", Labels: cos.StrKVs{"disk": disk}},
	)
	r.reg(snode, r.nameUtil(disk), KindGauge,
		&Extra{Help: "disk utilization (%%)", StrName: "disk_util", Labels: cos.StrKVs{"disk": disk}},
	)
}

func (r *Trunner) GetStats() (ds *Node) {
	ds = r.runner.GetStats()

	fs.InitCDF(&ds.Tcdf)
	fs.CapRefresh(cmn.GCO.Get(), &ds.Tcdf)
	return ds
}

// [backward compatibility] v3.22 and prior
func (r *Trunner) GetStatsV322() (out *NodeV322) {
	ds := r.GetStats()

	out = &NodeV322{}
	out.Snode = ds.Snode
	out.Tracker = ds.Tracker
	out.Tcdf.PctMax = ds.Tcdf.PctMax
	out.Tcdf.PctAvg = ds.Tcdf.PctAvg
	out.Tcdf.PctMin = ds.Tcdf.PctMin
	out.Tcdf.CsErr = ds.Tcdf.CsErr
	out.Tcdf.Mountpaths = make(map[string]*fs.CDFv322, len(ds.Tcdf.Mountpaths))
	for mpath := range ds.Tcdf.Mountpaths {
		cdf := &fs.CDFv322{
			Capacity: ds.Tcdf.Mountpaths[mpath].Capacity,
			Disks:    ds.Tcdf.Mountpaths[mpath].Disks,
			FS:       ds.Tcdf.Mountpaths[mpath].FS.String(),
		}
		out.Tcdf.Mountpaths[mpath] = cdf
	}
	return out
}

func (r *Trunner) numIOErrs() (n int64) {
	for _, name := range ioErrNames {
		n += r.Get(name)
	}
	return n
}

func (r *Trunner) _fshcMaybe(config *cmn.Config) {
	c := config.FSHC
	if !c.Enabled {
		return
	}
	if r.core.statsTime < 5*time.Second {
		return // cannot reliably recompute to c.IOErrTime (which is 10s or greater)
	}

	n := r.numIOErrs()
	d := n - r.ioErrs // since previous `r.log`
	r.ioErrs = n

	j := d * int64(c.IOErrTime) / int64(r.core.statsTime) // recompute
	if j < int64(c.IOErrs) {
		return
	}

	err := fmt.Errorf("## IO errors (%d) exceeded configured limit, which is: (no more than %d in %v)", d, c.IOErrs, c.IOErrTime)
	nlog.Errorln(err)
	nlog.Warningln("waking up FSHC to check all mountpaths...") // _all_

	r.t.SoftFSHC()
}

// log _and_ update various low-level states
func (r *Trunner) log(now int64, uptime time.Duration, config *cmn.Config) {
	r._fshcMaybe(config)

	r.lines = r.lines[:0]

	// 1. disk stats
	refreshCap := r.Tcdf.HasAlerts()
	fs.DiskStats(r.disk.stats, nil /*fs.TcdfExt*/, config, refreshCap)

	s := r.core
	for disk, stats := range r.disk.stats {
		n := r.nameRbps(disk)
		v := s.Tracker[n]
		if v == nil {
			nlog.Warningln("missing:", n)
			continue
		}
		v.Value = stats.RBps
		v = s.Tracker[r.nameRavg(disk)]
		v.Value = stats.Ravg
		v = s.Tracker[r.nameWbps(disk)]
		v.Value = stats.WBps
		v = s.Tracker[r.nameWavg(disk)]
		v.Value = stats.Wavg
		v = s.Tracker[r.nameUtil(disk)]
		v.Value = stats.Util
	}

	// 2 copy stats, reset latencies, send via StatsD if configured
	s.updateUptime(uptime)
	s.promLock()
	idle := s.copyT(r.ctracker, config.Disk.DiskUtilLowWM)
	s.promUnlock()

	verbose := cmn.Rom.FastV(4, cos.SmoduleStats)
	if (!idle && now >= r.next) || verbose {
		s.sgl.Reset() // sharing w/ CoreStats.copyT
		r.ctracker.write(s.sgl, r.sorted, true /*target*/, idle)
		if l := s.sgl.Len(); l > 3 { // skip '{}'
			line := string(s.sgl.Bytes())
			debug.Assert(l < s.sgl.Slab().Size(), l, " vs slab ", s.sgl.Slab().Size())
			if line != r.prev {
				r.lines = append(r.lines, line)
				r.prev = line
			}
		}
		r._next(config, now)
	}

	// 3. capacity, mountpath alerts, and associated node state flags
	set, clr := r._cap(config, now, verbose)

	if !refreshCap && set != 0 {
		// refill r.disk (ios.AllDiskStats) prior to logging
		fs.DiskStats(r.disk.stats, nil /*fs.TcdfExt*/, config, true /*refresh cap*/)
	}

	// 4. append disk stats to log subject to (idle) filtering (see related: `ignoreIdle`)
	r.logDiskStats(verbose)

	// 5. jobs
	if !idle {
		r.xln = r._jobs(verbose)
	} else {
		r.xln = ""
	}

	// 6. log
	for _, ln := range r.lines {
		nlog.Infoln(ln)
	}

	// clear 'node-restarted'
	if uptime > 10*time.Hour {
		clr |= cos.NodeRestarted
	}

	// 7. separately, memory and CPU alerts
	r._memload(r.t.PageMM(), set, clr)
}

func (r *Trunner) _cap(config *cmn.Config, now int64, verbose bool) (set, clr cos.NodeStateFlags) {
	cs, updated, err, errCap := fs.CapPeriodic(now, config, &r.Tcdf)
	if err != nil {
		nlog.Errorln(err)
		debug.Assert(!updated && errCap == nil, updated, " ", errCap)
		return 0, 0
	}
	if !updated && errCap == nil { // nothing to do
		return 0, 0
	}

	var (
		pcs       = &cs
		hasAlerts bool
	)
	if !updated {
		pcs = nil // to possibly force refresh via t.OOS
	} else {
		hasAlerts = r.Tcdf.HasAlerts()
	}

	// target to run x-space
	if errCap != nil {
		r.t.OOS(pcs, config, &r.Tcdf)
	} else if cs.PctMax > int32(config.Space.CleanupWM) { // remove deleted, other cleanup
		debug.Assert(!cs.IsOOS(), cs.String())
		errCap = cmn.NewErrCapExceeded(cs.TotalUsed, cs.TotalAvail+cs.TotalUsed, 0, config.Space.CleanupWM, cs.PctMax, false)
		r.t.OOS(pcs, config, &r.Tcdf)
	}

	//
	// (periodically | on error | verbose): log mountpath cap and state
	//
	if now >= r.cs.last+dlftCapLogInterval || errCap != nil || hasAlerts || verbose {
		r.logCapacity(now)
	}

	// and more
	flags := r.nodeStateFlags()
	if hasAlerts || flags.IsRed() {
		r.lines = append(r.lines, "Warning: state alerts:", flags.String())
	} else if flags.IsSet(cos.DiskFault) && updated {
		clr |= cos.DiskFault
	}

	// cap alert
	switch {
	case cs.IsOOS():
		set = cos.OOS
	case cs.Err() != nil:
		clr = cos.OOS
		set |= cos.LowCapacity
	default:
		clr = cos.OOS | cos.LowCapacity
	}
	return set, clr
}

func (r *Trunner) logCapacity(now int64) {
	fast := fs.NoneShared(len(r.Tcdf.Mountpaths))
	unique := fast // and vice versa
	if !fast {
		r.fsIDs = r.fsIDs[:0]
	}
	for mpath, cdf := range r.Tcdf.Mountpaths {
		if !fast {
			r.fsIDs, unique = cos.AddUniqueFsID(r.fsIDs, cdf.FS.FsID)
		}
		if unique { // to avoid log duplication
			var (
				sb    strings.Builder
				label = cdf.Label.ToLog()
				l     = 48 + len(mpath) + len(label)
			)
			sb.Grow(l)

			sb.WriteString(mpath)
			sb.WriteString(label)
			if alert, _ := fs.HasAlert(cdf.Disks); alert != "" {
				sb.WriteString(": ")
				sb.WriteString(alert)
			} else {
				sb.WriteString(": used ")
				sb.WriteString(strconv.Itoa(int(cdf.Capacity.PctUsed)))
				sb.WriteByte('%')
				sb.WriteString(", avail ")
				sb.WriteString(cos.ToSizeIEC(int64(cdf.Capacity.Avail), 2))
			}

			r.lines = append(r.lines, sb.String())
		}
	}
	r.cs.last = now
}

// log formatted disk stats:
// [ disk: read throughput, average read size, write throughput, average write size, disk utilization ]
// e.g.: [ sda: 94MiB/s, 68KiB, 25MiB/s, 21KiB, 82% ]
func (r *Trunner) logDiskStats(verbose bool) {
	for disk, stats := range r.disk.stats {
		if stats.Util < minLogDiskUtil && !verbose {
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

func (r *Trunner) _jobs(verbose bool) string {
	r.xallRun.Running = r.xallRun.Running[:0]
	r.xallRun.Idle = r.xallRun.Idle[:0]
	orig := r.xallRun.Idle
	if !verbose {
		r.xallRun.Idle = nil
	}
	r.t.GetAllRunning(&r.xallRun, true /*periodic*/)
	if !verbose {
		r.xallRun.Idle = orig
	}

	var sb strings.Builder
	if len(r.xallRun.Running) > 0 {
		cos.AppendStrings(&sb, "running: ", ' ', r.xallRun.Running...)
		if len(r.xallRun.Idle) > 0 {
			cos.AppendStrings(&sb, ";  idle: ", ' ', r.xallRun.Idle...)
		}
	} else if len(r.xallRun.Idle) > 0 {
		cos.AppendStrings(&sb, "idle: ", ' ', r.xallRun.Idle...)
	}

	ln := sb.String()
	if ln != "" && ln != r.xln {
		r.lines = append(r.lines, ln)
	}

	return ln
}

func (r *Trunner) statsTime(newval time.Duration) {
	r.core.statsTime = newval
}

func (r *Trunner) standingBy() bool { return r.standby }
