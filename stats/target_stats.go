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
	GetColdCount = "get.cold.n"
	GetColdSize  = "get.cold.size"

	LruEvictCount = "lru.evict.n"
	LruEvictSize  = "lru.evict.size"

	CleanupStoreCount = "cleanup.store.n"
	CleanupStoreSize  = "cleanup.store.size"

	VerChangeCount = "ver.change.n"
	VerChangeSize  = "ver.change.size"

	// errors
	ErrCksumCount    = "err.cksum.n"
	ErrCksumSize     = "err.cksum.size"
	ErrMetadataCount = "err.md.n"
	ErrIOCount       = "err.io.n"

	// KindLatency
	PutLatency      = "put.ns"
	AppendLatency   = "append.ns"
	GetRedirLatency = "get.redir.ns"
	PutRedirLatency = "put.redir.ns"
	DownloadLatency = "dl.ns"

	// (read remote, write local) latency (and note that cold-GET's "pure"
	// transmit-response latency = GetLatency - GetColdRwLatency)
	GetColdRwLatency = "get.cold.rw.ns"

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
	LcacheFlushColdCount = core.LcacheFlushColdCount

	// variable label used for prometheus disk metrics
	diskMetricLabel = "disk"
)

type (
	Trunner struct {
		runner    // the base (compare w/ Prunner)
		t         core.NodeCapacity
		TargetCDF fs.TargetCDF `json:"cdf"`
		disk      ios.AllDiskStats
		xln       string
		cs        struct {
			last int64 // mono.Nano
		}
		lines   []string
		fsIDs   []cos.FsID
		xallRun core.AllRunningInOut
		standby bool
	}
)

const (
	minLogDiskUtil = 10 // skip logging idle disks

	numTargetStats = 48 // approx. initial
)

/////////////
// Trunner //
/////////////

// interface guard
var _ cos.Runner = (*Trunner)(nil)

func NewTrunner(t core.NodeCapacity) *Trunner { return &Trunner{t: t} }

func (r *Trunner) Run() error     { return r._run(r /*as statsLogger*/) }
func (r *Trunner) Standby(v bool) { r.standby = v }

func (r *Trunner) Init(t core.Target) *atomic.Bool {
	r.core = &coreStats{}

	r.core.init(numTargetStats)

	r.regCommon(t.Snode())

	r.ctracker = make(copyTracker, numTargetStats) // these two are allocated once and only used in serial context
	r.lines = make([]string, 0, 16)
	r.disk = make(ios.AllDiskStats, 16)

	config := cmn.GCO.Get()
	r.core.statsTime = config.Periodic.StatsTime.D()

	r.runner.name = "targetstats"
	r.runner.node = t

	r.runner.stopCh = make(chan struct{}, 4)

	r.core.initMetricClient(t.Snode(), &r.runner)

	r.sorted = make([]string, 0, numTargetStats)

	r.xallRun.Running = make([]string, 16)
	r.xallRun.Idle = make([]string, 16)

	return &r.runner.startedUp
}

func (r *Trunner) InitCDF() error {
	fs.InitCDF(&r.TargetCDF)
	_, err, errCap := fs.CapRefresh(nil, &r.TargetCDF)
	if err != nil {
		return err
	}
	if errCap != nil {
		nlog.Errorln(r.t.String()+":", errCap)
	}
	return nil
}

func diskMetricName(disk, metric string) string {
	return fmt.Sprintf("%s.%s.%s", diskMetricLabel, disk, metric)
}
func nameRbps(disk string) string { return diskMetricName(disk, "read.bps") }
func nameRavg(disk string) string { return diskMetricName(disk, "avg.rsize") }
func nameWbps(disk string) string { return diskMetricName(disk, "write.bps") }
func nameWavg(disk string) string { return diskMetricName(disk, "avg.wsize") }
func nameUtil(disk string) string { return diskMetricName(disk, "util") }

// log vs idle logic
func isDiskMetric(name string) bool {
	return strings.HasPrefix(name, "disk.")
}
func isDiskUtilMetric(name string) bool {
	return isDiskMetric(name) && strings.HasSuffix(name, ".util")
}

// extractPromDiskMetricName returns prometheus friendly metrics name
// from disk tracker name of format `disk.<disk-name>.<metric-name>`
// it returns, two strings:
//  1. <disk-name> used as prometheus variable label
//  2. `disk.<metric-name>` used for prometheus metric name
func extractPromDiskMetricName(name string) (diskName, metricName string) {
	diskName = strings.Split(name, ".")[1]
	return diskName, strings.ReplaceAll(name, "."+diskName+".", ".")
}

// target-specific metrics, in addition to common and already added via regCommon()
func (r *Trunner) RegMetrics(snode *meta.Snode) {
	r.reg(snode, GetColdCount, KindCounter)
	r.reg(snode, GetColdSize, KindSize)

	r.reg(snode, LruEvictCount, KindCounter)
	r.reg(snode, LruEvictSize, KindSize)

	r.reg(snode, CleanupStoreCount, KindCounter)
	r.reg(snode, CleanupStoreSize, KindSize)

	r.reg(snode, VerChangeCount, KindCounter)
	r.reg(snode, VerChangeSize, KindSize)

	r.reg(snode, PutLatency, KindLatency)
	r.reg(snode, AppendLatency, KindLatency)
	r.reg(snode, GetRedirLatency, KindLatency)
	r.reg(snode, PutRedirLatency, KindLatency)
	r.reg(snode, GetColdRwLatency, KindLatency)

	// bps
	r.reg(snode, GetThroughput, KindThroughput)
	r.reg(snode, PutThroughput, KindThroughput)

	r.reg(snode, GetSize, KindSize)
	r.reg(snode, PutSize, KindSize)

	// errors
	r.reg(snode, ErrCksumCount, KindCounter)
	r.reg(snode, ErrCksumSize, KindSize)

	r.reg(snode, ErrMetadataCount, KindCounter)
	r.reg(snode, ErrIOCount, KindCounter)

	// streams
	r.reg(snode, cos.StreamsOutObjCount, KindCounter)
	r.reg(snode, cos.StreamsOutObjSize, KindSize)
	r.reg(snode, cos.StreamsInObjCount, KindCounter)
	r.reg(snode, cos.StreamsInObjSize, KindSize)

	// download
	r.reg(snode, DownloadSize, KindSize)
	r.reg(snode, DownloadLatency, KindLatency)

	// dsort
	r.reg(snode, DsortCreationReqCount, KindCounter)
	r.reg(snode, DsortCreationRespCount, KindCounter)
	r.reg(snode, DsortCreationRespLatency, KindLatency)
	r.reg(snode, DsortExtractShardDskCnt, KindCounter)
	r.reg(snode, DsortExtractShardMemCnt, KindCounter)
	r.reg(snode, DsortExtractShardSize, KindSize)

	// core
	r.reg(snode, RemoteDeletedDelCount, KindCounter)
	r.reg(snode, LcacheCollisionCount, KindCounter)
	r.reg(snode, LcacheEvictedCount, KindCounter)
	r.reg(snode, LcacheFlushColdCount, KindCounter)

	// Prometheus
	r.core.initProm(snode)
}

func (r *Trunner) RegDiskMetrics(snode *meta.Snode, disk string) {
	s, n := r.core.Tracker, nameRbps(disk)
	if _, ok := s[n]; ok { // must be config.TestingEnv()
		return
	}
	r.reg(snode, n, KindComputedThroughput)
	r.reg(snode, nameWbps(disk), KindComputedThroughput)

	r.reg(snode, nameRavg(disk), KindGauge)
	r.reg(snode, nameWavg(disk), KindGauge)
	r.reg(snode, nameUtil(disk), KindGauge)
}

func (r *Trunner) GetStats() (ds *Node) {
	ds = r.runner.GetStats()

	fs.InitCDF(&ds.TargetCDF)
	fs.CapRefresh(nil, &ds.TargetCDF)
	return ds
}

// [backward compatibility] v3.22 and prior
func (r *Trunner) GetStatsV322() (out *NodeV322) {
	ds := r.GetStats()

	out = &NodeV322{}
	out.Snode = ds.Snode
	out.Tracker = ds.Tracker
	out.TargetCDF.PctMax = ds.TargetCDF.PctMax
	out.TargetCDF.PctAvg = ds.TargetCDF.PctAvg
	out.TargetCDF.PctMin = ds.TargetCDF.PctMin
	out.TargetCDF.CsErr = ds.TargetCDF.CsErr
	out.TargetCDF.Mountpaths = make(map[string]*fs.CDFv322, len(ds.TargetCDF.Mountpaths))
	for mpath := range ds.TargetCDF.Mountpaths {
		cdf := &fs.CDFv322{
			Capacity: ds.TargetCDF.Mountpaths[mpath].Capacity,
			Disks:    ds.TargetCDF.Mountpaths[mpath].Disks,
			FS:       ds.TargetCDF.Mountpaths[mpath].FS.String(),
		}
		out.TargetCDF.Mountpaths[mpath] = cdf
	}
	return out
}

// log _and_ update various low-level states
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

	if now >= r.next || !idle {
		s.sgl.Reset() // sharing w/ CoreStats.copyT
		r.ctracker.write(s.sgl, r.sorted, true /*target*/, idle)
		if s.sgl.Len() > 3 { // skip '{}'
			line := string(s.sgl.Bytes())
			if line != r.prev {
				r.lines = append(r.lines, line)
				r.prev = line
			}
		}
	}

	// 3. capacity, mountpath alerts, and associated node state flags
	set, clr := r._cap(config, now)

	// 4. append disk stats to log subject to (idle) filtering (see related: `ignoreIdle`)
	r.logDiskStats(now)

	// 5. jobs
	verbose := cmn.Rom.FastV(4, cos.SmoduleStats)
	if !idle {
		var ln string
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
		if len(r.xallRun.Running) > 0 {
			ln = "running: " + strings.Join(r.xallRun.Running, " ")
			if len(r.xallRun.Idle) > 0 {
				ln += ";  idle: " + strings.Join(r.xallRun.Idle, " ")
			}
		} else if len(r.xallRun.Idle) > 0 {
			ln += "idle: " + strings.Join(r.xallRun.Idle, " ")
		}
		if ln != "" && ln != r.xln {
			r.lines = append(r.lines, ln)
		}
		r.xln = ln
	}

	// 6. log
	for _, ln := range r.lines {
		nlog.Infoln(ln)
	}

	// 7. separately, memory w/ set/clr flags cumulative
	r._mem(r.t.PageMM(), set, clr)

	if now > r.next {
		r.next = now + maxStatsLogInterval
	}
}

func (r *Trunner) _cap(config *cmn.Config, now int64) (set, clr cos.NodeStateFlags) {
	cs, updated, err, errCap := fs.CapPeriodic(now, config, &r.TargetCDF)
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
		hasAlerts = r.TargetCDF.HasAlerts()
	}

	// target to run x-space
	if errCap != nil {
		r.t.OOS(pcs, config, &r.TargetCDF)
	} else if cs.PctMax > int32(config.Space.CleanupWM) { // remove deleted, other cleanup
		debug.Assert(!cs.IsOOS(), cs.String())
		errCap = cmn.NewErrCapExceeded(cs.TotalUsed, cs.TotalAvail+cs.TotalUsed, 0, config.Space.CleanupWM, cs.PctMax, false)
		r.t.OOS(pcs, config, &r.TargetCDF)
	}

	//
	// (periodically | on error): log mountpath cap and state
	//
	if now >= min(r.next, r.cs.last+maxCapLogInterval) || errCap != nil || hasAlerts {
		fast := fs.NoneShared(len(r.TargetCDF.Mountpaths))
		unique := fast
		if !fast {
			r.fsIDs = r.fsIDs[:0]
		}
		for mpath, cdf := range r.TargetCDF.Mountpaths {
			if !fast {
				r.fsIDs, unique = cos.AddUniqueFsID(r.fsIDs, cdf.FS.FsID)
			}
			if unique { // to avoid log duplication
				var sb strings.Builder
				sb.WriteString(mpath)
				sb.WriteString(cdf.Label.ToLog())
				if alert, _ := cdf.HasAlert(); alert != "" {
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

	// and more
	flags := r.nodeStateFlags()
	if hasAlerts {
		r.lines = append(r.lines, "Warning: node-state-flags", flags.String(), "(check mountpath alerts!)")
	} else if flags.IsSet(cos.DiskFault) && updated {
		clr |= cos.DiskFault
	}

	// cap alert
	if cs.IsOOS() {
		set = cos.OOS
	} else if cs.Err() != nil {
		clr = cos.OOS
		set |= cos.LowCapacity
	} else {
		clr = cos.OOS | cos.LowCapacity
	}
	return set, clr
}

// log formatted disk stats:
// [ disk: read throughput, average read size, write throughput, average write size, disk utilization ]
// e.g.: [ sda: 94MiB/s, 68KiB, 25MiB/s, 21KiB, 82% ]
func (r *Trunner) logDiskStats(now int64) {
	for disk, stats := range r.disk {
		if stats.Util < minLogDiskUtil/2 || (stats.Util < minLogDiskUtil && now < r.next) {
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

func (r *Trunner) statsTime(newval time.Duration) {
	r.core.statsTime = newval
}

func (r *Trunner) standingBy() bool { return r.standby }
