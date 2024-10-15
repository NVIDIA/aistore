// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"encoding/json"
	iofs "io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	ratomic "sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/sys"
	jsoniter "github.com/json-iterator/go"
)

// Linkage:
// - this source is common for both Prometheus (common_prom.go) and StatsD (common_statsd.go)
// - one of the two pairs (common, common_prom) OR (common, common_statsd) gets compiled with
//   both Proxy (proxy_stats.go) and Target (target_stats.go)

// defaults and tunables
const (
	dfltKaliveClearAlert  = 5 * time.Minute      // clear `cos.KeepAliveErrors` alert when `ErrKaliveCount` doesn't inc that much time
	dfltPeriodicFlushTime = time.Minute          // when `config.Log.FlushTime` is 0 (zero)
	dfltPeriodicTimeStamp = time.Hour            // extended date/time complementary to log timestamps (e.g., "11:29:11.644596")
	dfltStatsLogInterval  = int64(time.Minute)   // stats logging interval when not idle; `config.Log.StatsTime` takes precedence if defined
	dlftCapLogInterval    = int64(4 * time.Hour) // capacity logging interval
)

// periodic
const (
	maxLogSizeCheckTime = time.Hour              // periodically check the logs for max accumulated size
	startupSleep        = 300 * time.Millisecond // periodically poll ClusterStarted()
)

const (
	ngrHighTime    = 10 * time.Minute // log a warning if the number of goroutines remains high
	ngrExtremeTime = 5 * time.Minute  // when more then twice the maximum (below)
	lshiftGorHigh  = 8                // max expressed as left shift of the num CPUs
)

// [naming convention] error counter prefixes
const (
	errPrefix   = "err."    // all error metric names (see `IsErrMetric` below)
	ioErrPrefix = "err.io." // excluding connection-reset-by-peer and similar (see ioErrNames)
)

// metrics
const (
	// KindCounter:
	// all basic counters are accompanied by the corresponding (errPrefix + kind) error count:
	// e.g.: "get.n" => "err.get.n", "put.n" => "err.put.n", etc.
	GetCount    = "get.n" // GET(object) count = (cold + warm)
	PutCount    = "put.n" // ditto PUT
	HeadCount   = "head.n"
	AppendCount = "append.n"
	DeleteCount = "del.n"
	RenameCount = "ren.n"
	ListCount   = "lst.n" // list-objects

	// error counters
	// see also: `IncErr`, `regCommon`, `ioErrNames`
	ErrGetCount    = errPrefix + GetCount
	ErrPutCount    = errPrefix + PutCount
	ErrHeadCount   = errPrefix + HeadCount
	ErrAppendCount = errPrefix + AppendCount
	ErrDeleteCount = errPrefix + DeleteCount
	ErrRenameCount = errPrefix + RenameCount
	ErrListCount   = errPrefix + ListCount

	ErrKaliveCount = errPrefix + "kalive.n"

	// more errors
	// (for even more errors, see target_stats)
	ErrHTTPWriteCount = errPrefix + "http.write.n"
	ErrDownloadCount  = errPrefix + "dl.n"
	ErrPutMirrorCount = errPrefix + "put.mirror.n"

	// KindLatency
	// latency stats have numSamples used to compute average latency
	GetLatency         = "get.ns"
	GetLatencyTotal    = "get.ns.total"
	GetE2ELatencyTotal = "e2e.get.ns.total" // // e2e cold-GET latency
	ListLatency        = "lst.ns"
	KeepAliveLatency   = "kalive.ns"

	// KindSpecial
	Uptime = "up.ns.time"

	// KindGauge, cos.NodeStateFlags enum
	NodeAlerts = cos.NodeAlerts // "state.flags"
)

// interfaces
type (
	// implemented by the stats runners
	statsLogger interface {
		log(now int64, uptime time.Duration, config *cmn.Config)
		statsTime(newval time.Duration)
		standingBy() bool
	}
)

// primitives: values and maps
type (
	copyValue struct {
		Value int64 `json:"v,string"`
	}
	copyTracker map[string]copyValue // aggregated every statsTime interval
)

// common part: Prunner and Trunner, both
type (
	runner struct {
		node      core.Node
		stopCh    chan struct{}
		ticker    *time.Ticker
		core      *coreStats
		ctracker  copyTracker // to avoid making it at runtime
		sorted    []string    // sorted names
		name      string      // this stats-runner's name
		prev      string      // prev ctracker.write
		next      int64       // mono.Nano
		mem       sys.MemStat
		startedUp atomic.Bool
	}
)

var ignoreIdle = [...]string{"kalive", Uptime, "disk."}

////////////
// runner //
////////////

func (r *runner) RegExtMetric(snode *meta.Snode, name, kind string, extra *Extra) {
	r.reg(snode, name, kind, extra)
}

// common (target, proxy) metrics
func (r *runner) regCommon(snode *meta.Snode) {
	// prometheus only
	initDfltlabel(snode)

	// basic counters
	r.reg(snode, GetCount, KindCounter,
		&Extra{
			Help: "total number of executed GET(object) requests",
		},
	)
	r.reg(snode, PutCount, KindCounter,
		&Extra{
			Help: "total number of executed PUT(object) requests",
		},
	)
	r.reg(snode, HeadCount, KindCounter,
		&Extra{
			Help: "total number of executed HEAD(object) requests", // NOTE: currently, we only count remote ("cold") HEAD
		},
	)
	r.reg(snode, AppendCount, KindCounter,
		&Extra{
			Help: "total number of executed APPEND(object) requests",
		},
	)
	r.reg(snode, DeleteCount, KindCounter,
		&Extra{
			Help: "total number of executed DELETE(object) requests",
		},
	)
	r.reg(snode, RenameCount, KindCounter,
		&Extra{
			Help: "total number of executed rename(object) requests",
		},
	)
	r.reg(snode, ListCount, KindCounter,
		&Extra{
			Help: "total number of executed list-objects requests",
		},
	)

	// basic error counters, respectively
	r.reg(snode, ErrGetCount, KindCounter,
		&Extra{
			Help: "total number of GET(object) errors",
		},
	)
	r.reg(snode, ErrPutCount, KindCounter,
		&Extra{
			Help: "total number of PUT(object) errors",
		},
	)
	r.reg(snode, ErrHeadCount, KindCounter,
		&Extra{
			Help: "total number of HEAD(object) errors", // ditto (HeadCount above)
		},
	)
	r.reg(snode, ErrAppendCount, KindCounter,
		&Extra{
			Help: "total number of APPEND(object) errors",
		},
	)
	r.reg(snode, ErrDeleteCount, KindCounter,
		&Extra{
			Help: "total number of DELETE(object) errors",
		},
	)
	r.reg(snode, ErrRenameCount, KindCounter,
		&Extra{
			Help: "total number of rename(object) errors",
		},
	)
	r.reg(snode, ErrListCount, KindCounter,
		&Extra{
			Help: "total number of list-objects errors",
		},
	)
	r.reg(snode, ErrKaliveCount, KindCounter,
		&Extra{
			Help: "total number of keep-alive failures",
		},
	)

	// even more error counters
	r.reg(snode, ErrHTTPWriteCount, KindCounter,
		&Extra{
			Help: "total number of HTTP write-response errors",
		},
	)
	r.reg(snode, ErrDownloadCount, KindCounter,
		&Extra{
			Help: "downloader: number of download errors",
		},
	)
	r.reg(snode, ErrPutMirrorCount, KindCounter,
		&Extra{
			Help: "number of n-way mirroring errors",
		},
	)

	// basic latencies
	r.reg(snode, GetLatency, KindLatency,
		&Extra{
			Help: "GET: average time (milliseconds) over the last periodic.stats_time interval",
		},
	)
	r.reg(snode, GetLatencyTotal, KindTotal,
		&Extra{
			Help: "GET: total cumulative time (nanoseconds)",
		},
	)
	r.reg(snode, ListLatency, KindLatency,
		&Extra{
			Help: "list-objects: average time (milliseconds) over the last periodic.stats_time interval",
		},
	)
	r.reg(snode, KeepAliveLatency, KindLatency,
		&Extra{
			Help: "in-cluster keep-alive (heartbeat): average time (milliseconds) over the last periodic.stats_time interval",
		},
	)

	// special uptime
	r.reg(snode, Uptime, KindSpecial,
		&Extra{
			Help:    "this node's uptime since its startup (seconds)",
			StrName: "uptime",
		},
	)

	// snode state flags
	r.reg(snode, NodeAlerts, KindGauge,
		&Extra{
			Help: "bitwise 64-bit value that carries enumerated node-state flags, including warnings and alerts; " +
				"see https://github.com/NVIDIA/aistore/blob/main/cmn/cos/node_state.go for details", // TODO: must have a readme
		},
	)
}

//
// as cos.StatsUpdater
//

func (r *runner) Add(name string, val int64) {
	r.core.update(cos.NamedVal64{Name: name, Value: val})
}

func (r *runner) Inc(name string) {
	r.core.update(cos.NamedVal64{Name: name, Value: 1})
}

// same as above (readability)
func (r *runner) IncErr(metric string) {
	debug.Assert(strings.HasPrefix(metric, errPrefix), metric)
	r.core.update(cos.NamedVal64{Name: metric, Value: 1})
}

func (r *runner) AddMany(nvs ...cos.NamedVal64) {
	for _, nv := range nvs {
		r.core.update(nv)
	}
}

func (r *runner) SetFlag(name string, set cos.NodeStateFlags) {
	v := r.core.Tracker[name]
	oval := ratomic.LoadInt64(&v.Value)
	nval := oval | int64(set)
	ratomic.StoreInt64(&v.Value, nval)
}

func (r *runner) ClrFlag(name string, clr cos.NodeStateFlags) {
	v := r.core.Tracker[name]
	oval := ratomic.LoadInt64(&v.Value)
	nval := oval &^ int64(clr)
	ratomic.StoreInt64(&v.Value, nval)
}

func (r *runner) SetClrFlag(name string, set, clr cos.NodeStateFlags) {
	v := r.core.Tracker[name]
	oval := ratomic.LoadInt64(&v.Value)
	nval := oval | int64(set)
	if cos.NodeStateFlags(nval).IsOK() && cos.NodeStateFlags(oval).IsOK() {
		return
	}
	nval &^= int64(clr)
	ratomic.StoreInt64(&v.Value, nval)
}

func (r *runner) Name() string { return r.name }

func (r *runner) Get(name string) (val int64) { return r.core.get(name) }

func (r *runner) nodeStateFlags() cos.NodeStateFlags {
	val := r.Get(NodeAlerts)
	return cos.NodeStateFlags(val)
}

func (r *runner) _next(config *cmn.Config, now int64) {
	if config.Log.StatsTime >= config.Periodic.StatsTime {
		r.next = now + int64(config.Log.StatsTime)
	} else {
		r.next = now + dfltStatsLogInterval
	}
}

func (r *runner) _run(logger statsLogger) error {
	var (
		i, j, k time.Duration
		sleep   = startupSleep
		ticker  = time.NewTicker(sleep)

		// NOTE: the maximum time we agree to wait for r.daemon.ClusterStarted()
		config   = cmn.GCO.Get()
		deadline = config.Timeout.JoinAtStartup.D()
	)
	if logger.standingBy() {
		deadline = hk.DayInterval
	} else if deadline == 0 {
		deadline = 2 * config.Timeout.Startup.D()
	}
waitStartup:
	for {
		select {
		case <-r.stopCh:
			ticker.Stop()
			return nil
		case <-ticker.C:
			k += sleep
			if k >= config.Periodic.StatsTime.D() {
				nlog.Flush(nlog.ActNone)
				k = 0
			}
			if r.node.ClusterStarted() {
				break waitStartup
			}
			if logger.standingBy() && sleep == startupSleep /*first time*/ {
				sleep = config.Periodic.StatsTime.D()
				ticker.Reset(sleep)
				deadline = time.Hour

				nlog.Infoln(r.Name() + ": standing by...")
				continue
			}
			j += sleep
			if j > deadline {
				ticker.Stop()
				return cmn.ErrStartupTimeout
			}
			i += sleep
			if i > config.Timeout.Startup.D() && !logger.standingBy() {
				nlog.Errorln(r.Name() + ": " + cmn.StartupMayTimeout)
				i = 0
			}
		}
	}
	ticker.Stop()

	config = cmn.GCO.Get()
	goMaxProcs := runtime.GOMAXPROCS(0)
	nlog.Infoln("Starting", r.Name())
	hk.Reg(r.Name()+"-logs"+hk.NameSuffix, hkLogs, maxLogSizeCheckTime)

	statsTime := config.Periodic.StatsTime.D() // (NOTE: not to confuse with config.Log.StatsTime)
	r.ticker = time.NewTicker(statsTime)
	r.startedUp.Store(true)

	// one or the other, depending on the build tag
	r.core.initStatsdOrProm(r.node.Snode(), r)

	var (
		lastNgr           int64
		lastKaliveErrInc  int64
		kaliveErrs        int64
		startTime         = mono.NanoTime() // uptime henceforth
		lastDateTimestamp = startTime       // RFC822
	)
	for {
		select {
		case <-r.ticker.C:
			now := mono.NanoTime()
			config = cmn.GCO.Get()
			logger.log(now, time.Duration(now-startTime) /*uptime*/, config)

			// 1. "High number of"
			lastNgr = r.checkNgr(now, lastNgr, goMaxProcs)

			if statsTime != config.Periodic.StatsTime.D() {
				statsTime = config.Periodic.StatsTime.D()
				r.ticker.Reset(statsTime)
				logger.statsTime(statsTime)
			}

			// 2. flush logs (NOTE: stats runner is solely responsible)
			flushTime := cos.NonZero(config.Log.FlushTime.D(), dfltPeriodicFlushTime)
			if nlog.Since(now) > flushTime || nlog.OOB() {
				nlog.Flush(nlog.ActNone)
			}

			// 3. dated time => info log
			if time.Duration(now-lastDateTimestamp) > dfltPeriodicTimeStamp {
				nlog.Infoln(cos.FormatTime(time.Now(), "" /* RFC822 */) + " =============")
				lastDateTimestamp = now
			}

			// 4. kalive alert
			n := r.Get(ErrKaliveCount)
			if n != kaliveErrs {
				// raise
				lastKaliveErrInc = now
				if n > kaliveErrs { // vs. 'reset errors-only'
					r.SetFlag(NodeAlerts, cos.KeepAliveErrors)
				}
				kaliveErrs = n
			} else if n > 0 && time.Duration(now-lastKaliveErrInc) > dfltKaliveClearAlert {
				// clear
				r.ClrFlag(NodeAlerts, cos.KeepAliveErrors)
			}
		case <-r.stopCh:
			r.ticker.Stop()
			return nil
		}
	}
}

func (r *runner) StartedUp() bool { return r.startedUp.Load() }

// - check OOM, and
// - set NodeStateFlags with both capacity and memory flags
func (r *runner) _mem(mm *memsys.MMSA, set, clr cos.NodeStateFlags) {
	_ = r.mem.Get()
	pressure := mm.Pressure(&r.mem)

	switch {
	case pressure >= memsys.PressureExtreme:
		set |= cos.OOM
		nlog.Errorln(mm.Str(&r.mem))
	case pressure >= memsys.PressureHigh:
		set |= cos.LowMemory
		clr |= cos.OOM
		nlog.Warningln(mm.Str(&r.mem))
	default:
		clr |= cos.OOM | cos.LowMemory
	}
	r.SetClrFlag(NodeAlerts, set, clr)
}

func (r *runner) GetStats() *Node {
	ctracker := make(copyTracker, 48)
	r.core.copyCumulative(ctracker)
	return &Node{Tracker: ctracker}
}

func (r *runner) GetStatsV322() (out *NodeV322) {
	ds := r.GetStats()

	out = &NodeV322{}
	out.Snode = ds.Snode
	out.Tracker = ds.Tracker
	return out
}

func (r *runner) ResetStats(errorsOnly bool) {
	r.core.reset(errorsOnly)
}

func (r *runner) GetMetricNames() cos.StrKVs {
	out := make(cos.StrKVs, 32)
	for name, v := range r.core.Tracker {
		out[name] = v.kind
	}
	return out
}

func (r *runner) checkNgr(now, lastNgr int64, goMaxProcs int) int64 {
	lim := goMaxProcs << lshiftGorHigh
	ngr := runtime.NumGoroutine()
	if ngr < lim {
		if lastNgr != 0 {
			r.ClrFlag(NodeAlerts, cos.NumGoroutines)
			nlog.Infoln("Number of goroutines is now back to normal:", ngr)
		}
		return 0
	}
	if lastNgr == 0 {
		r.SetFlag(NodeAlerts, cos.NumGoroutines)
		lastNgr = now
	} else if d := time.Duration(now - lastNgr); (d >= ngrHighTime) || (ngr > lim<<1 && d >= ngrExtremeTime) {
		lastNgr = now
	}
	if lastNgr == now {
		nlog.Warningln("High number of goroutines:", ngr)
	}
	return lastNgr
}

////////////////
// statsValue //
////////////////

// interface guard
var (
	_ json.Marshaler   = (*statsValue)(nil)
	_ json.Unmarshaler = (*statsValue)(nil)
)

func (v *statsValue) MarshalJSON() ([]byte, error) {
	s := strconv.FormatInt(ratomic.LoadInt64(&v.Value), 10)
	return cos.UnsafeB(s), nil
}

func (v *statsValue) UnmarshalJSON(b []byte) error { return jsoniter.Unmarshal(b, &v.Value) }

///////////////
// copyValue //
///////////////

// interface guard
var (
	_ json.Marshaler   = (*copyValue)(nil)
	_ json.Unmarshaler = (*copyValue)(nil)
)

func (v copyValue) MarshalJSON() (b []byte, err error) { return jsoniter.Marshal(v.Value) }
func (v *copyValue) UnmarshalJSON(b []byte) error      { return jsoniter.Unmarshal(b, &v.Value) }

/////////////////
// copyTracker //
/////////////////

// serialize itself (slightly more efficiently than JSON)
func (ctracker copyTracker) write(sgl *memsys.SGL, sorted []string, target, idle bool) {
	var (
		next  bool
		disks bool // whether to write target disk metrics
	)
	if len(sorted) == 0 {
		for n := range ctracker {
			sorted = append(sorted, n)
		}
		sort.Strings(sorted)
	}
	sgl.WriteByte('{')
	for _, n := range sorted {
		v := ctracker[n]
		// exclude
		if v.Value == 0 || n == Uptime { // always skip zeros and uptime
			continue
		}
		if isDiskMetric(n) {
			if isDiskUtilMetric(n) && v.Value > minLogDiskUtil {
				disks = true // not idle - all all
			}
			continue
		}
		if idle && n == KeepAliveLatency {
			continue
		}
		// add
		if next {
			sgl.WriteByte(',')
		}
		sgl.Write(cos.UnsafeB(n))
		sgl.WriteByte(':')
		sgl.Write(cos.UnsafeB(strconv.FormatInt(v.Value, 10))) // raw value
		next = true
	}
	if disks {
		debug.Assert(target)
		for n, v := range ctracker {
			if v.Value == 0 || !isDiskMetric(n) {
				continue
			}
			sgl.WriteByte(',')
			sgl.Write(cos.UnsafeB(n))
			sgl.WriteByte(':')
			sgl.Write(cos.UnsafeB(strconv.FormatInt(v.Value, 10))) // ditto
		}
	}
	sgl.WriteByte('}')
}

//
// log rotation and GC
//

const gcLogs = "GC logs:"

// keep total log size below the configured max
func hkLogs(int64) time.Duration {
	var (
		config   = cmn.GCO.Get()
		maxtotal = int64(config.Log.MaxTotal)
		logdir   = config.LogDir
	)
	dentries, err := os.ReadDir(logdir)
	if err != nil {
		nlog.Errorln(gcLogs, "cannot read log dir", logdir, "err:", err)
		_ = cos.CreateDir(logdir) // (local non-containerized + kill/restart under test)
		return maxLogSizeCheckTime
	}

	var (
		tot     int64
		n       = len(dentries)
		nn      = n - n>>2
		finfos  = make([]iofs.FileInfo, 0, nn)
		verbose = cmn.Rom.FastV(4, cos.SmoduleStats)
	)
	for i, logtype := range []string{".INFO.", ".ERROR."} {
		finfos, tot = _sizeLogs(dentries, logtype, finfos)
		l := len(finfos)
		switch {
		case tot < maxtotal:
			if verbose {
				nlog.Infoln(gcLogs, "skipping:", logtype, "total:", tot, "max:", maxtotal)
			}
		case l > 1:
			go _rmLogs(tot, maxtotal, logdir, logtype, finfos)
			if i == 0 {
				finfos = make([]iofs.FileInfo, 0, nn)
			}
		default:
			nlog.Warningln(gcLogs, "cannot cleanup a single large", logtype, "size:", tot, "configured max:", maxtotal)
			debug.Assert(l == 1)
			for _, finfo := range finfos {
				nlog.Warningln("\t>>>", gcLogs, filepath.Join(logdir, finfo.Name()))
			}
		}
	}

	return maxLogSizeCheckTime
}

// e.g. name: ais.ip-10-0-2-19.root.log.INFO.20180404-031540.2249
// see also: nlog.InfoLogName, nlog.ErrLogName
func _sizeLogs(dentries []os.DirEntry, logtype string, finfos []iofs.FileInfo) (_ []iofs.FileInfo, tot int64) {
	clear(finfos)
	finfos = finfos[:0]
	for _, dent := range dentries {
		if !dent.Type().IsRegular() {
			continue
		}
		if n := dent.Name(); !strings.Contains(n, logtype) {
			continue
		}
		if finfo, err := dent.Info(); err == nil {
			tot += finfo.Size()
			finfos = append(finfos, finfo)
		}
	}
	return finfos, tot
}

func _rmLogs(tot, maxtotal int64, logdir, logtype string, finfos []iofs.FileInfo) {
	less := func(i, j int) bool {
		return finfos[i].ModTime().Before(finfos[j].ModTime())
	}
	l := len(finfos)
	verbose := cmn.Rom.FastV(4, cos.SmoduleStats)
	if verbose {
		nlog.Infoln(gcLogs, logtype, "total:", tot, "max:", maxtotal, "num:", l)
	}
	sort.Slice(finfos, less)
	finfos = finfos[:l-1] // except the last, i.e. current

	for _, finfo := range finfos {
		fqn := filepath.Join(logdir, finfo.Name())
		if err := cos.RemoveFile(fqn); err == nil {
			tot -= finfo.Size()
			if verbose {
				nlog.Infoln(gcLogs, "removed", fqn)
			}
			if tot < maxtotal {
				break
			}
		} else {
			nlog.Errorln(gcLogs, "failed to remove", fqn, "err:", err)
		}
	}
	nlog.Infoln(gcLogs, "done, new total:", tot)

	clear(finfos)
	finfos = finfos[:0]
}

//
// common helpers
//

func ignore(s string) bool {
	for _, p := range ignoreIdle {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

// convert bytes to meGabytes with a fixed rounding precision = 2 digits
// - KindThroughput and KindComputedThroughput only
// - MB, not MiB
// - math.Ceil wouldn't produce two decimals
func roundMBs(val int64) (mbs float64) {
	mbs = float64(val) / 1000 / 10
	num := int(mbs + 0.5)
	mbs = float64(num) / 100
	return
}
