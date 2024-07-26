// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"encoding/json"
	rfs "io/fs"
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
	dfltPeriodicFlushTime = time.Minute            // when `config.Log.FlushTime` is 0 (zero)
	dfltPeriodicTimeStamp = time.Hour              // extended date/time complementary to log timestamps (e.g., "11:29:11.644596")
	maxStatsLogInterval   = int64(3 * time.Minute) // when idle; secondly, an upper limit on `config.Log.StatsTime`
	maxCapLogInterval     = int64(4 * time.Hour)   // to see capacity at least few times a day (when idle)
)

// periodic
const (
	maxLogSizeCheckTime = time.Hour              // periodically check the logs for max accumulated size
	startupSleep        = 300 * time.Millisecond // periodically poll ClusterStarted()
	numGorHighCheckTime = 10 * time.Minute       // periodically log a warning if the number of goroutines remains high
)

// number-of-goroutines watermarks expressed as multipliers over the number of CPUs (GOMAXPROCS)
const (
	numGorHigh    = 100
	numGorExtreme = 1000
)

// metrics
const (
	// KindCounter:
	// all basic counters are accompanied by the corresponding (errPrefix + kind) error count:
	// e.g.: "get.n" => "err.get.n", "put.n" => "err.put.n", etc.
	//
	// See also: `IncErr`, `IncNonIOErr`, `regCommon`
	// See also: `softErrNames`
	GetCount    = "get.n"    // GET(object) count = (cold + warm)
	PutCount    = "put.n"    // ditto PUT
	AppendCount = "append.n" // ditto etc.
	DeleteCount = "del.n"    // ditto
	RenameCount = "ren.n"    // ditto
	ListCount   = "lst.n"    // list-objects

	// statically defined err counts (NOTE: update regCommon when adding/updating)
	ErrHTTPWriteCount = errPrefix + "http.write.n"
	ErrDownloadCount  = errPrefix + "dl.n"
	ErrPutMirrorCount = errPrefix + "put.mirror.n"

	// KindLatency
	GetLatency         = "get.ns"
	GetLatencyTotal    = "get.ns.total"
	GetE2ELatencyTotal = "e2e.get.ns.total" // // e2e cold-GET latency
	ListLatency        = "lst.ns"
	KeepAliveLatency   = "kalive.ns"

	// KindSpecial
	Uptime = "up.ns.time"

	// KindGauge, cos.NodeStateFlags enum
	NodeStateFlags = "state.flags"
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
	// Stats are tracked via a map of stats names (key) to statsValue (values).
	// There are two main types of stats: counter and latency declared
	// using the the kind field. Only latency stats have numSamples used to compute latency.
	statsValue struct {
		kind  string // enum { KindCounter, ..., KindSpecial }
		label struct {
			comm string // common part of the metric label (as in: <prefix> . comm . <suffix>)
			stpr string // StatsD _or_ Prometheus label (depending on build tag)
		}
		Value      int64 `json:"v,string"`
		numSamples int64 // (log + StatsD) only
		cumulative int64
	}
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
		nonIOErr  int64
		mem       sys.MemStat
		startedUp atomic.Bool
	}
)

// sample name ais.ip-10-0-2-19.root.log.INFO.20180404-031540.2249
var logtypes = [...]string{".INFO.", ".WARNING.", ".ERROR."}

var ignoreIdle = [...]string{"kalive", Uptime, "disk."}

var softErrNames = [...]string{errPrefix + GetCount, errPrefix + PutCount, errPrefix + DeleteCount}

////////////
// runner //
////////////

// soft IO error correction, whereby (the corrected)
// number of soft IO errors = number of (GET, PUT, DELETE errors) - r.nonIOErr
func (r *runner) IncNonIOErr() { ratomic.AddInt64(&r.nonIOErr, 1) }

func (r *runner) InitPrometheus(snode *meta.Snode) {
	r.core.initProm(snode)
}

func (r *runner) RegExtMetric(snode *meta.Snode, name, kind string) { r.reg(snode, name, kind) }

// common (target, proxy) metrics
func (r *runner) regCommon(snode *meta.Snode) {
	// basic counters
	r.reg(snode, GetCount, KindCounter)
	r.reg(snode, PutCount, KindCounter)
	r.reg(snode, AppendCount, KindCounter)
	r.reg(snode, DeleteCount, KindCounter)
	r.reg(snode, RenameCount, KindCounter)
	r.reg(snode, ListCount, KindCounter)

	// basic error counters, respectively
	r.reg(snode, errPrefix+GetCount, KindCounter)
	r.reg(snode, errPrefix+PutCount, KindCounter)
	r.reg(snode, errPrefix+AppendCount, KindCounter)
	r.reg(snode, errPrefix+DeleteCount, KindCounter)
	r.reg(snode, errPrefix+RenameCount, KindCounter)
	r.reg(snode, errPrefix+ListCount, KindCounter)

	// more error counters
	r.reg(snode, ErrHTTPWriteCount, KindCounter)
	r.reg(snode, ErrDownloadCount, KindCounter)
	r.reg(snode, ErrPutMirrorCount, KindCounter)

	// latency
	r.reg(snode, GetLatency, KindLatency)
	r.reg(snode, GetLatencyTotal, KindTotal)
	r.reg(snode, ListLatency, KindLatency)
	r.reg(snode, KeepAliveLatency, KindLatency)

	// special uptime
	r.reg(snode, Uptime, KindSpecial)

	// snode state flags
	r.reg(snode, NodeStateFlags, KindGauge)
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

func (r *runner) IncErr(metric string) {
	if IsErrMetric(metric) {
		r.core.update(cos.NamedVal64{Name: metric, Value: 1})
	} else { // e.g. "err." + GetCount
		r.core.update(cos.NamedVal64{Name: errPrefix + metric, Value: 1})
	}
}

func (r *runner) AddMany(nvs ...cos.NamedVal64) {
	for _, nv := range nvs {
		r.core.update(nv)
	}
}

func (r *runner) SetFlag(name string, set cos.NodeStateFlags) {
	v := r.core.Tracker[name]
	oval := cos.BitFlags(ratomic.LoadInt64(&v.Value))
	nval := oval | cos.BitFlags(set)
	ratomic.StoreInt64(&v.Value, int64(nval))
}

func (r *runner) SetClrFlag(name string, set, clr cos.NodeStateFlags) {
	v := r.core.Tracker[name]
	oval := cos.BitFlags(ratomic.LoadInt64(&v.Value))
	nval := oval | cos.BitFlags(set)
	if cos.NodeStateFlags(nval).IsOK() && cos.NodeStateFlags(oval).IsOK() {
		return
	}
	nval &^= cos.BitFlags(clr)
	ratomic.StoreInt64(&v.Value, int64(nval))
}

func (r *runner) Name() string { return r.name }

func (r *runner) Get(name string) (val int64) { return r.core.get(name) }

func (r *runner) nodeStateFlags() cos.NodeStateFlags {
	val := r.Get(NodeStateFlags)
	return cos.NodeStateFlags(val)
}

func (r *runner) numSoftErrs() (n int64) {
	for _, name := range softErrNames {
		n += r.Get(name)
	}
	return n
}

func (r *runner) _run(logger statsLogger /* Prunner or Trunner */) error {
	var (
		i, j, k time.Duration
		sleep   = startupSleep
		ticker  = time.NewTicker(sleep)

		// NOTE: the maximum time we agree to wait for r.daemon.ClusterStarted()
		config   = cmn.GCO.Get()
		deadline = config.Timeout.JoinAtStartup.D()
	)
	if logger.standingBy() {
		deadline = 24 * time.Hour
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
	nlog.Infof("Starting %s", r.Name())
	hk.Reg(r.Name()+"-logs"+hk.NameSuffix, recycleLogs, maxLogSizeCheckTime)

	statsTime := config.Periodic.StatsTime.D() // (NOTE: not to confuse with config.Log.StatsTime)
	r.ticker = time.NewTicker(statsTime)
	r.startedUp.Store(true)
	var (
		checkNumGorHigh   int64
		startTime         = mono.NanoTime() // uptime henceforth
		lastDateTimestamp = startTime
	)
	for {
		select {
		case <-r.ticker.C:
			now := mono.NanoTime()
			config = cmn.GCO.Get()
			logger.log(now, time.Duration(now-startTime) /*uptime*/, config)
			checkNumGorHigh = _checkGor(now, checkNumGorHigh, goMaxProcs)

			if statsTime != config.Periodic.StatsTime.D() {
				statsTime = config.Periodic.StatsTime.D()
				r.ticker.Reset(statsTime)
				logger.statsTime(statsTime)
			}
			// stats runner is now solely responsible to flush the logs
			// both periodically and on (OOB) demand
			flushTime := dfltPeriodicFlushTime
			if config.Log.FlushTime != 0 {
				flushTime = config.Log.FlushTime.D()
			}
			if nlog.Since() > flushTime || nlog.OOB() {
				nlog.Flush(nlog.ActNone)
			}

			now = mono.NanoTime()
			if time.Duration(now-lastDateTimestamp) > dfltPeriodicTimeStamp {
				nlog.Infoln(cos.FormatTime(time.Now(), "" /* RFC822 */) + " =============")
				lastDateTimestamp = now
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
	r.SetClrFlag(NodeStateFlags, set, clr)
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

func recycleLogs() time.Duration {
	// keep total log size below the configured max
	go removeLogs(cmn.GCO.Get())
	return maxLogSizeCheckTime
}

func removeLogs(config *cmn.Config) {
	maxtotal := int64(config.Log.MaxTotal)
	dentries, err := os.ReadDir(config.LogDir)
	if err != nil {
		nlog.Errorln(gcLogs, "cannot read log dir", config.LogDir, "err:", err)
		_ = cos.CreateDir(config.LogDir) // (local non-containerized + kill/restart under test)
		return
	}
	for _, logtype := range logtypes {
		var tot int64
		finfos := make([]rfs.FileInfo, 0, len(dentries))
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
		if tot > maxtotal {
			removeOlderLogs(tot, maxtotal, config.LogDir, logtype, finfos)
		} else if cmn.Rom.FastV(4, cos.SmoduleStats) {
			nlog.Infoln(gcLogs, "skipping log type:", logtype, "total:", tot, "max:", maxtotal)
		}
	}
}

func removeOlderLogs(tot, maxtotal int64, logdir, logtype string, filteredInfos []rfs.FileInfo) {
	l := len(filteredInfos)
	if l <= 1 {
		nlog.Warningln(gcLogs, "cannot cleanup", logtype, "dir:", logdir, "total:", tot, "max:", maxtotal)
		return
	}
	fiLess := func(i, j int) bool {
		return filteredInfos[i].ModTime().Before(filteredInfos[j].ModTime())
	}

	verbose := cmn.Rom.FastV(4, cos.SmoduleStats)
	if verbose {
		nlog.Infoln(gcLogs, "started for log type:", logtype)
	}
	sort.Slice(filteredInfos, fiLess)
	filteredInfos = filteredInfos[:l-1] // except the last = current
	for _, logfi := range filteredInfos {
		logfqn := filepath.Join(logdir, logfi.Name())
		if err := cos.RemoveFile(logfqn); err == nil {
			tot -= logfi.Size()
			if verbose {
				nlog.Infoln(gcLogs, "removed", logfqn)
			}
			if tot < maxtotal {
				break
			}
		} else {
			nlog.Errorln(gcLogs, "failed to remove", logfqn, "err:", err)
		}
	}
	nlog.Infoln(gcLogs, "done, new total:", tot)
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

func _checkGor(now, checkNumGorHigh int64, goMaxProcs int) int64 {
	var (
		ngr     = runtime.NumGoroutine()
		extreme bool
	)
	if ngr < goMaxProcs*numGorHigh {
		return 0
	}
	if ngr >= goMaxProcs*numGorExtreme {
		extreme = true
		nlog.Errorf("Extremely high number of goroutines: %d", ngr)
	}
	if checkNumGorHigh == 0 {
		checkNumGorHigh = now
	} else if time.Duration(now-checkNumGorHigh) > numGorHighCheckTime {
		if !extreme {
			nlog.Warningf("High number of goroutines: %d", ngr)
		}
		checkNumGorHigh = 0
	}
	return checkNumGorHigh
}
