//go:build statsd

// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"encoding/json"
	"fmt"
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
	"github.com/NVIDIA/aistore/stats/statsd"
	"github.com/NVIDIA/aistore/sys"
	jsoniter "github.com/json-iterator/go"
)

type (
	metric = statsd.Metric // type alias
)

// main types
type (
	coreStats struct {
		Tracker   map[string]*statsValue
		statsdC   *statsd.Client
		sgl       *memsys.SGL
		statsTime time.Duration
	}

	// Prunner and Trunner
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

// interface guard
var (
	_ Tracker = (*Prunner)(nil)
	_ Tracker = (*Trunner)(nil)
)

///////////////
// coreStats //
///////////////

// interface guard
var (
	_ json.Marshaler   = (*coreStats)(nil)
	_ json.Unmarshaler = (*coreStats)(nil)
)

func (s *coreStats) init(size int) {
	s.Tracker = make(map[string]*statsValue, size)

	s.sgl = memsys.PageMM().NewSGL(memsys.PageSize)
}

// NOTE: nil StatsD client means that we provide metrics to Prometheus (see below)
func (s *coreStats) statsdDisabled() bool { return s.statsdC == nil }

func (*coreStats) initProm(*meta.Snode) {}
func (*coreStats) promLock()            {}
func (*coreStats) promUnlock()          {}

// init MetricClient client: StatsD (default) or Prometheus
func (s *coreStats) initMetricClient(snode *meta.Snode, _ *runner) {
	var (
		port  = 8125  // StatsD default port, see https://github.com/etsy/stats
		probe = false // test-probe StatsD server at init time
	)
	if portStr := os.Getenv("AIS_STATSD_PORT"); portStr != "" {
		if portNum, err := cmn.ParsePort(portStr); err != nil {
			debug.AssertNoErr(err)
			nlog.Errorln(err)
		} else {
			port = portNum
		}
	}
	if probeStr := os.Getenv("AIS_STATSD_PROBE"); probeStr != "" {
		if probeBool, err := cos.ParseBool(probeStr); err != nil {
			nlog.Errorln(err)
		} else {
			probe = probeBool
		}
	}
	id := strings.ReplaceAll(snode.ID(), ":", "_") // ":" delineates name and value for StatsD
	statsD, err := statsd.New("localhost", port, "ais"+snode.Type()+"."+id, probe)
	if err != nil {
		nlog.Errorf("Starting up without StatsD: %v", err)
	} else {
		nlog.Infoln("Using StatsD")
	}
	s.statsdC = statsD
}

func (s *coreStats) updateUptime(d time.Duration) {
	v := s.Tracker[Uptime]
	ratomic.StoreInt64(&v.Value, d.Nanoseconds())
}

func (s *coreStats) MarshalJSON() ([]byte, error) { return jsoniter.Marshal(s.Tracker) }
func (s *coreStats) UnmarshalJSON(b []byte) error { return jsoniter.Unmarshal(b, &s.Tracker) }

func (s *coreStats) get(name string) (val int64) {
	v := s.Tracker[name]
	val = ratomic.LoadInt64(&v.Value)
	return
}

func (s *coreStats) update(nv cos.NamedVal64) {
	v, ok := s.Tracker[nv.Name]
	debug.Assertf(ok, "invalid metric name %q", nv.Name)
	switch v.kind {
	case KindLatency:
		ratomic.AddInt64(&v.numSamples, 1)
		fallthrough
	case KindThroughput:
		ratomic.AddInt64(&v.Value, nv.Value)
		ratomic.AddInt64(&v.cumulative, nv.Value)
	case KindCounter, KindSize, KindTotal:
		ratomic.AddInt64(&v.Value, nv.Value)
		// - non-empty suffix forces an immediate Tx with no aggregation (see below);
		// - suffix is an arbitrary string that can be defined at runtime;
		// - e.g. usage: per-mountpath error counters.
		if !s.statsdDisabled() && nv.NameSuffix != "" {
			s.statsdC.Send(v.label.comm+"."+nv.NameSuffix,
				1, metric{Type: statsd.Counter, Name: "count", Value: nv.Value})
		}
	default:
		debug.Assert(false, v.kind)
	}
}

// log + StatsD (Prometheus is done separately via `Collect`)
func (s *coreStats) copyT(out copyTracker, diskLowUtil ...int64) bool {
	idle := true
	intl := max(int64(s.statsTime.Seconds()), 1)
	s.sgl.Reset()
	for name, v := range s.Tracker {
		switch v.kind {
		case KindLatency:
			var lat int64
			if num := ratomic.SwapInt64(&v.numSamples, 0); num > 0 {
				lat = ratomic.SwapInt64(&v.Value, 0) / num
				if !ignore(name) {
					idle = false
				}
			}
			out[name] = copyValue{lat}
			// NOTE: ns => ms, and not reporting zeros
			millis := cos.DivRound(lat, int64(time.Millisecond))
			if !s.statsdDisabled() && millis > 0 {
				s.statsdC.AppMetric(metric{Type: statsd.Timer, Name: v.label.stsd, Value: float64(millis)}, s.sgl)
			}
		case KindThroughput:
			var throughput int64
			if throughput = ratomic.SwapInt64(&v.Value, 0); throughput > 0 {
				throughput /= intl
				if !ignore(name) {
					idle = false
				}
			}
			out[name] = copyValue{throughput}
			if !s.statsdDisabled() && throughput > 0 {
				fv := roundMBs(throughput)
				s.statsdC.AppMetric(metric{Type: statsd.Gauge, Name: v.label.stsd, Value: fv}, s.sgl)
			}
		case KindComputedThroughput:
			if throughput := ratomic.SwapInt64(&v.Value, 0); throughput > 0 {
				out[name] = copyValue{throughput}
				if !s.statsdDisabled() {
					fv := roundMBs(throughput)
					s.statsdC.AppMetric(metric{Type: statsd.Gauge, Name: v.label.stsd, Value: fv}, s.sgl)
				}
			}
		case KindCounter, KindSize, KindTotal:
			var (
				val     = ratomic.LoadInt64(&v.Value)
				changed bool
			)
			if prev, ok := out[name]; !ok || prev.Value != val {
				changed = true
			}
			if val > 0 {
				out[name] = copyValue{val}
				if changed && !ignore(name) {
					idle = false
				}
			}
			// StatsD iff changed
			if !s.statsdDisabled() && changed {
				if v.kind == KindCounter {
					s.statsdC.AppMetric(metric{Type: statsd.Counter, Name: v.label.stsd, Value: val}, s.sgl)
				} else {
					// target only suffix
					metricType := statsd.Counter
					if v.label.comm == "dl" {
						metricType = statsd.PersistentCounter
					}
					s.statsdC.AppMetric(metric{Type: metricType, Name: v.label.stsd, Value: float64(val)}, s.sgl)
				}
			}
		case KindGauge:
			val := ratomic.LoadInt64(&v.Value)
			out[name] = copyValue{val}
			if !s.statsdDisabled() {
				s.statsdC.AppMetric(metric{Type: statsd.Gauge, Name: v.label.stsd, Value: float64(val)}, s.sgl)
			}
			if isDiskUtilMetric(name) && val > diskLowUtil[0] {
				idle = false
			}
		default:
			out[name] = copyValue{ratomic.LoadInt64(&v.Value)}
		}
	}
	if !s.statsdDisabled() {
		s.statsdC.SendSGL(s.sgl)
	}
	return idle
}

// REST API what=stats query
// NOTE: not reporting zero counts
func (s *coreStats) copyCumulative(ctracker copyTracker) {
	for name, v := range s.Tracker {
		switch v.kind {
		case KindLatency:
			ctracker[name] = copyValue{ratomic.LoadInt64(&v.cumulative)}
		case KindThroughput:
			val := copyValue{ratomic.LoadInt64(&v.cumulative)}
			ctracker[name] = val

			// NOTE: effectively, add same-value metric that was never added/updated
			// via `runner.Add` and friends. Is OK to replace ".bps" suffix
			// as statsValue.cumulative _is_ the total size (aka, KindSize)
			n := name[:len(name)-3] + "size"
			ctracker[n] = val
		case KindCounter, KindSize, KindTotal:
			if val := ratomic.LoadInt64(&v.Value); val > 0 {
				ctracker[name] = copyValue{val}
			}
		default: // KindSpecial, KindComputedThroughput, KindGauge
			ctracker[name] = copyValue{ratomic.LoadInt64(&v.Value)}
		}
	}
}

func (s *coreStats) reset(errorsOnly bool) {
	if errorsOnly {
		for name, v := range s.Tracker {
			if IsErrMetric(name) {
				debug.Assert(v.kind == KindCounter || v.kind == KindSize, name)
				ratomic.StoreInt64(&v.Value, 0)
			}
		}
		return
	}

	for _, v := range s.Tracker {
		switch v.kind {
		case KindLatency:
			ratomic.StoreInt64(&v.numSamples, 0)
			fallthrough
		case KindThroughput:
			ratomic.StoreInt64(&v.Value, 0)
			ratomic.StoreInt64(&v.cumulative, 0)
		case KindCounter, KindSize, KindComputedThroughput, KindGauge, KindTotal:
			ratomic.StoreInt64(&v.Value, 0)
		default: // KindSpecial - do nothing
		}
	}
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

////////////
// runner //
////////////

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

// NOTE naming convention: ".n" for the count and ".ns" for duration (nanoseconds)
// compare with coreStats.initProm()
func (r *runner) reg(snode *meta.Snode, name, kind string) {
	v := &statsValue{kind: kind}
	// in StatsD metrics ":" delineates the name and the value - replace with underscore
	switch kind {
	case KindCounter:
		debug.Assert(strings.HasSuffix(name, ".n"), name) // naming convention
		v.label.comm = strings.TrimSuffix(name, ".n")
		v.label.comm = strings.ReplaceAll(v.label.comm, ":", "_")
		v.label.stsd = fmt.Sprintf("%s.%s.%s.%s", "ais"+snode.Type(), snode.ID(), v.label.comm, "count")
	case KindTotal:
		debug.Assert(strings.HasSuffix(name, ".total"), name) // naming convention
		v.label.comm = strings.ReplaceAll(v.label.comm, ":", "_")
		v.label.stsd = fmt.Sprintf("%s.%s.%s.%s", "ais"+snode.Type(), snode.ID(), v.label.comm, "total")
	case KindSize:
		debug.Assert(strings.HasSuffix(name, ".size"), name) // naming convention
		v.label.comm = strings.TrimSuffix(name, ".size")
		v.label.comm = strings.ReplaceAll(v.label.comm, ":", "_")
		v.label.stsd = fmt.Sprintf("%s.%s.%s.%s", "ais"+snode.Type(), snode.ID(), v.label.comm, "mbytes")
	case KindLatency:
		debug.Assert(strings.Contains(name, ".ns"), name) // ditto
		v.label.comm = strings.TrimSuffix(name, ".ns")
		v.label.comm = strings.ReplaceAll(v.label.comm, ".ns.", ".")
		v.label.comm = strings.ReplaceAll(v.label.comm, ":", "_")
		v.label.stsd = fmt.Sprintf("%s.%s.%s.%s", "ais"+snode.Type(), snode.ID(), v.label.comm, "ms")
	case KindThroughput, KindComputedThroughput:
		debug.Assert(strings.HasSuffix(name, ".bps"), name) // ditto
		v.label.comm = strings.TrimSuffix(name, ".bps")
		v.label.comm = strings.ReplaceAll(v.label.comm, ":", "_")
		v.label.stsd = fmt.Sprintf("%s.%s.%s.%s", "ais"+snode.Type(), snode.ID(), v.label.comm, "mbps")
	default:
		debug.Assert(kind == KindGauge || kind == KindSpecial)
		v.label.comm = name
		v.label.comm = strings.ReplaceAll(v.label.comm, ":", "_")
		if name == Uptime {
			v.label.comm = strings.ReplaceAll(v.label.comm, ".ns.", ".")
			v.label.stsd = fmt.Sprintf("%s.%s.%s.%s", "ais"+snode.Type(), snode.ID(), v.label.comm, "seconds")
		} else {
			v.label.stsd = fmt.Sprintf("%s.%s.%s", "ais"+snode.Type(), snode.ID(), v.label.comm)
		}
	}
	r.core.Tracker[name] = v
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

func (r *runner) Flag(name string, set, clr cos.NodeStateFlags) {
	var (
		nval  cos.NodeStateFlags
		v, ok = r.core.Tracker[name]
	)
	debug.Assertf(ok, "invalid metric name %q", name)
	oval := cos.NodeStateFlags(ratomic.LoadInt64(&v.Value))
	if set != 0 {
		nval = oval.Set(set)
		if clr != 0 {
			nval = nval.Clear(clr)
		}
	} else if clr != 0 {
		nval = oval.Clear(clr)
	}
	if nval != oval {
		ratomic.StoreInt64(&v.Value, int64(nval))
	}
}

func (*runner) IsPrometheus() bool { return false }

func (r *runner) Name() string { return r.name }

func (r *runner) Get(name string) (val int64) { return r.core.get(name) }

func (r *runner) nodeStateFlags() cos.NodeStateFlags {
	val := r.Get(NodeStateFlags)
	return cos.NodeStateFlags(val)
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
			checkNumGorHigh = _whingeGoroutines(now, checkNumGorHigh, goMaxProcs)

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

func _whingeGoroutines(now, checkNumGorHigh int64, goMaxProcs int) int64 {
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

func (r *runner) StartedUp() bool { return r.startedUp.Load() }

func (r *runner) Stop(err error) {
	nlog.Infof("Stopping %s, err: %v", r.Name(), err)
	r.stopCh <- struct{}{}
	r.core.statsdC.Close()
	close(r.stopCh)
}

func recycleLogs() time.Duration {
	// keep total log size below the configured max
	go removeLogs(cmn.GCO.Get())
	return maxLogSizeCheckTime
}

func removeLogs(config *cmn.Config) {
	maxtotal := int64(config.Log.MaxTotal)
	dentries, err := os.ReadDir(config.LogDir)
	if err != nil {
		nlog.Errorf("GC logs: cannot read log dir %s, err: %v", config.LogDir, err)
		_ = cos.CreateDir(config.LogDir) // FIXME: (local non-containerized + kill/restart under test)
		return
	}
	for _, logtype := range logtypes {
		var tot int64
		finfos := make([]rfs.FileInfo, 0, len(dentries))
		for _, dent := range dentries {
			if dent.IsDir() || !dent.Type().IsRegular() {
				continue
			}
			if n := dent.Name(); !strings.Contains(n, ".log.") || !strings.Contains(n, logtype) {
				continue
			}
			if finfo, err := dent.Info(); err == nil {
				tot += finfo.Size()
				finfos = append(finfos, finfo)
			}
		}
		if tot > maxtotal {
			removeOlderLogs(tot, maxtotal, config.LogDir, logtype, finfos)
		}
	}
}

func removeOlderLogs(tot, maxtotal int64, logdir, logtype string, filteredInfos []rfs.FileInfo) {
	const prefix = "GC logs"
	l := len(filteredInfos)
	if l <= 1 {
		nlog.Warningf("%s: cannot cleanup %s, dir %s, tot %d, max %d", prefix, logtype, logdir, tot, maxtotal)
		return
	}
	fiLess := func(i, j int) bool {
		return filteredInfos[i].ModTime().Before(filteredInfos[j].ModTime())
	}

	verbose := cmn.Rom.FastV(4, cos.SmoduleStats)
	if verbose {
		nlog.Infoln(prefix + ": started")
	}
	sort.Slice(filteredInfos, fiLess)
	filteredInfos = filteredInfos[:l-1] // except the last = current
	for _, logfi := range filteredInfos {
		logfqn := filepath.Join(logdir, logfi.Name())
		if err := cos.RemoveFile(logfqn); err == nil {
			tot -= logfi.Size()
			if verbose {
				nlog.Infof("%s: removed %s", prefix, logfqn)
			}
			if tot < maxtotal {
				break
			}
		} else {
			nlog.Errorf("%s: failed to remove %s", prefix, logfqn)
		}
	}
	if verbose {
		nlog.Infoln(prefix + ": done")
	}
}

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
	r.Flag(NodeStateFlags, set, clr)
}

// debug.NewExpvar & debug.SetExpvar to visualize:
// * all counters including errors
// * latencies including keepalive
// * mountpath capacities
// * mountpath (disk) utilizations
// * total number of goroutines, etc.
// (access via host:port/debug/vars in debug mode)

// reusable sgl => (udp) => StatsD
