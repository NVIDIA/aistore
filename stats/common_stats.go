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
	"sync"
	ratomic "sync/atomic"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/atomic"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/core/meta"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats/statsd"
	jsoniter "github.com/json-iterator/go"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	dfltPeriodicFlushTime = time.Minute            // when `config.Log.FlushTime` is 0 (zero)
	dfltPeriodicTimeStamp = time.Hour              // extended date/time complementary to log timestamps (e.g., "11:29:11.644596")
	maxStatsLogInterval   = int64(3 * time.Minute) // when idle; secondly, an upper limit on `config.Log.StatsTime`
)

// more periodic
const (
	logsMaxSizeCheckTime = 48 * time.Minute       // periodically check the logs for max accumulated size
	startupSleep         = 300 * time.Millisecond // periodically poll ClusterStarted()
	numGorHighCheckTime  = 2 * time.Minute        // periodically log a warning if the number of goroutines remains high
)

// number-of-goroutines watermarks expressed as multipliers over the number of available logical CPUs (GOMAXPROCS)
const (
	numGorHigh    = 100
	numGorExtreme = 1000
)

// metrics
const (
	// KindCounter:
	// all basic counters are accompanied by the corresponding (errPrefix + kind) error count:
	// e.g.: "get.n" => "err.get.n", "put.n" => "err.put.n", etc.
	// See also: `IncErr`, `regCommon`
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
	GetLatency       = "get.ns"
	ListLatency      = "lst.ns"
	KeepAliveLatency = "kalive.ns"

	// KindSpecial
	Uptime = "up.ns.time"
)

// interfaces
type (
	metric = statsd.Metric // type alias

	// implemented by the stats runners
	statsLogger interface {
		log(now int64, uptime time.Duration, config *cmn.Config)
		statsTime(newval time.Duration)
		standingBy() bool
	}
	runnerHost interface {
		ClusterStarted() bool
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
			stsd string // StatsD label
			prom string // Prometheus label
		}
		Value      int64 `json:"v,string"`
		numSamples int64 // (log + StatsD) only
		cumulative int64
	}
	copyValue struct {
		Value int64 `json:"v,string"`
	}
	copyTracker map[string]copyValue // aggregated every statsTime interval
	promDesc    map[string]*prometheus.Desc
)

// main types
type (
	coreStats struct {
		Tracker   map[string]*statsValue
		promDesc  promDesc
		statsdC   *statsd.Client
		sgl       *memsys.SGL
		statsTime time.Duration
		cmu       sync.RWMutex // ctracker vs Prometheus Collect()
	}

	// Prunner and Trunner
	runner struct {
		daemon    runnerHost
		stopCh    chan struct{}
		ticker    *time.Ticker
		core      *coreStats
		ctracker  copyTracker // to avoid making it at runtime
		sorted    []string    // sorted names
		name      string      // this stats-runner's name
		prev      string      // prev ctracker.write
		next      int64       // mono.NanoTime()
		startedUp atomic.Bool
	}
)

// interface guard
var (
	_ Tracker = (*Prunner)(nil)
	_ Tracker = (*Trunner)(nil)
)

// sample name ais.ip-10-0-2-19.root.log.INFO.20180404-031540.2249
var logtypes = []string{".INFO.", ".WARNING.", ".ERROR."}

var ignoreIdle = []string{"kalive", Uptime, "disk."}

func ignore(s string) bool {
	for _, p := range ignoreIdle {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}

///////////////
// coreStats //
///////////////

// interface guard
var (
	_ json.Marshaler   = (*coreStats)(nil)
	_ json.Unmarshaler = (*coreStats)(nil)
)

// helper: convert bytes to megabytes with a fixed rounding precision = 2 digits (NOTE: MB not MiB)
func roundMBs(val int64) (mbs float64) {
	mbs = float64(val) / 1000 / 10
	num := int(mbs + 0.5)
	mbs = float64(num) / 100
	return
}

func (s *coreStats) init(size int) {
	s.Tracker = make(map[string]*statsValue, size)
	s.promDesc = make(promDesc, size)

	s.sgl = memsys.PageMM().NewSGL(memsys.PageSize)
}

// NOTE: nil StatsD client means that we provide metrics to Prometheus (see below)
func (s *coreStats) isPrometheus() bool { return s.statsdC == nil }

// vs Collect()
func (s *coreStats) promRLock() {
	if s.isPrometheus() {
		s.cmu.RLock()
	}
}

func (s *coreStats) promRUnlock() {
	if s.isPrometheus() {
		s.cmu.RUnlock()
	}
}

func (s *coreStats) promLock() {
	if s.isPrometheus() {
		s.cmu.Lock()
	}
}

func (s *coreStats) promUnlock() {
	if s.isPrometheus() {
		s.cmu.Unlock()
	}
}

// init MetricClient client: StatsD (default) or Prometheus
func (s *coreStats) initMetricClient(node *meta.Snode, parent *runner) {
	// Either Prometheus
	if prom := os.Getenv("AIS_PROMETHEUS"); prom != "" {
		nlog.Infoln("Using Prometheus")
		prometheus.MustRegister(parent) // as prometheus.Collector
		return
	}

	// or StatsD
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
	id := strings.ReplaceAll(node.ID(), ":", "_") // ":" delineates name and value for StatsD
	statsD, err := statsd.New("localhost", port, "ais"+node.Type()+"."+id, probe)
	if err != nil {
		nlog.Errorf("Starting up without StatsD: %v", err)
	} else {
		nlog.Infoln("Using StatsD")
	}
	s.statsdC = statsD
}

// populate *prometheus.Desc and statsValue.label.prom
// NOTE: naming; compare with statsTracker.register()
func (s *coreStats) initProm(node *meta.Snode) {
	if !s.isPrometheus() {
		return
	}
	id := strings.ReplaceAll(node.ID(), ".", "_")
	for name, v := range s.Tracker {
		var variableLabels []string
		if isDiskMetric(name) {
			// obtain prometheus specific disk-metric name from tracker name
			// e.g. `disk.nvme0.read.bps` -> `disk.read.bps`.
			_, name = extractPromDiskMetricName(name)
			variableLabels = []string{diskMetricLabel}
		}
		label := strings.ReplaceAll(name, ".", "_")
		v.label.prom = strings.ReplaceAll(label, ":", "_")

		help := v.kind
		if strings.HasSuffix(v.label.prom, "_n") {
			help = "total number of operations"
		} else if strings.HasSuffix(v.label.prom, "_size") {
			help = "total size (MB)"
		} else if strings.HasSuffix(v.label.prom, "avg_rsize") {
			help = "average read size (bytes)"
		} else if strings.HasSuffix(v.label.prom, "avg_wsize") {
			help = "average write size (bytes)"
		} else if strings.HasSuffix(v.label.prom, "_ns") {
			v.label.prom = strings.TrimSuffix(v.label.prom, "_ns") + "_ms"
			help = "latency (milliseconds)"
		} else if strings.Contains(v.label.prom, "_ns_") {
			v.label.prom = strings.ReplaceAll(v.label.prom, "_ns_", "_ms_")
			if name == Uptime {
				v.label.prom = strings.ReplaceAll(v.label.prom, "_ns_", "")
				help = "uptime (seconds)"
			} else {
				help = "latency (milliseconds)"
			}
		} else if strings.HasSuffix(v.label.prom, "_bps") {
			v.label.prom = strings.TrimSuffix(v.label.prom, "_bps") + "_mbps"
			help = "throughput (MB/s)"
		}

		fullqn := prometheus.BuildFQName("ais", node.Type(), v.label.prom)
		// e.g. metric: ais_target_disk_avg_wsize{disk="nvme0n1",node_id="fqWt8081"}
		s.promDesc[name] = prometheus.NewDesc(fullqn, help, variableLabels, prometheus.Labels{"node_id": id})
	}
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
	case KindCounter, KindSize:
		ratomic.AddInt64(&v.Value, nv.Value)
		// - non-empty suffix forces an immediate Tx with no aggregation (see below);
		// - suffix is an arbitrary string that can be defined at runtime;
		// - e.g. usage: per-mountpath error counters.
		if !s.isPrometheus() && nv.NameSuffix != "" {
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
			if !s.isPrometheus() && millis > 0 {
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
			if !s.isPrometheus() && throughput > 0 {
				fv := roundMBs(throughput)
				s.statsdC.AppMetric(metric{Type: statsd.Gauge, Name: v.label.stsd, Value: fv}, s.sgl)
			}
		case KindComputedThroughput:
			if throughput := ratomic.SwapInt64(&v.Value, 0); throughput > 0 {
				out[name] = copyValue{throughput}
				if !s.isPrometheus() {
					fv := roundMBs(throughput)
					s.statsdC.AppMetric(metric{Type: statsd.Gauge, Name: v.label.stsd, Value: fv}, s.sgl)
				}
			}
		case KindCounter, KindSize:
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
			if !s.isPrometheus() && changed {
				if v.kind == KindCounter {
					s.statsdC.AppMetric(metric{Type: statsd.Counter, Name: v.label.stsd, Value: val}, s.sgl)
				} else {
					// target only suffix
					metricType := statsd.Counter
					if v.label.comm == "dl" {
						metricType = statsd.PersistentCounter
					}
					fv := roundMBs(val)
					s.statsdC.AppMetric(metric{Type: metricType, Name: v.label.stsd, Value: fv}, s.sgl)
				}
			}
		case KindGauge:
			val := ratomic.LoadInt64(&v.Value)
			out[name] = copyValue{val}
			if !s.isPrometheus() {
				s.statsdC.AppMetric(metric{Type: statsd.Gauge, Name: v.label.stsd, Value: float64(val)}, s.sgl)
			}
			if isDiskUtilMetric(name) && val > diskLowUtil[0] {
				idle = false
			}
		default:
			out[name] = copyValue{ratomic.LoadInt64(&v.Value)}
		}
	}
	if !s.isPrometheus() {
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
		case KindCounter, KindSize:
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
		case KindCounter, KindSize, KindComputedThroughput, KindGauge:
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

// interface guard
var (
	_ prometheus.Collector = (*runner)(nil)
)

func (r *runner) GetStats() *Node {
	ctracker := make(copyTracker, 48)
	r.core.copyCumulative(ctracker)
	return &Node{Tracker: ctracker}
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
func (r *runner) regCommon(node *meta.Snode) {
	// basic counters
	r.reg(node, GetCount, KindCounter)
	r.reg(node, PutCount, KindCounter)
	r.reg(node, AppendCount, KindCounter)
	r.reg(node, DeleteCount, KindCounter)
	r.reg(node, RenameCount, KindCounter)
	r.reg(node, ListCount, KindCounter)

	// basic error counters, respectively
	r.reg(node, errPrefix+GetCount, KindCounter)
	r.reg(node, errPrefix+PutCount, KindCounter)
	r.reg(node, errPrefix+AppendCount, KindCounter)
	r.reg(node, errPrefix+DeleteCount, KindCounter)
	r.reg(node, errPrefix+RenameCount, KindCounter)
	r.reg(node, errPrefix+ListCount, KindCounter)

	// more error counters
	r.reg(node, ErrHTTPWriteCount, KindCounter)
	r.reg(node, ErrDownloadCount, KindCounter)
	r.reg(node, ErrPutMirrorCount, KindCounter)

	// latency
	r.reg(node, GetLatency, KindLatency)
	r.reg(node, ListLatency, KindLatency)
	r.reg(node, KeepAliveLatency, KindLatency)

	// special uptime
	r.reg(node, Uptime, KindSpecial)
}

// NOTE naming convention: ".n" for the count and ".ns" for duration (nanoseconds)
// compare with coreStats.initProm()
func (r *runner) reg(node *meta.Snode, name, kind string) {
	v := &statsValue{kind: kind}
	// in StatsD metrics ":" delineates the name and the value - replace with underscore
	switch kind {
	case KindCounter:
		debug.Assert(strings.HasSuffix(name, ".n"), name) // naming convention
		v.label.comm = strings.TrimSuffix(name, ".n")
		v.label.comm = strings.ReplaceAll(v.label.comm, ":", "_")
		v.label.stsd = fmt.Sprintf("%s.%s.%s.%s", "ais"+node.Type(), node.ID(), v.label.comm, "count")
	case KindSize:
		debug.Assert(strings.HasSuffix(name, ".size"), name) // naming convention
		v.label.comm = strings.TrimSuffix(name, ".size")
		v.label.comm = strings.ReplaceAll(v.label.comm, ":", "_")
		v.label.stsd = fmt.Sprintf("%s.%s.%s.%s", "ais"+node.Type(), node.ID(), v.label.comm, "mbytes")
	case KindLatency:
		debug.Assert(strings.Contains(name, ".ns"), name) // ditto
		v.label.comm = strings.TrimSuffix(name, ".ns")
		v.label.comm = strings.ReplaceAll(v.label.comm, ".ns.", ".")
		v.label.comm = strings.ReplaceAll(v.label.comm, ":", "_")
		v.label.stsd = fmt.Sprintf("%s.%s.%s.%s", "ais"+node.Type(), node.ID(), v.label.comm, "ms")
	case KindThroughput, KindComputedThroughput:
		debug.Assert(strings.HasSuffix(name, ".bps"), name) // ditto
		v.label.comm = strings.TrimSuffix(name, ".bps")
		v.label.comm = strings.ReplaceAll(v.label.comm, ":", "_")
		v.label.stsd = fmt.Sprintf("%s.%s.%s.%s", "ais"+node.Type(), node.ID(), v.label.comm, "mbps")
	default:
		debug.Assert(kind == KindGauge || kind == KindSpecial)
		v.label.comm = name
		v.label.comm = strings.ReplaceAll(v.label.comm, ":", "_")
		if name == Uptime {
			v.label.comm = strings.ReplaceAll(v.label.comm, ".ns.", ".")
			v.label.stsd = fmt.Sprintf("%s.%s.%s.%s", "ais"+node.Type(), node.ID(), v.label.comm, "seconds")
		} else {
			v.label.stsd = fmt.Sprintf("%s.%s.%s", "ais"+node.Type(), node.ID(), v.label.comm)
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

func (r *runner) IsPrometheus() bool { return r.core.isPrometheus() }

func (r *runner) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range r.core.promDesc {
		ch <- desc
	}
}

func (r *runner) Collect(ch chan<- prometheus.Metric) {
	if !r.StartedUp() {
		return
	}
	r.core.promRLock()
	for name, v := range r.core.Tracker {
		var (
			val int64
			fv  float64

			variableLabels []string
		)
		copyV, okc := r.ctracker[name]
		if !okc {
			continue
		}
		val = copyV.Value
		fv = float64(val)
		// 1. convert units
		switch v.kind {
		case KindCounter:
			// do nothing
		case KindSize:
			fv = roundMBs(val)
		case KindLatency:
			millis := cos.DivRound(val, int64(time.Millisecond))
			fv = float64(millis)
		case KindThroughput:
			fv = roundMBs(val)
		default:
			if name == Uptime {
				seconds := cos.DivRound(val, int64(time.Second))
				fv = float64(seconds)
			}
		}
		// 2. convert kind
		promMetricType := prometheus.GaugeValue
		if v.kind == KindCounter || v.kind == KindSize {
			promMetricType = prometheus.CounterValue
		}
		if isDiskMetric(name) {
			var diskName string
			diskName, name = extractPromDiskMetricName(name)
			variableLabels = []string{diskName}
		}
		// 3. publish
		desc, ok := r.core.promDesc[name]
		debug.Assert(ok, name)
		m, err := prometheus.NewConstMetric(desc, promMetricType, fv, variableLabels...)
		debug.AssertNoErr(err)
		ch <- m
	}
	r.core.promRUnlock()
}

func (r *runner) Name() string { return r.name }

func (r *runner) Get(name string) (val int64) { return r.core.get(name) }

func (r *runner) _run(logger statsLogger /* Prunner or Trunner */) error {
	var (
		i, j   time.Duration
		sleep  = startupSleep
		ticker = time.NewTicker(sleep)

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
			if r.daemon.ClusterStarted() {
				break waitStartup
			}
			if logger.standingBy() && sleep == startupSleep /*first time*/ {
				sleep = config.Periodic.StatsTime.D()
				ticker.Reset(sleep)
				deadline = time.Hour

				nlog.Infoln(r.Name() + ": standing by...")
				nlog.Flush(nlog.ActNone)
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
	hk.Reg(r.Name()+"-logs"+hk.NameSuffix, recycleLogs, logsMaxSizeCheckTime)

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

			// refresh assorted read-mostly
			cmn.Rom.Set(&config.ClusterConfig)
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
	if !r.IsPrometheus() {
		r.core.statsdC.Close()
	}
	close(r.stopCh)
}

func recycleLogs() time.Duration {
	// keep total log size below the configured max
	go removeLogs(cmn.GCO.Get())
	return logsMaxSizeCheckTime
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

// debug.NewExpvar & debug.SetExpvar to visualize:
// * all counters including errors
// * latencies including keepalive
// * mountpath capacities
// * mountpath (disk) utilizations
// * total number of goroutines, etc.
// (access via host:port/debug/vars in debug mode)

// reusable sgl => (udp) => StatsD
