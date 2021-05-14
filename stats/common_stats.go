// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/NVIDIA/aistore/3rdparty/atomic"
	"github.com/NVIDIA/aistore/3rdparty/glog"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/mono"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/stats/statsd"
	"github.com/NVIDIA/aistore/xaction"
	jsoniter "github.com/json-iterator/go"
)

const (
	logsMaxSizeCheckTime      = 48 * time.Minute       // periodically check the logs for max accumulated size
	startupSleep              = 300 * time.Millisecond // periodically poll ClusterStarted()
	startupDeadlineMultiplier = 2                      // deadline = startupDeadlineMultiplier * config.Timeout.Startup
	numGorHighCheckTime       = 2 * time.Minute        // periodically log a warning if the number of goroutines remains high
)

const (
	KindCounter    = "counter"
	KindLatency    = "latency"
	KindThroughput = "throughput"
	KindSpecial    = "special"
)

// number-of-goroutines watermarks expressed as multipliers over the number of available logical CPUs (GOMAXPROCS)
const (
	numGorHigh    = 100
	numGorExtreme = 1000
)

var kinds = []string{KindCounter, KindLatency, KindThroughput, KindSpecial} // NOTE: supported metric types

// CoreStats stats
const (
	// KindCounter
	GetCount         = "get.n"
	PutCount         = "put.n"
	AppendCount      = "append.n"
	DeleteCount      = "del.n"
	RenameCount      = "ren.n"
	ListCount        = "lst.n"
	ErrCount         = "err.n"
	ErrGetCount      = "err.get.n"
	ErrDeleteCount   = "err.delete.n"
	ErrPostCount     = "err.post.n"
	ErrPutCount      = "err.put.n"
	ErrHeadCount     = "err.head.n"
	ErrListCount     = "err.list.n"
	ErrRangeCount    = "err.range.n"
	ErrDownloadCount = "err.dl.n"

	// KindLatency
	GetLatency          = "get.ns"
	ListLatency         = "lst.ns"
	KeepAliveMinLatency = "kalive.ns.min"
	KeepAliveMaxLatency = "kalive.ns.max"
	KeepAliveLatency    = "kalive.ns"

	// KindSpecial
	Uptime = "up.ns.time"
)

//
// public types
//

type (
	Tracker interface {
		StartedUp() bool
		Add(name string, val int64)
		Get(name string) int64
		AddErrorHTTP(method string, val int64)
		AddMany(namedVal64 ...NamedVal64)
		CoreStats() *CoreStats
		GetWhatStats() interface{}
		RegMetrics()
	}
	NamedVal64 struct {
		Name       string
		NameSuffix string // forces immediate send when non-empty (see NOTE below)
		Value      int64
	}
	CoreStats struct {
		Tracker   statsTracker
		statsdC   *statsd.Client
		statsTime time.Duration
		sgl       *memsys.SGL
	}

	RebalanceTargetStats struct {
		xaction.BaseXactStats
		Ext ExtRebalanceStats `json:"ext"`
	}

	ExtRebalanceStats struct {
		RebTxCount int64 `json:"reb.tx.n,string"`
		RebTxSize  int64 `json:"reb.tx.size,string"`
		RebRxCount int64 `json:"reb.rx.n,string"`
		RebRxSize  int64 `json:"reb.rx.size,string"`
		RebID      int64 `json:"glob.id,string"`
	}

	TargetStatus struct {
		RebalanceStats *RebalanceTargetStats `json:"rebalance_stats,omitempty"`
	}

	DaemonStatus struct {
		Snode       *cluster.Snode `json:"snode"`
		Stats       *CoreStats     `json:"daemon_stats"`
		Capacity    fs.MPCap       `json:"capacity"`
		SysInfo     cos.SysInfo    `json:"sys_info"`
		SmapVersion int64          `json:"smap_version,string"`
		TStatus     *TargetStatus  `json:"target_status,omitempty"`
		Status      string         `json:"status"`
		DeployedOn  string         `json:"deployment"`
		Version     string         `json:"ais_version"` // major.minor.build
		BuildTime   string         `json:"build_time"`  // YYYY-MM-DD HH:MM:SS-TZ
	}
)

// interface guard
var (
	_ Tracker           = (*Prunner)(nil)
	_ Tracker           = (*Trunner)(nil)
	_ cluster.XactStats = (*RebalanceTargetStats)(nil)
)

//
// private types
//

type (
	metric = statsd.Metric // type alias

	// implemented by the stats runners
	statsLogger interface {
		log(uptime time.Duration)
		doAdd(nv NamedVal64)
		statsTime(newval time.Duration)
	}
	runnerHost interface {
		ClusterStarted() bool
	}
	// implements Tracker, inherited by Prunner and Trunner
	statsRunner struct {
		name      string
		stopCh    chan struct{}
		workCh    chan NamedVal64
		ticker    *time.Ticker
		Core      *CoreStats  `json:"core"`
		ctracker  copyTracker // to avoid making it at runtime
		daemon    runnerHost
		startedUp atomic.Bool
	}
	// Stats are tracked via a map of stats names (key) to statsValue (values).
	// There are two main types of stats: counter and latency declared
	// using the the kind field. Only latency stats have numSamples used to compute latency.
	statsValue struct {
		sync.RWMutex
		Value      int64 `json:"v,string"`
		kind       string
		nroot      string
		numSamples int64
		cumulative int64
		isCommon   bool // optional, common to the proxy and target
	}
	copyValue struct {
		Value int64 `json:"v,string"`
	}
	statsTracker map[string]*statsValue
	copyTracker  map[string]*copyValue
)

///////////////
// CoreStats //
///////////////

// interface guard
var (
	_ json.Marshaler   = (*CoreStats)(nil)
	_ json.Unmarshaler = (*CoreStats)(nil)
)

func (s *CoreStats) init(size int) {
	s.Tracker = make(statsTracker, size)
	// NOTE:
	// accessible in debug mode via host:port/debug/vars
	// * all counters including errors
	// * latencies including keepalive
	// * mountpath capacities
	// * mountpath (disk) utilizations (see ios)
	// * total number of goroutines
	debug.NewExpvar(glog.SmoduleStats)
	s.Tracker.regCommonMetrics()

	// reusable sgl => (udp) => StatsD
	s.sgl = memsys.DefaultPageMM().NewSGL(memsys.PageSize)
}

func (s *CoreStats) UpdateUptime(d time.Duration) {
	v := s.Tracker[Uptime]
	v.Lock()
	v.Value = d.Nanoseconds()
	v.Unlock()
}

func (s *CoreStats) MarshalJSON() ([]byte, error) { return jsoniter.Marshal(s.Tracker) }
func (s *CoreStats) UnmarshalJSON(b []byte) error { return jsoniter.Unmarshal(b, &s.Tracker) }

func (s *CoreStats) get(name string) (val int64) {
	v := s.Tracker[name]
	v.RLock()
	val = v.Value
	v.RUnlock()
	return
}

//
// NOTE naming convention: ".n" for the count and ".ns" for duration (nanoseconds)
//
func (s *CoreStats) doAdd(name, nameSuffix string, val int64) {
	v, ok := s.Tracker[name]
	debug.Assertf(ok, "invalid stats name %q", name)
	switch v.kind {
	case KindLatency:
		v.Lock()
		v.numSamples++
		v.cumulative += val
		v.Value += val
		v.Unlock()
	case KindThroughput:
		v.Lock()
		v.cumulative += val
		v.Value += val
		v.Unlock()
	case KindCounter:
		v.Lock()
		v.Value += val
		v.Unlock()
		// NOTE:
		// - currently only counters;
		// - non-empty suffix forces an immediate Tx with no aggregation (see below);
		// - suffix is an arbitrary string that can be defined at runtime;
		// - e.g. usage: per-mountpath error counters.
		if nameSuffix != "" {
			s.statsdC.Send(v.nroot+"."+nameSuffix,
				1, metric{Type: statsd.Counter, Name: "count", Value: val})
		}
	default:
		debug.AssertMsg(false, v.kind)
	}
}

func (s *CoreStats) copyT(ctracker copyTracker, idlePrefs []string) (idle bool) {
	var newline bool
	idle = true
	s.sgl.Reset()
	for name, v := range s.Tracker {
		switch v.kind {
		case KindLatency:
			var lat int64
			v.Lock()
			if v.numSamples > 0 {
				lat = v.Value / v.numSamples
				ctracker[name] = &copyValue{Value: lat}
				debug.SetExpvar(glog.SmoduleStats, name, lat)
				if !match(name, idlePrefs) {
					idle = false
				}
			}
			v.Value = 0
			v.numSamples = 0
			v.Unlock()
			// NOTE: ns to ms and don't report zeros
			millis := cos.DivRound(lat, int64(time.Millisecond))
			if millis > 0 && strings.HasSuffix(name, ".ns") {
				s.statsdC.AppendMetric(
					v.nroot,
					metric{
						Type:  statsd.Timer,
						Name:  "latency",
						Value: float64(millis),
					},
					s.sgl, newline)
				newline = true
			}
		case KindThroughput:
			var throughput int64
			v.Lock()
			if v.Value > 0 {
				throughput = v.Value / cos.MaxI64(int64(s.statsTime.Seconds()), 1)
				ctracker[name] = &copyValue{Value: throughput}
				idle = false
				v.Value = 0
			}
			v.Unlock()
			if throughput > 0 {
				s.statsdC.AppendMetric(
					v.nroot,
					metric{Type: statsd.Gauge, Name: "throughput", Value: throughput},
					s.sgl, newline,
				)
				newline = true
			}
		case KindCounter:
			var cnt int64
			v.RLock()
			if v.Value > 0 {
				cnt = v.Value
				if prev, ok := ctracker[name]; !ok || prev.Value != cnt {
					ctracker[name] = &copyValue{Value: cnt}
					debug.SetExpvar(glog.SmoduleStats, name, cnt)
					if !match(name, idlePrefs) {
						idle = false
					}
				} else {
					cnt = 0
				}
			}
			v.RUnlock()
			if cnt > 0 {
				if strings.HasSuffix(name, ".size") {
					// target only suffix
					metricType := statsd.Counter
					if v.nroot == "dl" {
						metricType = statsd.PersistentCounter
					}
					s.statsdC.AppendMetric(
						v.nroot,
						metric{Type: metricType, Name: "bytes", Value: cnt},
						s.sgl, newline,
					)
					newline = true
				} else {
					s.statsdC.AppendMetric(
						v.nroot,
						metric{Type: statsd.Counter, Name: "count", Value: cnt},
						s.sgl, newline,
					)
					newline = true
				}
			}
		default:
			ctracker[name] = &copyValue{Value: v.Value} // KindSpecial/KindDelta as is and wo/ lock
			debug.SetExpvar(glog.SmoduleStats, name, v.Value)
		}
	}
	s.statsdC.SendSGL(s.sgl)

	debug.SetExpvar(glog.SmoduleStats, "num-goroutines", int64(runtime.NumGoroutine()))
	return
}

func (s *CoreStats) copyCumulative(ctracker copyTracker) {
	// serves to satisfy REST API what=stats query

	for name, v := range s.Tracker {
		v.RLock()
		if v.kind == KindLatency || v.kind == KindThroughput {
			ctracker[name] = &copyValue{Value: v.cumulative}
		} else if v.kind == KindCounter {
			if v.Value != 0 {
				ctracker[name] = &copyValue{Value: v.Value}
			}
		} else {
			ctracker[name] = &copyValue{Value: v.Value} // KindSpecial as is and wo/ lock
		}
		v.RUnlock()
	}
}

// init StatsD client
func (s *CoreStats) initStatsD(node *cluster.Snode) {
	var (
		suffix = strings.ReplaceAll(node.ID(), ":", "_") // ":" delineates name and value for StatsD
		port   = 8125                                    // StatsD default port, see https://github.com/etsy/stats
		probe  = false                                   // test-probe StatsD server at init time
	)
	if portStr := os.Getenv("AIS_STATSD_PORT"); portStr != "" {
		if portNum, err := cmn.ParsePort(portStr); err != nil {
			glog.Error(err)
		} else {
			port = portNum
		}
	}
	if probeStr := os.Getenv("AIS_STATSD_PROBE"); probeStr != "" {
		if probeBool, err := cos.ParseBool(probeStr); err != nil {
			glog.Error(err)
		} else {
			probe = probeBool
		}
	}
	statsD, err := statsd.New("localhost", port, "ais"+node.Type()+"."+suffix, probe)
	if err != nil {
		glog.Errorf("Starting up without StatsD: %v", err)
	}
	s.statsdC = &statsD
}

//
// statsValue
//

// interface guard
var (
	_ json.Marshaler   = (*statsValue)(nil)
	_ json.Unmarshaler = (*statsValue)(nil)
)

func (v *statsValue) MarshalJSON() (b []byte, err error) {
	v.RLock()
	b, err = jsoniter.Marshal(v.Value)
	v.RUnlock()
	return
}
func (v *statsValue) UnmarshalJSON(b []byte) error { return jsoniter.Unmarshal(b, &v.Value) }

//
// copyValue
//

// interface guard
var (
	_ json.Marshaler   = (*copyValue)(nil)
	_ json.Unmarshaler = (*copyValue)(nil)
)

func (v *copyValue) MarshalJSON() (b []byte, err error) { return jsoniter.Marshal(v.Value) }
func (v *copyValue) UnmarshalJSON(b []byte) error       { return jsoniter.Unmarshal(b, &v.Value) }

//
// statsTracker
//

func (tracker statsTracker) register(name, kind string, isCommon ...bool) {
	v := &statsValue{kind: kind}
	if len(isCommon) > 0 {
		v.isCommon = isCommon[0]
	}

	debug.Assertf(cos.StringInSlice(kind, kinds), "invalid metric kind %q", kind)
	switch kind {
	case KindCounter:
		debug.AssertMsg(strings.HasSuffix(name, ".n") || strings.HasSuffix(name, ".size"), name)
		v.nroot = strings.TrimSuffix(name, ".n")
		v.nroot = strings.TrimSuffix(v.nroot, ".size")
	case KindLatency:
		debug.AssertMsg(strings.Contains(name, ".ns"), name)
		v.nroot = strings.TrimSuffix(name, ".ns")
		v.nroot = strings.ReplaceAll(v.nroot, ".ns.", ".")
	case KindThroughput:
		v.nroot = strings.TrimSuffix(name, ".bps")
	default:
		v.nroot = name
	}
	v.nroot = strings.ReplaceAll(v.nroot, ":", "_") // ":" delineates name and value for StatsD
	tracker[name] = v
}

// register common metrics; see RegMetrics() in target_stats.go
func (tracker statsTracker) regCommonMetrics() {
	tracker.register(GetCount, KindCounter, true)
	tracker.register(PutCount, KindCounter, true)
	tracker.register(AppendCount, KindCounter, true)
	tracker.register(DeleteCount, KindCounter, true)
	tracker.register(RenameCount, KindCounter, true)
	tracker.register(ListCount, KindCounter, true)
	tracker.register(GetLatency, KindLatency, true)
	tracker.register(ListLatency, KindLatency, true)
	tracker.register(KeepAliveMinLatency, KindLatency, true)
	tracker.register(KeepAliveMaxLatency, KindLatency, true)
	tracker.register(KeepAliveLatency, KindLatency, true)
	tracker.register(ErrCount, KindCounter, true)
	tracker.register(ErrGetCount, KindCounter, true)
	tracker.register(ErrDeleteCount, KindCounter, true)
	tracker.register(ErrPostCount, KindCounter, true)
	tracker.register(ErrPutCount, KindCounter, true)
	tracker.register(ErrHeadCount, KindCounter, true)
	tracker.register(ErrListCount, KindCounter, true)
	tracker.register(ErrRangeCount, KindCounter, true)
	tracker.register(ErrDownloadCount, KindCounter, true)

	tracker.register(Uptime, KindSpecial, true)
}

////////////////
// statsunner //
////////////////

func (r *statsRunner) Name() string { return r.name }

func (r *statsRunner) runcommon(logger statsLogger) error {
	var (
		i, j   time.Duration
		ticker = time.NewTicker(startupSleep)

		// NOTE: the maximum time we agree to wait for r.daemon.ClusterStarted()
		config   = cmn.GCO.Get()
		deadline = startupDeadlineMultiplier * config.Timeout.Startup.D()
	)
waitStartup:
	for {
		select {
		case <-r.workCh:
			// Drain workCh until the daemon (proxy or target) starts up.
		case <-r.stopCh:
			ticker.Stop()
			return nil
		case <-ticker.C:
			if r.daemon.ClusterStarted() {
				break waitStartup
			}
			j += startupSleep
			if j > deadline {
				ticker.Stop()
				return cmn.ErrStartupTimeout
			}
			i += startupSleep
			if i > config.Timeout.Startup.D() {
				glog.Errorln("startup is taking unusually long time...")
				i = 0
			}
		}
	}
	ticker.Stop()

	config = cmn.GCO.Get()
	goMaxProcs := runtime.GOMAXPROCS(0)
	glog.Infof("Starting %s", r.Name())
	hk.Reg(r.Name()+".gc.logs", r.recycleLogs, logsMaxSizeCheckTime)

	statsTime := config.Periodic.StatsTime.D()
	r.ticker = time.NewTicker(statsTime)
	r.startedUp.Store(true)

	startTime, checkNumGorHigh := mono.NanoTime(), int64(0)
	for {
		select {
		case nv, ok := <-r.workCh:
			if ok {
				logger.doAdd(nv)
			}
		case <-r.ticker.C:
			now := mono.NanoTime()
			logger.log(time.Duration(now - startTime)) // uptime
			checkNumGorHigh = _whingeGoroutines(now, checkNumGorHigh, goMaxProcs)

			config = cmn.GCO.Get()
			if statsTime != config.Periodic.StatsTime.D() {
				statsTime = config.Periodic.StatsTime.D()
				r.ticker.Reset(statsTime)
				logger.statsTime(statsTime)
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
		glog.Errorf("Extremely high number of goroutines: %d", ngr)
	}
	if checkNumGorHigh == 0 {
		checkNumGorHigh = now
	} else if time.Duration(now-checkNumGorHigh) > numGorHighCheckTime {
		if !extreme {
			glog.Warningf("High number of goroutines: %d", ngr)
		}
		checkNumGorHigh = 0
	}
	return checkNumGorHigh
}

func (r *statsRunner) StartedUp() bool { return r.startedUp.Load() }

func (r *statsRunner) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.Name(), err)
	r.stopCh <- struct{}{}
	r.Core.statsdC.Close()
	close(r.stopCh)
}

// common impl
// NOTE: currently, proxy's stats == common and hardcoded
func (r *statsRunner) Add(name string, val int64) { r.workCh <- NamedVal64{Name: name, Value: val} }
func (r *statsRunner) Get(name string) int64      { cos.Assert(false); return 0 }

func (r *statsRunner) AddMany(nvs ...NamedVal64) {
	for _, nv := range nvs {
		r.workCh <- nv
	}
}

func (r *statsRunner) recycleLogs() time.Duration {
	// keep total log size below the configured max
	go r.removeLogs(cmn.GCO.Get())
	return logsMaxSizeCheckTime
}

func (r *statsRunner) removeLogs(config *cmn.Config) {
	maxtotal := int64(config.Log.MaxTotal)
	logfinfos, err := ioutil.ReadDir(config.LogDir)
	if err != nil {
		glog.Errorf("GC logs: cannot read log dir %s, err: %v", config.LogDir, err)
		_ = cos.CreateDir(config.LogDir) // FIXME: (local non-containerized + kill/restart under test)
		return
	}
	// sample name ais.ip-10-0-2-19.root.log.INFO.20180404-031540.2249
	logtypes := []string{".INFO.", ".WARNING.", ".ERROR."}
	for _, logtype := range logtypes {
		var (
			tot   = int64(0)
			infos = make([]os.FileInfo, 0, len(logfinfos))
		)
		for _, logfi := range logfinfos {
			if logfi.IsDir() {
				continue
			}
			if !strings.Contains(logfi.Name(), ".log.") {
				continue
			}
			if strings.Contains(logfi.Name(), logtype) {
				tot += logfi.Size()
				infos = append(infos, logfi)
			}
		}
		if tot > maxtotal {
			r.removeOlderLogs(tot, maxtotal, config.LogDir, logtype, infos)
		}
	}
}

func (r *statsRunner) removeOlderLogs(tot, maxtotal int64, logdir, logtype string, filteredInfos []os.FileInfo) {
	l := len(filteredInfos)
	if l <= 1 {
		glog.Warningf("GC logs: cannot cleanup %s, dir %s, tot %d, max %d", logtype, logdir, tot, maxtotal)
		return
	}
	fiLess := func(i, j int) bool {
		return filteredInfos[i].ModTime().Before(filteredInfos[j].ModTime())
	}
	if glog.FastV(4, glog.SmoduleStats) {
		glog.Infof("GC logs: started")
	}
	sort.Slice(filteredInfos, fiLess)
	filteredInfos = filteredInfos[:l-1] // except the last = current
	for _, logfi := range filteredInfos {
		logfqn := filepath.Join(logdir, logfi.Name())
		if err := cos.RemoveFile(logfqn); err == nil {
			tot -= logfi.Size()
			if glog.FastV(4, glog.SmoduleStats) {
				glog.Infof("GC logs: removed %s", logfqn)
			}
			if tot < maxtotal {
				break
			}
		} else {
			glog.Errorf("GC logs: failed to remove %s", logfqn)
		}
	}
	if glog.FastV(4, glog.SmoduleStats) {
		glog.Infof("GC logs: done")
	}
}

func (r *statsRunner) AddErrorHTTP(method string, val int64) {
	switch method {
	case http.MethodGet:
		r.workCh <- NamedVal64{Name: ErrGetCount, Value: val}
	case http.MethodDelete:
		r.workCh <- NamedVal64{Name: ErrDeleteCount, Value: val}
	case http.MethodPost:
		r.workCh <- NamedVal64{Name: ErrPostCount, Value: val}
	case http.MethodPut:
		r.workCh <- NamedVal64{Name: ErrPutCount, Value: val}
	case http.MethodHead:
		r.workCh <- NamedVal64{Name: ErrHeadCount, Value: val}
	default:
		r.workCh <- NamedVal64{Name: ErrCount, Value: val}
	}
}

// (don't log when the only updated vars are those that match "idle" prefixes)
func match(s string, prefs []string) bool {
	for _, p := range prefs {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}
