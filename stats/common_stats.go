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
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/hk"
	"github.com/NVIDIA/aistore/stats/statsd"
	"github.com/NVIDIA/aistore/xaction"
	jsoniter "github.com/json-iterator/go"
)

//==============================
//
// constants
//
//==============================

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

var (
	kinds      = []string{KindCounter, KindLatency, KindThroughput, KindSpecial}
	goMaxProcs int
)

// CoreStats stats
const (
	// KindCounter
	GetCount         = "get.n"
	PutCount         = "put.n"
	AppendCount      = "append.n"
	PostCount        = "pst.n"
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
		RegisterAll()
	}
	NamedVal64 struct {
		Name       string
		NameSuffix string
		Value      int64
	}
	CoreStats struct {
		Tracker   statsTracker
		statsdC   *statsd.Client
		statsTime time.Duration
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
		SysInfo     cmn.SysInfo    `json:"sys_info"`
		SmapVersion int64          `json:"smap_version,string"`
		TStatus     *TargetStatus  `json:"target_status,omitempty"`
		Status      string         `json:"status"`
	}
)

// interface guard
var (
	_ Tracker           = &statsRunner{}
	_ Tracker           = &Prunner{}
	_ Tracker           = &Trunner{}
	_ cluster.XactStats = &RebalanceTargetStats{}
)

//
// private types
//

type (
	metric = statsd.Metric // type alias

	// implemented by the stats runners
	statslogger interface {
		log(uptime time.Duration)
		doAdd(nv NamedVal64)
	}
	runnerHost interface {
		ClusterStarted() bool
	}
	// implements Tracker, inherited by Prunner and Trunner
	statsRunner struct {
		cmn.Named
		stopCh    chan struct{}
		workCh    chan NamedVal64
		ticker    *time.Ticker
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

var (
	_ json.Marshaler   = &CoreStats{}
	_ json.Unmarshaler = &CoreStats{}
)

func (s *CoreStats) init(size int) {
	s.Tracker = make(statsTracker, size)
	s.Tracker.registerCommonStats()
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
	cmn.Assertf(ok, "invalid stats name %q", name)
	switch v.kind {
	case KindLatency:
		if strings.HasSuffix(name, ".ns") {
			nroot := strings.TrimSuffix(name, ".ns")
			if nameSuffix != "" {
				nroot += "." + nameSuffix
			}
			s.statsdC.Send(nroot, 1, metric{
				Type:  statsd.Timer,
				Name:  "latency",
				Value: float64(time.Duration(val) / time.Millisecond),
			})
		}
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
		if strings.HasSuffix(name, ".n") {
			nroot := strings.TrimSuffix(name, ".n")
			if nameSuffix != "" {
				nroot += "." + nameSuffix
			}
			s.statsdC.Send(nroot, 1, metric{Type: statsd.Counter, Name: "count", Value: val})
			v.Lock()
			v.Value += val
			v.Unlock()
		}
	default:
		cmn.AssertMsg(false, v.kind)
	}
}

func (s *CoreStats) copyT(ctracker copyTracker, idlePrefs []string) (idle bool) {
	idle = true
	for name, v := range s.Tracker {
		switch v.kind {
		case KindLatency:
			v.Lock()
			if v.numSamples > 0 {
				ctracker[name] = &copyValue{Value: v.Value / v.numSamples}
				if !match(name, idlePrefs) {
					idle = false
				}
			}
			v.Value = 0
			v.numSamples = 0
			v.Unlock()
		case KindThroughput:
			var throughput int64
			v.Lock()
			if v.Value > 0 {
				throughput = v.Value / cmn.MaxI64(int64(s.statsTime.Seconds()), 1)
				ctracker[name] = &copyValue{Value: throughput}
				idle = false
				v.Value = 0
			}
			v.Unlock()
			if throughput > 0 {
				nroot := strings.TrimSuffix(name, ".bps")
				s.statsdC.Send(nroot, 1,
					metric{Type: statsd.Gauge, Name: "throughput", Value: throughput},
				)
			}
		case KindCounter:
			v.RLock()
			if v.Value > 0 {
				if prev, ok := ctracker[name]; !ok || prev.Value != v.Value {
					ctracker[name] = &copyValue{Value: v.Value}
					if !match(name, idlePrefs) {
						idle = false
					}
				}
			}
			v.RUnlock()
		default:
			ctracker[name] = &copyValue{Value: v.Value} // KindSpecial/KindDelta as is and wo/ lock
		}
	}
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

//
// StatsD client using 8125 (default) StatsD port - https://github.com/etsy/statsd
//
func (s *CoreStats) initStatsD(node *cluster.Snode) (err error) {
	suffix := strings.ReplaceAll(node.ID(), ":", "_")
	statsD, err := statsd.New("localhost", 8125, "ais"+node.Type()+"."+suffix)
	s.statsdC = &statsD
	if err != nil {
		glog.Infof("Failed to connect to StatsD daemon: %v", err)
	}
	return
}

//
// statsValue
//

var (
	_ json.Marshaler   = &statsValue{}
	_ json.Unmarshaler = &statsValue{}
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

var (
	_ json.Marshaler   = &copyValue{}
	_ json.Unmarshaler = &copyValue{}
)

func (v *copyValue) MarshalJSON() (b []byte, err error) { return jsoniter.Marshal(v.Value) }
func (v *copyValue) UnmarshalJSON(b []byte) error       { return jsoniter.Unmarshal(b, &v.Value) }

//
// statsTracker
//

func (tracker statsTracker) register(key, kind string, isCommon ...bool) {
	cmn.Assertf(cmn.StringInSlice(kind, kinds), "invalid stats kind %q", kind)

	tracker[key] = &statsValue{kind: kind}
	if len(isCommon) > 0 {
		tracker[key].isCommon = isCommon[0]
	}
}

// stats that are common to proxy and target
func (tracker statsTracker) registerCommonStats() {
	tracker.register(GetCount, KindCounter, true)
	tracker.register(PutCount, KindCounter, true)
	tracker.register(AppendCount, KindCounter, true)
	tracker.register(PostCount, KindCounter, true)
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

//
// statsunner
//

// interface guard
var (
	_ Tracker            = &statsRunner{}
	_ cmn.ConfigListener = &statsRunner{}
)

func (r *statsRunner) runcommon(logger statslogger) error {
	var (
		i, j   time.Duration
		ticker = time.NewTicker(startupSleep)

		// NOTE: the maximum time we agree to wait for r.daemon.ClusterStarted()
		config   = cmn.GCO.Get()
		deadline = startupDeadlineMultiplier * config.Timeout.Startup
	)
dummy:
	for {
		select {
		case <-r.workCh:
			// drain workCh until the daemon (proxy or target) starts up
		case <-r.stopCh:
			ticker.Stop()
			return nil
		case <-ticker.C:
			if r.daemon.ClusterStarted() {
				break dummy
			}
			j += startupSleep
			if j > deadline {
				ticker.Stop()
				return cmn.ErrStartupTimeout
			}
			i += startupSleep
			if i > config.Timeout.Startup {
				glog.Errorln("startup is taking unusually long time...")
				i = 0
			}
		}
	}
	ticker.Stop()

	// for real now
	glog.Infof("Starting %s", r.GetRunName())
	goMaxProcs = runtime.GOMAXPROCS(0)
	hk.Reg(r.GetRunName()+".gc.logs", r.recycleLogs, logsMaxSizeCheckTime)
	r.ticker = time.NewTicker(config.Periodic.StatsTime)
	startTime, checkNumGorHigh := time.Now(), time.Time{}
	r.startedUp.Store(true)
	for {
		select {
		case nv, ok := <-r.workCh:
			if ok {
				logger.doAdd(nv)
			}
		case <-r.ticker.C:
			uptime := time.Since(startTime)
			logger.log(uptime)
			if ngr := runtime.NumGoroutine(); ngr > goMaxProcs*numGorHigh {
				if ngr >= goMaxProcs*numGorExtreme {
					glog.Errorf("extremely high number of goroutines: %d", ngr)
				}
				now := time.Now()
				if checkNumGorHigh.IsZero() {
					checkNumGorHigh = now
				} else if now.Sub(checkNumGorHigh) > numGorHighCheckTime {
					if ngr < goMaxProcs*numGorExtreme {
						glog.Warningf("high number of goroutines: %d", ngr)
					}
					checkNumGorHigh = time.Time{}
				}
			} else {
				checkNumGorHigh = time.Time{}
			}
		case <-r.stopCh:
			r.ticker.Stop()
			return nil
		}
	}
}

func (r *statsRunner) StartedUp() bool { return r.startedUp.Load() }

func (r *statsRunner) ConfigUpdate(oldConf, newConf *cmn.Config) {
	if oldConf.Periodic.StatsTime != newConf.Periodic.StatsTime {
		r.ticker.Stop()
		r.ticker = time.NewTicker(newConf.Periodic.StatsTime)
	}
}

func (r *statsRunner) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.GetRunName(), err)
	r.stopCh <- struct{}{}
	close(r.stopCh)
}

// common impl
func (r *statsRunner) RegisterAll()               { cmn.Assert(false) } // NOTE: currently, proxy's stats == common and hardcoded
func (r *statsRunner) Add(name string, val int64) { r.workCh <- NamedVal64{Name: name, Value: val} }
func (r *statsRunner) Get(name string) int64      { cmn.Assert(false); return 0 }
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
	logfinfos, err := ioutil.ReadDir(config.Log.Dir)
	if err != nil {
		glog.Errorf("GC logs: cannot read log dir %s, err: %v", config.Log.Dir, err)
		_ = cmn.CreateDir(config.Log.Dir) // FIXME: (local non-containerized + kill/restart under test)
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
			r.removeOlderLogs(tot, maxtotal, config.Log.Dir, logtype, infos)
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
	if glog.V(3) {
		glog.Infof("GC logs: started")
	}
	sort.Slice(filteredInfos, fiLess)
	filteredInfos = filteredInfos[:l-1] // except the last = current
	for _, logfi := range filteredInfos {
		logfqn := filepath.Join(logdir, logfi.Name())
		if err := cmn.RemoveFile(logfqn); err == nil {
			tot -= logfi.Size()
			glog.Infof("GC logs: removed %s", logfqn)
			if tot < maxtotal {
				break
			}
		} else {
			glog.Errorf("GC logs: failed to remove %s", logfqn)
		}
	}
	if glog.V(3) {
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
