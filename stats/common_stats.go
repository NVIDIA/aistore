// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"net/http"
	"sync"
	"time"

	"github.com/NVIDIA/dfcpub/3rdparty/glog"
	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/stats/statsd"
	jsoniter "github.com/json-iterator/go"
)

//==============================
//
// constants
//
//==============================

const logsTotalSizeCheckTime = time.Hour * 3

const (
	KindCounter = "counter"
	KindLatency = "latency"
	KindSpecial = "special"
)

// Stats common to ProxyCoreStats and targetCoreStats
const (
	// KindCounter
	GetCount       = "get.n"
	PutCount       = "put.n"
	PostCount      = "pst.n"
	DeleteCount    = "del.n"
	RenameCount    = "ren.n"
	ListCount      = "lst.n"
	ErrCount       = "err.n"
	ErrGetCount    = "err.get.n"
	ErrDeleteCount = "err.delete.n"
	ErrPostCount   = "err.post.n"
	ErrPutCount    = "err.put.n"
	ErrHeadCount   = "err.head.n"
	ErrListCount   = "err.list.n"
	ErrRangeCount  = "err.range.n"
	// KindLatency
	GetLatency          = "get.μs"
	ListLatency         = "lst.μs"
	KeepAliveMinLatency = "kalive.μs.min"
	KeepAliveMaxLatency = "kalive.μs.max"
	KeepAliveLatency    = "kalive.μs"
	// KindSpecial
	Uptime = "up.μs.time"
)

//
// public types
//

type (
	Tracker interface {
		Add(name string, val int64)
		AddErrorHTTP(method string, val int64)
		AddMany(namedVal64 ...NamedVal64)
		Register(name string, kind string)
	}
	NamedVal64 struct {
		Name string
		Val  int64
	}
)

//
// private types
//

type (
	metric = statsd.Metric // type alias

	// implemented by the stats runners
	statslogger interface {
		log() (runlru bool)
		housekeep(bool)
		doAdd(nv NamedVal64)
	}
	// implements Tracker, inherited by Prunner ans Trunner
	statsRunner struct {
		cmn.Named
		stopCh    chan struct{}
		workCh    chan NamedVal64
		starttime time.Time
		ticker    *time.Ticker
		ctracker  copyTracker // to avoid making it at runtime
	}
	// Stats are tracked via a map of stats names (key) to statsValue (values).
	// There are two main types of stats: counter and latency declared
	// using the the kind field. Only latency stats have numSamples used to compute latency.
	statsValue struct {
		sync.RWMutex
		Value      int64 `json:"v"`
		kind       string
		numSamples int64
		cumulative int64
		isCommon   bool // optional, common to the proxy and target
	}
	copyValue struct {
		Value int64 `json:"v"`
	}
	statsTracker map[string]*statsValue
	copyTracker  map[string]*copyValue
)

//
// globals
//
var jsonCompat = jsoniter.ConfigCompatibleWithStandardLibrary

//
// statsValue
//

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

func (v *copyValue) MarshalJSON() (b []byte, err error) { return jsoniter.Marshal(v.Value) }
func (v *copyValue) UnmarshalJSON(b []byte) error       { return jsoniter.Unmarshal(b, &v.Value) }

//
// statsTracker
//

func (tracker statsTracker) register(key string, kind string, isCommon ...bool) {
	cmn.Assert(kind == KindCounter || kind == KindLatency || kind == KindSpecial, "Invalid stats kind '"+kind+"'")
	tracker[key] = &statsValue{kind: kind}
	if len(isCommon) > 0 {
		tracker[key].isCommon = isCommon[0]
	}
}

// stats that are common to proxy and target
func (tracker statsTracker) registerCommonStats() {
	tracker.register(GetCount, KindCounter, true)
	tracker.register(PutCount, KindCounter, true)
	tracker.register(PostCount, KindCounter, true)
	tracker.register(DeleteCount, KindCounter, true)
	tracker.register(RenameCount, KindCounter, true)
	tracker.register(ListCount, KindCounter, true)
	tracker.register(GetLatency, KindCounter, true)
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
	//
	tracker.register(Uptime, KindSpecial, true)
}

//
// statsunner
//

var (
	_ Tracker            = &statsRunner{}
	_ cmn.ConfigListener = &statsRunner{}
)

func (r *statsRunner) runcommon(logger statslogger) error {
	r.stopCh = make(chan struct{}, 4)
	r.workCh = make(chan NamedVal64, 256)
	r.starttime = time.Now()

	glog.Infof("Starting %s", r.Getname())
	r.ticker = time.NewTicker(cmn.GCO.Get().Periodic.StatsTime)
	for {
		select {
		case nv, ok := <-r.workCh:
			if ok {
				logger.doAdd(nv)
			}
		case <-r.ticker.C:
			runlru := logger.log()
			logger.housekeep(runlru)
		case <-r.stopCh:
			r.ticker.Stop()
			return nil
		}
	}
}

func (r *statsRunner) ConfigUpdate(oldConf, newConf *cmn.Config) {
	if oldConf.Periodic.StatsTime != newConf.Periodic.StatsTime {
		r.ticker.Stop()
		r.ticker = time.NewTicker(newConf.Periodic.StatsTime)
	}
}

func (r *statsRunner) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.Getname(), err)
	r.stopCh <- struct{}{}
	close(r.stopCh)
}

// statslogger interface impl
func (r *statsRunner) Register(name string, kind string) { cmn.Assert(false) } // NOTE: currently, proxy's stats == common and hardcoded
func (r *statsRunner) housekeep(bool)                    {}
func (r *statsRunner) Add(name string, val int64)        { r.workCh <- NamedVal64{name, val} }
func (r *statsRunner) AddMany(nvs ...NamedVal64) {
	for _, nv := range nvs {
		r.workCh <- nv
	}
}

func (r *statsRunner) AddErrorHTTP(method string, val int64) {
	switch method {
	case http.MethodGet:
		r.workCh <- NamedVal64{ErrGetCount, val}
	case http.MethodDelete:
		r.workCh <- NamedVal64{ErrDeleteCount, val}
	case http.MethodPost:
		r.workCh <- NamedVal64{ErrPostCount, val}
	case http.MethodPut:
		r.workCh <- NamedVal64{ErrPutCount, val}
	case http.MethodHead:
		r.workCh <- NamedVal64{ErrHeadCount, val}
	default:
		r.workCh <- NamedVal64{ErrCount, val}
	}
}
