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
	"github.com/json-iterator/go"
)

//==============================
//
// constants
//
//==============================

const logsTotalSizeCheckTime = time.Hour * 3

const (
	statsKindCounter = "counter"
	statsKindLatency = "latency"
)

// Stats common to ProxyCoreStats and targetCoreStats
const (
	GetCount            = "get.n"
	PutCount            = "put.n"
	PostCount           = "pst.n"
	DeleteCount         = "del.n"
	RenameCount         = "ren.n"
	ListCount           = "lst.n"
	GetLatency          = "get.μs"
	ListLatency         = "lst.μs"
	KeepAliveMinLatency = "kalive.μs.min"
	KeepAliveMaxLatency = "kalive.μs.max"
	KeepAliveLatency    = "kalive.μs"
	Uptime              = "uptime.μs"
	ErrCount            = "err.n"
	ErrGetCount         = "err.get.n"
	ErrDeleteCount      = "err.delete.n"
	ErrPostCount        = "err.post.n"
	ErrPutCount         = "err.put.n"
	ErrHeadCount        = "err.head.n"
	ErrListCount        = "err.list.n"
	ErrRangeCount       = "err.range.n"
)

//==============================
//
// types
//
//==============================

type (
	metric = statsd.Metric // type alias

	// implemented by the stats runners
	statslogger interface {
		log() (runlru bool)
		housekeep(bool)
		doAdd(nv NamedVal64)
	}
	// implemented by the *CoreStats types
	Tracker interface {
		Add(name string, val int64)
		AddErrorHTTP(method string, val int64)
		AddMany(namedVal64 ...NamedVal64)
	}
	NamedVal64 struct {
		Name string
		Val  int64
	}
	statsrunner struct {
		sync.RWMutex
		cmn.NamedConfigured
		stopCh    chan struct{}
		workCh    chan NamedVal64
		starttime time.Time
	}
	// Stats are tracked via a map of stats names (key) to statInstances (values).
	// There are two main types of stats: counter and latency declared
	// using the the kind field. Only latency stats have associatedVals to them
	// that are used in calculating latency measurements.
	statsInstance struct {
		Value         int64 `json:"value"`
		kind          string
		associatedVal int64
	}
	statsTracker map[string]*statsInstance
)

func (stats statsTracker) register(key string, kind string) {
	cmn.Assert(kind == statsKindCounter || kind == statsKindLatency, "Invalid stats kind "+kind)
	stats[key] = &statsInstance{0, kind, 0}
}

// These stats are common to ProxyCoreStats and targetCoreStats
func (stats statsTracker) registerCommonStats() {
	cmn.Assert(stats != nil, "Error attempting to register stats into nil map")

	stats.register(GetCount, statsKindCounter)
	stats.register(PutCount, statsKindCounter)
	stats.register(PostCount, statsKindCounter)
	stats.register(DeleteCount, statsKindCounter)
	stats.register(RenameCount, statsKindCounter)
	stats.register(ListCount, statsKindCounter)
	stats.register(GetLatency, statsKindCounter)
	stats.register(ListLatency, statsKindLatency)
	stats.register(KeepAliveMinLatency, statsKindLatency)
	stats.register(KeepAliveMaxLatency, statsKindLatency)
	stats.register(KeepAliveLatency, statsKindLatency)
	stats.register(Uptime, statsKindLatency)
	stats.register(ErrCount, statsKindCounter)
	stats.register(ErrGetCount, statsKindCounter)
	stats.register(ErrDeleteCount, statsKindCounter)
	stats.register(ErrPostCount, statsKindCounter)
	stats.register(ErrPutCount, statsKindCounter)
	stats.register(ErrHeadCount, statsKindCounter)
	stats.register(ErrListCount, statsKindCounter)
	stats.register(ErrRangeCount, statsKindCounter)
}

func (stat *statsInstance) MarshalJSON() ([]byte, error) {
	return jsoniter.Marshal(stat.Value)
}

func (stat *statsInstance) UnmarshalJSON(b []byte) error {
	return jsoniter.Unmarshal(b, &stat.Value)
}

//
// common statsunner
//

// implements Tracker interface
var _ Tracker = &statsrunner{}

func (r *statsrunner) runcommon(logger statslogger) error {
	r.stopCh = make(chan struct{}, 4)
	r.workCh = make(chan NamedVal64, 256)
	r.starttime = time.Now()

	glog.Infof("Starting %s", r.Getname())
	config := r.Getconf()
	ticker := time.NewTicker(config.Periodic.StatsTime)
	for {
		select {
		case nv, ok := <-r.workCh:
			if ok {
				logger.doAdd(nv)
			}
		case <-ticker.C:
			runlru := logger.log()
			logger.housekeep(runlru)
		case <-r.stopCh:
			ticker.Stop()
			return nil
		}
	}
}

func (r *statsrunner) Stop(err error) {
	glog.Infof("Stopping %s, err: %v", r.Getname(), err)
	r.stopCh <- struct{}{}
	close(r.stopCh)
}

// statslogger interface impl
func (r *statsrunner) log() (runlru bool)  { return false }
func (r *statsrunner) housekeep(bool)      {}
func (r *statsrunner) doAdd(nv NamedVal64) {}

func (r *statsrunner) AddMany(nvs ...NamedVal64) {
	for _, nv := range nvs {
		r.workCh <- nv
	}
}

func (r *statsrunner) Add(name string, val int64) {
	r.workCh <- NamedVal64{name, val}
}

func (r *statsrunner) AddErrorHTTP(method string, val int64) {
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
