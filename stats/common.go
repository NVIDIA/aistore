// Package stats provides methods and functionality to register, track, log,
// and StatsD-notify statistics that, for the most part, include "counter" and "latency" kinds.
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package stats

import (
	"strings"
	"time"

	"github.com/NVIDIA/aistore/cmn"
)

const (
	dfltPeriodicFlushTime = time.Minute            // when `config.Log.FlushTime` is 0 (zero)
	dfltPeriodicTimeStamp = time.Hour              // extended date/time complementary to log timestamps (e.g., "11:29:11.644596")
	maxStatsLogInterval   = int64(3 * time.Minute) // when idle; secondly, an upper limit on `config.Log.StatsTime`
	maxCapLogInterval     = int64(4 * time.Hour)   // to see capacity at least few times a day (when idle)
)

// more periodic
const (
	maxLogSizeCheckTime = 48 * time.Minute       // periodically check the logs for max accumulated size
	startupSleep        = 300 * time.Millisecond // periodically poll ClusterStarted()
	numGorHighCheckTime = 2 * time.Minute        // periodically log a warning if the number of goroutines remains high
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
	GetLatencyTotal  = "get.ns.total"
	ListLatency      = "lst.ns"
	KeepAliveLatency = "kalive.ns"

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
