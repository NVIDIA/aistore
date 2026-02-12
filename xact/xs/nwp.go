// Package xs is a collection of eXtended actions (xactions), including multi-object
// operations, list-objects, (cluster) rebalance and (target) resilver, ETL, and more.
/*
 * Copyright (c) 2025-2026, NVIDIA CORPORATION. All rights reserved.
 */
package xs

import (
	"errors"
	"runtime"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/load"
	"github.com/NVIDIA/aistore/cmn/nlog"
	"github.com/NVIDIA/aistore/cmn/oom"
	"github.com/NVIDIA/aistore/fs"
	"github.com/NVIDIA/aistore/stats"
	"github.com/NVIDIA/aistore/sys"
)

// num-workers parallelism (nwp) ========================================================
//
// Multi-object xactions (tco, tcb, evict, prefetch, and archive) crunch lists, ranges,
// and/or prefixes of multi-object distributed workloads. Distributed processing is done
// by joggers (one per mountpath) and, in addition, by the so-called "nwp workers",
// or simply, workers.
//
// When the user does not explicitly specify NumWorkers,
// we use media-type intelligence (HDD, SSD, NVMe) to determine a reasonable default.
// For references, see: `grep NumWorkers api/`
//
// Default num-workers = numMpaths * media-multiplier, where:
// - NVMe:  8 workers per mountpath
// - SSD:   4 workers per mountpath
// - HDD:   1 worker  per mountpath
//
// tuneNumWorkers() - the main function below - does both:
// - computing the default number of workers,
// - throttling (or clamping) the latter down based on current system load
// =======================================================================================

// shared work channel sizing
const (
	nwpBurstMult = 48   // channel size = burst * num-workers
	nwpBurstMax  = 4096 // upper bound on channel size
)

// num-workers specials
const (
	nwpNone = -1 // no workers: iterated LOMs executed by the iterating goroutine
	nwpMin  = 2  // throttled minimum
	nwpDflt = 0  // resolve at runtime via defaultNW()
)

// media-type multipliers (workers per mountpath)
const (
	nwpMultNVMe = 8
	nwpMultSSD  = 4
	nwpMultHDD  = 1
)

// return the default number of workers based on:
// - number of available mountpaths
// - detected storage media type (NVMe, SSD, HDD)
// - hardcoded multiplier (above)
func _defaultNW(numMpaths int) int {
	switch {
	case fs.IsNVMe():
		return numMpaths * nwpMultNVMe
	case fs.IsRotational():
		return numMpaths * nwpMultHDD
	default: // SSD
		return numMpaths * nwpMultSSD
	}
}

// estimate the requested number of workers given:
// a) user-specified number, if any
// b) current load
// used by all list-range type jobs, tcb, and blob-download
// - xname: xaction name (for logging)
// - n: requested number of workers
// - numMpaths: number of available mountpaths
func tuneNumWorkers(xname string, numWorkers, numMpaths int) (int, error) {
	const memExtremeMsg = "extreme memory pressure"
	var (
		ngr     = runtime.NumGoroutine()
		ngrLoad = load.Gor(ngr)
	)
	// 1. apply system default
	if numWorkers == nwpDflt {
		numWorkers = _defaultNW(numMpaths)
	}

	// 2. goroutine load
	if ngrLoad == load.Critical {
		nlog.Warningln(xname, stats.NgrPrompt, ngr)
		return nwpNone, nil
	}

	numWorkers = min(sys.MaxParallelism()+4, numWorkers)

	// yellow alert (high)
	if ngrLoad == load.High {
		nlog.Warningln(xname, stats.NgrPrompt, ngr)
		numWorkers = min(numWorkers, numMpaths)
	}

	// 3. memory pressure
	memLoad := load.Mem()
	switch memLoad {
	case load.Critical:
		oom.FreeToOS(true)
		if !cmn.Rom.TestingEnv() {
			return 0, errors.New(xname + ": " + memExtremeMsg + " - not starting")
		}
		return nwpNone, nil
	case load.High:
		if ngrLoad == load.High {
			return nwpNone, nil
		}
		numWorkers = min(nwpMin+1, numWorkers)
	}

	// 4. CPU load averages
	cpuLoad := load.CPU()
	if cpuLoad >= load.High {
		if lv, wm := sys.MaxLoad(), sys.HighLoadWM(); lv >= float64(wm) {
			nlog.Warningln(xname, "high load [", lv, wm, "]")
		}
		if ngrLoad == load.High {
			return nwpNone, nil
		}
		numWorkers = min(nwpMin, numWorkers)
	}

	return numWorkers, nil
}
