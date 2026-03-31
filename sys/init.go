// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"fmt"
	"os"
	"runtime"

	"github.com/NVIDIA/aistore/cmn/debug"
	"github.com/NVIDIA/aistore/cmn/nlog"
)

var (
	gcpu cpu

	cgroupVer    int  // 0 = none/bare-metal, 1 = v1, 2 = v2
	contForced   bool // <-- feat.ForceContainerCPUMem at startup
	contDetected bool
)

// num CPUs may get adjusted by Init() below
func init() {
	gcpu.num = runtime.NumCPU()
}

func isContainerized() bool { return contDetected || contForced }

// container-aware CPU count; GOMAXPROCS
// - AIS node (`aisnode`) calls Init() once upon startup
// - external modules that skip it still get a sane NumCPU() - see above
// - return ("cgroup-v2", etc.) enumerated tags
func Init(forceCont bool) string {
	debug.Assert(gcpu.num > 0)

	contForced = forceCont
	if contDetected = detect(); contDetected || forceCont {
		if err := gcpu.setNumCgroup(); err != nil {
			fmt.Fprintln(os.Stderr, err) // (cannot nlog yet)
		}
	}

	// - warn GOMEMLIMIT
	// - possibly reduce GOMAXPROCS to container's num CPUs
	if val, exists := os.LookupEnv("GOMEMLIMIT"); exists {
		nlog.Warningln("Go environment: GOMEMLIMIT =", val) // soft memory limit for the runtime (IEC units or raw bytes)
	}
	if val, exists := os.LookupEnv("GOMAXPROCS"); exists {
		nlog.Warningln("Go environment: GOMAXPROCS =", val)
		return _tag()
	}

	maxprocs := runtime.GOMAXPROCS(0)
	ncpu := NumCPU() // gcpu.num
	if maxprocs > ncpu {
		nlog.Warningf("Reducing GOMAXPROCS (prev = %d) to %d", maxprocs, ncpu)
		runtime.GOMAXPROCS(ncpu)
	}
	return _tag()
}

func _tag() string {
	switch cgroupVer {
	case 1:
		return "cgroup-v1"
	case 2:
		return "cgroup-v2"
	default:
		debug.Assert(cgroupVer == 0, cgroupVer)
		switch {
		case contDetected:
			return "detected-no-cgroup"
		case contForced:
			return "forced-no-cgroup"
		default:
			return "" // bare-metal
		}
	}
}
