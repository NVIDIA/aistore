// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"fmt"
	"os"
	"runtime"

	"github.com/NVIDIA/aistore/cmn/nlog"
)

type LoadAvg struct {
	One, Five, Fifteen float64
}

var (
	contCPUs      int
	containerized bool
)

func init() {
	contCPUs = runtime.NumCPU()
	if containerized = isContainerized(); containerized {
		if c, err := containerNumCPU(); err == nil {
			contCPUs = c
		} else {
			fmt.Fprintln(os.Stderr, err) // (cannot nlog yet)
		}
	}
}

func Containerized() bool { return containerized }
func NumCPU() int         { return contCPUs }

func GoEnvMaxprocs() {
	if val, exists := os.LookupEnv("GOMEMLIMIT"); exists {
		nlog.Warningln("Go environment: GOMEMLIMIT =", val) // soft memory limit for the runtime (IEC units or raw bytes)
	}
	if val, exists := os.LookupEnv("GOMAXPROCS"); exists {
		nlog.Warningln("Go environment: GOMAXPROCS =", val)
		return
	}

	maxprocs := runtime.GOMAXPROCS(0)
	ncpu := NumCPU() // TODO: (see comment at the top)
	if maxprocs > ncpu {
		nlog.Warningf("Reducing GOMAXPROCS (prev = %d) to %d", maxprocs, ncpu)
		runtime.GOMAXPROCS(ncpu)
	}
}

// return max(1 minute, 5 minute) load average
func MaxLoad() (load float64) {
	avg, err := LoadAverage()
	if err != nil {
		nlog.ErrorDepth(1, err) // unlikely
		return 100
	}
	return max(avg.One, avg.Five)
}
