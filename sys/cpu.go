// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"os"
	"runtime"

	"github.com/NVIDIA/aistore/3rdparty/glog"
)

const maxProcsEnvVar = "GOMAXPROCS"

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
			glog.Error(err)
		}
	}
	cache = &procCache{procs: make(map[int]*ProcStats, 1)}
}

func Containerized() bool { return containerized }
func NumCPU() int         { return contCPUs }

// SetMaxProcs sets GOMAXPROCS = NumCPU unless already overridden via Go environment
func SetMaxProcs() {
	if val, exists := os.LookupEnv(maxProcsEnvVar); exists {
		glog.Infof("GOMAXPROCS is already set via Go environment: %q", val)
		return
	}
	maxprocs := runtime.GOMAXPROCS(0)
	ncpu := NumCPU()
	glog.Infof("GOMAXPROCS %d, num CPUs %d", maxprocs, ncpu)
	if maxprocs > ncpu {
		glog.Infof("Reducing GOMAXPROCS to %d (num CPUs)", ncpu)
		runtime.GOMAXPROCS(ncpu)
	}
}
