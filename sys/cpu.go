// Package sys provides methods to read system information
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"os"
	"runtime"

	"github.com/NVIDIA/aistore/3rdparty/glog"
)

const maxProcsEnvVar = "GOMAXPROCS"

// NumCPU detects if the app is running inside a container and returns
// the number of available CPUs (number of hardware CPU for host OS and
// number of CPU allocated for the container), and if the CPU usage is limited
func NumCPU() (cpus int, limited bool) {
	hostCPU := runtime.NumCPU()
	if !Containerized() {
		return hostCPU, false
	}

	cpus, err := ContainerNumCPU()
	if err != nil {
		cpus = hostCPU
	}

	return cpus, hostCPU != cpus
}

// UpdateMaxProcs sets the new value for GOMAXPROCS if CPU usage is limited
// and is not overridden already by an environment variable
func UpdateMaxProcs() {
	// GOMAXPROCS is overridden - do nothing
	if val, exists := os.LookupEnv(maxProcsEnvVar); exists {
		glog.Infof("GOMAXPROCS is overridden by environment variable: %s", val)
		return
	}

	curr := runtime.GOMAXPROCS(0)
	calc, limited := NumCPU()
	// do nothing is GOMAXPROCS is already equal or less than calculated
	if !limited || curr <= calc {
		glog.Infof("GOMAXPROCS value is OK: %d(%d)", curr, calc)
		return
	}

	glog.Infof("Setting GOMAXPROCS to %d", calc)
	runtime.GOMAXPROCS(calc)
}
