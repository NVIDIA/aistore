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

// TODO -- FIXME:
// - see cpu_linux.go comment on detecting containerization
// - blog https://www.riverphillips.dev/blog/go-cfs
// - available "maxprocs" open-source

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
