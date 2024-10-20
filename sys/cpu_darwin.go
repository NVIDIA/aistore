// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2024, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"errors"

	"github.com/lufia/iostat"
)

// Containerized returns true if the application is running
// inside a container(docker/lxc/k8s)
func isContainerized() bool { return false }

// ContainerNumCPU returns the approximate number of CPUs allocated for the container.
func containerNumCPU() (int, error) {
	return 0, errors.New("cannot get container cpu stats")
}

// LoadAverage returns the system load average.
func LoadAverage() (avg LoadAvg, err error) {
	loadAvg, err := iostat.ReadLoadAvg()
	if err != nil {
		return avg, err
	}
	return LoadAvg{
		One:     loadAvg.Load1,
		Five:    loadAvg.Load5,
		Fifteen: loadAvg.Load15,
	}, nil
}
