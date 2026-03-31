// Package sys provides helpers to read system info (CPU, memory, loadavg, processes)
// with support for cgroup v2 and a moving-average CPU estimator.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

type (
	LoadAvg struct {
		One, Five, Fifteen float64
	}
	errLoadAvg struct {
		err error
	}
)
