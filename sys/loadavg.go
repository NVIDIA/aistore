// Package sys provides helpers to read system info (CPU, memory, loadavg, processes)
// with support for cgroup v2 and a moving-average CPU estimator.
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

type (
	LoadAvg struct {
		One     float64 `msg:"o"`
		Five    float64 `msg:"f"`
		Fifteen float64 `msg:"t"`
	}
	//msgp:ignore errLoadAvg
	errLoadAvg struct {
		err error
	}
)
