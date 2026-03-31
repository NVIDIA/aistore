// Package sys provides helpers to read system info (CPU, memory, loadavg, processes)
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import "github.com/lufia/iostat"

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
