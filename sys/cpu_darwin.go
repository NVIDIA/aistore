// Package sys provides helpers to read system info (CPU, memory, loadavg, processes)
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"errors"
)

var errDarwin = errors.New("darwin: sys/cpu and sys/mem not supported")

func (*cpu) setNumCgroup() error   { return errDarwin }
func (*cpu) read() (sample, error) { return sample{}, errDarwin }
