// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/NVIDIA/aistore/cmn/cos"
)

// load averages, with the following limited usage:
// - fallback when cpuTracker has no prior sample or read error
// - `ais show cluster` and friends

func (e *errLoadAvg) Error() string {
	return fmt.Sprint("failed to load averages: ", e.err)
}

func LoadAverage() (avg LoadAvg, _ error) {
	line, err := cos.ReadOneLine(hostLoadAvgPath)
	if err != nil {
		return avg, &errLoadAvg{err}
	}

	fields := strings.Fields(line) // TODO: manual parse to reduce string allocations (can wait)
	if l := len(fields); l < 3 {
		err := fmt.Errorf("unexpected %s format: num-fields=%d", hostLoadAvgPath, l)
		return avg, &errLoadAvg{err}
	}

	avg.One, err = strconv.ParseFloat(fields[0], 64)
	if err == nil {
		avg.Five, err = strconv.ParseFloat(fields[1], 64)
	}
	if err == nil {
		avg.Fifteen, err = strconv.ParseFloat(fields[2], 64)
	}
	if err == nil {
		return avg, nil
	}
	return avg, &errLoadAvg{err}
}
