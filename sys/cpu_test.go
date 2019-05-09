// Package sys provides methods to read system information
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package sys

import (
	"runtime"
	"testing"

	"github.com/NVIDIA/aistore/tutils/tassert"
)

func TestNumCPU(t *testing.T) {
	numReal := runtime.NumCPU()
	numVirt, _ := NumCPU()
	numCont, err := ContainerNumCPU()
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Hardware CPUs: %d, Containerized: %d, calculated: %d\n", numReal, numCont, numVirt)
	if numCont < 1 || numCont > numReal {
		t.Errorf("Number of CPUs must be between 1 and %d, got %d", numReal, numCont)
	}

	if numCont > numVirt || numVirt < 1 {
		t.Errorf("Number of CPUs(%d) must be greater than 0 and not less than number of CPUs containerized(%d)", numVirt, numCont)
	}
}

func TestLoadAvg(t *testing.T) {
	la, err := LoadAverage()
	tassert.CheckFatal(t, err)
	t.Logf("Load average: %.2f, %.2f, %.2f\n", la.One, la.Five, la.Fifteen)
	tassert.Errorf(t, la.One > 0.0 && la.Five > 0.0 && la.Fifteen > 0.0,
		"All load average must be positive ones")
}

func TestLimitMaxProc(t *testing.T) {
	prev := runtime.GOMAXPROCS(0)
	defer runtime.GOMAXPROCS(prev)

	calc, limited := NumCPU()
	UpdateMaxProcs()
	curr := runtime.GOMAXPROCS(0)
	tassert.Errorf(t, calc == curr, "Failed to set GOMAXPROCS to %d, current value is %d", calc, curr)
	tassert.Errorf(t, !limited && curr == prev, "Failed to set GOMAXPROCS to %d, current value is %d", calc, curr)
}
