// Package sys provides methods to read system information
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package sys

// Do not import main 'tutils' package because of circular dependency
// Use t.Logf or t.Errorf instead of tutils.Logf
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

func TestMemoryStats(t *testing.T) {
	mem, err := Mem()
	tassert.CheckFatal(t, err)

	tassert.Errorf(t, mem.Total > 0 && mem.Free > 0, "Free or Total memory is zero: %+v", mem)
	tassert.Errorf(t, mem.Total > mem.Free, "Free is greater than Total memory: %+v", mem)
	tassert.Errorf(t, mem.Total > mem.Used, "Used is greater than Total memory: %+v", mem)
	tassert.Errorf(t, mem.ActualFree >= mem.Free, "Free is lower than ActualFree memory: %+v", mem)
	tassert.Errorf(t, mem.ActualUsed <= mem.Used, "Used is greater than ActualUsed memory: %+v", mem)
	t.Logf("Memory stats: %+v", mem)

	memOS, err := HostMem()
	tassert.CheckFatal(t, err)
	memCont, err := ContainerMem()
	tassert.CheckFatal(t, err)
	tassert.Errorf(t,
		memOS.Total >= memCont.Total &&
			memOS.Free >= memCont.Free &&
			memOS.SwapTotal >= memCont.SwapTotal &&
			memOS.SwapFree >= memCont.SwapFree,
		"Container's memory stats are greater than host's ones.\nOS: %+v\nContainer: %+v", memOS, memCont)
	if memOS.SwapTotal == 0 && memOS.SwapFree == 0 {
		// not a error(e.g, Jenkins VM has swap off) - just a warining
		t.Logf("Either swap is off or failed to read its stats")
	}
}
