// Package sys provides methods to read system information
/*
 * Copyright (c) 2019, NVIDIA CORPORATION. All rights reserved.
 */
package sys

// Do not import main 'tutils' package because of circular dependency
// Use t.Logf or t.Errorf instead of tutils.Logf
import (
	"math"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/tutils/tassert"
)

func checkSkipOS(t *testing.T, os ...string) {
	if cmn.StringInSlice(runtime.GOOS, os) {
		t.Skipf("skipping test for %s platform", runtime.GOOS)
	}
}

func TestNumCPU(t *testing.T) {
	checkSkipOS(t, "darwin")

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

	tassert.Errorf(t, mem.Total > 0 && mem.Free > 0 && mem.ActualFree > 0 && mem.ActualUsed > 0,
		"All items must be greater than zero: %+v", mem)
	tassert.Errorf(t, mem.Total > mem.Free, "Free is greater than Total memory: %+v", mem)
	tassert.Errorf(t, mem.Total > mem.Used, "Used is greater than Total memory: %+v", mem)
	tassert.Errorf(t, mem.Total > mem.ActualUsed, "ActualUsed is greater than Total memory: %+v", mem)
	tassert.Errorf(t, mem.Total > mem.ActualFree, "ActualFree is greater than Total memory: %+v", mem)
	tassert.Errorf(t, mem.Total == mem.Free+mem.Used, "Total must be = Free + Used: %+v", mem)
	t.Logf("Memory stats: %+v", mem)

	checkSkipOS(t, "darwin")

	memOS, err := HostMem()
	tassert.CheckFatal(t, err)
	memCont, err := ContainerMem()
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, memOS.Total >= memCont.Total,
		"Container's memory stats are greater than host's ones.\nOS: %+v\nContainer: %+v", memOS, memCont)
	if memOS.SwapTotal == 0 && memOS.SwapFree == 0 {
		// Not an error(e.g, Jenkins VM has swap off) - just a warning
		t.Logf("Either swap is off or failed to read its stats")
	}
}

func TestProc(t *testing.T) {
	checkSkipOS(t, "darwin")

	pid := os.Getpid()
	stats, err := ProcessStats(pid)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, stats.Mem.Size > 0 && stats.Mem.Resident > 0 && stats.Mem.Share > 0,
		"Failed to read memory stats: %+v", stats.Mem)

	// burn CPU for a few seconds by calculating prime numbers
	// and make a short break to make usage lower than 100%
	for i := 0; i < 20; i++ {
		n := int64(1)<<52 + int64((i*2)|1)
		middle := int64(math.Sqrt(float64(n)))
		divider := int64(3)
		prime := true
		for divider <= middle {
			if n%divider == 0 {
				prime = false
			}
			divider += 2
		}
		t.Logf("%d is prime: %v", n, prime)
		time.Sleep(100 * time.Millisecond)
	}

	newStats, err := ProcessStats(pid)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, newStats.CPU.User > 0, "Failed to read CPU stats: %+v", newStats.CPU)
	tassert.Errorf(t, newStats.CPU.User+newStats.CPU.System == newStats.CPU.Total,
		"Total must be equal to sum of User and System: %+v", newStats.CPU)
	tassert.Errorf(t, newStats.CPU.Total > stats.CPU.Total, "New stats must show more CPU used. Old usage %d, new one: %d", stats.CPU.Total, newStats.CPU.Total)
	tassert.Errorf(t, newStats.CPU.Percent > 0.0, "Process must use some CPU. Usage: %g", stats.CPU.Percent)
	tassert.Errorf(t, newStats.CPU.Percent < 100.0, "Process should use less than 100%% CPU. Usage: %g", newStats.CPU.Percent)
	tassert.Errorf(t, newStats.CPU.LastTime > stats.CPU.LastTime, "Time must change: new %d, old %d", newStats.CPU.LastTime, stats.CPU.LastTime)
	t.Logf("Process CPU usage: %6.2f%%", newStats.CPU.Percent)
}
