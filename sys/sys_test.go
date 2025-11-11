// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2025, NVIDIA CORPORATION. All rights reserved.
 */
package sys_test

// Do not import the main 'tools' package because of circular dependency
// Use t.Logf or t.Errorf instead of tlog.Logf
import (
	"math"
	"os"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/tools/tassert"
)

func checkSkipOS(t *testing.T, oss ...string) {
	if slices.Contains(oss, runtime.GOOS) {
		t.Skipf("skipping test for %s platform", runtime.GOOS)
	}
}

func TestNumCPU(t *testing.T) {
	checkSkipOS(t, "darwin")
	if sys.NumCPU() < 1 || sys.NumCPU() > runtime.NumCPU() {
		t.Errorf("Wrong number of CPUs %d (%d)", sys.NumCPU(), runtime.NumCPU())
	}
}

func TestLoadAvg(t *testing.T) {
	la, err := sys.LoadAverage()
	tassert.CheckFatal(t, err)
	t.Logf("Load average: %.2f, %.2f, %.2f\n", la.One, la.Five, la.Fifteen)
	tassert.Errorf(t, la.One > 0.0 && la.Five > 0.0 && la.Fifteen > 0.0,
		"All load average must be positive ones")
}

func TestMaxProcs(t *testing.T) {
	newval := 4
	prev := runtime.GOMAXPROCS(newval)
	tassert.Errorf(t, runtime.GOMAXPROCS(0) == newval, "Failed to set GOMAXPROCS to %d", newval)

	runtime.GOMAXPROCS(prev)
	tassert.Errorf(t, runtime.GOMAXPROCS(0) == prev, "Failed to restore GOMAXPROCS to %d", prev)
}

func TestMemoryStats(t *testing.T) {
	var mem sys.MemStat
	err := mem.Get()
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

	var memHost, memCont sys.MemStat
	err = memHost.Get()
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, memHost.Total >= memCont.Total,
		"Container's memory total is greater than the host one.\nOS: %+v\nContainer: %+v", memHost, memCont)
	if memHost.SwapTotal == 0 && memHost.SwapFree == 0 {
		// Not an error(e.g, Jenkins VM has swap off) - just a warning
		t.Log("Either swap is off or failed to read its stats")
	}
}

func TestProc(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping %s in short mode", t.Name())
	}
	checkSkipOS(t, "darwin")

	pid := os.Getpid()
	stats, err := sys.ProcessStats(pid)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, stats.Mem.Size > 0 && stats.Mem.Resident > 0 && stats.Mem.Share > 0,
		"Failed to read memory stats: %+v", stats.Mem)

	// burn CPU for a few seconds by calculating prime numbers
	// and make a short break to make usage lower than 100%
	for i := range 20 {
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

	newStats, err := sys.ProcessStats(pid)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, newStats.CPU.User > 0, "Failed to read CPU stats: %+v", newStats.CPU)
	tassert.Errorf(t, newStats.CPU.User+newStats.CPU.System == newStats.CPU.Total,
		"Total must be equal to sum of User and System: %+v", newStats.CPU)
	tassert.Errorf(t, newStats.CPU.Total > stats.CPU.Total, "New stats must show more CPU used. Old usage %d, new one: %d", stats.CPU.Total, newStats.CPU.Total)
	tassert.Errorf(t, newStats.CPU.Percent > 0.0, "Process must use some CPU. Usage: %g", stats.CPU.Percent)
	t.Logf("Process CPU usage: %6.2f%%", newStats.CPU.Percent)
}
