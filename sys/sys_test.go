// Package sys provides methods to read system information
/*
 * Copyright (c) 2018-2026, NVIDIA CORPORATION. All rights reserved.
 */
package sys_test

import (
	"fmt"
	"math"
	"os"
	"runtime"
	"slices"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/sys"
	"github.com/NVIDIA/aistore/tools/tassert"
)

// example running it in a constrained container:
// docker run --rm --cpus=1.5 --memory=512m   -v "$PWD":/src -w /src \
//            -v "$HOME/go/pkg/mod":/go/pkg/mod -v "$HOME/.cache/go-build":/root/.cache/go-build \
//            golang:1.25   go test ./sys -run . -v -count=1 2>&

func TestMain(m *testing.M) {
	contTag := sys.Init(false /*forceCont*/)
	if contTag != "" {
		fmt.Println("TestMain/sys: running in container [", contTag, "]")
	}
	m.Run()
}

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

	tassert.Errorf(t, mem.ActualUsed+mem.ActualFree == mem.Total, "ActualUsed + ActualFree must equal Total: %+v", mem)
	tassert.Errorf(t, mem.Used >= mem.ActualUsed, "Used must be >= ActualUsed (BuffCache=%d): %+v", mem.BuffCache, mem)

	var sb cos.SB
	sb.Grow(80)
	mem.Str(&sb)
	t.Logf("Memory stats: %s", sb)

	if mem.SwapTotal == 0 && mem.SwapFree == 0 {
		t.Log("Either swap is off or failed to read its stats")
	}
}

func TestProcAndMaxLoad(t *testing.T) {
	if testing.Short() {
		t.Skipf("skipping %s in short mode", t.Name())
	}
	checkSkipOS(t, "darwin")

	pid := os.Getpid()
	stats, err := sys.ProcessStats(pid)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, stats.Mem.Size > 0 && stats.Mem.Resident > 0 && stats.Mem.Share > 0,
		"Failed to read memory stats: %+v", stats.Mem)

	load1, extreme1 := sys.MaxLoad2()
	t.Logf("First call: load=%d, extreme=%t", load1, extreme1)

	// make sure cpu tracker gets updated
	time.Sleep(time.Duration(sys.MinWallIval))

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

	load2, extreme2 := sys.MaxLoad2()
	t.Logf("Second call: load=%d, extreme=%v", load2, extreme2)
	tassert.Errorf(t, load2 > load1 || extreme2, "Second call: (%d, %t) vs (%d, %t)", load2, extreme2, load1, extreme1)

	newStats, err := sys.ProcessStats(pid)
	tassert.CheckFatal(t, err)
	tassert.Errorf(t, newStats.CPU.User > 0, "Failed to read CPU stats: %+v", newStats.CPU)
	tassert.Errorf(t, newStats.CPU.User+newStats.CPU.System == newStats.CPU.Total,
		"Total must be equal to sum of User and System: %+v", newStats.CPU)
	tassert.Errorf(t, newStats.CPU.Total > stats.CPU.Total, "New stats must show more CPU used. Old usage %d, new one: %d", stats.CPU.Total, newStats.CPU.Total)

	tassert.Errorf(t, newStats.CPU.Percent > 0.1, "Process must use some CPU. Usage: %g", stats.CPU.Percent)
	tassert.Errorf(t, newStats.CPU.Percent < 100.0, "Process CPU percent looks bogus: %g", newStats.CPU.Percent)
	t.Logf("Process CPU usage: %6.2f%%", newStats.CPU.Percent)
}
