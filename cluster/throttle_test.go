/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
// Package cluster provides common interfaces and local access to cluster-level metadata
package cluster

import (
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/ios"
)

const fmstr = "Expected [%v] actual [%v]"

type Test struct {
	name   string
	method func(*testing.T)
}

type ctxT struct {
	config cmn.Config
}

var riostat *ios.IostatRunner

var ctx = &ctxT{}

var aTests = []Test{
	{"testHighDiskUtilization", testHighDiskUtilization},
	{"testLowDiskUtilization", testLowDiskUtilization},
	{"testHighDiskUtilizationShortDuration", testHighDiskUtilizationShortDuration},
	{"testHighDiskUtilizationLongDuration", testHighDiskUtilizationLongDuration},
	{"testIncreasingDiskUtilization", testIncreasingDiskUtilization},
	{"testDecreasingDiskUtilization", testDecreasingDiskUtilization},
	{"testMediumDiskUtilization", testMediumDiskUtilization},
	{"testConstantDiskUtilization", testConstantDiskUtilization},
	{"testMultipleLRUContexts", testMultipleLRUContexts},
	{"testChangedFSUsedPercentageBeforeCapCheck", testChangedFSUsedPercentageBeforeCapCheck},
	{"testChangedDiskUtilBeforeUtilCheck", testChangedDiskUtilBeforeUtilCheck},
}

func init() {
	ctx.config.LRU.LowWM = 75
	ctx.config.LRU.HighWM = 90
	ctx.config.Xaction.DiskUtilLowWM = 60
	ctx.config.Xaction.DiskUtilHighWM = 80
	ctx.config.Periodic.StatsTime = time.Second
}

func Test_Throttle(t *testing.T) {
	var dcopy = *ctx
	cmn.CopyStruct(&dcopy, ctx)
	defer func() { cmn.CopyStruct(ctx, &dcopy) }()
	ctx.config.LRU.LowWM = 0
	ctx.config.LRU.HighWM = 90
	ctx.config.Xaction.DiskUtilLowWM = 10
	ctx.config.Xaction.DiskUtilHighWM = 40
	ctx.config.Periodic.StatsTime = time.Second

	fs.Mountpaths = fs.NewMountedFS("local", "cloud")
	fs.Mountpaths.Add("/")

	riostat = ios.NewIostatRunner(fs.Mountpaths)
	riostat.Setconf(&ctx.config)

	go riostat.Run()
	time.Sleep(time.Second)
	defer func() { riostat.Stop(nil) }()

	for disk := range riostat.Disk {
		riostat.Disk[disk] = make(cmn.SimpleKVs, 0)
		riostat.Disk[disk]["%util"] = strconv.Itoa(0)

	}

	testHighFSCapacityUsed(t, ctx.config.Xaction.DiskUtilLowWM-10)
	testHighFSCapacityUsed(t, ctx.config.Xaction.DiskUtilLowWM+10)
	testHighFSCapacityUsed(t, ctx.config.Xaction.DiskUtilHighWM+10)

	for _, test := range aTests {
		t.Run(test.name, test.method)
	}
}

func testHighFSCapacityUsed(t *testing.T, diskUtil int64) {
	curCapacity, _ := ios.GetFSUsedPercentage("/")
	oldLRUHWM := ctx.config.LRU.HighWM
	ctx.config.LRU.HighWM = int64(curCapacity - 1)
	thrctx := newThrottleContext()
	sleepDuration := getSleepDuration(diskUtil, thrctx)
	if sleepDuration != 0 {
		t.Errorf(fmstr, 0, sleepDuration)
	}
	ctx.config.LRU.HighWM = oldLRUHWM
}

func testHighDiskUtilization(t *testing.T) {
	diskUtil := ctx.config.Xaction.DiskUtilHighWM + 10
	thrctx := newThrottleContext()
	sleepDuration := getSleepDuration(diskUtil, thrctx)
	expectedDuration := 1 * time.Millisecond
	if sleepDuration != expectedDuration {
		t.Errorf(fmstr, expectedDuration, sleepDuration)
	}
}

func testLowDiskUtilization(t *testing.T) {
	diskUtil := ctx.config.Xaction.DiskUtilLowWM - 10
	thrctx := newThrottleContext()
	sleepDuration := getSleepDuration(diskUtil, thrctx)
	if sleepDuration != 0 {
		t.Errorf(fmstr, 0, sleepDuration)
	}
}

func testHighDiskUtilizationShortDuration(t *testing.T) {
	diskUtil := ctx.config.Xaction.DiskUtilHighWM + 5
	thrctx := newThrottleContext()
	sleepDuration := getSleepDuration(diskUtil, thrctx)
	if sleepDuration != initThrottleSleep {
		t.Errorf(fmstr, initThrottleSleep, sleepDuration)
	}

	counter := 3
	for i := 0; i < counter; i++ {
		diskUtil += 5
		sleepDuration := getSleepDuration(diskUtil, thrctx)
		expectedDuration := time.Duration(1<<uint(i+1)) * time.Millisecond
		if sleepDuration != expectedDuration {
			t.Errorf(fmstr, expectedDuration, sleepDuration)
		}
	}
}

func testHighDiskUtilizationLongDuration(t *testing.T) {
	thrctx := newThrottleContext()
	diskUtil := ctx.config.Xaction.DiskUtilHighWM + 5
	sleepDuration := getSleepDuration(diskUtil, thrctx)
	if sleepDuration != initThrottleSleep {
		t.Errorf(fmstr, initThrottleSleep, sleepDuration)
	}

	counter := 9
	for i := 0; i < counter; i++ {
		diskUtil += 5
		sleepDuration := getSleepDuration(diskUtil, thrctx)
		expectedDuration := time.Duration(1<<uint(i+1)) * time.Millisecond
		if sleepDuration != expectedDuration {
			t.Errorf(fmstr, expectedDuration, sleepDuration)
		}
	}

	diskUtil += 5
	sleepDuration = getSleepDuration(diskUtil, thrctx)
	expectedDuration := 1 * time.Second
	if sleepDuration != expectedDuration {
		t.Errorf(fmstr, expectedDuration, sleepDuration)
	}
}

func testIncreasingDiskUtilization(t *testing.T) {
	thrctx := newThrottleContext()
	diskUtil := ctx.config.Xaction.DiskUtilLowWM - 10
	sleepDuration := getSleepDuration(diskUtil, thrctx)
	if sleepDuration != 0 {
		t.Errorf(fmstr, 0, sleepDuration)
	}

	diskUtil = ctx.config.Xaction.DiskUtilLowWM + 10
	sleepDuration = getSleepDuration(diskUtil, thrctx)
	if sleepDuration <= 1*time.Millisecond || sleepDuration >= 2*time.Millisecond {
		t.Errorf("Sleep duration [%v] expected between 1ms and 2ms", sleepDuration)
	}

	expectedDuration := sleepDuration * 2
	diskUtil = ctx.config.Xaction.DiskUtilHighWM + 10
	sleepDuration = getSleepDuration(diskUtil, thrctx)
	if sleepDuration != expectedDuration {
		t.Errorf(fmstr, expectedDuration, sleepDuration)
	}
}

func testDecreasingDiskUtilization(t *testing.T) {
	thrctx := newThrottleContext()
	diskUtil := ctx.config.Xaction.DiskUtilHighWM + 10
	sleepDuration := getSleepDuration(diskUtil, thrctx)
	expectedDuration := 1 * time.Millisecond
	if sleepDuration != expectedDuration {
		t.Errorf(fmstr, expectedDuration, sleepDuration)
	}

	diskUtil = ctx.config.Xaction.DiskUtilLowWM + 10
	sleepDuration = getSleepDuration(diskUtil, thrctx)
	if sleepDuration <= 1*time.Millisecond || sleepDuration >= 2*time.Millisecond {
		t.Errorf("Sleep duration [%v] expected between 1ms and 2ms", sleepDuration)
	}

	diskUtil = ctx.config.Xaction.DiskUtilLowWM - 10
	sleepDuration = getSleepDuration(diskUtil, thrctx)
	if sleepDuration != 0 {
		t.Errorf("Throttling is not required when disk utilization [%d] is lower than LowWM [%d].",
			diskUtil, ctx.config.Xaction.DiskUtilLowWM)
	}
}

func testMediumDiskUtilization(t *testing.T) {
	thrctx := newThrottleContext()
	diskUtil := ctx.config.Xaction.DiskUtilLowWM + 5
	sleepDuration := getSleepDuration(diskUtil, thrctx)
	expectedDuration := 1 * time.Millisecond
	if sleepDuration <= expectedDuration || sleepDuration >= expectedDuration*2 {
		t.Errorf("Sleep duration [%v] expected between [%v] and [%v]",
			sleepDuration, expectedDuration, expectedDuration*2)
	}

	diskUtil += 5
	sleepDuration = getSleepDuration(diskUtil, thrctx)
	if sleepDuration <= 1*time.Millisecond || sleepDuration >= 2*time.Millisecond {
		t.Errorf("Sleep duration [%v] expected between 1ms and 2ms", sleepDuration)
	}

	expectedDuration = sleepDuration
	diskUtil += 5
	sleepDuration = getSleepDuration(diskUtil, thrctx)
	if sleepDuration <= expectedDuration || sleepDuration >= expectedDuration*2 {
		t.Errorf("Sleep duration [%v] expected between [%v] and [%v]",
			sleepDuration, expectedDuration, expectedDuration*2)
	}
}

func testConstantDiskUtilization(t *testing.T) {
	thrctx := newThrottleContext()
	diskUtil := ctx.config.Xaction.DiskUtilLowWM + 10
	sleepDuration := getSleepDuration(diskUtil, thrctx)
	expectedDuration := 1 * time.Millisecond
	if sleepDuration <= expectedDuration || sleepDuration >= expectedDuration*2 {
		t.Errorf("Sleep duration [%v] expected between [%v] and [%v]",
			sleepDuration, expectedDuration, expectedDuration*2)
	}

	expectedDuration = sleepDuration
	sleepDuration = getSleepDuration(diskUtil, thrctx)
	if sleepDuration <= expectedDuration || sleepDuration >= expectedDuration*2 {
		t.Errorf(fmstr, expectedDuration, sleepDuration)
	}
}

func testMultipleLRUContexts(t *testing.T) {
	thrctx := newThrottleContext()
	diskUtil := ctx.config.Xaction.DiskUtilLowWM + 10
	sleepDuration := getSleepDuration(diskUtil, thrctx)
	expectedDuration := 1 * time.Millisecond
	if sleepDuration <= expectedDuration || sleepDuration >= expectedDuration*2 {
		t.Errorf("Sleep duration [%v] expected between [%v] and [%v]",
			sleepDuration, expectedDuration, expectedDuration*2)
	}
	expectedDuration = sleepDuration

	thrctx2 := newThrottleContext()
	diskUtil = ctx.config.Xaction.DiskUtilLowWM - 10
	sleepDuration = getSleepDuration(diskUtil, thrctx2)
	if sleepDuration != 0 {
		t.Errorf(fmstr, 0, sleepDuration)
	}

	diskUtil = ctx.config.Xaction.DiskUtilLowWM + 11
	sleepDuration = getSleepDuration(diskUtil, thrctx)
	if sleepDuration <= expectedDuration || sleepDuration >= expectedDuration*2 {
		t.Errorf(fmstr, expectedDuration, sleepDuration)
	}
}

func testChangedFSUsedPercentageBeforeCapCheck(t *testing.T) {
	thrctx := newThrottleContext()
	oldLRUHWM := ctx.config.LRU.HighWM
	curCapacity, _ := ios.GetFSUsedPercentage("/")
	diskUtil := ctx.config.Xaction.DiskUtilHighWM + 10
	ctx.config.LRU.HighWM = int64(curCapacity - 2)
	*thrctx.CapUsedHigh = ctx.config.LRU.HighWM // NOTE: init time only
	sleepDuration := getSleepDuration(diskUtil, thrctx)
	if sleepDuration != 0 {
		t.Errorf(fmstr, 0, sleepDuration)
	}

	ctx.config.LRU.HighWM = int64(curCapacity + 10)
	*thrctx.CapUsedHigh = ctx.config.LRU.HighWM
	thrctx.recompute()
	sleepDuration = thrctx.sleep
	if sleepDuration != initThrottleSleep {
		t.Errorf(fmstr, initThrottleSleep, sleepDuration)
	}
	ctx.config.LRU.HighWM = oldLRUHWM
}

func testChangedDiskUtilBeforeUtilCheck(t *testing.T) {
	thrctx := newThrottleContext()
	oldStatsTime := ctx.config.Periodic.StatsTime
	ctx.config.Periodic.StatsTime = 10 * time.Minute
	*thrctx.Period = ctx.config.Periodic.StatsTime // NOTE: ditto
	sleepDuration := getSleepDuration(0, thrctx)
	if sleepDuration != 0 {
		t.Errorf(fmstr, 0, sleepDuration)
	}

	for disk := range riostat.Disk {
		riostat.Disk[disk]["%util"] = "99"
	}

	thrctx.recompute()
	sleepDuration = thrctx.sleep
	if sleepDuration != 0 {
		t.Errorf(fmstr, 0, sleepDuration)
	}
	ctx.config.Periodic.StatsTime = oldStatsTime
}

func getSleepDuration(diskUtil int64, thrctx *Throttle) time.Duration {
	for disk := range riostat.Disk {
		riostat.Disk[disk]["%util"] = strconv.Itoa(int(diskUtil))
	}

	thrctx.nextCapCheck = time.Time{}
	thrctx.nextUtilCheck = time.Time{}

	thrctx.recompute()
	return thrctx.sleep
}

func newThrottleContext() *Throttle {
	fileSystem, _ := fs.Fqn2fsAtStartup("/")
	return &Throttle{
		Riostat:      riostat,
		CapUsedHigh:  &ctx.config.LRU.HighWM,
		DiskUtilLow:  &ctx.config.Xaction.DiskUtilLowWM,
		DiskUtilHigh: &ctx.config.Xaction.DiskUtilHighWM,
		Period:       &ctx.config.Periodic.StatsTime,
		Path:         "/",
		FS:           fileSystem,
		Flag:         OnDiskUtil | OnFSUsed}
}
