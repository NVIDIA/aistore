// Package cluster provides common interfaces and local access to cluster-level metadata
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
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

var riostat *ios.IostatRunner

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

func beforeEach() {
	config := cmn.GCO.BeginUpdate()
	config.LRU.LowWM = 75
	config.LRU.HighWM = 90
	config.Xaction.DiskUtilLowWM = 60
	config.Xaction.DiskUtilHighWM = 80
	config.Periodic.StatsTime = time.Second
	cmn.GCO.CommitUpdate(config)
}

func Test_Throttle(t *testing.T) {
	config := cmn.GCO.BeginUpdate()
	config.LRU.LowWM = 0
	config.LRU.HighWM = 90
	config.Xaction.DiskUtilLowWM = 10
	config.Xaction.DiskUtilHighWM = 40
	config.Periodic.StatsTime = time.Second
	cmn.GCO.CommitUpdate(config)

	fs.Mountpaths = fs.NewMountedFS()
	fs.Mountpaths.Add("/")

	riostat = ios.NewIostatRunner(fs.Mountpaths)

	go riostat.Run()
	time.Sleep(time.Second)
	defer func() { riostat.Stop(nil) }()

	riostat.Lock()
	for disk := range riostat.Disk {
		riostat.Disk[disk] = make(cmn.SimpleKVs, 1)
		riostat.Disk[disk]["%util"] = strconv.Itoa(0)
	}
	riostat.Unlock()

	testHighFSCapacityUsed(t, config.Xaction.DiskUtilLowWM-10)
	testHighFSCapacityUsed(t, config.Xaction.DiskUtilLowWM+10)
	testHighFSCapacityUsed(t, config.Xaction.DiskUtilHighWM+10)

	for _, test := range aTests {
		beforeEach()
		t.Run(test.name, test.method)
	}
}

func testHighFSCapacityUsed(t *testing.T, diskUtil int64) {
	config := cmn.GCO.BeginUpdate()
	curCapacity, _ := ios.GetFSUsedPercentage("/")
	config.LRU.HighWM = int64(curCapacity - 1)
	cmn.GCO.CommitUpdate(config)

	thrctx := newThrottleContext()
	sleepDuration := getSleepDuration(diskUtil, thrctx)
	if sleepDuration != 0 {
		t.Errorf(fmstr, 0, sleepDuration)
	}
}

func testHighDiskUtilization(t *testing.T) {
	config := cmn.GCO.Get()
	diskUtil := config.Xaction.DiskUtilHighWM + 10
	thrctx := newThrottleContext()
	sleepDuration := getSleepDuration(diskUtil, thrctx)
	expectedDuration := 1 * time.Millisecond
	if sleepDuration != expectedDuration {
		t.Errorf(fmstr, expectedDuration, sleepDuration)
	}
}

func testLowDiskUtilization(t *testing.T) {
	config := cmn.GCO.Get()
	diskUtil := config.Xaction.DiskUtilLowWM - 10
	thrctx := newThrottleContext()
	sleepDuration := getSleepDuration(diskUtil, thrctx)
	if sleepDuration != 0 {
		t.Errorf(fmstr, 0, sleepDuration)
	}
}

func testHighDiskUtilizationShortDuration(t *testing.T) {
	config := cmn.GCO.Get()
	diskUtil := config.Xaction.DiskUtilHighWM + 5
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
	config := cmn.GCO.Get()
	thrctx := newThrottleContext()
	diskUtil := config.Xaction.DiskUtilHighWM + 5
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
	config := cmn.GCO.Get()
	thrctx := newThrottleContext()
	diskUtil := config.Xaction.DiskUtilLowWM - 10
	sleepDuration := getSleepDuration(diskUtil, thrctx)
	if sleepDuration != 0 {
		t.Errorf(fmstr, 0, sleepDuration)
	}

	diskUtil = config.Xaction.DiskUtilLowWM + 10
	sleepDuration = getSleepDuration(diskUtil, thrctx)
	if sleepDuration <= 1*time.Millisecond || sleepDuration >= 2*time.Millisecond {
		t.Errorf("Sleep duration [%v] expected between 1ms and 2ms", sleepDuration)
	}

	expectedDuration := sleepDuration * 2
	diskUtil = config.Xaction.DiskUtilHighWM + 10
	sleepDuration = getSleepDuration(diskUtil, thrctx)
	if sleepDuration != expectedDuration {
		t.Errorf(fmstr, expectedDuration, sleepDuration)
	}
}

func testDecreasingDiskUtilization(t *testing.T) {
	config := cmn.GCO.Get()
	thrctx := newThrottleContext()
	diskUtil := config.Xaction.DiskUtilHighWM + 10
	sleepDuration := getSleepDuration(diskUtil, thrctx)
	expectedDuration := 1 * time.Millisecond
	if sleepDuration != expectedDuration {
		t.Errorf(fmstr, expectedDuration, sleepDuration)
	}

	diskUtil = config.Xaction.DiskUtilLowWM + 10
	sleepDuration = getSleepDuration(diskUtil, thrctx)
	if sleepDuration <= 1*time.Millisecond || sleepDuration >= 2*time.Millisecond {
		t.Errorf("Sleep duration [%v] expected between 1ms and 2ms", sleepDuration)
	}

	diskUtil = config.Xaction.DiskUtilLowWM - 10
	sleepDuration = getSleepDuration(diskUtil, thrctx)
	if sleepDuration != 0 {
		t.Errorf("Throttling is not required when disk utilization [%d] is lower than LowWM [%d].",
			diskUtil, config.Xaction.DiskUtilLowWM)
	}
}

func testMediumDiskUtilization(t *testing.T) {
	config := cmn.GCO.Get()
	thrctx := newThrottleContext()
	diskUtil := config.Xaction.DiskUtilLowWM + 5
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
	config := cmn.GCO.Get()
	thrctx := newThrottleContext()
	diskUtil := config.Xaction.DiskUtilLowWM + 10
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
	config := cmn.GCO.Get()
	thrctx := newThrottleContext()
	diskUtil := config.Xaction.DiskUtilLowWM + 10
	sleepDuration := getSleepDuration(diskUtil, thrctx)
	expectedDuration := 1 * time.Millisecond
	if sleepDuration <= expectedDuration || sleepDuration >= expectedDuration*2 {
		t.Errorf("Sleep duration [%v] expected between [%v] and [%v]",
			sleepDuration, expectedDuration, expectedDuration*2)
	}
	expectedDuration = sleepDuration

	thrctx2 := newThrottleContext()
	diskUtil = config.Xaction.DiskUtilLowWM - 10
	sleepDuration = getSleepDuration(diskUtil, thrctx2)
	if sleepDuration != 0 {
		t.Errorf(fmstr, 0, sleepDuration)
	}

	diskUtil = config.Xaction.DiskUtilLowWM + 11
	sleepDuration = getSleepDuration(diskUtil, thrctx)
	if sleepDuration <= expectedDuration || sleepDuration >= expectedDuration*2 {
		t.Errorf(fmstr, expectedDuration, sleepDuration)
	}
}

func testChangedFSUsedPercentageBeforeCapCheck(t *testing.T) {
	config := cmn.GCO.BeginUpdate()
	thrctx := newThrottleContext()
	curCapacity, _ := ios.GetFSUsedPercentage("/")
	config.LRU.HighWM = int64(curCapacity - 2)
	cmn.GCO.CommitUpdate(config)

	diskUtil := config.Xaction.DiskUtilHighWM + 10
	sleepDuration := getSleepDuration(diskUtil, thrctx)
	if sleepDuration != 0 {
		t.Errorf(fmstr, 0, sleepDuration)
	}

	config = cmn.GCO.BeginUpdate()
	config.LRU.HighWM = int64(curCapacity + 10)
	cmn.GCO.CommitUpdate(config)

	thrctx.recompute()
	sleepDuration = thrctx.sleep
	if sleepDuration != initThrottleSleep {
		t.Errorf(fmstr, initThrottleSleep, sleepDuration)
	}
}

func testChangedDiskUtilBeforeUtilCheck(t *testing.T) {
	thrctx := newThrottleContext()
	config := cmn.GCO.BeginUpdate()
	config.Periodic.StatsTime = 10 * time.Minute
	cmn.GCO.CommitUpdate(config)

	sleepDuration := getSleepDuration(0, thrctx)
	if sleepDuration != 0 {
		t.Errorf(fmstr, 0, sleepDuration)
	}
	thrctx.Riostat.Lock()
	for disk := range thrctx.Riostat.Disk {
		thrctx.Riostat.Disk[disk]["%util"] = "99"
	}
	thrctx.Riostat.Unlock()
	thrctx.recompute()
	sleepDuration = thrctx.sleep
	if sleepDuration != 0 {
		t.Errorf(fmstr, 0, sleepDuration)
	}
}

func getSleepDuration(diskUtil int64, thrctx *Throttle) time.Duration {
	thrctx.Riostat.Lock()
	for disk := range thrctx.Riostat.Disk {
		thrctx.Riostat.Disk[disk]["%util"] = strconv.Itoa(int(diskUtil))
	}
	thrctx.Riostat.Unlock()
	thrctx.nextCapCheck = time.Time{}
	thrctx.nextUtilCheck = time.Time{}

	thrctx.recompute()
	return thrctx.sleep
}

func newThrottleContext() *Throttle {
	fileSystem, _ := fs.Fqn2fsAtStartup("/")
	return &Throttle{
		Riostat: riostat,
		Path:    "/",
		FS:      fileSystem,
		Flag:    OnDiskUtil | OnFSUsed}
}
