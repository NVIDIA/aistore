/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dfc

import (
	"container/heap"
	"reflect"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/fs"
)

type fileInfos []fileInfo

func (fis fileInfos) Len() int {
	return len(fis)
}

func (fis fileInfos) Less(i, j int) bool {
	return fis[i].usetime.Before(fis[j].usetime)
}

func (fis fileInfos) Swap(i, j int) {
	fis[i], fis[j] = fis[j], fis[i]
}

func TestLRUBasic(t *testing.T) {
	tcs := []fileInfos{
		{
			{
				"o1",
				time.Date(2018, time.June, 26, 1, 2, 3, 0, time.UTC),
				1024,
			},
			{
				"o2",
				time.Date(2018, time.June, 26, 1, 3, 3, 0, time.UTC),
				1025,
			},
		},
		{
			{
				"o3",
				time.Date(2018, time.June, 26, 1, 5, 3, 0, time.UTC),
				1024,
			},
			{
				"o4",
				time.Date(2018, time.June, 26, 1, 4, 3, 0, time.UTC),
				1025,
			},
		},
		{
			{
				"o5",
				time.Date(2018, time.June, 26, 1, 5, 3, 0, time.UTC),
				1024,
			},
		},
		{
			{
				"o6",
				time.Date(2018, time.June, 26, 1, 5, 3, 0, time.UTC),
				10240,
			},
			{
				"o7",
				time.Date(2018, time.June, 28, 1, 4, 3, 0, time.UTC),
				102500,
			},
			{
				"o8",
				time.Date(2018, time.June, 30, 1, 5, 3, 0, time.UTC),
				1024,
			},
			{
				"o9",
				time.Date(2018, time.June, 20, 1, 4, 3, 0, time.UTC),
				10250,
			},
		},
	}

	h := &fileInfoMinHeap{}
	heap.Init(h)

	for tcNum, tc := range tcs {
		for i := range tc {
			heap.Push(h, &tc[i])
		}

		var act fileInfos
		for i := 0; i < len(tc); i++ {
			fi := heap.Pop(h).(*fileInfo)
			act = append(act, *fi)
		}

		sort.Sort(tc)
		if !reflect.DeepEqual(act, tc) {
			t.Fatalf("Test case %d failed", tcNum+1)
		}
	}
}

func TestLRUThrottling(t *testing.T) {
	oldLRULWM := ctx.config.LRU.LowWM
	oldLRUHWM := ctx.config.LRU.HighWM
	oldDiskLWM := ctx.config.Xaction.DiskUtilLowWM
	oldDiskHWM := ctx.config.Xaction.DiskUtilHighWM
	oldStatsTime := ctx.config.Periodic.StatsTime
	oldRg := ctx.rg

	ctx.config.LRU.LowWM = 0
	ctx.config.LRU.HighWM = 90
	ctx.config.Xaction.DiskUtilLowWM = 10
	ctx.config.Xaction.DiskUtilHighWM = 40
	ctx.config.Periodic.StatsTime = -1 * time.Second

	ctx.mountpaths.AddMountpath("/")
	fileSystem := ctx.mountpaths.Available["/"].FileSystem

	disks := fs2disks(fileSystem)
	riostat := newIostatRunner()
	riostat.fsdisks = make(map[string]StringSet, len(ctx.mountpaths.Available))
	riostat.fsdisks[fileSystem] = disks
	for disk := range disks {
		riostat.Disk[disk] = make(simplekvs, 0)
		riostat.Disk[disk]["%util"] = strconv.Itoa(0)

	}
	ctx.rg = &rungroup{
		runarr: make([]runner, 0, 4),
		runmap: make(map[string]runner),
	}
	ctx.rg.add(riostat, xiostat)

	testHighFSCapacityUsed(t, ctx.config.Xaction.DiskUtilLowWM-10, riostat)
	testHighFSCapacityUsed(t, ctx.config.Xaction.DiskUtilLowWM+10, riostat)
	testHighFSCapacityUsed(t, ctx.config.Xaction.DiskUtilHighWM+10, riostat)
	testHighDiskUtilization(t, riostat)
	testLowDiskUtilization(t, riostat)
	testHighDiskUtilizationShortDuration(t, riostat)
	testHighDiskUtilizationLongDuration(t, riostat)
	testIncreasingDiskUtilization(t, riostat)
	testDecreasingDiskUtilization(t, riostat)
	testMediumDiskUtilization(t, riostat)
	testConstantDiskUtilization(t, riostat)
	testMultipleLRUContexts(t, riostat)
	testChangedFSUsedPercentageBeforeCapCheck(t, riostat)
	testChangedDiskUtilBeforeUtilCheck(t, riostat)

	ctx.config.LRU.LowWM = oldLRULWM
	ctx.config.LRU.HighWM = oldLRUHWM
	ctx.config.Xaction.DiskUtilLowWM = oldDiskLWM
	ctx.config.Xaction.DiskUtilHighWM = oldDiskHWM
	ctx.config.Periodic.StatsTime = oldStatsTime
	ctx.rg = oldRg
}

func testHighFSCapacityUsed(t *testing.T, diskUtil uint32, riostat *iostatrunner) {
	curCapacity, _ := getFSUsedPercentage("/")
	oldLRUHWM := ctx.config.LRU.HighWM
	ctx.config.LRU.HighWM = uint32(curCapacity - 1)
	lctx := newLruContext()
	sleepDuration := getSleepDuration(diskUtil, lctx, riostat)
	if sleepDuration != 0 {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
			0, sleepDuration)
	}
	ctx.config.LRU.HighWM = oldLRUHWM
}

func testHighDiskUtilization(t *testing.T, riostat *iostatrunner) {
	diskUtil := ctx.config.Xaction.DiskUtilHighWM + 10
	lctx := newLruContext()
	sleepDuration := getSleepDuration(diskUtil, lctx, riostat)
	expectedDuration := 1 * time.Millisecond
	if sleepDuration != expectedDuration {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
			expectedDuration, sleepDuration)
	}
}

func testLowDiskUtilization(t *testing.T, riostat *iostatrunner) {
	diskUtil := ctx.config.Xaction.DiskUtilLowWM - 10
	lctx := newLruContext()
	sleepDuration := getSleepDuration(diskUtil, lctx, riostat)
	if sleepDuration != 0 {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
			0, sleepDuration)
	}
}

func testHighDiskUtilizationShortDuration(t *testing.T, riostat *iostatrunner) {
	diskUtil := ctx.config.Xaction.DiskUtilHighWM + 5
	lctx := newLruContext()
	sleepDuration := getSleepDuration(diskUtil, lctx, riostat)
	if sleepDuration != initThrottleSleep {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
			initThrottleSleep, sleepDuration)
	}

	counter := 3
	for i := 0; i < counter; i++ {
		diskUtil += 5
		sleepDuration := getSleepDuration(diskUtil, lctx, riostat)
		expectedDuration := time.Duration(1<<uint(i+1)) * time.Millisecond
		if sleepDuration != expectedDuration {
			t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
				expectedDuration, sleepDuration)
		}
	}
}

func testHighDiskUtilizationLongDuration(t *testing.T, riostat *iostatrunner) {
	lctx := newLruContext()
	diskUtil := ctx.config.Xaction.DiskUtilHighWM + 5
	sleepDuration := getSleepDuration(diskUtil, lctx, riostat)
	if sleepDuration != initThrottleSleep {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
			initThrottleSleep, sleepDuration)
	}

	counter := 9
	for i := 0; i < counter; i++ {
		diskUtil += 5
		sleepDuration := getSleepDuration(diskUtil, lctx, riostat)
		expectedDuration := time.Duration(1<<uint(i+1)) * time.Millisecond
		if sleepDuration != expectedDuration {
			t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
				expectedDuration, sleepDuration)
		}
	}

	diskUtil += 5
	sleepDuration = getSleepDuration(diskUtil, lctx, riostat)
	expectedDuration := 1 * time.Second
	if sleepDuration != expectedDuration {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
			expectedDuration, sleepDuration)
	}
}

func testIncreasingDiskUtilization(t *testing.T, riostat *iostatrunner) {
	lctx := newLruContext()
	diskUtil := ctx.config.Xaction.DiskUtilLowWM - 10
	sleepDuration := getSleepDuration(diskUtil, lctx, riostat)
	if sleepDuration != 0 {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
			0, sleepDuration)
	}

	diskUtil = ctx.config.Xaction.DiskUtilLowWM + 10
	sleepDuration = getSleepDuration(diskUtil, lctx, riostat)
	if sleepDuration <= 1*time.Millisecond || sleepDuration >= 2*time.Millisecond {
		t.Errorf("Sleep duration [%v] expected between 1ms and 2ms", sleepDuration)
	}

	expectedDuration := sleepDuration * 2
	diskUtil = ctx.config.Xaction.DiskUtilHighWM + 10
	sleepDuration = getSleepDuration(diskUtil, lctx, riostat)
	if sleepDuration != expectedDuration {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
			expectedDuration, sleepDuration)
	}
}

func testDecreasingDiskUtilization(t *testing.T, riostat *iostatrunner) {
	lctx := newLruContext()
	diskUtil := ctx.config.Xaction.DiskUtilHighWM + 10
	sleepDuration := getSleepDuration(diskUtil, lctx, riostat)
	expectedDuration := 1 * time.Millisecond
	if sleepDuration != expectedDuration {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
			expectedDuration, sleepDuration)
	}

	diskUtil = ctx.config.Xaction.DiskUtilLowWM + 10
	sleepDuration = getSleepDuration(diskUtil, lctx, riostat)
	if sleepDuration <= 1*time.Millisecond || sleepDuration >= 2*time.Millisecond {
		t.Errorf("Sleep duration [%v] expected between 1ms and 2ms", sleepDuration)
	}

	diskUtil = ctx.config.Xaction.DiskUtilLowWM - 10
	sleepDuration = getSleepDuration(diskUtil, lctx, riostat)
	if sleepDuration != 0 {
		t.Errorf("Throttling is not required when disk utilization [%d] is lower than LowWM [%d].",
			diskUtil, ctx.config.Xaction.DiskUtilLowWM)
	}
}

func testMediumDiskUtilization(t *testing.T, riostat *iostatrunner) {
	lctx := newLruContext()
	diskUtil := ctx.config.Xaction.DiskUtilLowWM + 5
	sleepDuration := getSleepDuration(diskUtil, lctx, riostat)
	expectedDuration := 1 * time.Millisecond
	if sleepDuration <= expectedDuration || sleepDuration >= expectedDuration*2 {
		t.Errorf("Sleep duration [%v] expected between [%v] and [%v]",
			sleepDuration, expectedDuration, expectedDuration*2)
	}

	diskUtil += 5
	sleepDuration = getSleepDuration(diskUtil, lctx, riostat)
	if sleepDuration <= 1*time.Millisecond || sleepDuration >= 2*time.Millisecond {
		t.Errorf("Sleep duration [%v] expected between 1ms and 2ms", sleepDuration)
	}

	expectedDuration = sleepDuration
	diskUtil += 5
	sleepDuration = getSleepDuration(diskUtil, lctx, riostat)
	if sleepDuration <= expectedDuration || sleepDuration >= expectedDuration*2 {
		t.Errorf("Sleep duration [%v] expected between [%v] and [%v]",
			sleepDuration, expectedDuration, expectedDuration*2)
	}
}

func testConstantDiskUtilization(t *testing.T, riostat *iostatrunner) {
	lctx := newLruContext()

	diskUtil := ctx.config.Xaction.DiskUtilLowWM + 10
	sleepDuration := getSleepDuration(diskUtil, lctx, riostat)
	expectedDuration := 1 * time.Millisecond
	if sleepDuration <= expectedDuration || sleepDuration >= expectedDuration*2 {
		t.Errorf("Sleep duration [%v] expected between [%v] and [%v]",
			sleepDuration, expectedDuration, expectedDuration*2)
	}

	expectedDuration = sleepDuration
	sleepDuration = getSleepDuration(diskUtil, lctx, riostat)
	if sleepDuration <= expectedDuration || sleepDuration >= expectedDuration*2 {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
			expectedDuration, sleepDuration)
	}
}

func testMultipleLRUContexts(t *testing.T, riostat *iostatrunner) {
	lctx := newLruContext()
	diskUtil := ctx.config.Xaction.DiskUtilLowWM + 10
	sleepDuration := getSleepDuration(diskUtil, lctx, riostat)
	expectedDuration := 1 * time.Millisecond
	if sleepDuration <= expectedDuration || sleepDuration >= expectedDuration*2 {
		t.Errorf("Sleep duration [%v] expected between [%v] and [%v]",
			sleepDuration, expectedDuration, expectedDuration*2)
	}
	expectedDuration = sleepDuration

	lctx2 := newLruContext()
	diskUtil = ctx.config.Xaction.DiskUtilLowWM - 10
	sleepDuration = getSleepDuration(diskUtil, lctx2, riostat)
	if sleepDuration != 0 {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]", 0, sleepDuration)
	}

	diskUtil = ctx.config.Xaction.DiskUtilLowWM + 11
	sleepDuration = getSleepDuration(diskUtil, lctx, riostat)
	if sleepDuration <= expectedDuration || sleepDuration >= expectedDuration*2 {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
			expectedDuration, sleepDuration)
	}
}

func testChangedFSUsedPercentageBeforeCapCheck(t *testing.T, riostat *iostatrunner) {
	lctx := newLruContext()
	oldLRUHWM := ctx.config.LRU.HighWM
	curCapacity, _ := getFSUsedPercentage("/")
	diskUtil := ctx.config.Xaction.DiskUtilHighWM + 10
	ctx.config.LRU.HighWM = uint32(curCapacity - 2)
	sleepDuration := getSleepDuration(diskUtil, lctx, riostat)
	if sleepDuration != 0 {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]", 0, sleepDuration)
	}

	ctx.config.LRU.HighWM = uint32(curCapacity + 10)
	lctx.computeThrottle("/")
	sleepDuration = lctx.throttle.sleep
	if sleepDuration != initThrottleSleep {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]", initThrottleSleep, sleepDuration)
	}
	ctx.config.LRU.HighWM = oldLRUHWM
}

func testChangedDiskUtilBeforeUtilCheck(t *testing.T, riostat *iostatrunner) {
	lctx := newLruContext()
	oldStatsTime := ctx.config.Periodic.StatsTime
	ctx.config.Periodic.StatsTime = 10 * time.Minute
	sleepDuration := getSleepDuration(0, lctx, riostat)
	if sleepDuration != 0 {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]", 0, sleepDuration)
	}

	for disk := range riostat.Disk {
		riostat.Disk[disk]["%util"] = "99"
	}

	lctx.computeThrottle("/")
	sleepDuration = lctx.throttle.sleep
	if sleepDuration != 0 {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]", 0, sleepDuration)
	}
	ctx.config.Periodic.StatsTime = oldStatsTime
}

func getSleepDuration(diskUtil uint32, lctx *lructx, riostat *iostatrunner) time.Duration {
	for disk := range riostat.Disk {
		riostat.Disk[disk]["%util"] = strconv.Itoa(int(diskUtil))
	}

	lctx.throttle.nextCapCheck = time.Time{}
	lctx.throttle.nextUtilCheck = time.Time{}

	lctx.computeThrottle("/")
	return lctx.throttle.sleep
}

func newLruContext() *lructx {
	fileSystem, _ := fs.Fqn2fsAtStartup("/")
	lruContext := &lructx{
		xlru: new(xactLRU),
		throttle: struct {
			sleep         time.Duration
			nextUtilCheck time.Time
			nextCapCheck  time.Time
			prevUtilPct   float32
			prevFSUsedPct uint64
		}{sleep: 0},
		fs: fileSystem,
	}
	return lruContext
}
