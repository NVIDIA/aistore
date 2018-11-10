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

	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
)

const fmstr = "Expected [%v] actual [%v]"

type Test struct {
	name   string
	method func(*testing.T)
}

type fileInfos []fileInfo

var riostat *iostatrunner
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

func (fis fileInfos) Len() int {
	return len(fis)
}

func (fis fileInfos) Less(i, j int) bool {
	return fis[i].usetime.Before(fis[j].usetime)
}

func (fis fileInfos) Swap(i, j int) {
	fis[i], fis[j] = fis[j], fis[i]
}

func Test_HeapEqual(t *testing.T) {
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

		act := make(fileInfos, len(tc))
		for i := 0; i < len(tc); i++ {
			act[i] = *heap.Pop(h).(*fileInfo)
		}

		sort.Sort(tc)
		if !reflect.DeepEqual(act, tc) {
			t.Fatalf("Test case %d failed", tcNum+1)
		}
	}
}

func Test_Throttle(t *testing.T) {
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

	fs.Mountpaths = fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
	fs.Mountpaths.Add("/")
	available, _ := fs.Mountpaths.Get()
	fileSystem := available["/"].FileSystem

	disks := fs2disks(fileSystem)
	riostat = newIostatRunner(fs.Mountpaths)
	riostat.fsdisks = make(map[string]cmn.StringSet, len(available))
	riostat.fsdisks[fileSystem] = disks
	for disk := range disks {
		riostat.Disk[disk] = make(cmn.SimpleKVs, 0)
		riostat.Disk[disk]["%util"] = strconv.Itoa(0)

	}
	ctx.rg = &rungroup{
		runarr: make([]cmn.Runner, 0, 4),
		runmap: make(map[string]cmn.Runner),
	}
	ctx.rg.add(riostat, xiostat)

	testHighFSCapacityUsed(t, ctx.config.Xaction.DiskUtilLowWM-10)
	testHighFSCapacityUsed(t, ctx.config.Xaction.DiskUtilLowWM+10)
	testHighFSCapacityUsed(t, ctx.config.Xaction.DiskUtilHighWM+10)

	for _, test := range aTests {
		t.Run(test.name, test.method)
	}

	ctx.config.LRU.LowWM = oldLRULWM
	ctx.config.LRU.HighWM = oldLRUHWM
	ctx.config.Xaction.DiskUtilLowWM = oldDiskLWM
	ctx.config.Xaction.DiskUtilHighWM = oldDiskHWM
	ctx.config.Periodic.StatsTime = oldStatsTime
	ctx.rg = oldRg
}

func testHighFSCapacityUsed(t *testing.T, diskUtil uint32) {
	curCapacity, _ := getFSUsedPercentage("/")
	oldLRUHWM := ctx.config.LRU.HighWM
	ctx.config.LRU.HighWM = uint32(curCapacity - 1)
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
	curCapacity, _ := getFSUsedPercentage("/")
	diskUtil := ctx.config.Xaction.DiskUtilHighWM + 10
	ctx.config.LRU.HighWM = uint32(curCapacity - 2)
	thrctx.capUsedHigh = int64(ctx.config.LRU.HighWM) // NOTE: init time only
	sleepDuration := getSleepDuration(diskUtil, thrctx)
	if sleepDuration != 0 {
		t.Errorf(fmstr, 0, sleepDuration)
	}

	ctx.config.LRU.HighWM = uint32(curCapacity + 10)
	thrctx.capUsedHigh = int64(ctx.config.LRU.HighWM)
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
	thrctx.period = ctx.config.Periodic.StatsTime // NOTE: ditto
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

func getSleepDuration(diskUtil uint32, thrctx *throttleContext) time.Duration {
	for disk := range riostat.Disk {
		riostat.Disk[disk]["%util"] = strconv.Itoa(int(diskUtil))
	}

	thrctx.nextCapCheck = time.Time{}
	thrctx.nextUtilCheck = time.Time{}

	thrctx.recompute()
	return thrctx.sleep
}

func newThrottleContext() *throttleContext {
	fileSystem, _ := fs.Fqn2fsAtStartup("/")
	return &throttleContext{
		capUsedHigh:  int64(ctx.config.LRU.HighWM),
		diskUtilLow:  int64(ctx.config.Xaction.DiskUtilLowWM),
		diskUtilHigh: int64(ctx.config.Xaction.DiskUtilHighWM),
		period:       ctx.config.Periodic.StatsTime,
		path:         "/",
		fs:           fileSystem,
		flag:         onDiskUtil | onFSUsed}
}
