/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package dfc

import (
	"container/heap"
	"reflect"
	"sort"
	"testing"
	"time"
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

type MockThrottleChekcer struct {
	UtilizationToReturn uint32
	FSUsedPercentage    uint64
}

func (m *MockThrottleChekcer) getDiskUtilizationFromPath(mountPath string) (float32, bool) {
	return float32(m.UtilizationToReturn), true
}

func (m *MockThrottleChekcer) getFSUsedPercentage(string) (uint64, bool) {
	return m.FSUsedPercentage, true
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

const FILESYSTEM = "/tmp"

func TestLRUThrottling(t *testing.T) {
	oldLRULWM := ctx.config.LRU.LowWM
	oldLRUHWM := ctx.config.LRU.HighWM
	oldDiskLWM := ctx.config.Xaction.DiskUtilLowWM
	oldDiskHWM := ctx.config.Xaction.DiskUtilHighWM
	oldAvailableMP := ctx.mountpaths.Available
	oldStatsTime := ctx.config.Periodic.StatsTime

	ctx.config.LRU.LowWM = 75
	ctx.config.LRU.HighWM = 90
	ctx.config.Xaction.DiskUtilLowWM = 10
	ctx.config.Xaction.DiskUtilHighWM = 40
	ctx.mountpaths.Available = make(map[string]*mountPath)
	ctx.config.Periodic.StatsTime = -1 * time.Second

	mp := &mountPath{Path: FILESYSTEM}
	ctx.mountpaths.Available[mp.Path] = mp

	testHighFSCapacityUsed(t, ctx.config.Xaction.DiskUtilLowWM-10)
	testHighFSCapacityUsed(t, ctx.config.Xaction.DiskUtilLowWM+10)
	testHighFSCapacityUsed(t, ctx.config.Xaction.DiskUtilHighWM+10)
	testHighDiskUtilization(t)
	testLowDiskUtilization(t)
	testHighDiskUtilizationShortDuration(t)
	testHighDiskUtilizationLongDuration(t)
	testIncreasingDiskUtilization(t)
	testDecreasingDiskUtilization(t)
	testMediumDiskUtilization(t)
	testConstantDiskUtilization(t)
	testMultipleLRUContexts(t)
	testSlightlyIncreasedDiskUtilization(t)
	testChangedDiskUtilizationBeforeStatsTime(t)
	testChangedFSUsedPercentageBeforeDuration(t)

	ctx.config.LRU.LowWM = oldLRULWM
	ctx.config.LRU.HighWM = oldLRUHWM
	ctx.config.Xaction.DiskUtilLowWM = oldDiskLWM
	ctx.config.Xaction.DiskUtilHighWM = oldDiskHWM
	ctx.mountpaths.Available = oldAvailableMP
	ctx.config.Periodic.StatsTime = oldStatsTime
}

func testHighFSCapacityUsed(t *testing.T, diskUtilization uint32) {
	lruContext := newLruContext()
	mockThrottleChecker := &MockThrottleChekcer{
		UtilizationToReturn: diskUtilization,
		FSUsedPercentage:    uint64(ctx.config.LRU.HighWM + 1),
	}
	_, isThrottleRequired := lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if isThrottleRequired {
		t.Errorf("No throttling required when FS used capacity [%d] is higher than High WM [%d].",
			mockThrottleChecker.FSUsedPercentage, ctx.config.LRU.HighWM)
	}
}

func testHighDiskUtilization(t *testing.T) {
	lruContext := newLruContext()
	mockThrottleChecker := &MockThrottleChekcer{
		UtilizationToReturn: ctx.config.Xaction.DiskUtilHighWM + 10,
	}
	sleepDuration, isThrottleRequired := lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if !isThrottleRequired {
		t.Errorf("Throttling is required when disk utilization [%d] is higher than HighWM [%d].",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilHighWM)
	}

	expectedDuration := 1 * time.Millisecond
	if sleepDuration != expectedDuration {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
			expectedDuration, sleepDuration)
	}
}

func testLowDiskUtilization(t *testing.T) {
	lruContext := newLruContext()
	mockThrottleChecker := &MockThrottleChekcer{
		UtilizationToReturn: ctx.config.Xaction.DiskUtilLowWM - 10,
	}
	_, isThrottleRequired := lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if isThrottleRequired {
		t.Errorf("Throttling is not required when disk utilization [%d] is lower than LowWM [%d].",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilLowWM)
	}
}

func testHighDiskUtilizationShortDuration(t *testing.T) {
	lruContext := newLruContext()
	mockThrottleChecker := &MockThrottleChekcer{
		UtilizationToReturn: ctx.config.Xaction.DiskUtilHighWM + 5,
	}
	sleepDuration, isThrottleRequired := lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if !isThrottleRequired {
		t.Errorf("Throttling is required when disk utilization [%d] is higher than HighWM [%d].",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilHighWM)
	}

	if sleepDuration != InitialThrottleSleepDurationInMS*time.Millisecond {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
			InitialThrottleSleepDurationInMS, sleepDuration)
	}

	counter := 3
	for i := 0; i < counter; i++ {
		mockThrottleChecker.UtilizationToReturn += 5
		sleepDuration, isThrottleRequired := lruContext.getSleepDurationForThrottle(
			"/tmp/test", -1*time.Second, mockThrottleChecker)
		if !isThrottleRequired {
			t.Errorf("Throttling is required when disk utilization [%d] is higher than HighWM [%d].",
				mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilHighWM)
		}

		expectedDuration := time.Duration(1<<uint(i+1)) * time.Millisecond
		if sleepDuration != expectedDuration {
			t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
				expectedDuration, sleepDuration)
		}
	}
}

func testHighDiskUtilizationLongDuration(t *testing.T) {
	lruContext := newLruContext()
	mockThrottleChecker := &MockThrottleChekcer{
		UtilizationToReturn: ctx.config.Xaction.DiskUtilHighWM + 3,
	}
	sleepDuration, isThrottleRequired := lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if !isThrottleRequired {
		t.Errorf("Throttling is required when disk utilization [%d] is higher than HighWM [%d].",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilHighWM)
	}

	if sleepDuration != InitialThrottleSleepDurationInMS*time.Millisecond {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
			InitialThrottleSleepDurationInMS, sleepDuration)
	}

	counter := 9
	for i := 0; i < counter; i++ {
		mockThrottleChecker.UtilizationToReturn += 5
		sleepDuration, isThrottleRequired := lruContext.getSleepDurationForThrottle(
			"/tmp/test", -1*time.Second, mockThrottleChecker)
		if !isThrottleRequired {
			t.Errorf("Throttling is required when disk utilization [%d] is higher than HighWM [%d].",
				mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilHighWM)
		}

		expectedDuration := time.Duration(1<<uint(i+1)) * time.Millisecond
		if sleepDuration != expectedDuration {
			t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
				expectedDuration, sleepDuration)
		}
	}

	mockThrottleChecker.UtilizationToReturn += 5
	sleepDuration, isThrottleRequired = lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if !isThrottleRequired {
		t.Errorf("Throttling is required when disk utilization [%d] is higher than HighWM [%d].",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilHighWM)
	}

	expectedDuration := 1 * time.Second
	if sleepDuration != expectedDuration {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
			expectedDuration, sleepDuration)
	}
}

func testIncreasingDiskUtilization(t *testing.T) {
	lruContext := newLruContext()
	mockThrottleChecker := &MockThrottleChekcer{
		UtilizationToReturn: ctx.config.Xaction.DiskUtilLowWM - 10,
	}
	_, isThrottleRequired := lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if isThrottleRequired {
		t.Errorf("Throttling is not required when disk utilization [%d] is lower than LowWM [%d].",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilLowWM)
	}

	mockThrottleChecker.UtilizationToReturn = ctx.config.Xaction.DiskUtilLowWM + 10
	sleepDuration, isThrottleRequired := lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if !isThrottleRequired {
		t.Errorf("Throttling is required when disk utilization [%d] is above LowWM [%d] and below HighWM [%d].",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilLowWM, ctx.config.Xaction.DiskUtilHighWM)
	}

	if sleepDuration <= 1*time.Millisecond || sleepDuration >= 2*time.Millisecond {
		t.Errorf("Sleep duration [%v] expected between 1ms and 2ms",
			sleepDuration)
	}

	expectedDuration := sleepDuration * 2
	mockThrottleChecker.UtilizationToReturn = ctx.config.Xaction.DiskUtilHighWM + 10
	sleepDuration, isThrottleRequired = lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if !isThrottleRequired {
		t.Errorf("Throttling is required when disk utilization [%d] is higher than HighWM [%d].",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilHighWM)
	}

	// For handling precision error
	sleepDurationMSInt := int(sleepDuration.Nanoseconds()) / 1000000
	expectedSleepDurationMSInt := int(expectedDuration.Nanoseconds()) / 1000000
	if sleepDurationMSInt != expectedSleepDurationMSInt {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
			expectedSleepDurationMSInt, sleepDurationMSInt)
	}
}

func testDecreasingDiskUtilization(t *testing.T) {
	lruContext := newLruContext()
	mockThrottleChecker := &MockThrottleChekcer{
		UtilizationToReturn: ctx.config.Xaction.DiskUtilHighWM + 10,
	}

	expectedDuration := 1 * time.Millisecond
	sleepDuration, isThrottleRequired := lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if !isThrottleRequired {
		t.Errorf("Throttling is required when disk utilization [%d] is higher than HighWM [%d].",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilHighWM)
	}

	if sleepDuration != expectedDuration {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
			expectedDuration, sleepDuration)
	}

	mockThrottleChecker.UtilizationToReturn = ctx.config.Xaction.DiskUtilLowWM + 10
	sleepDuration, isThrottleRequired = lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if !isThrottleRequired {
		t.Errorf("Throttling is required when disk utilization [%d] is above LowWM [%d] and below HighWM [%d].",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilLowWM, ctx.config.Xaction.DiskUtilHighWM)
	}

	if sleepDuration <= 1*time.Millisecond || sleepDuration >= 2*time.Millisecond {
		t.Errorf("Sleep duration [%v] expected between 1ms and 2ms",
			sleepDuration)
	}

	mockThrottleChecker.UtilizationToReturn = ctx.config.Xaction.DiskUtilLowWM - 10
	_, isThrottleRequired = lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if isThrottleRequired {
		t.Errorf("Throttling is not required when disk utilization [%d] is lower than LowWM [%d].",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilLowWM)
	}
}

func testMediumDiskUtilization(t *testing.T) {
	lruContext := newLruContext()
	mockThrottleChecker := &MockThrottleChekcer{
		UtilizationToReturn: ctx.config.Xaction.DiskUtilLowWM + 5,
	}

	sleepDuration, isThrottleRequired := lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if !isThrottleRequired {
		t.Errorf("Throttling is required when disk utilization [%d] is above LowWM [%d] and below HighWM[%d].",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilLowWM, ctx.config.Xaction.DiskUtilHighWM)
	}

	expectedDuration := 1 * time.Millisecond
	if sleepDuration <= expectedDuration || sleepDuration >= expectedDuration*2 {
		t.Errorf("Sleep duration [%v] expected between [%v] and [%v]",
			sleepDuration, expectedDuration, expectedDuration*2)
	}

	mockThrottleChecker.UtilizationToReturn += 5
	sleepDuration, isThrottleRequired = lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if !isThrottleRequired {
		t.Errorf("Throttling is required when disk utilization [%d] is above LowWM [%d] and below HighWM[%d].",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilLowWM, ctx.config.Xaction.DiskUtilHighWM)
	}

	if sleepDuration <= 1*time.Millisecond || sleepDuration >= 2*time.Millisecond {
		t.Errorf("Sleep duration [%v] expected between 1ms and 2ms",
			sleepDuration)
	}

	expectedDuration = sleepDuration
	mockThrottleChecker.UtilizationToReturn += 5
	sleepDuration, isThrottleRequired = lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if !isThrottleRequired {
		t.Errorf("Throttling is required when disk utilization [%d] is above LowWM [%d] and below HighWM[%d].",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilLowWM, ctx.config.Xaction.DiskUtilHighWM)
	}

	if sleepDuration <= expectedDuration || sleepDuration >= expectedDuration*2 {
		t.Errorf("Sleep duration [%v] expected between [%v] and [%v]",
			sleepDuration, expectedDuration, expectedDuration*2)
	}
}

func testConstantDiskUtilization(t *testing.T) {
	lruContext := newLruContext()
	mockThrottleChecker := &MockThrottleChekcer{
		UtilizationToReturn: ctx.config.Xaction.DiskUtilLowWM + 10,
	}

	sleepDuration, isThrottleRequired := lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if !isThrottleRequired {
		t.Errorf("Throttling is required when disk utilization [%d] is above LowWM [%d] and below HighWM[%d].",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilLowWM, ctx.config.Xaction.DiskUtilHighWM)
	}

	expectedDuration := 1 * time.Millisecond
	if sleepDuration <= expectedDuration || sleepDuration >= expectedDuration*2 {
		t.Errorf("Sleep duration [%v] expected between [%v] and [%v]",
			sleepDuration, expectedDuration, expectedDuration*2)
	}

	expectedDuration = sleepDuration
	sleepDuration, isThrottleRequired = lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if !isThrottleRequired {
		t.Errorf("Throttling is required when disk utilization [%d] is above LowWM [%d] and below HighWM[%d].",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilLowWM, ctx.config.Xaction.DiskUtilHighWM)
	}

	if sleepDuration != expectedDuration {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
			expectedDuration, sleepDuration)
	}
}

func testMultipleLRUContexts(t *testing.T) {
	lruContext := newLruContext()
	mockThrottleChecker := &MockThrottleChekcer{
		UtilizationToReturn: ctx.config.Xaction.DiskUtilLowWM + 10,
	}

	sleepDuration, isThrottleRequired := lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if !isThrottleRequired {
		t.Errorf("Throttling is required when disk utilization [%d] is above LowWM [%d] and below HighWM[%d].",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilLowWM, ctx.config.Xaction.DiskUtilHighWM)
	}

	expectedDuration := 1 * time.Millisecond
	if sleepDuration <= expectedDuration || sleepDuration >= expectedDuration*2 {
		t.Errorf("Sleep duration [%v] expected between [%v] and [%v]",
			sleepDuration, expectedDuration, expectedDuration*2)
	}

	lruContext2 := newLruContext()
	mockThrottleChecker2 := &MockThrottleChekcer{
		UtilizationToReturn: ctx.config.Xaction.DiskUtilLowWM - 10,
	}
	_, isThrottleRequired = lruContext2.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker2)
	if isThrottleRequired {
		t.Errorf("Throttling is not required when disk utilization [%d] is lower than LowWM [%d].",
			mockThrottleChecker2.UtilizationToReturn, ctx.config.Xaction.DiskUtilLowWM)
	}

	mockThrottleChecker.UtilizationToReturn += 1
	sleepDuration, isThrottleRequired = lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if !isThrottleRequired {
		t.Errorf("Throttling is required when disk utilization [%d] is above LowWM [%d] and below HighWM[%d].",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilLowWM, ctx.config.Xaction.DiskUtilHighWM)
	}

	if sleepDuration <= 1*time.Millisecond || sleepDuration >= 2*time.Millisecond {
		t.Errorf("Sleep duration [%v] expected between 1ms and 2ms",
			sleepDuration)
	}
}

func testSlightlyIncreasedDiskUtilization(t *testing.T) {
	lruContext := newLruContext()
	mockThrottleChecker := &MockThrottleChekcer{
		UtilizationToReturn: ctx.config.Xaction.DiskUtilLowWM + 10,
	}

	sleepDuration, isThrottleRequired := lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if !isThrottleRequired {
		t.Errorf("Throttling is required when disk utilization [%d] is above LowWM [%d] and below HighWM[%d].",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilLowWM, ctx.config.Xaction.DiskUtilHighWM)
	}

	expectedDuration := 1 * time.Millisecond
	if sleepDuration <= expectedDuration || sleepDuration >= expectedDuration*2 {
		t.Errorf("Sleep duration [%v] expected between [%v] and [%v]",
			sleepDuration, expectedDuration, expectedDuration*2)
	}

	expectedDuration = sleepDuration
	mockThrottleChecker.UtilizationToReturn += 1
	sleepDuration, isThrottleRequired = lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if !isThrottleRequired {
		t.Errorf("Throttling is required when disk utilization [%d] is above LowWM [%d] and below HighWM[%d].",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilLowWM, ctx.config.Xaction.DiskUtilHighWM)
	}

	if sleepDuration != expectedDuration {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
			expectedDuration, sleepDuration)
	}
}

func newLruContext() *lructx {
	lruContext := &lructx{
		xlru: new(xactLRU),
		currentThrottleSleepInMS: 0,
	}
	return lruContext
}

func testChangedDiskUtilizationBeforeStatsTime(t *testing.T) {
	lruContext := newLruContext()
	mockThrottleChecker := &MockThrottleChekcer{
		UtilizationToReturn: ctx.config.Xaction.DiskUtilLowWM + 10,
	}

	sleepDuration, isThrottleRequired := lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if !isThrottleRequired {
		t.Errorf("Throttling is required when disk utilization [%d] is above LowWM [%d] and below HighWM[%d].",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilLowWM, ctx.config.Xaction.DiskUtilHighWM)
	}

	expectedDuration := 1 * time.Millisecond
	if sleepDuration <= expectedDuration || sleepDuration >= expectedDuration*2 {
		t.Errorf("Sleep duration [%v] expected between [%v] and [%v]",
			sleepDuration, expectedDuration, expectedDuration*2)
	}

	expectedDuration = sleepDuration
	mockThrottleChecker.UtilizationToReturn = ctx.config.Xaction.DiskUtilHighWM + 5
	oldStatsTime := ctx.config.Periodic.StatsTime
	ctx.config.Periodic.StatsTime = 5 * time.Second
	sleepDuration, isThrottleRequired = lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if !isThrottleRequired {
		t.Errorf("Throttling is required when disk utilization [%d] is "+
			"above LowWM [%d] and below HighWM[%d] and stats duration hasn't elapsed.",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilLowWM, ctx.config.Xaction.DiskUtilHighWM)
	}

	if sleepDuration != expectedDuration {
		t.Errorf("Expected sleep duration [%v] Actual sleep duration: [%v]",
			expectedDuration, sleepDuration)
	}
	ctx.config.Periodic.StatsTime = oldStatsTime
}

func testChangedFSUsedPercentageBeforeDuration(t *testing.T) {
	lruContext := newLruContext()
	mockThrottleChecker := &MockThrottleChekcer{
		UtilizationToReturn: ctx.config.Xaction.DiskUtilLowWM + 10,
		FSUsedPercentage:    uint64(ctx.config.LRU.HighWM + 1),
	}
	_, isThrottleRequired := lruContext.getSleepDurationForThrottle(
		"/tmp/test", -1*time.Second, mockThrottleChecker)
	if isThrottleRequired {
		t.Errorf("No throttling required when FS used capacity [%d] is higher than High WM [%d].",
			mockThrottleChecker.FSUsedPercentage, ctx.config.LRU.HighWM)
	}

	mockThrottleChecker.FSUsedPercentage = 0
	mockThrottleChecker.UtilizationToReturn = 99
	_, isThrottleRequired = lruContext.getSleepDurationForThrottle(
		"/tmp/test", 5*time.Second, mockThrottleChecker)
	if isThrottleRequired {
		t.Errorf("No throttling is required when disk utilization [%d] is "+
			"above LowWM [%d] and below HighWM[%d] and capacity update duration hasn't elapsed.",
			mockThrottleChecker.UtilizationToReturn, ctx.config.Xaction.DiskUtilLowWM, ctx.config.Xaction.DiskUtilHighWM)
	}
}
