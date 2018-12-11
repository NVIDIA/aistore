// Package atime tracks object access times in the system while providing a number of performance enhancements.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package atime

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/ios"
)

var (
	riostat     *ios.IostatRunner
	statsPeriod = time.Second
	maxMapSize  = uint64(1000)
)

func init() {
	fs.Mountpaths = fs.NewMountedFS()
	mpath := "/tmp"
	fs.Mountpaths.Add(mpath)

	updateTestConfig(statsPeriod)
	riostat = ios.NewIostatRunner(fs.Mountpaths)
}

func TestAtimerunnerStop(t *testing.T) {
	mpath := "/tmp"
	fileName := "/tmp/local/bck1/fqn1"

	atimer := NewRunner(fs.Mountpaths, riostat)
	go atimer.Run()
	atimer.ReqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)
	atimer.Stop(fmt.Errorf("test"))

	waitCh := make(chan struct{})
	go func() {
		atimer.Touch(fileName)
		waitCh <- struct{}{}
	}()

	select {
	case <-waitCh:
		t.Error("Touch was successful so atimerunner did not stop")
	case <-time.After(50 * time.Millisecond):
		break
	}
}

func TestAtimerunnerTouch(t *testing.T) {
	mpath := "/tmp"
	fileName := "/tmp/local/bck1/fqn1"

	atimer := NewRunner(fs.Mountpaths, riostat)
	go atimer.Run()
	atimer.ReqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)

	timeBeforeTouch := time.Now()
	atimer.Touch(fileName)
	time.Sleep(50 * time.Millisecond) // wait for runner to process
	if len(atimer.mpathRunners) != 1 || len(atimer.mpathRunners[mpath].atimemap) != 1 {
		t.Error("One file must be present in the map")
	}
	atimeResponse := <-atimer.Atime(fileName)
	accessTime, ok := atimeResponse.AccessTime, atimeResponse.Ok
	if !ok {
		t.Error("File is not present in atime map")
	}

	if accessTime.After(time.Now()) {
		t.Error("Access time exceeds current time")
	}

	if accessTime.Before(timeBeforeTouch) {
		t.Error("Access time is too old")
	}

	atimer.Stop(fmt.Errorf("test"))
}

func TestAtimerunnerTouchNoMpath(t *testing.T) {
	q := fs.Mountpaths
	fs.Mountpaths = fs.NewMountedFS()
	defer func() { fs.Mountpaths = q }()

	updateTestConfig(statsPeriod)
	iostatr := ios.NewIostatRunner(fs.Mountpaths)

	fileName := "/tmp/local/bck1/fqn1"

	atimer := NewRunner(fs.Mountpaths, iostatr)

	go atimer.Run()
	time.Sleep(50 * time.Millisecond)

	atimer.Touch(fileName)
	time.Sleep(50 * time.Millisecond) // wait for runner to process
	if len(atimer.mpathRunners) != 0 {
		t.Error("No files must be present in the map when the file's bucket has LRU Disabled")
	}

	atimer.Stop(fmt.Errorf("test"))
}

func TestAtimerunnerTouchNonExistingFile(t *testing.T) {
	atimer := NewRunner(fs.Mountpaths, riostat)
	go atimer.Run()
	atimer.ReqAddMountpath("/tmp")

	fileName := "test"
	atimer.Touch(fileName)
	time.Sleep(50 * time.Millisecond) // wait for mpathAtimeRunner to process
	if len(atimer.mpathRunners) != 1 {
		t.Error("One mpathAtimeRunners should be present because one mountpath was added")
	}

	atimeResponse := <-atimer.Atime(fileName)
	ok := atimeResponse.Ok
	if ok {
		t.Error("Atime should not be returned for a non existing file.")
	}

	atimer.Stop(fmt.Errorf("test"))
}

// TestAtimerunnerMultipleTouchSameFile touches the same
// file belonging to a local bucket where LRU is enabled multiple times.
func TestAtimerunnerMultipleTouchSameFile(t *testing.T) {
	mpath := "/tmp"
	fileName := "/tmp/local/bck1/fqn1"

	atimer := NewRunner(fs.Mountpaths, riostat)
	go atimer.Run()
	atimer.ReqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)

	atimer.Touch(fileName)
	time.Sleep(50 * time.Millisecond) // wait for runner to process
	if len(atimer.mpathRunners) != 1 || len(atimer.mpathRunners[mpath].atimemap) != 1 {
		t.Error("One mpathAtimeRunner and one file must be present in the atimemap")
	}

	atimeResponse := <-atimer.Atime(fileName)
	accessTime, ok := atimeResponse.AccessTime, atimeResponse.Ok
	if !ok {
		t.Errorf("File [%s] is not present in %s's atime map", fileName, mpath)
	}

	// Make sure that the access time will be a little different
	time.Sleep(50 * time.Millisecond)

	atimer.Touch(fileName)
	time.Sleep(50 * time.Millisecond) // wait for runner to process

	atimeResponse = <-atimer.Atime(fileName)
	accessTimeNext, okNext := atimeResponse.AccessTime, atimeResponse.Ok
	if !okNext {
		t.Errorf("File [%s] is not present in atime map", fileName)
	}

	if !accessTimeNext.After(accessTime) {
		t.Errorf("Access time was not updated. First access: %v, Second access: %v",
			accessTime, accessTimeNext)
	}

	atimer.Stop(fmt.Errorf("test"))
}

func TestAtimerunnerTouchMultipleFile(t *testing.T) {
	mpath := "/tmp"
	fileName1 := "/tmp/cloud/bck1/fqn1"
	fileName2 := "/tmp/local/bck2/fqn2"

	atimer := NewRunner(fs.Mountpaths, riostat)
	go atimer.Run()
	atimer.ReqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)

	atimer.Touch(fileName1)
	time.Sleep(50 * time.Millisecond) // wait for runner to process
	if len(atimer.mpathRunners) != 1 || len(atimer.mpathRunners[mpath].atimemap) != 1 {
		t.Error("One file must be present in the map")
	}
	atimeResponse := <-atimer.Atime(fileName1)
	if !atimeResponse.Ok {
		t.Errorf("File [%s] is not present in atime map", fileName1)
	}

	atimer.Touch(fileName2)
	time.Sleep(50 * time.Millisecond) // wait for runner to process
	if len(atimer.mpathRunners) != 1 || len(atimer.mpathRunners[mpath].atimemap) != 2 {
		t.Error("Two files must be present in the map")
	}

	atimeResponse = <-atimer.Atime(fileName2)
	if !atimeResponse.Ok {
		t.Errorf("File [%s] is not present in atime map", fileName2)
	}

	atimer.Stop(fmt.Errorf("test"))
}

func TestAtimerunnerFlush(t *testing.T) {
	mpath := "/tmp"
	fileName1 := "/tmp/cloud/bck1/fqn1"
	fileName2 := "/tmp/local/bck2/fqn2"
	fileName3 := "/tmp/local/bck2/fqn3"

	atimer := NewRunner(fs.Mountpaths, riostat)
	go atimer.Run()
	atimer.ReqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)

	atimer.Touch(fileName1)
	atimer.Touch(fileName2)
	atimer.Touch(fileName3)
	time.Sleep(50 * time.Millisecond) // wait for runner to process

	atimer.mpathRunners[mpath].flush(1)
	time.Sleep(50 * time.Millisecond) // wait for runner to process
	if len(atimer.mpathRunners) != 1 || len(atimer.mpathRunners[mpath].atimemap) != 2 {
		t.Error("Invalid number of files in atimerunner")
	}

	atimer.mpathRunners[mpath].flush(2)
	time.Sleep(50 * time.Millisecond) // wait for runner to process
	if len(atimer.mpathRunners) != 1 || len(atimer.mpathRunners[mpath].atimemap) != 0 {
		t.Error("Invalid number of files in atimerunner")
	}

	atimer.Stop(fmt.Errorf("test"))
}

// TestAtimerunnerGetNumberItemsToFlushSimple tests the number of items to flush.
func TestAtimerunnerGetNumberItemsToFlushSimple(t *testing.T) {
	mpath := "/tmp"
	fileName1 := "/tmp/local/bck1/fqn1"
	fileName2 := "/tmp/cloud/bck2/fqn2"

	atimer := NewRunner(fs.Mountpaths, riostat)

	go riostat.Run()
	go atimer.Run()
	defer func() {
		riostat.Stop(nil)
		atimer.Stop(nil)
	}()

	atimer.ReqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)

	// When the initial capacity was not achieved, function should return 0
	atimer.Touch(fileName1)
	atimer.Touch(fileName2)
	time.Sleep(time.Millisecond) // wait for runner to process

	n := atimer.mpathRunners[mpath].getNumberItemsToFlush()
	if n != 0 {
		t.Error("number items to flush should be 0 when capacity not achieved")
	}
}

func TestAtimerunnerGetNumberItemsToFlushDiskIdle(t *testing.T) {
	mpath := "/tmp"
	statsPeriod = time.Second

	q := riostat.Disk
	defer func() { riostat.Disk = q }()

	riostat.Disk = map[string]cmn.SimpleKVs{
		"disk1": cmn.SimpleKVs{
			"%util": "21.34",
		},
	}

	config := cmn.GCO.BeginUpdate()
	config.LRU.AtimeCacheMax = 1
	cmn.GCO.CommitUpdate(config)

	atimer := NewRunner(fs.Mountpaths, riostat)

	go atimer.Run()
	defer func() { atimer.Stop(nil) }()

	atimer.ReqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)

	itemCount := atimeCacheFlushThreshold * 2
	// split files between the two types of buckets
	for i := 0; i < itemCount; i++ {
		if i%2 == 0 {
			atimer.Touch("/tmp/cloud/bck1/fqn" + strconv.Itoa(i))
		} else {
			atimer.Touch("/tmp/local/bck2/fqn" + strconv.Itoa(i))
		}
	}

	time.Sleep(time.Millisecond * 10) // wait for runner to process

	n := atimer.mpathRunners[mpath].getNumberItemsToFlush()
	if n != itemCount/4 {
		t.Error("when idle we should flush 25% of the cache")
	}
}

func TestAtimerunnerGetNumberItemsToFlushVeryHighWatermark(t *testing.T) {
	mpath := "/tmp"
	itemCount := atimeCacheFlushThreshold * 2
	maxMapSize = uint64(itemCount)
	statsPeriod = time.Second

	updateTestConfig(statsPeriod)
	iostatr := ios.NewIostatRunner(fs.Mountpaths)

	atimer := NewRunner(fs.Mountpaths, iostatr)
	go atimer.Run()
	defer func() { atimer.Stop(nil) }()
	atimer.ReqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)

	// split files between the two types of buckets
	for i := 0; i < itemCount; i++ {
		if i%2 == 0 {
			atimer.Touch("/tmp/cloud/bck1/fqn" + strconv.Itoa(i))
		} else {
			atimer.Touch("/tmp/local/bck2/fqn" + strconv.Itoa(i))
		}
	}

	time.Sleep(time.Millisecond * 10) // wait for runner to process

	// simulate highly utilized disk
	go iostatr.Run()
	defer func() { iostatr.Stop(nil) }()
	iostatr.Disk = make(map[string]cmn.SimpleKVs)

	for disk := range iostatr.Disk {
		iostatr.Disk[disk] = make(cmn.SimpleKVs, 0)
		iostatr.Disk[disk]["%util"] = "99.94"
	}
	n := atimer.mpathRunners[mpath].getNumberItemsToFlush()
	if n != itemCount/2 {
		t.Errorf("%d != %d: when used cap is 100%% we should flush 50%% of the cache", n, itemCount/2)
	}
}

func TestAtimerunnerGetNumberItemsToFlushHighWatermark(t *testing.T) {
	mpath := "/tmp"
	itemCount := atimeCacheFlushThreshold * 2
	maxMapSize = uint64(itemCount*(200-atimeHWM)/100) + 10
	statsPeriod = time.Second

	updateTestConfig(statsPeriod)
	iostatr := ios.NewIostatRunner(fs.Mountpaths)

	atimer := NewRunner(fs.Mountpaths, iostatr)

	go iostatr.Run()
	go atimer.Run()
	defer func() {
		iostatr.Stop(nil)
		atimer.Stop(nil)
	}()
	atimer.ReqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)

	// split files between the two types of buckets
	for i := 0; i < itemCount; i++ {
		if i%2 == 0 {
			atimer.Touch("/tmp/cloud/bck1/fqn" + strconv.Itoa(i))
		} else {
			atimer.Touch("/tmp/local/bck2/fqn" + strconv.Itoa(i))
		}
	}

	time.Sleep(time.Millisecond * 10) // wait for runner to process

	// simulate highly utilized disk
	for disk := range iostatr.Disk {
		iostatr.Disk[disk] = make(cmn.SimpleKVs, 0)
		iostatr.Disk[disk]["%util"] = "99.94"
	}
	n := atimer.mpathRunners[mpath].getNumberItemsToFlush()
	if n != itemCount/4 {
		t.Error("when used cap is above high watermark we should flush 25% of the cache")
	}
}

func TestAtimerunnerGetNumberItemsToFlushLowWatermark(t *testing.T) {
	mpath := "/tmp"
	itemCount := atimeCacheFlushThreshold * 2
	maxMapSize = uint64(itemCount*(200-atimeLWM)/100) + 10
	statsPeriod = time.Second

	updateTestConfig(statsPeriod)
	iostatr := ios.NewIostatRunner(fs.Mountpaths)

	atimer := NewRunner(fs.Mountpaths, iostatr)
	go atimer.Run()
	defer func() { atimer.Stop(nil) }()
	atimer.ReqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)

	// split files between the two types of buckets
	for i := 0; i < itemCount; i++ {
		if i%2 == 0 {
			atimer.Touch("/tmp/cloud/bck1/fqn" + strconv.Itoa(i))
		} else {
			atimer.Touch("/tmp/local/bck2/fqn" + strconv.Itoa(i))
		}
	}

	time.Sleep(time.Millisecond * 10) // wait for runner to process

	// simulate highly utilized disk
	go iostatr.Run()
	defer func() { iostatr.Stop(nil) }()
	iostatr.Disk = make(map[string]cmn.SimpleKVs)

	for disk := range iostatr.Disk {
		iostatr.Disk[disk] = make(cmn.SimpleKVs, 0)
		iostatr.Disk[disk]["%util"] = "99.94"
	}
	n := atimer.mpathRunners[mpath].getNumberItemsToFlush()
	if n == 0 {
		t.Error("when used cap is above low watermark we should flush some of the cache")
	}
}

func TestAtimerunnerGetNumberItemsToFlushLowFilling(t *testing.T) {
	mpath := "/tmp"
	itemCount := atimeCacheFlushThreshold * 2
	maxMapSize = uint64(itemCount * 1000)
	statsPeriod = time.Second

	updateTestConfig(statsPeriod)
	iostatr := ios.NewIostatRunner(fs.Mountpaths)

	atimer := NewRunner(fs.Mountpaths, iostatr)
	go atimer.Run()
	defer func() { atimer.Stop(nil) }()
	atimer.ReqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)

	// split files between the two types of buckets
	for i := 0; i < itemCount; i++ {
		if i%2 == 0 {
			atimer.Touch("/tmp/cloud/bck1/fqn" + strconv.Itoa(i))
		} else {
			atimer.Touch("/tmp/local/bck2/fqn" + strconv.Itoa(i))
		}
	}

	time.Sleep(time.Millisecond * 10) // wait for runner to process

	// simulate highly utilized disk
	go iostatr.Run()
	defer func() { iostatr.Stop(nil) }()
	iostatr.Disk = make(map[string]cmn.SimpleKVs)

	for disk := range iostatr.Disk {
		iostatr.Disk[disk] = make(cmn.SimpleKVs, 0)
		iostatr.Disk[disk]["%util"] = "99.34"
	}
	n := atimer.mpathRunners[mpath].getNumberItemsToFlush()
	if n != 0 {
		t.Errorf("%d: when used capacity is low and disk is busy we should not flush at all", n)
	}
}

func cleanMountpaths() {
	availableMountpaths, disabledMountpaths := fs.Mountpaths.Get()
	for _, mpathInfo := range availableMountpaths {
		fs.Mountpaths.Remove(mpathInfo.Path)
	}
	for _, mpathInfo := range disabledMountpaths {
		fs.Mountpaths.Remove(mpathInfo.Path)
	}
}

func updateTestConfig(d time.Duration) {
	config := cmn.GCO.BeginUpdate()
	config.Periodic.StatsTime = d
	config.LRU.AtimeCacheMax = maxMapSize
	cmn.GCO.CommitUpdate(config)
}
