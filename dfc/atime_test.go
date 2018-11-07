/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
package dfc

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
)

func TestAtimerunnerStop(t *testing.T) {
	mpath := "/tmp"
	fileName := "/tmp/local/bck1/fqn1"

	fs.Mountpaths = fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
	fs.Mountpaths.DisableFsIDCheck()
	fs.Mountpaths.AddMountpath(mpath)

	target := newFakeTargetRunner()
	atimer := newAtimeRunner(target, fs.Mountpaths)

	go atimer.Run()
	atimer.reqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)
	atimer.Stop(fmt.Errorf("test"))

	waitCh := make(chan struct{})
	go func() {
		atimer.touch(fileName)
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

	fs.Mountpaths = fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
	fs.Mountpaths.AddMountpath(mpath)
	target := newFakeTargetRunner()
	atimer := newAtimeRunner(target, fs.Mountpaths)

	go atimer.Run()
	atimer.reqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)

	timeBeforeTouch := time.Now()
	atimer.touch(fileName)
	time.Sleep(50 * time.Millisecond) // wait for runner to process
	if len(atimer.mpathRunners) != 1 || len(atimer.mpathRunners[mpath].atimemap) != 1 {
		t.Error("One file must be present in the map")
	}
	atimeResponse := <-atimer.atime(fileName)
	accessTime, ok := atimeResponse.accessTime, atimeResponse.ok
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
	fileName := "/tmp/local/bck1/fqn1"

	fs.Mountpaths = fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
	target := newFakeTargetRunner()
	atimer := newAtimeRunner(target, fs.Mountpaths)

	go atimer.Run()
	time.Sleep(50 * time.Millisecond)

	atimer.touch(fileName)
	time.Sleep(50 * time.Millisecond) // wait for runner to process
	if len(atimer.mpathRunners) != 0 {
		t.Error("No files must be present in the map when the file's bucket has LRU Disabled")
	}

	atimer.Stop(fmt.Errorf("test"))
}

func TestAtimerunnerTouchNonExistingFile(t *testing.T) {
	fs.Mountpaths = fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
	fs.Mountpaths.AddMountpath("/tmp")
	atimer := newAtimeRunner(newFakeTargetRunner(), fs.Mountpaths)
	go atimer.Run()
	atimer.reqAddMountpath("/tmp")

	fileName := "test"
	atimer.touch(fileName)
	time.Sleep(50 * time.Millisecond) // wait for mpathAtimeRunner to process
	if len(atimer.mpathRunners) != 1 {
		t.Error("One mpathAtimeRunners should be present because one mountpath was added")
	}

	atimeResponse := <-atimer.atime(fileName)
	ok := atimeResponse.ok
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

	fs.Mountpaths = fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
	target := newFakeTargetRunner()
	fs.Mountpaths.AddMountpath(mpath)

	atimer := newAtimeRunner(target, fs.Mountpaths)
	go atimer.Run()
	atimer.reqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)

	atimer.touch(fileName)
	time.Sleep(50 * time.Millisecond) // wait for runner to process
	if len(atimer.mpathRunners) != 1 || len(atimer.mpathRunners[mpath].atimemap) != 1 {
		t.Error("One mpathAtimeRunner and one file must be present in the atimemap")
	}

	atimeResponse := <-atimer.atime(fileName)
	accessTime, ok := atimeResponse.accessTime, atimeResponse.ok
	if !ok {
		t.Errorf("File [%s] is not present in %s's atime map", fileName, mpath)
	}

	// Make sure that the access time will be a little different
	time.Sleep(50 * time.Millisecond)

	atimer.touch(fileName)
	time.Sleep(50 * time.Millisecond) // wait for runner to process

	atimeResponse = <-atimer.atime(fileName)
	accessTimeNext, okNext := atimeResponse.accessTime, atimeResponse.ok
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

	fs.Mountpaths = fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
	target := newFakeTargetRunner()
	fs.Mountpaths.AddMountpath(mpath)

	atimer := newAtimeRunner(target, fs.Mountpaths)
	go atimer.Run()
	atimer.reqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)

	atimer.touch(fileName1)
	time.Sleep(50 * time.Millisecond) // wait for runner to process
	if len(atimer.mpathRunners) != 1 || len(atimer.mpathRunners[mpath].atimemap) != 1 {
		t.Error("One file must be present in the map")
	}
	atimeResponse := <-atimer.atime(fileName1)
	if !atimeResponse.ok {
		t.Errorf("File [%s] is not present in atime map", fileName1)
	}

	atimer.touch(fileName2)
	time.Sleep(50 * time.Millisecond) // wait for runner to process
	if len(atimer.mpathRunners) != 1 || len(atimer.mpathRunners[mpath].atimemap) != 2 {
		t.Error("Two files must be present in the map")
	}

	atimeResponse = <-atimer.atime(fileName2)
	if !atimeResponse.ok {
		t.Errorf("File [%s] is not present in atime map", fileName2)
	}

	atimer.Stop(fmt.Errorf("test"))
}

func TestAtimerunnerFlush(t *testing.T) {
	mpath := "/tmp"
	fileName1 := "/tmp/cloud/bck1/fqn1"
	fileName2 := "/tmp/local/bck2/fqn2"
	fileName3 := "/tmp/local/bck2/fqn3"

	fs.Mountpaths = fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
	target := newFakeTargetRunner()
	fs.Mountpaths.AddMountpath(mpath)

	atimer := newAtimeRunner(target, fs.Mountpaths)
	go atimer.Run()
	atimer.reqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)

	atimer.touch(fileName1)
	atimer.touch(fileName2)
	atimer.touch(fileName3)
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

// TestAtimerunnerGetNumberItemsToFlushSimple test the number of items to
// flush.
func TestAtimerunnerGetNumberItemsToFlushSimple(t *testing.T) {
	mpath := "/tmp"
	fileName1 := "/tmp/local/bck1/fqn1"
	fileName2 := "/tmp/cloud/bck2/fqn2"

	ctx.config.Periodic.StatsTime = 1 * time.Second
	ctx.rg = &rungroup{
		runarr: make([]cmn.Runner, 0, 4),
		runmap: make(map[string]cmn.Runner),
	}

	fs.Mountpaths = fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
	cleanMountpaths()
	fs.Mountpaths.AddMountpath(mpath)
	target := newFakeTargetRunner()

	atimer := newAtimeRunner(target, fs.Mountpaths)
	ctx.rg.add(atimer, xatime)
	ctx.rg.add(newIostatRunner(), xiostat)
	go ctx.rg.run()
	atimer.reqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)

	// When the initial capacity was not achieved, function should return 0
	atimer.touch(fileName1)
	atimer.touch(fileName2)
	time.Sleep(time.Millisecond) // wait for runner to process

	n := atimer.mpathRunners[mpath].getNumberItemsToFlush()
	if n != 0 {
		t.Error("number items to flush should be 0 when capacity not achieved")
	}

	getiostatrunner().stopCh <- struct{}{}
}

func TestAtimerunnerGetNumberItemsToFlushDiskIdle(t *testing.T) {
	mpath := "/tmp"
	ctx.config.LRU.AtimeCacheMax = 1
	ctx.config.Periodic.StatsTime = 1 * time.Second
	ctx.rg = &rungroup{
		runarr: make([]cmn.Runner, 0, 1),
		runmap: make(map[string]cmn.Runner),
	}

	fs.Mountpaths = fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
	cleanMountpaths()
	fs.Mountpaths.AddMountpath(mpath)

	iostatr := newIostatRunner()
	iostatr.Disk = map[string]cmn.SimpleKVs{
		"disk1": cmn.SimpleKVs{
			"%util": "21.34",
		},
	}
	ctx.rg.add(iostatr, xiostat)

	target := newFakeTargetRunner()
	target.fsprg.addMountpath(mpath)
	atimer := newAtimeRunner(target, fs.Mountpaths)
	ctx.rg.add(atimer, xatime)

	go ctx.rg.run()
	atimer.reqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)

	itemCount := atimeCacheFlushThreshold * 2
	// split files between the two types of buckets
	for i := 0; i < itemCount; i++ {
		if i%2 == 0 {
			atimer.touch("/tmp/cloud/bck1/fqn" + strconv.Itoa(i))
		} else {
			atimer.touch("/tmp/local/bck2/fqn" + strconv.Itoa(i))
		}
	}

	time.Sleep(time.Millisecond * 10) // wait for runner to process

	n := atimer.mpathRunners[mpath].getNumberItemsToFlush()
	if n != itemCount/4 {
		t.Error("when idle we should flush 25% of the cache")
	}

	getiostatrunner().stopCh <- struct{}{}
}

func TestAtimerunnerGetNumberItemsToFlushVeryHighWatermark(t *testing.T) {
	mpath := "/tmp"
	itemCount := atimeCacheFlushThreshold * 2
	ctx.config.LRU.AtimeCacheMax = uint64(itemCount)
	ctx.config.Periodic.StatsTime = 1 * time.Second
	ctx.rg = &rungroup{
		runarr: make([]cmn.Runner, 0, 1),
		runmap: make(map[string]cmn.Runner),
	}

	fs.Mountpaths = fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
	cleanMountpaths()
	fs.Mountpaths.AddMountpath(mpath)

	iostatr := newIostatRunner()
	ctx.rg.add(iostatr, xiostat)

	target := newFakeTargetRunner()
	target.fsprg.addMountpath(mpath)
	atimer := newAtimeRunner(target, fs.Mountpaths)
	ctx.rg.add(atimer, xatime)

	go ctx.rg.run()
	atimer.reqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)

	// split files between the two types of buckets
	for i := 0; i < itemCount; i++ {
		if i%2 == 0 {
			atimer.touch("/tmp/cloud/bck1/fqn" + strconv.Itoa(i))
		} else {
			atimer.touch("/tmp/local/bck2/fqn" + strconv.Itoa(i))
		}
	}

	time.Sleep(time.Millisecond * 10) // wait for runner to process

	// simulate highly utilized disk
	iostatr.Disk = make(map[string]cmn.SimpleKVs)
	mpathInfo, _ := path2mpathInfo(mpath)
	disks := fs2disks(mpathInfo.FileSystem)
	for disk := range disks {
		iostatr.Disk[disk] = make(cmn.SimpleKVs, 0)
		iostatr.Disk[disk]["%util"] = "99.94"
	}
	n := atimer.mpathRunners[mpath].getNumberItemsToFlush()
	if n != itemCount/2 {
		t.Error("when filling is 100% we should flush 50% of the cache")
	}

	getiostatrunner().stopCh <- struct{}{}
}

func TestAtimerunnerGetNumberItemsToFlushHighWatermark(t *testing.T) {
	mpath := "/tmp"
	itemCount := atimeCacheFlushThreshold * 2
	ctx.config.LRU.AtimeCacheMax = uint64(itemCount*(200-atimeHWM)/100) + 10
	ctx.config.Periodic.StatsTime = 1 * time.Second
	ctx.rg = &rungroup{
		runarr: make([]cmn.Runner, 0, 1),
		runmap: make(map[string]cmn.Runner),
	}
	fs.Mountpaths = fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
	cleanMountpaths()
	fs.Mountpaths.AddMountpath(mpath)

	iostatr := newIostatRunner()
	ctx.rg.add(iostatr, xiostat)

	target := newFakeTargetRunner()
	target.fsprg.addMountpath(mpath)
	atimer := newAtimeRunner(target, fs.Mountpaths)
	ctx.rg.add(atimer, xatime)

	go ctx.rg.run()
	atimer.reqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)

	// split files between the two types of buckets
	for i := 0; i < itemCount; i++ {
		if i%2 == 0 {
			atimer.touch("/tmp/cloud/bck1/fqn" + strconv.Itoa(i))
		} else {
			atimer.touch("/tmp/local/bck2/fqn" + strconv.Itoa(i))
		}
	}

	time.Sleep(time.Millisecond * 10) // wait for runner to process

	// simulate highly utilized disk
	iostatr.Disk = make(map[string]cmn.SimpleKVs)
	mpathInfo, _ := path2mpathInfo(mpath)
	disks := fs2disks(mpathInfo.FileSystem)
	for disk := range disks {
		iostatr.Disk[disk] = make(cmn.SimpleKVs, 0)
		iostatr.Disk[disk]["%util"] = "99.94"
	}
	n := atimer.mpathRunners[mpath].getNumberItemsToFlush()
	if n != itemCount/4 {
		t.Error("when filling is above high watermark we should flush 25% of the cache")
	}

	getiostatrunner().stopCh <- struct{}{}
}

func TestAtimerunnerGetNumberItemsToFlushLowWatermark(t *testing.T) {
	mpath := "/tmp"
	itemCount := atimeCacheFlushThreshold * 2

	ctx.config.LRU.AtimeCacheMax = uint64(itemCount*(200-atimeLWM)/100) + 10
	ctx.config.Periodic.StatsTime = 1 * time.Second
	ctx.rg = &rungroup{
		runarr: make([]cmn.Runner, 0, 1),
		runmap: make(map[string]cmn.Runner),
	}

	fs.Mountpaths = fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
	cleanMountpaths()
	fs.Mountpaths.AddMountpath(mpath)

	iostatr := newIostatRunner()
	ctx.rg.add(iostatr, xiostat)

	target := newFakeTargetRunner()
	target.fsprg.addMountpath(mpath)
	atimer := newAtimeRunner(target, fs.Mountpaths)
	ctx.rg.add(atimer, xatime)

	go ctx.rg.run()
	atimer.reqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)

	// split files between the two types of buckets
	for i := 0; i < itemCount; i++ {
		if i%2 == 0 {
			atimer.touch("/tmp/cloud/bck1/fqn" + strconv.Itoa(i))
		} else {
			atimer.touch("/tmp/local/bck2/fqn" + strconv.Itoa(i))
		}
	}

	time.Sleep(time.Millisecond * 10) // wait for runner to process

	// simulate highly utilized disk
	iostatr.Disk = make(map[string]cmn.SimpleKVs)
	mpathInfo, _ := path2mpathInfo(mpath)
	disks := fs2disks(mpathInfo.FileSystem)
	for disk := range disks {
		iostatr.Disk[disk] = make(cmn.SimpleKVs, 0)
		iostatr.Disk[disk]["%util"] = "99.94"
	}
	n := atimer.mpathRunners[mpath].getNumberItemsToFlush()
	if n == 0 {
		t.Error("when filling is above low watermark we should flush some of the cache")
	}

	getiostatrunner().stopCh <- struct{}{}
}

func TestAtimerunnerGetNumberItemsToFlushLowFilling(t *testing.T) {
	mpath := "/tmp"
	itemCount := atimeCacheFlushThreshold * 2

	ctx.config.LRU.AtimeCacheMax = uint64(itemCount * 1000)
	ctx.config.Periodic.StatsTime = 1 * time.Second
	ctx.rg = &rungroup{
		runarr: make([]cmn.Runner, 0, 1),
		runmap: make(map[string]cmn.Runner),
	}

	fs.Mountpaths = fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
	cleanMountpaths()
	fs.Mountpaths.AddMountpath(mpath)

	iostatr := newIostatRunner()

	ctx.rg.add(iostatr, xiostat)

	target := newFakeTargetRunner()
	target.fsprg.addMountpath(mpath)
	atimer := newAtimeRunner(target, fs.Mountpaths)
	ctx.rg.add(atimer, xatime)

	go ctx.rg.run()
	atimer.reqAddMountpath(mpath)
	time.Sleep(50 * time.Millisecond)

	// split files between the two types of buckets
	for i := 0; i < itemCount; i++ {
		if i%2 == 0 {
			atimer.touch("/tmp/cloud/bck1/fqn" + strconv.Itoa(i))
		} else {
			atimer.touch("/tmp/local/bck2/fqn" + strconv.Itoa(i))
		}
	}

	time.Sleep(time.Millisecond * 10) // wait for runner to process

	// simulate highly utilized disk
	iostatr.Disk = make(map[string]cmn.SimpleKVs)
	mpathInfo, _ := path2mpathInfo(mpath)
	disks := fs2disks(mpathInfo.FileSystem)
	for disk := range disks {
		iostatr.Disk[disk] = make(cmn.SimpleKVs, 0)
		iostatr.Disk[disk]["%util"] = "99.34"
	}
	n := atimer.mpathRunners[mpath].getNumberItemsToFlush()
	if n != 0 {
		t.Error("when filling is low and disk is busy we should not flush at all")
	}

	getiostatrunner().stopCh <- struct{}{}
}

func cleanMountpaths() {
	availableMountpaths, disabledMountpaths := fs.Mountpaths.Mountpaths()
	for _, mpathInfo := range availableMountpaths {
		fs.Mountpaths.RemoveMountpath(mpathInfo.Path)
	}
	for _, mpathInfo := range disabledMountpaths {
		fs.Mountpaths.RemoveMountpath(mpathInfo.Path)
	}
}
