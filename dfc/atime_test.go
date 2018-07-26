// Package dfc is a scalable object-storage based caching system with Amazon and Google Cloud backends.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

import (
	"fmt"
	"io/ioutil"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/fs"
)

func TestAtimerunnerStop(t *testing.T) {
	ctx.config.LRU.LRUEnabled = true
	atimer := &atimerunner{
		chfqn:  make(chan string),
		chstop: make(chan struct{}),
		atimemap: &atimemap{
			fsToFilesMap: make(map[string]map[string]time.Time),
		},
	}
	go atimer.run()

	atimer.stop(fmt.Errorf("test"))

	waitChan := make(chan struct{})
	go func() {
		atimer.touch("/tmp/fqn1")
		var v struct{}
		waitChan <- v
	}()

	select {
	case <-waitChan:
		t.Error("Touch was successful so atimerunner did not stop")
	case <-time.After(50 * time.Millisecond):
		break
	}
}

func TestAtimerunnerTouch(t *testing.T) {
	ctx.config.LRU.LRUEnabled = true
	atimer := newAtimeRunner()
	go atimer.run()

	tempFile, fs := getTempFile(t, "1")
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()
	timeBeforeTouch := time.Now()
	atimer.touch(tempFile.Name())
	time.Sleep(50 * time.Millisecond) // wait for runner to process
	if len(atimer.atimemap.fsToFilesMap) != 1 || len(atimer.atimemap.fsToFilesMap[fs]) != 1 {
		t.Error("One file must be present in the map")
	}
	atimer.atime(tempFile.Name())
	accessTimeResponse := <-atimer.chSendAtime
	if !accessTimeResponse.ok {
		t.Error("File is not present in atime map")
	}

	if accessTimeResponse.accessTime.After(time.Now()) {
		t.Error("Access time exceeds current time")
	}

	if accessTimeResponse.accessTime.Before(timeBeforeTouch) {
		t.Error("Access time is too old")
	}

	atimer.stop(fmt.Errorf("test"))
}

func TestAtimerunnerTouchNonExistingFile(t *testing.T) {
	ctx.config.LRU.LRUEnabled = true
	atimer := newAtimeRunner()
	go atimer.run()

	fileName := "test"
	atimer.touch(fileName)
	time.Sleep(50 * time.Millisecond) // wait for runner to process
	if len(atimer.atimemap.fsToFilesMap) != 0 {
		t.Error("No files must be present in the map")
	}

	atimer.atime(fileName)
	accessTimeResponse := <-atimer.chSendAtime
	if accessTimeResponse.ok {
		t.Error("Atime should not be returned for a non existing file.")
	}

	atimer.stop(fmt.Errorf("test"))
}

func TestAtimerunnerMultipleTouchSameFile(t *testing.T) {
	ctx.config.LRU.LRUEnabled = true
	atimer := newAtimeRunner()
	go atimer.run()

	tempFile, fs := getTempFile(t, "1")
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()
	atimer.touch(tempFile.Name())
	time.Sleep(50 * time.Millisecond) // wait for runner to process
	if len(atimer.atimemap.fsToFilesMap) != 1 || len(atimer.atimemap.fsToFilesMap[fs]) != 1 {
		t.Error("One file must be present in the map")
	}

	atimer.atime(tempFile.Name())
	accessTimeResponse := <-atimer.chSendAtime
	if !accessTimeResponse.ok {
		t.Errorf("File [%s] is not present in atime map", tempFile.Name())
	}

	// Make sure that the access time will be a little different
	time.Sleep(50 * time.Millisecond)

	atimer.touch(tempFile.Name())
	time.Sleep(50 * time.Millisecond) // wait for runner to process

	atimer.atime(tempFile.Name())
	accessTimeResponseNext := <-atimer.chSendAtime
	if !accessTimeResponseNext.ok {
		t.Errorf("File [%s] is not present in atime map", tempFile.Name())
	}

	if !accessTimeResponseNext.accessTime.After(accessTimeResponse.accessTime) {
		t.Errorf("Access time was not updated. First access: %v, Second access: %v",
			accessTimeResponse.accessTime, accessTimeResponseNext.accessTime)
	}

	atimer.stop(fmt.Errorf("test"))
}

func TestAtimerunnerMultipleTouchMultipleFile(t *testing.T) {
	ctx.config.LRU.LRUEnabled = true
	atimer := newAtimeRunner()
	go atimer.run()

	tempFile, fs := getTempFile(t, "1")
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()
	atimer.touch(tempFile.Name())
	time.Sleep(50 * time.Millisecond) // wait for runner to process
	if len(atimer.atimemap.fsToFilesMap) != 1 || len(atimer.atimemap.fsToFilesMap[fs]) != 1 {
		t.Error("One file must be present in the map")
	}
	atimer.atime(tempFile.Name())
	accessTimeResponse := <-atimer.chSendAtime
	if !accessTimeResponse.ok {
		t.Errorf("File [%s] is not present in atime map", tempFile.Name())
	}

	tempFile2, _ := getTempFile(t, "2")
	defer func() {
		tempFile2.Close()
		os.Remove(tempFile2.Name())
	}()
	atimer.touch(tempFile2.Name())
	time.Sleep(50 * time.Millisecond) // wait for runner to process
	if len(atimer.atimemap.fsToFilesMap) != 1 || len(atimer.atimemap.fsToFilesMap[fs]) != 2 {
		t.Error("Two files must be present in the map")
	}

	atimer.atime(tempFile2.Name())
	accessTimeResponse = <-atimer.chSendAtime
	if !accessTimeResponse.ok {
		t.Errorf("File [%s] is not present in atime map", tempFile2.Name())
	}

	atimer.stop(fmt.Errorf("test"))
}

func TestAtimerunnerFlush(t *testing.T) {
	ctx.config.LRU.LRUEnabled = true
	atimer := newAtimeRunner()
	go atimer.run()

	file1, _ := getTempFile(t, "1")
	atimer.touch(file1.Name())
	defer func() {
		file1.Close()
		os.Remove(file1.Name())
	}()
	file2, _ := getTempFile(t, "2")
	atimer.touch(file2.Name())
	defer func() {
		file2.Close()
		os.Remove(file2.Name())
	}()
	file3, fileSystem := getTempFile(t, "3")
	atimer.touch(file3.Name())
	defer func() {
		file3.Close()
		os.Remove(file3.Name())
	}()
	time.Sleep(50 * time.Millisecond) // wait for runner to process

	atimer.flush(fileSystem, 1)
	if len(atimer.atimemap.fsToFilesMap) != 1 || len(atimer.atimemap.fsToFilesMap[fileSystem]) != 2 {
		t.Error("Invalid number of files in atimerunner")
	}

	atimer.flush(fileSystem, 2)
	if len(atimer.atimemap.fsToFilesMap) != 1 || len(atimer.atimemap.fsToFilesMap[fileSystem]) != 0 {
		t.Error("Invalid number of files in atimerunner")
	}

	atimer.stop(fmt.Errorf("test"))
}

func TestAtimeNonExistingFile(t *testing.T) {
	ctx.config.LRU.LRUEnabled = true
	atimer := newAtimeRunner()
	go atimer.run()
	atimer.atime("test")
	accessTimeResponse := <-atimer.chSendAtime
	if accessTimeResponse.ok {
		t.Errorf("Expected to fail while getting atime of a non existing file.")
	}
}

func TestTouchNonExistingFile(t *testing.T) {
	ctx.config.LRU.LRUEnabled = true
	atimer := newAtimeRunner()
	go atimer.run()
	atimer.touch("test")
	if len(atimer.atimemap.fsToFilesMap) != 0 {
		t.Errorf("No file system should be present in the map.")
	}
}

func TestAtimerunnerGetNumberItemsToFlushSimple(t *testing.T) {
	ctx.config.Periodic.StatsTime = 1 * time.Second
	ctx.rg = &rungroup{
		runarr: make([]runner, 0, 4),
		runmap: make(map[string]runner),
	}

	ctx.mountpaths.Available = make(map[string]*fs.MountpathInfo, len(ctx.config.FSpaths))
	tempFile, fs := getTempFile(t, "1")
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	ctx.rg.add(newIostatRunner(), xiostat)
	go ctx.rg.run()

	// When LRU is not enabled, function should return 0
	{
		ctx.config.LRU.LRUEnabled = false
		atimer := newAtimeRunner()
		go atimer.run()
		n := atimer.getNumberItemsToFlush(fs)
		if n != 0 {
			t.Error("number items to flush should be 0 when LRU not enabled")
		}

		atimer.stop(fmt.Errorf("test"))
	}

	// When the initial capacity was not achieved, function should return 0
	{
		ctx.config.LRU.LRUEnabled = true
		atimer := newAtimeRunner()
		go atimer.run()

		atimer.touch("/tmp/fqn1")
		atimer.touch("/tmp/fqn2")
		time.Sleep(time.Millisecond) // wait for runner to process

		n := atimer.getNumberItemsToFlush(fs)
		if n != 0 {
			t.Error("number items to flush should be 0 when capacity not achieved")
		}

		atimer.stop(fmt.Errorf("test"))
	}

	getiostatrunner().chsts <- struct{}{}
}

func TestAtimerunnerGetNumberItemsToFlushDiskIdle(t *testing.T) {
	ctx.config.LRU.AtimeCacheMax = 1
	ctx.config.LRU.LRUEnabled = true
	ctx.config.Periodic.StatsTime = 1 * time.Second
	ctx.rg = &rungroup{
		runarr: make([]runner, 0, 1),
		runmap: make(map[string]runner),
	}

	ctx.mountpaths.Available = make(map[string]*fs.MountpathInfo, len(ctx.config.FSpaths))
	tempFile, fs := getTempFile(t, "1")
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	iostatr := newIostatRunner()
	iostatr.Disk = map[string]simplekvs{
		"disk1": simplekvs{
			"%util": "21.34",
		},
	}
	ctx.rg.add(iostatr, xiostat)
	go ctx.rg.run()

	atimer := newAtimeRunner()
	go atimer.run()

	itemCount := atimeCacheFlushThreshold * 2
	for i := 0; i < itemCount; i++ {
		atimer.touch("/tmp/fqn" + strconv.Itoa(i))
	}

	time.Sleep(time.Millisecond) // wait for runner to process

	n := atimer.getNumberItemsToFlush(fs)
	if n != itemCount/4 {
		t.Error("when idle we should flush 25% of the cache")
	}

	atimer.stop(fmt.Errorf("test"))
	getiostatrunner().chsts <- struct{}{}
}

func TestAtimerunnerGetNumberItemsToFlushVeryHighWatermark(t *testing.T) {
	itemCount := atimeCacheFlushThreshold * 2
	ctx.config.LRU.AtimeCacheMax = uint64(itemCount)
	ctx.config.LRU.LRUEnabled = true
	ctx.config.Periodic.StatsTime = 1 * time.Second
	ctx.rg = &rungroup{
		runarr: make([]runner, 0, 1),
		runmap: make(map[string]runner),
	}

	ctx.mountpaths.Available = make(map[string]*fs.MountpathInfo, len(ctx.config.FSpaths))
	tempFile, fs := getTempFile(t, "1")
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	iostatr := newIostatRunner()
	ctx.rg.add(iostatr, xiostat)
	go ctx.rg.run()

	atimer := newAtimeRunner()
	go atimer.run()

	for i := 0; i < itemCount; i++ {
		atimer.touch("/tmp/fqn" + strconv.Itoa(i))
	}

	time.Sleep(time.Millisecond) // wait for runner to process

	// simulate highly utilized disk
	iostatr.Disk = make(map[string]simplekvs)
	disks := fs2disks(fs)
	for disk := range disks {
		iostatr.Disk[disk] = make(simplekvs, 0)
		iostatr.Disk[disk]["%util"] = "99.94"
	}
	n := atimer.getNumberItemsToFlush(fs)
	if n != itemCount/2 {
		t.Error("when filling is 100% we should flush 50% of the cache")
	}

	atimer.stop(fmt.Errorf("test"))
	getiostatrunner().chsts <- struct{}{}
}

func TestAtimerunnerGetNumberItemsToFlushHighWatermark(t *testing.T) {
	itemCount := atimeCacheFlushThreshold * 2
	ctx.config.LRU.AtimeCacheMax = uint64(itemCount*(200-atimeHWM)/100) + 10
	ctx.config.LRU.LRUEnabled = true
	ctx.config.Periodic.StatsTime = 1 * time.Second
	ctx.rg = &rungroup{
		runarr: make([]runner, 0, 1),
		runmap: make(map[string]runner),
	}

	ctx.mountpaths.Available = make(map[string]*fs.MountpathInfo, len(ctx.config.FSpaths))
	tempFile, fs := getTempFile(t, "1")
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	iostatr := newIostatRunner()
	ctx.rg.add(iostatr, xiostat)
	go ctx.rg.run()

	atimer := newAtimeRunner()
	go atimer.run()

	for i := 0; i < itemCount; i++ {
		atimer.touch("/tmp/fqn" + strconv.Itoa(i))
	}

	time.Sleep(time.Millisecond) // wait for runner to process

	// simulate highly utilized disk
	iostatr.Disk = make(map[string]simplekvs)
	disks := fs2disks(fs)
	for disk := range disks {
		iostatr.Disk[disk] = make(simplekvs, 0)
		iostatr.Disk[disk]["%util"] = "99.94"
	}
	n := atimer.getNumberItemsToFlush(fs)
	if n != itemCount/4 {
		t.Error("when filling is above high watermark we should flush 25% of the cache")
	}

	atimer.stop(fmt.Errorf("test"))
	getiostatrunner().chsts <- struct{}{}
}

func TestAtimerunnerGetNumberItemsToFlushLowWatermark(t *testing.T) {
	itemCount := atimeCacheFlushThreshold * 2

	ctx.config.LRU.AtimeCacheMax = uint64(itemCount*(200-atimeLWM)/100) + 10
	ctx.config.LRU.LRUEnabled = true
	ctx.config.Periodic.StatsTime = 1 * time.Second
	ctx.rg = &rungroup{
		runarr: make([]runner, 0, 1),
		runmap: make(map[string]runner),
	}

	ctx.mountpaths.Available = make(map[string]*fs.MountpathInfo, len(ctx.config.FSpaths))
	tempFile, fs := getTempFile(t, "1")
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	iostatr := newIostatRunner()
	ctx.rg.add(iostatr, xiostat)
	go ctx.rg.run()

	atimer := newAtimeRunner()
	go atimer.run()

	for i := 0; i < itemCount; i++ {
		atimer.touch("/tmp/fqn" + strconv.Itoa(i))
	}

	time.Sleep(time.Millisecond) // wait for runner to process

	// simulate highly utilized disk
	iostatr.Disk = make(map[string]simplekvs)
	disks := fs2disks(fs)
	for disk := range disks {
		iostatr.Disk[disk] = make(simplekvs, 0)
		iostatr.Disk[disk]["%util"] = "99.94"
	}
	n := atimer.getNumberItemsToFlush(fs)
	if n == 0 {
		t.Error("when filling is above low watermark we should flush some of the cache")
	}

	atimer.stop(fmt.Errorf("test"))
	getiostatrunner().chsts <- struct{}{}
}

func TestAtimerunnerGetNumberItemsToFlushLowFilling(t *testing.T) {
	itemCount := atimeCacheFlushThreshold * 2

	ctx.config.LRU.AtimeCacheMax = uint64(itemCount * 1000)
	ctx.config.LRU.LRUEnabled = true
	ctx.config.Periodic.StatsTime = 1 * time.Second
	ctx.rg = &rungroup{
		runarr: make([]runner, 0, 1),
		runmap: make(map[string]runner),
	}

	ctx.mountpaths.Available = make(map[string]*fs.MountpathInfo, len(ctx.config.FSpaths))
	tempFile, fs := getTempFile(t, "1")
	defer func() {
		tempFile.Close()
		os.Remove(tempFile.Name())
	}()

	iostatr := newIostatRunner()

	ctx.rg.add(iostatr, xiostat)
	go ctx.rg.run()

	atimer := newAtimeRunner()
	go atimer.run()

	for i := 0; i < itemCount; i++ {
		atimer.touch("/tmp/fqn" + strconv.Itoa(i))
	}

	time.Sleep(time.Millisecond) // wait for runner to process

	// simulate highly utilized disk
	iostatr.Disk = make(map[string]simplekvs)
	disks := fs2disks(fs)
	for disk := range disks {
		iostatr.Disk[disk] = make(simplekvs, 0)
		iostatr.Disk[disk]["%util"] = "99.34"
	}
	n := atimer.getNumberItemsToFlush(fs)
	if n != 0 {
		t.Error("when filling is low and disk is busy we should not flush at all")
	}

	atimer.stop(fmt.Errorf("test"))
	getiostatrunner().chsts <- struct{}{}
}

func getTempFile(t *testing.T, prefix string) (*os.File, string) {
	tempFile, err := ioutil.TempFile("", "fqn"+prefix)
	if err != nil {
		t.Fatalf("Unable to create temp file.")
	}

	fileSystem, _ := fs.Fqn2fsAtStartup(tempFile.Name())
	tempRoot := "/tmp"
	ctx.mountpaths.AddMountpath(tempRoot)

	return tempFile, fileSystem
}

func newAtimeRunner() *atimerunner {
	atimer := &atimerunner{
		chfqn:  make(chan string),
		chstop: make(chan struct{}),
		atimemap: &atimemap{
			fsToFilesMap: make(map[string]map[string]time.Time),
		},
		chGetAtime:  make(chan string, 1),
		chSendAtime: make(chan accessTimeResponse, 1),
	}

	return atimer
}
