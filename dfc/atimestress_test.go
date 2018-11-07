/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc

//================================= How to Run ========================
// 1.To run the test for a specified duration while redirecting errors to stderror:
//        go test -v -logtostderr=true -duration 2m -run=Test_AtimeReadWriteStress
// NOTE: If the duration flag is not present, the default will be 30s.
//
// 2. To simulate flushing mechanincs that occur in atime.go, set the flushFreq flag. By default,
// flushing occurs ever 3 minutes (thus the duration flag must be set to greater than 3 minutes to
// see the effects of flushing). This will simulate a very high watermark by default (99.94)
//        go test -v -logtostderr=true -duration 5m -flushFreq 180s -run=Test_AtimeReadWriteStress
//
// 3. To simulate different disk utilizations while flushing, set the diskUtil flag.
//        go test -v -logtostderr=true -duration 5m -flushFreq 180s -diskUtil 99.94 -run=Test_AtimeReadWriteStress

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/fs"
	"github.com/NVIDIA/dfcpub/tutils"
)

var (
	duration  time.Duration // test duration
	flushFreq time.Duration // how often atimerunner flushes mpaths
	diskUtil  string        // disk utility percentage
	verbose   bool
)

func init() {
	var (
		d   string
		f   string
		err error
	)
	flag.StringVar(&d, "duration", "30s", "test duration")
	flag.StringVar(&f, "flushFreq", "180s", "test atime flush frequency")
	flag.StringVar(&diskUtil, "diskUtil", "99.94", "test disk util as percentage")
	flag.Parse()

	if duration, err = time.ParseDuration(d); err != nil {
		fmt.Printf("Invalid duration '%s'\n", d)
		os.Exit(1)
	}

	if flushFreq, err = time.ParseDuration(f); err != nil {
		fmt.Printf("Invalid duration '%s'\n", d)
		os.Exit(1)
	}

	atimeSyncTime = flushFreq
	ctx.config.CloudBuckets = "cloud"
	ctx.config.LocalBuckets = "local"
	ctx.config.LRU.AtimeCacheMax = atimeCacheFlushThreshold * 2
	ctx.config.Periodic.StatsTime = time.Second
}

// used to insert new files into the mapth's corresponding atimerunner
func touchFakeFiles(r *atimerunner, mpath, fqn1, fqn2 string, numFiles int) {
	start := time.Now()
	for j := 0; j < numFiles/2; j++ {
		numStr := strconv.Itoa(j)
		r.touch(fqn1 + numStr)
		r.touch(fqn2 + numStr)
	}
	tutils.Logf("%v to touch %d files in %s\n", time.Since(start), numFiles, mpath)
}

func touchRandomFiles(r *atimerunner, mpath, fqn1, fqn2 string, numFiles int, duration time.Duration) {
	numTouches := 0
	fileRange := numFiles / 2
	for start := time.Now(); time.Since(start) < duration; {
		numStr := strconv.Itoa(rand.Intn(fileRange))
		r.touch(fqn1 + numStr)
		r.touch(fqn2 + numStr)
		numTouches += 2
	}
	tutils.Logf("Mpath: %q. Touched %d files.\n", mpath, numTouches)
}

func atimeRandomFiles(r *atimerunner, mpath, fqn1, fqn2 string, numFiles int, duration time.Duration) {
	numOk := 0
	numAccesses := 0
	fileRange := numFiles / 2
	for start := time.Now(); time.Since(start) < duration; {
		randomInt := rand.Intn(fileRange)
		numStr := strconv.Itoa(randomInt)
		if randomInt%2 == 0 {
			response := <-r.atime(fqn1 + numStr)
			if response.ok {
				numOk++
			}
		} else {
			response := <-r.atime(fqn2 + numStr)
			if response.ok {
				numOk++
			}
		}
		numAccesses++
	}
	tutils.Logf("Mpath: %q. Successfully accessed %d files. Total access atempts %d\n", mpath, numOk, numAccesses)
}

func cleanDirectories(dir string) {
	os.RemoveAll(dir)
}

func Test_AtimeReadWriteStress(t *testing.T) {
	tmpDir := "/tmp/atime"
	mpaths := []string{tmpDir + "/A", tmpDir + "/B", tmpDir + "/C", tmpDir + "/D", tmpDir + "/E", tmpDir + "/F", tmpDir + "/G", tmpDir + "/H", tmpDir + "/I", tmpDir + "/J"}
	nonExistingMpaths := []string{tmpDir + "/Z"}
	allMpaths := append(mpaths, nonExistingMpaths...)
	fileName := "/local/bck1/fqn"
	fileName2 := "/cloud/bck2/fqn"

	numFiles := 100000
	numFilesTotal := 1000000

	ctx.rg = &rungroup{
		runarr: make([]cmn.Runner, 0, 1),
		runmap: make(map[string]cmn.Runner),
	}
	iostatr := newIostatRunner()
	ctx.rg.add(iostatr, xiostat)

	target := newFakeTargetRunner()
	fs.Mountpaths = fs.NewMountedFS(ctx.config.LocalBuckets, ctx.config.CloudBuckets)
	fs.Mountpaths.DisableFsIDCheck()
	cleanMountpaths()
	for _, mpath := range mpaths {
		cmn.CreateDir(mpath)
		fs.Mountpaths.AddMountpath(mpath)
	}

	atimer := newAtimeRunner(target, fs.Mountpaths)
	ctx.rg.add(atimer, xatime)

	go atimer.Run()
	for _, mpath := range mpaths {
		atimer.reqAddMountpath(mpath)
	}
	time.Sleep(100 * time.Millisecond)
	if len(atimer.mpathRunners) != len(mpaths) {
		t.Error(fmt.Errorf("There must be %d mpathAtimeRunners, one for each mpath", len(mpaths)))
	}
	wg := &sync.WaitGroup{}
	start := time.Now()
	// add 100,000 files per mountpath by touching
	for _, mpath := range mpaths {
		wg.Add(1)
		go func(mpath string) {
			touchFakeFiles(atimer, mpath, mpath+fileName, mpath+fileName2, numFiles)
			wg.Done()
		}(mpath)
	}

	wg.Wait()

	tutils.Logf("%v to touch %d files.\n", time.Since(start), numFilesTotal)

	// simulate highly utilized disk
	iostatr.Disk = make(map[string]cmn.SimpleKVs)
	mpathInfo, _ := path2mpathInfo(mpaths[0])
	disks := fs2disks(mpathInfo.FileSystem)
	for disk := range disks {
		iostatr.Disk[disk] = make(cmn.SimpleKVs, 0)
		iostatr.Disk[disk]["%util"] = diskUtil
	}

	// start 22 go routines. 2 of them just create useless noise
	for _, mpath := range allMpaths {
		wg.Add(2)
		go func(mpath string) {
			touchRandomFiles(atimer, mpath, mpath+fileName, mpath+fileName2, numFiles, duration)
			wg.Done()
		}(mpath)
		go func(mpath string) {
			atimeRandomFiles(atimer, mpath, mpath+fileName, mpath+fileName2, numFiles, duration)
			wg.Done()
		}(mpath)
	}

	wg.Wait()
	cleanDirectories(tmpDir)
	atimer.Stop(fmt.Errorf("Test Complete"))
}
