/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/dfc"
	"github.com/NVIDIA/dfcpub/pkg/client"
)

const (
	fshcDetectTimeMax = time.Second * 5
	fshcRunTimeMax    = time.Second * 20
	fshcDir           = "fschecker"
)

func waitForMountpathChanges(t *testing.T, target string, availLen, disabledLen int, failIfDiffer bool) bool {
	// wait for target disables the failed filesystem
	var err error
	detectStart := time.Now()
	detectLimit := time.Now().Add(fshcDetectTimeMax)
	var newMpaths *dfc.MountpathList
	for detectLimit.After(time.Now()) {
		newMpaths, err = client.TargetMountpaths(target)
		if err != nil {
			t.Errorf("Failed to read target mountpaths: %v\n", err)
			break
		}
		if len(newMpaths.Disabled) == disabledLen {
			break
		}
		time.Sleep(time.Millisecond * 100)
	}
	detectTime := time.Since(detectStart)
	tlogf("passed %v\n", detectTime)

	if len(newMpaths.Disabled) == disabledLen &&
		len(newMpaths.Available) == availLen {
		tlogf("Check is successful in %v\n", detectTime)
		return true
	}

	if !failIfDiffer {
		return false
	}

	if len(newMpaths.Disabled) != disabledLen {
		t.Errorf("Failed mpath count mismatch.\nOld count: %v\nNew list:%v\n",
			disabledLen, newMpaths.Disabled)
	} else if len(newMpaths.Available) != availLen {
		t.Errorf("Available mpath count mismatch.\nOld count: %v\nNew list:%v\n",
			availLen, newMpaths.Available)
	}
	return false
}

func repairMountpath(t *testing.T, target, mpath string, availLen, disabledLen int) {
	var err error
	// cleanup
	// restore original mountpath
	os.Remove(mpath)
	dfc.CreateDir(mpath)

	// ask fschecker to check all mountpath - it should make disabled
	// mountpath back to available list
	client.EnableTargetMountpath(target, mpath)
	tlogf("Recheck mountpaths\n")
	detectStart := time.Now()
	detectLimit := time.Now().Add(fshcDetectTimeMax)
	var mpaths *dfc.MountpathList
	// Wait for fsckeeper detects that the mountpath is accessible now
	for detectLimit.After(time.Now()) {
		mpaths, err = client.TargetMountpaths(target)
		if err != nil {
			t.Errorf("Failed to read target mountpaths: %v\n", err)
			break
		}
		if len(mpaths.Disabled) == disabledLen && len(mpaths.Available) == availLen {
			break
		}
		time.Sleep(time.Second)
	}

	// final test checks - available and disabled lists must equal list
	// before starting the test
	if len(mpaths.Disabled) != disabledLen {
		t.Errorf("Failed mountpath is still disabled in %v\nExpected disabled count: %d\nNew list:%v\n",
			time.Since(detectStart), disabledLen, mpaths.Disabled)
	} else if len(mpaths.Available) != availLen {
		t.Errorf("Failed mountpath is not back in %v.\nExpected available count: %d\nNew list:%v\n",
			time.Since(detectStart), availLen, mpaths.Available)
	}
}

func runAsyncJob(t *testing.T, wg *sync.WaitGroup, op, mpath string, filelist []string, chfail, chstop chan struct{}, sgl *dfc.SGLIO) {
	var (
		seed     = baseseed + 300
		filesize = uint64(64 * 1024)
		ldir     = LocalSrcDir + "/" + fshcDir
	)
	tlogf("Testing mpath fail detection on %s\n", op)
	stopTime := time.Now().Add(fshcRunTimeMax)

	for stopTime.After(time.Now()) {
		errch := make(chan error, len(filelist))
		filesput := make(chan string, len(filelist))

		for _, fname := range filelist {
			select {
			case <-chfail:
				// simulating mountpath death requested
				// It is the easiest way to simulate: stop putting data and
				// replace the mountpath with regular file. If we do not stop
				// putting objects it recreates the mountpath and does not fail
				os.RemoveAll(mpath)
				f, err := os.OpenFile(mpath, os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					t.Errorf("Failed to create file: %v", err)
				}
				f.Close()
			case <-chstop:
				wg.Done()
				return
			default:
				// do nothing and just start the next loop
			}

			switch op {
			case "PUT":
				fileList := []string{fname}
				fillWithRandomData(seed, filesize, fileList, clibucket, t, errch, filesput, ldir, fshcDir, true, sgl)
				select {
				case <-errch:
					// do nothing
				default:
				}
			case "GET":
				_, _, _ = client.Get(proxyurl, clibucket, fshcDir+"/"+fname, nil, nil, true, false)
				time.Sleep(time.Millisecond * 10)
			default:
				t.Errorf("Invalid operation: %s", op)
			}
		}

		close(errch)
		close(filesput)
	}

	wg.Done()
}

func TestFSCheckerDetection(t *testing.T) {
	var (
		err      error
		sgl      *dfc.SGLIO
		seed     = baseseed + 300
		numObjs  = 100
		filesize = uint64(64 * 1024)
	)

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	if isCloudBucket(t, proxyurl, clibucket) {
		t.Skip("test is for local buckets only")
	}

	smap, err := client.GetClusterMap(proxyurl)
	checkFatal(err, t)

	mpList := make(map[string]string, 0)
	allMps := make(map[string]*dfc.MountpathList, 0)
	origAvail := 0
	for target, tinfo := range smap.Tmap {
		tlogf("Target: %s\n", target)
		lst, err := client.TargetMountpaths(tinfo.DirectURL)
		checkFatal(err, t)
		tlogf("    Mountpaths: %v\n", lst)

		for _, fqn := range lst.Available {
			mpList[fqn] = tinfo.DirectURL
		}
		allMps[tinfo.DirectURL] = lst

		origAvail += len(lst.Available)
	}

	if origAvail == 0 {
		t.Fatal("No available mountpaths found")
	}

	// select random target and mountpath
	failedTarget, failedMpath := "", ""
	var failedMap *dfc.MountpathList
	for m, t := range mpList {
		failedTarget, failedMpath = t, m
		failedMap = allMps[failedTarget]
		break
	}

	// create a local bucket to write to
	tlogf("Mpath %s of %s is going offline\n", failedMpath, failedTarget)
	_ = createLocalBucketIfNotExists(t, proxyurl, clibucket)

	defer func() {
		err = client.DestroyLocalBucket(proxyurl, clibucket)
		checkFatal(err, t)
	}()

	if usingSG {
		sgl = dfc.NewSGLIO(filesize)
		defer sgl.Free()
	}

	// generate some filenames to PUT to them in a loop
	generateRandomData(t, seed, numObjs)

	// start PUTting in a loop for some time
	chstop := make(chan struct{})
	chfail := make(chan struct{})
	wg := &sync.WaitGroup{}

	// Checking detection on object PUT
	{
		wg.Add(1)
		go runAsyncJob(t, wg, "PUT", failedMpath, fileNames, chfail, chstop, sgl)
		time.Sleep(time.Second * 2)
		chfail <- struct{}{}
		if detected := waitForMountpathChanges(t, failedTarget, len(failedMap.Available)-1, len(failedMap.Disabled)+1, true); detected {
			time.Sleep(time.Second * 2)
		}
		chstop <- struct{}{}
		wg.Wait()

		repairMountpath(t, failedTarget, failedMpath, len(failedMap.Available), len(failedMap.Disabled))
	}

	// Checking detection on object GET
	{
		wg.Add(1)
		go runAsyncJob(t, wg, "GET", failedMpath, fileNames, chfail, chstop, sgl)
		time.Sleep(time.Second * 2)
		chfail <- struct{}{}
		if detected := waitForMountpathChanges(t, failedTarget, len(failedMap.Available)-1, len(failedMap.Disabled)+1, true); detected {
			time.Sleep(time.Second * 1)
		}
		chstop <- struct{}{}
		wg.Wait()

		repairMountpath(t, failedTarget, failedMpath, len(failedMap.Available), len(failedMap.Disabled))
	}

	// reading non-existing objects should not disable mountpath
	{
		tlogf("Reading non-existing objects: read must fails, but mpath must be available\n")
		for n := 1; n < 10; n++ {
			_, _, err = client.Get(proxyurl, clibucket, fmt.Sprintf("%s/%d", fshcDir, n), nil, nil, true, false)
		}
		if detected := waitForMountpathChanges(t, failedTarget, len(failedMap.Available)-1, len(failedMap.Disabled)+1, false); detected {
			t.Error("GETting non-existing objects should not disable mountpath")
			repairMountpath(t, failedTarget, failedMpath, len(failedMap.Available), len(failedMap.Disabled))
		}
	}

	// try PUT and GET with disabled FSChecker
	tlogf("*** Testing with disabled FSHC***\n")
	setConfig("fschecker_enabled", fmt.Sprint("false"), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	defer setConfig("fschecker_enabled", fmt.Sprint("true"), proxyurl+"/"+dfc.Rversion+"/"+dfc.Rcluster, httpclient, t)
	{
		wg.Add(1)
		go runAsyncJob(t, wg, "PUT", failedMpath, fileNames, chfail, chstop, sgl)
		time.Sleep(time.Second * 2)
		chfail <- struct{}{}
		if detected := waitForMountpathChanges(t, failedTarget, len(failedMap.Available)-1, len(failedMap.Disabled)+1, false); detected {
			t.Error("PUTting objects to a broken mountpath should not disable the mountpath when FSHC is disabled")
		}
		chstop <- struct{}{}
		wg.Wait()

		repairMountpath(t, failedTarget, failedMpath, len(failedMap.Available), len(failedMap.Disabled))
	}
	{
		wg.Add(1)
		go runAsyncJob(t, wg, "GET", failedMpath, fileNames, chfail, chstop, sgl)
		time.Sleep(time.Second * 2)
		chfail <- struct{}{}
		if detected := waitForMountpathChanges(t, failedTarget, len(failedMap.Available)-1, len(failedMap.Disabled)+1, false); detected {
			t.Error("GETting objects from a broken mountpath should not disable the mountpath when FSHC is disabled")
		}
		chstop <- struct{}{}
		wg.Wait()

		repairMountpath(t, failedTarget, failedMpath, len(failedMap.Available), len(failedMap.Disabled))
	}

}

func TestFSCheckerEnablingMpath(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	if isCloudBucket(t, proxyurl, clibucket) {
		t.Skip("test is for local buckets only")
	}

	smap, err := client.GetClusterMap(proxyurl)
	checkFatal(err, t)

	mpList := make(map[string]string, 0)
	allMps := make(map[string]*dfc.MountpathList, 0)
	origAvail := 0
	origOff := 0
	for target, tinfo := range smap.Tmap {
		tlogf("Target: %s\n", target)
		lst, err := client.TargetMountpaths(tinfo.DirectURL)
		checkFatal(err, t)
		tlogf("    Mountpaths: %v\n", lst)

		for _, fqn := range lst.Available {
			mpList[fqn] = tinfo.DirectURL
		}
		allMps[tinfo.DirectURL] = lst

		origAvail += len(lst.Available)
		origOff += len(lst.Disabled)
	}

	if origAvail == 0 {
		t.Fatal("No available mountpaths found")
	}

	// select random target and mountpath
	failedTarget, failedMpath := "", ""
	for m, t := range mpList {
		failedTarget, failedMpath = t, m
		break
	}

	// create a local bucket to write to
	tlogf("Mpath %s of %s is going offline\n", failedMpath, failedTarget)

	err = client.EnableTargetMountpath(failedTarget, failedMpath)
	if err != nil {
		t.Errorf("Enabling available mountpath should return success, got: %v", err)
	}

	err = client.EnableTargetMountpath(failedTarget, failedMpath+"some_text")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("Enabling non-existing mountpath should return not-found error, got: %v", err)
	}
}
