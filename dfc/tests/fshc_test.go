/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 *
 */
package dfc_test

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/dfcpub/cmn"
	"github.com/NVIDIA/dfcpub/memsys"
	"github.com/NVIDIA/dfcpub/tutils"
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
	var newMpaths *cmn.MountpathList
	for detectLimit.After(time.Now()) {
		newMpaths, err = tutils.TargetMountpaths(target)
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
	tutils.Logf("passed %v\n", detectTime)

	if len(newMpaths.Disabled) == disabledLen &&
		len(newMpaths.Available) == availLen {
		tutils.Logf("Check is successful in %v\n", detectTime)
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
	cmn.CreateDir(mpath)

	// ask fschecker to check all mountpath - it should make disabled
	// mountpath back to available list
	tutils.EnableTargetMountpath(target, mpath)
	tutils.Logf("Recheck mountpaths\n")
	detectStart := time.Now()
	detectLimit := time.Now().Add(fshcDetectTimeMax)
	var mpaths *cmn.MountpathList
	// Wait for fsckeeper detects that the mountpath is accessible now
	for detectLimit.After(time.Now()) {
		mpaths, err = tutils.TargetMountpaths(target)
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

func runAsyncJob(t *testing.T, wg *sync.WaitGroup, op, mpath string, filelist []string, chfail,
	chstop chan struct{}, sgl *memsys.SGL, bucket string) {
	const filesize = 64 * 1024
	var (
		seed     = baseseed + 300
		ldir     = LocalSrcDir + "/" + fshcDir
		proxyURL = getPrimaryURL(t, proxyURLRO)
	)
	tutils.Logf("Testing mpath fail detection on %s\n", op)
	stopTime := time.Now().Add(fshcRunTimeMax)

	for stopTime.After(time.Now()) {
		errCh := make(chan error, len(filelist))
		filesPutCh := make(chan string, len(filelist))

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
				putRandObjsFromList(proxyURL, seed, filesize, fileList, bucket, errCh, filesPutCh, ldir, fshcDir, true, sgl)
				select {
				case <-errCh:
					// do nothing
				default:
				}
			case "GET":
				_, _, _ = tutils.Get(proxyURL, bucket, fshcDir+"/"+fname, nil, nil, true, false)
				time.Sleep(time.Millisecond * 10)
			default:
				t.Errorf("Invalid operation: %s", op)
			}
		}

		close(errCh)
		close(filesPutCh)
	}

	wg.Done()
}

func TestFSCheckerDetection(t *testing.T) {
	const filesize = 64 * 1024
	var (
		err      error
		sgl      *memsys.SGL
		seed     = baseseed + 300
		numObjs  = 100
		proxyURL = getPrimaryURL(t, proxyURLRO)
		bucket   = TestLocalBucketName
	)

	if testing.Short() {
		t.Skip(skipping)
	}

	createFreshLocalBucket(t, proxyURL, bucket)
	defer destroyLocalBucket(t, proxyURL, bucket)

	smap, err := tutils.GetClusterMap(proxyURL)
	tutils.CheckFatal(err, t)

	mpList := make(map[string]string, 0)
	allMps := make(map[string]*cmn.MountpathList, 0)
	origAvail := 0
	for target, tinfo := range smap.Tmap {
		tutils.Logf("Target: %s\n", target)
		lst, err := tutils.TargetMountpaths(tinfo.PublicNet.DirectURL)
		tutils.CheckFatal(err, t)
		tutils.Logf("    Mountpaths: %v\n", lst)

		for _, fqn := range lst.Available {
			mpList[fqn] = tinfo.PublicNet.DirectURL
		}
		allMps[tinfo.PublicNet.DirectURL] = lst

		origAvail += len(lst.Available)
	}

	if origAvail == 0 {
		t.Fatal("No available mountpaths found")
	}

	// select random target and mountpath
	failedTarget, failedMpath := "", ""
	var failedMap *cmn.MountpathList
	for m, t := range mpList {
		failedTarget, failedMpath = t, m
		failedMap = allMps[failedTarget]
		break
	}
	tutils.Logf("mountpath %s of %s is going offline\n", failedMpath, failedTarget)

	if usingSG {
		sgl = tutils.Mem2.NewSGL(filesize)
		defer sgl.Free()
	}

	// generate some filenames to PUT to them in a loop
	generateRandomData(t, seed, numObjs)

	// start PUT in a loop for some time
	chstop := make(chan struct{})
	chfail := make(chan struct{})
	wg := &sync.WaitGroup{}

	// Checking detection on object PUT
	{
		wg.Add(1)
		go runAsyncJob(t, wg, http.MethodPut, failedMpath, fileNames, chfail, chstop, sgl, bucket)
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
		go runAsyncJob(t, wg, http.MethodGet, failedMpath, fileNames, chfail, chstop, sgl, bucket)
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
		tutils.Logf("Reading non-existing objects: read is expected to fail but mountpath must be available\n")
		for n := 1; n < 10; n++ {
			_, _, err = tutils.Get(proxyURL, bucket, fmt.Sprintf("%s/%d", fshcDir, n), nil, nil, true, false)
		}
		if detected := waitForMountpathChanges(t, failedTarget, len(failedMap.Available)-1, len(failedMap.Disabled)+1, false); detected {
			t.Error("GETting non-existing objects should not disable mountpath")
			repairMountpath(t, failedTarget, failedMpath, len(failedMap.Available), len(failedMap.Disabled))
		}
	}

	// try PUT and GET with disabled FSChecker
	tutils.Logf("*** Testing with disabled FSHC***\n")
	setConfig("fschecker_enabled", fmt.Sprint("false"), proxyURL+cmn.URLPath(cmn.Version, cmn.Cluster), t)
	defer setConfig("fschecker_enabled", fmt.Sprint("true"), proxyURL+cmn.URLPath(cmn.Version, cmn.Cluster), t)
	// generate a short list of file to run the test (to avoid flooding the log with false errors)
	fileList := []string{}
	for n := 0; n < 5; n++ {
		fileList = append(fileList, fileNames[n])
	}
	ldir := LocalSrcDir + "/" + fshcDir
	{
		os.RemoveAll(failedMpath)

		f, err := os.OpenFile(failedMpath, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			t.Errorf("Failed to create file: %v", err)
		}
		f.Close()
		filesPutCh := make(chan string, len(fileList))
		errCh := make(chan error, len(fileList))
		putRandObjsFromList(proxyURL, seed, filesize, fileList, bucket, errCh, filesPutCh, ldir, fshcDir, true, sgl)
		selectErr(errCh, "put", t, false)
		close(filesPutCh)
		close(errCh)
		if detected := waitForMountpathChanges(t, failedTarget, len(failedMap.Available)-1, len(failedMap.Disabled)+1, false); detected {
			t.Error("PUT objects to a broken mountpath should not disable the mountpath when FSHC is disabled")
		}

		repairMountpath(t, failedTarget, failedMpath, len(failedMap.Available), len(failedMap.Disabled))
	}
	{
		os.RemoveAll(failedMpath)

		f, err := os.OpenFile(failedMpath, os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			t.Errorf("Failed to create file: %v", err)
		}
		f.Close()
		for _, n := range fileList {
			_, _, err = tutils.Get(proxyURL, bucket, n, nil, nil, true, false)
		}
		if detected := waitForMountpathChanges(t, failedTarget, len(failedMap.Available)-1, len(failedMap.Disabled)+1, false); detected {
			t.Error("GETting objects from a broken mountpath should not disable the mountpath when FSHC is disabled")
		}

		repairMountpath(t, failedTarget, failedMpath, len(failedMap.Available), len(failedMap.Disabled))
	}

}

func TestFSCheckerEnablingMpath(t *testing.T) {
	if testing.Short() {
		t.Skip(skipping)
	}

	proxyURL := getPrimaryURL(t, proxyURLRO)
	bucket := clibucket
	if isCloudBucket(t, proxyURL, bucket) {
		bucket = TestLocalBucketName
	}

	smap, err := tutils.GetClusterMap(proxyURL)
	tutils.CheckFatal(err, t)

	mpList := make(map[string]string, 0)
	allMps := make(map[string]*cmn.MountpathList, 0)
	origAvail := 0
	origOff := 0
	for target, tinfo := range smap.Tmap {
		tutils.Logf("Target: %s\n", target)
		lst, err := tutils.TargetMountpaths(tinfo.PublicNet.DirectURL)
		tutils.CheckFatal(err, t)
		tutils.Logf("    Mountpaths: %v\n", lst)

		for _, fqn := range lst.Available {
			mpList[fqn] = tinfo.PublicNet.DirectURL
		}
		allMps[tinfo.PublicNet.DirectURL] = lst

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
	tutils.Logf("mountpath %s of %s is going offline\n", failedMpath, failedTarget)

	err = tutils.EnableTargetMountpath(failedTarget, failedMpath)
	if err != nil {
		t.Errorf("Enabling available mountpath should return success, got: %v", err)
	}

	err = tutils.EnableTargetMountpath(failedTarget, failedMpath+"some_text")
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Errorf("Enabling non-existing mountpath should return not-found error, got: %v", err)
	}
}

func TestFSCheckerTargetDisable(t *testing.T) {
	proxyURL := getPrimaryURL(t, proxyURLRO)
	smap, err := tutils.GetClusterMap(proxyURL)
	tutils.CheckFatal(err, t)

	proxyCnt := len(smap.Pmap)
	targetCnt := len(smap.Tmap)
	if targetCnt < 2 {
		t.Skip("The number of targets must be at least 2")
	}

	tgtURL := ""
	for _, tinfo := range smap.Tmap {
		tgtURL = tinfo.PublicNet.DirectURL
		break
	}

	oldMpaths, err := tutils.TargetMountpaths(tgtURL)
	tutils.CheckFatal(err, t)
	if len(oldMpaths.Available) == 0 {
		t.Fatalf("Target %s does not have availalble mountpaths", tgtURL)
	}

	tutils.Logf("Removing all mountpaths from target: %s\n", tgtURL)
	for _, mpath := range oldMpaths.Available {
		err = tutils.DisableTargetMountpath(tgtURL, mpath)
		tutils.CheckFatal(err, t)
	}

	smap, err = waitForPrimaryProxy(proxyURL, "all mpath disabled", smap.Version, false, proxyCnt, targetCnt-1)
	tutils.CheckFatal(err, t)

	tutils.Logf("Restoring target %s mountpaths\n", tgtURL)
	for _, mpath := range oldMpaths.Available {
		err = tutils.EnableTargetMountpath(tgtURL, mpath)
		tutils.CheckFatal(err, t)
	}

	smap, err = waitForPrimaryProxy(proxyURL, "all mpath enabled", smap.Version, false, proxyCnt, targetCnt)
	tutils.CheckFatal(err, t)
}
