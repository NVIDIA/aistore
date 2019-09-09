// Package ais_test contains AIS integration tests.
/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */
package ais_test

import (
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/containers"

	"github.com/NVIDIA/aistore/tutils/tassert"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/memsys"
	"github.com/NVIDIA/aistore/tutils"
)

const (
	fshcDetectTimeMax      = time.Second * 20
	fshcDetectTimeDisabled = time.Second * 10
	fshcRunTimeMax         = time.Second * 20
	fshcDir                = "fschecker"
)

type checkerMD struct {
	t          *testing.T
	seed       int64
	numObjs    int
	proxyURL   string
	bucket     string
	smap       *cluster.Smap
	mpList     map[string]*cluster.Snode
	allMps     map[*cluster.Snode]*cmn.MountpathList
	origAvail  int
	fileSize   int64
	baseParams *api.BaseParams
	chstop     chan struct{}
	chfail     chan struct{}
	wg         *sync.WaitGroup
}

func newCheckerMD(t *testing.T) *checkerMD {
	md := &checkerMD{
		t:        t,
		seed:     baseseed + 300,
		numObjs:  100,
		proxyURL: getPrimaryURL(t, proxyURLReadOnly),
		bucket:   TestBucketName,
		fileSize: 64 * cmn.KiB,
		mpList:   make(map[string]*cluster.Snode, 10),
		allMps:   make(map[*cluster.Snode]*cmn.MountpathList, 10),
		chstop:   make(chan struct{}),
		chfail:   make(chan struct{}),
		wg:       &sync.WaitGroup{},
	}

	md.init()

	return md
}

func (md *checkerMD) init() {
	md.baseParams = tutils.BaseAPIParams(md.proxyURL)
	md.smap = getClusterMap(md.t, md.proxyURL)

	for targetID, tinfo := range md.smap.Tmap {
		tutils.Logf("Target: %s\n", targetID)
		lst, err := api.GetMountpaths(md.baseParams, tinfo)
		tassert.CheckFatal(md.t, err)
		tutils.Logf("    Mountpaths: %v\n", lst)

		for _, fqn := range lst.Available {
			md.mpList[fqn] = tinfo
		}
		md.allMps[tinfo] = lst

		md.origAvail += len(lst.Available)
	}
}

func (md *checkerMD) randomTargetMpath() (target *cluster.Snode, mpath string, mpathMap *cmn.MountpathList) {
	// select random target and mountpath
	for m, t := range md.mpList {
		target, mpath = t, m
		mpathMap = md.allMps[target]
		break
	}
	return
}

func (md *checkerMD) runTestAsync(method string, target *cluster.Snode, mpath string, mpathList *cmn.MountpathList, sgl *memsys.SGL) {
	md.wg.Add(1)
	go runAsyncJob(md.t, md.wg, method, mpath, fileNames, md.chfail, md.chstop, sgl, md.bucket)
	// let the job run for a while and then make a mountpath broken
	time.Sleep(time.Second * 2)
	md.chfail <- struct{}{}
	if detected := waitForMountpathChanges(md.t, target, len(mpathList.Available)-1, len(mpathList.Disabled)+1, true); detected {
		// let the job run for a while with broken mountpath, so FSHC detects the trouble
		time.Sleep(time.Second * 2)
	}
	md.chstop <- struct{}{}
	md.wg.Wait()

	repairMountpath(md.t, target, mpath, len(mpathList.Available), len(mpathList.Disabled))
}

func (md *checkerMD) runTestSync(method string, target *cluster.Snode, mpath string, mpathList *cmn.MountpathList, objList []string, sgl *memsys.SGL) {
	ldir := filepath.Join(LocalSrcDir, fshcDir)
	os.RemoveAll(mpath)
	defer repairMountpath(md.t, target, mpath, len(mpathList.Available), len(mpathList.Disabled))

	f, err := cmn.CreateFile(mpath)
	if err != nil {
		md.t.Errorf("Failed to create file: %v", err)
		return
	}
	f.Close()

	switch method {
	case http.MethodPut:
		tutils.PutObjsFromList(md.proxyURL, md.bucket, ldir, readerType, fshcDir, uint64(md.fileSize), objList, nil, nil, sgl)
	case http.MethodGet:
		for _, objName := range objList {
			// GetObject must fail - so no error checking
			_, err := api.GetObject(md.baseParams, md.bucket, objName)
			if err == nil {
				md.t.Errorf("Get %q must fail", objName)
			}
		}
	}

	if detected := waitForMountpathChanges(md.t, target, len(mpathList.Available)-1, len(mpathList.Disabled)+1, false, fshcDetectTimeDisabled); detected {
		md.t.Error("PUT objects to a broken mountpath should not disable the mountpath when FSHC is disabled")
	}

}

func waitForMountpathChanges(t *testing.T, target *cluster.Snode, availLen, disabledLen int, failIfDiffer bool, timeout ...time.Duration) bool {
	var (
		err        error
		newMpaths  *cmn.MountpathList
		baseParams = tutils.DefaultBaseAPIParams(t)
	)

	maxWaitTime := fshcDetectTimeMax
	if len(timeout) != 0 && timeout[0] != 0 {
		maxWaitTime = timeout[0]
	}

	detectStart := time.Now()
	detectLimit := time.Now().Add(maxWaitTime)

	for detectLimit.After(time.Now()) {
		newMpaths, err = api.GetMountpaths(baseParams, target)
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

	tutils.Logf("Current mpath list: %v\n", newMpaths)
	if len(newMpaths.Disabled) != disabledLen {
		t.Errorf("Failed mpath count mismatch.\nOld count: %v\nNew list:%v\n",
			disabledLen, newMpaths.Disabled)
	} else if len(newMpaths.Available) != availLen {
		t.Errorf("Available mpath count mismatch.\nOld count: %v\nNew list:%v\n",
			availLen, newMpaths.Available)
	}
	return false
}

func repairMountpath(t *testing.T, target *cluster.Snode, mpath string, availLen, disabledLen int) {
	var (
		err        error
		baseParams = tutils.DefaultBaseAPIParams(t)
	)
	// cleanup
	// restore original mountpath
	os.Remove(mpath)
	cmn.CreateDir(mpath)

	// ask fschecker to check all mountpath - it should make disabled
	// mountpath back to available list
	api.EnableMountpath(baseParams, target, mpath)
	tutils.Logf("Recheck mountpaths\n")
	detectStart := time.Now()
	detectLimit := time.Now().Add(fshcDetectTimeMax)
	var mpaths *cmn.MountpathList
	// Wait for fsckeeper detects that the mountpath is accessible now
	for detectLimit.After(time.Now()) {
		mpaths, err = api.GetMountpaths(baseParams, target)
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

func runAsyncJob(t *testing.T, wg *sync.WaitGroup, op, mpath string, filelist []string, chfail, chstop chan struct{}, sgl *memsys.SGL, bucket string) {
	defer wg.Done()

	const fileSize = 64 * cmn.KiB
	var proxyURL = getPrimaryURL(t, proxyURLReadOnly)

	tutils.Logf("Testing mpath fail detection on %s\n", op)
	stopTime := time.Now().Add(fshcRunTimeMax)

	for stopTime.After(time.Now()) {
		errCh := make(chan error, len(filelist))
		objsPutCh := make(chan string, len(filelist))

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

				return
			default:
				// do nothing and just start the next loop
			}

			switch op {
			case "PUT":
				fileList := []string{fname}
				ldir := filepath.Join(LocalSrcDir, fshcDir)
				tutils.PutObjsFromList(proxyURL, bucket, ldir, readerType, fshcDir, fileSize, fileList, errCh, objsPutCh, sgl)
			case "GET":
				api.GetObject(tutils.DefaultBaseAPIParams(t), bucket, path.Join(fshcDir, fname))
				time.Sleep(time.Millisecond * 10)
			default:
				t.Errorf("Invalid operation: %s", op)
			}
		}

		close(errCh)
		close(objsPutCh)
	}
}

func TestFSCheckerDetectionEnabled(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	if containers.DockerRunning() {
		t.Skipf("%s requires direct filesystem access, doesn't work with docker", t.Name())
	}

	var (
		md = newCheckerMD(t)
	)

	if md.origAvail == 0 {
		t.Fatal("No available mountpaths found")
	}

	tutils.CreateFreshBucket(t, md.proxyURL, md.bucket)
	defer tutils.DestroyBucket(t, md.proxyURL, md.bucket)

	selectedTarget, selectedMpath, selectedMap := md.randomTargetMpath()
	tutils.Logf("mountpath %s of %s is selected for the test\n", selectedMpath, selectedTarget)

	sgl := tutils.Mem2.NewSGL(md.fileSize)
	defer sgl.Free()

	// generate some filenames to PUT to them in a loop
	generateRandomData(md.seed, md.numObjs)

	// Checking detection on object PUT
	md.runTestAsync(http.MethodPut, selectedTarget, selectedMpath, selectedMap, sgl)
	// Checking detection on object GET
	md.runTestAsync(http.MethodGet, selectedTarget, selectedMpath, selectedMap, sgl)

	// Checking that reading "bad" objects does not disable mpath if the mpath is OK
	tutils.Logf("Reading non-existing objects: read is expected to fail but mountpath must be available\n")
	for n := 1; n < 10; n++ {
		objName := fmt.Sprintf("%s/o%d", fshcDir, n)
		if _, err := api.GetObject(md.baseParams, md.bucket, objName); err == nil {
			t.Error("Should not be able to GET non-existing objects")
		}
	}
	if detected := waitForMountpathChanges(t, selectedTarget, len(selectedMap.Available)-1, len(selectedMap.Disabled)+1, false); detected {
		t.Error("GETting non-existing objects should not disable mountpath")
		repairMountpath(t, selectedTarget, selectedMpath, len(selectedMap.Available), len(selectedMap.Disabled))
	}

	waitForRebalanceToComplete(t, tutils.BaseAPIParams(md.proxyURL), rebalanceTimeout)
}

func TestFSCheckerDetectionDisabled(t *testing.T) {
	if testing.Short() {
		t.Skip(tutils.SkipMsg)
	}

	if containers.DockerRunning() {
		t.Skipf("%s requires direct filesystem access, doesn't work with docker", t.Name())
	}

	md := newCheckerMD(t)

	if md.origAvail == 0 {
		t.Fatal("No available mountpaths found")
	}

	tutils.Logf("*** Testing with disabled FSHC***\n")
	setClusterConfig(t, proxyURL, cmn.SimpleKVs{"fshc_enabled": "false"})
	defer setClusterConfig(t, proxyURL, cmn.SimpleKVs{"fshc_enabled": "true"})

	tutils.CreateFreshBucket(t, md.proxyURL, md.bucket)
	defer tutils.DestroyBucket(t, md.proxyURL, md.bucket)

	selectedTarget, selectedMpath, selectedMap := md.randomTargetMpath()
	tutils.Logf("mountpath %s of %s is selected for the test\n", selectedMpath, selectedTarget)

	sgl := tutils.Mem2.NewSGL(md.fileSize)
	defer sgl.Free()

	// generate a short list of file to run the test (to avoid flooding the log with false errors)
	objList := []string{}
	for n := 0; n < 5; n++ {
		objName := fmt.Sprintf("obj-fshc-%d", n)
		objList = append(objList, objName)
	}

	// Checking detection on object PUT
	md.runTestSync(http.MethodPut, selectedTarget, selectedMpath, selectedMap, objList, sgl)
	// Checking detection on object GET
	md.runTestSync(http.MethodGet, selectedTarget, selectedMpath, selectedMap, objList, sgl)

	waitForRebalanceToComplete(t, tutils.BaseAPIParams(md.proxyURL), rebalanceTimeout)
}

func TestFSCheckerEnablingMpath(t *testing.T) {
	var (
		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		baseParams = tutils.DefaultBaseAPIParams(t)
		smap       = getClusterMap(t, proxyURL)
		mpList     = make(map[string]*cluster.Snode, 10)
		allMps     = make(map[*cluster.Snode]*cmn.MountpathList, 10)
		origAvail  = 0
		origOff    = 0
	)

	for targetID, tinfo := range smap.Tmap {
		tutils.Logf("Target: %s\n", targetID)
		lst, err := api.GetMountpaths(baseParams, tinfo)
		tassert.CheckFatal(t, err)
		tutils.Logf("    Mountpaths: %v\n", lst)

		for _, fqn := range lst.Available {
			mpList[fqn] = tinfo
		}
		allMps[tinfo] = lst

		origAvail += len(lst.Available)
		origOff += len(lst.Disabled)
	}

	if origAvail == 0 {
		t.Fatal("No available mountpaths found")
	}

	// select random target and mountpath
	var (
		selectedTarget *cluster.Snode
		selectedMpath  string
	)
	for m, t := range mpList {
		selectedTarget, selectedMpath = t, m
		break
	}

	// create an ais bucket to write to
	tutils.Logf("mountpath %s of %s is going offline\n", selectedMpath, selectedTarget.ID())

	err := api.EnableMountpath(baseParams, selectedTarget, selectedMpath)
	if err != nil {
		t.Errorf("Enabling available mountpath should return success, got: %v", err)
	}

	err = api.EnableMountpath(baseParams, selectedTarget, selectedMpath+"some_text")
	if err == nil {
		t.Errorf("Enabling non-existing mountpath should return error")
	} else {
		httpErr := err.(*cmn.HTTPError)
		if httpErr.Status != http.StatusNotFound {
			t.Errorf("Expected status: %d, got: %d. Error: %s", http.StatusNotFound, httpErr.Status, err.Error())
		}
	}
}

func TestFSCheckerTargetDisable(t *testing.T) {
	var (
		target *cluster.Snode

		proxyURL   = getPrimaryURL(t, proxyURLReadOnly)
		baseParams = tutils.DefaultBaseAPIParams(t)
		smap       = getClusterMap(t, proxyURL)
		proxyCnt   = smap.CountProxies()
		targetCnt  = smap.CountTargets()
	)

	if targetCnt < 2 {
		t.Skip("The number of targets must be at least 2")
	}

	for _, tinfo := range smap.Tmap {
		target = tinfo
		break
	}
	oldMpaths, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)
	if len(oldMpaths.Available) == 0 {
		t.Fatalf("Target %s does not have available mountpaths", target)
	}

	tutils.Logf("Removing all mountpaths from target: %s\n", target)
	for _, mpath := range oldMpaths.Available {
		err = api.DisableMountpath(baseParams, target.ID(), mpath)
		tassert.CheckFatal(t, err)
	}

	smap, err = tutils.WaitForPrimaryProxy(proxyURL, "all mpath disabled", smap.Version, false, proxyCnt, targetCnt-1)
	tassert.CheckFatal(t, err)

	tutils.Logf("Restoring target %s mountpaths\n", target.ID())
	for _, mpath := range oldMpaths.Available {
		err = api.EnableMountpath(baseParams, target, mpath)
		tassert.CheckFatal(t, err)
	}

	_, err = tutils.WaitForPrimaryProxy(proxyURL, "all mpath enabled", smap.Version, false, proxyCnt, targetCnt)
	tassert.CheckFatal(t, err)
}
