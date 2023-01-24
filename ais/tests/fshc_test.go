// Package integration contains AIS integration tests.
/*
 * Copyright (c) 2018-2023, NVIDIA CORPORATION. All rights reserved.
 */
package integration

import (
	"fmt"
	"net/http"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/NVIDIA/aistore/api"
	"github.com/NVIDIA/aistore/api/apc"
	"github.com/NVIDIA/aistore/cluster"
	"github.com/NVIDIA/aistore/cmn"
	"github.com/NVIDIA/aistore/cmn/cos"
	"github.com/NVIDIA/aistore/tools"
	"github.com/NVIDIA/aistore/tools/readers"
	"github.com/NVIDIA/aistore/tools/tassert"
	"github.com/NVIDIA/aistore/tools/tlog"
	"github.com/NVIDIA/aistore/tools/trand"
)

const (
	fshcDetectTimeMax = time.Second * 10
	fshcRunTimeMax    = time.Second * 15
	fshcDir           = "fschecker"
)

type checkerMD struct {
	t          *testing.T
	seed       int64
	numObjs    int
	proxyURL   string
	bck        cmn.Bck
	smap       *cluster.Smap
	mpList     cluster.NodeMap
	allMps     map[string]*apc.MountpathList
	origAvail  int
	fileSize   int64
	baseParams api.BaseParams
	chstop     chan struct{}
	chfail     chan struct{}
	wg         *sync.WaitGroup
}

func newCheckerMD(t *testing.T) *checkerMD {
	md := &checkerMD{
		t:        t,
		seed:     300,
		proxyURL: tools.RandomProxyURL(),
		bck: cmn.Bck{
			Name:     testBucketName,
			Provider: apc.AIS,
		},
		fileSize: 64 * cos.KiB,
		mpList:   make(cluster.NodeMap, 10),
		allMps:   make(map[string]*apc.MountpathList, 10),
		chstop:   make(chan struct{}),
		chfail:   make(chan struct{}),
		wg:       &sync.WaitGroup{},
	}

	md.init()
	md.numObjs = 20 * len(md.mpList)
	tlog.Logf("Create %d objects[%d mountpaths] for test\n", md.numObjs, len(md.mpList))

	return md
}

func (md *checkerMD) init() {
	md.baseParams = tools.BaseAPIParams(md.proxyURL)
	md.smap = tools.GetClusterMap(md.t, md.proxyURL)

	for targetID, tsi := range md.smap.Tmap {
		tlog.Logf("Target: %s\n", targetID)
		lst, err := api.GetMountpaths(md.baseParams, tsi)
		tassert.CheckFatal(md.t, err)
		tlog.Logf("    Mountpaths: %v\n", lst)

		for _, mpath := range lst.Available {
			si, ok := md.mpList[mpath]
			tassert.Errorf(md.t, !ok, "duplication (%s, %s, %s)", si, mpath, tsi)
			md.mpList[mpath] = tsi
		}
		md.allMps[targetID] = lst

		md.origAvail += len(lst.Available)
	}
}

func (md *checkerMD) ensureNumMountpaths(target *cluster.Snode, mpList *apc.MountpathList) {
	ensureNumMountpaths(md.t, target, mpList)
}

func (md *checkerMD) randomTargetMpath() (target *cluster.Snode, mpath string, mpathMap *apc.MountpathList) {
	// select random target and mountpath
	for m, t := range md.mpList {
		target, mpath = t, m
		mpathMap = md.allMps[target.ID()]
		break
	}
	return
}

func (md *checkerMD) runTestAsync(method string, target *cluster.Snode, mpath string, mpathList *apc.MountpathList, suffix string) {
	md.wg.Add(1)
	go runAsyncJob(md.t, md.bck, md.wg, method, mpath, fileNames, md.chfail, md.chstop, suffix)
	// let the job run for a while and then make a mountpath broken
	time.Sleep(2 * time.Second)
	md.chfail <- struct{}{}
	if detected := waitForMountpathChanges(md.t, target, len(mpathList.Available)-1, len(mpathList.Disabled)+1, true); detected {
		// let the job run for a while with broken mountpath, so FSHC detects the trouble
		time.Sleep(2 * time.Second)
		md.chstop <- struct{}{}
	}
	md.wg.Wait()

	repairMountpath(md.t, target, mpath, len(mpathList.Available), len(mpathList.Disabled), suffix)
}

func (md *checkerMD) runTestSync(method string, target *cluster.Snode, mpath string, mpathList *apc.MountpathList,
	objList []string, suffix string) {
	breakMountpath(md.t, mpath, suffix)
	defer repairMountpath(md.t, target, mpath, len(mpathList.Available), len(mpathList.Disabled), suffix)

	switch method {
	case http.MethodPut:
		p, err := api.HeadBucket(md.baseParams, md.bck, true /* don't add */)
		tassert.CheckFatal(md.t, err)
		for _, objName := range objList {
			r, _ := readers.NewRandReader(md.fileSize, p.Cksum.Type)
			err := api.PutObject(api.PutArgs{
				BaseParams: md.baseParams,
				Bck:        md.bck,
				ObjName:    path.Join(fshcDir, objName),
				Reader:     r,
				Size:       uint64(md.fileSize),
			})
			if err != nil {
				tlog.Logf("%s: %v\n", objName, err)
			}
		}
	case http.MethodGet:
		for _, objName := range objList {
			// GetObject must fail - so no error checking
			_, err := api.GetObject(md.baseParams, md.bck, objName, nil)
			if err == nil {
				md.t.Errorf("Get %q must fail", objName)
			}
		}
	}

	if detected := waitForMountpathChanges(md.t, target, len(mpathList.Available)-1, len(mpathList.Disabled)+1, false); detected {
		md.t.Error("PUT objects to a broken mountpath should not disable the mountpath when FSHC is disabled")
	}
}

func waitForMountpathChanges(t *testing.T, target *cluster.Snode, availLen, disabledLen int, failIfDiffer bool) bool {
	var (
		err        error
		newMpaths  *apc.MountpathList
		baseParams = tools.BaseAPIParams()
	)

	detectStart := time.Now()
	detectLimit := time.Now().Add(fshcDetectTimeMax)

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
	tlog.Logf("passed %v\n", detectTime)

	if len(newMpaths.Disabled) == disabledLen && len(newMpaths.Available) == availLen {
		tlog.Logf("Check is successful in %v\n", detectTime)
		return true
	}

	if !failIfDiffer {
		return false
	}

	tlog.Logf("Current mpath list: %v\n", newMpaths)
	if len(newMpaths.Disabled) != disabledLen {
		t.Errorf("Disabled mpath count mismatch, old count: %v, new list: %v",
			disabledLen, newMpaths.Disabled)
	} else if len(newMpaths.Available) != availLen {
		t.Errorf("Available mpath count mismatch, old count: %v, new list: %v",
			availLen, newMpaths.Available)
	}
	return false
}

// Simulating mountpath death requested.
// It is the easiest way to simulate: stop putting data and
// replace the mountpath with regular file. If we do not stop
// putting objects it recreates the mountpath and does not fail
func breakMountpath(t *testing.T, mpath, suffix string) {
	os.Rename(mpath, mpath+suffix)
	f, err := os.OpenFile(mpath, os.O_CREATE|os.O_WRONLY, cos.PermRWR)
	if err != nil {
		t.Errorf("Failed to create file: %v", err)
	}
	f.Close()
}

func repairMountpath(t *testing.T, target *cluster.Snode, mpath string, availLen, disabledLen int, suffix string) {
	var (
		err        error
		baseParams = tools.BaseAPIParams()
	)

	// "broken" mpath does no exist, nothing to restore
	if err := cos.Stat(mpath + suffix); err != nil {
		return
	}
	// cleanup
	// restore original mountpath
	os.Remove(mpath)
	cos.Rename(mpath+suffix, mpath)

	// ask fschecker to check all mountpath - it should make disabled
	// mountpath back to available list
	api.EnableMountpath(baseParams, target, mpath)
	tlog.Logln("Recheck mountpaths")
	detectStart := time.Now()
	detectLimit := time.Now().Add(fshcDetectTimeMax)
	var mpaths *apc.MountpathList
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

func runAsyncJob(t *testing.T, bck cmn.Bck, wg *sync.WaitGroup, op, mpath string, filelist []string, chfail,
	chstop chan struct{}, suffix string) {
	defer wg.Done()

	const fileSize = 64 * cos.KiB
	var (
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
	)

	tlog.Logf("Testing mpath fail detection on %s\n", op)
	stopTime := time.Now().Add(fshcRunTimeMax)

	p, err := api.HeadBucket(baseParams, bck, true /* don't add */)
	tassert.CheckFatal(t, err)

	for stopTime.After(time.Now()) {
		errCh := make(chan error, len(filelist))
		objsPutCh := make(chan string, len(filelist))

		for _, fname := range filelist {
			select {
			case <-chfail:
				breakMountpath(t, mpath, suffix)
			case <-chstop:
				return
			default:
				// do nothing and just start the next loop
			}

			switch op {
			case "PUT":
				r, _ := readers.NewRandReader(fileSize, p.Cksum.Type)
				api.PutObject(api.PutArgs{
					BaseParams: baseParams,
					Bck:        bck,
					ObjName:    path.Join(fshcDir, fname),
					Reader:     r,
					Size:       fileSize,
				})
			case "GET":
				api.GetObject(baseParams, bck, path.Join(fshcDir, fname), nil)
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
	// TODO -- FIXME:
	// revise all fs-checker tests that manipulate mountpaths, make sure
	// those (mountpaths) are always getting restored correctly when (and if) a test fails -
	// then remove the "skipping" - here and elsewhere
	if true {
		t.Skipf("skipping %s", t.Name())
	}
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	var (
		md     = newCheckerMD(t)
		suffix = "-" + trand.String(5)
	)

	if md.origAvail == 0 {
		t.Fatal("No available mountpaths found")
	}

	tools.CreateBucketWithCleanup(t, md.proxyURL, md.bck, nil)
	selectedTarget, selectedMpath, selectedMpathList := md.randomTargetMpath()
	tlog.Logf("mountpath %s of %s is selected for the test\n", selectedMpath, selectedTarget.StringEx())
	defer func() {
		if err := api.DetachMountpath(md.baseParams, selectedTarget, selectedMpath, true /*dont-resil*/); err != nil {
			t.Logf("Failed to remove mpath %s of %s: %v", selectedMpath, selectedTarget.StringEx(), err)
		}
		if err := api.AttachMountpath(md.baseParams, selectedTarget, selectedMpath, false /*force*/); err != nil {
			t.Logf("Failed to add mpath %s of %s: %v", selectedMpath, selectedTarget.StringEx(), err)
		}

		tools.WaitForResilvering(t, md.baseParams, nil)

		md.ensureNumMountpaths(selectedTarget, md.allMps[selectedTarget.ID()])
	}()

	// generate some filenames to PUT to them in a loop
	generateRandomNames(md.numObjs)

	// Checking detection on object PUT
	md.runTestAsync(http.MethodPut, selectedTarget, selectedMpath, selectedMpathList, suffix)
	// Checking detection on object GET
	md.runTestAsync(http.MethodGet, selectedTarget, selectedMpath, selectedMpathList, suffix)

	// Checking that reading "bad" objects does not disable mpath if the mpath is OK
	tlog.Logf("Reading non-existing objects: read is expected to fail but mountpath must be available\n")
	for n := 1; n < 10; n++ {
		objName := fmt.Sprintf("%s/o%d", fshcDir, n)
		if _, err := api.GetObject(md.baseParams, md.bck, objName, nil); err == nil {
			t.Error("Should not be able to GET non-existing objects")
		}
	}
	if detected := waitForMountpathChanges(t, selectedTarget, len(selectedMpathList.Available), len(selectedMpathList.Disabled), false); !detected {
		t.Error("GETting non-existing objects should not disable mountpath")
		repairMountpath(t, selectedTarget, selectedMpath, len(selectedMpathList.Available), len(selectedMpathList.Disabled), suffix)
	}
}

func TestFSCheckerDetectionDisabled(t *testing.T) {
	if true {
		t.Skipf("skipping %s", t.Name())
	}
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})

	var (
		md     = newCheckerMD(t)
		suffix = "-" + trand.String(5)
	)

	if md.origAvail == 0 {
		t.Fatal("No available mountpaths found")
	}

	tlog.Logf("*** Testing with disabled FSHC***\n")
	tools.SetClusterConfig(t, cos.StrKVs{"fshc.enabled": "false"})
	defer tools.SetClusterConfig(t, cos.StrKVs{"fshc.enabled": "true"})

	selectedTarget, selectedMpath, selectedMap := md.randomTargetMpath()
	tlog.Logf("mountpath %s of %s is selected for the test\n", selectedMpath, selectedTarget.StringEx())
	tools.CreateBucketWithCleanup(t, md.proxyURL, md.bck, nil)
	defer func() {
		if err := api.DetachMountpath(md.baseParams, selectedTarget, selectedMpath, true /*dont-resil*/); err != nil {
			t.Logf("Failed to remove mpath %s of %s: %v", selectedMpath, selectedTarget.StringEx(), err)
		}
		if err := api.AttachMountpath(md.baseParams, selectedTarget, selectedMpath, false /*force*/); err != nil {
			t.Logf("Failed to add mpath %s of %s: %v", selectedMpath, selectedTarget.StringEx(), err)
		}

		tools.WaitForResilvering(t, md.baseParams, nil)

		md.ensureNumMountpaths(selectedTarget, md.allMps[selectedTarget.ID()])
	}()

	// generate a short list of file to run the test (to avoid flooding the log with false errors)
	objList := make([]string, 0, 5)
	for n := 0; n < 5; n++ {
		objName := fmt.Sprintf("obj-fshc-%d", n)
		objList = append(objList, objName)
	}

	// Checking detection on object PUT
	md.runTestSync(http.MethodPut, selectedTarget, selectedMpath, selectedMap, objList, suffix)
	// Checking detection on object GET
	md.runTestSync(http.MethodGet, selectedTarget, selectedMpath, selectedMap, objList, suffix)
}

func TestFSCheckerEnablingMountpath(t *testing.T) {
	if true {
		t.Skipf("skipping %s", t.Name())
	}
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})
	var (
		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams(proxyURL)
		smap       = tools.GetClusterMap(t, proxyURL)
		mpList     = make(cluster.NodeMap, 10)
		origAvail  = 0
	)

	for targetID, tsi := range smap.Tmap {
		tlog.Logf("Target: %s\n", targetID)
		lst, err := api.GetMountpaths(baseParams, tsi)
		tassert.CheckFatal(t, err)
		tlog.Logf("    Mountpaths: %v\n", lst)

		for _, mpath := range lst.Available {
			mpList[mpath] = tsi
		}

		origAvail += len(lst.Available)
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

	origMpl, err := api.GetMountpaths(baseParams, selectedTarget)
	tassert.CheckFatal(t, err)

	err = api.EnableMountpath(baseParams, selectedTarget, selectedMpath)
	if err != nil {
		t.Errorf("Enabling available mountpath should return success, got: %v", err)
	}

	err = api.EnableMountpath(baseParams, selectedTarget, selectedMpath+"some_text")
	if err == nil {
		t.Errorf("Enabling non-existing mountpath should return error")
	} else {
		status := api.HTTPStatus(err)
		if status != http.StatusNotFound {
			t.Errorf("Expected status %d, got %d, %v", http.StatusNotFound, status, err)
		}
	}
	tools.WaitForResilvering(t, baseParams, selectedTarget)

	ensureNumMountpaths(t, selectedTarget, origMpl)
}

func TestFSCheckerTargetDisableAllMountpaths(t *testing.T) {
	if true {
		t.Skipf("skipping %s", t.Name())
	}
	tools.CheckSkip(t, tools.SkipTestArgs{Long: true})
	var (
		target *cluster.Snode

		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams()
		smap       = tools.GetClusterMap(t, proxyURL)
		proxyCnt   = smap.CountActivePs()
		targetCnt  = smap.CountActiveTs()
	)

	if targetCnt < 2 {
		t.Skip("The number of targets must be at least 2")
	}

	target, _ = smap.GetRandTarget()
	oldMpaths, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)
	if len(oldMpaths.Available) == 0 {
		t.Fatalf("Target %s does not have mountpaths", target)
	}

	tlog.Logf("Removing all mountpaths from target: %s\n", target.StringEx())
	for _, mpath := range oldMpaths.Available {
		err = api.DisableMountpath(baseParams, target, mpath, true /*dont-resil*/)
		tassert.CheckFatal(t, err)
	}

	smap, err = tools.WaitForClusterState(proxyURL, "all mountpaths disabled", smap.Version, proxyCnt, targetCnt-1)
	tassert.CheckFatal(t, err)
	tlog.Logf("Wait for rebalance (triggered by %s leaving the cluster after having lost all mountpaths)\n",
		target.StringEx())
	args := api.XactReqArgs{Kind: apc.ActRebalance, Timeout: rebalanceTimeout}
	_, _ = api.WaitForXactionIC(baseParams, args)

	tlog.Logf("Restoring target %s mountpaths\n", target.ID())
	for _, mpath := range oldMpaths.Available {
		err = api.EnableMountpath(baseParams, target, mpath)
		tassert.CheckFatal(t, err)
	}

	_, err = tools.WaitForClusterState(proxyURL, "all mountpaths enabled", smap.Version, proxyCnt, targetCnt)
	tassert.CheckFatal(t, err)

	tlog.Logf("Wait for rebalance (when target %s that has previously lost all mountpaths joins back)\n", target.StringEx())
	args = api.XactReqArgs{Kind: apc.ActRebalance, Timeout: rebalanceTimeout}
	_, _ = api.WaitForXactionIC(baseParams, args)

	tools.WaitForResilvering(t, baseParams, nil)

	ensureNumMountpaths(t, target, oldMpaths)
}

func TestFSAddMountpathRestartNode(t *testing.T) {
	if true {
		t.Skipf("skipping %s", t.Name())
	}
	var (
		target *cluster.Snode

		proxyURL   = tools.RandomProxyURL()
		baseParams = tools.BaseAPIParams()
		smap       = tools.GetClusterMap(t, proxyURL)
		proxyCnt   = smap.CountProxies()
		targetCnt  = smap.CountActiveTs()
		tmpMpath   = "/tmp/testmp"
	)
	if targetCnt < 2 {
		t.Skip("The number of targets must be at least 2")
	}
	target, _ = smap.GetRandTarget()
	oldMpaths, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)
	numMpaths := len(oldMpaths.Available)
	tassert.Fatalf(t, numMpaths != 0, "target %s doesn't have mountpaths", target.StringEx())

	cos.CreateDir(tmpMpath)
	tlog.Logf("Adding mountpath to %s\n", target.StringEx())
	err = api.AttachMountpath(baseParams, target, tmpMpath, true /*force*/)
	tassert.CheckFatal(t, err)

	tools.WaitForResilvering(t, baseParams, target)

	t.Cleanup(func() {
		api.DetachMountpath(baseParams, target, tmpMpath, true /*dont-resil*/)
		time.Sleep(2 * time.Second)
		os.Remove(tmpMpath)

		ensureNumMountpaths(t, target, oldMpaths)
	})

	newMpaths, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)

	tassert.Fatalf(t, numMpaths+1 == len(newMpaths.Available),
		"should add new mountpath - available %d!=%d", numMpaths+1, len(newMpaths.Available))

	// Kill and restore target
	tlog.Logf("Killing %s\n", target.StringEx())
	tcmd, err := tools.KillNode(target)
	tassert.CheckFatal(t, err)
	smap, err = tools.WaitForClusterState(proxyURL, "target removed", smap.Version, proxyCnt, targetCnt-1)

	tassert.CheckError(t, err)
	tools.RestoreNode(tcmd, false, "target")
	smap, err = tools.WaitForClusterState(smap.Primary.URL(cmn.NetPublic), "target restored", smap.Version,
		proxyCnt, targetCnt)
	tassert.CheckFatal(t, err)
	if _, ok := smap.Tmap[target.ID()]; !ok {
		t.Fatalf("Removed target didn't rejoin")
	}
	tlog.Logf("Wait for rebalance\n")
	args := api.XactReqArgs{Kind: apc.ActRebalance, Timeout: rebalanceTimeout}
	_, _ = api.WaitForXactionIC(baseParams, args)

	// Check if the node has newly added mountpath
	newMpaths, err = api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, numMpaths+1 == len(newMpaths.Available),
		"should include newly added mountpath after restore - available %d!=%d", numMpaths+1, len(newMpaths.Available))
}

func TestFSDisableAllExceptOneMountpathRestartNode(t *testing.T) {
	if true {
		t.Skipf("skipping %s", t.Name())
	}
	tools.CheckSkip(t, tools.SkipTestArgs{
		Long:               true,
		MinMountpaths:      3,
		MinTargets:         2,
		RequiredDeployment: tools.ClusterTypeLocal,
	})
	var (
		target *cluster.Snode

		smap       = tools.GetClusterMap(t, tools.RandomProxyURL())
		baseParams = tools.BaseAPIParams()
		proxyURL   = smap.Primary.URL(cmn.NetPublic)
		proxyCnt   = smap.CountProxies()
		targetCnt  = smap.CountActiveTs()
		enabled    bool
	)
	for _, tsi := range smap.Tmap {
		target = tsi
		break
	}

	oldMpaths, err := api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)
	mpathCnt := len(oldMpaths.Available)
	tlog.Logf("Target %s has %d mountpaths\n", target.ID(), mpathCnt)

	// Disable, temporarily, all mountpaths except 1.
	mpaths := oldMpaths.Available[:mpathCnt-1]
	for _, mpath := range mpaths {
		tlog.Logf("Disable mountpath %q at %s\n", mpath, target.StringEx())
		err = api.DisableMountpath(baseParams, target, mpath, false /*dont-resil*/)
		tassert.CheckFatal(t, err)
	}
	tools.WaitForResilvering(t, baseParams, target)

	t.Cleanup(func() {
		if enabled {
			return
		}
		for _, mpath := range mpaths {
			api.EnableMountpath(baseParams, target, mpath)
		}
		time.Sleep(time.Second)

		tools.WaitForResilvering(t, baseParams, target)

		ensureNumMountpaths(t, target, oldMpaths)
	})

	// Kill and restore target
	tlog.Logf("Killing target %s\n", target.StringEx())
	tcmd, err := tools.KillNode(target)
	tassert.CheckFatal(t, err)
	smap, err = tools.WaitForClusterState(proxyURL, "remove target", smap.Version, proxyCnt, targetCnt-1)
	tassert.CheckFatal(t, err)

	time.Sleep(time.Second)
	err = tools.RestoreNode(tcmd, false, "target")
	tassert.CheckFatal(t, err)
	smap, err = tools.WaitForClusterState(proxyURL, "restore", smap.Version, proxyCnt, targetCnt)
	tassert.CheckFatal(t, err)
	tassert.Fatalf(t, smap.GetTarget(target.ID()) != nil, "removed target didn't rejoin")

	args := api.XactReqArgs{Kind: apc.ActRebalance, Timeout: rebalanceTimeout}
	_, _ = api.WaitForXactionIC(baseParams, args)

	// Check if the the mountpaths are disabled after restart.
	newMpaths, err := api.GetMountpaths(baseParams, target)
	tassert.CheckError(t, err)
	tassert.Errorf(
		t, len(newMpaths.Available) == 1,
		"unexpected count of available mountpaths, got: %d, expected: %d",
		len(newMpaths.Available), 1,
	)
	tassert.Errorf(
		t, len(newMpaths.Disabled) == mpathCnt-1,
		"unexpected count of disabled mountpaths, got: %d, expected: %d",
		len(newMpaths.Disabled), mpathCnt-1,
	)

	// Re-enable the mountpaths
	for _, mpath := range mpaths {
		err = api.EnableMountpath(baseParams, target, mpath)
		tassert.CheckFatal(t, err)
	}
	tools.WaitForResilvering(t, baseParams, target)

	enabled = true

	newMpaths, err = api.GetMountpaths(baseParams, target)
	tassert.CheckFatal(t, err)
	tassert.Errorf(
		t, len(newMpaths.Available) == mpathCnt,
		"unexpected count of available mountpaths, got: %d, expected: %d",
		len(newMpaths.Available), mpathCnt,
	)
	tassert.Errorf(
		t, len(newMpaths.Disabled) == 0,
		"unexpected count of disabled mountpaths, got: %d, expected: %d",
		len(newMpaths.Disabled), 0,
	)
}
