/*
 * Copyright (c) 2018, NVIDIA CORPORATION. All rights reserved.
 */

package dfc

import (
	"fmt"
	"os"
	"testing"
	"time"
)

const (
	fsKeeperTmpDir = "/tmp/fskeeper"
)

func testTmpFileName(fname string) string {
	return fname + "-tmp"
}

func testKeeperMountPaths() *mountedFS {
	CreateDir(fsKeeperTmpDir)
	CreateDir(fsKeeperTmpDir + "/1")
	CreateDir(fsKeeperTmpDir + "/2")

	avail := make(map[string]*mountPath)
	unavail := make(map[string]*mountPath)

	for i := 1; i < 3; i++ {
		name := fmt.Sprintf("%s/%d", fsKeeperTmpDir, i)
		avail[name] = &mountPath{Path: name}
	}
	name := fmt.Sprintf("%s/%d", fsKeeperTmpDir, 3)
	unavail[name] = &mountPath{Path: name}

	return &mountedFS{
		Available: avail,
		Offline:   unavail,
	}
}

func testKeeperConfig() *fskeeperconf {
	return &fskeeperconf{
		Enabled:            true,
		FSCheckTime:        time.Second * 3,
		OfflineFSCheckTime: time.Second * 3,
	}
}

func testKeeperCleanup() {
	os.RemoveAll(fsKeeperTmpDir)
}

func TestFSKeeper(t *testing.T) {
	keeper := newFSKeeper(testKeeperConfig(), testKeeperMountPaths(), testTmpFileName)

	if keeper == nil {
		t.Error("Failed to create keeper")
	}

	// intial state = 2 availble FSes - must pass
	keeper.checkAlivePaths("")
	if len(keeper.mountpaths.Available) != 2 || len(keeper.mountpaths.Offline) != 1 {
		t.Errorf("CheckAlivePath changes mountpoints: %v - %v",
			keeper.mountpaths.Available, keeper.mountpaths.Offline)
	}

	// intial state = 1 offline FS - must pass
	keeper.checkOfflinePaths("")
	if len(keeper.mountpaths.Available) != 2 || len(keeper.mountpaths.Offline) != 1 {
		t.Errorf("CheckOfflinePath changes mountpoints: %v - %v",
			keeper.mountpaths.Available, keeper.mountpaths.Offline)
	}

	// make offline FS available
	CreateDir(fsKeeperTmpDir + "/3")
	// wait until information about FSes expires
	time.Sleep(keeper.config.OfflineFSCheckTime * 2) // wait for time OfflineFSCheckTime passes
	// the offline FS must be detected as available
	keeper.checkOfflinePaths("")
	if len(keeper.mountpaths.Available) != 3 || len(keeper.mountpaths.Offline) != 0 {
		t.Errorf("CheckOfflinePath should make directory '3' available: %v - %v",
			keeper.mountpaths.Available, keeper.mountpaths.Offline)
	}

	// refresh last time check for FSes
	keeper.checkAlivePaths("")
	if len(keeper.mountpaths.Available) != 3 || len(keeper.mountpaths.Offline) != 0 {
		t.Errorf("CheckAlivePath changes mountpoints: %v - %v",
			keeper.mountpaths.Available, keeper.mountpaths.Offline)
	}

	// make the path unavailable again
	os.RemoveAll(fsKeeperTmpDir + "/3")
	keeper.checkAlivePaths("")
	// warm check should not detect any trouble
	if len(keeper.mountpaths.Available) != 3 || len(keeper.mountpaths.Offline) != 0 {
		t.Errorf("Warm CheckAlivePath changes mountpoints: %v - %v",
			keeper.mountpaths.Available, keeper.mountpaths.Offline)
	}
	// initiate cold check that must mark the FS as offline
	keeper.checkAlivePaths("Check now")
	if len(keeper.mountpaths.Available) != 2 || len(keeper.mountpaths.Offline) != 1 {
		t.Errorf("Cold CheckAlivePath did not detect FS dead: %v - %v",
			keeper.mountpaths.Available, keeper.mountpaths.Offline)
	}

	CreateDir(fsKeeperTmpDir + "/3")
	keeper.checkOfflinePaths("/some/path")
	if len(keeper.mountpaths.Available) != 3 || len(keeper.mountpaths.Offline) != 0 {
		t.Errorf("CheckOfflinePath should make directory '3' available: %v - %v",
			keeper.mountpaths.Available, keeper.mountpaths.Offline)
	}
	os.RemoveAll(fsKeeperTmpDir + "/3")
	os.RemoveAll(fsKeeperTmpDir + "/2")
	keeper.checkAlivePaths(fmt.Sprintf("%s/2/filetest", fsKeeperTmpDir))
	if len(keeper.mountpaths.Available) != 2 || len(keeper.mountpaths.Offline) != 1 {
		t.Errorf("CheckOfflinePath should make only one directory unavailable: %v - %v",
			keeper.mountpaths.Available, keeper.mountpaths.Offline)
	}
	if _, ok := keeper.mountpaths.Available[fsKeeperTmpDir+"/2"]; ok {
		t.Errorf("CheckOfflinePath should make directory '2' unavailable: %#v",
			keeper.mountpaths.Available)
	}

	testKeeperCleanup()
}

func TestFSKeeperFilename(t *testing.T) {
	keeper := newFSKeeper(testKeeperConfig(), testKeeperMountPaths(), testTmpFileName)

	if keeper == nil {
		t.Error("Failed to create keeper")
	}

	type mpathTest struct {
		mpath    string
		expected string
	}
	mpathTests := []mpathTest{
		mpathTest{fsKeeperTmpDir + "/11", ""},
		mpathTest{fsKeeperTmpDir + "/1/45", fsKeeperTmpDir + "/1"},
		mpathTest{"/abcd/1", ""},
		mpathTest{fsKeeperTmpDir, ""},
		mpathTest{fsKeeperTmpDir + "/3/abc", ""},
		mpathTest{fsKeeperTmpDir + "/2/abc/def", fsKeeperTmpDir + "/2"},
	}
	for _, mtest := range mpathTests {
		res := keeper.filenameToMpath(mtest.mpath)
		if res != mtest.expected {
			t.Errorf("For [%s] expected [%s] but got [%s]", mtest.mpath, mtest.expected, res)
		}
	}

	failedName := fsKeeperTmpDir + "/1/abc"
	keeper.setFailedFilename(fsKeeperTmpDir+"/1", failedName)
	failedNameRead := keeper.getFailedFilename(fsKeeperTmpDir + "/1")
	if failedName != failedNameRead {
		t.Errorf("Failed to set/get failed filename, expected [%s], got [%s]", failedName, failedNameRead)
	}
	failedNameRead = keeper.getFailedFilename(fsKeeperTmpDir + "/2")
	if failedNameRead != "" {
		t.Errorf("Failed to get failed filename, got [%s] instead of empty", failedNameRead)
	}
	keeper.setFailedFilename(fsKeeperTmpDir+"/1", "")
	failedNameRead = keeper.getFailedFilename(fsKeeperTmpDir + "/1")
	if failedNameRead != "" {
		t.Errorf("Failed to clean failed filename, got [%s] instead of empty", failedNameRead)
	}

	testKeeperCleanup()
}
